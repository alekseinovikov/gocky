package redis

import (
	"context"
	"github.com/alekseinovikov/gocky"
	"github.com/alekseinovikov/gocky/common"
	"github.com/redis/go-redis/v9"
	"time"
)

var (
	keyPrefix               = "gocky:lock:"
	defaultKeyTTL           = 100 // In milliseconds
	defaultSpinLockDuration = time.Duration(defaultKeyTTL/2) * time.Millisecond

	lockAcquireScript = `
		if redis.call("EXISTS", KEYS[1]) == 1 then
			return 0
		end
		return redis.call("SET", KEYS[1], 1, "PX", ARGV[1], "NX")
	`
	lockAcquireScriptDescriptor = redis.NewScript(lockAcquireScript)
)

type redisLockFactory struct {
	client    *redis.Client
	lockCache common.LockCache
}

func NewRedisLockFactory(options redis.Options) (gocky.LockFactory, error) {
	client := redis.NewClient(&options)
	return &redisLockFactory{
		client:    client,
		lockCache: common.NewLockCache(),
	}, nil
}

func (r *redisLockFactory) GetLock(lockName string, ctx context.Context) gocky.Lock {
	return r.lockCache.GetLock(lockName, ctx, func(ctx context.Context) gocky.Lock {
		return &redisLock{
			client: r.client,
			ctx:    ctx,
			name:   lockName,
			key:    generateKey(lockName),
		}
	})
}

// generate key for lock
func generateKey(lockName string) string {
	return keyPrefix + lockName
}

type redisLock struct {
	name       string
	ctx        context.Context
	key        string
	client     *redis.Client
	ticker     *time.Ticker
	tickerDone chan struct{}
}

func (r *redisLock) Name() string {
	return r.name
}

func (r *redisLock) Locked() (bool, error) {
	cmd := r.client.Exists(r.ctx, r.key)
	result, err := cmd.Result()
	if err != nil {
		return false, err
	}

	return result == 1, nil
}

func (r *redisLock) TryLock() (bool, error) {
	locked, err := r.tryToUpdateRedisLock()
	if err != nil || !locked {
		return locked, err
	}

	r.scheduleLockUpdater()
	return true, nil
}

func (r *redisLock) Lock() error {
	locked, err := r.TryLock()
	if err != nil {
		return err
	}

	// we are trying to keep the lock
	for !locked {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
			time.Sleep(defaultSpinLockDuration)
			locked, err = r.TryLock()
			if err != nil {
				return err
			}
		}
	}

	r.scheduleLockUpdater()
	return nil
}

func (r *redisLock) Unlock() {
	defer r.client.Del(r.ctx, r.key)
	if r.ticker == nil {
		return
	}

	// we stop the ticker
	r.ticker.Stop()
	r.tickerDone <- struct{}{}
	close(r.tickerDone)
}

func (r *redisLock) tryToUpdateRedisLock() (bool, error) {
	result := lockAcquireScriptDescriptor.Run(r.ctx, r.client, []string{r.key}, defaultKeyTTL)
	if result.Err() != nil {
		return false, result.Err()
	}

	if result.Val() == "OK" {
		return true, nil
	}

	return false, nil
}

func (r *redisLock) scheduleLockUpdater() {
	// once lock is acquired we are starting a ticker to keep it alive
	r.ticker = time.NewTicker(time.Duration(defaultKeyTTL / 2))
	r.tickerDone = make(chan struct{})
	go func() {
		for {
			select {
			case <-r.tickerDone:
				return
			case <-r.ticker.C:
				_, _ = r.tryToUpdateRedisLock()
			case <-r.ctx.Done():
				r.Unlock()
				return
			}
		}
	}()
}
