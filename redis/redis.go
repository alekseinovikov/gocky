package redis

import (
	"context"
	"github.com/alekseinovikov/gocky"
	"github.com/alekseinovikov/gocky/common"
	"github.com/redis/go-redis/v9"
	"sync"
	"time"
)

var (
	keyPrefix               = "gocky:lock:"
	defaultKeyTTLMillis     = 10000                                                   // In milliseconds - 10 secs
	defaultSpinLockDuration = time.Duration(defaultKeyTTLMillis/2) * time.Millisecond // Every 5 seconds

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

func (r *redisLockFactory) GetLock(lockName string, ctx context.Context) (gocky.Lock, error) {
	return r.lockCache.GetLock(lockName, ctx, func(ctx context.Context) (gocky.Lock, error) {
		return &redisLock{
			client: r.client,
			ctx:    ctx,
			name:   lockName,
			key:    generateKey(lockName),
			ticker: common.NewTicker(defaultSpinLockDuration),
		}, nil
	})
}

// generate key for lock
func generateKey(lockName string) string {
	return keyPrefix + lockName
}

type redisLock struct {
	ctx    context.Context
	name   string
	key    string
	mutex  sync.Mutex
	client *redis.Client
	ticker *common.Ticker
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
	r.mutex.Lock()
	defer r.mutex.Unlock()

	locked, err := r.tryToUpdateRedisLock()
	if err != nil || !locked {
		return false, err
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

	return nil
}

func (r *redisLock) Unlock() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	defer r.client.Del(r.ctx, r.key)

	r.ticker.Stop()
}

func (r *redisLock) scheduleLockUpdater() {
	r.ticker.Start(func() error {
		_, err := r.tryToUpdateRedisLock()
		return err
	})
}

func (r *redisLock) tryToUpdateRedisLock() (bool, error) {
	result := lockAcquireScriptDescriptor.Run(r.ctx, r.client, []string{r.key}, defaultKeyTTLMillis)
	if result.Err() != nil {
		return false, result.Err()
	}

	if result.Val() == "OK" {
		return true, nil
	}

	return false, nil
}
