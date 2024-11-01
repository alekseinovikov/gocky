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
	defaultKeyTTL           = 10 * time.Second
	defaultSpinLockDuration = 5 * time.Second

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

func (r *redisLockFactory) GetLock(
	lockName string,
	ctx context.Context,
	options ...func(config *gocky.Config),
) (gocky.Lock, error) {
	return r.lockCache.GetLock(lockName, ctx, func(ctx context.Context) (gocky.Lock, error) {
		config := &gocky.Config{
			TTL:                 defaultKeyTTL,
			LockRefreshInterval: defaultSpinLockDuration,
		}

		for _, option := range options {
			option(config)
		}

		redisLock := &redisLock{
			client: r.client,
			ctx:    ctx,
			name:   lockName,
			key:    generateKey(lockName),
			config: config,
		}

		return common.NewUpdatableLock(ctx, config, redisLock), nil
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
	config *gocky.Config
	client *redis.Client
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

func (r *redisLock) ProlongLock() error {
	expire := r.client.Expire(r.ctx, r.key, r.config.TTL)
	return expire.Err()
}

func (r *redisLock) TryToAcquireLock() (bool, error) {
	milliseconds := r.config.TTL.Milliseconds()
	result := lockAcquireScriptDescriptor.Run(r.ctx, r.client, []string{r.key}, milliseconds)
	if result.Err() != nil {
		return false, result.Err()
	}

	if result.Val() == "OK" {
		return true, nil
	}

	return false, nil
}

func (r *redisLock) ReleaseLock() {
	r.client.Del(r.ctx, r.key)
}
