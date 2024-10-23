package redis

import (
	"context"
	"github.com/alekseinovikov/gocky/core"
	"github.com/redis/go-redis/v9"
	"sync"
	"time"
)

var (
	keyPrefix               = "gocky:lock:"
	defaultKeyTTL           = time.Second            // minimal supported TTL for redis is 1 second
	defaultSpinLockDuration = 100 * time.Millisecond // default spin lock duration
)

type redisLockFactory struct {
	client      *redis.Client
	cache       map[string]core.Lock
	cacheRWLock sync.RWMutex
}

func NewRedisLockFactory(options redis.Options) core.LockFactory {
	client := redis.NewClient(&options)
	return &redisLockFactory{
		client: client,
		cache:  make(map[string]core.Lock),
	}
}

func (r *redisLockFactory) NewLock(lockName string) core.Lock {
	r.cacheRWLock.RLock()
	if lock, ok := r.cache[lockName]; ok {
		r.cacheRWLock.RUnlock()
		return lock
	}
	r.cacheRWLock.RUnlock()

	r.cacheRWLock.Lock()
	defer r.cacheRWLock.Unlock()

	newLock := &redisLock{
		client: r.client,
		name:   lockName,
		key:    generateKey(lockName),
	}
	r.cache[lockName] = newLock
	return newLock
}

// generate key for lock
func generateKey(lockName string) string {
	return keyPrefix + lockName
}

type redisLock struct {
	name       string
	key        string
	client     *redis.Client
	ticker     *time.Ticker
	tickerDone chan struct{}
}

func (r *redisLock) Name() string {
	return r.name
}

func (r *redisLock) TryLock() (bool, error) {
	return r.tryToUpdateRedisLock()
}

func (r *redisLock) Lock() error {
	locked, err := r.TryLock()
	if err != nil {
		return err
	}

	// we are trying to keep the lock
	for !locked {
		time.Sleep(defaultSpinLockDuration)
		locked, err = r.TryLock()
		if err != nil {
			return err
		}
	}

	// once lock is acquired we are starting a ticker to keep it alive
	r.ticker = time.NewTicker(defaultKeyTTL / 2)
	r.tickerDone = make(chan struct{})
	go func() {
		for {
			select {
			case <-r.tickerDone:
				return
			case <-r.ticker.C:
				_, _ = r.tryToUpdateRedisLock()
			}
		}
	}()

	return nil
}

func (r *redisLock) Unlock() {
	if r.ticker == nil {
		return
	}

	// we stop the ticker
	r.ticker.Stop()
	r.tickerDone <- struct{}{}

	// and remove the lock from redis
	r.client.Del(context.Background(), r.key)
}

func (r *redisLock) tryToUpdateRedisLock() (bool, error) {
	intCmd := r.client.Incr(context.Background(), r.key)
	result, err := intCmd.Result()
	if err != nil {
		return false, err
	}

	if result != 1 {
		return false, nil
	}

	boolCmd := r.client.Expire(context.Background(), r.key, defaultKeyTTL)
	_, err = boolCmd.Result()

	return err == nil, err
}
