package redis

import (
	"context"
	"github.com/alekseinovikov/gocky/core"
	"github.com/redis/go-redis/v9"
)

type RedisLock struct {
	job    *core.Job
	client *redis.Client
}

type redisLocker struct {
	client *redis.Client
}

func Init(options redis.Options) {
	client := redis.NewClient(&options)
	locker := &redisLocker{client: client}

	core.RegisterJobLocker(locker)
}

func (r *redisLocker) NewLock(job *core.Job) core.Lock {
	return &RedisLock{
		job:    job,
		client: r.client,
	}
}

func (l *RedisLock) Lock() (bool, error) {
	ctx := context.Background()
	lockName := l.lockName()

	incrValue, err := l.client.Incr(ctx, lockName).Result()
	if err != nil {
		return false, err
	}

	// the value has been already increased by another process
	if incrValue > 1 {
		return false, nil
	}

	// set the expiration time
	l.client.Expire(ctx, lockName, l.job.Duration)
	return true, nil
}

func (l *RedisLock) Unlock() {
	ctx := context.Background()
	lockName := l.lockName()

	l.client.Del(ctx, lockName)
}

func (l *RedisLock) lockName() string {
	return "gocky:" + l.job.Name + ":lock"
}
