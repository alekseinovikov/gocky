package redis

import (
	"context"
	"os"
	"testing"
	"time"

	r "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

var (
	client *r.Client
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	redisContainer, err := redis.Run(ctx, "redis:latest")
	if err != nil {
		panic("could not start redis container: " + err.Error())
	}

	host, err := redisContainer.Host(ctx)
	if err != nil {
		panic("could not get redis container connection host: " + err.Error())
	}

	port, err := redisContainer.MappedPort(ctx, "6379")
	if err != nil {
		panic("could not get redis container connection port: " + err.Error())
	}

	options := &r.Options{
		Addr: host + ":" + port.Port(),
	}
	client = r.NewClient(options)
	client.FlushDB(ctx)

	teardown := func() {
		_ = client.Close()
		_ = redisContainer.Terminate(ctx)
	}

	code := m.Run()

	teardown()
	os.Exit(code)
}

func TestNewRedisLockFactory(t *testing.T) {
	factory := NewRedisLockFactory(*client.Options())
	assert.NotNil(t, factory)
}

func TestRedisLockFactory_NewLock(t *testing.T) {
	factory := NewRedisLockFactory(*client.Options())
	lock := factory.NewLock("test-lock")
	assert.NotNil(t, lock)
	assert.Equal(t, "test-lock", lock.Name())
}

func TestRedisLock_TryLock(t *testing.T) {
	factory := NewRedisLockFactory(*client.Options())
	lock := factory.NewLock("test-lock")

	locked, err := lock.TryLock()
	assert.NoError(t, err)
	assert.True(t, locked)

	locked, err = lock.TryLock()
	assert.NoError(t, err)
	assert.False(t, locked)
}

func TestRedisLock_Lock_Unlock(t *testing.T) {
	factory := NewRedisLockFactory(*client.Options())
	lock := factory.NewLock("test-lock")

	err := lock.Lock()
	assert.NoError(t, err)

	lock.Unlock()

	locked, err := lock.TryLock()
	assert.NoError(t, err)
	assert.True(t, locked)
}

func TestRedisLock_Expire(t *testing.T) {
	factory := NewRedisLockFactory(*client.Options())
	lock := factory.NewLock("test-lock")

	err := lock.Lock()
	assert.NoError(t, err)

	go func() {
		time.Sleep(defaultSpinLockDuration)
		lock.Unlock()
	}()

	time.Sleep(defaultKeyTTL + 10*time.Millisecond)

	locked, err := lock.TryLock()
	assert.NoError(t, err)
	assert.True(t, locked)
}

func TestRedisLock_MeasureTimeOfWaitingForLock(t *testing.T) {
	factory := NewRedisLockFactory(*client.Options())
	lock := factory.NewLock("test-lock-measure")

	err := lock.Lock()
	started := time.Now()
	assert.NoError(t, err)
	go func() {
		time.Sleep(20 * time.Millisecond)
		lock.Unlock()
	}()

	_ = lock.Lock()
	finished := time.Now()

	assert.LessOrEqual(t, finished.Sub(started), defaultSpinLockDuration+10*time.Millisecond)
}