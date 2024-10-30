package tests

import (
	"context"
	"database/sql"
	"github.com/alekseinovikov/gocky"
	"github.com/alekseinovikov/gocky/redis"
	goRedis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"os"
	"testing"
)

var (
	redisClient *goRedis.Client
	postgresDb  *sql.DB
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	var redisContainer testcontainers.Container
	redisContainer, redisClient = startRedisContainer(ctx)

	var postgresContainer testcontainers.Container
	postgresContainer, postgresDb = startPostgresContainer(ctx)

	teardown := func() {
		_ = redisClient.Close()
		_ = postgresDb.Close()

		_ = testcontainers.TerminateContainer(redisContainer)
		_ = testcontainers.TerminateContainer(postgresContainer)
	}

	code := m.Run()

	teardown()
	os.Exit(code)
}

func TestAllCases(t *testing.T) {
	redisLockFactory, err := redis.NewRedisLockFactory(*redisClient.Options())
	if err != nil {
		t.Fatalf("Failed to create Redis lock factory: %v", err)
	}

	factoriesMap := map[string]gocky.LockFactory{
		"Redis": redisLockFactory,
		//"PostgreSQL": postgresql.NewPostgresqlLockFactory(postgresDb),
	}

	testCasesMap := map[string]func(t *testing.T, factory gocky.LockFactory){
		"Lock name":                            caseLockName,
		"Initial lock status":                  caseInitialLockStatus,
		"TryLock success":                      caseTryLockSuccess,
		"TryLock fail":                         caseTryLockFail,
		"Lock and Unlock sequence":             caseLockAndUnlockSequence,
		"Lock factory same instance":           caseLockFactorySameInstance,
		"Lock factory different instances":     caseLockFactoryDifferentInstances,
		"Lock concurrency with two goroutines": caseLockConcurrencyWithTwoGoroutines,
		"Lock concurrency with 100 goroutines": caseLockConcurrencyWith100Goroutines,
		"TryLock and Unlock in sequence":       caseTryLockAndUnlockInSequence,
	}

	for factoryName, factory := range factoriesMap {
		for testName, testCase := range testCasesMap {
			t.Run(factoryName+": "+testName, func(t *testing.T) {
				testCase(t, factory)
			})
		}
	}
}

func caseLockName(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("caseLockName", context.Background())
	assert.EqualValues(t, "caseLockName", lock.Name(), "Expected lock name 'testLock'")
}

func caseInitialLockStatus(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("caseInitialLockStatus", context.Background())
	locked, err := lock.Locked()
	assert.NoError(t, err)
	assert.False(t, locked, "Lock should be initially unlocked")
}

func caseTryLockSuccess(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("caseTryLockSuccess", context.Background())
	success, err := lock.TryLock()
	assert.NoError(t, err)
	assert.True(t, success, "TryLock should succeed")
}

func caseTryLockFail(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("caseTryLockFail", context.Background())
	success, err := lock.TryLock()
	assert.NoError(t, err)
	assert.True(t, success, "TryLock should succeed")

	success, err = lock.TryLock()
	assert.NoError(t, err)
	assert.False(t, success, "Expected TryLock to fail on already locked lock, but it succeeded")
}

func caseLockAndUnlockSequence(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("caseLockAndUnlockSequence", context.Background())
	err := lock.Lock()
	assert.NoError(t, err)

	locked, err := lock.Locked()
	assert.NoError(t, err)
	assert.True(t, locked, "Lock should be locked after calling Lock()")

	lock.Unlock()
	locked, err = lock.Locked()
	assert.NoError(t, err)
	assert.False(t, locked, "Lock should be unlocked after calling Unlock()")
}

func caseLockFactorySameInstance(t *testing.T, factory gocky.LockFactory) {
	lock1 := factory.GetLock("caseLockFactorySameInstance", context.Background())
	lock2 := factory.GetLock("caseLockFactorySameInstance", context.Background())
	assert.EqualValues(t, lock1, lock2, "Expected GetLock to return the same instance for the same lock name")
}

func caseLockFactoryDifferentInstances(t *testing.T, factory gocky.LockFactory) {
	lock1 := factory.GetLock("caseLockFactoryDifferentInstances1", context.Background())
	lock2 := factory.GetLock("caseLockFactoryDifferentInstances2", context.Background())
	assert.NotEqualValues(t, lock1, lock2, "Expected GetLock to return different instances for different lock names")
}

func caseLockConcurrencyWithTwoGoroutines(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("caseLockConcurrencyWithTwoGoroutines", context.Background())
	startLockSignal := make(chan bool)
	endLockSignal := make(chan bool)

	go func() {
		_ = lock.Lock()
		startLockSignal <- true

		<-endLockSignal
		lock.Unlock()
	}()

	<-startLockSignal

	success := make(chan bool)
	go func() {
		locked, err := lock.TryLock()
		assert.NoError(t, err)
		success <- locked
	}()

	result := <-success
	assert.False(t, result, "Expected TryLock to fail on locked lock, but it succeeded")

	endLockSignal <- true
}

func caseTryLockAndUnlockInSequence(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("caseTryLockAndUnlockInSequence", context.Background())
	success, err := lock.TryLock()

	assert.NoError(t, err)
	assert.True(t, success, "TryLock should succeed")

	locked, err := lock.Locked()
	assert.NoError(t, err)
	assert.True(t, locked, "Lock should be locked after TryLock()")

	success, err = lock.TryLock()
	assert.NoError(t, err)
	assert.False(t, success, "Expected TryLock to fail on already locked lock, but it succeeded")
}

func caseLockConcurrencyWith100Goroutines(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("caseLockConcurrencyWith100Goroutines", context.Background())
	success := make(chan bool)
	for i := 0; i < 100; i++ {
		go func() {
			locked, err := lock.TryLock()
			assert.NoError(t, err)

			success <- locked
		}()
	}

	successCounter := 0
	for i := 0; i < 100; i++ {
		result := <-success
		if result {
			successCounter++
		}
	}

	assert.Equal(t, 1, successCounter, "Expected only one TryLock to succeed")
}
