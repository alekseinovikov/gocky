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
	"time"
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
	lock := factory.GetLock("testLock", context.Background())
	if lock.Name() != "testLock" {
		t.Errorf("Expected lock name 'testLock', but got %s", lock.Name())
	}
}

func caseInitialLockStatus(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("testLock", context.Background())
	locked, err := lock.Locked()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if locked {
		t.Errorf("Lock should be initially unlocked")
	}
}

func caseTryLockSuccess(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("tryLockSuccess", context.Background())
	success, err := lock.TryLock()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !success {
		t.Errorf("Expected TryLock to succeed, but it failed")
	}
}

func caseTryLockFail(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("tryLockFail", context.Background())
	_, _ = lock.TryLock()
	success, err := lock.TryLock()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if success {
		t.Errorf("Expected TryLock to fail on already locked lock, but it succeeded")
	}

	lock.Unlock()
}

func caseLockAndUnlockSequence(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("lockAndUnlockSequence", context.Background())
	err := lock.Lock()
	if err != nil {
		t.Errorf("Unexpected error during Lock(): %v", err)
	}

	locked, err := lock.Locked()
	if err != nil {
		t.Errorf("Unexpected error during Locked(): %v", err)
	}
	if !locked {
		t.Errorf("Lock should be locked after calling Lock()")
	}

	lock.Unlock()
	locked, err = lock.Locked()
	if err != nil {
		t.Errorf("Unexpected error during Locked() after Unlock(): %v", err)
	}
	if locked {
		t.Errorf("Lock should be unlocked after calling Unlock()")
	}
}

func caseLockFactorySameInstance(t *testing.T, factory gocky.LockFactory) {
	lock1 := factory.GetLock("sameInstance", context.Background())
	lock2 := factory.GetLock("sameInstance", context.Background())
	if lock1 != lock2 {
		t.Errorf("Expected GetLock to return the same instance for the same lock name, but got different instances")
	}
}

func caseLockFactoryDifferentInstances(t *testing.T, factory gocky.LockFactory) {
	lock1 := factory.GetLock("differentInstance1", context.Background())
	lock2 := factory.GetLock("differentInstance2", context.Background())
	if lock1 == lock2 {
		t.Errorf("Expected GetLock to return different instances for different lock names, but got the same instance")
	}
}

func caseLockConcurrencyWithTwoGoroutines(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("concurrentLock", context.Background())
	success := make(chan bool)

	go func() {
		_ = lock.Lock()
		time.Sleep(50 * time.Millisecond)
		lock.Unlock()
	}()

	time.Sleep(10 * time.Millisecond)

	go func() {
		locked, err := lock.TryLock()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		success <- locked
	}()

	result := <-success

	if result {
		t.Errorf("Expected TryLock to fail on locked lock, but it succeeded")
	}
}

func caseTryLockAndUnlockInSequence(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("tryLockAndUnlockInSequence", context.Background())
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
	started := make(chan bool)
	success := make(chan bool)
	for i := 0; i < 100; i++ {
		go func() {
			started <- true
			lock := factory.GetLock("concurrentLock100", context.Background())
			locked, err := lock.TryLock()
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			success <- locked

			if locked {
				lock.Unlock()
			}
		}()
	}

	// make sure all goroutines started
	for i := 0; i < 100; i++ {
		<-started
	}

	successCounter := 0
	for i := 0; i < 100; i++ {
		result := <-success
		if result {
			successCounter++
		}
	}

	if successCounter > 1 {
		t.Errorf("Expected only one TryLock to succeed, but got %d successes", successCounter)
	}
}
