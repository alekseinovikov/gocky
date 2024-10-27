package tests

import (
	"context"
	"github.com/alekseinovikov/gocky"
	"github.com/alekseinovikov/gocky/redis"
	goRedis "github.com/redis/go-redis/v9"
	redisTestContainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"os"
	"testing"
)

var (
	redisClient *goRedis.Client
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	redisContainer, err := redisTestContainer.Run(ctx, "redis:latest")
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

	options := &goRedis.Options{
		Addr: host + ":" + port.Port(),
	}
	redisClient = goRedis.NewClient(options)
	redisClient.FlushDB(ctx)

	teardown := func() {
		_ = redisClient.Close()
		_ = redisContainer.Terminate(ctx)
	}

	code := m.Run()

	teardown()
	os.Exit(code)
}

func TestAllTests(t *testing.T) {
	factoriesMap := map[string]gocky.LockFactory{
		"Redis": redis.NewRedisLockFactory(*redisClient.Options()),
	}

	testCasesMap := map[string]func(t *testing.T, factory gocky.LockFactory){
		"Name":                        caseName,
		"InitialLockedState":          caseInitialLockedState,
		"TryLock":                     caseTryLock,
		"Unlock":                      caseUnlock,
		"UnlockWithTwoLocksRequested": caseUnlockWithTwoLocksRequested,
		"Lock":                        caseLock,
		"GetLock_SameName":            caseFactoryGetLockSameName,
		"GetLock_DifferentNames":      caseFactoryGetLockDifferentNames,
		"GetLock_Concurrency":         caseFactoryGetLockConcurrency,
	}

	for factoryName, factory := range factoriesMap {
		for testName, testCase := range testCasesMap {
			t.Run(factoryName+": "+testName, func(t *testing.T) {
				testCase(t, factory)
			})
		}
	}
}

func caseName(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("case", context.Background())
	if lock.Name() != "case" {
		t.Errorf("expected lock name to be 'case', got %s", lock.Name())
	}
}

func caseInitialLockedState(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("initLock", context.Background())
	if locked, _ := lock.Locked(); locked {
		t.Errorf("expected lock to be initially unlocked")
	}
}

func caseTryLock(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("tryLock", context.Background())
	if success, _ := lock.TryLock(); !success {
		t.Errorf("expected TryLock to succeed")
	}
	if locked, _ := lock.Locked(); !locked {
		t.Errorf("expected lock to be locked after TryLock")
	}
}

func caseUnlock(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("unlock", context.Background())
	_, _ = lock.TryLock()
	lock.Unlock()
	if locked, _ := lock.Locked(); locked {
		t.Errorf("expected lock to be unlocked after Unlock")
	}
}

func caseUnlockWithTwoLocksRequested(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("unlock", context.Background())
	lock2 := factory.GetLock("unlock", context.Background())
	_, _ = lock.TryLock()
	lock2.Unlock()
	if locked, _ := lock.Locked(); locked {
		t.Errorf("expected lock to be unlocked after Unlock")
	}
}

func caseLock(t *testing.T, factory gocky.LockFactory) {
	lock := factory.GetLock("lock", context.Background())
	_ = lock.Lock()
	if locked, _ := lock.Locked(); !locked {
		t.Errorf("expected lock to be locked after Lock")
	}
}

func caseFactoryGetLockSameName(t *testing.T, factory gocky.LockFactory) {
	ctx := context.Background()
	lock1 := factory.GetLock("lock1", ctx)
	lock2 := factory.GetLock("lock1", ctx)
	if lock1 != lock2 {
		t.Errorf("expected GetLock to return the same lock instance for the same name")
	}
}

func caseFactoryGetLockDifferentNames(t *testing.T, factory gocky.LockFactory) {
	ctx := context.Background()
	lock1 := factory.GetLock("lock1", ctx)
	lock2 := factory.GetLock("lock2", ctx)
	if lock1 == lock2 {
		t.Errorf("expected GetLock to return different lock instances for different names")
	}
}

func caseFactoryGetLockConcurrency(t *testing.T, factory gocky.LockFactory) {
	ctx := context.Background()
	lock1 := factory.GetLock("lock1", ctx)
	lock2 := factory.GetLock("lock1", ctx)
	lock3 := factory.GetLock("lock2", ctx)

	if lock1 == lock3 {
		t.Errorf("expected GetLock to return different lock instances for different names")
	}

	if lock1 != lock2 {
		t.Errorf("expected GetLock to return the same lock instance for the same name")
	}
}
