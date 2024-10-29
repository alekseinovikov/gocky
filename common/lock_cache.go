package common

import (
	"context"
	"github.com/alekseinovikov/gocky"
	"sync"
)

type LockCache struct {
	cache       map[string]gocky.Lock
	cacheRWLock sync.RWMutex
}

func NewLockCache() LockCache {
	return LockCache{cache: make(map[string]gocky.Lock)}
}

func (l *LockCache) GetLock(lockName string, ctx context.Context, lockCreator func(ctx context.Context) gocky.Lock) gocky.Lock {
	l.cacheRWLock.RLock()
	if lock, ok := l.cache[lockName]; ok {
		l.cacheRWLock.RUnlock()
		return lock
	}

	l.cacheRWLock.RUnlock()

	l.cacheRWLock.Lock()
	defer l.cacheRWLock.Unlock()

	newLock := lockCreator(ctx)
	l.cache[lockName] = newLock
	return newLock
}
