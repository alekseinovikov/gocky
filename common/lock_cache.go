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

func (l *LockCache) GetLock(
	lockName string,
	ctx context.Context,
	lockCreator func(ctx context.Context) (gocky.Lock, error),
) (gocky.Lock, error) {
	l.cacheRWLock.RLock()
	if lock, ok := l.cache[lockName]; ok {
		l.cacheRWLock.RUnlock()
		return lock, nil
	}

	l.cacheRWLock.RUnlock()

	l.cacheRWLock.Lock()
	defer l.cacheRWLock.Unlock()

	newLock, err := lockCreator(ctx)
	if err != nil {
		return nil, err
	}

	l.cache[lockName] = newLock
	return newLock, nil
}
