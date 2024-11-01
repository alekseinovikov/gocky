package common

import (
	"context"
	"github.com/alekseinovikov/gocky"
	"sync"
	"time"
)

type TTLLock interface {
	Name() string
	ProlongLock() error
	TryToAcquireLock() (bool, error)
	ReleaseLock()
	Locked() (bool, error)
}

type UpdatableLock struct {
	ctx     context.Context
	config  *gocky.Config
	mutex   sync.Mutex
	ticker  *Ticker
	ttlLock TTLLock
}

func NewUpdatableLock(
	ctx context.Context,
	config *gocky.Config,
	ttlLock TTLLock,
) *UpdatableLock {
	return &UpdatableLock{
		ctx:     ctx,
		config:  config,
		ttlLock: ttlLock,
		ticker:  NewTicker(config.LockRefreshInterval),
	}

}

func (r *UpdatableLock) Name() string {
	return r.ttlLock.Name()
}

func (r *UpdatableLock) Locked() (bool, error) {
	return r.ttlLock.Locked()
}

func (r *UpdatableLock) Unlock() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	defer r.ttlLock.ReleaseLock()

	r.ticker.Stop()
}

func (r *UpdatableLock) Lock() error {
	locked, err := r.TryLock()
	if err != nil {
		return err
	}

	for !locked {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
			time.Sleep(r.config.LockRefreshInterval)
			locked, err = r.TryLock()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *UpdatableLock) TryLock() (bool, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	locked, err := r.ttlLock.TryToAcquireLock()
	if err != nil || !locked {
		return false, err
	}

	r.scheduleLockUpdater()
	return true, nil
}

func (r *UpdatableLock) scheduleLockUpdater() {
	r.ticker.Start(func() error {
		return r.ttlLock.ProlongLock()
	})
}
