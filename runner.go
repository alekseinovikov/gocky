package gocky

import "context"

type UnderLockRunner struct {
	lockFactory LockFactory
}

func NewUnderLockRunner(lockFactory LockFactory) *UnderLockRunner {
	return &UnderLockRunner{lockFactory: lockFactory}
}

func (r *UnderLockRunner) RunWaitingForLock(lockName string, ctx context.Context, f func()) error {
	lock := r.lockFactory.GetLock(lockName, ctx)
	err := lock.Lock()
	if err != nil {
		return err
	}

	defer lock.Unlock()
	f()

	return nil
}

func (r *UnderLockRunner) RunIfNotLocked(lockName string, ctx context.Context, f func()) error {
	lock := r.lockFactory.GetLock(lockName, ctx)
	ok, err := lock.TryLock()
	if err != nil {
		return err
	}

	if !ok {
		return nil
	}

	defer lock.Unlock()
	f()

	return nil
}
