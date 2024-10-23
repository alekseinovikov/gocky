package core

type UnderLockRunner struct {
	lockFactory LockFactory
}

func NewUnderLockRunner(lockFactory LockFactory) *UnderLockRunner {
	return &UnderLockRunner{lockFactory: lockFactory}
}

func (r *UnderLockRunner) RunWaitingForLock(lockName string, f func()) error {
	lock := r.lockFactory.GetLock(lockName)
	err := lock.Lock()
	if err != nil {
		return err
	}

	defer lock.Unlock()
	f()

	return nil
}

func (r *UnderLockRunner) RunIfNotLocked(lockName string, f func()) error {
	lock := r.lockFactory.GetLock(lockName)
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
