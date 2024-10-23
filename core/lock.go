package core

type Lock interface {
	Name() string
	TryLock() (bool, error)
	Lock() error
	Unlock()
}

type LockFactory interface {
	NewLock(lockName string) Lock
}
