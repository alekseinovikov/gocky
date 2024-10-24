package gocky

import "context"

// Lock abstraction for locking, unlocking and checking lock status
type Lock interface {
	Name() string
	Locked() (bool, error)
	TryLock() (bool, error)
	Lock() error
	Unlock()
}

// LockFactory is a factory for creating locks
// It is used for getting locks
// It's not guaranteed that every time new lock will be created
// Some implementations may cache locks with the same names
type LockFactory interface {

	// GetLock returns a lock with the given name, may return a cached lock
	GetLock(lockName string, ctx context.Context) Lock
}
