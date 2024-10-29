package postgresql

import (
	"context"
	"database/sql"
	"github.com/alekseinovikov/gocky"
	"github.com/alekseinovikov/gocky/common"
)

type postgresqlLockFactory struct {
	db        *sql.DB
	lockCache common.LockCache
}

func NewPostgresqlLockFactory(db *sql.DB) gocky.LockFactory {
	return &postgresqlLockFactory{
		db:        db,
		lockCache: common.NewLockCache(),
	}
}

func (p postgresqlLockFactory) GetLock(lockName string, ctx context.Context) gocky.Lock {
	//TODO implement me
	panic("implement me")
}
