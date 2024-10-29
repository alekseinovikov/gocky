package postgresql

import (
	"context"
	"database/sql"
	"github.com/alekseinovikov/gocky"
	"github.com/alekseinovikov/gocky/common"
)

func initDbSchema(db *sql.DB) error {
	sql := "CREATE TABLE IF NOT EXISTS locks (name VARCHAR(255) PRIMARY KEY, acquired BOOLEAN, acquired_at TIMESTAMP)"
	_, err := db.Exec(sql)
	return err
}

type postgresqlLockFactory struct {
	db        *sql.DB
	lockCache common.LockCache
}

func NewPostgresqlLockFactory(db *sql.DB) (gocky.LockFactory, error) {
	err := initDbSchema(db)
	if err != nil {
		return nil, err
	}

	return &postgresqlLockFactory{
		db:        db,
		lockCache: common.NewLockCache(),
	}, nil
}

func (p *postgresqlLockFactory) GetLock(lockName string, ctx context.Context) gocky.Lock {
	return p.lockCache.GetLock(lockName, ctx, func(ctx context.Context) gocky.Lock {
		return &postgresqlLock{
			db:   p.db,
			ctx:  ctx,
			name: lockName,
		}
	})
}

type postgresqlLock struct {
	name string
	ctx  context.Context
	db   *sql.DB
}

func (p *postgresqlLock) Name() string {
	//TODO implement me
	panic("implement me")
}

func (p *postgresqlLock) Locked() (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (p *postgresqlLock) TryLock() (bool, error) {
	//TODO implement me
	panic("implement me")
}

func (p *postgresqlLock) Lock() error {
	//TODO implement me
	panic("implement me")
}

func (p *postgresqlLock) Unlock() {
	//TODO implement me
	panic("implement me")
}
