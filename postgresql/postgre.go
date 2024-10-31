package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"github.com/alekseinovikov/gocky"
	"github.com/alekseinovikov/gocky/common"
	"time"
)

// As we relly on the client node time - we should keep in mind the time difference between the nodes
// So we use high TTL and refresh the lock every 30 seconds
var (
	defaultLockTTL          = 60 * time.Second
	defaultSpinLockDuration = 30 * time.Second
)

func initDbSchema(db *sql.DB) error {
	createTableSql := "CREATE TABLE IF NOT EXISTS locks (name VARCHAR(255) PRIMARY KEY, acquired BOOLEAN, acquired_at TIMESTAMP)"
	_, err := db.Exec(createTableSql)

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

func (p *postgresqlLockFactory) GetLock(lockName string, ctx context.Context) (gocky.Lock, error) {
	return p.lockCache.GetLock(lockName, ctx, func(ctx context.Context) (gocky.Lock, error) {
		lockCreationSql := "INSERT INTO locks (name, acquired, acquired_at) VALUES ($1, false, now()) ON CONFLICT (name) DO NOTHING"
		_, err := p.db.Exec(lockCreationSql, lockName)
		if err != nil {
			return nil, err
		}

		return &postgresqlLock{
			db:     p.db,
			ctx:    ctx,
			name:   lockName,
			ticker: common.NewTicker(defaultSpinLockDuration),
		}, nil
	})
}

type postgresqlLock struct {
	name   string
	ctx    context.Context
	db     *sql.DB
	ticker *common.Ticker
}

func (p *postgresqlLock) Name() string {
	return p.name
}

func (p *postgresqlLock) Locked() (bool, error) {
	maxTimeAcquired := time.Now().Add(-defaultLockTTL)
	row := p.db.QueryRow(
		`SELECT l.acquired FROM locks as l 
                  WHERE l.name = $1 AND l.acquired_at > $2`,
		p.name,
		maxTimeAcquired,
	)

	var acquired bool
	err := row.Scan(&acquired)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}

		return false, err
	}

	return acquired, nil
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
