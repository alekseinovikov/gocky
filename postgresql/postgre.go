package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"github.com/alekseinovikov/gocky"
	"github.com/alekseinovikov/gocky/common"
	"strconv"
	"sync"
	"time"
)

var (
	defaultLockTTL                 = 10 * time.Second
	defaultSpinLockDuration        = 5 * time.Second
	defaultLockTTLAsIntervalString = strconv.Itoa(int(defaultLockTTL.Seconds())) + " seconds"
)

func initDbSchema(db *sql.DB) error {
	createTableSql := "CREATE TABLE IF NOT EXISTS gocky_locks (name VARCHAR(255) PRIMARY KEY, acquired BOOLEAN, acquired_at TIMESTAMP)"
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
		lockCreationSql := "INSERT INTO gocky_locks (name, acquired, acquired_at) VALUES ($1, false, CURRENT_TIMESTAMP) ON CONFLICT (name) DO NOTHING"
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
	mutex  sync.Mutex
	ctx    context.Context
	db     *sql.DB
	ticker *common.Ticker
}

func (p *postgresqlLock) Name() string {
	return p.name
}

func (p *postgresqlLock) Locked() (bool, error) {
	row := p.db.QueryRow(
		`SELECT acquired FROM gocky_locks 
                  WHERE name = $1 
                    AND acquired_at > (CURRENT_TIMESTAMP - interval '`+defaultLockTTLAsIntervalString+`')`,
		p.name,
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
	p.mutex.Lock()
	defer p.mutex.Unlock()

	locked, err := p.tryToUpdatePostgresqlLock()
	if err != nil || !locked {
		return false, err
	}

	p.scheduleLockUpdater()
	return true, nil
}

func (p *postgresqlLock) Lock() error {
	locked, err := p.TryLock()
	if err != nil {
		return err
	}

	// we are trying to keep the lock
	for !locked {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		default:
			time.Sleep(defaultSpinLockDuration)
			locked, err = p.TryLock()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (p *postgresqlLock) Unlock() {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	defer p.tryToReleasePostgresqlLock()

	p.ticker.Stop()
}

func (p *postgresqlLock) scheduleLockUpdater() {
	p.ticker.Start(func() error {
		_, err := p.tryToUpdatePostgresqlLock()
		return err
	})
}

func (p *postgresqlLock) tryToUpdatePostgresqlLock() (bool, error) {
	updateSql := `UPDATE gocky_locks 
					SET acquired = true, acquired_at = CURRENT_TIMESTAMP 
             		WHERE name = $1 
             		  AND (acquired IS FALSE 
             		           OR acquired_at < CURRENT_TIMESTAMP - interval '` + defaultLockTTLAsIntervalString + `')`
	result, err := p.db.Exec(updateSql, p.name)
	if err != nil {
		return false, err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	return rowsAffected == 1, nil
}

func (p *postgresqlLock) tryToReleasePostgresqlLock() {
	updateSql := `UPDATE gocky_locks 
					SET acquired = false 
					WHERE name = $1`
	_, _ = p.db.Exec(updateSql, p.name)
}
