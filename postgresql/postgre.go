package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"github.com/alekseinovikov/gocky"
	"github.com/alekseinovikov/gocky/common"
	"strconv"
	"time"
)

var (
	defaultLockTTL          = 10 * time.Second
	defaultSpinLockDuration = 5 * time.Second
)

func initDbSchema(db *sql.DB) error {
	createTableSql := `CREATE TABLE IF NOT EXISTS gocky_locks (name VARCHAR(255) PRIMARY KEY, 
    															acquired BOOLEAN, 
    															acquired_at TIMESTAMP)`
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

func (p *postgresqlLockFactory) GetLock(
	lockName string,
	ctx context.Context,
	options ...func(config *gocky.Config),
) (gocky.Lock, error) {
	return p.lockCache.GetLock(lockName, ctx, func(ctx context.Context) (gocky.Lock, error) {
		lockCreationSql := `INSERT INTO gocky_locks (name, acquired, acquired_at) 
							VALUES ($1, false, CURRENT_TIMESTAMP) ON CONFLICT (name) DO NOTHING`
		_, err := p.db.Exec(lockCreationSql, lockName)
		if err != nil {
			return nil, err
		}

		config := &gocky.Config{
			TTL:                 defaultLockTTL,
			LockRefreshInterval: defaultSpinLockDuration,
		}

		for _, option := range options {
			option(config)
		}

		postgresLock := &postgresqlLock{
			name:   lockName,
			config: config,
			db:     p.db,
		}
		return common.NewUpdatableLock(ctx, config, postgresLock), nil
	})
}

type postgresqlLock struct {
	name   string
	config *gocky.Config
	db     *sql.DB
}

func (p *postgresqlLock) Name() string {
	return p.name
}

func (p *postgresqlLock) Locked() (bool, error) {
	lockTTL := strconv.Itoa(int(p.config.TTL.Milliseconds())) + " milliseconds"
	row := p.db.QueryRow(
		`SELECT acquired FROM gocky_locks 
                  WHERE name = $1 
                    AND acquired_at > (CURRENT_TIMESTAMP - interval '`+lockTTL+`')`,
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

func (p *postgresqlLock) ProlongLock() error {
	updateSql := `UPDATE gocky_locks 
					SET acquired = true, acquired_at = CURRENT_TIMESTAMP 
             		WHERE name = $1`
	_, err := p.db.Exec(updateSql, p.name)
	return err
}

func (p *postgresqlLock) TryToAcquireLock() (bool, error) {
	lockTTL := strconv.Itoa(int(p.config.TTL.Milliseconds())) + " milliseconds"
	updateSql := `UPDATE gocky_locks 
					SET acquired = true, acquired_at = CURRENT_TIMESTAMP 
             		WHERE name = $1 
             		  AND (acquired IS FALSE 
             		           OR acquired_at < CURRENT_TIMESTAMP - interval '` + lockTTL + `')`
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

func (p *postgresqlLock) ReleaseLock() {
	updateSql := `UPDATE gocky_locks 
					SET acquired = false 
					WHERE name = $1`
	_, _ = p.db.Exec(updateSql, p.name)
}
