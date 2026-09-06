package interfaces

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgconn"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/db_mock.go -package=mocks

type Banner interface{
	BanByUserIDs(ctx context.Context, userIDs []string, duration time.Duration, txID uuid.UUID) error
}

type PostgresDB interface {
	Begin(ctx context.Context) (Tx, error)
	Query(ctx context.Context, sql string, args ...interface{}) (Rows, error)
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Close()
}

type Tx interface{
	QueryRow(ctx context.Context, sql string, args ...interface{}) Row
	Rollback(ctx context.Context) error
	Commit(ctx context.Context) error
}

type Row interface{
	Scan(dest ...interface{}) error
}

type Rows interface{
	Close()
	Err() error
	Next() bool
	Scan(dest ...interface{}) error
}

type CH interface{
	Query(ctx context.Context, query string, args ...any) (CHRows, error)
	QueryRow(ctx context.Context, query string, args ...any) CHRow
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (CHBatch, error)
	Close() error
}

type CHRows interface{
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}

type CHRow interface{
	Scan(dest ...any) error
}

type CHBatch interface{
	Append(v ...any) error
	Send() error
	Close() error
}
