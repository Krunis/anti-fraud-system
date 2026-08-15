package interfaces

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

//go:generate mockgen -source=interfaces.go -destination=mocks/db_mock.go -package=mocks

type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...interface{}) (Rows, error)
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Close()
}

type Rows interface{
	Close()
	Err() error
	Next() bool
	Scan(dest ...interface{}) error
}

