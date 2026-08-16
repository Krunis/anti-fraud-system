package interfaces

import (
	"context"

	"github.com/jackc/pgconn")

//go:generate mockgen -source=interfaces.go -destination=mocks/db_mock.go -package=mocks

type DB interface {
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

