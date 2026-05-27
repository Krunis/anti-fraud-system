package common

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/redis/go-redis/v9"
)

type Lifecycle struct {
	Ctx context.Context
	Cancel context.CancelFunc
}

type TransactionType struct {
	Amount   uint32 `json:"amount"`
	Currency string `json:"currency"`
	Type     string `json:"transaction_type"`
}

type PayerType struct {
	AccountID int64 `json:"account_id"`
}

type PayeeType struct {
	MerchantID   int64 `json:"merchant_id"`
	MerchantName string `json:"merchant_name"`
	Country      string `json:"country"`
}

type ContextData struct {
	Channel   string `json:"channel"`
	DeviceID  string `json:"device_id"`
	IP        string `json:"ip"`
	UserAgent string `json:"user-agent"`
}

type PaymentEvent struct {
	EventID   string `json:"event_id"`
	
	EventTime string `json:"event_time"`

	Direction string `json:"direction"`

	Transaction *TransactionType `json:"transaction"`

	Payer *PayerType `json:"payer"`

	Payee *PayeeType `json:"payee"`

	Context *ContextData `json:"context"`
}

type Redis struct {
	*redis.Client
}

type CHWriter struct {
	Conn      clickhouse.Conn
	TableName string
}


func ConnectToRedis(ctx context.Context) (*Redis, error) {
	redisPort := os.Getenv("REDIS_PORT")

	redisDB := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("redis:%s", redisPort),
		DB:   0,
	})

	if err := redisDB.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect Redis db: %s", err)
	}

	log.Println("Connected to Redis")

	return &Redis{Client: redisDB}, nil
}

func (r *Redis) CheckBan(ctx context.Context, userID string) bool{
	val, err := r.Exists(ctx, fmt.Sprintf("fraud:ban:%s", userID)).Result()

	return err == nil && val == 1
}

func NewClickHouseWriter(host string, port uint16, table, user string) (*CHWriter, error) {
	ctx := context.Background()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Username: user,
			Password: "changeme",

		},
		DialTimeout:     time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return nil, err
	}
	
	err = conn.Exec(ctx, `
        CREATE DATABASE IF NOT EXISTS fraud
    `)
    if err != nil {
        return nil, err
    }

	if err := conn.Exec(ctx, `
                    CREATE TABLE IF NOT EXISTS fraud.payments
                    (
                        event_id String,
                        event_time DateTime,
                        direction String,
                        amount UInt32,
                        currency FixedString(3) default 'USD',
                        transaction_type String default 'unknown',
                        account_id Int64 default 0,
						merchant_id Int64 default 0,
						merchant_name String,
						country FixedString(3),
						channel String,
						device_id String,
						ip String,
						user_agent Nullable(String)
						
                    )
                    ENGINE = MergeTree()
                    ORDER BY (event_time, account_id)
                    `); err != nil{
                        return nil, err
                    }

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}

	return &CHWriter{Conn: conn, TableName: table}, nil
}
