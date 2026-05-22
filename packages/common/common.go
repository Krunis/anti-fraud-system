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
	Amount   uint32
	Currency string
	Type     string
}

type PayerType struct {
	AccountID string
}

type PayeeType struct {
	MerchantID   string
	MerchantName string
	Country      string
}

type ContextData struct {
	Channel   string
	DeviceID  string
	IP        string
	UserAgent string
}

type PaymentEvent struct {
	EventID   string
	
	EventTime string

	Direction string

	Transaction *TransactionType

	Payer *PayerType

	Payee *PayeeType

	Context *ContextData
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

func NewClickHouseWriter(host string, port uint16, database, table, user string) (*CHWriter, error) {
	ctx := context.Background()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: user,
			Password: "changeme",
		},
		Debug:           true,
		DialTimeout:     time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		return nil, err
	}
	//edit db.table()
	if err := conn.Exec(ctx, `
                    CREATE TABLE IF NOT EXISTS payments.fraud
                    (
                        event_id String,
                        event_time DateTime,
                        direction String,
                        amount UInt32,
                        currency FixedString(3) default 0,
                        transaction_type String default 0,
                        account_id Int64 default 0,
						merchant_id String,
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

	return &CHWriter{conn: conn, tableName: table}, nil
}
