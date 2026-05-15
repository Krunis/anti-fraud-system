package common

import (
	"context"
	"fmt"
	"log"
	"os"

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
