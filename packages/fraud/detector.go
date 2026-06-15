package fraud

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/Krunis/anti-fraud-system/packages/common"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/redis/go-redis/v9"
)

var (
	prefixFailedLogins     = "failed_logins:"
	prefixPaymentCountries = "payment_countries:"
	prefixPaymentDevices   = "payment_devices:"
	prefixPaymentAmounts   = "payment_amounts:"
)

type PaymentStats struct {
	failedLogins     int
	paymentCountries []string
	paymentDevices   []string
	paymentAmounts   []float64
}

type AntiFraud struct {
	consumer *Consumer

	redisDB *common.Redis

	postgresDB *pgxpool.Pool

	clickHouse     *common.CHWriter
	paymentCh      chan *common.PaymentEvent
	paymentChMutex sync.RWMutex

	lifecycle common.Lifecycle

	wg       sync.WaitGroup
	stopOnce sync.Once
}

func NewAntiFraud() *AntiFraud {
	ctx, cancel := context.WithCancel(context.Background())

	return &AntiFraud{
		paymentCh: make(chan *common.PaymentEvent, 1000),
		lifecycle: common.Lifecycle{
			Ctx:    ctx,
			Cancel: cancel,
		},
	}
}

func (a *AntiFraud) Start(databaseCH, tableCH, userCH, dbConnectionString string) error {
	var err error

	a.postgresDB, err = common.ConnectToDB(a.lifecycle.Ctx, dbConnectionString)
	if err != nil{
		return err
	}

	a.redisDB, err = common.ConnectToRedis(a.lifecycle.Ctx)
	if err != nil {
		return err
	}

	a.clickHouse, err = common.NewClickHouseWriter("clickhouse", 9000, tableCH, userCH)
	if err != nil {
		return err
	}

	a.wg.Go(a.startDetector)

	a.consumer, err = NewConsumer([]string{"kafka:9092"})
	if err != nil {
		return err
	}

	a.wg.Go(a.pollerToClickHouse)

	if err = a.consumer.Consume(a.lifecycle.Ctx, []string{"payment-events"}, a); err != nil {
		return err
	}

	return nil
}

func (a *AntiFraud) refreshInRedis(ctx context.Context, payment *common.PaymentEvent) error {
	var pipeline redis.Pipeliner

	pipeline = a.redisDB.Pipeline()

	pipeline.Incr(ctx, fmt.Sprintf("fraud:%s%d", prefixFailedLogins, payment.Payer.AccountID))
	pipeline.Expire(ctx, fmt.Sprintf("fraud:%s%d", prefixFailedLogins, payment.Payer.AccountID), time.Second*30)
	//ip -> country
	pipeline.SAdd(ctx, fmt.Sprintf("fraud:%s%d", prefixPaymentCountries, payment.Payer.AccountID), payment.Context.IP)
	pipeline.Expire(ctx, fmt.Sprintf("fraud:%s%d", prefixPaymentCountries, payment.Payer.AccountID), time.Minute*10)

	pipeline.SAdd(ctx, fmt.Sprintf("fraud:%s%d", prefixPaymentDevices, payment.Payer.AccountID), payment.Context.DeviceID)
	pipeline.Expire(ctx, fmt.Sprintf("fraud:%s%d", prefixPaymentDevices, payment.Payer.AccountID), time.Minute*5)

	pipeline.LPush(ctx, fmt.Sprintf("fraud:%s%d", prefixPaymentAmounts, payment.Payer.AccountID), payment.Transaction.Amount)
	pipeline.Expire(ctx, fmt.Sprintf("fraud:%s%d", prefixPaymentAmounts, payment.Payer.AccountID), time.Minute*5)

	cmds, err := pipeline.Exec(a.lifecycle.Ctx)
	if err != nil {
		return err
	}

	for _, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			log.Printf("Failed to update user stats in Redis: %s", err.Error())
		}
	}

	return nil
}

func (a *AntiFraud) startDetector(){
	timer := time.NewTimer(time.Second * 3)
	defer timer.Stop()

	for{
		select{
		case <-timer.C:
			a.Detect()
		case <-a.lifecycle.Ctx.Done():
			return 
		}
	}
}

func (a *AntiFraud) getStatsFromRedis(ctx context.Context, payment *common.PaymentEvent) (*PaymentStats, error) {
	pipeline := a.redisDB.Pipeline()

	failedLogins := pipeline.Get(ctx, fmt.Sprintf("fraud:%s%d", prefixFailedLogins, payment.Payer.AccountID))

	paymentCountries := pipeline.SMembers(ctx, fmt.Sprintf("fraud:%s%d", prefixPaymentCountries, payment.Payer.AccountID))

	paymentDevices := pipeline.SMembers(ctx, fmt.Sprintf("fraud:%s%d", prefixPaymentDevices, payment.Payer.AccountID))

	paymentAmounts := pipeline.LRange(ctx, fmt.Sprintf("fraud:%s%d", prefixPaymentAmounts, payment.Payer.AccountID), 0, -1)

	_, err := pipeline.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	failedLoginsInt, err := failedLogins.Int()
	if err != nil {
		log.Printf("Wrong type in Redis. Key: %s", fmt.Sprintf("fraud:%s%d", prefixFailedLogins, payment.Payer.AccountID))
	}

	return &PaymentStats{
		failedLogins:     failedLoginsInt,
		paymentCountries: paymentCountries.Val(),
		paymentDevices:   paymentDevices.Val(),
		paymentAmounts:   stringSliceToFloat64(paymentAmounts.Val()),
	}, nil
}

func (a *AntiFraud) banUser(ctx context.Context, userID string) error {
	return a.redisDB.Set(ctx, fmt.Sprintf("fraud:ban:%s", userID), "1", time.Minute*15).Err()
}

func stringSliceToFloat64(slice []string) []float64 {
	intSlice := []float64{}

	for _, el := range slice {
		intEl, err := strconv.Atoi(el)
		if err != nil {
			log.Printf("Wrong type in slice: %s", slice)
		}

		intSlice = append(intSlice, float64(intEl))
	}

	return intSlice
}

func (a *AntiFraud) Detect() (bool, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rows, err := a.postgresDB.Query(ctx, `
					SELECT * FROM fraud_requests
					WHERE interval_since < NOW()
					LIMIT 10`)
	if err != nil{
		return true, err
	}

	for rows.Next(){
		var row *common.DetectRequest

		if err := rows.Scan(&row); err != nil{
			log.Printf("Failed to scan row: %s", err)
		}

		a.aggrFromClickHouse(ctx, row)
	}

	
}

func (a *AntiFraud) Stop() error {
	var errs []error

	a.stopOnce.Do(func() {
		a.lifecycle.Cancel()

		if a.consumer != nil {
			if err := a.consumer.ConsumerGroup.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		if a.clickHouse != nil {
			if err := a.clickHouse.Conn.Close(); err != nil {
				errs = append(errs, err)
			}
		}

		if a.redisDB != nil {
			if err := a.redisDB.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	})

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}
