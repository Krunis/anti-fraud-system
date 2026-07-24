package fraud

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/byteonabeach/ip2country"
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

	a.consumer, err = NewConsumer(a.lifecycle.Ctx, []string{"kafka:9092"})
	if err != nil {
		return err
	}

	a.wg.Go(a.pollerToClickHouse)

	a.wg.Go(a.startDetector)

	if err = a.consumer.Consume(a.lifecycle.Ctx, []string{"payment-events"}, a); err != nil {
		return err
	}

	return nil
}

func (a *AntiFraud) refreshInRedis(ctx context.Context, payment *common.PaymentEvent) error {
	pipeline := a.redisDB.Pipeline()

	pipeline.Incr(ctx, fmt.Sprintf("fraud:%s%s", prefixFailedLogins, payment.Payer.AccountID))
	pipeline.Expire(ctx, fmt.Sprintf("fraud:%s%s", prefixFailedLogins, payment.Payer.AccountID), time.Second*30)
	
	ipDB := ip2country.NewIPCountryDB("../../countries.csv")
	country, err := ipDB.GetCountry(payment.Context.IP)
	if err != nil{
		log.Printf("Failed to look country from IP: %s", err)
		country = payment.Context.IP
	}

	pipeline.SAdd(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentCountries, payment.Payer.AccountID), country)
	pipeline.Expire(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentCountries, payment.Payer.AccountID), time.Minute*10)

	pipeline.SAdd(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentDevices, payment.Payer.AccountID), payment.Context.DeviceID)
	pipeline.Expire(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentDevices, payment.Payer.AccountID), time.Minute*5)

	pipeline.LPush(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentAmounts, payment.Payer.AccountID), payment.Transaction.Amount)
	pipeline.Expire(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentAmounts, payment.Payer.AccountID), time.Minute*5)

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

func (a *AntiFraud) getStatsFromRedis(ctx context.Context, payment *common.PaymentEvent) (*PaymentStats, error) {
	pipeline := a.redisDB.Pipeline()

	failedLogins := pipeline.Get(ctx, fmt.Sprintf("fraud:%s%s", prefixFailedLogins, payment.Payer.AccountID))

	paymentCountries := pipeline.SMembers(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentCountries, payment.Payer.AccountID))

	paymentDevices := pipeline.SMembers(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentDevices, payment.Payer.AccountID))

	paymentAmounts := pipeline.LRange(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentAmounts, payment.Payer.AccountID), 0, -1)

	_, err := pipeline.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}

	failedLoginsInt, err := failedLogins.Int()
	if err != nil {
		log.Printf("Wrong type in Redis. Key: %s", fmt.Sprintf("fraud:%s%s", prefixFailedLogins, payment.Payer.AccountID))
	}

	return &PaymentStats{
		failedLogins:     failedLoginsInt,
		paymentCountries: paymentCountries.Val(),
		paymentDevices:   paymentDevices.Val(),
		paymentAmounts:   stringSliceToFloat64(paymentAmounts.Val()),
	}, nil
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

func (a *AntiFraud) Stop() error {
	var errs []error

	a.stopOnce.Do(func() {
		a.lifecycle.Cancel()

		a.wg.Wait()

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
