package fraud

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/Krunis/anti-fraud-system/packages/common"
	"github.com/redis/go-redis/v9"
)

var (
	prefixFailedLogins = "failed_logins:"
	prefixPaymentCountries = "payment_countries:"
	prefixPaymentDevices = "payment_devices:"
	prefixPaymentAmounts = "payment_amounts:"
)

type PaymentStats struct{
	failedLogins int
	paymentCountries []string
	paymentDevices []string
	paymentAmounts []float64
}

type AntiFraud struct {
	consumer *Consumer

	redisDB *common.Redis

	clickHouse *common.CHWriter
	paymentCh chan *common.PaymentEvent

	lifecycle common.Lifecycle
}

func NewAntiFraud() *AntiFraud{
	ctx, cancel := context.WithCancel(context.Background())

	return &AntiFraud{
		paymentCh: make(chan *common.PaymentEvent, 1000),
		lifecycle: common.Lifecycle{
			Ctx: ctx,
			Cancel: cancel,
		},
	}
}

func (a *AntiFraud) Start(databaseCH, tableCH, userCH string) error {
	var err error

	a.redisDB, err = common.ConnectToRedis(a.lifecycle.Ctx)
	if err != nil{
		return err
	}

	a.clickHouse, err = common.NewClickhouseWriter("localhost", 19000, databaseCH, tableCH, userCH)
	if err != nil{
		return err
	}

	if err = a.consumer.Consume(a.lifecycle.Ctx, []string{"payment-events"}, a); err != nil {
		log.Printf("Error while consuming: %s", err)
	}

	return nil
}

func (a *AntiFraud) refreshInRedis(ctx context.Context, payment *common.PaymentEvent) error {
	var pipeline redis.Pipeliner

	pipeline = a.redisDB.Pipeline()

	pipeline.Incr(ctx, fmt.Sprintf("fraud:%s%s", prefixFailedLogins, payment.Payer.AccountID))
	pipeline.Expire(ctx, fmt.Sprintf("fraud:%s%s", prefixFailedLogins, payment.Payer.AccountID), time.Second * 30)
	//ip -> country
	pipeline.SAdd(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentCountries, payment.Payer.AccountID), payment.Context.IP)
	pipeline.Expire(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentCountries, payment.Payer.AccountID), time.Minute * 10)

	pipeline.SAdd(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentDevices, payment.Payer.AccountID), payment.Context.DeviceID)
	pipeline.Expire(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentDevices, payment.Payer.AccountID), time.Minute * 5)

	pipeline.LPush(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentAmounts, payment.Payer.AccountID), payment.Transaction.Amount)
	pipeline.Expire(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentAmounts, payment.Payer.AccountID), time.Minute * 5)
	
	cmds, err := pipeline.Exec(a.lifecycle.Ctx)
	if err != nil{
		return err
	}

	for _, cmd := range cmds{
		err := cmd.Err()
		log.Printf("Failed to update user stats in Redis: %s", err)
	}

	return nil
}

func (a *AntiFraud) getStatsFromRedis(ctx context.Context, payment *common.PaymentEvent) (*PaymentStats, error){
	pipeline := a.redisDB.Pipeline()

	failedLogins := pipeline.Get(ctx, fmt.Sprintf("fraud:%s%s", prefixFailedLogins, payment.Payer.AccountID))

	paymentCountries := pipeline.SMembers(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentCountries, payment.Payer.AccountID))

	paymentDevices := pipeline.SMembers(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentDevices, payment.Payer.AccountID))

	paymentAmounts := pipeline.LRange(ctx, fmt.Sprintf("fraud:%s%s", prefixPaymentAmounts, payment.Payer.AccountID), 0, -1)

	_, err := pipeline.Exec(ctx)
	if err != nil && err != redis.Nil{
		return nil, err
	}

	failedLoginsInt, err := failedLogins.Int()
	if err != nil{
		log.Printf("Wrong type in Redis. Key: %s", fmt.Sprintf("fraud:%s%s", prefixFailedLogins, payment.Payer.AccountID))
	}

	return &PaymentStats{
		failedLogins: failedLoginsInt,
		paymentCountries: paymentCountries.Val(),
		paymentDevices: paymentDevices.Val(),
		paymentAmounts: stringSliceToFloat64(paymentAmounts.Val()),
	}, nil
}

func (a *AntiFraud) banUser(ctx context.Context, userID string) error{
	return a.redisDB.Set(ctx, fmt.Sprintf("fraud:ban:%s", userID), "1", time.Minute * 15).Err()
}

func stringSliceToFloat64(slice []string) []float64{
	intSlice := []float64{}

	for _, el := range slice{
		intEl, err := strconv.Atoi(el)
		if err != nil{
			log.Printf("Wrong type in slice: %s", slice)
		}

		intSlice = append(intSlice, float64(intEl))
	}

	return intSlice
}



func (a *AntiFraud) Detect() (bool, error) {

}