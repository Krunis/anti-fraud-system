package fraud

import (
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

	redisDB *redis.Client

	lifecycle common.Lifecycle
}

func (a *AntiFraud) Start() error {
	var err error

	a.redisDB, err = common.ConnectToRedis(a.lifecycle.Ctx)
	if err != nil{
		return err
	}

	if err = a.consumer.Consume(a.lifecycle.Ctx, []string{"payment-events"}, a); err != nil {

	}
}

func (a *AntiFraud) refreshInRedis(payment *common.PaymentEvent) error {
	var pipeline redis.Pipeliner

	pipeline = a.redisDB.Pipeline()

	pipeline.Incr(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixFailedLogins))
	pipeline.Expire(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixFailedLogins), time.Second * 30)
	//ip -> country
	pipeline.SAdd(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixPaymentCountries), payment.Context.IP)
	pipeline.Expire(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixPaymentCountries), time.Minute * 10)

	pipeline.SAdd(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixPaymentDevices), payment.Context.DeviceID)
	pipeline.Expire(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixPaymentDevices), time.Minute * 5)

	pipeline.LPush(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixPaymentAmounts), payment.Transaction.Amount)
	pipeline.Expire(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixPaymentAmounts), time.Minute * 5)
	
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

func (a *AntiFraud) getStatsFromRedis(payment *common.PaymentEvent) (*PaymentStats, error){
	pipeline := a.redisDB.Pipeline()

	failedLogins := pipeline.Get(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixFailedLogins))

	paymentCountries := pipeline.SMembers(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixPaymentCountries))

	paymentDevices := pipeline.SMembers(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixPaymentDevices))

	paymentAmounts := pipeline.LRange(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixPaymentAmounts), 0, -1)

	_, err := pipeline.Exec(a.lifecycle.Ctx)
	if err != nil && err != redis.Nil{
		return nil, err
	}

	failedLoginsInt, err := failedLogins.Int()
	if err != nil{
		log.Printf("Wrong type in Redis. Key: %s", fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixFailedLogins))
	}

	return &PaymentStats{
		failedLogins: failedLoginsInt,
		paymentCountries: paymentCountries.Val(),
		paymentDevices: paymentDevices.Val(),
		paymentAmounts: stringSliceToFloat64(paymentAmounts.Val()),
	}, nil
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