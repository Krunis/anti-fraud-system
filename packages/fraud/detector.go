package fraud

import (
	"fmt"
	"log"
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

type paymentStats struct{
	failedLogins int
	paymentCountries []string
	paymentDevices []string
	paymentAmounts []string
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

func (a *AntiFraud) checkFromRedis(payment *common.PaymentEvent) (*paymentStats, error){
	pipeline := a.redisDB.Pipeline()

	failedLogins, err := pipeline.Get(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixFailedLogins)).Int()
	if err != nil{
		log.Printf("Wrong type in Redis. Key: %s", fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixFailedLogins))
	}

	paymentCountries := pipeline.SMembers(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixPaymentCountries)).Val()

	paymentDevices := pipeline.SMembers(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixPaymentDevices)).Val()

	paymentAmounts := pipeline.LRange(a.lifecycle.Ctx, fmt.Sprintf("fraud:%s%s", payment.Payer.AccountID, prefixPaymentAmounts), 0, -1).Val()

	_, err = pipeline.Exec(a.lifecycle.Ctx)
	if err != nil && err != redis.Nil{
		return nil, err
	}

	return &paymentStats{
		failedLogins: failedLogins,
		paymentCountries: paymentCountries,
		paymentDevices: paymentDevices,
		paymentAmounts: paymentAmounts,
	}, nil
}


func (a *AntiFraud) Detect() (bool, error) {

}