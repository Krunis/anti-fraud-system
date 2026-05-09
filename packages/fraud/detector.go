package fraud

import (
	"fmt"
	"time"

	"github.com/Krunis/anti-fraud-system/packages/common"
	"github.com/redis/go-redis/v9"
)

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
	_, err := a.redisDB.Incr(a.lifecycle.Ctx, fmt.Sprintf("fraud:failed_logins:%s", payment.Payer.AccountID)).Result()
	if err != nil{
		return err
	}
	//ip -> country
	_, err = a.redisDB.SAdd(a.lifecycle.Ctx, fmt.Sprintf("fraud:payment_countries:%s", payment.Payer.AccountID), payment.Context.IP).Result()
	if err != nil{
		return err
	}

	_, err = a.redisDB.SAdd(a.lifecycle.Ctx, fmt.Sprintf("fraud:payment_devices:%s", payment.Payer.AccountID), payment.Context.DeviceID).Result()
	if err != nil{
		return err
	}

	pipeline := a.redisDB.Pipeline()

	pipeline.LPush(a.lifecycle.Ctx, fmt.Sprintf("fraud:payment_amounts:%s", payment.Payer.AccountID), payment.Transaction.Amount)
	pipeline.Expire(a.lifecycle.Ctx, fmt.Sprintf("fraud:payment_amounts:%s", payment.Payer.AccountID), time.Minute * 5)
	//???? err
	cmds, err := pipeline.Exec(a.lifecycle.Ctx)
	if err != nil{
		return err
	}

	for _, cmd := range cmds{
		err := cmd.Err()
		return err
	}

func (a *AntiFraud) Detect() (bool, error) {

}