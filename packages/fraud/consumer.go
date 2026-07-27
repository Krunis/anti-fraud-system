package fraud

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/Krunis/anti-fraud-system/packages/common"
)

type Consumer struct {
	sarama.ConsumerGroup
}

func NewConsumer(ctx context.Context, addrs []string) (*Consumer, error) {
	config := sarama.NewConfig()

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit = struct {
		Enable   bool
		Interval time.Duration
	}{Enable: false, Interval: 1}

	config.Consumer.IsolationLevel = sarama.ReadCommitted

	var err error

	timer := time.NewTimer(time.Second * 26)
	defer timer.Stop()

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			consumerGroup, err := sarama.NewConsumerGroup(addrs, "A", config)
			if err == nil {
				return &Consumer{ConsumerGroup: consumerGroup}, nil
			}

			log.Printf("Failed to connect to Kafka: %s. Retrying...\n", err)

		case <-timer.C:
			return nil, fmt.Errorf("db connection timeout (25s): %v", err)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (a *AntiFraud) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (a *AntiFraud) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (a *AntiFraud) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			var redisScores int

			payment := &common.PaymentEvent{}

			json.Unmarshal(msg.Value, &payment)

			a.paymentCh <- payment

			if err := a.refreshInRedis(session.Context(), payment); err != nil {
				return err
			}

			paymentStats, err := a.getStatsFromRedis(session.Context(), payment)
			if err != nil {
				return err
			}

			considerScores(paymentStats, &redisScores)

			if redisScores > 120 {
				if err := a.banUsers(session.Context(), []string{payment.Payer.AccountID}); err != nil {
					return err
				}
			}

			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			log.Println("Session context done, exiting")

			return nil
		}
	}
}

func considerScores(paymentStats *PaymentStats, redisScores *int) {
	if paymentStats.failedLogins > 5 {
		*redisScores += paymentStats.failedLogins * 10
	}
	if len(paymentStats.paymentCountries) > 3 {
		*redisScores += len(paymentStats.paymentCountries) * 15
	}
	if len(paymentStats.paymentDevices) > 3 {
		*redisScores += len(paymentStats.paymentDevices) * 15
	}

	sumAmounts := 0.0
	for _, el := range paymentStats.paymentAmounts {
		sumAmounts += el
	}

	*redisScores += (int(sumAmounts) % 1000000) * 15


}
