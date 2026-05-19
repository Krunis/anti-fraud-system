package fraud

import (
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/Krunis/anti-fraud-system/packages/common"
)

type Consumer struct {
	sarama.ConsumerGroup
}

func NewConsumer(addrs []string) (*Consumer, error) {
	config := sarama.NewConfig()

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit = struct {
		Enable   bool
		Interval time.Duration
	}{Enable: false}

	config.Consumer.IsolationLevel = sarama.ReadCommitted

	consumerGroup, err := sarama.NewConsumerGroup(addrs, "A", config)
	if err != nil {
		return nil, err
	}

	return &Consumer{ConsumerGroup: consumerGroup}, nil
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
			payment := &common.PaymentEvent{}

			json.Unmarshal(msg.Value, &payment)

			a.paymentCh <- payment

			if err := a.refreshInRedis(a.lifecycle.Ctx, payment); err != nil {
				return err
			}

			paymentStats, err := a.getStatsFromRedis(a.lifecycle.Ctx, payment)
			if err != nil {
				return err
			}

			score := considerScore(paymentStats)

			score += a.aggrFromClickHouse()

			if score > 120 {
				if err := a.banUser(a.lifecycle.Ctx, payment.Payer.AccountID); err != nil {
					return err
				}
			}

			session.MarkMessage(msg, "")

			session.Commit()

		case <-session.Context().Done():
			log.Println("Session context done, committing and exiting")

            session.Commit()
            return nil
		}
	}
}

func considerScore(paymentStats *PaymentStats) (score int) {
	score = 0

	if paymentStats.failedLogins > 5 {
		score += paymentStats.failedLogins * 10
	}
	if len(paymentStats.paymentCountries) > 3 {
		score += len(paymentStats.paymentCountries) * 15
	}
	if len(paymentStats.paymentDevices) > 3 {
		score += len(paymentStats.paymentDevices) * 15
	}

	sumAmounts := 0.0
	for _, el := range paymentStats.paymentAmounts {
		sumAmounts += el
	}

	score += (int(sumAmounts) % 1000000) * 15

	return score
}
