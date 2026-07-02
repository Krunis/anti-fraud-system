package serverpayments

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/Krunis/anti-fraud-system/packages/common"
)

type SyncProducer struct {
	sarama.SyncProducer
}

func NewSyncProducer(ctx context.Context, addrs []string) (*SyncProducer, error) {
	cfg := sarama.NewConfig()

	cfg.Producer.Idempotent = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Retry.Max = 30
	cfg.Producer.Retry.Backoff = 100 * time.Millisecond

	cfg.Producer.Return.Successes = true

	cfg.Producer.Transaction.ID = "fraud-txn-0001"
	cfg.Producer.Transaction.Retry.Max = 10
	cfg.Producer.Transaction.Retry.Backoff = 100 * time.Millisecond

	var err error

	timer := time.NewTimer(time.Second * 26)
	defer timer.Stop()

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			prod, err := sarama.NewSyncProducer(addrs, cfg)
			if err == nil {
				return &SyncProducer{SyncProducer: prod}, nil
			}

			log.Printf("Failed to connect to Kafka: %s. Retrying...\n", err)

		case <-timer.C:
			return nil, fmt.Errorf("db connection timeout (25s): %v", err)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *ServerPayments) ProduceToKafka(topic string, payments []*common.PaymentEvent) error {
	if s.syncProducer == nil {
		return fmt.Errorf("syncProducer is nil")
	}

	if len(payments) == 0 {
		return nil
	}

	if err := s.syncProducer.BeginTxn(); err != nil {
		return err
	}

	commited := false

	defer func() {
		if !commited {
			if err := s.syncProducer.AbortTxn(); err != nil {
				log.Printf("abort Kafka transaction failed: %s", err)
			}
		}
	}()

	msgs := make([]*sarama.ProducerMessage, len(payments))

	for _, payment := range payments {
		value, err := json.Marshal(payment)
		if err != nil {
			return err
		}

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(payment.EventID),
			Value: sarama.ByteEncoder(value),
		}

		msgs = append(msgs, msg)
	}

	if err := s.syncProducer.SendMessages(msgs); err != nil {
		return err
	}

	if err := s.syncProducer.CommitTxn(); err != nil {
		return err
	}

	commited = true

	return nil
}
