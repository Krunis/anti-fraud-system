package serverpayments

import (
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/Krunis/anti-fraud-system/packages/common"
)

type SyncProducer struct {
	sarama.SyncProducer
}

func NewSyncProducer(addrs []string) (*SyncProducer, error) {
	cfg := sarama.NewConfig()

	cfg.Producer.Idempotent = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Retry.Max = 30
	cfg.Producer.Retry.Backoff = 10 * time.Millisecond

	cfg.Producer.Return.Successes = true

	cfg.Producer.Transaction.ID = "fraud-txn-0001"
	cfg.Producer.Transaction.Retry.Max = 10
	cfg.Producer.Transaction.Retry.Backoff = 100 * time.Millisecond

	prod, err := sarama.NewSyncProducer(addrs, cfg)
	if err != nil {
		return nil, err
	}

	return &SyncProducer{SyncProducer: prod}, nil
}

func (s *ServerPayments) ProduceToKafka(topic string, payments []*common.PaymentEvent) error {
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
