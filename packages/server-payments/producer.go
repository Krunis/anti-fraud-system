package serverpayments

import (
	"time"

	"github.com/IBM/sarama"
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

	// Для SyncProducer
	cfg.Producer.Return.Successes = true

	// Для транзакционного exactly‑once (через перезапуск)
	cfg.Producer.Transaction.ID = "fraud-txn-0001"
	cfg.Producer.Transaction.Retry.Max = 10
	cfg.Producer.Transaction.Retry.Backoff = 100 * time.Millisecond

	prod, err := sarama.NewSyncProducer(addrs, cfg)
	if err != nil{
		return nil, err
	}

	return &SyncProducer{SyncProducer: prod}, nil
}
