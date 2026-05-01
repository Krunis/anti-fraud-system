package fraud

import (
	"time"

	"github.com/IBM/sarama"
)

type Consumer struct{
	sarama.ConsumerGroup
	
}

func NewConsumer(addrs []string) (*Consumer, error){
	config := sarama.NewConfig()

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit = struct{Enable bool; Interval time.Duration}{Enable: false}

	consumerGroup, err := sarama.NewConsumerGroup(addrs, "A", config)
	if err != nil{
		return nil, err
	}

	return &Consumer{ConsumerGroup: consumerGroup}, nil
}