package fraud

import (
	"encoding/json"
	"time"

	"github.com/IBM/sarama"
	"github.com/Krunis/anti-fraud-system/packages/common"
)

type Consumer struct{
	sarama.ConsumerGroup
}

func NewConsumer(addrs []string) (*Consumer, error){
	config := sarama.NewConfig()

	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit = struct{Enable bool; Interval time.Duration}{Enable: false}

	config.Consumer.IsolationLevel = sarama.ReadCommitted
	
	consumerGroup, err := sarama.NewConsumerGroup(addrs, "A", config)
	if err != nil{
		return nil, err
	}

	return &Consumer{ConsumerGroup: consumerGroup}, nil
}

func (a *AntiFraud) Setup(session sarama.ConsumerGroupSession) error{
	return nil
}

func (a *AntiFraud) Cleanup(session sarama.ConsumerGroupSession) error{
	return nil
}

func (a *AntiFraud) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages(){
		payment := &common.PaymentEvent{}

		json.Unmarshal(msg.Value, &payment)
		
		a.checkInRedis()

		session.MarkMessage(msg, "")

		session.Commit()

		
	}
}		