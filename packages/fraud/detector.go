package fraud

type AntiFraud struct{
	consumer *Consumer
}

func (a *AntiFraud) Start() error{
	if err := a.consumer.Consume(); err != nil{
		
	}
}