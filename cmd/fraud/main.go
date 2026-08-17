package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Krunis/anti-fraud-system/packages/common"
	"github.com/Krunis/anti-fraud-system/packages/fraud"
)

func main() {
var err error

	postgresDB, err := common.ConnectToPostgres(context.Background(), common.GetDBConnectionString())
	if err != nil{
		log.Fatalf("error while starting: %s", err)
	}

	redisDB, err := common.ConnectToRedis(context.Background())
	if err != nil {
		log.Fatalf("error while starting: %s", err)
	}

	conn, err := common.NewClickHouseConn("clickhouse", 9000, "payments", "default")
	if err != nil {
		log.Fatalf("error while starting: %s", err)
	}

	consumer, err := fraud.NewConsumer(context.Background(), []string{"kafka:9092"})
	if err != nil {
		log.Fatalf("error while starting: %s", err)
	}

	db := common.NewDB(postgresDB)

	ch := common.NewCH(conn)

	fr := fraud.NewAntiFraud(db, ch, redisDB, consumer)

	errCh := make(chan error, 1)

	go func ()  {
		if err := fr.Start("fraud", "payments", "default", common.GetDBConnectionString()); err != nil{
			errCh <- err
	}
	}()
	
	signCh := make(chan os.Signal, 1)
	signal.Notify(signCh, syscall.SIGTERM, os.Interrupt)
	defer signal.Stop(signCh)

	select{
	case <-signCh:
		log.Println("Signal received")

		stop(fr)
	case err := <-errCh:
		log.Printf("Error while working: %s", err)
		
		stop(fr)
	}
}

func stop(fr *fraud.AntiFraud) {
	if err := fr.Stop(); err != nil{
		log.Printf("Error while stopping: %s", err)
	}
}