package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	serverpayments "github.com/Krunis/anti-fraud-system/packages/server-payments"
)

func main() {
	serv := serverpayments.NewServerPayments(":8082")

	errCh := make(chan error, 1)

	go func() {
		if err := serv.Start(); err != nil {
			errCh <- err
		}
	}()

	signCh := make(chan os.Signal, 1)
	signal.Notify(signCh, syscall.SIGTERM, os.Interrupt)
	defer signal.Stop(signCh)

	select{
	case <-signCh:
		log.Println("Signal received")
		
		stop(serv)
	case err := <-errCh:
		log.Printf("Error while working: %s", err)

		stop(serv)
	}
}

func stop(serv *serverpayments.ServerPayments){
	if err := serv.Stop(); err != nil{
		log.Printf("Error while stopping: %s", err)
	}
}