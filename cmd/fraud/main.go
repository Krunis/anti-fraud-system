package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/Krunis/anti-fraud-system/packages/fraud"
)

func main() {
	fr := fraud.NewAntiFraud()

	errCh := make(chan error, 1)

	go func ()  {
		if err := fr.Start("fraud", "payments", "changeme"); err != nil{
			errCh <- err
	}
	}()
	

	signCh := make(chan os.Signal, 1)
	signal.Notify(signCh, syscall.SIGTERM, os.Interrupt)
	defer signal.Stop(signCh)

	select{
	case <-signCh:
		log.Println("Signal received")
	case err := <-errCh:
		log.Printf("Error while working: %s", err)
	}
}

func stop(fr *fraud.AntiFraud) {
	if err := fr.Stop(); err != nil{
		log.Printf("Error while stopping: %s", err)
	}
}