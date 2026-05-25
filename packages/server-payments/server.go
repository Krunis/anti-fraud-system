package serverpayments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/Krunis/anti-fraud-system/packages/common"
)

type ServerPayments struct {
	lis     net.Listener
	address string

	httpServer *http.Server
	mux        *http.ServeMux

	redisDB *common.Redis

	syncProducer *SyncProducer

	lifecycle *common.Lifecycle

	stopOnce sync.Once
}

func NewServerPayments(address string) *ServerPayments {
	ctx, cancel := context.WithCancel(context.Background())

	return &ServerPayments{
		address: address,
		mux: http.NewServeMux(),
		lifecycle: &common.Lifecycle{
			Ctx:    ctx,
			Cancel: cancel,
		},
	}
}

func (s *ServerPayments) Start() error {
	var err error

	s.redisDB, err = common.ConnectToRedis(s.lifecycle.Ctx)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	s.httpServer = &http.Server{}
	s.mux.HandleFunc("/payment/add", s.paymentHandler)

	s.httpServer.Handler = s.mux

	if err := s.httpServer.Serve(lis); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}

func (s *ServerPayments) paymentHandler(w http.ResponseWriter, r *http.Request) {
	select {
	case <-s.lifecycle.Ctx.Done():
		log.Println("Request cancelled (shutdown or client disconnected)")
		return
	default:
		if r.Method != "POST" {
			http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
			return
		}

		payment := &common.PaymentEvent{}

		err := json.NewDecoder(r.Body).Decode(&payment)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		if err := ValidatePaymentEvent(payment); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if s.redisDB.CheckBan(r.Context(), payment.Payer.AccountID) {
			http.Error(w, "banned", http.StatusForbidden)
			return
		}

		if err := s.ProduceToKafka("payment-events", payment); err != nil {
			http.Error(w, "server unavailable", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func (s *ServerPayments) Stop() error {
	var errs []error

	s.stopOnce.Do(func() {
		s.lifecycle.Cancel()

		if s.httpServer != nil {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second*15)
			defer cancel()

			log.Println("Graceful shutdown...")
			if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
				err = fmt.Errorf("graceful shutdown failed: %s\n", err)
				errs = append(errs, err)

				if err = s.httpServer.Close(); err != nil {
					log.Printf("Force close failed: %s\n", err)
					err = fmt.Errorf("shutdown failed: %v, close failed: %v", err, err)
					errs = append(errs, err)
				}
				err = fmt.Errorf("shutdown failed: %v, forced close", err)
			}
		}

		if s.redisDB != nil {
			if err := s.redisDB.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	})

	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	return nil
}
