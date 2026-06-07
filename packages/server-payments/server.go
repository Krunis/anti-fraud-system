package serverpayments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/Krunis/anti-fraud-system/packages/common"
	"github.com/jackc/pgx/v4/pgxpool"
)

type ServerPayments struct {
	lis     net.Listener
	address string

	httpServer *http.Server
	mux        *http.ServeMux

	redisDB *common.Redis

	syncProducer *SyncProducer

	postgresDB *pgxpool.Pool

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

func (s *ServerPayments) Start(dbConnectionString string) error {
	var err error

	s.postgresDB, err = common.ConnectToDB(s.lifecycle.Ctx, dbConnectionString)
	if err != nil{
		return err
	}

	s.redisDB, err = common.ConnectToRedis(s.lifecycle.Ctx)
	if err != nil {
		return err
	}

	s.syncProducer, err = NewSyncProducer([]string{"kafka:9092"})
	if err != nil{
		return err
	}

	lis, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}

	s.httpServer = &http.Server{}
	s.mux.HandleFunc("/payment/add", s.paymentHandler)
	s.mux.HandleFunc("/detect/", s.detectRequestHandler)

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

		log.Println(payment)
		log.Println("хуй")

		if err := ValidatePaymentEvent(payment); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if s.redisDB.CheckBan(r.Context(), strconv.Itoa(int(payment.Payer.AccountID))) {
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

func (s *ServerPayments) detectRequestHandler(w http.ResponseWriter, r *http.Request){
	select {
	case <-s.lifecycle.Ctx.Done():
		log.Println("Request cancelled (shutdown or client disconnected)")
		return
	default:
		if r.Method != "POST" {
			http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
			return
		}

		detreq := &common.DetectRequest{}

		err := json.NewDecoder(r.Body).Decode(&detreq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		dbCtx, cancel := context.WithTimeout(r.Context(), time.Second * 1)
		defer cancel()

		if err := s.detReqInPostgres(dbCtx, detreq); err != nil{
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func (s *ServerPayments) detReqInPostgres(ctx context.Context, detreq *common.DetectRequest) error{
	_, err := s.postgresDB.Exec(ctx, `
	INSERT INTO fraud_requests()
	VALUES()`)
	if err != nil{
		return err
	}
	
	return nil
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

		if s.syncProducer != nil{
			if err := s.syncProducer.Close(); err != nil{
				errs = append(errs, err)
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
