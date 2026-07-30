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

	paymentsToKafka chan *common.PaymentEvent

	lifecycle *common.Lifecycle

	stopOnce sync.Once
}

func NewServerPayments(address string) *ServerPayments {
	ctx, cancel := context.WithCancel(context.Background())

	return &ServerPayments{
		address:         address,
		mux:             http.NewServeMux(),
		paymentsToKafka: make(chan *common.PaymentEvent, 2000),
		lifecycle: &common.Lifecycle{
			Ctx:    ctx,
			Cancel: cancel,
		},
	}
}

func (s *ServerPayments) Start(dbConnectionString string) error {
	var err error

	s.postgresDB, err = common.ConnectToDB(s.lifecycle.Ctx, dbConnectionString)
	if err != nil {
		return err
	}

	s.redisDB, err = common.ConnectToRedis(s.lifecycle.Ctx)
	if err != nil {
		return err
	}

	s.syncProducer, err = NewSyncProducer(s.lifecycle.Ctx, []string{"kafka:9092"})
	if err != nil {
		return err
	}

	go s.senderPaymentsToKafka()

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
			log.Printf("HTTP Error: %s", err)
			return
		}
		defer r.Body.Close()

		log.Println(payment)
		log.Println("хуй")

		if err := ValidatePaymentEvent(payment); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			log.Printf("HTTP Error: %s", err)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), time.Second*2)
		defer cancel()

		if s.redisDB.CheckBan(ctx, payment.Payer.AccountID) {
			http.Error(w, "banned", http.StatusForbidden)
			log.Printf("HTTP Error: %s", err)
			return
		}

		if err := s.paymentEventInPostgres(ctx, payment); err != nil{
			log.Println(err)
			http.Error(w, "try again later", http.StatusInternalServerError)
			return
		}

		select {
		case s.paymentsToKafka <- payment:
			w.WriteHeader(http.StatusCreated)
		case <-ctx.Done():
			http.Error(w, ctx.Err().Error(), http.StatusInternalServerError)
			return
		}
	}
}

func (s *ServerPayments) detectRequestHandler(w http.ResponseWriter, r *http.Request) {
	select {
	case <-s.lifecycle.Ctx.Done():
		log.Println("Request cancelled (shutdown or client disconnected)")
		return
	default:
		if r.Method != "POST" {
			http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
			return
		}

		detReq := &common.DetectRequest{}

		err := json.NewDecoder(r.Body).Decode(&detReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		if err := ValidateDetectRequest(detReq); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			log.Printf("HTTP Error: %s", err)
			return
		}

		dbCtx, cancel := context.WithTimeout(r.Context(), time.Second*1)
		defer cancel()

		if err := s.detReqInPostgres(dbCtx, detReq); err != nil {
			log.Println(err)
			http.Error(w, "try again later", http.StatusInternalServerError)
			return
		}

		log.Printf("Detect request sent: %s", detReq.Payer.AccountID)

		w.WriteHeader(http.StatusCreated)
	}
}

func (s *ServerPayments) detReqInPostgres(ctx context.Context, detReq *common.DetectRequest) error {
	_, err := s.postgresDB.Exec(ctx, `
									INSERT INTO fraud_requests(
										account_id,
										merchant_id, 
										interaction, 
										interval_since, 
										timestamp_req
									)
									VALUES($1, $2, $3, $4)
									`,
									detReq.Payer.AccountID, detReq.Payee.MerchantID, detReq.Interaction, detReq.IntervalSince, time.Now())
	if err != nil {
		return fmt.Errorf("failed to insert detect req in postgres: %s", err)
	}

	return nil
}

func (s *ServerPayments) paymentEventInPostgres(ctx context.Context, payment *common.PaymentEvent) error {
	_, err := s.postgresDB.Exec(ctx, `
										INSERT INTO fraud_requests(
											event_id,
											event_time,
											direction,
											amount,
											currency,
											transaction_type,
											account_id,
											merchant_id,
											merchant_name,
											country,
											channel,
											device_id,
											ip TEXT, 
											user_agent
										)
										VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
										`,
										payment.EventID, payment.EventTime, payment.Direction, payment.Transaction.Amount,
										payment.Transaction.Currency, payment.Transaction.Type, payment.Payer.AccountID,
										payment.Payee.MerchantID, payment.Payee.MerchantName, payment.Payee.Country,
										payment.Context.Channel, payment.Context.DeviceID, payment.Context.IP,
										payment.Context.UserAgent)
	if err != nil {
		return fmt.Errorf("failed to insert payment in postgres: %s", err)
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

		if s.syncProducer != nil {
			if err := s.syncProducer.Close(); err != nil {
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

func (s *ServerPayments) senderPaymentsToKafka() {
	paymentsSlice := make([]*common.PaymentEvent, 0, 2000)

	timer := time.NewTimer(time.Second * 1)
	defer timer.Stop()

	for {
		select {
		case payment := <-s.paymentsToKafka:
			paymentsSlice = append(paymentsSlice, payment)

			log.Println(len(paymentsSlice))

			if len(paymentsSlice) >= 2000 {
				log.Printf("Flushing %d payments to Kafka (limit)", len(paymentsSlice))

				if err := s.ProduceToKafka("payment-events", paymentsSlice); err != nil {
					log.Printf("Error while producing to Kafka: %s", err)
				}

				paymentsSlice = make([]*common.PaymentEvent, 0, 2000)
			}
		case <-timer.C:
			log.Println(len(paymentsSlice))

			if len(paymentsSlice) > 0 {
				log.Printf("Flushing %d payments to Kafka (timeout)", len(paymentsSlice))

				if err := s.ProduceToKafka("payment-events", paymentsSlice); err != nil {
					log.Printf("Error while producing to Kafka: %s", err)
				}

				paymentsSlice = make([]*common.PaymentEvent, 0, 2000)
			}

			timer.Reset(time.Second * 1)
		}
	}

}
