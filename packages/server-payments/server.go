package serverpayments

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"

	"github.com/Krunis/anti-fraud-system/packages/common"
)

type ServerPayments struct{
	lis net.Listener
	address string

	httpServer *http.Server

	redisDB *common.Redis

	syncProducer *SyncProducer

	lifecycle *common.Lifecycle
}

func NewServerPayments(address string) *ServerPayments{
	ctx, cancel := context.WithCancel(context.Background())

	return &ServerPayments{
		address: address,
		lifecycle: &common.Lifecycle{
			Ctx: ctx,
			Cancel: cancel,
		},
	}
}

func (s *ServerPayments) Start() error{
	lis, err := net.Listen("tcp", s.address)
	if err != nil{
		return err
	}

	s.redisDB, err = common.ConnectToRedis(s.lifecycle.Ctx)
	if err != nil{
		return err
	}

	s.httpServer = &http.Server{}

	if err := s.httpServer.Serve(lis); err != nil{
		return err
	}
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

		if err := ValidatePaymentEvent(payment); err != nil{
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if s.redisDB.CheckBan(r.Context(), payment.Payer.AccountID){
			http.Error(w, "banned", http.StatusForbidden)
			return
			}

		if err := s.ProduceToKafka("payment-events", payment); err != nil{
			http.Error(w, "server unavailable", http.StatusInternalServerError)
			return
		}
		
		w.WriteHeader(http.StatusCreated)
	}
} 