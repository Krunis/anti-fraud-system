package common

import "context"

type Lifecycle struct {
	Ctx context.Context
	Cancel context.CancelFunc
}

type TransactionType struct {
	Amount   uint32
	Currency string
	Type     string
}

type PayerType struct {
	AccountID string
}

type PayeeType struct {
	MerchantID   string
	MerchantName string
	Country      string
}

type ContextData struct {
	Channel   string
	DeviceID  string
	IP        string
	UserAgent string
}

type PaymentEvent struct {
	EventID   string
	EventTime string
	Direction string

	Transaction *TransactionType

	Payer *PayerType

	Payee *PayeeType

	Context *ContextData
}