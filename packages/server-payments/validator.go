package serverpayments

import (
	"fmt"
	"github.com/Krunis/anti-fraud-system/packages/common"
	"net"
	"strings"
)

type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ValidationErrors собирает все ошибки валидации
type ValidationErrors []*ValidationError

func (ve ValidationErrors) Error() string {
	var sb strings.Builder
	for i, err := range ve {
		if i > 0 {
			sb.WriteString("; ")
		}
		sb.WriteString(err.Error())
	}
	return sb.String()
}

// ValidatePaymentEvent основная функция валидации
func ValidatePaymentEvent(event *common.PaymentEvent) error {
	var errors ValidationErrors

	if event == nil {
		return &ValidationError{Field: "PaymentEvent", Message: "event is nil"}
	}

	// 1. Валидация EventID
	if event.EventID == "" {
		errors = append(errors, &ValidationError{Field: "EventID", Message: "cannot be empty"})
	} else if len(event.EventID) > 128 {
		errors = append(errors, &ValidationError{Field: "EventID", Message: "exceeds maximum length of 128 characters"})
	}

	// 2. Валидация EventTime
	if event.EventTime == "" {
		errors = append(errors, &ValidationError{Field: "EventTime", Message: "cannot be empty"})
	}

	// 3. Валидация Direction
	validDirections := map[string]bool{"inbound": true, "outbound": true}
	if event.Direction == "" {
		errors = append(errors, &ValidationError{Field: "Direction", Message: "cannot be empty"})
	} else if !validDirections[event.Direction] {
		errors = append(errors, &ValidationError{Field: "Direction", Message: "must be 'inbound' or 'outbound'"})
	}

	// 4. Валидация Transaction
	if event.Transaction == nil {
		errors = append(errors, &ValidationError{Field: "Transaction", Message: "cannot be nil"})
	} else {
		// Amount
		if event.Transaction.Amount == 0 {
			errors = append(errors, &ValidationError{Field: "Transaction.Amount", Message: "must be greater than 0"})
		}

		// Currency
		if event.Transaction.Currency == "" {
			errors = append(errors, &ValidationError{Field: "Transaction.Currency", Message: "cannot be empty"})
		}

		// Type
		validTypes := map[string]bool{
			"authorization": true,
			"capture":       true,
			"sale":          true,
			"refund":        true,
			"void":          true,
		}
		if event.Transaction.Type == "" {
			errors = append(errors, &ValidationError{Field: "Transaction.Type", Message: "cannot be empty"})
		} else if !validTypes[strings.ToLower(event.Transaction.Type)] {
			errors = append(errors, &ValidationError{Field: "Transaction.Type", Message: "must be 'authorization', 'capture', 'sale', 'refund', or 'void'"})
		}
	}

	// 5. Валидация Payer
	validatePayer(&errors, event.Payer)

	// 6. Валидация Payee
	validatePayee(&errors, event.Payee)

	// 7. Валидация Context
	if event.Context == nil {
		errors = append(errors, &ValidationError{Field: "Context", Message: "cannot be nil"})
	} else {
		validChannels := map[string]bool{
			"mobile_app": true,
			"web":        true,
			"pos":        true,
			"atm":        true,
			"api":        true,
		}
		if event.Context.Channel == "" {
			errors = append(errors, &ValidationError{Field: "Context.Channel", Message: "cannot be empty"})
		} else if !validChannels[strings.ToLower(event.Context.Channel)] {
			errors = append(errors, &ValidationError{Field: "Context.Channel", Message: "must be 'mobile_app', 'web', 'pos', 'atm', or 'api'"})
		}

		if len(event.Context.DeviceID) > 255 {
			errors = append(errors, &ValidationError{Field: "Context.DeviceID", Message: "exceeds maximum length of 255 characters"})
		}

		if event.Context.IP != "" {
			if net.ParseIP(event.Context.IP) == nil {
				errors = append(errors, &ValidationError{Field: "Context.IP", Message: "invalid IP address format"})
			}
		}

		if len(event.Context.UserAgent) > 512 {
			errors = append(errors, &ValidationError{Field: "Context.UserAgent", Message: "exceeds maximum length of 512 characters"})
		}
	}

	if len(errors) > 0 {
		return errors
	}

	return nil
}

func validatePayer(errors *ValidationErrors, payer *common.PayerType) {
	if payer == nil {
		*errors = append(*errors, &ValidationError{Field: "Payer", Message: "cannot be nil"})
	} else {
		if payer.AccountID == "" {
			*errors = append(*errors, &ValidationError{Field: "Payer.AccountID", Message: "cannot be empty"})
		}
	}
}

func validatePayee(errors *ValidationErrors, payee *common.PayeeType) {
	if payee == nil {
		*errors = append(*errors, &ValidationError{Field: "Payee", Message: "cannot be nil"})
	} else {
		if payee.MerchantID == "" {
			*errors = append(*errors, &ValidationError{Field: "Payee.MerchantID", Message: "cannot be empty"})
		}

		if len(payee.MerchantName) > 128 {
			*errors = append(*errors, &ValidationError{Field: "Payee.MerchantName", Message: "exceeds maximum length of 128 characters"})
		}

		if payee.Country == "" {
			*errors = append(*errors, &ValidationError{Field: "Payee.Country", Message: "cannot be empty"})
		} else if len(payee.Country) != 2 {
			*errors = append(*errors, &ValidationError{Field: "Payee.Country", Message: "must be 2-letter ISO code"})
		} else {
			countryUpper := strings.ToUpper(payee.Country)
			if countryUpper != payee.Country {
				*errors = append(*errors, &ValidationError{Field: "Payee.Country", Message: "must be uppercase"})
			}
		}
	}
}

func ValidateDetectRequest(detReq *common.DetectRequest) error {
	var errors ValidationErrors

	if detReq == nil {
		return &ValidationError{Field: "DetectRequest", Message: "request is nil"}
	}

	validatePayer(&errors, detReq.Payer)

	validatePayee(&errors, detReq.Payee)

	if detReq.Interaction != common.PayerInteraction &&
	detReq.Interaction != common.PayeeInteraction &&
	detReq.Interaction != common.PersonalInteraction &&
	detReq.Interaction != common.GeneralInteraction {
		errors = append(errors, &ValidationError{Field: "Interaction", Message: "must be PAYER, PAYEE, PERSONAL or GENERAL"})
	}

	if detReq.IntervalSince.String() == "" {
		errors = append(errors, &ValidationError{Field: "IntervalSince", Message: "cannot be empty"})
	}

    if len(errors) > 0 {
		return errors
	}

	return nil
}
