package fraud

import (
	"context"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Krunis/anti-fraud-system/packages/common"
)

func (a *AntiFraud) sendInClickHouse(ctx context.Context, payment *common.PaymentEvent) error {
	err := a.clickHouse.Conn.Exec(ctx, `
									INSERT INTO fraud.events (
										event_id, event_time, direction, 
										amount, currency, transaction_type,
										account_id, merchant_id, merchant_name, country,
										channel, device_id, ip, user_agent
									) VALUES (
										@event_id, @event_time, @direction, 
										@amount, @currency, @tx_type,
										@account_id, @merchant_id, @merchant_name, @country,
										@channel, @device_id, @ip, @user_agent
									)`,
		clickhouse.Named("event_id", payment.EventID),
		clickhouse.Named("event_time", payment.EventTime),
		clickhouse.Named("direction", payment.Direction),
		clickhouse.Named("amount", payment.Transaction.Amount),
		clickhouse.Named("currency", payment.Transaction.Currency),
		clickhouse.Named("tx_type", payment.Transaction.Type),
		clickhouse.Named("account_id", payment.Payer.AccountID),
		clickhouse.Named("merchant_id", payment.Payee.MerchantID),
		clickhouse.Named("merchant_name", payment.Payee.MerchantName),
		clickhouse.Named("country", payment.Payee.Country),
		clickhouse.Named("channel", payment.Context.Channel),
		clickhouse.Named("device_id", payment.Context.DeviceID),
		clickhouse.Named("ip", payment.Context.IP),
		clickhouse.Named("user_agent", payment.Context.UserAgent),
	)
	if err != nil {
		return err
	}

	return nil
}

func (a *AntiFraud) pollerToClickHouse() {
	

	timer := time.NewTimer(time.Second * 5)
	defer timer.Stop()

	for {
		select {
		case <-a.lifecycle.Ctx.Done():
			return
		case <-timer.C:
			batch, err := a.clickHouse.Conn.PrepareBatch(a.lifecycle.Ctx, `
			INSERT INTO fraud.payments (
										event_id, event_time, direction, 
										amount, currency, transaction_type,
										account_id, merchant_id, merchant_name, country,
										channel, device_id, ip, user_agent)`,
									) 
			if err != nil{
				log.Printf("Failed to prepare batch: %s", err)
			}

			for payment := range a.paymentCh{
				err := batch.Append(
					payment.EventID,
					payment.EventTime,
					payment.Direction,
					payment.Transaction.Amount,
					payment.Transaction.Currency,
					payment.Transaction.Type,
					payment.Payer.AccountID,
					payment.Payee.MerchantID,
					payment.Payee.MerchantName,
					payment.Payee.Country,
					payment.Context.Channel,
					payment.Context.DeviceID,
					payment.Context.IP,
					payment.Context.UserAgent,
				)
				if err != nil{
					log.Printf("Failed to append in batch: %s", err)

				}
			}

			if err := batch.Send(); err != nil{
				log.Printf("Failed to send batch: %s", err)
			}
		}
	}
}

func (a *AntiFraud) aggrFromClickHouse(ctx context.Context, payment *common.PaymentEvent) (int32, error) {
	var sum int

	var score int32 = 0

	var exists bool

	row := a.clickHouse.Conn.QueryRow(ctx, `
											SELECT sum(amount)
											FROM fraud.payments
											WHERE account_id=$1 AND event_time + INTERVAL 1 WEEK <= NOW()`,
												payment.Payer.AccountID)
	if row.Err() != nil{
		return 100000, row.Err()
	}
	row.Scan(&sum)
	
	if sum >= 50000000{
		score += 40
	}

	row = a.clickHouse.Conn.QueryRow(ctx, `
											SELECT EXISTS(
												SELECT 1
												FROM fraud.payments
												WHERE account_id=$1 AND merchant_id=$2 AND event_time BETWEEN NOW() - INTERVAL 1 MONTH AND NOW()`,
												payment.Payer.AccountID, payment.Payee.MerchantID)
	if row.Err() != nil{
		return 100000, row.Err()
	}
	row.Scan(&exists)

	if !exists{
		score += 30
	}

	return score, nil
}
