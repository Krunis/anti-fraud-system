package fraud

import (
	"context"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Krunis/anti-fraud-system/packages/common"
)

// func (a *AntiFraud) sendInClickHouse(ctx context.Context, payment *common.PaymentEvent) error {
// 	err := a.clickHouse.Conn.Exec(ctx, `
// 									INSERT INTO fraud.events (
// 										event_id, event_time, direction,
// 										amount, currency, transaction_type,
// 										account_id, merchant_id, merchant_name, country,
// 										channel, device_id, ip, user_agent
// 									) VALUES (
// 										@event_id, @event_time, @direction,
// 										@amount, @currency, @tx_type,
// 										@account_id, @merchant_id, @merchant_name, @country,
// 										@channel, @device_id, @ip, @user_agent
// 									)`,
// 		clickhouse.Named("event_id", payment.EventID),
// 		clickhouse.Named("event_time", payment.EventTime),
// 		clickhouse.Named("direction", payment.Direction),
// 		clickhouse.Named("amount", payment.Transaction.Amount),
// 		clickhouse.Named("currency", payment.Transaction.Currency),
// 		clickhouse.Named("tx_type", payment.Transaction.Type),
// 		clickhouse.Named("account_id", payment.Payer.AccountID),
// 		clickhouse.Named("merchant_id", payment.Payee.MerchantID),
// 		clickhouse.Named("merchant_name", payment.Payee.MerchantName),
// 		clickhouse.Named("country", payment.Payee.Country),
// 		clickhouse.Named("channel", payment.Context.Channel),
// 		clickhouse.Named("device_id", payment.Context.DeviceID),
// 		clickhouse.Named("ip", payment.Context.IP),
// 		clickhouse.Named("user_agent", payment.Context.UserAgent),
// 	)
// 	if err != nil {
// 		return err
// 	}

// 	return nil
// }

func (a *AntiFraud) pollerToClickHouse() {
	timer := time.NewTimer(time.Second * 5)
	defer timer.Stop()

	var err error
	var batch driver.Batch
	var count int

	for {
		select {
		case <-a.lifecycle.Ctx.Done():
			return
		case <-timer.C:
			sendBatch(batch, count)

			batch = nil

			count = 0

			timer.Reset(time.Second * 5)
		case payment, ok := <-a.paymentCh:
			if !ok {
				log.Println("Sending batch by fail to get from channel")

				sendBatch(batch, count)

				return
			}

			if batch == nil {
				batch, err = a.clickHouse.Conn.PrepareBatch(a.lifecycle.Ctx, `
										INSERT INTO fraud.payments (
										event_id, event_time, direction, 
										amount, currency, transaction_type,
										account_id, merchant_id, merchant_name, country,
										channel, device_id, ip, user_agent)`,
				)
				if err != nil {
					log.Printf("Failed to prepare batch: %s", err)
					continue
				}

				count = 0
			}

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
			if err != nil {
				log.Printf("Failed to append in batch: %s", err)
			}

			count++

			log.Printf("Appended in batch: %s", payment.EventID)
		}
	}
}

func sendBatch(batch driver.Batch, count int) {
	if batch == nil || count == 0 {
		return
	}

	if err := batch.Send(); err != nil {
		log.Printf("Failed to send batch. Error: %s", err)
		return
	}

	log.Println("Batch sent")

}

func (a *AntiFraud) aggrFromClickHouse(ctx context.Context, detReq *common.DetectRequest) (int, error) {
	var score int = 0

	switch detReq.Interaction {
	case common.PayerInteraction:
		var sum, uniqCountries, countPayments int

		row := a.clickHouse.Conn.QueryRow(ctx, `
												SELECT 
													SUM(amount),
													COUNT(DISTINCT country),
													COUNT(event_id)
												FROM fraud.payments
												WHERE account_id = $1 AND event_time >= $2`,
												detReq.Payer.AccountID, detReq.IntervalSince)
		if row.Err() != nil {
			return 100000, row.Err()
		}
		row.Scan(&sum, &uniqCountries, &countPayments)

		if sum >= 500000 {
			score += 40
		}
		if uniqCountries > 3 {
			score += 30
		}
		if countPayments > 10 {
			score += 30
		}
	case common.PayeeInteraction:
		var merchantId, merchantName string
		var uniqueDevices, uniqueAccounts uint64

		row := a.clickHouse.Conn.QueryRow(ctx, `
		SELECT
			merchant_id,
			merchant_name,
			UNIQ(device_id) as unique_devices,
			UNIQ(account_id) as unique_accounts
		FROM fraud.payments
		WHERE merchant_id = $1 AND event_time >= $2
		GROUP BY merchant_id, merchant_name
		HAVING unique_devices / unique_accounts > 2
		`, detReq.Payee.MerchantID, detReq.IntervalSince)

		row.Scan(&merchantId, &merchantName, &uniqueDevices, &uniqueAccounts)

	case common.PersonalInteraction:
		var exists bool

		row := a.clickHouse.Conn.QueryRow(ctx, `
											SELECT EXISTS(
												SELECT 1
												FROM fraud.payments
												WHERE account_id = $1 AND merchant_id = $2 AND event_time >= $3)`,
			detReq.Payer.AccountID, detReq.Payee.MerchantID, detReq.IntervalSince)
		if row.Err() != nil {
			return 100000, row.Err()
		}
		row.Scan(&exists)

		if !exists {
			score += 30
		}
	case common.GeneralInteraction:
		var deviceID string

		var uniqAccounts, uniqIPs int

		row := a.clickHouse.Conn.QueryRow(ctx, `
												SELECT 
													device_id,
													uniq(account_id) AS unique_accounts,
													uniq(ip) AS unique_ips,
												FROM fraud_table
												WHERE event_time >= $1
												GROUP BY device_id
												HAVING unique_accounts > 3`,
												detReq.IntervalSince)	
		if row.Err() != nil{
			return 100000, row.Err()
		}

		row.Scan(&deviceID, &uniqAccounts, &uniqIPs)

		log.Printf("From clickhouse: with %s device id %d unique accounts with %d unique IPs, banning...", deviceID, uniqAccounts, uniqIPs)

		//ban device_id

		//rework returning
	}

	return score, nil
}
