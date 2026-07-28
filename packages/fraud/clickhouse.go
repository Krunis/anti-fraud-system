package fraud

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Krunis/anti-fraud-system/packages/common"
)

type FraudCheckResult struct {
	Score   int
	Reasons []string

	BanDetails []*BanDetail
}

type BanDetail struct {
	BanType  string
	Targets  []string
	Reason   string
	Duration string
}

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
	switch detReq.Interaction {
	case common.PayerInteraction:
		a.checkPayerInteraction(ctx, detReq)
	case common.PayeeInteraction:
		a.checkPayeeInteraction(ctx, detReq)
	case common.PersonalInteraction:
		a.checkPersonalInteraction(ctx, detReq)
	case common.GeneralInteraction:
		a.checkGeneralInteraction(ctx, detReq)
	}

	return score, nil
}

func (a *AntiFraud) checkPayerInteraction(ctx context.Context, detReq *common.DetectRequest) (*FraudCheckResult, error) {
	fraudResult := &FraudCheckResult{}

	var sum, uniqCountries, countPayments int

	row := a.clickHouse.Conn.QueryRow(ctx, `
												SELECT 
													SUM(amount),
													COUNT(DISTINCT country),
													COUNT(event_id)
												FROM fraud.payments
												WHERE account_id = $1 AND event_time >= $2
												`, detReq.Payer.AccountID, detReq.IntervalSince)

	if err := row.Scan(&sum, &uniqCountries, &countPayments); err != nil {
		return fraudResult, fmt.Errorf("failed to scan row while %s interaction", detReq.Interaction)
	}

	if sum >= 500000 {
		fraudResult.Score += 40
	}
	if uniqCountries > 3 {
		fraudResult.Score += 30
	}
	if countPayments > 10 {
		fraudResult.Score += 30
	}
}

func (a *AntiFraud) checkPayeeInteraction(ctx context.Context, detReq *common.DetectRequest) (*FraudCheckResult, error) {
	fraudResult := &FraudCheckResult{}

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

	if err := row.Scan(&merchantId, &merchantName, &uniqueDevices, &uniqueAccounts); err != nil {
		return fraudResult, fmt.Errorf("failed to scan row while %s interaction", detReq.Interaction)
	}

	fraudResult.Score += 30
}

func (a *AntiFraud) checkPersonalInteraction(ctx context.Context, detReq *common.DetectRequest) (*FraudCheckResult, error) {
	fraudResult := &FraudCheckResult{}

	var exists bool

	row := a.clickHouse.Conn.QueryRow(ctx, `
												SELECT EXISTS(
													SELECT 1
													FROM fraud.payments
													WHERE account_id = $1 AND merchant_id = $2 AND event_time >= $3
												)
													`, detReq.Payer.AccountID, detReq.Payee.MerchantID, detReq.IntervalSince)

	if err := row.Scan(&exists); err != nil {
		return fraudResult, fmt.Errorf("failed to scan row while %s interaction", detReq.Interaction)
	}

	if exists {
		fraudResult.Score += 30
	}
}

func (a *AntiFraud) checkGeneralInteraction(ctx context.Context, detReq *common.DetectRequest) (*FraudCheckResult, error) {
	var deviceID string

	var uniqAccounts, uniqIPs int

	rows, err := a.clickHouse.Conn.Query(ctx, `
												SELECT 
													device_id,
													uniq(account_id) AS unique_accounts,
													uniq(ip) AS unique_ips,
												FROM fraud_table
												WHERE event_time >= $1
												GROUP BY device_id
												HAVING unique_accounts > 3`,
		detReq.IntervalSince)
	if err != nil {
		return nil, err
	}

	for rows.Next(){
		if err := rows.Scan(&deviceID, &uniqAccounts, &uniqIPs); err != nil{
			log.Printf("failed to scan row while %s interaction", detReq.Interaction)
		}

		log.Printf("From clickhouse: with %s device id %d unique accounts with %d unique IPs, banning...", deviceID, uniqAccounts, uniqIPs)


	}

	

	

	//ban device_id

	//rework returning
}
