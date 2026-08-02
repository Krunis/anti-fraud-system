package fraud

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/Krunis/anti-fraud-system/packages/common"
	"github.com/jackc/pgx/v4"
)

type FraudCheckResult struct {
	Score   int
	Reasons []string
	ShouldBan bool
	BanDetails []*BanDetail
}

type BanDetail struct {
	Targets  []string
	Reason   string
	Duration time.Duration
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
				log.Println("While sending batch fail to get from channel")

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

func (a *AntiFraud) aggrFromClickHouse(ctx context.Context, detReq *common.DetectRequest) (*FraudCheckResult, error) {
	var checkResult *FraudCheckResult

	var err error
	
	switch detReq.Interaction {
	case common.PayerInteraction:
		checkResult, err = a.checkPayerInteraction(ctx, detReq)
		if err != nil{
			return nil, err
		}
	case common.PayeeInteraction:
		checkResult, err = a.checkPayeeInteraction(ctx, detReq)
		if err != nil{
			return nil, err
		}
	case common.PersonalInteraction:
		checkResult, err = a.checkPersonalInteraction(ctx, detReq)
		if err != nil{
			return nil, err
		}
	case common.GeneralInteraction:
		checkResult, err = a.checkGeneralInteraction(ctx, detReq)
		if err != nil{
			return nil, err
		}
	}

	if checkResult.Score > 100{
		checkResult.ShouldBan = true
	}

	return checkResult, nil
}

func (a *AntiFraud) checkPayerInteraction(ctx context.Context, detReq *common.DetectRequest) (*FraudCheckResult, error) {
	fraudResult := &FraudCheckResult{Score: 0}

	var uniqCountries, countPayments int

	row := a.clickHouse.Conn.QueryRow(ctx, `
											SELECT
												COUNT(DISTINCT country),
												COUNT(event_id)
											FROM fraud.payments
											WHERE account_id = $1 AND event_time >= $2
											`, detReq.Payer.AccountID, detReq.IntervalSince)

	if err := row.Scan(&uniqCountries, &countPayments); err != nil {
		return fraudResult, fmt.Errorf("failed to scan row while %s interaction", detReq.Interaction)
	}

	if uniqCountries > 3 {
		fraudResult.Score += 30
		fraudResult.Reasons = append(fraudResult.Reasons, "too many country for account")
	}
	if countPayments > 10 {
		fraudResult.Score += 30
		fraudResult.Reasons = append(fraudResult.Reasons, "too many payments for account")
	}

	if fraudResult.Score > 100{
		fraudResult.ShouldBan = true
	}

	return fraudResult, nil
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

	log.Printf("Payee interaction for merchant id: %s unique_devices/unique_accounts=%d", merchantId, uniqueDevices/uniqueAccounts)

	fraudResult.Score += 30
	fraudResult.Reasons = append(fraudResult.Reasons, "too many devices for so low accounts")

	return fraudResult, nil
}

func (a *AntiFraud) checkPersonalInteraction(ctx context.Context, detReq *common.DetectRequest) (*FraudCheckResult, error) {
	fraudResult := &FraudCheckResult{}

	var txsCount, activeDays uint64

	var totalVolume, netFlow, growthRatio float64

	var timeSpan time.Duration

	row := a.clickHouse.Conn.QueryRow(ctx, `
											SELECT
												COUNT() as txs_count,
												SUM(amount) as total_volume,
												sumIf(amount, account_id = $1) - sumIf(amount, account_id = $2) as net_flow,
												uniqExact(toYYYYMMDD(event_time)) as active_days,
												max(event_time) - min(event_time) as time_span,
												max(amount) / min(amount) as growth_ratio
											FROM fraud.payments
											WHERE event_time >= $3 
												AND (account_id = $1 AND merchant_id = $2
														OR account_id = $2 AND merchant_id = $1)
											GROUP BY account_id, merchant_id
											HAVING
												txs_count > 20
												AND total_volume >= 3000000
												AND ABS(net_flow) < total_volume * 0.05
												AND active_days >= 5
												AND growth_ratio >= 2.5
											`, detReq.Payer.AccountID, detReq.Payee.MerchantID, detReq.IntervalSince)
	if err := row.Scan(&txsCount, &totalVolume, &netFlow, &activeDays, &timeSpan, &growthRatio); err != nil && err != pgx.ErrNoRows {
		return fraudResult, fmt.Errorf("failed to scan row while %s interaction", detReq.Interaction)
	}

	log.Printf("Found fraud between %s and %s with %d transactions.\nTotal volume: %v.\nNet flow: %v\nTime span %v active %d",
				detReq.Payer.AccountID, detReq.Payee.MerchantID, txsCount, totalVolume, netFlow, timeSpan, activeDays)

	fraudResult.Score += 100
	fraudResult.BanDetails = append(fraudResult.BanDetails, &BanDetail{
		Targets: []string{detReq.Payer.AccountID, detReq.Payee.MerchantID},
		Reason: "falsification of turnover",
		Duration: 15,
	})

	return fraudResult, nil
}

func (a *AntiFraud) checkGeneralInteraction(ctx context.Context, detReq *common.DetectRequest) (*FraudCheckResult, error) {
	fraudResult := &FraudCheckResult{}

	deviceIDs := []string{}

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
		if err := rows.Scan(&deviceID, &uniqAccounts, &uniqIPs); err != nil || err != pgx.ErrNoRows{
			log.Printf("failed to scan row while %s interaction", detReq.Interaction)
		}

		log.Printf("From clickhouse: with %s device id %d unique accounts with %d unique IPs, banning...", deviceID, uniqAccounts, uniqIPs)

		deviceIDs = append(deviceIDs, deviceID)
	}
	if err := rows.Err(); err != nil{
		return nil, err
	}

	fraudResult.Score += 100
	userIDs, err := a.userIDsByDevices(ctx, deviceIDs)
	fraudResult.BanDetails = append(fraudResult.BanDetails, &BanDetail{
		Targets: userIDs,
		Reason: "in the users list that has a suspicious device" ,
		Duration: 15,
	})
	if err != nil{
		return nil, err
	}

	return fraudResult, nil
}
