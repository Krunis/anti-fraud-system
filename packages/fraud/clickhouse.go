package fraud

import (
	"context"
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
			payment := <-a.paymentCh

			a.clickHouse.Conn.Query()
		}
	}
}

func (a *AntiFraud) aggrFromClickHouse(ctx context.Context, payment *common.PaymentEvent) error {
	a.clickHouse.Conn.Query(ctx, `
	SELECT sum(amount)
	FROM db.table
	WHERE account_id=$1 AND event_time + WEEK() <= NOW()`,
	)

	a.clickHouse.Conn.Query(ctx, `
	`)
	
	return nil
}
