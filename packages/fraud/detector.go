package fraud

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Krunis/anti-fraud-system/packages/common"
	"github.com/google/uuid"
)

func (a *AntiFraud) startDetector() {
	timer := time.NewTimer(time.Second * 3)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			log.Println("Detecting...")

			func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
				defer cancel()

				if err := a.detectAndBan(ctx); err != nil {
					log.Printf("Failed to detect fraud: %s", err)
				}
			}()

			timer.Reset(time.Second * 3)
		case <-a.lifecycle.Ctx.Done():
			return
		}
	}
}

func (a *AntiFraud) detectAndBan(ctx context.Context) error {
	tx, err := a.postgresDB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("Failed to start DB transaction: %s", err)
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
					SELECT id, account_id, merchant_id, interval_since FROM fraud_requests
					WHERE timestamp_req < NOW() AND executed=FALSE
					ORDER BY timestamp_req`)

	var id uuid.UUID

	detReq := &common.DetectRequest{
		Payer: &common.PayerType{},
		Payee: &common.PayeeType{},
	}

	if err := row.Scan(&id, &detReq.Payer.AccountID, &detReq.Payee.MerchantID, &detReq.IntervalSince); err != nil {
		return fmt.Errorf("Failed to scan row: %s", err)
	}

	if detReq.IntervalSince == nil {
		startTime := time.Unix(0, 0)
		detReq.IntervalSince = &startTime
	}

	log.Printf("Aggregating for %d", detReq.Payer.AccountID)

	scores, err := a.aggrFromClickHouse(ctx, detReq)
	if err != nil {
		return fmt.Errorf("Failed to aggregate: %s", err)
	}

	log.Println(scores)

	_, err = tx.Exec(ctx, `UPDATE fraud_requests
								SET executed=TRUE
								WHERE id=$1
								`, id.String())
	if err != nil {
		return fmt.Errorf("Failed to update executed status: %s", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("Failed to commit: %s", err)
	}

	if scores >= 40 {
		if err := a.banUser(ctx, detReq.Payer.AccountID); err != nil {
			log.Printf("Failed to ban user: %d error: %s", detReq.Payer.AccountID, err)
		}
	}

	return nil
}

func (a *AntiFraud) banUser(ctx context.Context, userID int64) error {
	if err := a.redisDB.Set(ctx, fmt.Sprintf("fraud:ban:%d", userID), "1", time.Minute*15).Err(); err != nil {
		return err
	}

	log.Printf("Banned: %d", userID)

	return nil
}
