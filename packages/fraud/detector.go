package fraud

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Krunis/anti-fraud-system/packages/common"
)

func (a *AntiFraud) startDetector() {
	timer := time.NewTimer(time.Second * 3)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)

			if err := a.detectAndBan(ctx); err != nil {
				log.Printf("Failed to detect fraud: %s", err)
			}

			cancel()
		case <-a.lifecycle.Ctx.Done():
			return
		}
	}
}

func (a *AntiFraud) detectAndBan(ctx context.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rows, err := a.postgresDB.Query(ctx, `
					SELECT * FROM fraud_requests
					WHERE interval_since < NOW()
					LIMIT 10`)
	if err != nil {
		return err
	}

	for rows.Next() {
		var row *common.DetectRequest

		if err := rows.Scan(&row); err != nil {
			log.Printf("Failed to scan row: %s", err)
		}

		scores, err := a.aggrFromClickHouse(ctx, row)
		if err != nil {
			return err
		}

		if scores >= 150 {
			if err := a.banUser(ctx, row.Payer.AccountID); err != nil {
				log.Printf("Failed to ban user: %d error: %s", row.Payer.AccountID, err)
			}
		}
	}

	return nil
}

func (a *AntiFraud) banUser(ctx context.Context, userID int64) error {
	return a.redisDB.Set(ctx, fmt.Sprintf("fraud:ban:%d", userID), "1", time.Minute*15).Err()
}