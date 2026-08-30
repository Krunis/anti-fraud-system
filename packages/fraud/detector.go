package fraud

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Krunis/anti-fraud-system/packages/common"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
)

type TwoPhaseBan struct {
	redisDB *common.Redis
	txID    string
	userIDs []string
	banDur  time.Duration
	ttl     time.Duration
}

type banTarget struct {
	UserIDs  []string
	Duration time.Duration
}

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
	detReq, err := a.fetchNextRequest(ctx)
	if err != nil{
		return err
	}
	if detReq == nil{
		return nil
	}

	log.Printf("Aggregating for %s", detReq.Payer.AccountID)

	checkResult, err := a.aggrFromClickHouse(ctx, detReq)
	if err != nil {
		return fmt.Errorf("Failed to aggregate: %s", err)
	}
	if checkResult == nil || !checkResult.ShouldBan {
		return nil
	}

	targets := banTargets(detReq, checkResult)

	for _, target := range targets {
		if err := a.banByUserIDs(ctx, target.UserIDs, target.Duration); err != nil {
			log.Printf("failed to ban users %v: %s", target.UserIDs, err)
			return err
		}
	}
	
	return nil
}

func (a *AntiFraud) fetchNextRequest(ctx context.Context) (*common.DetectRequest, error){
	tx, err := a.postgresDB.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("Failed to start DB transaction: %s", err)
	}
	defer tx.Rollback(ctx)

	var id uuid.UUID

	detReq := &common.DetectRequest{
		Payer: &common.PayerType{},
		Payee: &common.PayeeType{},
	}	

	err = tx.QueryRow(ctx, `
					UPDATE fraud_requests
					SET executed=TRUE
					WHERE id = (
						SELECT id FROM fraud_requests
						WHERE timestamp_req <= NOW() AND executed = FALSE
						ORDER BY timestamp_req
						LIMIT 1
						FOR UPDATE SKIP LOCKED
						)
					RETURNING id, account_id, merchant_id, interaction, interval_since
					`).Scan(
						&id, &detReq.Payer.AccountID, &detReq.Payee.MerchantID,
						&detReq.Interaction, &detReq.IntervalSince,
					)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to update and fetch row: %s", err)
	}

	if err := tx.Commit(ctx); err != nil{
		return nil, fmt.Errorf("Failed to commit: %s", err)
	}

	//retrying after hard operations

	if detReq.IntervalSince == nil {
		startTime := time.Unix(0, 0)
		detReq.IntervalSince = &startTime
	}

	return detReq, nil
}

func banTargets(detReq *common.DetectRequest, checkResult *FraudCheckResult) []banTarget {
	if len(checkResult.BanDetails) > 0 {
		targets := make([]banTarget, 0, len(checkResult.BanDetails))
		for _, bd := range checkResult.BanDetails {
			targets = append(targets, banTarget{UserIDs: bd.Targets, Duration: bd.Duration})
		}
		return targets
	}

	var toBan []string

	switch detReq.Interaction {
	case common.PayerInteraction:
		toBan = append(toBan, detReq.Payer.AccountID)
	case common.PayeeInteraction:
		toBan = append(toBan, detReq.Payee.MerchantID)
	}
	
	if len(toBan) == 0 {
		return nil
	}

	return []banTarget{{UserIDs: toBan, Duration: 15 * time.Minute}} 
}

func (a *AntiFraud) banByUserIDs(ctx context.Context, userIDs []string, duration time.Duration) error {
	t := &TwoPhaseBan{
		redisDB: a.redisDB,
		txID:    "1",
		userIDs: userIDs,
		banDur:  duration,
		ttl:     time.Minute * 5,
	}

	if err := t.prepareBan(ctx); err != nil {
		return fmt.Errorf("failed to prepare bans: %s", err)
	}

	if err := t.validateUsers(ctx); err != nil {
		log.Printf("validation error: %s", err)

		err := t.rollbackBans(ctx)
		return fmt.Errorf("failed to rollback bans: %s", err)
	}

	if err := t.commitBan(ctx); err != nil {
		return fmt.Errorf("CRITICAL ERROR to commit bans failed consistence: %s", err)
	}

	log.Printf("Banned: %s", userIDs)

	return nil
}

func (a *AntiFraud) userIDsByDevices(ctx context.Context, devices []string) ([]string, error) {
	userIDs := make([]string, 0, len(devices))

	var userID string

	for _, device := range devices {
		rows, err := a.postgresDB.Query(ctx, `
								SELECT account_id
								FROM payment_events
								WHERE device_id = $1
								`, device)
		if err != nil {
			log.Printf("failed to get account id from psotgres: %s", err)
		}

		for rows.Next() {
			if err := rows.Scan(&userID); err != nil {
				log.Printf("failed to scan account_id row: %s", err)
			}

			userIDs = append(userIDs, userID)
		}

		rows.Close()
	}

	return userIDs, nil
}

func (t *TwoPhaseBan) prepareBan(ctx context.Context) error {
	pipeline := t.redisDB.Client.Pipeline()

	for _, userID := range t.userIDs {
		key := fmt.Sprintf("ban:tx:%s:user:%s", t.txID, userID)

		pipeline.Set(ctx, key, "PENDING", t.ttl)
	}

	pipeline.Set(ctx, fmt.Sprintf("ban:tx:%s:status", t.txID), "PREPARED", t.ttl)

	_, err := pipeline.Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (t *TwoPhaseBan) validateUsers(ctx context.Context) error {

	return nil
}

func (t *TwoPhaseBan) commitBan(ctx context.Context) error {
	pipeline := t.redisDB.Client.Pipeline()

	for _, userID := range t.userIDs {
		pipeline.Set(ctx, fmt.Sprintf("fraud:ban:%s", userID), "1", t.banDur)

		tempKey := fmt.Sprintf("ban:tx:%s:user:%s", t.txID, userID)
		pipeline.Del(ctx, tempKey)
	}

	pipeline.Del(ctx, fmt.Sprintf("ban:tx:%s:status", t.txID))

	_, err := pipeline.Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (t *TwoPhaseBan) rollbackBans(ctx context.Context) error {
	pipeline := t.redisDB.Client.Pipeline()

	for _, userID := range t.userIDs {
		tempKey := fmt.Sprintf("ban:tx:%s:user:%s", t.txID, userID)
		pipeline.Del(ctx, tempKey)
	}

	pipeline.Del(ctx, fmt.Sprintf("ban:tx:%s:status", t.txID))

	_, err := pipeline.Exec(ctx)
	if err != nil {
		return err
	}

	return nil
}
