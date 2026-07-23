package fraud

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Krunis/anti-fraud-system/packages/common"
	"github.com/google/uuid"
)

type TwoPhaseBan struct{
	redisDB *common.Redis
	txID string
	userIDs []int64
	ttl time.Duration
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
	tx, err := a.postgresDB.Begin(ctx)
	if err != nil {
		return fmt.Errorf("Failed to start DB transaction: %s", err)
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `
					SELECT id, account_id, merchant_id, interaction, interval_since FROM fraud_requests
					WHERE timestamp_req < NOW() AND executed=FALSE
					ORDER BY timestamp_req`)

	var id uuid.UUID

	detReq := &common.DetectRequest{
		Payer: &common.PayerType{},
		Payee: &common.PayeeType{},
	}

	if err := row.Scan(&id, &detReq.Payer.AccountID, &detReq.Payee.MerchantID, &detReq.Interaction, &detReq.IntervalSince); err != nil {
		return fmt.Errorf("Failed to scan row: %s", err)
	}

	if detReq.IntervalSince == nil {
		startTime := time.Unix(0, 0)
		detReq.IntervalSince = &startTime
	}

	var toBan []int64

	log.Printf("Aggregating for %d", detReq.Payer.AccountID)

	chScores, err := a.aggrFromClickHouse(ctx, detReq)
	if err != nil {
		return fmt.Errorf("Failed to aggregate: %s", err)
	}

	log.Println(chScores)

	switch detReq.Interaction {
	case common.PayerInteraction:
		toBan = append(toBan, detReq.Payer.AccountID)
	case common.PayeeInteraction:
		toBan = append(toBan, detReq.Payee.MerchantID)
	case common.PersonalInteraction:
		toBan = append(toBan, detReq.Payer.AccountID, detReq.Payee.MerchantID)
	case common.GeneralInteraction:
		// toBan = append(toBan, )
	}

	_, err = tx.Exec(ctx, `UPDATE fraud_requests
								SET executed=TRUE
								WHERE id=$1
								`, id.String())
	if err != nil {
		return fmt.Errorf("Failed to update executed status: %s", err)
	}

	if chScores >= 40 {
		if err := a.banUsers(ctx, toBan); err != nil {
			log.Printf("Failed to ban user: %d error: %s", detReq.Payer.AccountID, err)
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("Failed to commit: %s", err)
	}

	return nil
}

func (a *AntiFraud) banUsers(ctx context.Context, userIDs []int64) error {
	t := &TwoPhaseBan{
		redisDB: a.redisDB,
		txID: fmt.Sprintf("%d", time.Now().UnixNano()),
		userIDs: userIDs,
		ttl: time.Minute * 5,
	}

	if err := t.prepareBan(ctx); err != nil{
		return fmt.Errorf("failed to prepare bans: %s", err)
	}

	//validator

	if err := t.commitBan(ctx); err != nil{
return fmt.Errorf("CRITICAL ERROR to commit bans failed consistence: %s", err)
	}

	log.Printf("Banned: %d", userIDs)

	return nil
}

func (t *TwoPhaseBan) prepareBan(ctx context.Context) error{
	pipeline := t.redisDB.Pipeline()

	for _, userID := range t.userIDs{
		//?????? int64 -> string
		key := fmt.Sprintf("ban:tx:%s:user:%d", t.txID, userID)

		pipeline.Set(ctx, key, "PENDING", t.ttl)
	}

	pipeline.Set(ctx, fmt.Sprintf("ban:tx:%s:status", t.txID), "PREPARED", t.ttl)

	_, err := pipeline.Exec(ctx)
	if err != nil{
		return err
	}

	return nil
}

func (t *TwoPhaseBan) commitBan(ctx context.Context) error{
	pipeline := t.redisDB.Pipeline()

	for _, userID := range t.userIDs {
		pipeline.Set(ctx, fmt.Sprintf("fraud:ban:%d", userID), "1", 0)

		tempKey := fmt.Sprintf("ban:tx:%s:user:%d", t.txID, userID)
		pipeline.Del(ctx, tempKey)
	}

	pipeline.Del(ctx, fmt.Sprintf("ban:tx:%s:status", t.txID))

	_, err := pipeline.Exec(ctx)
	if err != nil{
		return err
	}

	return nil
}

//rollback

