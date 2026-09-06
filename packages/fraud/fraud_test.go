package fraud

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/Krunis/anti-fraud-system/packages/common"
	"github.com/Krunis/anti-fraud-system/packages/interfaces/mocks"
	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redismock/v9"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func Test_pollerToClickHouse(t *testing.T) {
	tests := []struct {
		name          string
		setupMocks    func(*mocks.MockCH, *mocks.MockCHBatch)
		sendEvent     bool
		closeChannel  bool
		cancelContext bool
		waitTimer     bool
	}{
		{
			name: "by ctx",
			setupMocks: func(mockCH *mocks.MockCH, mockCHBatch *mocks.MockCHBatch) {
				mockCH.EXPECT().PrepareBatch(gomock.Any(), gomock.Any()).Return(mockCHBatch, nil)
				gomock.InOrder(
					mockCHBatch.EXPECT().Append(gomock.Any(), gomock.Any(), gomock.Any(),
						gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
						gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
					mockCHBatch.EXPECT().Send().Return(nil),
					mockCHBatch.EXPECT().Close().Return(nil),
				)
			},
			sendEvent:     true,
			closeChannel:  false,
			cancelContext: true,
		},
		{
			name: "by timer",
			setupMocks: func(mockCH *mocks.MockCH, mockCHBatch *mocks.MockCHBatch) {
				mockCH.EXPECT().PrepareBatch(gomock.Any(), gomock.Any()).Return(mockCHBatch, nil)
				gomock.InOrder(
					mockCHBatch.EXPECT().Append(gomock.Any(), gomock.Any(), gomock.Any(),
						gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
						gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
					mockCHBatch.EXPECT().Send().Return(nil),
					mockCHBatch.EXPECT().Close().Return(nil),
				)
			},
			sendEvent:     true,
			closeChannel:  false,
			cancelContext: true,
			waitTimer:     true,
		},
		{
			name:          "by timer empty",
			setupMocks:    func(mockCH *mocks.MockCH, mockCHBatch *mocks.MockCHBatch) {},
			sendEvent:     false,
			closeChannel:  false,
			cancelContext: true,
			waitTimer:     true,
		},
		{
			name: "preparebatch fail",
			setupMocks: func(mockCH *mocks.MockCH, mockCHBatch *mocks.MockCHBatch) {
				mockCH.EXPECT().PrepareBatch(gomock.Any(), gomock.Any()).Return(nil, errors.New("prepare failed"))
			},
			sendEvent:     true,
			closeChannel:  false,
			cancelContext: true,
		},
		{
			name: "appendbatch fail",
			setupMocks: func(mockCH *mocks.MockCH, mockCHBatch *mocks.MockCHBatch) {
				mockCH.EXPECT().PrepareBatch(gomock.Any(), gomock.Any()).Return(mockCHBatch, nil)
				gomock.InOrder(
					mockCHBatch.EXPECT().Append(gomock.Any(), gomock.Any(), gomock.Any(),
						gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
						gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("append failed")),
					mockCHBatch.EXPECT().Close().Return(nil),
				)
			},
			sendEvent:     true,
			closeChannel:  false,
			cancelContext: true,
		},
		{
			name: "!ok from chan in batch",
			setupMocks: func(mockCH *mocks.MockCH, mockCHBatch *mocks.MockCHBatch) {
				mockCH.EXPECT().PrepareBatch(gomock.Any(), gomock.Any()).Return(mockCHBatch, nil)
				gomock.InOrder(
					mockCHBatch.EXPECT().Append(gomock.Any(), gomock.Any(), gomock.Any(),
						gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
						gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil),
					mockCHBatch.EXPECT().Send().Return(nil),
					mockCHBatch.EXPECT().Close().Return(nil),
				)
			},
			sendEvent:     true,
			closeChannel:  true,
			cancelContext: false,
		},
		{
			name:          "!ok from chan in nil batch",
			setupMocks:    func(mockCH *mocks.MockCH, mockCHBatch *mocks.MockCHBatch) {},
			sendEvent:     false,
			closeChannel:  true,
			cancelContext: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)

			mockCH := mocks.NewMockCH(ctrl)
			mockCHBatch := mocks.NewMockCHBatch(ctrl)

			ctx, cancel := context.WithCancel(context.Background())

			af := &AntiFraud{
				clickHouse: mockCH,
				paymentCh:  make(chan *common.PaymentEvent, 1000),
				lifecycle: common.Lifecycle{
					Ctx:    ctx,
					Cancel: cancel,
				},
			}

			tt.setupMocks(mockCH, mockCHBatch)

			af.wg.Go(af.pollerToClickHouse)

			if tt.sendEvent {
				af.paymentCh <- createTestPaymentEvent()
			}

			if tt.closeChannel {
				close(af.paymentCh)
			}

			if tt.waitTimer {
				time.Sleep(time.Second * 6)
			}

			if tt.cancelContext {
				time.Sleep(time.Millisecond * 100)
				af.lifecycle.Cancel()
			}

			done := make(chan struct{})
			go func() {
				af.wg.Wait()
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(1 * time.Second):
				t.Fatal("pollerToClickHouse did not stop after context cancel")
			}
		})
	}

}
func Test_fetchNextRequest(t *testing.T) {
	t.Run("success with interval", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockDB := mocks.NewMockPostgresDB(ctrl)
		mockTx := mocks.NewMockTx(ctrl)
		mockRow := mocks.NewMockRow(ctrl)

		mockRow.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(dest ...any) error {
				*dest[1].(*string) = "123"
				*dest[2].(*string) = "321"
				*dest[3].(*common.InteractionType) = common.PayerInteraction
				return nil
			})

		gomock.InOrder(
			mockTx.EXPECT().QueryRow(gomock.Any(), gomock.Any()).Return(mockRow),
			mockTx.EXPECT().Commit(gomock.Any()).Return(nil),
			mockTx.EXPECT().Rollback(gomock.Any()).Return(pgx.ErrTxClosed),
		)
		mockDB.EXPECT().Begin(gomock.Any()).Return(mockTx, nil)

		af := &AntiFraud{postgresDB: mockDB}
		req, err := af.fetchNextRequest(context.Background())

		startTime := time.Unix(0, 0)

		require.WithinDuration(t, startTime, *req.IntervalSince, time.Second)
		require.NoError(t, err)
		require.Equal(t, "123", req.Payer.AccountID)
	})

	t.Run("success w/o interval", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockDB := mocks.NewMockPostgresDB(ctrl)
		mockTx := mocks.NewMockTx(ctrl)
		mockRow := mocks.NewMockRow(ctrl)

		mockRow.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(dest ...any) error {
				*dest[1].(*string) = "123"
				*dest[2].(*string) = "321"
				*dest[3].(*common.InteractionType) = common.PayerInteraction
				*dest[4].(**time.Time) = &time.Time{}
				return nil
			})

		gomock.InOrder(
			mockTx.EXPECT().QueryRow(gomock.Any(), gomock.Any()).Return(mockRow),
			mockTx.EXPECT().Commit(gomock.Any()).Return(nil),
			mockTx.EXPECT().Rollback(gomock.Any()).Return(pgx.ErrTxClosed),
		)
		mockDB.EXPECT().Begin(gomock.Any()).Return(mockTx, nil)

		af := &AntiFraud{postgresDB: mockDB}
		req, err := af.fetchNextRequest(context.Background())

		require.NotNil(t, req.IntervalSince)
		require.NoError(t, err)
		require.Equal(t, "123", req.Payer.AccountID)
	})

	t.Run("begintx fail", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockDB := mocks.NewMockPostgresDB(ctrl)

		mockDB.EXPECT().Begin(gomock.Any()).Return(nil, errors.New("connect to DB failed"))

		af := &AntiFraud{postgresDB: mockDB}

		req, err := af.fetchNextRequest(context.Background())

		assert.Nil(t, req)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "connect to DB failed")
	})

	tests := []struct {
		name    string
		scanErr error
		retErr  error
	}{
		{
			name:    "scan fail noRows",
			scanErr: pgx.ErrNoRows,
			retErr:  nil,
		},
		{
			name:    "scan fail some error",
			scanErr: errors.New("some err"),
			retErr:  fmt.Errorf("Failed to update and fetch row: %s", "some err"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockDB := mocks.NewMockPostgresDB(ctrl)
			mockTx := mocks.NewMockTx(ctrl)
			mockRow := mocks.NewMockRow(ctrl)

			mockRow.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(tt.scanErr)

			mockTx.EXPECT().QueryRow(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRow)
			mockTx.EXPECT().Rollback(gomock.Any()).Return(nil)

			mockDB.EXPECT().Begin(gomock.Any()).Return(mockTx, nil)

			af := &AntiFraud{postgresDB: mockDB}

			req, err := af.fetchNextRequest(context.Background())

			require.Nil(t, req)
			require.Equal(t, err, tt.retErr)
		})
	}

	t.Run("commit fail", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		mockDB := mocks.NewMockPostgresDB(ctrl)
		mockTx := mocks.NewMockTx(ctrl)
		mockRow := mocks.NewMockRow(ctrl)

		mockRow.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

		mockTx.EXPECT().Commit(gomock.Any()).Return(errors.New("some err"))
		mockTx.EXPECT().QueryRow(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRow)
		mockTx.EXPECT().Rollback(gomock.Any()).Return(nil)

		mockDB.EXPECT().Begin(gomock.Any()).Return(mockTx, nil)

		af := &AntiFraud{postgresDB: mockDB}

		req, err := af.fetchNextRequest(context.Background())

		require.Nil(t, req)
		require.Contains(t, err.Error(), "some err")
	})

}

func Test_aggrFromClickhouse_PayerInteraction(t *testing.T) {
	tests := []struct {
		name      string
		scanErr   error
		wantScore int
		wantErr   bool
	}{
		{
			name:      "success",
			scanErr:   nil,
			wantScore: 60,
			wantErr:   false,
		},
		{
			name:      "norows error",
			scanErr:   sql.ErrNoRows,
			wantScore: 0,
			wantErr:   false,
		},
		{
			name:      "scan error",
			scanErr:   errors.New("db connection lost"),
			wantScore: 0,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockCH := mocks.NewMockCH(ctrl)
			mockCHRow := mocks.NewMockCHRow(ctrl)

			mockCHRow.EXPECT().Scan(gomock.Any(), gomock.Any()).DoAndReturn(
				func(dest ...any) error {
					if tt.scanErr != nil {
						return tt.scanErr
					}

					*dest[0].(*int) = 4
					*dest[1].(*int) = 11
					return nil
				})

			mockCH.EXPECT().QueryRow(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockCHRow)

			af := &AntiFraud{clickHouse: mockCH}
			detReq := &common.DetectRequest{
				Interaction:   common.PayerInteraction,
				Payer:         &common.PayerType{AccountID: "acc1"},
				IntervalSince: nil,
			}

			result, err := af.aggrFromClickHouse(context.Background(), detReq)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantScore, result.Score)
		})
	}
}

func Test_aggrFromClickhouse_PayeeInteraction(t *testing.T) {
	tests := []struct {
		name      string
		scanErr   error
		wantScore int
		wantErr   bool
	}{
		{
			name:      "success",
			scanErr:   nil,
			wantScore: 30,
			wantErr:   false,
		},
		{
			name:      "norows error",
			scanErr:   sql.ErrNoRows,
			wantScore: 0,
			wantErr:   false,
		},
		{
			name:      "scan error",
			scanErr:   errors.New("db connection lost"),
			wantScore: 0,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockCH := mocks.NewMockCH(ctrl)
			mockCHRow := mocks.NewMockCHRow(ctrl)

			mockCHRow.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(dest ...any) error {
					if tt.scanErr != nil {
						return tt.scanErr
					}

					*dest[0].(*string) = "1cca"
					*dest[1].(*string) = "bank"
					*dest[2].(*uint64) = 10
					*dest[3].(*uint64) = 4
					return nil
				})

			mockCH.EXPECT().QueryRow(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockCHRow)

			af := &AntiFraud{clickHouse: mockCH}
			detReq := &common.DetectRequest{
				Interaction:   common.PayeeInteraction,
				Payee:         &common.PayeeType{MerchantID: "1cca"},
				IntervalSince: nil,
			}

			result, err := af.aggrFromClickHouse(context.Background(), detReq)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantScore, result.Score)
		})
	}
}

func Test_aggrFromClickhouse_PersonalInteraction(t *testing.T) {
	tests := []struct {
		name      string
		scanErr   error
		wantScore int
		wantErr   bool
	}{
		{
			name:      "success",
			scanErr:   nil,
			wantScore: 100,
			wantErr:   false,
		},
		{
			name:      "norows error",
			scanErr:   sql.ErrNoRows,
			wantScore: 0,
			wantErr:   false,
		},
		{
			name:      "scan error",
			scanErr:   errors.New("db connection lost"),
			wantScore: 0,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockCH := mocks.NewMockCH(ctrl)
			mockCHRow := mocks.NewMockCHRow(ctrl)

			mockCHRow.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(dest ...any) error {
					if tt.scanErr != nil {
						return tt.scanErr
					}

					*dest[0].(*uint64) = 30
					*dest[1].(*float64) = 4000000
					*dest[2].(*float64) = 10000000
					*dest[3].(*uint64) = 6
					*dest[4].(*time.Duration) = 1
					*dest[5].(*float64) = 3
					return nil
				})

			mockCH.EXPECT().QueryRow(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockCHRow)

			af := &AntiFraud{clickHouse: mockCH}
			detReq := &common.DetectRequest{
				Interaction:   common.PersonalInteraction,
				Payer:         &common.PayerType{AccountID: "acc1"},
				Payee:         &common.PayeeType{MerchantID: "1cca"},
				IntervalSince: nil,
			}

			result, err := af.aggrFromClickHouse(context.Background(), detReq)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantScore, result.Score)
		})
	}
}

type generalScanRow struct {
	deviceID   string
	accountIDs []string
	uniqIPs    int
}

func setupCHRowsMock(ctrl *gomock.Controller, rows []generalScanRow, rowsErr error) *mocks.MockCHRows {
	mockCHRows := mocks.NewMockCHRows(ctrl)

	calls := make([]*gomock.Call, 0, len(rows)*2+2)

	for _, r := range rows {
		calls = append(calls,
			mockCHRows.EXPECT().Next().Return(true),
			mockCHRows.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
				func(dest ...any) error {
					*dest[0].(*string) = r.deviceID
					*dest[1].(*[]string) = r.accountIDs
					*dest[2].(*int) = r.uniqIPs
					return nil
				}),
		)
	}

	calls = append(calls, mockCHRows.EXPECT().Next().Return(false))
	calls = append(calls, mockCHRows.EXPECT().Err().Return(rowsErr))

	gomock.InOrder(calls...)

	mockCHRows.EXPECT().Close().Return(nil)

	return mockCHRows
}

func Test_aggrFromClickhouse_GeneralInteraction(t *testing.T) {
	tests := []struct {
		name      string
		rows      []generalScanRow
		rowsErr   error
		wantScore int
		wantErr   bool
	}{
		{
			name: "single row",
			rows: []generalScanRow{
				{deviceID: "dev1", accountIDs: []string{"123", "324"}, uniqIPs: 3},
			},
			rowsErr:   nil,
			wantScore: 100,
			wantErr:   false,
		},
		{
			name: "multiple rows",
			rows: []generalScanRow{
				{deviceID: "dev1", accountIDs: []string{"123", "324"}, uniqIPs: 3},
				{deviceID: "dev2", accountIDs: []string{"123", "324"}, uniqIPs: 4},
			},
			rowsErr:   nil,
			wantScore: 100,
			wantErr:   false,
		},
		{
			name:      "w/o rows",
			rowsErr:   nil,
			wantScore: 0,
			wantErr:   false,
		},
		{
			name: "some error with row",
			rows: []generalScanRow{
				{deviceID: "dev1", accountIDs: []string{"123", "324"}, uniqIPs: 3},
			},
			rowsErr:   errors.New("some err"),
			wantScore: 0,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockCH := mocks.NewMockCH(ctrl)
			mockCHRows := setupCHRowsMock(ctrl, tt.rows, tt.rowsErr)

			mockCH.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockCHRows, nil)

			af := &AntiFraud{clickHouse: mockCH}
			detReq := &common.DetectRequest{
				Interaction:   common.GeneralInteraction,
				IntervalSince: nil,
			}

			result, err := af.aggrFromClickHouse(context.Background(), detReq)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantScore, result.Score)

		})
	}
}

func Test_aggrFromClickHouse_unknownInteraction(t *testing.T) {
	af := &AntiFraud{clickHouse: mocks.NewMockCH(gomock.NewController(t))}
	detReq := &common.DetectRequest{Interaction: "unknown"}

	result, err := af.aggrFromClickHouse(context.Background(), detReq)

	require.Error(t, err)
	require.Nil(t, result)
}

func Test_banTargets_withBanDetails(t *testing.T) {
	detReq := &common.DetectRequest{Interaction: common.GeneralInteraction}
	checkResult := &FraudCheckResult{
		BanDetails: []*BanDetail{{Targets: []string{"123"}, Duration: 15 * time.Minute}},
	}

	got := banTargets(detReq, checkResult)

	require.Equal(t, []banTarget{{UserIDs: []string{"123"}, Duration: 15 * time.Minute}}, got)
}

func Test_banTargets_payerFallback(t *testing.T) {
	detReq := &common.DetectRequest{
		Interaction: common.PayerInteraction,
		Payer:       &common.PayerType{AccountID: "acc1"},
	}
	checkResult := &FraudCheckResult{}

	got := banTargets(detReq, checkResult)

	require.Equal(t, []banTarget{{UserIDs: []string{"acc1"}, Duration: 15 * time.Minute}}, got)
}

func Test_banTargets_payeeFallback(t *testing.T) {
	detReq := &common.DetectRequest{
		Interaction: common.PayeeInteraction,
		Payee:       &common.PayeeType{MerchantID: "1cca"},
	}
	checkResult := &FraudCheckResult{}

	got := banTargets(detReq, checkResult)

	require.Equal(t, []banTarget{{UserIDs: []string{"1cca"}, Duration: 15 * time.Minute}}, got)
}

func Test_banTargets_withoutUserIDs(t *testing.T) {
	detReq := &common.DetectRequest{}
	checkResult := &FraudCheckResult{}

	got := banTargets(detReq, checkResult)

	var nilTarget []banTarget

	require.Equal(t, nilTarget, got)
}

func Test_prepareBan(t *testing.T) {
	// t.Run("set")
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dbRedis, mockRedis := redismock.NewClientMock()

	tpb := &TwoPhaseBan{
		redisDB: &common.Redis{Client: dbRedis},
		txID:    uuid.New().String(),
		userIDs: []string{"alice", "bob"},
		ttl:     time.Minute * 5,
	}

	mockRedis.ExpectSet(fmt.Sprintf("ban:tx:%s:user:%s", tpb.txID, tpb.userIDs[0]), "PENDING", tpb.ttl).SetVal("OK")
	mockRedis.ExpectSet(fmt.Sprintf("ban:tx:%s:user:%s", tpb.txID, tpb.userIDs[1]), "PENDING", tpb.ttl).SetErr(errors.New("redis connection lost"))

	err := tpb.prepareBan(context.Background())

	require.NoError(t, mockRedis.ExpectationsWereMet())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "redis connection lost")
}

func Test_validationFail_rollbackBans(t *testing.T) {
	t.Run("rollback success", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dbRedis, mockRedis := redismock.NewClientMock()
		tpb := &TwoPhaseBan{
			redisDB: &common.Redis{Client: dbRedis},
			txID:    uuid.New().String(),
			userIDs: []string{"alice", "bob", "charlie"},
			ttl:     time.Minute * 5,
		}

		for _, userID := range tpb.userIDs {
			mockRedis.ExpectDel(fmt.Sprintf("ban:tx:%s:user:%s", tpb.txID, userID)).SetVal(1)
		}

		mockRedis.ExpectDel(fmt.Sprintf("ban:tx:%s:status", tpb.txID)).SetVal(1)

		err := tpb.validateAndRollback(context.Background())

		require.NoError(t, mockRedis.ExpectationsWereMet())
		require.NoError(t, err)
	})

	t.Run("rollback failed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		dbRedis, mockRedis := redismock.NewClientMock()
		tpb := &TwoPhaseBan{
			redisDB: &common.Redis{Client: dbRedis},
			txID:    uuid.New().String(),
			userIDs: []string{"alice", "bob", "charlie"},
			ttl:     time.Minute * 5,
		}

		mockRedis.ExpectDel(fmt.Sprintf("ban:tx:%s:user:%s", tpb.txID, tpb.userIDs[0])).SetErr(errors.New("redis connection lost"))

		err := tpb.validateAndRollback(context.Background())

		require.NoError(t, mockRedis.ExpectationsWereMet())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "redis connection lost")
	})
}

func Test_validationSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dbRedis, mockRedis := redismock.NewClientMock()

	tpb := &TwoPhaseBan{
		redisDB: &common.Redis{Client: dbRedis},
		userIDs: []string{"alice", "bob"},
	}

	err := tpb.validateAndRollback(context.Background())

	require.NoError(t, err)
	require.NoError(t, mockRedis.ExpectationsWereMet())
}

func Test_Integration_commitBan_Prepared(t *testing.T) {
	s := miniredis.RunT(t)

	rdb := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	tpb := &TwoPhaseBan{
		redisDB: &common.Redis{
			Client: rdb,
		},
		txID:    uuid.NewString(),
		userIDs: []string{"alice", "bob"},
		banDur:  time.Minute * 5,
	}

	for _, userID := range tpb.userIDs {
		s.Set(fmt.Sprintf("ban:tx:%s:user:%s", tpb.txID, userID), "PENDING")
	}

	statusKey := fmt.Sprintf("ban:tx:%s:status", tpb.txID)

	s.Set(statusKey, "PREPARED")

	err := tpb.commitBan(context.Background())
	require.NoError(t, err)

	for _, userID := range tpb.userIDs {
		res, _ := s.Get(fmt.Sprintf("fraud:ban:%s", userID))
		require.Equal(t, "1", res)
	}

	for _, userID := range tpb.userIDs {
		require.False(t, s.Exists(fmt.Sprintf("ban:tx:%s:user:%s", tpb.txID, userID)))
	}

	require.False(t, s.Exists(statusKey))
}

func Test_Integration_commitBan_NotPrepared(t *testing.T) {
    s := miniredis.RunT(t)

    rdb := redis.NewClient(&redis.Options{
        Addr: s.Addr(),
    })

    tpb := &TwoPhaseBan{
        redisDB: &common.Redis{
            Client: rdb,
        },
        txID:   uuid.NewString(),
        userIDs: []string{"user-1"},
        banDur:  time.Minute * 5,
    }

    err := tpb.commitBan(context.Background())

    require.Error(t, err)
    assert.ErrorContains(t, err, "commit bans")
    require.ErrorContains(t, err, "transaction is not prepared")

    exists, err := rdb.Exists(
        context.Background(),
        fmt.Sprintf("fraud:ban:%s", tpb.userIDs[0]),
    ).Result()

    require.NoError(t, err)
    require.Equal(t, int64(0), exists)
}

func createTestPaymentEvent() *common.PaymentEvent {
	return &common.PaymentEvent{
		EventID:   "test-123",
		EventTime: "234",
		Direction: "incoming",
		Transaction: &common.TransactionType{
			Amount:   100,
			Currency: "USD",
			Type:     "payment",
		},
		Payer: &common.PayerType{
			AccountID: "acc-123",
		},
		Payee: &common.PayeeType{
			MerchantID:   "merch-123",
			MerchantName: "Test Merchant",
			Country:      "US",
		},
		Context: &common.ContextData{
			Channel:   "web",
			DeviceID:  "device-123",
			IP:        "192.168.1.1",
			UserAgent: "Mozilla/5.0",
		},
	}
}

func Test_Integration_banByUserIDs(t *testing.T) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}

	redisContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	defer func() {
		if err := redisContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %s", err)
		}
	}()

	host, err := redisContainer.Host(ctx)
	require.NoError(t, err)

	port, err := redisContainer.MappedPort(ctx, "6379")
	require.NoError(t, err)

	addr := fmt.Sprintf("%s:%s", host, port.Port())

	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	defer rdb.Close()

	af := &AntiFraud{
		redisDB: &common.Redis{Client: rdb},
	}

	af.banner = af

	userIDs := []string{"alice", "bob"}
	banDuration := 10 * time.Minute
	txID := uuid.New()

	err = af.banner.BanByUserIDs(ctx, userIDs, banDuration, txID)
	require.NoError(t, err)

	for _, userID := range userIDs {
		key := "fraud:ban:" + userID

		val, err := rdb.Get(ctx, key).Result()
		assert.NoError(t, err)
		assert.Equal(t, "1", val)

		ttl, err := rdb.TTL(ctx, key).Result()
		assert.NoError(t, err)
		assert.True(t, ttl > 5*time.Minute && ttl <= 10*time.Minute)
	}

	for _, userID := range userIDs {
		tempKey := "ban:tx:" + txID.String() + ":user:" + userID
		exists, err := rdb.Exists(ctx, tempKey).Result()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), exists)
	}

	statusKey := "ban:tx:" + txID.String() + ":status"
	exists, err := rdb.Exists(ctx, statusKey).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), exists)
}

func Test_handleConsume(t *testing.T){
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockBanner := mocks.NewMockBanner(ctrl)

	dbRedis, mockRedis := redismock.NewClientMock()

	af := AntiFraud{
		banner : mockBanner,
		redisDB: &common.Redis{Client: dbRedis},
		paymentCh: make(chan *common.PaymentEvent, 1000),
	}

	ctx := context.Background()
	payment := createTestPaymentEvent()

	mockRedis.ExpectIncr(fmt.Sprintf("fraud:%s%s", prefixFailedLogins, payment.Payer.AccountID)).SetVal(1)
	mockRedis.ExpectExpire(fmt.Sprintf("fraud:%s%s", prefixFailedLogins, payment.Payer.AccountID), time.Second*30).SetVal(true)

	mockRedis.ExpectSAdd(fmt.Sprintf("fraud:%s%s", prefixPaymentCountries, payment.Payer.AccountID), payment.Context.IP).SetVal(1)
	mockRedis.ExpectExpire(fmt.Sprintf("fraud:%s%s", prefixPaymentCountries, payment.Payer.AccountID), time.Minute * 10).SetVal(true)

	mockRedis.ExpectSAdd(fmt.Sprintf("fraud:%s%s", prefixPaymentDevices, payment.Payer.AccountID), payment.Context.DeviceID).SetVal(1)
	mockRedis.ExpectExpire(fmt.Sprintf("fraud:%s%s", prefixPaymentDevices, payment.Payer.AccountID), time.Minute*5).SetVal(true)

	mockRedis.ExpectLPush(fmt.Sprintf("fraud:%s%s", prefixPaymentAmounts, payment.Payer.AccountID), payment.Transaction.Amount).SetVal(1)
	mockRedis.ExpectExpire(fmt.Sprintf("fraud:%s%s", prefixPaymentAmounts, payment.Payer.AccountID), time.Minute*5).SetVal(true)

	mockRedis.ExpectGet(fmt.Sprintf("fraud:%s%s", prefixFailedLogins, payment.Payer.AccountID)).SetVal("6")

	mockRedis.ExpectSMembers(fmt.Sprintf("fraud:%s%s", prefixPaymentCountries, payment.Payer.AccountID)).SetVal([]string{"a", "b", "c", "d"})

	mockRedis.ExpectSMembers(fmt.Sprintf("fraud:%s%s", prefixPaymentDevices, payment.Payer.AccountID)).SetVal([]string{"a", "b", "c", "d"})

	mockRedis.ExpectLRange(fmt.Sprintf("fraud:%s%s", prefixPaymentAmounts, payment.Payer.AccountID), 0, -1).SetVal([]string{"5000000", "5000000", "5000000", "5000000"})

	mockBanner.EXPECT().BanByUserIDs(ctx, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	value, err := json.Marshal(payment)
	require.NoError(t, err)

	err = af.handleConsume(ctx, &sarama.ConsumerMessage{Value: value})

	require.NoError(t, mockRedis.ExpectationsWereMet())
	require.NoError(t, err)
}