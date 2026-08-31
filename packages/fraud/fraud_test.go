package fraud

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Krunis/anti-fraud-system/packages/common"
	"github.com/Krunis/anti-fraud-system/packages/interfaces/mocks"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/mock/gomock"
	"github.com/go-redis/redismock/v9"
)

// func Test_detectAndBan(t *testing.T) {
// 	ctrl := gomock.NewController(t)
// 	defer ctrl.Finish()

// 	dbRedis, mockRedis := redismock.NewClientMock()

// 	mockCH := mocks.NewMockCH(ctrl)
// 	mockCHRows := mocks.NewMockCHRows(ctrl)

// 	mockDB := mocks.NewMockDB(ctrl)
// 	mockTx := mocks.NewMockTx(ctrl)
// 	mockRow := mocks.NewMockRow(ctrl)
// 	mockRows := mocks.NewMockRows(ctrl)

// 	mockRow.EXPECT().Scan(gomock.Any(),
// 		gomock.Any(),
// 		gomock.Any(),
// 		gomock.Any(),
// 		gomock.Any()).DoAndReturn(func(dest ...any) error {
// 		*dest[0].(*uuid.UUID) = uuid.MustParse("bfb4c2d1-2693-4c5e-9ea1-4639c5988829")
// 		*dest[1].(*string) = "123"
// 		*dest[2].(*string) = "321"
// 		*dest[3].(*common.InteractionType) = common.GeneralInteraction
// 		val := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
// 		*dest[4].(**time.Time) = &val
// 		return nil
// 	})

// 	gomock.InOrder(mockTx.EXPECT().QueryRow(
// 		gomock.Any(),
// 		gomock.Any(),
// 		gomock.Any()).Return(mockRow),
// 		mockTx.EXPECT().Commit(gomock.Any()).Return(nil),
// 		mockTx.EXPECT().Rollback(gomock.Any()).Return(pgx.ErrTxClosed))

// 	mockDB.EXPECT().Begin(gomock.Any()).Return(mockTx, nil)

// 	gomock.InOrder(
// 		mockCHRows.EXPECT().Next().Return(true),
// 		mockCHRows.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
// 			func(dest ...any) error {
// 				*dest[0].(*string) = "321"
// 				*dest[1].(*int) = 4
// 				*dest[2].(*int) = 3
// 				return nil
// 			}),
// 		mockCHRows.EXPECT().Next().Return(false),
// 		mockCHRows.EXPECT().Err().Return(nil),
// 	)
// 	mockCHRows.EXPECT().Close().Return(nil)
// 	mockCH.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockCHRows, nil)

// 	gomock.InOrder(
// 		mockRows.EXPECT().Next().Return(true),
// 		mockRows.EXPECT().Scan(gomock.Any()).DoAndReturn(func(dest ...any) error {
// 			*dest[0].(*string) = "123"
// 			return nil
// 		}),
// 		mockRows.EXPECT().Next().Return(false),
// 		mockRows.EXPECT().Close(),
// 	)

// 	mockDB.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRows, nil)

// 	mockRedis.ExpectSet(fmt.Sprintf("ban:tx:%s:user:%s", "1", "123"), "PENDING", time.Minute*5).SetVal("OK")
// 	mockRedis.ExpectSet(fmt.Sprintf("ban:tx:%s:status", "1"), "PREPARED", time.Minute*5).SetVal("OK")
// 	mockRedis.ExpectSet(fmt.Sprintf("fraud:ban:%s", "123"), "1", time.Minute*15).SetVal("OK")
// 	mockRedis.ExpectDel(fmt.Sprintf("ban:tx:%s:user:%s", "1", "123")).SetVal(1)
// 	mockRedis.ExpectDel(fmt.Sprintf("ban:tx:%s:status", "1")).SetVal(1)

// 	af := &AntiFraud{postgresDB: mockDB, clickHouse: mockCH, redisDB: &common.Redis{Client: dbRedis}}

// 	err := af.detectAndBan(context.Background())
// 	if err != nil {
// 		t.Fatalf("unexpected error: %v", err)
// 	}

// 	if err := mockRedis.ExpectationsWereMet(); err != nil {
// 		t.Fatal(err)

// 	}
// }

func Test_fetchNextRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockDB := mocks.NewMockDB(ctrl)
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

	require.NoError(t, err)
	require.Equal(t, "123", req.Payer.AccountID)

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

	anyCalls := make([]any, len(calls))
	for i, c := range calls {
		anyCalls[i] = c
	}

	gomock.InOrder(anyCalls...)

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

func Test_prepareBan_execError(t *testing.T){
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dbRedis, mockRedis := redismock.NewClientMock()

	tpb := &TwoPhaseBan{
		redisDB: &common.Redis{Client: dbRedis},
		txID: uuid.New().String(),
		userIDs: []string{"alice", "bob"},
		ttl: time.Minute * 5,
	}

	mockRedis.ExpectSet(fmt.Sprintf("ban:tx:%s:user:%s", tpb.txID, tpb.userIDs[0]), "PENDING", tpb.ttl).SetVal("OK")
	mockRedis.ExpectSet(fmt.Sprintf("ban:tx:%s:user:%s", tpb.txID, tpb.userIDs[1]), "PENDING", tpb.ttl).SetErr(errors.New("redis connection lost"))
	mockRedis.ExpectSet(fmt.Sprintf("ban:tx:%s:status", "1"), "PREPARED", tpb.ttl).SetVal("OK")
	
	err := tpb.prepareBan(context.Background())
    
    require.Error(t, err)
    assert.Contains(t, err.Error(), "redis connection lost")
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

	userIDs := []string{"alice", "bob"}
	banDuration := 10 * time.Minute
	txID := uuid.New()

	err = af.banByUserIDs(ctx, userIDs, banDuration, txID)
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
