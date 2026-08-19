package fraud

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Krunis/anti-fraud-system/packages/common"
	"github.com/Krunis/anti-fraud-system/packages/interfaces/mocks"
	"github.com/go-redis/redismock/v9"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"go.uber.org/mock/gomock"
)

func Test_userIDsByDevices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)

	devices := []string{"dev-1", "dev-2"}

	rowsDev1 := mocks.NewMockRows(ctrl)
	gomock.InOrder(
		rowsDev1.EXPECT().Next().Return(true),
		rowsDev1.EXPECT().Scan(gomock.Any()).DoAndReturn(func(dest ...any) error {
			*dest[0].(*string) = "user-1"
			return nil
		}),
		rowsDev1.EXPECT().Next().Return(false),
	)
	rowsDev1.EXPECT().Close()

	rowsDev2 := mocks.NewMockRows(ctrl)
	gomock.InOrder(
		rowsDev2.EXPECT().Next().Return(true),
		rowsDev2.EXPECT().Scan(gomock.Any()).DoAndReturn(func(dest ...any) error {
			*dest[0].(*string) = "user-2"
			return nil
		}),
		rowsDev2.EXPECT().Next().Return(true),
		rowsDev2.EXPECT().Scan(gomock.Any()).DoAndReturn(func(dest ...any) error {
			*dest[0].(*string) = "user-3"
			return nil
		}),
		rowsDev2.EXPECT().Next().Return(false),
	)
	rowsDev2.EXPECT().Close()

	gomock.InOrder(
		mockDB.EXPECT().
			Query(gomock.Any(), gomock.Any(), "dev-1").
			Return(rowsDev1, nil),
		mockDB.EXPECT().
			Query(gomock.Any(), gomock.Any(), "dev-2").
			Return(rowsDev2, nil),
	)

	af := &AntiFraud{postgresDB: mockDB}

	userIDs, err := af.userIDsByDevices(context.Background(), devices)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"user-1", "user-2", "user-3"}
	if !reflect.DeepEqual(userIDs, want) {
		t.Errorf("expected %v, got %v", want, userIDs)
	}
}

func Test_gettingRequestInTx(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	dbRedis, mockRedis := redismock.NewClientMock()

	mockCH := mocks.NewMockCH(ctrl)
	mockCHRows := mocks.NewMockCHRows(ctrl)

	mockDB := mocks.NewMockDB(ctrl)
	mockTx := mocks.NewMockTx(ctrl)
	mockRow := mocks.NewMockRow(ctrl)
	mockRows := mocks.NewMockRows(ctrl)

	mockRow.EXPECT().Scan(gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any()).DoAndReturn(func(dest ...any) error {
		*dest[0].(*uuid.UUID) = uuid.MustParse("bfb4c2d1-2693-4c5e-9ea1-4639c5988829")
		*dest[1].(*string) = "123"
		*dest[2].(*string) = "321"
		*dest[3].(*common.InteractionType) = common.GeneralInteraction
		val := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
		*dest[4].(**time.Time) = &val
		return nil
	})

	gomock.InOrder(mockTx.EXPECT().QueryRow(
		gomock.Any(),
		gomock.Any(),
		gomock.Any()).Return(mockRow),
		mockTx.EXPECT().Commit(gomock.Any()).Return(nil),
		mockTx.EXPECT().Rollback(gomock.Any()).Return(pgx.ErrTxClosed))

	mockDB.EXPECT().Begin(gomock.Any()).Return(mockTx, nil)

	gomock.InOrder(
		mockCHRows.EXPECT().Next().Return(true),
		mockCHRows.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
			func(dest ...any) error {
				*dest[0].(*string) = "321"
				*dest[1].(*int) = 4
				*dest[2].(*int) = 3
				return nil
			}),
		mockCHRows.EXPECT().Next().Return(false),
		mockCHRows.EXPECT().Err().Return(nil),
	)
	mockCHRows.EXPECT().Close().Return(nil)
	mockCH.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockCHRows, nil)

	gomock.InOrder(
		mockRows.EXPECT().Next().Return(true),
		mockRows.EXPECT().Scan(gomock.Any()).DoAndReturn(func(dest ...any) error {
			*dest[0].(*string) = "123"
			return nil
		}),
		mockRows.EXPECT().Next().Return(false),
		mockRows.EXPECT().Close(),
	)

	mockDB.EXPECT().Query(gomock.Any(), gomock.Any(), gomock.Any()).Return(mockRows, nil)

	mockRedis.ExpectSet(fmt.Sprintf("ban:tx:%s:user:%s", "1", "123"), "PENDING", time.Minute * 5).SetVal("OK")

	af := &AntiFraud{postgresDB: mockDB, clickHouse: mockCH, redisDB: &common.Redis{Client: dbRedis}}

	err := af.detectAndBan(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mockRedis.ExpectationsWereMet(); err != nil {
		t.Fatal(err)

	}
}
