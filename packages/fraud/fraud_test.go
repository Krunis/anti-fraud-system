package fraud

import (
	"context"
	"reflect"
	"testing"

	"github.com/Krunis/anti-fraud-system/packages/interfaces/mocks"
	"go.uber.org/mock/gomock"
)

func TestAntiFraud_userIDsByDevices(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)

	devices := []string{"dev-1", "dev-2"}

	// --- rows для dev-1: один user ---
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

	// --- rows для dev-2: два user'а ---
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

	// --- ожидания вызовов Query, по порядку девайсов ---
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