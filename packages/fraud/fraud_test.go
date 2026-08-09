package fraud

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Krunis/anti-fraud-system/packages/common"
	"github.com/go-redis/redis/v8"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// ==================== MOCKS ====================

// MockPostgresPool - мок для пула соединений PostgreSQL
type MockPostgresPool struct {
	mock.Mock
}

func (m *MockPostgresPool) Begin(ctx context.Context) (pgx.Tx, error) {
	args := m.Called(ctx)
	return args.Get(0).(pgx.Tx), args.Error(1)
}

func (m *MockPostgresPool) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	mockedArgs := m.Called(ctx, sql, args)
	return mockedArgs.Get(0).(pgx.Rows), mockedArgs.Error(1)
}

func (m *MockPostgresPool) Close() {}

// MockTx - мок для транзакции PostgreSQL
type MockTx struct {
	mock.Mock
}

func (m *MockTx) Begin(ctx context.Context) (pgx.Tx, error) {
	args := m.Called(ctx)
	return args.Get(0).(pgx.Tx), args.Error(1)
}

func (m *MockTx) Commit(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockTx) Rollback(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockTx) QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	mockedArgs := m.Called(ctx, sql, args)
	return mockedArgs.Get(0).(pgx.Row)
}

func (m *MockTx) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	mockedArgs := m.Called(ctx, sql, args)
	return mockedArgs.Get(0).(pgx.Rows), mockedArgs.Error(1)
}

func (m *MockTx) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	args := m.Called(ctx, sql, arguments)
	return args.Get(0).(pgconn.CommandTag), args.Error(1)
}

// MockRows - мок для строк результата
type MockRows struct {
	mock.Mock
	data [][]interface{}
	idx  int
}

func NewMockRows(data [][]interface{}) *MockRows {
	return &MockRows{data: data, idx: -1}
}

func (m *MockRows) Next() bool {
	m.idx++
	return m.idx < len(m.data)
}

func (m *MockRows) Scan(dest ...interface{}) error {
	if m.idx >= len(m.data) {
		return pgx.ErrNoRows
	}
	for i, d := range m.data[m.idx] {
		switch dest[i].(type) {
		case *string:
			*dest[i].(*string) = d.(string)
		case *int:
			*dest[i].(*int) = d.(int)
		case *float64:
			*dest[i].(*float64) = d.(float64)
		}
	}
	return nil
}

func (m *MockRows) Close() {}

// MockRedisClient - мок для Redis
type MockRedisClient struct {
	mock.Mock
	data map[string]string
}

func NewMockRedisClient() *MockRedisClient {
	return &MockRedisClient{data: make(map[string]string)}
}

func (m *MockRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	m.Called(ctx, key, value, expiration)
	return redis.NewStatusCmd(ctx)
}

func (m *MockRedisClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	m.Called(ctx, keys)
	return redis.NewIntCmd(ctx)
}

func (m *MockRedisClient) Pipeline() redis.Pipeliner {
	m.Called()
	return &MockPipeline{client: m}
}

type MockPipeline struct {
	mock.Mock
	client *MockRedisClient
	cmds   []interface{}
}

func (m *MockPipeline) Append(ctx context.Context, string, k string) error {
	args := m.Called(ctx, cmd)
	return args.Error(0)
}

func (m *MockPipeline) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	m.client.Set(ctx, key, value, expiration)
	m.cmds = append(m.cmds, "set")
	return redis.NewStatusCmd(ctx)
}

func (m *MockPipeline) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	m.client.Del(ctx, keys...)
	m.cmds = append(m.cmds, "del")
	return redis.NewIntCmd(ctx)
}

func (m *MockPipeline) Exec(ctx context.Context) ([]redis.Cmder, error) {
	args := m.Called(ctx)
	return args.Get(0).([]redis.Cmder), args.Error(1)
}


// ==================== ТЕСТЫ ====================

func TestAntiFraud_DetectAndBan_Success(t *testing.T) {
	ctx := context.Background()
	
	// Создаем моки
	mockTx := new(MockTx)
	mockPool := new(MockPostgresPool)
	mockRedis := NewMockRedisClient()
	
	// Настраиваем ожидания
	mockPool.On("Begin", ctx).Return(mockTx, nil)
	
	// Мокаем QueryRow для успешного получения запроса
	row := &MockRows{
		data: [][]interface{}{
			{uuid.New(), "account123", "merchant456", common.PayerInteraction, time.Now().Add(-time.Hour)},
		},
	}
	mockTx.On("QueryRow", ctx, mock.Anything, mock.Anything).Return(row)
	mockTx.On("Commit", ctx).Return(nil)
	
	// Создаем экземпляр AntiFraud с моками
	af := &AntiFraud{
		redisDB:     mockRedis,
		postgresDB:  mockPool,
		lifecycle:   &common.Lifecycle{Ctx: context.Background()},
	}
	
	// Вызываем тестируемый метод
	err := af.detectAndBan(ctx)
	
	// Проверяем результат
	assert.NoError(t, err)
	mockPool.AssertExpectations(t)
	mockTx.AssertExpectations(t)
}

func TestAntiFraud_DetectAndBan_NoRows(t *testing.T) {
	ctx := context.Background()
	
	mockTx := new(MockTx)
	mockPool := new(MockPostgresPool)
	mockRedis := NewMockRedisClient()
	
	mockPool.On("Begin", ctx).Return(mockTx, nil)
	
	// Мокаем QueryRow с ошибкой pgx.ErrNoRows
	mockTx.On("QueryRow", ctx, mock.Anything, mock.Anything).Return(&MockRows{data: [][]interface{}{}})
	
	af := &AntiFraud{
		redisDB:     mockRedis,
		postgresDB:  mockPool,
		lifecycle:   &common.Lifecycle{Ctx: context.Background()},
	}
	
	err := af.detectAndBan(ctx)
	
	assert.NoError(t, err)
	mockPool.AssertExpectations(t)
}

func TestAntiFraud_DetectAndBan_DBError(t *testing.T) {
	ctx := context.Background()
	
	mockTx := new(MockTx)
	mockPool := new(MockPostgresPool)
	
	mockPool.On("Begin", ctx).Return(mockTx, errors.New("connection failed"))
	
	af := &AntiFraud{
		postgresDB: mockPool,
		lifecycle:  &common.Lifecycle{Ctx: context.Background()},
	}
	
	err := af.detectAndBan(ctx)
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Failed to start DB transaction")
}

func TestAntiFraud_BanByUserIDs_Success(t *testing.T) {
	ctx := context.Background()
	
	mockRedis := NewMockRedisClient()
	
	// Мокаем Pipeline
	pipeline := &MockPipeline{client: mockRedis}
	mockRedis.On("Pipeline").Return(pipeline)
	pipeline.On("Exec", ctx).Return([]redis.Cmder{}, nil)
	
	af := &AntiFraud{
		redisDB: mockRedis,
	}
	
	userIDs := []string{"user1", "user2", "user3"}
	err := af.banByUserIDs(ctx, userIDs, 15*time.Minute)
	
	assert.NoError(t, err)
	mockRedis.AssertExpectations(t)
}

func TestTwoPhaseBan_PrepareBan(t *testing.T) {
	ctx := context.Background()
	
	mockRedis := NewMockRedisClient()
	pipeline := &MockPipeline{client: mockRedis}
	mockRedis.On("Pipeline").Return(pipeline)
	pipeline.On("Exec", ctx).Return([]redis.Cmder{}, nil)
	
	ban := &TwoPhaseBan{
		redisDB: mockRedis,
		txID:    "tx123",
		userIDs: []string{"user1", "user2"},
		ttl:     time.Minute * 5,
	}
	
	err := ban.prepareBan(ctx)
	
	assert.NoError(t, err)
	mockRedis.AssertExpectations(t)
}

func TestTwoPhaseBan_CommitBan(t *testing.T) {
	ctx := context.Background()
	
	mockRedis := NewMockRedisClient()
	pipeline := &MockPipeline{client: mockRedis}
	mockRedis.On("Pipeline").Return(pipeline)
	pipeline.On("Exec", ctx).Return([]redis.Cmder{}, nil)
	
	ban := &TwoPhaseBan{
		redisDB: mockRedis,
		txID:    "tx123",
		userIDs: []string{"user1", "user2"},
		banDur:  15 * time.Minute,
	}
	
	err := ban.commitBan(ctx)
	
	assert.NoError(t, err)
	mockRedis.AssertExpectations(t)
}

func TestTwoPhaseBan_RollbackBans(t *testing.T) {
	ctx := context.Background()
	
	mockRedis := NewMockRedisClient()
	pipeline := &MockPipeline{client: mockRedis}
	mockRedis.On("Pipeline").Return(pipeline)
	pipeline.On("Exec", ctx).Return([]redis.Cmder{}, nil)
	
	ban := &TwoPhaseBan{
		redisDB: mockRedis,
		txID:    "tx123",
		userIDs: []string{"user1", "user2"},
	}
	
	err := ban.rollbackBans(ctx)
	
	assert.NoError(t, err)
	mockRedis.AssertExpectations(t)
}

func TestAntiFraud_UserIDsByDevices(t *testing.T) {
	ctx := context.Background()
	
	mockRows := &MockRows{
		data: [][]interface{}{
			{"user1"},
			{"user2"},
			{"user3"},
		},
	}
	
	mockPool := new(MockPostgresPool)
	mockPool.On("Query", ctx, mock.Anything, "device1").Return(mockRows, nil)
	
	af := &AntiFraud{
		postgresDB: mockPool,
	}
	
	userIDs, err := af.userIDsByDevices(ctx, []string{"device1"})
	
	assert.NoError(t, err)
	assert.Len(t, userIDs, 3)
	assert.Contains(t, userIDs, "user1")
	assert.Contains(t, userIDs, "user2")
	assert.Contains(t, userIDs, "user3")
	mockPool.AssertExpectations(t)
}

func TestAntiFraud_UserIDsByDevices_QueryError(t *testing.T) {
	ctx := context.Background()
	
	mockPool := new(MockPostgresPool)
	mockPool.On("Query", ctx, mock.Anything, "device1").Return(&MockRows{data: [][]interface{}{}}, errors.New("query failed"))
	
	af := &AntiFraud{
		postgresDB: mockPool,
	}
	
	userIDs, err := af.userIDsByDevices(ctx, []string{"device1"})
	
	assert.NoError(t, err) // Ошибка логируется, но не возвращается
	assert.Empty(t, userIDs)
}

// Интеграционный тест с использованием testcontainers (опционально)
func TestAntiFraud_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}
	
	// Здесь можно реализовать тест с реальными базами данных
	// используя testcontainers-go
}

// Бенчмарки
func BenchmarkAntiFraud_DetectAndBan(b *testing.B) {
	ctx := context.Background()
	
	mockTx := new(MockTx)
	mockPool := new(MockPostgresPool)
	mockRedis := NewMockRedisClient()
	
	mockPool.On("Begin", ctx).Return(mockTx, nil)
	
	row := &MockRows{
		data: [][]interface{}{
			{uuid.New(), "account123", "merchant456", common.PayerInteraction, time.Now().Add(-time.Hour)},
		},
	}
	mockTx.On("QueryRow", ctx, mock.Anything, mock.Anything).Return(row)
	mockTx.On("Commit", ctx).Return(nil)
	
	af := &AntiFraud{
		redisDB:     mockRedis,
		postgresDB:  mockPool,
		lifecycle:   &common.Lifecycle{Ctx: context.Background()},
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = af.detectAndBan(ctx)
	}
}