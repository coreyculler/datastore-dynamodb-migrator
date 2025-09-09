package interfaces

import (
	"context"

	"github.com/stretchr/testify/mock"
)

// MockDataStoreClient is a mock implementation of DataStoreClient
type MockDataStoreClient struct {
	mock.Mock
}

func (m *MockDataStoreClient) ListKinds(ctx context.Context) ([]string, error) {
	args := m.Called(ctx)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockDataStoreClient) AnalyzeKind(ctx context.Context, kind string) (*KindSchema, error) {
	args := m.Called(ctx, kind)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*KindSchema), args.Error(1)
}

func (m *MockDataStoreClient) GetEntities(ctx context.Context, kind string, batchSize int, order *QueryOrder) (<-chan interface{}, error) {
	args := m.Called(ctx, kind, batchSize, order)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(<-chan interface{}), args.Error(1)
}

// Close mocks the Close method
func (m *MockDataStoreClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockDynamoDBClient is a mock implementation of DynamoDBClient
type MockDynamoDBClient struct {
	mock.Mock
}

func (m *MockDynamoDBClient) CreateTable(ctx context.Context, config MigrationConfig, dryRun bool) error {
	args := m.Called(ctx, config, dryRun)
	return args.Error(0)
}

func (m *MockDynamoDBClient) TableExists(ctx context.Context, tableName string) (bool, error) {
	args := m.Called(ctx, tableName)
	return args.Bool(0), args.Error(1)
}

func (m *MockDynamoDBClient) PutItems(ctx context.Context, tableName string, items []map[string]interface{}, dryRun bool) error {
	args := m.Called(ctx, tableName, items, dryRun)
	return args.Error(0)
}

func (m *MockDynamoDBClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockIntrospector is a mock implementation of Introspector
type MockIntrospector struct {
	mock.Mock
}

func (m *MockIntrospector) AnalyzeEntity(entity interface{}) FieldInfo {
	args := m.Called(entity)
	return args.Get(0).(FieldInfo)
}

func (m *MockIntrospector) GetFieldValue(entity interface{}, fieldName string) interface{} {
	args := m.Called(entity, fieldName)
	return args.Get(0)
}

func (m *MockIntrospector) ConvertForDynamoDB(entity interface{}, config MigrationConfig) (map[string]interface{}, error) {
	args := m.Called(entity, config)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(map[string]interface{}), args.Error(1)
}

// MockMigrationEngine is a mock implementation of MigrationEngine
type MockMigrationEngine struct {
	mock.Mock
}

func (m *MockMigrationEngine) Migrate(ctx context.Context, config MigrationConfig, dryRun bool) (<-chan MigrationProgress, error) {
	args := m.Called(ctx, config, dryRun)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(<-chan MigrationProgress), args.Error(1)
}

func (m *MockMigrationEngine) MigrateAll(ctx context.Context, configs []MigrationConfig, dryRun bool) (<-chan MigrationProgress, error) {
	args := m.Called(ctx, configs, dryRun)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(<-chan MigrationProgress), args.Error(1)
}

func (m *MockMigrationEngine) ValidateConfig(config MigrationConfig) error {
	args := m.Called(config)
	return args.Error(0)
}

func (m *MockMigrationEngine) SetDryRun(dryRun bool) {
	m.Called(dryRun)
}
