package migration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"datastore-dynamodb-migrator/internal/interfaces"
)

type EngineTestSuite struct {
	suite.Suite
	engine           *Engine
	mockDataStore    *interfaces.MockDataStoreClient
	mockDynamoDB     *interfaces.MockDynamoDBClient
	mockIntrospector *interfaces.MockIntrospector
}

func (suite *EngineTestSuite) SetupTest() {
	suite.mockDataStore = new(interfaces.MockDataStoreClient)
	suite.mockDynamoDB = new(interfaces.MockDynamoDBClient)
	suite.mockIntrospector = new(interfaces.MockIntrospector)

	suite.engine = NewEngine(suite.mockDataStore, suite.mockDynamoDB)
	suite.engine.SetAnalyzer(suite.mockIntrospector)
}

func (suite *EngineTestSuite) TearDownTest() {
	suite.mockDataStore.AssertExpectations(suite.T())
	suite.mockDynamoDB.AssertExpectations(suite.T())
	suite.mockIntrospector.AssertExpectations(suite.T())
}

func (suite *EngineTestSuite) TestNewEngine() {
	engine := NewEngine(suite.mockDataStore, suite.mockDynamoDB)

	assert.NotNil(suite.T(), engine)
	assert.Equal(suite.T(), suite.mockDataStore, engine.datastoreClient)
	assert.Equal(suite.T(), suite.mockDynamoDB, engine.dynamoClient)
	assert.Equal(suite.T(), 100, engine.batchSize)
	assert.Equal(suite.T(), 5, engine.maxWorkers)
	assert.False(suite.T(), engine.dryRun)
	assert.Nil(suite.T(), engine.analyzer)
}

func (suite *EngineTestSuite) TestSetAnalyzer() {
	analyzer := new(interfaces.MockIntrospector)
	suite.engine.SetAnalyzer(analyzer)

	assert.Equal(suite.T(), analyzer, suite.engine.analyzer)
}

func (suite *EngineTestSuite) TestSetBatchSize() {
	// Test valid batch size
	suite.engine.SetBatchSize(250)
	assert.Equal(suite.T(), 250, suite.engine.batchSize)

	// Test invalid batch size (should not change)
	suite.engine.SetBatchSize(0)
	assert.Equal(suite.T(), 250, suite.engine.batchSize)

	suite.engine.SetBatchSize(-10)
	assert.Equal(suite.T(), 250, suite.engine.batchSize)
}

func (suite *EngineTestSuite) TestSetMaxWorkers() {
	// Test valid worker count
	suite.engine.SetMaxWorkers(10)
	assert.Equal(suite.T(), 10, suite.engine.maxWorkers)

	// Test invalid worker count (should not change)
	suite.engine.SetMaxWorkers(0)
	assert.Equal(suite.T(), 10, suite.engine.maxWorkers)

	suite.engine.SetMaxWorkers(-5)
	assert.Equal(suite.T(), 10, suite.engine.maxWorkers)
}

func (suite *EngineTestSuite) TestSetDryRun() {
	// Test setting dry run to true
	suite.engine.SetDryRun(true)
	assert.True(suite.T(), suite.engine.dryRun)

	// Test setting dry run to false
	suite.engine.SetDryRun(false)
	assert.False(suite.T(), suite.engine.dryRun)
}

func (suite *EngineTestSuite) TestValidateConfig_Success() {
	config := interfaces.TestMigrationConfig(
		"User",
		"Users",
		interfaces.TestKeySelection("id", nil),
		interfaces.SampleUserSchema,
	)

	err := suite.engine.ValidateConfig(config)
	assert.NoError(suite.T(), err)
}

func (suite *EngineTestSuite) TestValidateConfig_EmptySourceKind() {
	config := interfaces.TestMigrationConfig(
		"",
		"Users",
		interfaces.TestKeySelection("id", nil),
		interfaces.SampleUserSchema,
	)

	err := suite.engine.ValidateConfig(config)
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "source kind cannot be empty")
}

func (suite *EngineTestSuite) TestValidateConfig_EmptyTargetTable() {
	config := interfaces.TestMigrationConfig(
		"User",
		"",
		interfaces.TestKeySelection("id", nil),
		interfaces.SampleUserSchema,
	)

	err := suite.engine.ValidateConfig(config)
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "target table cannot be empty")
}

func (suite *EngineTestSuite) TestValidateConfig_EmptyPartitionKey() {
	config := interfaces.TestMigrationConfig(
		"User",
		"Users",
		interfaces.TestKeySelection("", nil),
		interfaces.SampleUserSchema,
	)

	err := suite.engine.ValidateConfig(config)
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "partition key cannot be empty")
}

func (suite *EngineTestSuite) TestValidateConfig_PartitionKeyNotInSchema() {
	config := interfaces.TestMigrationConfig(
		"User",
		"Users",
		interfaces.TestKeySelection("nonexistent_field", nil),
		interfaces.SampleUserSchema,
	)

	err := suite.engine.ValidateConfig(config)
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "partition key 'nonexistent_field' not found in schema")
}

func (suite *EngineTestSuite) TestValidateConfig_SortKeyNotInSchema() {
	sortKey := "nonexistent_sort_key"
	config := interfaces.TestMigrationConfig(
		"User",
		"Users",
		interfaces.TestKeySelection("id", &sortKey),
		interfaces.SampleUserSchema,
	)

	err := suite.engine.ValidateConfig(config)
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "sort key 'nonexistent_sort_key' not found in schema")
}

func (suite *EngineTestSuite) TestValidateConfig_SortKeySameAsPartitionKey() {
	sortKey := "id"
	config := interfaces.TestMigrationConfig(
		"User",
		"Users",
		interfaces.TestKeySelection("id", &sortKey),
		interfaces.SampleUserSchema,
	)

	err := suite.engine.ValidateConfig(config)
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "sort key cannot be the same as partition key")
}

func (suite *EngineTestSuite) TestMigrate_NoAnalyzer() {
	engine := NewEngine(suite.mockDataStore, suite.mockDynamoDB)
	// Don't set analyzer

	config := interfaces.SampleUserMigrationConfig
	ctx := context.Background()

	progressChan, err := engine.Migrate(ctx, config, false)

	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), progressChan)
	assert.Contains(suite.T(), err.Error(), "analyzer not set")
}

func (suite *EngineTestSuite) TestMigrate_InvalidConfig() {
	config := interfaces.TestMigrationConfig(
		"",
		"Users",
		interfaces.TestKeySelection("id", nil),
		interfaces.SampleUserSchema,
	)
	ctx := context.Background()

	progressChan, err := suite.engine.Migrate(ctx, config, false)

	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), progressChan)
	assert.Contains(suite.T(), err.Error(), "invalid configuration")
}

func (suite *EngineTestSuite) TestMigrate_Success() {
	config := interfaces.SampleUserMigrationConfig
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Mock expectations
	suite.mockDynamoDB.On("CreateTable", ctx, config, false).Return(nil)

	entityChan := interfaces.CreateTestEntityChannel(3)
	suite.mockDataStore.On("GetEntities", ctx, config.SourceKind, 100).Return(entityChan, nil)

	// Mock entity conversion
	testEntity := interfaces.CreateTestEntity()
	convertedEntity := map[string]interface{}{
		"id":   testEntity["id"],
		"name": testEntity["name"],
		"age":  testEntity["age"],
	}
	suite.mockIntrospector.On("ConvertForDynamoDB", mock.Anything, config).Return(convertedEntity, nil).Times(3)

	// Mock DynamoDB put
	suite.mockDynamoDB.On("PutItems", ctx, config.TargetTable, mock.Anything, false).Return(nil)

	progressChan, err := suite.engine.Migrate(ctx, config, false)

	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), progressChan)

	// Collect progress updates with timeout
	var progressList []interfaces.MigrationProgress
	done := make(chan bool)
	go func() {
		defer close(done)
		for progress := range progressChan {
			progressList = append(progressList, progress)
		}
	}()

	select {
	case <-done:
		// Progress collection completed normally
	case <-ctx.Done():
		suite.T().Fatal("Test timed out waiting for progress updates")
	}

	// Verify we got progress updates
	assert.Greater(suite.T(), len(progressList), 0)

	// First update should be initial progress
	firstProgress := progressList[0]
	assert.Equal(suite.T(), config.SourceKind, firstProgress.Kind)
	assert.Equal(suite.T(), int64(0), firstProgress.Processed)
	assert.Equal(suite.T(), config.Schema.Count, firstProgress.Total)
	assert.True(suite.T(), firstProgress.InProgress)
	assert.False(suite.T(), firstProgress.Completed)

	// Last update should be completion
	lastProgress := progressList[len(progressList)-1]
	assert.False(suite.T(), lastProgress.InProgress)
	assert.True(suite.T(), lastProgress.Completed)
}

func (suite *EngineTestSuite) TestMigrate_CreateTableError() {
	config := interfaces.SampleUserMigrationConfig
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Mock table creation failure
	suite.mockDynamoDB.On("CreateTable", ctx, config, false).Return(fmt.Errorf("table creation failed"))

	progressChan, err := suite.engine.Migrate(ctx, config, false)

	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), progressChan)

	// Collect progress updates with timeout
	var progressList []interfaces.MigrationProgress
	done := make(chan bool)
	go func() {
		defer close(done)
		for progress := range progressChan {
			progressList = append(progressList, progress)
		}
	}()

	select {
	case <-done:
		// Progress collection completed normally
	case <-ctx.Done():
		suite.T().Fatal("Test timed out waiting for progress updates")
	}

	// Verify error handling
	assert.Greater(suite.T(), len(progressList), 0)

	lastProgress := progressList[len(progressList)-1]
	assert.Greater(suite.T(), lastProgress.Errors, int64(0))
	assert.False(suite.T(), lastProgress.InProgress)
	assert.True(suite.T(), lastProgress.Completed)
}

func (suite *EngineTestSuite) TestMigrate_GetEntitiesError() {
	config := interfaces.SampleUserMigrationConfig
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Mock successful table creation
	suite.mockDynamoDB.On("CreateTable", ctx, config, false).Return(nil)

	// Mock GetEntities failure
	suite.mockDataStore.On("GetEntities", ctx, config.SourceKind, 100).Return(nil, fmt.Errorf("failed to get entities"))

	progressChan, err := suite.engine.Migrate(ctx, config, false)

	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), progressChan)

	// Collect progress updates with timeout
	var progressList []interfaces.MigrationProgress
	done := make(chan bool)
	go func() {
		defer close(done)
		for progress := range progressChan {
			progressList = append(progressList, progress)
		}
	}()

	select {
	case <-done:
		// Progress collection completed normally
	case <-ctx.Done():
		suite.T().Fatal("Test timed out waiting for progress updates")
	}

	// Verify error handling
	assert.Greater(suite.T(), len(progressList), 0)

	lastProgress := progressList[len(progressList)-1]
	assert.Greater(suite.T(), lastProgress.Errors, int64(0))
	assert.False(suite.T(), lastProgress.InProgress)
	assert.True(suite.T(), lastProgress.Completed)
}

func (suite *EngineTestSuite) TestMigrate_DryRun() {
	config := interfaces.SampleUserMigrationConfig
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Mock expectations for dry run
	suite.mockDynamoDB.On("CreateTable", ctx, config, true).Return(nil)

	entityChan := interfaces.CreateTestEntityChannel(2)
	suite.mockDataStore.On("GetEntities", ctx, config.SourceKind, 100).Return(entityChan, nil)

	// Mock entity conversion
	testEntity := interfaces.CreateTestEntity()
	convertedEntity := map[string]interface{}{
		"id":   testEntity["id"],
		"name": testEntity["name"],
	}
	suite.mockIntrospector.On("ConvertForDynamoDB", mock.Anything, config).Return(convertedEntity, nil).Times(2)

	// Mock DynamoDB put with dry run
	suite.mockDynamoDB.On("PutItems", ctx, config.TargetTable, mock.Anything, true).Return(nil)

	progressChan, err := suite.engine.Migrate(ctx, config, true)

	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), progressChan)

	// Wait for completion with timeout
	done := make(chan bool)
	go func() {
		defer close(done)
		for range progressChan {
			// Consume all progress updates
		}
	}()

	select {
	case <-done:
		// Migration completed normally
	case <-ctx.Done():
		suite.T().Fatal("Test timed out waiting for migration completion")
	}
}

func (suite *EngineTestSuite) TestMigrateAll_Success() {
	configs := []interfaces.MigrationConfig{
		interfaces.SampleUserMigrationConfig,
		interfaces.SampleOrderMigrationConfig,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Mock expectations for both configs
	for _, config := range configs {
		suite.mockDynamoDB.On("CreateTable", ctx, config, false).Return(nil)

		entityChan := interfaces.CreateTestEntityChannel(2)
		suite.mockDataStore.On("GetEntities", ctx, config.SourceKind, 100).Return(entityChan, nil)

		// Create appropriate converted entity based on the config
		var convertedEntity map[string]interface{}
		if config.SourceKind == "User" {
			convertedEntity = map[string]interface{}{
				"id":   "user_123",
				"name": "Test User",
			}
		} else if config.SourceKind == "Order" {
			convertedEntity = map[string]interface{}{
				"order_id":   "order_123",
				"created_at": int64(1234567890),
			}
		}

		suite.mockIntrospector.On("ConvertForDynamoDB", mock.Anything, config).Return(convertedEntity, nil).Times(2)
		suite.mockDynamoDB.On("PutItems", ctx, config.TargetTable, mock.Anything, false).Return(nil)
	}

	progressChan, err := suite.engine.MigrateAll(ctx, configs, false)

	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), progressChan)

	// Collect all progress updates with timeout
	var progressList []interfaces.MigrationProgress
	done := make(chan bool)
	go func() {
		defer close(done)
		for progress := range progressChan {
			progressList = append(progressList, progress)
		}
	}()

	select {
	case <-done:
		// Progress collection completed normally
	case <-ctx.Done():
		suite.T().Fatal("Test timed out waiting for progress updates")
	}

	// Should have progress for both kinds
	assert.Greater(suite.T(), len(progressList), 0)

	// Check that we have progress for both kinds
	kindsProcessed := make(map[string]bool)
	for _, progress := range progressList {
		kindsProcessed[progress.Kind] = true
	}
	assert.True(suite.T(), kindsProcessed["User"])
	assert.True(suite.T(), kindsProcessed["Order"])
}

func (suite *EngineTestSuite) TestMigrateAll_EmptyConfigs() {
	ctx := context.Background()

	progressChan, err := suite.engine.MigrateAll(ctx, []interfaces.MigrationConfig{}, false)

	// Empty configs should return an error according to implementation
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), progressChan)
	assert.Contains(suite.T(), err.Error(), "no configurations provided")
}

func (suite *EngineTestSuite) TestMigrateAll_SingleConfigError() {
	configs := []interfaces.MigrationConfig{
		interfaces.SampleUserMigrationConfig,
		interfaces.TestMigrationConfig("", "InvalidTable", interfaces.TestKeySelection("id", nil), interfaces.SampleUserSchema),
	}
	ctx := context.Background()

	progressChan, err := suite.engine.MigrateAll(ctx, configs, false)

	// Should return an error during validation since one config is invalid
	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), progressChan)
	assert.Contains(suite.T(), err.Error(), "invalid configuration")
}

func (suite *EngineTestSuite) TestConcurrentAccess() {
	// Test that engine methods are thread-safe
	done := make(chan bool, 4)

	// Concurrent setters
	go func() {
		for i := 0; i < 100; i++ {
			suite.engine.SetBatchSize(i + 1)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			suite.engine.SetMaxWorkers(i + 1)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			suite.engine.SetDryRun(i%2 == 0)
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			suite.engine.SetAnalyzer(suite.mockIntrospector)
		}
		done <- true
	}()

	// Wait for all goroutines to complete
	for i := 0; i < 4; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			suite.T().Fatal("Timeout waiting for concurrent operations")
		}
	}
}

func TestEngineTestSuite(t *testing.T) {
	suite.Run(t, new(EngineTestSuite))
}

// Additional tests for edge cases
func TestEngineProcessBatch_EmptyBatch(t *testing.T) {
	mockDataStore := new(interfaces.MockDataStoreClient)
	mockDynamoDB := new(interfaces.MockDynamoDBClient)
	mockIntrospector := new(interfaces.MockIntrospector)

	engine := NewEngine(mockDataStore, mockDynamoDB)
	engine.SetAnalyzer(mockIntrospector)

	ctx := context.Background()
	config := interfaces.SampleUserMigrationConfig

	// Test processing empty batch
	err := engine.processBatch(ctx, []interface{}{}, config, mockIntrospector, false)
	assert.NoError(t, err)
}
