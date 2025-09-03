package dynamodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/coreyculler/datastore-dynamodb-migrator/internal/cli"
	"github.com/coreyculler/datastore-dynamodb-migrator/internal/interfaces"
)

// Client wraps the AWS DynamoDB client and implements the DynamoDBClient interface
type Client struct {
	client *dynamodb.Client
	config aws.Config
	mu     sync.RWMutex
}

// NewClient creates a new DynamoDB client
func NewClient(ctx context.Context) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	client := dynamodb.NewFromConfig(cfg)

	return &Client{
		client: client,
		config: cfg,
	}, nil
}

// NewClientWithConfig creates a new DynamoDB client with custom configuration
func NewClientWithConfig(cfg aws.Config) *Client {
	return &Client{
		client: dynamodb.NewFromConfig(cfg),
		config: cfg,
	}
}

// CreateTable creates a DynamoDB table based on the migration configuration
func (c *Client) CreateTable(ctx context.Context, config interfaces.MigrationConfig, dryRun bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if table already exists
	exists, err := c.tableExists(ctx, config.TargetTable)
	if err != nil {
		return fmt.Errorf("failed to check if table exists: %w", err)
	}

	if exists {
		// If the table exists, ask the user how to proceed.
		selector := cli.NewInteractiveSelector() // We need a CLI selector here.
		action, err := selector.HandleExistingTable(ctx, config.TargetTable)
		if err != nil {
			return fmt.Errorf("failed to handle existing table: %w", err)
		}

		switch action {
		case "skip":
			fmt.Printf("⏭️  Skipping table %s as requested.\n", config.TargetTable)
			return nil // Returning nil to indicate graceful skip, not an error.
		case "truncate":
			fmt.Printf("⚠️  Truncating table %s...\n", config.TargetTable)
			if err := c.truncateTable(ctx, config.TargetTable, config, dryRun); err != nil {
				return fmt.Errorf("failed to truncate table: %w", err)
			}
			fmt.Printf("✅ Table %s truncated successfully.\n", config.TargetTable)
			// After truncating, we proceed to create it with the new schema.
		case "insert":
			fmt.Printf("➡️  Inserting/updating records in existing table %s.\n", config.TargetTable)
			return nil // Continue without creating a new table.
		}
	}

	// Build key schema
	keySchema := []types.KeySchemaElement{
		{
			AttributeName: aws.String(config.KeySelection.PartitionKey),
			KeyType:       types.KeyTypeHash,
		},
	}

	// Build attribute definitions
	attributeDefinitions := []types.AttributeDefinition{
		{
			AttributeName: aws.String(config.KeySelection.PartitionKey),
			AttributeType: c.getAttributeType(getPKTypeField(config), config.Schema),
		},
	}

	// Add sort key if specified
	if config.KeySelection.SortKey != nil && *config.KeySelection.SortKey != "" {
		keySchema = append(keySchema, types.KeySchemaElement{
			AttributeName: aws.String(*config.KeySelection.SortKey),
			KeyType:       types.KeyTypeRange,
		})

		attributeDefinitions = append(attributeDefinitions, types.AttributeDefinition{
			AttributeName: aws.String(*config.KeySelection.SortKey),
			AttributeType: c.getAttributeType(*config.KeySelection.SortKey, config.Schema),
		})
	}

	// Create table input
	createTableInput := &dynamodb.CreateTableInput{
		TableName:            aws.String(config.TargetTable),
		KeySchema:            keySchema,
		AttributeDefinitions: attributeDefinitions,
		BillingMode:          types.BillingModePayPerRequest, // Use on-demand billing
		Tags: []types.Tag{
			{
				Key:   aws.String("MigrationSource"),
				Value: aws.String("DataStore"),
			},
			{
				Key:   aws.String("SourceKind"),
				Value: aws.String(config.SourceKind),
			},
		},
	}

	if dryRun {
		// In dry-run mode, just show what would be created
		fmt.Printf("🔍 DRY RUN: Would create table '%s' with:\n", config.TargetTable)
		fmt.Printf("  - Partition Key: %s (%s)\n", config.KeySelection.PartitionKey,
			c.getAttributeType(getPKTypeField(config), config.Schema))
		if config.KeySelection.SortKey != nil && *config.KeySelection.SortKey != "" {
			fmt.Printf("  - Sort Key: %s (%s)\n", *config.KeySelection.SortKey,
				c.getAttributeType(*config.KeySelection.SortKey, config.Schema))
		}
		fmt.Printf("  - Billing Mode: Pay-per-request\n")
		fmt.Printf("  - Tags: MigrationSource=DataStore, SourceKind=%s\n", config.SourceKind)
		return nil
	}

	// Create the table
	_, err = c.client.CreateTable(ctx, createTableInput)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", config.TargetTable, err)
	}

	// Wait for table to be active
	fmt.Printf("Waiting for table %s to be active...\n", config.TargetTable)
	waiter := dynamodb.NewTableExistsWaiter(c.client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(config.TargetTable),
	}, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("failed to wait for table %s to be active: %w", config.TargetTable, err)
	}

	fmt.Printf("Table %s created successfully\n", config.TargetTable)
	return nil
}

// TableExists checks if a table exists in DynamoDB
func (c *Client) TableExists(ctx context.Context, tableName string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.tableExists(ctx, tableName)
}

// tableExists is the internal implementation without locking
func (c *Client) tableExists(ctx context.Context, tableName string) (bool, error) {
	_, err := c.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})

	if err != nil {
		// Check if it's a ResourceNotFoundException
		var resourceNotFound *types.ResourceNotFoundException
		if errors.As(err, &resourceNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("failed to describe table %s: %w", tableName, err)
	}

	return true, nil
}

// PutItems puts multiple items into a DynamoDB table
func (c *Client) PutItems(ctx context.Context, tableName string, items []map[string]interface{}, dryRun bool) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(items) == 0 {
		return nil
	}

	if dryRun {
		// In dry-run mode, just show what would be written
		fmt.Printf("🔍 DRY RUN: Would write %d items to table '%s'\n", len(items), tableName)
		if len(items) > 0 {
			fmt.Printf("  Sample item structure:\n")
			for key, value := range items[0] {
				fmt.Printf("    %s: %T\n", key, value)
			}
		}
		return nil
	}

	// DynamoDB batch write can handle up to 25 items at a time
	const batchSize = 25

	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}

		batch := items[i:end]
		if err := c.putItemsBatch(ctx, tableName, batch); err != nil {
			return fmt.Errorf("failed to put batch starting at index %d: %w", i, err)
		}
	}

	return nil
}

// putItemsBatch puts a single batch of items (up to 25)
func (c *Client) putItemsBatch(ctx context.Context, tableName string, items []map[string]interface{}) error {
	if len(items) > 25 {
		return fmt.Errorf("batch size cannot exceed 25 items, got %d", len(items))
	}

	writeRequests := make([]types.WriteRequest, len(items))

	for i, item := range items {
		// Convert the item to DynamoDB attribute values
		attributeValue, err := c.convertToDynamoDBItem(item)
		if err != nil {
			return fmt.Errorf("failed to convert item %d: %w", i, err)
		}

		writeRequests[i] = types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: attributeValue,
			},
		}
	}

	// Execute the batch write
	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			tableName: writeRequests,
		},
	}

	result, err := c.client.BatchWriteItem(ctx, input)
	if err != nil {
		return fmt.Errorf("batch write failed: %w", err)
	}

	// Handle unprocessed items
	if len(result.UnprocessedItems) > 0 {
		// Retry unprocessed items with exponential backoff
		return c.retryUnprocessedItems(ctx, result.UnprocessedItems, 0)
	}

	return nil
}

// retryUnprocessedItems retries unprocessed items with exponential backoff
func (c *Client) retryUnprocessedItems(ctx context.Context, unprocessedItems map[string][]types.WriteRequest, attempt int) error {
	const maxRetries = 3

	if attempt >= maxRetries {
		return fmt.Errorf("exceeded maximum retries (%d) for unprocessed items", maxRetries)
	}

	// Exponential backoff with context cancellation
	backoffDuration := time.Duration(1<<attempt) * time.Second
	timer := time.NewTimer(backoffDuration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: unprocessedItems,
	}

	result, err := c.client.BatchWriteItem(ctx, input)
	if err != nil {
		return fmt.Errorf("retry batch write failed: %w", err)
	}

	// If there are still unprocessed items, retry again
	if len(result.UnprocessedItems) > 0 {
		return c.retryUnprocessedItems(ctx, result.UnprocessedItems, attempt+1)
	}

	return nil
}

// convertToDynamoDBItem converts a map to DynamoDB attribute values
func (c *Client) convertToDynamoDBItem(item map[string]interface{}) (map[string]types.AttributeValue, error) {
	result := make(map[string]types.AttributeValue)

	for key, value := range item {
		attrValue, err := c.convertToDynamoDBAttributeValue(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", key, err)
		}
		result[key] = attrValue
	}

	return result, nil
}

// convertToDynamoDBAttributeValue converts a Go value to a DynamoDB AttributeValue
func (c *Client) convertToDynamoDBAttributeValue(value interface{}) (types.AttributeValue, error) {
	if value == nil {
		return &types.AttributeValueMemberNULL{Value: true}, nil
	}

	switch v := value.(type) {
	case string:
		if v == "" {
			return &types.AttributeValueMemberNULL{Value: true}, nil
		}
		return &types.AttributeValueMemberS{Value: v}, nil
	case int, int8, int16, int32, int64:
		return &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", v)}, nil
	case uint, uint8, uint16, uint32, uint64:
		return &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", v)}, nil
	case float32, float64:
		return &types.AttributeValueMemberN{Value: fmt.Sprintf("%g", v)}, nil
	case bool:
		return &types.AttributeValueMemberBOOL{Value: v}, nil
	case []byte:
		return &types.AttributeValueMemberB{Value: v}, nil
	case []interface{}:
		list := make([]types.AttributeValue, len(v))
		for i, item := range v {
			converted, err := c.convertToDynamoDBAttributeValue(item)
			if err != nil {
				return nil, err
			}
			list[i] = converted
		}
		return &types.AttributeValueMemberL{Value: list}, nil
	case map[string]interface{}:
		m := make(map[string]types.AttributeValue)
		for k, item := range v {
			converted, err := c.convertToDynamoDBAttributeValue(item)
			if err != nil {
				return nil, err
			}
			m[k] = converted
		}
		return &types.AttributeValueMemberM{Value: m}, nil
	default:
		// For unknown types, try JSON marshaling first, then fallback to string
		if jsonData, err := json.Marshal(v); err == nil {
			return &types.AttributeValueMemberS{Value: string(jsonData)}, nil
		}
		// Fallback to string conversion
		return &types.AttributeValueMemberS{Value: fmt.Sprintf("%v", v)}, nil
	}
}

// getAttributeType determines the DynamoDB attribute type for a field
func (c *Client) getAttributeType(fieldName string, schema *interfaces.KindSchema) types.ScalarAttributeType {
	if schema == nil {
		return types.ScalarAttributeTypeS // Default to string if schema is nil
	}
	// Find the field in the schema
	for _, field := range schema.Fields {
		if field.Name == fieldName {
			switch field.TypeName {
			case "string":
				return types.ScalarAttributeTypeS
			case "int", "int64", "float", "float64":
				return types.ScalarAttributeTypeN
			case "[]byte":
				return types.ScalarAttributeTypeB
			default:
				// Default to string for unknown types
				return types.ScalarAttributeTypeS
			}
		}
	}

	// If field not found in schema, default to string
	return types.ScalarAttributeTypeS
}

// getPKTypeField returns the field name that should be used to determine the attribute type for the PK
func getPKTypeField(config interfaces.MigrationConfig) string {
	if config.KeySelection.PartitionKeySource != "" {
		return config.KeySelection.PartitionKeySource
	}
	return config.KeySelection.PartitionKey
}

// truncateTable deletes and recreates a table to clear its contents.
func (c *Client) truncateTable(ctx context.Context, tableName string, config interfaces.MigrationConfig, dryRun bool) error {
	if dryRun {
		fmt.Printf("\n🔍 DRY RUN: Would truncate table %s by deleting and recreating it.", tableName)
		return nil
	}

	// Delete the table
	deleteInput := &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	}
	_, err := c.client.DeleteTable(ctx, deleteInput)
	if err != nil {
		return fmt.Errorf("failed to delete table %s for truncation: %w", tableName, err)
	}

	// Wait for the table to not exist anymore
	fmt.Printf("\nWaiting for table %s to be deleted...", tableName)
	waiter := dynamodb.NewTableNotExistsWaiter(c.client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(tableName)}, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("failed to wait for table %s to be deleted: %w", tableName, err)
	}
	fmt.Println(" Done.")

	// Re-create the table using the original CreateTable logic (but without the existence check)
	return c.createTableInternal(ctx, config, dryRun)
}

// createTableInternal handles the actual table creation logic.
func (c *Client) createTableInternal(ctx context.Context, config interfaces.MigrationConfig, dryRun bool) error {
	// Build key schema
	keySchema := []types.KeySchemaElement{
		{
			AttributeName: aws.String(config.KeySelection.PartitionKey),
			KeyType:       types.KeyTypeHash,
		},
	}

	// Build attribute definitions
	attributeDefinitions := []types.AttributeDefinition{
		{
			AttributeName: aws.String(config.KeySelection.PartitionKey),
			AttributeType: c.getAttributeType(getPKTypeField(config), config.Schema),
		},
	}

	// Add sort key if specified
	if config.KeySelection.SortKey != nil && *config.KeySelection.SortKey != "" {
		keySchema = append(keySchema, types.KeySchemaElement{
			AttributeName: aws.String(*config.KeySelection.SortKey),
			KeyType:       types.KeyTypeRange,
		})

		attributeDefinitions = append(attributeDefinitions, types.AttributeDefinition{
			AttributeName: aws.String(*config.KeySelection.SortKey),
			AttributeType: c.getAttributeType(*config.KeySelection.SortKey, config.Schema),
		})
	}

	// Create table input
	createTableInput := &dynamodb.CreateTableInput{
		TableName:            aws.String(config.TargetTable),
		KeySchema:            keySchema,
		AttributeDefinitions: attributeDefinitions,
		BillingMode:          types.BillingModePayPerRequest,
		Tags: []types.Tag{
			{Key: aws.String("MigrationSource"), Value: aws.String("DataStore")},
			{Key: aws.String("SourceKind"), Value: aws.String(config.SourceKind)},
		},
	}

	if dryRun {
		// In dry-run mode, just show what would be created
		fmt.Printf("🔍 DRY RUN: Would create table '%s' with:\n", config.TargetTable)
		fmt.Printf("  - Partition Key: %s (%s)\n", config.KeySelection.PartitionKey,
			c.getAttributeType(getPKTypeField(config), config.Schema))
		if config.KeySelection.SortKey != nil && *config.KeySelection.SortKey != "" {
			fmt.Printf("  - Sort Key: %s (%s)\n", *config.KeySelection.SortKey,
				c.getAttributeType(*config.KeySelection.SortKey, config.Schema))
		}
		fmt.Printf("  - Billing Mode: Pay-per-request\n")
		fmt.Printf("  - Tags: MigrationSource=DataStore, SourceKind=%s\n", config.SourceKind)
		return nil
	}

	// Create the table
	_, err := c.client.CreateTable(ctx, createTableInput)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", config.TargetTable, err)
	}

	// Wait for table to be active
	fmt.Printf("\nWaiting for table %s to be active...", config.TargetTable)
	waiter := dynamodb.NewTableExistsWaiter(c.client)
	err = waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(config.TargetTable)}, 5*time.Minute)
	if err != nil {
		return fmt.Errorf("failed to wait for table %s to be active: %w", config.TargetTable, err)
	}
	fmt.Println(" Done.")

	return nil
}

// Close closes the DynamoDB client (no-op for AWS SDK v2)
func (c *Client) Close() error {
	// AWS SDK v2 doesn't require explicit cleanup
	return nil
}
