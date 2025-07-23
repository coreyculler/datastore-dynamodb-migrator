package migration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"datastore-dynamodb-migrator/internal/datastore"
	"datastore-dynamodb-migrator/internal/interfaces"
)

// Engine implements the MigrationEngine interface and orchestrates migrations
type Engine struct {
	datastoreClient interfaces.DataStoreClient
	dynamoClient    interfaces.DynamoDBClient
	analyzer        interfaces.Introspector
	batchSize       int
	maxWorkers      int
	dryRun          bool
	mu              sync.RWMutex
}

// NewEngine creates a new migration engine
func NewEngine(datastoreClient interfaces.DataStoreClient, dynamoClient interfaces.DynamoDBClient) *Engine {
	return &Engine{
		datastoreClient: datastoreClient,
		dynamoClient:    dynamoClient,
		analyzer:        nil, // Will be set by SetAnalyzer method
		batchSize:       100, // Default batch size for processing
		maxWorkers:      5,   // Default number of concurrent workers
	}
}

// SetAnalyzer sets the introspector for the engine
func (e *Engine) SetAnalyzer(analyzer interfaces.Introspector) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.analyzer = analyzer
}

// SetBatchSize sets the batch size for processing entities
func (e *Engine) SetBatchSize(size int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if size > 0 {
		e.batchSize = size
	}
}

// SetMaxWorkers sets the maximum number of concurrent workers
func (e *Engine) SetMaxWorkers(workers int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if workers > 0 {
		e.maxWorkers = workers
	}
}

// SetDryRun sets the dry-run mode
func (e *Engine) SetDryRun(dryRun bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dryRun = dryRun
}

// ValidateConfig validates a migration configuration
func (e *Engine) ValidateConfig(config interfaces.MigrationConfig) error {
	// Validate source kind
	if config.SourceKind == "" {
		return fmt.Errorf("source kind cannot be empty")
	}

	// Validate target table
	if config.TargetTable == "" {
		return fmt.Errorf("target table cannot be empty")
	}

	// Validate partition key
	if config.KeySelection.PartitionKey == "" {
		return fmt.Errorf("partition key cannot be empty")
	}

	// Validate that partition key exists in schema
	partitionKeyExists := false
	for _, field := range config.Schema.Fields {
		if field.Name == config.KeySelection.PartitionKey {
			partitionKeyExists = true
			break
		}
	}
	if !partitionKeyExists {
		return fmt.Errorf("partition key '%s' not found in schema", config.KeySelection.PartitionKey)
	}

	// Validate sort key if provided
	if config.KeySelection.SortKey != nil && *config.KeySelection.SortKey != "" {
		sortKeyExists := false
		for _, field := range config.Schema.Fields {
			if field.Name == *config.KeySelection.SortKey {
				sortKeyExists = true
				break
			}
		}
		if !sortKeyExists {
			return fmt.Errorf("sort key '%s' not found in schema", *config.KeySelection.SortKey)
		}

		// Ensure sort key is different from partition key
		if *config.KeySelection.SortKey == config.KeySelection.PartitionKey {
			return fmt.Errorf("sort key cannot be the same as partition key")
		}
	}

	return nil
}

// Migrate performs the migration for a single configuration
func (e *Engine) Migrate(ctx context.Context, config interfaces.MigrationConfig, dryRun bool) (<-chan interfaces.MigrationProgress, error) {
	e.mu.RLock()
	batchSize := e.batchSize
	maxWorkers := e.maxWorkers
	analyzer := e.analyzer
	e.mu.RUnlock()

	// Check if analyzer is set
	if analyzer == nil {
		return nil, fmt.Errorf("analyzer not set - call SetAnalyzer before migrating")
	}

	// Validate configuration
	if err := e.ValidateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	// Create progress channel
	progressChan := make(chan interfaces.MigrationProgress, 10)

	go func() {
		defer close(progressChan)

		// Send initial progress
		progress := interfaces.MigrationProgress{
			Kind:       config.SourceKind,
			Processed:  0,
			Total:      config.Schema.Count,
			Errors:     0,
			InProgress: true,
			Completed:  false,
		}
		progressChan <- progress

		// Create DynamoDB table
		if err := e.dynamoClient.CreateTable(ctx, config, dryRun); err != nil {
			progress.Errors++
			progress.InProgress = false
			progress.Completed = true
			progressChan <- progress
			return
		}

		// Get entities from DataStore
		entityChan, err := e.datastoreClient.GetEntities(ctx, config.SourceKind, batchSize)
		if err != nil {
			progress.Errors++
			progress.InProgress = false
			progress.Completed = true
			progressChan <- progress
			return
		}

		// Process entities in batches using workers
		e.processEntitiesWithWorkers(ctx, entityChan, config, progressChan, batchSize, maxWorkers, analyzer, dryRun)

		// Send final progress
		progress.InProgress = false
		progress.Completed = true
		progressChan <- progress
	}()

	return progressChan, nil
}

// processEntitiesWithWorkers processes entities using multiple workers for concurrent processing
func (e *Engine) processEntitiesWithWorkers(
	ctx context.Context,
	entityChan <-chan interface{},
	config interfaces.MigrationConfig,
	progressChan chan<- interfaces.MigrationProgress,
	batchSize int,
	maxWorkers int,
	analyzer interfaces.Introspector,
	dryRun bool) {

	// Create work channels
	workChan := make(chan []interface{}, maxWorkers*2)
	var wg sync.WaitGroup

	// Track progress
	var processed, errors int64
	var mu sync.Mutex

	// Start workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for batch := range workChan {
				if err := e.processBatch(ctx, batch, config, analyzer, dryRun); err != nil {
					mu.Lock()
					errors++
					mu.Unlock()
					fmt.Printf("Worker %d: Error processing batch: %v\n", workerID, err)
				}

				mu.Lock()
				processed += int64(len(batch))
				currentProcessed := processed
				currentErrors := errors
				mu.Unlock()

				// Send progress update
				progress := interfaces.MigrationProgress{
					Kind:       config.SourceKind,
					Processed:  currentProcessed,
					Total:      config.Schema.Count,
					Errors:     currentErrors,
					InProgress: true,
					Completed:  false,
				}

				select {
				case progressChan <- progress:
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// Collect entities into batches and send to workers
	go func() {
		defer close(workChan)

		var batch []interface{}

		for entity := range entityChan {
			batch = append(batch, entity)

			if len(batch) >= batchSize {
				select {
				case workChan <- batch:
					batch = nil
				case <-ctx.Done():
					return
				}
			}
		}

		// Send remaining entities
		if len(batch) > 0 {
			select {
			case workChan <- batch:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for all workers to complete
	wg.Wait()
}

// processBatch processes a batch of entities
func (e *Engine) processBatch(ctx context.Context, entities []interface{}, config interfaces.MigrationConfig, analyzer interfaces.Introspector, dryRun bool) error {
	if len(entities) == 0 {
		return nil
	}

	// Convert entities to DynamoDB format
	var items []map[string]interface{}

	for _, entity := range entities {
		var item map[string]interface{}
		var err error

		// Handle different entity types
		switch ent := entity.(type) {
		case *datastore.EntityWithKey:
			item = ent.ToMap()
		default:
			// Use the analyzer to convert the entity
			item, err = analyzer.ConvertForDynamoDB(entity, config)
			if err != nil {
				return fmt.Errorf("failed to convert entity: %w", err)
			}
		}

		// Ensure required keys are present
		if err := e.validateRequiredKeys(item, config); err != nil {
			return fmt.Errorf("validation failed: %w", err)
		}

		items = append(items, item)
	}

	// Write to DynamoDB
	if err := e.dynamoClient.PutItems(ctx, config.TargetTable, items, dryRun); err != nil {
		return fmt.Errorf("failed to put items to DynamoDB: %w", err)
	}

	return nil
}

// validateRequiredKeys ensures that required keys are present in the item
func (e *Engine) validateRequiredKeys(item map[string]interface{}, config interfaces.MigrationConfig) error {
	// Check partition key
	if _, exists := item[config.KeySelection.PartitionKey]; !exists {
		return fmt.Errorf("partition key '%s' not found in item", config.KeySelection.PartitionKey)
	}

	// Check sort key if required
	if config.KeySelection.SortKey != nil && *config.KeySelection.SortKey != "" {
		if _, exists := item[*config.KeySelection.SortKey]; !exists {
			return fmt.Errorf("sort key '%s' not found in item", *config.KeySelection.SortKey)
		}
	}

	return nil
}

// MigrateAll migrates multiple configurations concurrently
func (e *Engine) MigrateAll(ctx context.Context, configs []interfaces.MigrationConfig, dryRun bool) (<-chan interfaces.MigrationProgress, error) {
	if len(configs) == 0 {
		return nil, fmt.Errorf("no configurations provided")
	}

	// Validate all configurations first
	for i, config := range configs {
		if err := e.ValidateConfig(config); err != nil {
			return nil, fmt.Errorf("invalid configuration at index %d: %w", i, err)
		}
	}

	// Create a combined progress channel
	combinedProgress := make(chan interfaces.MigrationProgress, len(configs)*10)

	go func() {
		defer close(combinedProgress)

		var wg sync.WaitGroup

		// Start migration for each configuration
		for _, config := range configs {
			wg.Add(1)
			go func(cfg interfaces.MigrationConfig) {
				defer wg.Done()

				progressChan, err := e.Migrate(ctx, cfg, dryRun)
				if err != nil {
					// Send error as a completed migration with errors
					errorProgress := interfaces.MigrationProgress{
						Kind:       cfg.SourceKind,
						Processed:  0,
						Total:      cfg.Schema.Count,
						Errors:     1,
						InProgress: false,
						Completed:  true,
					}
					combinedProgress <- errorProgress
					return
				}

				// Forward progress updates
				for progress := range progressChan {
					select {
					case combinedProgress <- progress:
					case <-ctx.Done():
						return
					}
				}
			}(config)
		}

		// Wait for all migrations to complete
		wg.Wait()
	}()

	return combinedProgress, nil
}

// GetMigrationStats returns statistics about the migration
func (e *Engine) GetMigrationStats(configs []interfaces.MigrationConfig) MigrationStats {
	var totalEntities, totalKinds int64

	for _, config := range configs {
		totalKinds++
		totalEntities += config.Schema.Count
	}

	return MigrationStats{
		TotalKinds:    totalKinds,
		TotalEntities: totalEntities,
		StartTime:     time.Now(),
	}
}

// MigrationStats holds statistics about a migration
type MigrationStats struct {
	TotalKinds    int64     `json:"total_kinds"`
	TotalEntities int64     `json:"total_entities"`
	StartTime     time.Time `json:"start_time"`
	EndTime       time.Time `json:"end_time,omitempty"`
}
