package migration

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/coreyculler/datastore-dynamodb-migrator/internal/interfaces"
)

// Engine implements the MigrationEngine interface and orchestrates migrations
type Engine struct {
	datastoreClient interfaces.DataStoreClient
	dynamoClient    interfaces.DynamoDBClient
	s3Client        interfaces.S3Client
	analyzer        interfaces.Introspector
	batchSize       int
	maxWorkers      int
	dryRun          bool
	debug           bool
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

// SetS3Client sets the S3 client (optional)
func (e *Engine) SetS3Client(s3 interfaces.S3Client) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.s3Client = s3
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

// SetDebug sets the debug mode for verbose logging.
func (e *Engine) SetDebug(debug bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.debug = debug
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

	if config.Schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}

	// Validate that partition key source exists in schema (fallback to alias for backwards-compat)
	sourceName := config.KeySelection.PartitionKeySource
	if sourceName == "" {
		sourceName = config.KeySelection.PartitionKey
	}
	partitionKeyExists := false
	for _, field := range config.Schema.Fields {
		if field.Name == sourceName {
			partitionKeyExists = true
			break
		}
	}
	if !partitionKeyExists {
		return fmt.Errorf("partition key '%s' not found in schema", sourceName)
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

		// Ensure sort key is different from partition key alias
		if *config.KeySelection.SortKey == config.KeySelection.PartitionKey {
			return fmt.Errorf("sort key cannot be the same as partition key")
		}
	}

	// Validate S3 settings if enabled
	if config.S3Storage != nil && config.S3Storage.Enabled {
		if config.S3Storage.Bucket == "" {
			return fmt.Errorf("S3 bucket must be specified when S3 storage is enabled")
		}
		if config.S3Storage.ObjectPrefix == "" {
			return fmt.Errorf("S3 object prefix must be specified when S3 storage is enabled")
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

		totalProcessed := int64(0)
		totalErrors := int64(0)

		// Helper function to send progress non-blocking
		sendProgress := func(progress interfaces.MigrationProgress) {
			select {
			case progressChan <- progress:
			case <-ctx.Done():
				return
			default:
				// Channel is full, skip this update
			}
		}

		// Send initial progress
		progress := interfaces.MigrationProgress{
			Kind:       config.SourceKind,
			Processed:  0,
			Total:      config.Schema.Count,
			Errors:     0,
			InProgress: true,
			Completed:  false,
		}
		sendProgress(progress)

		// Check for early cancellation
		select {
		case <-ctx.Done():
			progress.InProgress = false
			progress.Completed = true
			sendProgress(progress)
			return
		default:
		}

		// Create DynamoDB table
		if err := e.dynamoClient.CreateTable(ctx, config, dryRun); err != nil {
			// Check if error is due to context cancellation
			if ctx.Err() != nil {
				progress.InProgress = false
				progress.Completed = true
				sendProgress(progress)
				return
			}
			if e.debug {
				fmt.Printf("\nDEBUG: CreateTable failed for Kind %s: %v\n", config.SourceKind, err)
			}
			progress.Errors++
			progress.InProgress = false
			progress.Completed = true
			sendProgress(progress)
			return
		}

		// Check for cancellation after table creation
		select {
		case <-ctx.Done():
			progress.InProgress = false
			progress.Completed = true
			sendProgress(progress)
			return
		default:
		}

		// Check for cancellation before processing entities
		select {
		case <-ctx.Done():
			progress.InProgress = false
			progress.Completed = true
			sendProgress(progress)
			return
		default:
		}

		// In dry-run mode, skip actual entity processing for faster response
		if dryRun {
			// Simulate processing without actually reading all entities
			fmt.Printf("🔍 DRY RUN: Would process %d entities from Kind %s\n",
				config.Schema.Count, config.SourceKind)

			// Send incremental progress updates to simulate processing
			batchCount := (config.Schema.Count / int64(batchSize)) + 1
			for i := int64(0); i < batchCount; i++ {
				// Check for cancellation frequently during simulation
				select {
				case <-ctx.Done():
					progress.InProgress = false
					progress.Completed = true
					sendProgress(progress)
					return
				default:
				}

				processed := (i + 1) * int64(batchSize)
				if processed > config.Schema.Count {
					processed = config.Schema.Count
				}

				// Update the outer progress variable so final progress is correct
				progress = interfaces.MigrationProgress{
					Kind:       config.SourceKind,
					Processed:  processed,
					Total:      config.Schema.Count,
					Errors:     0,
					InProgress: processed < config.Schema.Count,
					Completed:  processed >= config.Schema.Count,
				}
				sendProgress(progress)

				// Small delay to simulate processing but allow fast cancellation
				select {
				case <-ctx.Done():
					progress.InProgress = false
					progress.Completed = true
					sendProgress(progress)
					return
				case <-time.After(10 * time.Millisecond):
					// Continue simulation
				}
			}
			totalProcessed = config.Schema.Count // In dry-run, we assume all are processed
		} else {
			// Normal mode: actually process entities
			entityChan, err := e.datastoreClient.GetEntities(ctx, config.SourceKind, batchSize, config.DatastoreOrder)
			if err != nil {
				// Check if error is due to context cancellation
				if ctx.Err() != nil {
					progress.InProgress = false
					progress.Completed = true
					sendProgress(progress)
					return // Exit without sending a final report if context is cancelled.
				}
				if e.debug {
					fmt.Printf("\nDEBUG: GetEntities failed for Kind %s: %v\n", config.SourceKind, err)
				}
				totalErrors++
				// Do not return here. Let the function proceed to the final progress report.
			} else {
				// Process entities in batches using workers
				processedCount, errorCount := e.processEntitiesWithWorkers(ctx, entityChan, config, progressChan, batchSize, maxWorkers, analyzer, dryRun)
				totalProcessed = processedCount
				totalErrors += errorCount
			}
		}

		// Send final progress report for this Kind
		finalProgress := interfaces.MigrationProgress{
			Kind:       config.SourceKind,
			Processed:  totalProcessed,
			Total:      config.Schema.Count,
			Errors:     totalErrors,
			InProgress: false,
			Completed:  true,
		}
		sendProgress(finalProgress)
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
	dryRun bool) (int64, int64) {

	// Create work channels
	workChan := make(chan []interface{}, maxWorkers*2)
	var wg sync.WaitGroup

	// Track progress
	var processed, errors int64
	var mu sync.Mutex

	// Create a context for workers that can be cancelled
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	// Start workers
	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for {
				select {
				case <-workerCtx.Done():
					return
				case batch, ok := <-workChan:
					if !ok {
						return
					}

					if err := e.processBatch(workerCtx, batch, config, analyzer, dryRun); err != nil {
						// Check if error is due to context cancellation
						if workerCtx.Err() != nil {
							return
						}
						mu.Lock()
						errors++
						mu.Unlock()
						if e.debug {
							fmt.Printf("\nWorker %d: DEBUG - Error processing batch for Kind %s: %v\n", workerID, config.SourceKind, err)
						} else {
							fmt.Printf("Worker %d: Error processing batch: %v\n", workerID, err)
						}
					}

					mu.Lock()
					processed += int64(len(batch))
					currentProcessed := processed
					currentErrors := errors
					mu.Unlock()

					// Send progress update with non-blocking send
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
					case <-workerCtx.Done():
						return
					default:
						// Progress channel full, continue anyway
					}
				}
			}
		}(i)
	}

	// Collect entities into batches and send to workers
	go func() {
		defer close(workChan)

		var batch []interface{}

		for {
			select {
			case <-ctx.Done():
				return
			case entity, ok := <-entityChan:
				if !ok {
					// Channel closed, send remaining batch
					if len(batch) > 0 {
						select {
						case workChan <- batch:
						case <-ctx.Done():
							return
						}
					}
					return
				}

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
		}
	}()

	// Wait for all workers to complete or context cancellation
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All workers completed normally
	case <-ctx.Done():
		// Context cancelled, cancel workers and wait for them to exit
		cancelWorkers()
		<-done
	}

	return processed, errors
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

		// Always use the analyzer to convert the entity to ensure consistent normalization
		item, err = analyzer.ConvertForDynamoDB(entity, config)
		if err != nil {
			return fmt.Errorf("failed to convert entity: %w", err)
		}

		// Ensure required keys are present on the item map (inject if missing)
		ensurePartitionKey(item, config)

		// If S3 storage is enabled, upload full item JSON and add S3ObjectPath
		if config.S3Storage != nil && config.S3Storage.Enabled {
			e.mu.RLock()
			s3 := e.s3Client
			e.mu.RUnlock()
			if s3 == nil {
				return fmt.Errorf("S3 storage is enabled but no S3 client is configured")
			}
			pk := stringifyValue(item[config.KeySelection.PartitionKey])
			objectKey := fmt.Sprintf("%s/%s.json", config.S3Storage.ObjectPrefix, pk)
			path, err := s3.PutJSON(ctx, config.S3Storage.Bucket, objectKey, item, dryRun)
			if err != nil {
				return fmt.Errorf("failed to upload S3 object for key %s: %w", pk, err)
			}
			item["S3ObjectPath"] = path
		}

		// Cleanup metadata/internal fields
		cleanupMetadataFields(item, config)

		// Apply projection filtering if configured
		if len(config.DynamoDBProjectionFields) > 0 {
			item = projectItem(item, config)
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

// ensurePartitionKey ensures the configured partition key exists on the item as a string.
func ensurePartitionKey(item map[string]interface{}, config interfaces.MigrationConfig) {
	pkName := config.KeySelection.PartitionKey
	if pkName == "" {
		return
	}
	if _, exists := item[pkName]; exists {
		// normalize to string if possible
		item[pkName] = stringifyValue(item[pkName])
		return
	}
	// Try source then common sources
	candidates := []string{}
	if config.KeySelection.PartitionKeySource != "" {
		candidates = append(candidates, config.KeySelection.PartitionKeySource)
	}
	candidates = append(candidates, "PK", "__key_name__", "__key_id__", "__key__", "id")
	for _, c := range candidates {
		if v, ok := item[c]; ok {
			item[pkName] = stringifyValue(v)
			return
		}
	}
}

func stringifyValue(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case int:
		return fmt.Sprintf("%d", t)
	case int8:
		return fmt.Sprintf("%d", t)
	case int16:
		return fmt.Sprintf("%d", t)
	case int32:
		return fmt.Sprintf("%d", t)
	case int64:
		return fmt.Sprintf("%d", t)
	case uint:
		return fmt.Sprintf("%d", t)
	case uint8:
		return fmt.Sprintf("%d", t)
	case uint16:
		return fmt.Sprintf("%d", t)
	case uint32:
		return fmt.Sprintf("%d", t)
	case uint64:
		return fmt.Sprintf("%d", t)
	case float32:
		f := float64(t)
		if math.Trunc(f) == f {
			return fmt.Sprintf("%d", int64(f))
		}
		return fmt.Sprintf("%g", f)
	case float64:
		if math.Trunc(t) == t {
			return fmt.Sprintf("%d", int64(t))
		}
		return fmt.Sprintf("%g", t)
	case fmt.Stringer:
		return t.String()
	default:
		return fmt.Sprintf("%v", t)
	}
}

// cleanupMetadataFields removes internal metadata fields (e.g., __key__*) and synthetic PK when aliased
func cleanupMetadataFields(item map[string]interface{}, config interfaces.MigrationConfig) {
	// Remove any fields that start with "__"
	for k := range item {
		if len(k) >= 2 && k[0] == '_' && k[1] == '_' {
			delete(item, k)
		}
	}
	// If user renamed PK alias, remove synthetic PK key field if present
	if config.KeySelection.PartitionKey != "PK" {
		delete(item, "PK")
	}
}

// projectItem returns a filtered item containing only projection fields, keys, and S3ObjectPath
func projectItem(full map[string]interface{}, config interfaces.MigrationConfig) map[string]interface{} {
	result := make(map[string]interface{})
	// Always include keys
	result[config.KeySelection.PartitionKey] = full[config.KeySelection.PartitionKey]
	if config.KeySelection.SortKey != nil && *config.KeySelection.SortKey != "" {
		if v, ok := full[*config.KeySelection.SortKey]; ok {
			result[*config.KeySelection.SortKey] = v
		}
	}
	// Include S3ObjectPath if present
	if v, ok := full["S3ObjectPath"]; ok {
		result["S3ObjectPath"] = v
	}
	allowed := map[string]struct{}{}
	for _, f := range config.DynamoDBProjectionFields {
		allowed[f] = struct{}{}
	}
	for f := range allowed {
		if f == config.KeySelection.PartitionKey {
			continue
		}
		if config.KeySelection.SortKey != nil && *config.KeySelection.SortKey == f {
			continue
		}
		if v, ok := full[f]; ok {
			result[f] = v
		}
	}
	return result
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

		// Helper function to send progress non-blocking
		sendProgress := func(progress interfaces.MigrationProgress) {
			select {
			case combinedProgress <- progress:
			case <-ctx.Done():
			default:
				// Channel is full, skip this update
			}
		}

		// Start migration for each configuration
		for _, config := range configs {
			// Check for cancellation before starting each migration
			select {
			case <-ctx.Done():
				return
			default:
			}

			wg.Add(1)
			go func(cfg interfaces.MigrationConfig) {
				defer wg.Done()

				progressChan, err := e.Migrate(ctx, cfg, dryRun)
				if err != nil {
					// Check if error is due to context cancellation
					if ctx.Err() != nil {
						return
					}
					// Send error as a completed migration with errors
					errorProgress := interfaces.MigrationProgress{
						Kind:       cfg.SourceKind,
						Processed:  0,
						Total:      cfg.Schema.Count,
						Errors:     1,
						InProgress: false,
						Completed:  true,
					}
					sendProgress(errorProgress)
					return
				}

				// Forward progress updates with context cancellation
				for {
					select {
					case <-ctx.Done():
						return // Exit if the main context is cancelled
					case progress, ok := <-progressChan:
						if !ok {
							// Channel closed, migration for this config is done
							return
						}
						// Check for cancellation before forwarding each update
						select {
						case <-ctx.Done():
							return
						default:
						}
						sendProgress(progress)
					}
				}
			}(config)
		}

		// Wait for all migrations to complete or context cancellation
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// All migrations completed normally
		case <-ctx.Done():
			// Context cancelled, wait for goroutines to exit
			<-done
		}
	}()

	return combinedProgress, nil
}

// GetMigrationStats returns statistics about the migration
func (e *Engine) GetMigrationStats(configs []interfaces.MigrationConfig) MigrationStats {
	var totalEntities, totalKinds int64

	for _, config := range configs {
		totalKinds++
		if config.Schema != nil {
			totalEntities += config.Schema.Count
		}
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
