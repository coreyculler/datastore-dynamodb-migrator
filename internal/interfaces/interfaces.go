package interfaces

import (
	"context"
)

// FieldInfo represents information about a field in a DataStore entity
type FieldInfo struct {
	Name     string      `json:"name"`
	TypeName string      `json:"type_name"`
	Sample   interface{} `json:"sample,omitempty"`
}

// KindSchema represents the schema information for a DataStore Kind
type KindSchema struct {
	Name   string      `json:"name"`
	Fields []FieldInfo `json:"fields"`
	Count  int64       `json:"count"`
}

// KeySelection represents the user's choice for primary and sort keys
type KeySelection struct {
	PartitionKey string  `json:"partition_key"`
	SortKey      *string `json:"sort_key,omitempty"`
}

// MigrationConfig holds configuration for a single Kind migration
type MigrationConfig struct {
	SourceKind   string       `json:"source_kind"`
	TargetTable  string       `json:"target_table"`
	KeySelection KeySelection `json:"key_selection"`
	Schema       KindSchema   `json:"schema"`
}

// MigrationProgress tracks the progress of a migration
type MigrationProgress struct {
	Kind       string `json:"kind"`
	Processed  int64  `json:"processed"`
	Total      int64  `json:"total"`
	Errors     int64  `json:"errors"`
	InProgress bool   `json:"in_progress"`
	Completed  bool   `json:"completed"`
}

// DataStoreClient interface for interacting with GCP DataStore
type DataStoreClient interface {
	ListKinds(ctx context.Context) ([]string, error)
	AnalyzeKind(ctx context.Context, kind string) (*KindSchema, error)
	GetEntities(ctx context.Context, kind string, batchSize int) (<-chan interface{}, error)
	Close() error
}

// DynamoDBClient interface for interacting with AWS DynamoDB
type DynamoDBClient interface {
	CreateTable(ctx context.Context, config MigrationConfig, dryRun bool) error
	TableExists(ctx context.Context, tableName string) (bool, error)
	PutItems(ctx context.Context, tableName string, items []map[string]interface{}, dryRun bool) error
	Close() error
}

// Introspector interface for analyzing entity schemas
type Introspector interface {
	AnalyzeEntity(entity interface{}) FieldInfo
	GetFieldValue(entity interface{}, fieldName string) interface{}
	ConvertForDynamoDB(entity interface{}, config MigrationConfig) (map[string]interface{}, error)
}

// MigrationEngine interface for orchestrating migrations
type MigrationEngine interface {
	Migrate(ctx context.Context, config MigrationConfig, dryRun bool) (<-chan MigrationProgress, error)
	MigrateAll(ctx context.Context, configs []MigrationConfig, dryRun bool) (<-chan MigrationProgress, error)
	ValidateConfig(config MigrationConfig) error
	SetDryRun(dryRun bool)
}
