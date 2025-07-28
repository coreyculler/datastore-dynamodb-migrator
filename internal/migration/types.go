package migration

import (
	"reflect"

	"github.com/coreyculler/datastore-dynamodb-migrator/internal/interfaces"
)

// Re-export the interfaces and types for backward compatibility
type FieldInfo = interfaces.FieldInfo
type KindSchema = interfaces.KindSchema
type KeySelection = interfaces.KeySelection
type MigrationConfig = interfaces.MigrationConfig
type MigrationProgress = interfaces.MigrationProgress
type DataStoreClient = interfaces.DataStoreClient
type DynamoDBClient = interfaces.DynamoDBClient
type Introspector = interfaces.Introspector
type MigrationEngine = interfaces.MigrationEngine

// Additional FieldInfo with reflection type for internal use
type FieldInfoWithType struct {
	interfaces.FieldInfo
	Type reflect.Type `json:"-"`
}
