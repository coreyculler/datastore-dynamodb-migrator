package interfaces

import (
	"context"
	"time"
)

// TestFieldInfo creates sample FieldInfo for testing
func TestFieldInfo(name, typeName string, sample interface{}) FieldInfo {
	return FieldInfo{
		Name:     name,
		TypeName: typeName,
		Sample:   sample,
	}
}

// TestKindSchema creates sample KindSchema for testing
func TestKindSchema(name string, count int64, fields ...FieldInfo) *KindSchema {
	return &KindSchema{
		Name:   name,
		Fields: fields,
		Count:  count,
	}
}

// TestKeySelection creates sample KeySelection for testing
func TestKeySelection(partitionKey string, sortKey *string) KeySelection {
	return KeySelection{
		PartitionKey: partitionKey,
		SortKey:      sortKey,
	}
}

// TestMigrationConfig creates sample MigrationConfig for testing
func TestMigrationConfig(sourceKind, targetTable string, keySelection KeySelection, schema *KindSchema) MigrationConfig {
	return MigrationConfig{
		SourceKind:   sourceKind,
		TargetTable:  targetTable,
		KeySelection: keySelection,
		Schema:       *schema,
	}
}

// TestMigrationProgress creates sample MigrationProgress for testing
func TestMigrationProgress(kind string, processed, total, errors int64, inProgress, completed bool) MigrationProgress {
	return MigrationProgress{
		Kind:       kind,
		Processed:  processed,
		Total:      total,
		Errors:     errors,
		InProgress: inProgress,
		Completed:  completed,
	}
}

// Sample test data
var (
	// Sample field definitions
	SampleStringField = TestFieldInfo("id", "string", "user_123")
	SampleIntField    = TestFieldInfo("age", "int64", int64(25))
	SampleFloatField  = TestFieldInfo("score", "float64", 98.5)
	SampleBoolField   = TestFieldInfo("active", "bool", true)
	SampleTimeField   = TestFieldInfo("created_at", "time.Time", time.Now())
	SampleMapField    = TestFieldInfo("metadata", "map[string]interface{}", map[string]interface{}{"key": "value"})
	SampleSliceField  = TestFieldInfo("tags", "[]string", []string{"tag1", "tag2"})

	// Sample schemas
	SampleUserSchema = TestKindSchema("User", 1000,
		SampleStringField,
		SampleIntField,
		SampleBoolField,
		SampleTimeField,
	)

	SampleOrderSchema = TestKindSchema("Order", 500,
		TestFieldInfo("order_id", "string", "order_123"),
		TestFieldInfo("user_id", "string", "user_456"),
		TestFieldInfo("amount", "float64", 99.99),
		TestFieldInfo("status", "string", "completed"),
		SampleTimeField,
	)

	// Sample key selections
	SampleUserKeySelection  = TestKeySelection("id", nil)
	SampleOrderKeySelection = TestKeySelection("order_id", stringPtr("created_at"))

	// Sample migration configs
	SampleUserMigrationConfig = TestMigrationConfig(
		"User",
		"Users",
		SampleUserKeySelection,
		SampleUserSchema,
	)

	SampleOrderMigrationConfig = TestMigrationConfig(
		"Order",
		"Orders",
		SampleOrderKeySelection,
		SampleOrderSchema,
	)
)

// Helper functions for creating test data

// stringPtr returns a pointer to the given string
func stringPtr(s string) *string {
	return &s
}

// CreateTestEntity creates a sample entity map for testing
func CreateTestEntity() map[string]interface{} {
	return map[string]interface{}{
		"id":         "test_123",
		"name":       "Test User",
		"email":      "test@example.com",
		"age":        int64(30),
		"active":     true,
		"created_at": time.Now(),
		"metadata": map[string]interface{}{
			"source":  "test",
			"version": 1,
		},
		"tags": []string{"test", "user"},
	}
}

// CreateTestEntityChannel creates a channel with test entities for testing GetEntities
func CreateTestEntityChannel(count int) <-chan interface{} {
	ch := make(chan interface{}, count)
	go func() {
		defer close(ch)
		for i := 0; i < count; i++ {
			entity := CreateTestEntity()
			entity["id"] = entity["id"].(string) + "_" + string(rune(i))
			ch <- entity
		}
	}()
	return ch
}

// CreateTestProgressChannel creates a channel with test progress updates
func CreateTestProgressChannel(kind string, total int64) <-chan MigrationProgress {
	ch := make(chan MigrationProgress, int(total)+2) // +2 for start and end
	go func() {
		defer close(ch)

		// Start progress
		ch <- TestMigrationProgress(kind, 0, total, 0, true, false)

		// Incremental progress
		for i := int64(1); i <= total; i++ {
			ch <- TestMigrationProgress(kind, i, total, 0, true, false)
			time.Sleep(time.Millisecond) // Small delay to simulate work
		}

		// Completion progress
		ch <- TestMigrationProgress(kind, total, total, 0, false, true)
	}()
	return ch
}

// CreateTestContext creates a context with timeout for testing
func CreateTestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// CreateTestContextWithCancel creates a cancellable context for testing
func CreateTestContextWithCancel() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// AssertFieldInfoEqual compares two FieldInfo structs for testing
func AssertFieldInfoEqual(expected, actual FieldInfo) bool {
	return expected.Name == actual.Name &&
		expected.TypeName == actual.TypeName &&
		expected.Sample == actual.Sample
}

// AssertKindSchemaEqual compares two KindSchema structs for testing
func AssertKindSchemaEqual(expected, actual *KindSchema) bool {
	if expected == nil && actual == nil {
		return true
	}
	if expected == nil || actual == nil {
		return false
	}

	if expected.Name != actual.Name || expected.Count != actual.Count {
		return false
	}

	if len(expected.Fields) != len(actual.Fields) {
		return false
	}

	for i, expectedField := range expected.Fields {
		if !AssertFieldInfoEqual(expectedField, actual.Fields[i]) {
			return false
		}
	}

	return true
}

// AssertMigrationConfigEqual compares two MigrationConfig structs for testing
func AssertMigrationConfigEqual(expected, actual MigrationConfig) bool {
	if expected.SourceKind != actual.SourceKind || expected.TargetTable != actual.TargetTable {
		return false
	}

	if expected.KeySelection.PartitionKey != actual.KeySelection.PartitionKey {
		return false
	}

	// Check sort key pointers
	if expected.KeySelection.SortKey == nil && actual.KeySelection.SortKey == nil {
		// Both nil, OK
	} else if expected.KeySelection.SortKey != nil && actual.KeySelection.SortKey != nil {
		if *expected.KeySelection.SortKey != *actual.KeySelection.SortKey {
			return false
		}
	} else {
		// One nil, one not
		return false
	}

	return AssertKindSchemaEqual(&expected.Schema, &actual.Schema)
}
