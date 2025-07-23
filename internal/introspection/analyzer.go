package introspection

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"datastore-dynamodb-migrator/internal/interfaces"
)

// EntityAnalyzer implements the Introspector interface
type EntityAnalyzer struct{}

// NewEntityAnalyzer creates a new EntityAnalyzer
func NewEntityAnalyzer() *EntityAnalyzer {
	return &EntityAnalyzer{}
}

// AnalyzeEntity analyzes a single entity and returns field information
func (a *EntityAnalyzer) AnalyzeEntity(entity interface{}) interfaces.FieldInfo {
	if entity == nil {
		return interfaces.FieldInfo{}
	}

	val := reflect.ValueOf(entity)
	typ := reflect.TypeOf(entity)

	// Handle pointers
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return interfaces.FieldInfo{
				TypeName: typ.String(),
			}
		}
		val = val.Elem()
		typ = typ.Elem()
	}

	return interfaces.FieldInfo{
		TypeName: a.getTypeName(typ),
		Sample:   entity,
	}
}

// AnalyzeStruct analyzes a struct and returns all field information
func (a *EntityAnalyzer) AnalyzeStruct(entity interface{}) []interfaces.FieldInfo {
	if entity == nil {
		return []interfaces.FieldInfo{}
	}

	val := reflect.ValueOf(entity)
	typ := reflect.TypeOf(entity)

	// Handle pointers
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return []interfaces.FieldInfo{}
		}
		val = val.Elem()
		typ = typ.Elem()
	}

	if val.Kind() != reflect.Struct {
		// For non-struct types, return a single field info
		return []interfaces.FieldInfo{a.AnalyzeEntity(entity)}
	}

	var fields []interfaces.FieldInfo

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		// Skip unexported fields
		if !field.CanInterface() {
			continue
		}

		// Get the field name (check for datastore tag first)
		fieldName := a.getFieldName(fieldType)

		// Skip fields marked as ignored
		if fieldName == "-" {
			continue
		}

		var sample interface{}
		if field.IsValid() && field.CanInterface() {
			sample = field.Interface()
		}

		fieldInfo := interfaces.FieldInfo{
			Name:     fieldName,
			TypeName: a.getTypeName(fieldType.Type),
			Sample:   sample,
		}

		fields = append(fields, fieldInfo)
	}

	return fields
}

// GetFieldValue retrieves the value of a specific field from an entity
func (a *EntityAnalyzer) GetFieldValue(entity interface{}, fieldName string) interface{} {
	if entity == nil {
		return nil
	}

	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil
	}

	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		if !field.CanInterface() {
			continue
		}

		// Check if this is the field we're looking for
		if a.getFieldName(fieldType) == fieldName {
			return field.Interface()
		}
	}

	return nil
}

// ConvertForDynamoDB converts an entity to DynamoDB format
func (a *EntityAnalyzer) ConvertForDynamoDB(entity interface{}, config interfaces.MigrationConfig) (map[string]interface{}, error) {
	if entity == nil {
		return nil, fmt.Errorf("entity is nil")
	}

	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("entity pointer is nil")
		}
		val = val.Elem()
	}

	result := make(map[string]interface{})

	if val.Kind() != reflect.Struct {
		// For non-struct types, use the partition key as the field name
		result[config.KeySelection.PartitionKey] = entity
		return result, nil
	}

	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		if !field.CanInterface() {
			continue
		}

		fieldName := a.getFieldName(fieldType)
		if fieldName == "-" {
			continue
		}

		value := field.Interface()

		// Convert the value to DynamoDB-compatible format
		convertedValue, err := a.convertValueForDynamoDB(value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", fieldName, err)
		}

		result[fieldName] = convertedValue
	}

	return result, nil
}

// getFieldName returns the field name to use, checking for datastore tags
func (a *EntityAnalyzer) getFieldName(field reflect.StructField) string {
	// Check for datastore tag
	if tag := field.Tag.Get("datastore"); tag != "" {
		// Handle tag options like "name,noindex"
		parts := strings.Split(tag, ",")
		if parts[0] != "" {
			return parts[0]
		}
	}

	// Check for json tag as fallback
	if tag := field.Tag.Get("json"); tag != "" {
		parts := strings.Split(tag, ",")
		if parts[0] != "" && parts[0] != "-" {
			return parts[0]
		}
	}

	// Use the struct field name
	return field.Name
}

// getTypeName returns a human-readable type name
func (a *EntityAnalyzer) getTypeName(typ reflect.Type) string {
	// Handle pointers
	if typ.Kind() == reflect.Ptr {
		return "*" + a.getTypeName(typ.Elem())
	}

	// Handle slices
	if typ.Kind() == reflect.Slice {
		return "[]" + a.getTypeName(typ.Elem())
	}

	// Handle maps
	if typ.Kind() == reflect.Map {
		return fmt.Sprintf("map[%s]%s",
			a.getTypeName(typ.Key()),
			a.getTypeName(typ.Elem()))
	}

	// For basic types, return the kind name
	switch typ.Kind() {
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "int"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "uint"
	case reflect.Float32, reflect.Float64:
		return "float"
	case reflect.Bool:
		return "bool"
	case reflect.Struct:
		// Check for common types
		if typ == reflect.TypeOf(time.Time{}) {
			return "time.Time"
		}
		return typ.Name()
	default:
		return typ.String()
	}
}

// convertValueForDynamoDB converts values to DynamoDB-compatible formats
func (a *EntityAnalyzer) convertValueForDynamoDB(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	val := reflect.ValueOf(value)

	// Handle pointers
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, nil
		}
		return a.convertValueForDynamoDB(val.Elem().Interface())
	}

	switch val.Kind() {
	case reflect.String, reflect.Bool:
		return value, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return val.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(val.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return val.Float(), nil
	case reflect.Slice:
		// Convert slice elements
		slice := make([]interface{}, val.Len())
		for i := 0; i < val.Len(); i++ {
			converted, err := a.convertValueForDynamoDB(val.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			slice[i] = converted
		}
		return slice, nil
	case reflect.Map:
		// Convert map to string keys
		result := make(map[string]interface{})
		for _, key := range val.MapKeys() {
			keyStr := fmt.Sprintf("%v", key.Interface())
			converted, err := a.convertValueForDynamoDB(val.MapIndex(key).Interface())
			if err != nil {
				return nil, err
			}
			result[keyStr] = converted
		}
		return result, nil
	case reflect.Struct:
		// Handle time.Time specially
		if t, ok := value.(time.Time); ok {
			return t.Unix(), nil
		}
		// For other structs, convert to map
		return a.structToMap(value)
	default:
		// For other types, convert to string
		return fmt.Sprintf("%v", value), nil
	}
}

// structToMap converts a struct to a map[string]interface{}
func (a *EntityAnalyzer) structToMap(entity interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("expected struct, got %v", val.Kind())
	}

	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)

		if !field.CanInterface() {
			continue
		}

		fieldName := a.getFieldName(fieldType)
		if fieldName == "-" {
			continue
		}

		converted, err := a.convertValueForDynamoDB(field.Interface())
		if err != nil {
			return nil, err
		}

		result[fieldName] = converted
	}

	return result, nil
}
