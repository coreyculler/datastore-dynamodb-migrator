package introspection

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/coreyculler/datastore-dynamodb-migrator/internal/interfaces"
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

	// Handle EntityWithKey from datastore
	if entityWithKey, ok := entity.(interface {
		ToMap() map[string]interface{}
	}); ok {
		entityMap := entityWithKey.ToMap()

		// For regular properties, get from the map first
		if value, exists := entityMap[fieldName]; exists {
			// If this is not an internal DataStore field, return it
			if !strings.HasPrefix(fieldName, "__") {
				return value
			}
		}

		// Handle DataStore key identifier fields (id or __key__) when no regular property exists
		if fieldName == "id" || fieldName == "__key__" {
			// Only extract from key if there's no regular property with this name
			if _, exists := entityMap[fieldName]; !exists || strings.HasPrefix(fieldName, "__") {
				if keyName, exists := entityMap["__key_name__"]; exists && keyName != "" {
					return keyName
				}
				if keyID, exists := entityMap["__key_id__"]; exists && keyID != 0 {
					return fmt.Sprintf("%v", keyID)
				}
				if keyStr, exists := entityMap["__key__"]; exists {
					return keyStr
				}
			}
		}

		// For internal DataStore fields, return from the map
		if strings.HasPrefix(fieldName, "__") {
			if value, exists := entityMap[fieldName]; exists {
				return value
			}
		}
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

	result := make(map[string]interface{})

	// Handle EntityWithKey from datastore
	if entityWithKey, ok := entity.(interface {
		ToMap() map[string]interface{}
	}); ok {
		entityMap := entityWithKey.ToMap()

		// Process all fields from the entity map
		for fieldName, value := range entityMap {
			// Skip internal datastore fields (they start with __)
			if strings.HasPrefix(fieldName, "__") {
				continue
			}

			// Convert the value to DynamoDB-compatible format
			convertedValue, err := a.convertValueForDynamoDB(value)
			if err != nil {
				return nil, fmt.Errorf("failed to convert field %s: %w", fieldName, err)
			}

			result[fieldName] = convertedValue
		}

		// Add the DataStore key identifier field
		// Try "id" first, then "__key__" as fallback
		keyFieldName := "id"
		if _, exists := result["id"]; exists {
			keyFieldName = "__key__"
		}

		keyValue := a.GetFieldValue(entity, keyFieldName)
		if keyValue != nil {
			convertedValue, err := a.convertValueForDynamoDB(keyValue)
			if err != nil {
				return nil, fmt.Errorf("failed to convert key field %s: %w", keyFieldName, err)
			}
			result[keyFieldName] = convertedValue
		}

		return result, nil
	}

	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, fmt.Errorf("entity pointer is nil")
		}
		val = val.Elem()
	}

	if val.Kind() != reflect.Struct {
		// Handle map[string]interface{} directly
		if entityMap, ok := entity.(map[string]interface{}); ok {
			// Process all fields from the entity map
			for fieldName, value := range entityMap {
				// Skip internal datastore fields (they start with __)
				if strings.HasPrefix(fieldName, "__") {
					continue
				}

				// Convert the value to DynamoDB-compatible format
				convertedValue, err := a.convertValueForDynamoDB(value)
				if err != nil {
					return nil, fmt.Errorf("failed to convert field %s: %w", fieldName, err)
				}

				result[fieldName] = convertedValue
			}
			return result, nil
		}

		// For other non-struct types, use the partition key as the field name
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
	case reflect.String:
		// Check if the string is already valid JSON
		if str, ok := value.(string); ok {
			if a.isValidJSON(str) {
				// If it's valid JSON, preserve it as-is
				return str, nil
			}
		}
		return value, nil
	case reflect.Bool:
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
		// For other types, try JSON marshaling first, then fallback to string
		if jsonData, err := json.Marshal(value); err == nil {
			return string(jsonData), nil
		}
		// Fallback to string conversion
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

// isValidJSON checks if a string is valid JSON
func (a *EntityAnalyzer) isValidJSON(str string) bool {
	var js interface{}
	return json.Unmarshal([]byte(str), &js) == nil
}
