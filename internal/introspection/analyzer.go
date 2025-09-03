package introspection

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
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

		// Do not inject additional metadata fields
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

		// Skip DataStore wrapper fields that shouldn't be migrated
		if a.isDataStoreWrapperField(fieldType.Name, fieldName) {
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

// isDataStoreWrapperField checks if a field is a DataStore wrapper field that should be skipped
func (a *EntityAnalyzer) isDataStoreWrapperField(structFieldName, jsonFieldName string) bool {
	// Skip common DataStore wrapper fields that shouldn't be migrated to DynamoDB
	wrapperFields := map[string]bool{
		"Key":        true, // DataStore key wrapper
		"Properties": true, // DataStore properties wrapper
		"Values":     true, // Common DataStore field name
		"key":        true, // lowercase variants
		"properties": true,
		"values":     true,
	}

	// Check both the struct field name and the JSON tag name
	return wrapperFields[structFieldName] || wrapperFields[jsonFieldName]
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

	// Handle pointers first
	val := reflect.ValueOf(value)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, nil
		}
		// Try to convert the pointed-to value
		return a.convertValueForDynamoDB(val.Elem().Interface())
	}

	// Handle DataStore nested entities before falling back to reflection
	if dsEntity, ok := value.(*datastore.Entity); ok {
		return a.convertPropertyListToMap(dsEntity.Properties)
	}
	// Also handle non-pointer datastore.Entity values
	if dsEntityVal, ok := value.(datastore.Entity); ok {
		return a.convertPropertyListToMap(dsEntityVal.Properties)
	}
	if propList, ok := value.(datastore.PropertyList); ok {
		return a.convertPropertyListToMap(propList)
	}

	switch val.Kind() {
	case reflect.String:
		// Check if the string is a serialized Datastore PropertyList/Entity and repair it
		if str, ok := value.(string); ok {
			trim := strings.TrimSpace(str)
			if strings.Contains(trim, "\"Properties\":") || strings.HasPrefix(trim, "{\"Key\":") {
				if repaired, ok := a.tryRepairDatastorePropertyListString(trim); ok {
					return repaired, nil
				}
			}
			// Otherwise, if the string is valid JSON, unmarshal as JSON value
			if a.isValidJSON(str) {
				var jsonData interface{}
				if json.Unmarshal([]byte(str), &jsonData) == nil {
					return jsonData, nil
				}
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
		// Recursively convert slice elements
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
		// Recursively convert map values
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
		// Special handling for time.Time
		if t, ok := value.(time.Time); ok {
			return t.Unix(), nil
		}
		// For other structs, convert to a map
		return a.structToMap(value)
	default:
		return value, nil
	}
}

// tryRepairDatastorePropertyListString attempts to parse a serialized Datastore Entity/PropertyList JSON
// like {"Key":null,"Properties":[{"Name":"field","Value":...,"NoIndex":true}, ...]}
// and rebuild a clean map[string]interface{} from the Properties array.
func (a *EntityAnalyzer) tryRepairDatastorePropertyListString(s string) (map[string]interface{}, bool) {
	type dsProp struct {
		Name    string      `json:"Name"`
		Value   interface{} `json:"Value"`
		NoIndex bool        `json:"NoIndex"`
	}
	type dsWrapper struct {
		Key        interface{} `json:"Key"`
		Properties []dsProp    `json:"Properties"`
	}

	var w dsWrapper
	if err := json.Unmarshal([]byte(s), &w); err != nil {
		return nil, false
	}
	if len(w.Properties) == 0 {
		return nil, false
	}

	rebuilt := make(map[string]interface{})
	for _, p := range w.Properties {
		converted, err := a.convertValueForDynamoDB(p.Value)
		if err != nil {
			return nil, false
		}
		rebuilt[p.Name] = converted
	}
	return rebuilt, true
}

// convertPropertyListToMap converts a DataStore PropertyList to a map[string]interface{}
func (a *EntityAnalyzer) convertPropertyListToMap(propList datastore.PropertyList) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for _, prop := range propList {
		// Recursively convert the property value
		convertedValue, err := a.convertValueForDynamoDB(prop.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to convert property %s: %w", prop.Name, err)
		}
		result[prop.Name] = convertedValue
	}

	return result, nil
}

// structToMap converts a struct to a map[string]interface{}
func (a *EntityAnalyzer) structToMap(entity interface{}) (map[string]interface{}, error) {
	if dsEntity, ok := entity.(*datastore.Entity); ok {
		return a.convertPropertyListToMap(dsEntity.Properties)
	}

	result := make(map[string]interface{})
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil, nil
		}
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
		if fieldName == "-" || a.isDataStoreWrapperField(fieldType.Name, fieldName) {
			continue
		}

		value, err := a.convertValueForDynamoDB(field.Interface())
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", fieldName, err)
		}
		result[fieldName] = value
	}

	return result, nil
}

// isValidJSON checks if a string is valid JSON
func (a *EntityAnalyzer) isValidJSON(str string) bool {
	var js interface{}
	return json.Unmarshal([]byte(str), &js) == nil
}
