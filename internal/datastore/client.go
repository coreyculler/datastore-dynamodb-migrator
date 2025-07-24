package datastore

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"datastore-dynamodb-migrator/internal/interfaces"

	"cloud.google.com/go/datastore"
)

// Client wraps the GCP DataStore client and implements the DataStoreClient interface
type Client struct {
	client    *datastore.Client
	projectID string
	mu        sync.RWMutex
}

// NewClient creates a new DataStore client
func NewClient(ctx context.Context, projectID string) (*Client, error) {
	if projectID == "" {
		return nil, fmt.Errorf("project ID is required")
	}

	client, err := datastore.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create datastore client: %w", err)
	}

	return &Client{
		client:    client,
		projectID: projectID,
	}, nil
}

// ListKinds lists all available Kinds in the DataStore
// Note: DataStore doesn't have a direct API to list all kinds, so we use a workaround
func (c *Client) ListKinds(ctx context.Context) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Use the __kind__ query to get all kinds
	query := datastore.NewQuery("__kind__").KeysOnly()

	keys, err := c.client.GetAll(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query kinds: %w", err)
	}

	// Filter out special DataStore kinds that begin with "__Stat"
	var kinds []string
	for _, key := range keys {
		kindName := key.Name
		// Skip special DataStore statistics kinds
		if !strings.HasPrefix(kindName, "__Stat") {
			kinds = append(kinds, kindName)
		}
	}

	return kinds, nil
}

// AnalyzeKind analyzes a specific Kind and returns its schema information
func (c *Client) AnalyzeKind(ctx context.Context, kind string) (*interfaces.KindSchema, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if kind == "" {
		return nil, fmt.Errorf("kind name is required")
	}

	// First, get the count of entities
	countQuery := datastore.NewQuery(kind).KeysOnly()
	keys, err := c.client.GetAll(ctx, countQuery, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to count entities for kind %s: %w", kind, err)
	}

	count := int64(len(keys))
	if count == 0 {
		return &interfaces.KindSchema{
			Name:   kind,
			Fields: []interfaces.FieldInfo{},
			Count:  0,
		}, nil
	}

	// Get a sample of entities to analyze the schema
	sampleSize := 10
	if count < 10 {
		sampleSize = int(count)
	}

	query := datastore.NewQuery(kind).Limit(sampleSize)

	// Use PropertyList to handle arbitrary schemas
	var entities []datastore.PropertyList
	_, err = c.client.GetAll(ctx, query, &entities)
	if err != nil {
		return nil, fmt.Errorf("failed to get sample entities for kind %s: %w", kind, err)
	}

	// Analyze the schema from the sample entities
	fieldMap := make(map[string]interfaces.FieldInfo)

	for _, entity := range entities {
		for _, prop := range entity {
			fieldName := prop.Name

			// Skip if we've already seen this field with the same type
			if existing, exists := fieldMap[fieldName]; exists {
				// If types match, keep the existing one
				if existing.TypeName == c.getPropertyTypeName(prop) {
					continue
				}
				// If types differ, update to indicate mixed types
				existing.TypeName = "mixed"
				fieldMap[fieldName] = existing
				continue
			}

			// Add new field
			fieldInfo := interfaces.FieldInfo{
				Name:     fieldName,
				TypeName: c.getPropertyTypeName(prop),
				Sample:   prop.Value,
			}
			fieldMap[fieldName] = fieldInfo
		}
	}

	// Add DataStore key fields based on actual key structure
	c.addKeyFields(keys, fieldMap)

	// Convert map to slice, ensuring DataStore Primary Key field is first (for default selection)
	fields := make([]interfaces.FieldInfo, 0, len(fieldMap))

	// Add DataStore Primary Key field first (using "PK" internal name)
	if primaryKeyField, exists := fieldMap["PK"]; exists {
		fields = append(fields, primaryKeyField)
	} else if primaryKeyField, exists := fieldMap["__primary_key__"]; exists {
		fields = append(fields, primaryKeyField)
	}

	// Add remaining fields
	for fieldName, field := range fieldMap {
		if fieldName != "PK" && fieldName != "__primary_key__" {
			fields = append(fields, field)
		}
	}

	return &interfaces.KindSchema{
		Name:   kind,
		Fields: fields,
		Count:  count,
	}, nil
}

// GetEntities returns a channel of entities for the specified Kind
func (c *Client) GetEntities(ctx context.Context, kind string, batchSize int) (<-chan interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if kind == "" {
		return nil, fmt.Errorf("kind name is required")
	}

	if batchSize <= 0 {
		batchSize = 100 // Default batch size
	}

	entityChan := make(chan interface{}, batchSize)

	go func() {
		defer close(entityChan)

		query := datastore.NewQuery(kind)
		cursor := ""

		for {
			// Check for context cancellation before each batch
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Apply cursor if we have one
			if cursor != "" {
				decodedCursor, err := datastore.DecodeCursor(cursor)
				if err != nil {
					fmt.Printf("Error decoding cursor: %v\n", err)
					return
				}
				query = query.Start(decodedCursor)
			}

			// Limit the batch size
			batchQuery := query.Limit(batchSize)

			// Use PropertyList to handle arbitrary schemas
			var entities []datastore.PropertyList
			keys, err := c.client.GetAll(ctx, batchQuery, &entities)
			if err != nil {
				// Check if error is due to context cancellation
				if ctx.Err() != nil {
					return
				}
				fmt.Printf("Error fetching entities: %v\n", err)
				return
			}

			// If no entities returned, we're done
			if len(entities) == 0 {
				break
			}

			// Send entities through the channel
			for i, entity := range entities {
				select {
				case <-ctx.Done():
					return
				case entityChan <- &EntityWithKey{
					Key:        keys[i],
					Properties: entity,
				}:
				}
			}

			// If we got fewer entities than requested, we're done
			if len(entities) < batchSize {
				break
			}

			// Check for context cancellation before getting next cursor
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Get cursor for next batch
			lastKey := keys[len(keys)-1]
			nextCursor := datastore.NewQuery(kind).Filter("__key__ >", lastKey).Limit(1)
			nextKeys, err := c.client.GetAll(ctx, nextCursor.KeysOnly(), nil)
			if err != nil {
				// Check if error is due to context cancellation
				if ctx.Err() != nil {
					return
				}
				break
			}
			if len(nextKeys) == 0 {
				break
			}
			cursor = nextKeys[0].String()
		}
	}()

	return entityChan, nil
}

// Close closes the DataStore client
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// EntityWithKey represents a DataStore entity with its key
type EntityWithKey struct {
	Key        *datastore.Key         `json:"key"`
	Properties datastore.PropertyList `json:"properties"`
}

// ToMap converts the entity to a map[string]interface{}
func (e *EntityWithKey) ToMap() map[string]interface{} {
	result := make(map[string]interface{})

	// Add the key information with both legacy and synthetic Primary Key field
	if e.Key != nil {
		result["__key__"] = e.Key.String()

		// Add legacy fields for compatibility
		if e.Key.Name != "" {
			result["__key_name__"] = e.Key.Name
		}
		if e.Key.ID != 0 {
			result["__key_id__"] = e.Key.ID
		}

		// Add synthetic DataStore Primary Key field as "PK" (always as string)
		if e.Key.Name != "" {
			result["PK"] = e.Key.Name
		} else if e.Key.ID != 0 {
			result["PK"] = fmt.Sprintf("%d", e.Key.ID) // Convert numeric ID to string
		} else {
			result["PK"] = e.Key.String()
		}
	}

	// Add all properties
	for _, prop := range e.Properties {
		result[prop.Name] = prop.Value
	}

	return result
}

// getPropertyTypeName returns a type name for a DataStore property
func (c *Client) getPropertyTypeName(prop datastore.Property) string {
	switch prop.Value.(type) {
	case string:
		return "string"
	case int64:
		return "int"
	case float64:
		return "float"
	case bool:
		return "bool"
	case []byte:
		return "[]byte"
	case *datastore.Key:
		return "*datastore.Key"
	case datastore.GeoPoint:
		return "GeoPoint"
	default:
		return fmt.Sprintf("%T", prop.Value)
	}
}

// getKeyIdentifierSample returns a sample key identifier from the provided keys
func (c *Client) getKeyIdentifierSample(keys []*datastore.Key) string {
	if len(keys) == 0 {
		return "key_identifier"
	}

	// Use the first key as a sample
	key := keys[0]
	if key.Name != "" {
		return key.Name
	}
	if key.ID != 0 {
		return fmt.Sprintf("%d", key.ID)
	}
	return "key_identifier"
}

// addKeyFields adds a single synthetic Primary Key field to the field map
func (c *Client) addKeyFields(keys []*datastore.Key, fieldMap map[string]interfaces.FieldInfo) {
	if len(keys) == 0 {
		return
	}

	// Determine the sample value (type will always be string for DynamoDB PK)
	var sampleValue interface{}
	hasStringName := false
	hasNumericID := false

	for _, key := range keys {
		if key.Name != "" {
			hasStringName = true
		}
		if key.ID != 0 {
			hasNumericID = true
		}
	}

	if hasStringName {
		sampleValue = c.getFirstKeyName(keys)
	} else if hasNumericID {
		sampleValue = c.getFirstKeyID(keys)
	} else {
		sampleValue = c.getKeyIdentifierSample(keys)
	}

	// Use "PK" as the DynamoDB attribute name and a more descriptive display name.
	internalName := "PK"
	displayName := "DataStore Primary Key (ID/Name)"

	// Check if the internal name already exists (unlikely but safe)
	if _, exists := fieldMap[internalName]; exists {
		internalName = "__primary_key__"
	}

	fieldMap[internalName] = interfaces.FieldInfo{
		Name:        internalName,
		DisplayName: displayName,
		TypeName:    "string",                       // Always string type for consistency in DynamoDB
		Sample:      fmt.Sprintf("%v", sampleValue), // Convert sample to string representation
	}
}

// getFirstKeyName returns the first non-empty key name from the provided keys
func (c *Client) getFirstKeyName(keys []*datastore.Key) string {
	for _, key := range keys {
		if key.Name != "" {
			return key.Name
		}
	}
	return ""
}

// getFirstKeyID returns the first non-zero key ID from the provided keys
func (c *Client) getFirstKeyID(keys []*datastore.Key) int64 {
	for _, key := range keys {
		if key.ID != 0 {
			return key.ID
		}
	}
	return 0
}
