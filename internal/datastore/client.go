package datastore

import (
	"context"
	"fmt"
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

	kinds := make([]string, len(keys))
	for i, key := range keys {
		kinds[i] = key.Name
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

	// Convert map to slice
	fields := make([]interfaces.FieldInfo, 0, len(fieldMap))
	for _, field := range fieldMap {
		fields = append(fields, field)
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

			// Get cursor for next batch
			lastKey := keys[len(keys)-1]
			nextCursor := datastore.NewQuery(kind).Filter("__key__ >", lastKey).Limit(1)
			nextKeys, err := c.client.GetAll(ctx, nextCursor.KeysOnly(), nil)
			if err != nil || len(nextKeys) == 0 {
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

	// Add the key information
	if e.Key != nil {
		result["__key__"] = e.Key.String()
		if e.Key.Name != "" {
			result["__key_name__"] = e.Key.Name
		}
		if e.Key.ID != 0 {
			result["__key_id__"] = e.Key.ID
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
