package datastore

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/api/iterator"

	"cloud.google.com/go/datastore"
	"github.com/coreyculler/datastore-dynamodb-migrator/internal/interfaces"
)

// Client wraps the GCP DataStore client and implements the DataStoreClient interface
type Client struct {
	client    *datastore.Client
	projectID string
	mu        sync.RWMutex
}

// NewClientWithDatabase creates a new DataStore client for the given project and optional database ID.
// When databaseID is empty, the default database is used.
func NewClientWithDatabase(ctx context.Context, projectID string, databaseID string) (*Client, error) {
	if projectID == "" {
		return nil, fmt.Errorf("project ID is required")
	}

	var (
		client *datastore.Client
		err    error
	)
	if strings.TrimSpace(databaseID) != "" {
		client, err = datastore.NewClientWithDatabase(ctx, projectID, databaseID)
	} else {
		client, err = datastore.NewClient(ctx, projectID)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create datastore client: %w", err)
	}

	return &Client{
		client:    client,
		projectID: projectID,
	}, nil
}

// NewClient creates a new DataStore client using the default database for backward compatibility.
func NewClient(ctx context.Context, projectID string) (*Client, error) {
	return NewClientWithDatabase(ctx, projectID, "")
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

	// Get the entity count from DataStore statistics for efficiency
	count, err := c.getEntityCount(ctx, kind)
	if err != nil {
		return nil, fmt.Errorf("failed to get entity count for kind %s: %w", kind, err)
	}

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
	keys, err := c.client.GetAll(ctx, query, &entities)
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
		batchSize = 100 // Default batch size for channel buffer
	}

	entityChan := make(chan interface{}, batchSize)

	go func() {
		defer close(entityChan)

		query := datastore.NewQuery(kind)
		it := c.client.Run(ctx, query)

		for {
			var entity datastore.PropertyList
			key, err := it.Next(&entity)
			if err == iterator.Done {
				break // All entities have been fetched
			}
			if err != nil {
				// If the context was cancelled, it's not a migration error, just an interruption.
				if ctx.Err() != nil {
					return
				}
				// This is a genuine error during fetching. Log it and terminate for this Kind.
				// The migration engine will detect that not all entities were processed.
				fmt.Printf("\nERROR: Failed to fetch entity for Kind '%s': %v\n", kind, err)
				return
			}

			// Send the fetched entity to the channel
			select {
			case <-ctx.Done():
				return
			case entityChan <- &EntityWithKey{
				Key:        key,
				Properties: entity,
			}:
			}
		}
	}()

	return entityChan, nil
}

// getEntityCount retrieves the number of entities for a kind from DataStore statistics.
func (c *Client) getEntityCount(ctx context.Context, kind string) (int64, error) {
	key := datastore.NameKey("__Stat_Kind__", kind, nil)

	var properties datastore.PropertyList
	err := c.client.Get(ctx, key, &properties)
	if err != nil {
		if err == datastore.ErrNoSuchEntity {
			// If stats are not available, fall back to counting keys (slower)
			fmt.Printf("⚠️  Statistics not available for Kind '%s', falling back to key count (this may be slow).\n", kind)
			return c.countKeys(ctx, kind)
		}
		return 0, err
	}

	for _, p := range properties {
		if p.Name == "count" {
			if count, ok := p.Value.(int64); ok {
				return count, nil
			}
			return 0, fmt.Errorf("unexpected type for 'count' statistic: %T", p.Value)
		}
	}

	return 0, fmt.Errorf("'count' statistic not found for Kind '%s'", kind)
}

// countKeys is a fallback method to count entities by fetching all keys.
func (c *Client) countKeys(ctx context.Context, kind string) (int64, error) {
	query := datastore.NewQuery(kind).KeysOnly()
	keys, err := c.client.GetAll(ctx, query, nil)
	if err != nil {
		return 0, err
	}
	return int64(len(keys)), nil
}

// GetEntitiesSample returns a channel of sample entities for the specified Kind
func (c *Client) GetEntitiesSample(ctx context.Context, kind string, limit int) (<-chan interface{}, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if kind == "" {
		return nil, fmt.Errorf("kind name is required")
	}

	entityChan := make(chan interface{}, limit)

	go func() {
		defer close(entityChan)

		query := datastore.NewQuery(kind).Limit(limit)
		it := c.client.Run(ctx, query)

		for {
			var entity datastore.PropertyList
			key, err := it.Next(&entity)
			if err == iterator.Done {
				break
			}
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				fmt.Printf("\nERROR: Failed to fetch entity for Kind '%s': %v\n", kind, err)
				return
			}

			select {
			case <-ctx.Done():
				return
			case entityChan <- &EntityWithKey{
				Key:        key,
				Properties: entity,
			}:
			}
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
