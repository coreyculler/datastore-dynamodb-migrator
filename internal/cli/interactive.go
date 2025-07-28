package cli

import (
	"context"
	"fmt"
	"strings"

	"github.com/coreyculler/datastore-dynamodb-migrator/internal/interfaces"

	"github.com/manifoldco/promptui"
)

// InteractiveSelector handles interactive user input for key selection
type InteractiveSelector struct{}

// NewInteractiveSelector creates a new InteractiveSelector
func NewInteractiveSelector() *InteractiveSelector {
	return &InteractiveSelector{}
}

// runPromptWithContext runs a promptui prompt with context cancellation support
func (s *InteractiveSelector) runPromptWithContext(ctx context.Context, prompt *promptui.Select) (int, string, error) {
	type result struct {
		idx   int
		value string
		err   error
	}

	resultChan := make(chan result, 1)

	go func() {
		idx, value, err := prompt.Run()
		resultChan <- result{idx: idx, value: value, err: err}
	}()

	select {
	case <-ctx.Done():
		return 0, "", ctx.Err()
	case res := <-resultChan:
		return res.idx, res.value, res.err
	}
}

// runPromptInputWithContext runs a promptui input prompt with context cancellation support
func (s *InteractiveSelector) runPromptInputWithContext(ctx context.Context, prompt *promptui.Prompt) (string, error) {
	type result struct {
		value string
		err   error
	}

	resultChan := make(chan result, 1)

	go func() {
		value, err := prompt.Run()
		resultChan <- result{value: value, err: err}
	}()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case res := <-resultChan:
		return res.value, res.err
	}
}

// AskToSkipKind asks the user if they want to skip migrating a particular Kind
func (s *InteractiveSelector) AskToSkipKind(ctx context.Context, schema interfaces.KindSchema) (bool, error) {
	fmt.Printf("\n=== DataStore Kind: %s ===\n", schema.Name)
	fmt.Printf("Total entities: %d\n", schema.Count)
	fmt.Printf("Available fields: %d\n\n", len(schema.Fields))

	// Display a brief summary of the schema
	if len(schema.Fields) > 0 {
		fmt.Println("Field preview:")
		for i, field := range schema.Fields {
			if i >= 5 { // Show only first 5 fields in preview
				fmt.Printf("  ... and %d more fields\n", len(schema.Fields)-5)
				break
			}
			displayName := field.Name
			if field.DisplayName != "" {
				displayName = field.DisplayName
			}
			fmt.Printf("  - %s (%s)\n", displayName, field.TypeName)
		}
		fmt.Println()
	}

	prompt := promptui.Select{
		Label: "What would you like to do with this Kind?",
		Items: []string{
			"Configure and migrate this Kind",
			"Skip this Kind (do not migrate)",
		},
		Templates: &promptui.SelectTemplates{
			Label:    "{{ . }}:",
			Active:   "▶ {{ . }}",
			Inactive: "  {{ . }}",
			Selected: "✓ {{ . }}",
		},
	}

	idx, _, err := s.runPromptWithContext(ctx, &prompt)
	if err != nil {
		return false, err
	}

	// Return true if user wants to skip (idx == 1)
	return idx == 1, nil
}

// SelectKeys interactively selects partition and sort keys for a Kind
func (s *InteractiveSelector) SelectKeys(ctx context.Context, schema interfaces.KindSchema) (interfaces.KeySelection, error) {
	fmt.Printf("\n=== Configuring Keys for Kind: %s ===\n", schema.Name)
	fmt.Printf("Total entities: %d\n", schema.Count)
	fmt.Printf("Available fields: %d\n\n", len(schema.Fields))

	// Display schema information
	s.displaySchema(schema)

	// Select partition key
	partitionKey, err := s.selectPartitionKey(ctx, schema)
	if err != nil {
		return interfaces.KeySelection{}, fmt.Errorf("failed to select partition key: %w", err)
	}

	// Ask if user wants a sort key
	wantsSortKey, err := s.askForSortKey(ctx)
	if err != nil {
		return interfaces.KeySelection{}, fmt.Errorf("failed to ask for sort key: %w", err)
	}

	var sortKey *string
	if wantsSortKey {
		selectedSortKey, err := s.selectSortKey(ctx, schema, partitionKey)
		if err != nil {
			return interfaces.KeySelection{}, fmt.Errorf("failed to select sort key: %w", err)
		}
		sortKey = &selectedSortKey
	}

	keySelection := interfaces.KeySelection{
		PartitionKey: partitionKey,
		SortKey:      sortKey,
	}

	// Confirm the selection
	confirmed, err := s.confirmKeySelection(ctx, keySelection, schema)
	if err != nil {
		return interfaces.KeySelection{}, fmt.Errorf("failed to confirm selection: %w", err)
	}

	if !confirmed {
		fmt.Println("Key selection cancelled. Please try again.")
		return s.SelectKeys(ctx, schema) // Recursive call to restart selection
	}

	return keySelection, nil
}

// displaySchema shows the schema information in a formatted way
func (s *InteractiveSelector) displaySchema(schema interfaces.KindSchema) {
	fmt.Println("Field Information:")
	fmt.Println("==================")

	for i, field := range schema.Fields {
		displayName := field.Name
		if field.DisplayName != "" {
			displayName = field.DisplayName
		}
		fmt.Printf("%d. %s\n", i+1, displayName)
		fmt.Printf("   Type: %s\n", field.TypeName)
		if field.Sample != nil {
			sampleStr := fmt.Sprintf("%v", field.Sample)
			if len(sampleStr) > 50 {
				sampleStr = sampleStr[:47] + "..."
			}
			fmt.Printf("   Sample: %s\n", sampleStr)
		}
		fmt.Println()
	}
}

// selectPartitionKey prompts user to select a partition key
func (s *InteractiveSelector) selectPartitionKey(ctx context.Context, schema interfaces.KindSchema) (string, error) {
	fieldNames := make([]string, len(schema.Fields))
	defaultCursorPos := 0

	for i, field := range schema.Fields {
		displayName := field.Name
		if field.DisplayName != "" {
			displayName = field.DisplayName
		}
		fieldNames[i] = fmt.Sprintf("%s (%s)", displayName, field.TypeName)

		// Set the primary key field as the default selection
		if field.Name == "PK" || field.Name == "__primary_key__" {
			defaultCursorPos = i
		}
	}

	prompt := promptui.Select{
		Label:     "Select the Partition Key - this field should uniquely identify most entities",
		Items:     fieldNames,
		CursorPos: defaultCursorPos,
		Templates: &promptui.SelectTemplates{
			Label:    "{{ . }}:",
			Active:   "▶ {{ . }}",
			Inactive: "  {{ . }}",
			Selected: "✓ Partition Key: {{ . }}",
		},
		Size: 10,
	}

	idx, _, err := s.runPromptWithContext(ctx, &prompt)
	if err != nil {
		return "", err
	}

	return schema.Fields[idx].Name, nil
}

// askForSortKey asks if the user wants to add a sort key
func (s *InteractiveSelector) askForSortKey(ctx context.Context) (bool, error) {
	prompt := promptui.Select{
		Label: "Do you want to add a Sort Key? (Useful for composite keys or ordering)",
		Items: []string{"No, partition key only", "Yes, add a sort key"},
		Templates: &promptui.SelectTemplates{
			Label:    "{{ . }}:",
			Active:   "▶ {{ . }}",
			Inactive: "  {{ . }}",
			Selected: "✓ {{ . }}",
		},
	}

	idx, _, err := s.runPromptWithContext(ctx, &prompt)
	if err != nil {
		return false, err
	}

	return idx == 1, nil
}

// selectSortKey prompts user to select a sort key
func (s *InteractiveSelector) selectSortKey(ctx context.Context, schema interfaces.KindSchema, partitionKey string) (string, error) {
	// Filter out the partition key from available options
	var availableFields []interfaces.FieldInfo
	var fieldNames []string

	for _, field := range schema.Fields {
		if field.Name != partitionKey {
			displayName := field.Name
			if field.DisplayName != "" {
				displayName = field.DisplayName
			}
			availableFields = append(availableFields, field)
			fieldNames = append(fieldNames, fmt.Sprintf("%s (%s)", displayName, field.TypeName))
		}
	}

	if len(availableFields) == 0 {
		return "", fmt.Errorf("no available fields for sort key (all fields are used as partition key)")
	}

	prompt := promptui.Select{
		Label: "Select the Sort Key - this will be used for ordering within the same partition",
		Items: fieldNames,
		Templates: &promptui.SelectTemplates{
			Label:    "{{ . }}:",
			Active:   "▶ {{ . }}",
			Inactive: "  {{ . }}",
			Selected: "✓ Sort Key: {{ . }}",
		},
		Size: 10,
	}

	idx, _, err := s.runPromptWithContext(ctx, &prompt)
	if err != nil {
		return "", err
	}

	return availableFields[idx].Name, nil
}

// confirmKeySelection asks user to confirm their key selection
func (s *InteractiveSelector) confirmKeySelection(ctx context.Context, selection interfaces.KeySelection, schema interfaces.KindSchema) (bool, error) {
	fmt.Println("\n=== Key Selection Summary ===")

	// Find the partition key field and get its display name
	partitionDisplayName := selection.PartitionKey
	for _, field := range schema.Fields {
		if field.Name == selection.PartitionKey {
			if field.DisplayName != "" {
				partitionDisplayName = field.DisplayName
			}
			break
		}
	}
	fmt.Printf("Partition Key: %s\n", partitionDisplayName)

	if selection.SortKey != nil {
		// Find the sort key field and get its display name
		sortDisplayName := *selection.SortKey
		for _, field := range schema.Fields {
			if field.Name == *selection.SortKey {
				if field.DisplayName != "" {
					sortDisplayName = field.DisplayName
				}
				break
			}
		}
		fmt.Printf("Sort Key: %s\n", sortDisplayName)
	} else {
		fmt.Println("Sort Key: None")
	}
	fmt.Println()

	prompt := promptui.Select{
		Label: "Confirm this key configuration?",
		Items: []string{"Yes, proceed with this configuration", "No, let me choose again"},
		Templates: &promptui.SelectTemplates{
			Label:    "{{ . }}:",
			Active:   "▶ {{ . }}",
			Inactive: "  {{ . }}",
			Selected: "✓ {{ . }}",
		},
	}

	idx, _, err := s.runPromptWithContext(ctx, &prompt)
	if err != nil {
		return false, err
	}

	return idx == 0, nil
}

// SelectTargetTableName prompts user to enter or confirm the target table name
func (s *InteractiveSelector) SelectTargetTableName(ctx context.Context, defaultName string) (string, error) {
	prompt := promptui.Prompt{
		Label:   "Enter DynamoDB table name",
		Default: defaultName,
		Validate: func(input string) error {
			input = strings.TrimSpace(input)
			if input == "" {
				return fmt.Errorf("table name cannot be empty")
			}
			if len(input) < 3 || len(input) > 255 {
				return fmt.Errorf("table name must be between 3 and 255 characters")
			}
			// Basic validation for DynamoDB table naming rules
			for _, char := range input {
				if !((char >= 'a' && char <= 'z') ||
					(char >= 'A' && char <= 'Z') ||
					(char >= '0' && char <= '9') ||
					char == '-' || char == '_' || char == '.') {
					return fmt.Errorf("table name can only contain letters, numbers, hyphens, underscores, and periods")
				}
			}
			return nil
		},
	}

	result, err := s.runPromptInputWithContext(ctx, &prompt)
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(result), nil
}

// ConfirmMigration asks user to confirm the migration before starting
func (s *InteractiveSelector) ConfirmMigration(ctx context.Context, configs []interfaces.MigrationConfig) (bool, error) {
	fmt.Println("\n=== Migration Plan Summary ===")
	fmt.Printf("Total Kinds to migrate: %d\n\n", len(configs))

	for i, config := range configs {
		fmt.Printf("%d. %s → %s\n", i+1, config.SourceKind, config.TargetTable)
		fmt.Printf("   Entities: %d\n", config.Schema.Count)

		// Find the partition key field and get its display name
		partitionDisplayName := config.KeySelection.PartitionKey
		for _, field := range config.Schema.Fields {
			if field.Name == config.KeySelection.PartitionKey {
				if field.DisplayName != "" {
					partitionDisplayName = field.DisplayName
				}
				break
			}
		}
		fmt.Printf("   Partition Key: %s\n", partitionDisplayName)

		if config.KeySelection.SortKey != nil {
			// Find the sort key field and get its display name
			sortDisplayName := *config.KeySelection.SortKey
			for _, field := range config.Schema.Fields {
				if field.Name == *config.KeySelection.SortKey {
					if field.DisplayName != "" {
						sortDisplayName = field.DisplayName
					}
					break
				}
			}
			fmt.Printf("   Sort Key: %s\n", sortDisplayName)
		} else {
			fmt.Printf("   Sort Key: None\n")
		}
		fmt.Println()
	}

	prompt := promptui.Select{
		Label: "Ready to start migration?",
		Items: []string{"Yes, start migration", "No, cancel migration"},
		Templates: &promptui.SelectTemplates{
			Label:    "{{ . }}:",
			Active:   "▶ {{ . }}",
			Inactive: "  {{ . }}",
			Selected: "✓ {{ . }}",
		},
	}

	idx, _, err := s.runPromptWithContext(ctx, &prompt)
	if err != nil {
		return false, err
	}

	return idx == 0, nil
}

// ShowMigrationProgress displays migration progress in a user-friendly way
func (s *InteractiveSelector) ShowMigrationProgress(progress interfaces.MigrationProgress) {
	percentage := float64(progress.Processed) / float64(progress.Total) * 100

	// Using '\r' moves the cursor to the beginning of the line, so each update overwrites the last.
	// We no longer print a newline here, as the final summary will be handled separately.
	fmt.Printf("\r%s: %d/%d (%.1f%%) | Errors: %d",
		progress.Kind,
		progress.Processed,
		progress.Total,
		percentage,
		progress.Errors)
}

// HandleExistingTable prompts the user for an action when a DynamoDB table already exists.
// It returns a string representing the chosen action: "truncate", "insert", or "skip".
func (s *InteractiveSelector) HandleExistingTable(ctx context.Context, tableName string) (string, error) {
	fmt.Printf("\n⚠️  DynamoDB table '%s' already exists.\n", tableName)
	prompt := promptui.Select{
		Label: "How would you like to proceed?",
		Items: []string{
			"Truncate the table (delete all existing items and re-migrate)",
			"Insert/Update records (migrate new records, overwrite existing ones with the same key)",
			"Skip this table (do not migrate)",
		},
		Templates: &promptui.SelectTemplates{
			Label:    "{{ . }}:",
			Active:   "▶ {{ . }}",
			Inactive: "  {{ . }}",
			Selected: "✓ {{ . | green }}",
		},
	}

	idx, _, err := s.runPromptWithContext(ctx, &prompt)
	if err != nil {
		if err == promptui.ErrInterrupt {
			return "skip", nil // Default to skipping if the user cancels.
		}
		return "", fmt.Errorf("failed to get user choice for existing table: %w", err)
	}

	switch idx {
	case 0:
		return "truncate", nil
	case 1:
		return "insert", nil
	case 2:
		return "skip", nil
	default:
		return "skip", nil // Should not happen, but default to safe option.
	}
}
