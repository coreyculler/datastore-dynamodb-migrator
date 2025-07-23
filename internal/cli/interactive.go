package cli

import (
	"fmt"
	"strings"

	"datastore-dynamodb-migrator/internal/interfaces"

	"github.com/manifoldco/promptui"
)

// InteractiveSelector handles interactive user input for key selection
type InteractiveSelector struct{}

// NewInteractiveSelector creates a new InteractiveSelector
func NewInteractiveSelector() *InteractiveSelector {
	return &InteractiveSelector{}
}

// SelectKeys interactively selects partition and sort keys for a Kind
func (s *InteractiveSelector) SelectKeys(schema interfaces.KindSchema) (interfaces.KeySelection, error) {
	fmt.Printf("\n=== Configuring Keys for Kind: %s ===\n", schema.Name)
	fmt.Printf("Total entities: %d\n", schema.Count)
	fmt.Printf("Available fields: %d\n\n", len(schema.Fields))

	// Display schema information
	s.displaySchema(schema)

	// Select partition key
	partitionKey, err := s.selectPartitionKey(schema)
	if err != nil {
		return interfaces.KeySelection{}, fmt.Errorf("failed to select partition key: %w", err)
	}

	// Ask if user wants a sort key
	wantsSortKey, err := s.askForSortKey()
	if err != nil {
		return interfaces.KeySelection{}, fmt.Errorf("failed to ask for sort key: %w", err)
	}

	var sortKey *string
	if wantsSortKey {
		selectedSortKey, err := s.selectSortKey(schema, partitionKey)
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
	confirmed, err := s.confirmKeySelection(keySelection)
	if err != nil {
		return interfaces.KeySelection{}, fmt.Errorf("failed to confirm selection: %w", err)
	}

	if !confirmed {
		fmt.Println("Key selection cancelled. Please try again.")
		return s.SelectKeys(schema) // Recursive call to restart selection
	}

	return keySelection, nil
}

// displaySchema shows the schema information in a formatted way
func (s *InteractiveSelector) displaySchema(schema interfaces.KindSchema) {
	fmt.Println("Field Information:")
	fmt.Println("==================")

	for i, field := range schema.Fields {
		fmt.Printf("%d. %s\n", i+1, field.Name)
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
func (s *InteractiveSelector) selectPartitionKey(schema interfaces.KindSchema) (string, error) {
	fieldNames := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		fieldNames[i] = fmt.Sprintf("%s (%s)", field.Name, field.TypeName)
	}

	prompt := promptui.Select{
		Label: "Select the Partition Key (Primary Key) - this field should uniquely identify most entities",
		Items: fieldNames,
		Templates: &promptui.SelectTemplates{
			Label:    "{{ . }}:",
			Active:   "▶ {{ . }}",
			Inactive: "  {{ . }}",
			Selected: "✓ Partition Key: {{ . }}",
		},
		Size: 10,
	}

	idx, _, err := prompt.Run()
	if err != nil {
		return "", err
	}

	return schema.Fields[idx].Name, nil
}

// askForSortKey asks if the user wants to add a sort key
func (s *InteractiveSelector) askForSortKey() (bool, error) {
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

	idx, _, err := prompt.Run()
	if err != nil {
		return false, err
	}

	return idx == 1, nil
}

// selectSortKey prompts user to select a sort key
func (s *InteractiveSelector) selectSortKey(schema interfaces.KindSchema, partitionKey string) (string, error) {
	// Filter out the partition key from available options
	var availableFields []interfaces.FieldInfo
	var fieldNames []string

	for _, field := range schema.Fields {
		if field.Name != partitionKey {
			availableFields = append(availableFields, field)
			fieldNames = append(fieldNames, fmt.Sprintf("%s (%s)", field.Name, field.TypeName))
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

	idx, _, err := prompt.Run()
	if err != nil {
		return "", err
	}

	return availableFields[idx].Name, nil
}

// confirmKeySelection asks user to confirm their key selection
func (s *InteractiveSelector) confirmKeySelection(selection interfaces.KeySelection) (bool, error) {
	fmt.Println("\n=== Key Selection Summary ===")
	fmt.Printf("Partition Key: %s\n", selection.PartitionKey)
	if selection.SortKey != nil {
		fmt.Printf("Sort Key: %s\n", *selection.SortKey)
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

	idx, _, err := prompt.Run()
	if err != nil {
		return false, err
	}

	return idx == 0, nil
}

// SelectTargetTableName prompts user to enter or confirm the target table name
func (s *InteractiveSelector) SelectTargetTableName(defaultName string) (string, error) {
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

	result, err := prompt.Run()
	if err != nil {
		return "", err
	}

	return strings.TrimSpace(result), nil
}

// ConfirmMigration asks user to confirm the migration before starting
func (s *InteractiveSelector) ConfirmMigration(configs []interfaces.MigrationConfig) (bool, error) {
	fmt.Println("\n=== Migration Plan Summary ===")
	fmt.Printf("Total Kinds to migrate: %d\n\n", len(configs))

	for i, config := range configs {
		fmt.Printf("%d. %s → %s\n", i+1, config.SourceKind, config.TargetTable)
		fmt.Printf("   Entities: %d\n", config.Schema.Count)
		fmt.Printf("   Partition Key: %s\n", config.KeySelection.PartitionKey)
		if config.KeySelection.SortKey != nil {
			fmt.Printf("   Sort Key: %s\n", *config.KeySelection.SortKey)
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

	idx, _, err := prompt.Run()
	if err != nil {
		return false, err
	}

	return idx == 0, nil
}

// ShowMigrationProgress displays migration progress in a user-friendly way
func (s *InteractiveSelector) ShowMigrationProgress(progress interfaces.MigrationProgress) {
	percentage := float64(progress.Processed) / float64(progress.Total) * 100

	fmt.Printf("\r%s: %d/%d (%.1f%%) | Errors: %d",
		progress.Kind,
		progress.Processed,
		progress.Total,
		percentage,
		progress.Errors)

	if progress.Completed {
		if progress.Errors > 0 {
			fmt.Printf(" - COMPLETED WITH ERRORS\n")
		} else {
			fmt.Printf(" - COMPLETED SUCCESSFULLY\n")
		}
	}
}
