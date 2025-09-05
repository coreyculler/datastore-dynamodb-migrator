package cli

import (
	"context"
	"fmt"
	"os"
	"regexp"
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

// SelectDatabaseID prompts the user to select a Datastore/Firestore database ID.
// The provided list should include "(default)" entry when appropriate.
func (s *InteractiveSelector) SelectDatabaseID(ctx context.Context, projectID string, databaseIDs []string) (string, error) {
	if len(databaseIDs) == 0 {
		return "", nil
	}

	display := make([]string, len(databaseIDs))
	defaultIndex := 0
	for i, id := range databaseIDs {
		if id == "" || id == "(default)" {
			display[i] = "(default)"
			defaultIndex = i
		} else {
			display[i] = id
		}
	}

	prompt := promptui.Select{
		Label:     fmt.Sprintf("Select Datastore database for project %s", projectID),
		Items:     display,
		CursorPos: defaultIndex,
		Templates: &promptui.SelectTemplates{
			Label:    "{{ . }}:",
			Active:   "▶ {{ . }}",
			Inactive: "  {{ . }}",
			Selected: "✓ {{ . }}",
		},
		Size: 10,
	}

	idx, _, err := s.runPromptWithContext(ctx, &prompt)
	if err != nil {
		return "", err
	}

	chosen := databaseIDs[idx]
	if chosen == "(default)" {
		return "", nil
	}
	return chosen, nil
}

// shouldHideFieldFromDisplay determines whether a field should be hidden from
// CLI previews and selection prompts. We hide explicit "id" and "name"
// fields to avoid confusion with the synthetic DataStore Primary Key (PK),
// but always show the synthetic primary key field.
func (s *InteractiveSelector) shouldHideFieldFromDisplay(field interfaces.FieldInfo) bool {
	// Always show the synthetic primary key field
	if field.Name == "PK" || field.Name == "__primary_key__" {
		return false
	}

	nameLower := strings.ToLower(field.Name)
	if nameLower == "id" || nameLower == "name" {
		return true
	}
	return false
}

// getDisplayFields returns fields filtered for display purposes only,
// removing duplicate/confusing identifiers like explicit id/name.
func (s *InteractiveSelector) getDisplayFields(schema interfaces.KindSchema) []interfaces.FieldInfo {
	filtered := make([]interfaces.FieldInfo, 0, len(schema.Fields))
	for _, f := range schema.Fields {
		if s.shouldHideFieldFromDisplay(f) {
			continue
		}
		filtered = append(filtered, f)
	}
	return filtered
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

	displayFields := s.getDisplayFields(schema)
	fmt.Printf("Available fields: %d\n\n", len(displayFields))

	// Display a brief summary of the schema
	if len(displayFields) > 0 {
		fmt.Println("Field preview:")
		for i, field := range displayFields {
			if i >= 5 { // Show only first 5 fields in preview
				fmt.Printf("  ... and %d more fields\n", len(displayFields)-5)
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
	displayFields := s.getDisplayFields(schema)
	fmt.Printf("Available fields: %d\n\n", len(displayFields))

	// Display schema information
	s.displaySchema(schema)

	// Select partition key (source in the entity)
	partitionKeySource, err := s.selectPartitionKey(ctx, schema)
	if err != nil {
		return interfaces.KeySelection{}, fmt.Errorf("failed to select partition key: %w", err)
	}

	// Ask for a DynamoDB attribute name (alias) for the partition key
	aliasPrompt := promptui.Prompt{
		Label:   fmt.Sprintf("Enter DynamoDB attribute name for Partition Key (source: %s)", partitionKeySource),
		Default: partitionKeySource,
		Validate: func(input string) error {
			input = strings.TrimSpace(input)
			if input == "" {
				return fmt.Errorf("attribute name cannot be empty")
			}
			for _, char := range input {
				if !((char >= 'a' && char <= 'z') ||
					(char >= 'A' && char <= 'Z') ||
					(char >= '0' && char <= '9') ||
					char == '-' || char == '_' || char == '.') {
					return fmt.Errorf("attribute name can only contain letters, numbers, hyphens, underscores, and periods")
				}
			}
			return nil
		},
	}
	partitionKeyAlias, err := s.runPromptInputWithContext(ctx, &aliasPrompt)
	if err != nil {
		return interfaces.KeySelection{}, fmt.Errorf("failed to get partition key attribute name: %w", err)
	}
	partitionKeyAlias = strings.TrimSpace(partitionKeyAlias)

	// Ask if user wants a sort key
	wantsSortKey, err := s.askForSortKey(ctx)
	if err != nil {
		return interfaces.KeySelection{}, fmt.Errorf("failed to ask for sort key: %w", err)
	}

	var sortKey *string
	if wantsSortKey {
		selectedSortKey, err := s.selectSortKey(ctx, schema, partitionKeySource)
		if err != nil {
			return interfaces.KeySelection{}, fmt.Errorf("failed to select sort key: %w", err)
		}
		sortKey = &selectedSortKey
	}

	keySelection := interfaces.KeySelection{
		PartitionKey:       partitionKeyAlias,
		PartitionKeySource: partitionKeySource,
		SortKey:            sortKey,
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

	fields := s.getDisplayFields(schema)
	for i, field := range fields {
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
	fields := s.getDisplayFields(schema)
	fieldNames := make([]string, len(fields))
	defaultCursorPos := 0

	for i, field := range fields {
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

	return fields[idx].Name, nil
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

	for _, field := range s.getDisplayFields(schema) {
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
	partitionDisplayName := selection.PartitionKeySource
	if partitionDisplayName == "" {
		partitionDisplayName = selection.PartitionKey
	}
	for _, field := range schema.Fields {
		if field.Name == partitionDisplayName {
			if field.DisplayName != "" {
				partitionDisplayName = field.DisplayName
			}
			break
		}
	}
	if selection.PartitionKeySource != "" && selection.PartitionKeySource != selection.PartitionKey {
		fmt.Printf("Partition Key: %s (DynamoDB attribute: %s)\n", partitionDisplayName, selection.PartitionKey)
	} else {
		fmt.Printf("Partition Key: %s\n", partitionDisplayName)
	}

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
		partitionDisplayName := config.KeySelection.PartitionKeySource
		if partitionDisplayName == "" {
			partitionDisplayName = config.KeySelection.PartitionKey
		}
		for _, field := range config.Schema.Fields {
			if field.Name == partitionDisplayName {
				if field.DisplayName != "" {
					partitionDisplayName = field.DisplayName
				}
				break
			}
		}
		if config.KeySelection.PartitionKeySource != "" && config.KeySelection.PartitionKeySource != config.KeySelection.PartitionKey {
			fmt.Printf("   Partition Key: %s (DynamoDB attribute: %s)\n", partitionDisplayName, config.KeySelection.PartitionKey)
		} else {
			fmt.Printf("   Partition Key: %s\n", partitionDisplayName)
		}

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

		if config.S3Storage != nil && config.S3Storage.Enabled {
			fmt.Printf("   S3 Storage: Enabled (bucket: %s, prefix: %s/)\n", config.S3Storage.Bucket, config.S3Storage.ObjectPrefix)
			if len(config.DynamoDBProjectionFields) > 0 {
				fmt.Printf("   DynamoDB Fields: %s\n", strings.Join(config.DynamoDBProjectionFields, ", "))
			} else {
				fmt.Printf("   DynamoDB Fields: All fields\n")
			}
		} else {
			fmt.Printf("   S3 Storage: Disabled\n")
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

// SelectS3OptionsAndProjection asks whether to store full records in S3 and which fields to keep in DynamoDB
func (s *InteractiveSelector) SelectS3OptionsAndProjection(ctx context.Context, schema interfaces.KindSchema) (*interfaces.S3StorageOptions, []string, error) {
	prompt := promptui.Select{
		Label: "Store full records in S3 as JSON?",
		Items: []string{"No", "Yes"},
		Templates: &promptui.SelectTemplates{
			Label:    "{{ . }}:",
			Active:   "▶ {{ . }}",
			Inactive: "  {{ . }}",
			Selected: "✓ {{ . }}",
		},
	}
	idx, _, err := s.runPromptWithContext(ctx, &prompt)
	if err != nil {
		return nil, nil, err
	}
	if idx == 0 {
		return nil, nil, nil
	}

	defaultBucket := strings.TrimSpace(os.Getenv("MIGRATION_S3_BUCKET"))
	bucketPrompt := promptui.Prompt{
		Label:   "Enter S3 bucket name",
		Default: defaultBucket,
		Validate: func(input string) error {
			input = strings.TrimSpace(input)
			if input == "" {
				return fmt.Errorf("bucket name cannot be empty")
			}
			// basic bucket name validation
			matched, _ := regexp.MatchString(`^[a-z0-9.-]{3,63}$`, input)
			if !matched {
				return fmt.Errorf("invalid bucket name format")
			}
			return nil
		},
	}
	bucket, err := s.runPromptInputWithContext(ctx, &bucketPrompt)
	if err != nil {
		return nil, nil, err
	}
	bucket = strings.TrimSpace(bucket)

	prefix := toKebabCase(schema.Name)
	options := &interfaces.S3StorageOptions{
		Enabled:      true,
		Bucket:       bucket,
		ObjectPrefix: prefix,
	}

	// Projection selection
	fields := s.getDisplayFields(schema)
	fmt.Println("\nSelect fields to keep in DynamoDB (comma-separated numbers). Leave blank for all fields:")
	for i, f := range fields {
		dn := f.Name
		if f.DisplayName != "" {
			dn = f.DisplayName
		}
		fmt.Printf("%d) %s (%s)\n", i+1, dn, f.TypeName)
	}
	projPrompt := promptui.Prompt{
		Label:   "Fields to keep",
		Default: "",
	}
	answer, err := s.runPromptInputWithContext(ctx, &projPrompt)
	if err != nil {
		return options, nil, nil // if cancelled, just keep all
	}
	answer = strings.TrimSpace(answer)
	if answer == "" {
		return options, nil, nil
	}
	indices := parseIndexList(answer)
	var selected []string
	for _, idx := range indices {
		if idx > 0 && idx <= len(fields) {
			selected = append(selected, fields[idx-1].Name)
		}
	}
	return options, selected, nil
}

func toKebabCase(s string) string {
	var out []rune
	for i, r := range s {
		if r >= 'A' && r <= 'Z' {
			if i > 0 && s[i-1] != '-' {
				out = append(out, '-')
			}
			out = append(out, r+('a'-'A'))
		} else if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			out = append(out, r)
		} else {
			out = append(out, '-')
		}
	}
	return strings.Trim(strings.ReplaceAll(string(out), "--", "-"), "-")
}

func parseIndexList(s string) []int {
	var result []int
	parts := strings.Split(s, ",")
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		val := 0
		for _, ch := range p {
			if ch < '0' || ch > '9' {
				val = 0
				break
			}
			val = val*10 + int(ch-'0')
		}
		if val > 0 {
			result = append(result, val)
		}
	}
	return result
}
