package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"datastore-dynamodb-migrator/config"
	"datastore-dynamodb-migrator/internal/cli"
	"datastore-dynamodb-migrator/internal/datastore"
	"datastore-dynamodb-migrator/internal/dynamodb"
	"datastore-dynamodb-migrator/internal/interfaces"
	"datastore-dynamodb-migrator/internal/introspection"
	"datastore-dynamodb-migrator/internal/migration"
)

// Version information
var (
	version = "1.0.0"
	build   = "dev"
)

// Global flags
var (
	projectID   string
	awsRegion   string
	batchSize   int
	maxWorkers  int
	dryRun      bool
	interactive bool = true
	debug       bool
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "datastore-migrator",
		Short: "Migrate GCP DataStore to AWS DynamoDB",
		Long: `A command-line tool to migrate Google Cloud Platform DataStore entities 
to Amazon Web Services DynamoDB tables. Each DataStore Kind becomes a separate 
DynamoDB table with user-configured primary and sort keys.`,
		Version: fmt.Sprintf("%s (build: %s)", version, build),
		RunE:    runMigration,
	}

	// Add persistent flags
	rootCmd.PersistentFlags().StringVar(&projectID, "project", "", "GCP Project ID (can also use GCP_PROJECT_ID env var)")
	rootCmd.PersistentFlags().StringVar(&awsRegion, "region", "us-east-1", "AWS Region (can also use AWS_REGION env var)")
	rootCmd.PersistentFlags().IntVar(&batchSize, "batch-size", 100, "Number of entities to process in each batch")
	rootCmd.PersistentFlags().IntVar(&maxWorkers, "max-workers", 5, "Maximum number of concurrent workers")
	rootCmd.PersistentFlags().BoolVar(&dryRun, "dry-run", false, "Show what would be migrated without actually doing it")
	rootCmd.PersistentFlags().BoolVar(&interactive, "interactive", true, "Use interactive mode for key selection")
	rootCmd.PersistentFlags().BoolVar(&debug, "debug", false, "Enable debug logging for detailed error information")

	// Add subcommands
	rootCmd.AddCommand(listKindsCmd())
	rootCmd.AddCommand(analyzeCmd())
	rootCmd.AddCommand(versionCmd())

	// Execute the root command
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func runMigration(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown with timeout
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	shutdownStarted := false
	var shutdownMu sync.Mutex

	go func() {
		<-sigChan
		shutdownMu.Lock()
		if shutdownStarted {
			shutdownMu.Unlock()
			fmt.Println("\nForce terminating...")
			os.Exit(1)
		}
		shutdownStarted = true
		shutdownMu.Unlock()

		fmt.Println("\nReceived interrupt signal, shutting down gracefully...")
		fmt.Println("Press Ctrl+C again to force terminate")
		cancel()

		// Force termination after 30 seconds
		go func() {
			time.Sleep(30 * time.Second)
			fmt.Println("\nGraceful shutdown timeout, force terminating...")
			os.Exit(1)
		}()
	}()

	// Load configuration
	cfg, err := loadConfiguration()
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}

	fmt.Printf("DataStore to DynamoDB Migration Tool v%s\n", version)
	fmt.Printf("Configuration: %s\n\n", cfg.GetConnectionInfo())

	if dryRun {
		fmt.Println("🔍 DRY RUN MODE - No actual migration will be performed")
	}

	// Initialize clients
	datastoreClient, err := datastore.NewClient(ctx, cfg.GCP.ProjectID)
	if err != nil {
		return fmt.Errorf("failed to create DataStore client: %w", err)
	}
	defer datastoreClient.Close()

	dynamoClient, err := dynamodb.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create DynamoDB client: %w", err)
	}
	defer dynamoClient.Close()

	// Create migration engine
	engine := migration.NewEngine(datastoreClient, dynamoClient)
	engine.SetBatchSize(cfg.Migration.BatchSize)
	engine.SetMaxWorkers(cfg.Migration.MaxWorkers)
	engine.SetDebug(debug)

	// Set up the analyzer
	analyzer := introspection.NewEntityAnalyzer()
	engine.SetAnalyzer(analyzer)

	// Get list of Kinds
	fmt.Println("📋 Discovering DataStore Kinds...")
	kinds, err := datastoreClient.ListKinds(ctx)
	if err != nil {
		// Check if error is due to context cancellation
		if ctx.Err() != nil {
			fmt.Println("\nOperation cancelled by user")
			return nil
		}
		return fmt.Errorf("failed to list DataStore kinds: %w", err)
	}

	// Check for cancellation after listing kinds
	select {
	case <-ctx.Done():
		fmt.Println("\nOperation cancelled by user")
		return nil
	default:
	}

	if len(kinds) == 0 {
		fmt.Println("❌ No DataStore Kinds found in project")
		return nil
	}

	fmt.Printf("✅ Found %d DataStore Kinds\n\n", len(kinds))

	// Analyze schemas and configure migrations
	var migrationConfigs []interfaces.MigrationConfig
	selector := cli.NewInteractiveSelector()

	for _, kind := range kinds {
		// Check for cancellation at the beginning of each table processing
		select {
		case <-ctx.Done():
			fmt.Println("\nOperation cancelled by user")
			return nil
		default:
		}

		fmt.Printf("🔍 Analyzing Kind: %s...\n", kind)

		schema, err := datastoreClient.AnalyzeKind(ctx, kind)
		if err != nil {
			// Check if error is due to context cancellation
			if ctx.Err() != nil {
				fmt.Println("\nOperation cancelled by user")
				return nil
			}
			fmt.Printf("❌ Failed to analyze Kind %s: %v\n", kind, err)
			continue
		}

		// Check for cancellation after analysis
		select {
		case <-ctx.Done():
			fmt.Println("\nOperation cancelled by user")
			return nil
		default:
		}

		if schema.Count == 0 {
			fmt.Printf("⚠️  Kind %s has no entities, skipping\n\n", kind)
			continue
		}

		if len(schema.Fields) == 0 {
			fmt.Printf("⚠️  Kind %s has no analyzable fields, skipping\n\n", kind)
			continue
		}

		// Interactive key selection
		if interactive {
			// Ask if user wants to skip this Kind
			skipKind, err := selector.AskToSkipKind(ctx, *schema)
			if err != nil {
				if ctx.Err() != nil {
					fmt.Println("\nOperation cancelled by user")
					return nil
				}
				fmt.Printf("❌ Failed to get user choice for Kind %s: %v\n", kind, err)
				continue
			}

			if skipKind {
				fmt.Printf("⏭️  Skipping Kind: %s\n\n", kind)
				continue
			}

			keySelection, err := selector.SelectKeys(ctx, *schema)
			if err != nil {
				if ctx.Err() != nil {
					fmt.Println("\nOperation cancelled by user")
					return nil
				}
				fmt.Printf("❌ Failed to select keys for Kind %s: %v\n", kind, err)
				continue
			}

			// Get target table name
			defaultTableName := kind
			tableName, err := selector.SelectTargetTableName(ctx, defaultTableName)
			if err != nil {
				if ctx.Err() != nil {
					fmt.Println("\nOperation cancelled by user")
					return nil
				}
				fmt.Printf("❌ Failed to get table name for Kind %s: %v\n", kind, err)
				continue
			}

			config := interfaces.MigrationConfig{
				SourceKind:   kind,
				TargetTable:  tableName,
				KeySelection: keySelection,
				Schema:       *schema,
			}

			migrationConfigs = append(migrationConfigs, config)
		} else {
			// Non-interactive mode - use first field as partition key
			if len(schema.Fields) == 0 {
				fmt.Printf("⚠️  No fields available for Kind %s\n", kind)
				continue
			}

			config := interfaces.MigrationConfig{
				SourceKind:  kind,
				TargetTable: kind,
				KeySelection: interfaces.KeySelection{
					PartitionKey: schema.Fields[0].Name,
				},
				Schema: *schema,
			}

			migrationConfigs = append(migrationConfigs, config)
		}

		// Check for cancellation after configuration is built
		select {
		case <-ctx.Done():
			fmt.Println("\nOperation cancelled by user")
			return nil
		default:
		}
	}

	if len(migrationConfigs) == 0 {
		fmt.Println("❌ No valid migration configurations created")
		return nil
	}

	// Confirm migration plan
	if interactive && !dryRun {
		confirmed, err := selector.ConfirmMigration(ctx, migrationConfigs)
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("\nOperation cancelled by user")
				return nil
			}
			return fmt.Errorf("failed to confirm migration: %w", err)
		}

		if !confirmed {
			fmt.Println("Migration cancelled by user")
			return nil
		}
	}

	// Start migration (this now handles both dry-run and actual migration)
	if dryRun {
		fmt.Println("\n🔍 DRY RUN - Starting migration analysis...")
	} else {
		fmt.Println("\n🚀 Starting migration...")
	}

	stats := engine.GetMigrationStats(migrationConfigs)

	progressChan, err := engine.MigrateAll(ctx, migrationConfigs, dryRun)
	if err != nil {
		return fmt.Errorf("failed to start migration: %w", err)
	}

	// Track progress
	completed := make(map[string]bool)
	var totalErrors int64

	for {
		select {
		case <-ctx.Done():
			fmt.Println("\nMigration interrupted")
			return nil
		case progress, ok := <-progressChan:
			if !ok {
				// Channel closed, migration completed or stopped
				break
			}

			if interactive {
				selector.ShowMigrationProgress(progress)
			} else {
				if dryRun {
					fmt.Printf("DRY RUN - Kind %s: %d/%d analyzed, %d errors\n",
						progress.Kind, progress.Processed, progress.Total, progress.Errors)
				} else {
					fmt.Printf("Kind %s: %d/%d processed, %d errors\n",
						progress.Kind, progress.Processed, progress.Total, progress.Errors)
				}
			}

			if progress.Completed {
				completed[progress.Kind] = true
				totalErrors += progress.Errors
			}

			// Check if all migrations are complete
			if len(completed) == len(migrationConfigs) {
				break
			}
		}
	}

	// Final summary
	if dryRun {
		fmt.Printf("\n✅ DRY RUN completed!\n")
		fmt.Printf("📊 Analysis Summary: %d Kinds would be migrated, %d total entities would be processed\n",
			len(migrationConfigs), stats.TotalEntities)
		fmt.Println("💡 No actual changes were made. Run without --dry-run to perform the migration.")
	} else {
		fmt.Printf("\n✅ Migration completed!\n")
		fmt.Printf("📊 Summary: %d Kinds migrated, %d total entities\n",
			len(migrationConfigs), stats.TotalEntities)
	}

	if totalErrors > 0 {
		fmt.Printf("⚠️  Total errors: %d\n", totalErrors)
	}

	return nil
}

func loadConfiguration() (*config.Config, error) {
	cfg, err := config.LoadConfig()
	if err != nil {
		return nil, err
	}

	// Override with command line flags
	if projectID != "" {
		cfg.SetGCPProjectID(projectID)
	}

	if awsRegion != "" {
		cfg.SetAWSRegion(awsRegion)
	}

	cfg.SetBatchSize(batchSize)
	cfg.SetMaxWorkers(maxWorkers)
	cfg.SetDryRun(dryRun)

	return cfg, nil
}

func listKindsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list-kinds",
		Short: "List all DataStore Kinds in the project",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			cfg, err := loadConfiguration()
			if err != nil {
				return fmt.Errorf("failed to load configuration: %w", err)
			}

			client, err := datastore.NewClient(ctx, cfg.GCP.ProjectID)
			if err != nil {
				return fmt.Errorf("failed to create DataStore client: %w", err)
			}
			defer client.Close()

			kinds, err := client.ListKinds(ctx)
			if err != nil {
				return fmt.Errorf("failed to list kinds: %w", err)
			}

			if len(kinds) == 0 {
				fmt.Println("No DataStore Kinds found")
				return nil
			}

			fmt.Printf("Found %d DataStore Kinds:\n", len(kinds))
			for i, kind := range kinds {
				fmt.Printf("%d. %s\n", i+1, kind)
			}

			return nil
		},
	}
}

func analyzeCmd() *cobra.Command {
	var kindName string

	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Analyze a specific DataStore Kind",
		RunE: func(cmd *cobra.Command, args []string) error {
			if kindName == "" {
				return fmt.Errorf("kind name is required (use --kind flag)")
			}

			ctx := context.Background()

			cfg, err := loadConfiguration()
			if err != nil {
				return fmt.Errorf("failed to load configuration: %w", err)
			}

			client, err := datastore.NewClient(ctx, cfg.GCP.ProjectID)
			if err != nil {
				return fmt.Errorf("failed to create DataStore client: %w", err)
			}
			defer client.Close()

			schema, err := client.AnalyzeKind(ctx, kindName)
			if err != nil {
				return fmt.Errorf("failed to analyze kind: %w", err)
			}

			fmt.Printf("Kind: %s\n", schema.Name)
			fmt.Printf("Entity Count: %d\n", schema.Count)
			fmt.Printf("Fields: %d\n\n", len(schema.Fields))

			if len(schema.Fields) > 0 {
				fmt.Println("Fields:")
				for i, field := range schema.Fields {
					fmt.Printf("%d. %s (%s)\n", i+1, field.Name, field.TypeName)
					if field.Sample != nil {
						sampleStr := fmt.Sprintf("%v", field.Sample)
						if len(sampleStr) > 100 {
							sampleStr = sampleStr[:97] + "..."
						}
						fmt.Printf("   Sample: %s\n", sampleStr)
					}
				}
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&kindName, "kind", "", "Name of the Kind to analyze")
	cmd.MarkFlagRequired("kind")

	return cmd
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("DataStore to DynamoDB Migrator\n")
			fmt.Printf("Version: %s\n", version)
			fmt.Printf("Build: %s\n", build)
		},
	}
}
