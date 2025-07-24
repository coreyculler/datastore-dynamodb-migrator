# DataStore to DynamoDB Migration Utility

A command-line tool to migrate Google Cloud Platform (GCP) DataStore entities to Amazon Web Services (AWS) DynamoDB tables. Each DataStore Kind becomes a separate DynamoDB table with user-configured primary and sort keys.

## Features

- **Automatic Schema Discovery**: Uses reflection to analyze DataStore entities without requiring predefined types
- **Interactive Key Selection**: Guides users through selecting appropriate partition and sort keys for each Kind
- **Selective Migration**: Allows users to skip specific Kinds during the migration process
- **Concurrent Processing**: Efficiently handles large datasets with configurable batch sizes and worker pools
- **Progress Tracking**: Real-time progress indicators with error reporting
- **Dry Run Mode**: Preview migration plans without executing them
- **Automatic Table Creation**: Creates DynamoDB tables with optimal configurations
- **Robust Error Handling**: Graceful failure recovery and detailed error reporting
- **Smart Kind Filtering**: Automatically excludes DataStore system entities (kinds beginning with `__Stat`) from migration

## Prerequisites

### GCP Requirements
- Google Cloud Platform project with DataStore enabled
- Service account with DataStore read permissions
- `GOOGLE_APPLICATION_CREDENTIALS` environment variable set (or use `gcloud auth application-default login`)

### AWS Requirements
- AWS account with DynamoDB access
- AWS credentials configured (via AWS CLI, environment variables, or IAM roles)
- Appropriate DynamoDB permissions (CreateTable, PutItem, DescribeTable)

## Installation

### Option 1: Build from Source
```bash
git clone <repository-url>
cd datastore-dynamodb-migrator
make build
```

### Option 2: Using Go Install
```bash
go install github.com/your-org/datastore-dynamodb-migrator@latest
```

### Available Make Targets
The project includes a comprehensive Makefile with the following targets:

```bash
make build       # Build the binary
make clean       # Clean build artifacts
make test        # Run tests
make fmt         # Format code
make tidy        # Tidy dependencies
make run         # Build and run the application
make dev         # Quick development build
make help        # Show all available targets
```

For cross-platform builds:
```bash
make build-all   # Build for Linux, macOS, and Windows
```

## Configuration

The tool can be configured through environment variables or command-line flags.

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GCP_PROJECT_ID` | GCP Project ID containing DataStore | **Required** |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service account JSON | Auto-discovered |
| `AWS_REGION` | AWS region for DynamoDB tables | `us-east-1` |
| `AWS_PROFILE` | AWS profile to use | Default profile |
| `MIGRATION_BATCH_SIZE` | Number of entities per batch | `100` |
| `MIGRATION_MAX_WORKERS` | Maximum concurrent workers | `5` |
| `MIGRATION_DRY_RUN` | Enable dry run mode | `false` |

### Command-Line Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--project` | GCP Project ID | From env var |
| `--region` | AWS Region | `us-east-1` |
| `--batch-size` | Batch size for processing | `100` |
| `--max-workers` | Maximum concurrent workers | `5` |
| `--dry-run` | Show what would be migrated | `false` |
| `--interactive` | Use interactive mode | `true` |

## Usage

### Basic Migration
```bash
# Interactive migration with guided key selection
export GCP_PROJECT_ID="your-gcp-project"
export AWS_REGION="us-west-2"

./datastore-dynamodb-migrator
```

### Non-Interactive Migration
```bash
# Automatic migration using first field as partition key
./datastore-dynamodb-migrator --interactive=false --project=your-gcp-project
```

### Dry Run
```bash
# Preview migration plan without executing
./datastore-dynamodb-migrator --dry-run --project=your-gcp-project
```

### Custom Configuration
```bash
# High-performance migration with custom settings
./datastore-dynamodb-migrator \
  --project=your-gcp-project \
  --region=us-west-2 \
  --batch-size=500 \
  --max-workers=10
```

## Subcommands

### List DataStore Kinds
```bash
./datastore-dynamodb-migrator list-kinds --project=your-gcp-project
```

### Analyze Specific Kind
```bash
./datastore-dynamodb-migrator analyze --kind=Users --project=your-gcp-project
```

### Version Information
```bash
./datastore-dynamodb-migrator version
```

## Interactive Mode

When running in interactive mode, the tool will:

1. **Discover Kinds**: Automatically find all DataStore Kinds in your project
2. **Analyze Schemas**: Sample entities to understand field types and structures
3. **Kind Selection**: For each Kind, you'll be prompted to:
   - Choose whether to migrate or skip the Kind
   - Preview field information to help make the decision
4. **Key Selection**: For Kinds you choose to migrate, you'll be prompted to:
   - Select a partition key from available fields (including the DataStore entity key identifier)
   - Optionally select a sort key
   - Confirm table names
5. **Migration Plan**: Review the complete migration plan before execution
6. **Progress Tracking**: Monitor real-time progress with detailed statistics

## Key Selection

### DataStore Entity Key Identifier

The migration tool automatically includes the DataStore entity key identifier for every Kind. This represents the unique identifier from DataStore entity keys and is available for selection as the partition key.

**Field name selection:**
- **`id`**: Used when no existing "id" field exists in the entity properties
- **`__key__`**: Used when an "id" field already exists to avoid conflicts

**Key characteristics:**
- **Always available**: Present for all DataStore entities regardless of their schema
- **Unique identifier**: Contains either the entity's key name (string) or key ID (converted to string)
- **Default selection**: Automatically selected as the default partition key choice
- **Recommended**: Generally the best choice for partition key as it ensures unique identification

**Field extraction logic:**
1. If the entity key has a name, uses the key name
2. If the entity key has an ID (numeric), converts it to string format
3. Falls back to the full key string representation if neither is available

This field bridges the gap between DataStore's key-based identification system and DynamoDB's attribute-based keys, ensuring every entity has a reliable unique identifier for migration.

### Example Interactive Session

```
DataStore to DynamoDB Migration Tool v1.0.0
Configuration: GCP Project: my-project, AWS Region: us-east-1, Batch Size: 100, Workers: 5

📋 Discovering DataStore Kinds...
✅ Found 3 DataStore Kinds

🔍 Analyzing Kind: Users...

=== DataStore Kind: Users ===
Total entities: 1,247
Available fields: 8

Field preview:
  - id (string)
  - email (string)
  - created_at (time.Time)
  - profile (map[string]interface{})
  - user_id (string)

What would you like to do with this Kind?
▶ Configure and migrate this Kind
  Skip this Kind (do not migrate)

✓ Configure and migrate this Kind

=== Configuring Keys for Kind: Users ===
Total entities: 1,247
Available fields: 8

Field Information:
==================
1. id
   Type: string
   Sample: user_12345_key

2. email
   Type: string  
   Sample: john@example.com

3. created_at
   Type: time.Time
   Sample: 2023-10-15T10:30:00Z

4. profile
   Type: map[string]interface{}
   Sample: {"name": "John Doe", "age": 30}

5. user_id
   Type: string
   Sample: custom_user_123

Select the Partition Key - this field should uniquely identify most entities:
▶ id (string)
  email (string)
  created_at (time.Time)
  profile (map[string]interface{})
  user_id (string)

✓ Partition Key: id (string)

Do you want to add a Sort Key? (Useful for composite keys or ordering):
▶ No, partition key only
  Yes, add a sort key

✓ No, partition key only

=== Key Selection Summary ===
Partition Key: id
Sort Key: None

Confirm this key configuration?
▶ Yes, proceed with this configuration
  No, let me choose again

✓ Yes, proceed with this configuration

Enter DynamoDB table name [Users]: Users

=== Migration Plan Summary ===
Total Kinds to migrate: 3

1. Users → Users
   Entities: 1,247
   Partition Key: id
   Sort Key: None

2. Orders → Orders
   Entities: 5,432
   Partition Key: order_id
   Sort Key: created_at

3. Products → Products
   Entities: 892
   Partition Key: product_id
   Sort Key: None

Ready to start migration?
▶ Yes, start migration
  No, cancel migration

✓ Yes, start migration

🚀 Starting migration...
Users: 1247/1247 (100.0%) | Errors: 0 - COMPLETED SUCCESSFULLY
Orders: 5432/5432 (100.0%) | Errors: 0 - COMPLETED SUCCESSFULLY  
Products: 892/892 (100.0%) | Errors: 0 - COMPLETED SUCCESSFULLY

✅ Migration completed!
📊 Summary: 3 Kinds migrated, 7571 total entities
```

### Example: Skipping a Kind

You can also choose to skip Kinds that you don't want to migrate:

```
🔍 Analyzing Kind: AuditLogs...

=== DataStore Kind: AuditLogs ===
Total entities: 15,234
Available fields: 6

Field preview:
  - id (string)
  - timestamp (time.Time)
  - action (string)
  - user_id (string)
  - details (map[string]interface{})

What would you like to do with this Kind?
  Configure and migrate this Kind
▶ Skip this Kind (do not migrate)

✓ Skip this Kind (do not migrate)

⏭️  Skipping Kind: AuditLogs

🔍 Analyzing Kind: Users...
```

## DataStore Kind Filtering

The migration tool automatically filters out certain DataStore system entities to ensure only user data is migrated:

### Excluded Kinds
- **System Statistics**: All kinds beginning with `__Stat` are automatically excluded from both the `list-kinds` command and migration process
- These include system-generated entities like:
  - `__Stat_Kind__` (DataStore kind statistics)
  - `__Stat_PropertyType__` (Property type statistics)  
  - `__Stat_PropertyName_Kind__` (Property name statistics)
  - Any other `__Stat*` entities

### Why Filter These?
DataStore automatically generates these system entities for internal statistics and monitoring. They:
- Are not user data that should be migrated
- May have different access patterns and constraints
- Could interfere with normal migration processes
- Are specific to DataStore's internal implementation

This filtering ensures you only migrate your actual application data while avoiding system-level entities that aren't relevant in DynamoDB.

## DynamoDB Table Configuration

The tool automatically creates DynamoDB tables with the following settings:

- **Billing Mode**: Pay-per-request (on-demand)
- **Key Schema**: Based on your interactive selections
- **Attribute Definitions**: Automatically inferred from field types
- **Tags**: Includes migration metadata for tracking

### Supported Field Types

| DataStore Type | DynamoDB Type | Notes |
|----------------|---------------|--------|
| `string` | String (S) | Direct mapping |
| `int64` | Number (N) | Converted to string representation |
| `float64` | Number (N) | Converted to string representation |
| `bool` | Boolean (BOOL) | Direct mapping |
| `[]byte` | Binary (B) | Direct mapping |
| `time.Time` | Number (N) | Converted to Unix timestamp |
| `[]interface{}` | List (L) | Recursive conversion |
| `map[string]interface{}` | Map (M) | Recursive conversion |
| Custom structs | Map (M) | Reflected and converted |

## Performance Optimization

### Batch Size Tuning
- **Small datasets (< 1K entities)**: Use batch size 50-100
- **Medium datasets (1K - 100K entities)**: Use batch size 100-250  
- **Large datasets (> 100K entities)**: Use batch size 250-500

### Worker Pool Tuning
- **Conservative**: 3-5 workers (default)
- **Aggressive**: 10-20 workers (ensure adequate AWS service limits)
- **Monitor**: Watch for DynamoDB throttling and adjust accordingly

### Example High-Performance Configuration
```bash
export MIGRATION_BATCH_SIZE=500
export MIGRATION_MAX_WORKERS=15

./datastore-dynamodb-migrator --project=large-project
```

## Error Handling

The tool provides comprehensive error handling:

- **Network Issues**: Automatic retries with exponential backoff
- **Rate Limiting**: Respects AWS DynamoDB throttling limits  
- **Data Conversion**: Clear error messages for unsupported data types
- **Partial Failures**: Continues processing other entities/kinds on errors
- **Progress Preservation**: Resume capabilities for large migrations

### Common Issues and Solutions

#### Authentication Errors
```bash
# GCP Authentication
gcloud auth application-default login

# AWS Authentication  
aws configure
# or
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
```

#### Permission Errors
Ensure your credentials have:
- **GCP**: `datastore.entities.list`, `datastore.entities.get`
- **AWS**: `dynamodb:CreateTable`, `dynamodb:PutItem`, `dynamodb:DescribeTable`

#### Rate Limiting
Reduce batch size and worker count:
```bash
./datastore-dynamodb-migrator --batch-size=50 --max-workers=3
```

## Development

### Project Structure
```
├── main.go                 # CLI entry point
├── config/                 # Configuration management
├── internal/
│   ├── interfaces/         # Common interfaces and types
│   ├── datastore/         # GCP DataStore client
│   ├── dynamodb/          # AWS DynamoDB client
│   ├── migration/         # Migration orchestration
│   ├── introspection/     # Schema analysis
│   └── cli/               # Interactive user interface
└── README.md              # This file
```

### Building
```bash
make build        # Standard build
make dev          # Quick development build (no optimizations)
make build-all    # Cross-platform builds
```

### Testing
```bash
make test            # Run all tests
make test-coverage   # Run tests with coverage report
```

### Development Workflow
```bash
make clean          # Clean previous builds
make fmt            # Format code
make lint           # Run linter (requires golangci-lint)
make tidy           # Clean up dependencies
make build          # Build the project
make run            # Build and run
```

### Contributing
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues, questions, or contributions:
- Create an issue in the GitHub repository
- Check existing documentation and examples
- Review error messages and logs for troubleshooting guidance 