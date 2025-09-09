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
- **S3 Offloading for Large Records**: Optionally store full records as JSON in S3 and keep a minimal searchable projection in DynamoDB, with an added `S3ObjectPath` attribute
- **Datastore Query Ordering**: Optionally order the Datastore query by any field (asc/desc) to migrate oldest/newest first. This affects only read order, not DynamoDB schema.

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
git clone https://github.com/coreyculler/datastore-dynamodb-migrator.git
cd datastore-dynamodb-migrator
make build
```

### Option 2: Using Go Install
```bash
go install github.com/coreyculler/datastore-dynamodb-migrator@latest
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
| `DATASTORE_DATABASE_ID` | Datastore/Firestore database ID to use (`(default)` if empty) | empty = `(default)` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP service account JSON | Auto-discovered |
| `AWS_REGION` | AWS region for DynamoDB tables | `us-east-1` |
| `AWS_PROFILE` | AWS profile to use | Default profile |
| `MIGRATION_BATCH_SIZE` | Number of entities per batch | `100` |
| `MIGRATION_MAX_WORKERS` | Maximum concurrent workers | `5` |
| `MIGRATION_DRY_RUN` | Enable dry run mode | `false` |
| `MIGRATION_S3_BUCKET` | Default S3 bucket for full-record JSON storage | none |

### Command-Line Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--project` | GCP Project ID | From env var |
| `--database-id` | Datastore/Firestore database ID (defaults to `(default)`) | From env var |
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
# Optional: target a non-default database
export DATASTORE_DATABASE_ID="my-db"

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
```bashWSmake 
./datastore-dynamodb-migrator analyze --kind=Users --project=your-gcp-project
```

### Version Information
```bash
./datastore-dynamodb-migrator version
```

## Interactive Mode

When running in interactive mode, the tool will:

1. **Select Database (if not provided)**: Lists available Datastore/Firestore database IDs in the project and lets you choose. `(default)` is the default.
2. **Discover Kinds**: Automatically find all DataStore Kinds in your project
3. **Analyze Schemas**: Sample entities to understand field types and structures
4. **Kind Selection**: For each Kind, you'll be prompted to:
   - Choose whether to migrate or skip the Kind
   - Preview field information to help make the decision
5. **Key Selection**: For Kinds you choose to migrate, you'll be prompted to:
   - Select a partition key from available fields (including the DataStore entity key identifier)
   - Optionally select a sort key
   - Optionally rename the DynamoDB partition key attribute (the value still comes from the selected source field)
   - Confirm table names
6. **S3 Storage & Projection**: Optionally enable S3 storage for full records and choose which fields to keep in DynamoDB
7. **Datastore Query Ordering (Optional)**: Choose a field and direction (ascending/descending) to order the Datastore query. This only affects the order in which records are read and migrated; it has no impact on DynamoDB schema or stored items.
8. **Migration Plan**: Review the complete migration plan before execution
9. **Progress Tracking**: Monitor real-time progress with detailed statistics

### Ordering Notes
- Selecting a field adds `.Order("field")` or `.Order("-field")` to the underlying Datastore query.
- Useful for time-based migrations (e.g., `created_at` ascending for oldest-first, descending for newest-first).
- If no ordering is selected, Datastore's default ordering is used.

## Key Selection

### DataStore Entity Key Identifier

The migration tool automatically includes the DataStore entity key identifier for every Kind. This represents the unique identifier from DataStore entity keys and is available for selection as the partition key. In the interactive UI, this field is shown as "DataStore Primary Key (ID/Name)".

**Field name selection (internal):**
- Internally stored as `PK` and derived from the DataStore entity key
- You may rename the DynamoDB attribute used for the partition key. The alias affects only the attribute name in DynamoDB; the value continues to be sourced from the DataStore key (ID or Name) via the `PK` source

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

### Attribute Renaming

During key selection, after choosing the source field for the partition key, you can enter a custom DynamoDB attribute name. The tool will:
- Use the chosen source field (e.g., `PK`) to populate the partition key value
- Create the DynamoDB table using your alias as the key attribute name
- Normalize the value to a string and remove any synthetic/metadata fields like `PK`, `__key__`, `__key_id__`, `__key_name__` from the final item payload

### Metadata Stripping

To preserve the original record shape, any metadata fields not present on the original DataStore record are removed before writing to DynamoDB. Examples include `__key__`, `__key_name__`, `__key_id__`. The tool injects only the alias attribute for the partition key you selected.

## S3 Offloading Mode

You can choose to store the entire record in an Amazon S3 bucket as a JSON object while keeping only a minimal projection in DynamoDB for search/indexing:

- When enabled per Kind, every entity is uploaded to S3 as a JSON file.
- The S3 object key uses the Kind name as a kebab-case prefix and the primary key as the filename: `<kebab-kind>/<primary-key>.json`.
  - Example: Kind `UserActions` → prefix `user-actions/`; PK `abc123` → `user-actions/abc123.json`.
- The full S3 path is added to the DynamoDB item as `S3ObjectPath` (e.g., `s3://my-bucket/user-actions/abc123.json`).
- You will be prompted to select which fields should remain in DynamoDB. The partition/sort keys and `S3ObjectPath` are always included.
- If you leave the projection selection blank, all fields are kept in DynamoDB in addition to S3.

### Configuration

- Default bucket can be provided via `MIGRATION_S3_BUCKET`. You can override it per Kind during interactive prompts.
- If the provided S3 bucket does not exist, the tool will automatically create it in your configured AWS region during the interactive setup (skipped in dry-run).

### Notes

- S3 JSON uses the normalized entity representation used for DynamoDB conversion (metadata fields like `__key__` are omitted).
- This mode is useful when records are too large for DynamoDB item size limits or when you prefer to store rich payloads externally.

### Example Interactive Session

``` 
# See Migration Plan section for ordering summary lines like:
#    Datastore Order: created_at (desc)
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