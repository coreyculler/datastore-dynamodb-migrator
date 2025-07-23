# DataStore to DynamoDB Migration Utility

A command-line tool to migrate Google Cloud Platform (GCP) DataStore entities to Amazon Web Services (AWS) DynamoDB tables. Each DataStore Kind becomes a separate DynamoDB table with user-configured primary and sort keys.

## Features

- **Automatic Schema Discovery**: Uses reflection to analyze DataStore entities without requiring predefined types
- **Interactive Key Selection**: Guides users through selecting appropriate partition and sort keys for each Kind
- **Concurrent Processing**: Efficiently handles large datasets with configurable batch sizes and worker pools
- **Progress Tracking**: Real-time progress indicators with error reporting
- **Dry Run Mode**: Preview migration plans without executing them
- **Automatic Table Creation**: Creates DynamoDB tables with optimal configurations
- **Robust Error Handling**: Graceful failure recovery and detailed error reporting

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

./datastore-migrator
```

### Non-Interactive Migration
```bash
# Automatic migration using first field as partition key
./datastore-migrator --interactive=false --project=your-gcp-project
```

### Dry Run
```bash
# Preview migration plan without executing
./datastore-migrator --dry-run --project=your-gcp-project
```

### Custom Configuration
```bash
# High-performance migration with custom settings
./datastore-migrator \
  --project=your-gcp-project \
  --region=us-west-2 \
  --batch-size=500 \
  --max-workers=10
```

## Subcommands

### List DataStore Kinds
```bash
./datastore-migrator list-kinds --project=your-gcp-project
```

### Analyze Specific Kind
```bash
./datastore-migrator analyze --kind=Users --project=your-gcp-project
```

### Version Information
```bash
./datastore-migrator version
```

## Interactive Mode

When running in interactive mode, the tool will:

1. **Discover Kinds**: Automatically find all DataStore Kinds in your project
2. **Analyze Schemas**: Sample entities to understand field types and structures
3. **Key Selection**: For each Kind, you'll be prompted to:
   - Select a partition key (primary key)
   - Optionally select a sort key
   - Confirm table names
4. **Migration Plan**: Review the complete migration plan before execution
5. **Progress Tracking**: Monitor real-time progress with detailed statistics

### Example Interactive Session

```
DataStore to DynamoDB Migration Tool v1.0.0
Configuration: GCP Project: my-project, AWS Region: us-east-1, Batch Size: 100, Workers: 5

📋 Discovering DataStore Kinds...
✅ Found 3 DataStore Kinds

🔍 Analyzing Kind: Users...

=== Configuring Keys for Kind: Users ===
Total entities: 1,247
Available fields: 8

Field Information:
==================
1. id
   Type: string
   Sample: user_12345

2. email
   Type: string  
   Sample: john@example.com

3. created_at
   Type: time.Time
   Sample: 2023-10-15T10:30:00Z

4. profile
   Type: map[string]interface{}
   Sample: {"name": "John Doe", "age": 30}

Select the Partition Key (Primary Key) - this field should uniquely identify most entities:
▶ id (string)
  email (string)
  created_at (time.Time)
  profile (map[string]interface{})

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

./datastore-migrator --project=large-project
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
./datastore-migrator --batch-size=50 --max-workers=3
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