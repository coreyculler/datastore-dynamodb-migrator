package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type ConfigTestSuite struct {
	suite.Suite
	originalEnv map[string]string
}

func (suite *ConfigTestSuite) SetupTest() {
	// Store original environment variables
	suite.originalEnv = map[string]string{
		"GCP_PROJECT_ID":                 os.Getenv("GCP_PROJECT_ID"),
		"GOOGLE_APPLICATION_CREDENTIALS": os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"),
		"AWS_REGION":                     os.Getenv("AWS_REGION"),
		"AWS_PROFILE":                    os.Getenv("AWS_PROFILE"),
		"AWS_ACCESS_KEY_ID":              os.Getenv("AWS_ACCESS_KEY_ID"),
		"AWS_SECRET_ACCESS_KEY":          os.Getenv("AWS_SECRET_ACCESS_KEY"),
		"MIGRATION_BATCH_SIZE":           os.Getenv("MIGRATION_BATCH_SIZE"),
		"MIGRATION_MAX_WORKERS":          os.Getenv("MIGRATION_MAX_WORKERS"),
		"MIGRATION_DRY_RUN":              os.Getenv("MIGRATION_DRY_RUN"),
		"MIGRATION_CONTINUE_ON_ERROR":    os.Getenv("MIGRATION_CONTINUE_ON_ERROR"),
	}

	// Clear environment variables for clean test state
	for key := range suite.originalEnv {
		os.Unsetenv(key)
	}
}

func (suite *ConfigTestSuite) TearDownTest() {
	// Restore original environment variables
	for key, value := range suite.originalEnv {
		if value != "" {
			os.Setenv(key, value)
		} else {
			os.Unsetenv(key)
		}
	}
}

func (suite *ConfigTestSuite) TestLoadConfig_WithDefaults() {
	// Set required environment variables
	os.Setenv("GCP_PROJECT_ID", "test-project")

	config, err := LoadConfig()

	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), config)

	// Check GCP defaults
	assert.Equal(suite.T(), "test-project", config.GCP.ProjectID)
	assert.Equal(suite.T(), "", config.GCP.Credentials)

	// Check AWS defaults
	assert.Equal(suite.T(), "us-east-1", config.AWS.Region)
	assert.Equal(suite.T(), "", config.AWS.Profile)
	assert.Equal(suite.T(), "", config.AWS.AccessKeyID)
	assert.Equal(suite.T(), "", config.AWS.SecretAccessKey)

	// Check migration defaults
	assert.Equal(suite.T(), 100, config.Migration.BatchSize)
	assert.Equal(suite.T(), 5, config.Migration.MaxWorkers)
	assert.Equal(suite.T(), false, config.Migration.DryRun)
	assert.Equal(suite.T(), true, config.Migration.ContinueOnError)
}

func (suite *ConfigTestSuite) TestLoadConfig_WithEnvironmentVariables() {
	// Set all environment variables
	os.Setenv("GCP_PROJECT_ID", "env-project")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "/path/to/creds.json")
	os.Setenv("AWS_REGION", "us-west-2")
	os.Setenv("AWS_PROFILE", "test-profile")
	os.Setenv("AWS_ACCESS_KEY_ID", "test-key-id")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret-key")
	os.Setenv("MIGRATION_BATCH_SIZE", "250")
	os.Setenv("MIGRATION_MAX_WORKERS", "10")
	os.Setenv("MIGRATION_DRY_RUN", "true")
	os.Setenv("MIGRATION_CONTINUE_ON_ERROR", "false")

	config, err := LoadConfig()

	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), config)

	// Check GCP config
	assert.Equal(suite.T(), "env-project", config.GCP.ProjectID)
	assert.Equal(suite.T(), "/path/to/creds.json", config.GCP.Credentials)

	// Check AWS config
	assert.Equal(suite.T(), "us-west-2", config.AWS.Region)
	assert.Equal(suite.T(), "test-profile", config.AWS.Profile)
	assert.Equal(suite.T(), "test-key-id", config.AWS.AccessKeyID)
	assert.Equal(suite.T(), "test-secret-key", config.AWS.SecretAccessKey)

	// Check migration config
	assert.Equal(suite.T(), 250, config.Migration.BatchSize)
	assert.Equal(suite.T(), 10, config.Migration.MaxWorkers)
	assert.Equal(suite.T(), true, config.Migration.DryRun)
	assert.Equal(suite.T(), false, config.Migration.ContinueOnError)
}

func (suite *ConfigTestSuite) TestLoadConfig_MissingProjectID() {
	// Don't set GCP_PROJECT_ID
	config, err := LoadConfig()

	assert.Error(suite.T(), err)
	assert.Nil(suite.T(), config)
	assert.Contains(suite.T(), err.Error(), "GCP project ID is required")
}

func (suite *ConfigTestSuite) TestValidate_Success() {
	config := &Config{
		GCP: GCPConfig{
			ProjectID: "test-project",
		},
		AWS: AWSConfig{
			Region: "us-east-1",
		},
		Migration: MigrationSettings{
			BatchSize:  100,
			MaxWorkers: 5,
		},
	}

	err := config.Validate()
	assert.NoError(suite.T(), err)
}

func (suite *ConfigTestSuite) TestValidate_MissingProjectID() {
	config := &Config{
		GCP: GCPConfig{
			ProjectID: "",
		},
		AWS: AWSConfig{
			Region: "us-east-1",
		},
		Migration: MigrationSettings{
			BatchSize:  100,
			MaxWorkers: 5,
		},
	}

	err := config.Validate()
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "GCP project ID is required")
}

func (suite *ConfigTestSuite) TestValidate_MissingRegion() {
	config := &Config{
		GCP: GCPConfig{
			ProjectID: "test-project",
		},
		AWS: AWSConfig{
			Region: "",
		},
		Migration: MigrationSettings{
			BatchSize:  100,
			MaxWorkers: 5,
		},
	}

	err := config.Validate()
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "AWS region is required")
}

func (suite *ConfigTestSuite) TestValidate_InvalidBatchSize() {
	config := &Config{
		GCP: GCPConfig{
			ProjectID: "test-project",
		},
		AWS: AWSConfig{
			Region: "us-east-1",
		},
		Migration: MigrationSettings{
			BatchSize:  0,
			MaxWorkers: 5,
		},
	}

	err := config.Validate()
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "batch size must be greater than 0")
}

func (suite *ConfigTestSuite) TestValidate_InvalidMaxWorkers() {
	config := &Config{
		GCP: GCPConfig{
			ProjectID: "test-project",
		},
		AWS: AWSConfig{
			Region: "us-east-1",
		},
		Migration: MigrationSettings{
			BatchSize:  100,
			MaxWorkers: -1,
		},
	}

	err := config.Validate()
	assert.Error(suite.T(), err)
	assert.Contains(suite.T(), err.Error(), "max workers must be greater than 0")
}

func (suite *ConfigTestSuite) TestSetters() {
	config := &Config{}

	// Test SetGCPProjectID
	config.SetGCPProjectID("new-project")
	assert.Equal(suite.T(), "new-project", config.GCP.ProjectID)

	// Test SetAWSRegion
	config.SetAWSRegion("eu-west-1")
	assert.Equal(suite.T(), "eu-west-1", config.AWS.Region)

	// Test SetBatchSize
	config.SetBatchSize(500)
	assert.Equal(suite.T(), 500, config.Migration.BatchSize)

	// Test SetBatchSize with invalid value
	config.SetBatchSize(-10)
	assert.Equal(suite.T(), 500, config.Migration.BatchSize) // Should remain unchanged

	// Test SetMaxWorkers
	config.SetMaxWorkers(20)
	assert.Equal(suite.T(), 20, config.Migration.MaxWorkers)

	// Test SetMaxWorkers with invalid value
	config.SetMaxWorkers(0)
	assert.Equal(suite.T(), 20, config.Migration.MaxWorkers) // Should remain unchanged

	// Test SetDryRun
	config.SetDryRun(true)
	assert.Equal(suite.T(), true, config.Migration.DryRun)

	// Test SetContinueOnError
	config.SetContinueOnError(false)
	assert.Equal(suite.T(), false, config.Migration.ContinueOnError)
}

func (suite *ConfigTestSuite) TestGetConnectionInfo() {
	config := &Config{
		GCP: GCPConfig{
			ProjectID: "my-project",
		},
		AWS: AWSConfig{
			Region: "us-west-2",
		},
		Migration: MigrationSettings{
			BatchSize:  250,
			MaxWorkers: 8,
		},
	}

	info := config.GetConnectionInfo()
	expected := "GCP Project: my-project, AWS Region: us-west-2, Batch Size: 250, Workers: 8"
	assert.Equal(suite.T(), expected, info)
}

func (suite *ConfigTestSuite) TestHelperFunctions() {
	// Test getEnvOrDefault
	os.Setenv("TEST_STRING", "test-value")
	assert.Equal(suite.T(), "test-value", getEnvOrDefault("TEST_STRING", "default"))
	assert.Equal(suite.T(), "default", getEnvOrDefault("NON_EXISTENT", "default"))

	// Test getEnvOrDefaultInt
	os.Setenv("TEST_INT", "42")
	assert.Equal(suite.T(), 42, getEnvOrDefaultInt("TEST_INT", 10))
	assert.Equal(suite.T(), 10, getEnvOrDefaultInt("NON_EXISTENT", 10))

	os.Setenv("TEST_INVALID_INT", "not-a-number")
	assert.Equal(suite.T(), 10, getEnvOrDefaultInt("TEST_INVALID_INT", 10))

	// Test getEnvOrDefaultBool
	os.Setenv("TEST_BOOL_TRUE", "true")
	os.Setenv("TEST_BOOL_1", "1")
	os.Setenv("TEST_BOOL_YES", "yes")
	os.Setenv("TEST_BOOL_FALSE", "false")
	assert.Equal(suite.T(), true, getEnvOrDefaultBool("TEST_BOOL_TRUE", false))
	assert.Equal(suite.T(), true, getEnvOrDefaultBool("TEST_BOOL_1", false))
	assert.Equal(suite.T(), true, getEnvOrDefaultBool("TEST_BOOL_YES", false))
	assert.Equal(suite.T(), false, getEnvOrDefaultBool("TEST_BOOL_FALSE", true))
	assert.Equal(suite.T(), true, getEnvOrDefaultBool("NON_EXISTENT", true))

	// Clean up test environment variables
	os.Unsetenv("TEST_STRING")
	os.Unsetenv("TEST_INT")
	os.Unsetenv("TEST_INVALID_INT")
	os.Unsetenv("TEST_BOOL_TRUE")
	os.Unsetenv("TEST_BOOL_1")
	os.Unsetenv("TEST_BOOL_YES")
	os.Unsetenv("TEST_BOOL_FALSE")
}

func (suite *ConfigTestSuite) TestParseInt() {
	assert.Equal(suite.T(), 123, parseInt("123"))
	assert.Equal(suite.T(), 0, parseInt("abc"))
	assert.Equal(suite.T(), 0, parseInt("12a3"))
	assert.Equal(suite.T(), 0, parseInt(""))
	assert.Equal(suite.T(), 999, parseInt("999"))
}

func TestConfigSuite(t *testing.T) {
	suite.Run(t, new(ConfigTestSuite))
}

// Additional standalone tests
func TestBooleanEnvironmentVariableParsing(t *testing.T) {
	testCases := []struct {
		value    string
		expected bool
	}{
		{"true", true},
		{"TRUE", false}, // case sensitive
		{"1", true},
		{"yes", true},
		{"YES", false}, // case sensitive
		{"false", false},
		{"0", false},
		{"no", false},
		{"", false},
		{"invalid", false},
	}

	for _, tc := range testCases {
		t.Run(tc.value, func(t *testing.T) {
			os.Setenv("TEST_BOOL", tc.value)
			result := getEnvOrDefaultBool("TEST_BOOL", false)
			assert.Equal(t, tc.expected, result)
			os.Unsetenv("TEST_BOOL")
		})
	}
}
