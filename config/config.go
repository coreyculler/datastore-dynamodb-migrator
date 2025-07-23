package config

import (
	"fmt"
	"os"
)

// Config holds all configuration for the migration tool
type Config struct {
	GCP       GCPConfig         `json:"gcp"`
	AWS       AWSConfig         `json:"aws"`
	Migration MigrationSettings `json:"migration"`
}

// GCPConfig holds Google Cloud Platform configuration
type GCPConfig struct {
	ProjectID   string `json:"project_id"`
	Credentials string `json:"credentials,omitempty"` // Path to service account JSON file
}

// AWSConfig holds Amazon Web Services configuration
type AWSConfig struct {
	Region          string `json:"region"`
	Profile         string `json:"profile,omitempty"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
}

// MigrationSettings holds settings for the migration process
type MigrationSettings struct {
	BatchSize       int  `json:"batch_size"`
	MaxWorkers      int  `json:"max_workers"`
	DryRun          bool `json:"dry_run"`
	ContinueOnError bool `json:"continue_on_error"`
}

// LoadConfig loads configuration from environment variables and defaults
func LoadConfig() (*Config, error) {
	config := &Config{
		GCP: GCPConfig{
			ProjectID:   getEnvOrDefault("GCP_PROJECT_ID", ""),
			Credentials: getEnvOrDefault("GOOGLE_APPLICATION_CREDENTIALS", ""),
		},
		AWS: AWSConfig{
			Region:          getEnvOrDefault("AWS_REGION", "us-east-1"),
			Profile:         getEnvOrDefault("AWS_PROFILE", ""),
			AccessKeyID:     getEnvOrDefault("AWS_ACCESS_KEY_ID", ""),
			SecretAccessKey: getEnvOrDefault("AWS_SECRET_ACCESS_KEY", ""),
		},
		Migration: MigrationSettings{
			BatchSize:       getEnvOrDefaultInt("MIGRATION_BATCH_SIZE", 100),
			MaxWorkers:      getEnvOrDefaultInt("MIGRATION_MAX_WORKERS", 5),
			DryRun:          getEnvOrDefaultBool("MIGRATION_DRY_RUN", false),
			ContinueOnError: getEnvOrDefaultBool("MIGRATION_CONTINUE_ON_ERROR", true),
		},
	}

	// Validate required fields
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return config, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	// Validate GCP configuration
	if c.GCP.ProjectID == "" {
		return fmt.Errorf("GCP project ID is required (set GCP_PROJECT_ID environment variable)")
	}

	// Validate AWS configuration
	if c.AWS.Region == "" {
		return fmt.Errorf("AWS region is required (set AWS_REGION environment variable)")
	}

	// Validate migration settings
	if c.Migration.BatchSize <= 0 {
		return fmt.Errorf("batch size must be greater than 0")
	}

	if c.Migration.MaxWorkers <= 0 {
		return fmt.Errorf("max workers must be greater than 0")
	}

	return nil
}

// SetGCPProjectID sets the GCP project ID
func (c *Config) SetGCPProjectID(projectID string) {
	c.GCP.ProjectID = projectID
}

// SetAWSRegion sets the AWS region
func (c *Config) SetAWSRegion(region string) {
	c.AWS.Region = region
}

// SetBatchSize sets the migration batch size
func (c *Config) SetBatchSize(size int) {
	if size > 0 {
		c.Migration.BatchSize = size
	}
}

// SetMaxWorkers sets the maximum number of workers
func (c *Config) SetMaxWorkers(workers int) {
	if workers > 0 {
		c.Migration.MaxWorkers = workers
	}
}

// SetDryRun sets the dry run flag
func (c *Config) SetDryRun(dryRun bool) {
	c.Migration.DryRun = dryRun
}

// SetContinueOnError sets the continue on error flag
func (c *Config) SetContinueOnError(continueOnError bool) {
	c.Migration.ContinueOnError = continueOnError
}

// GetConnectionInfo returns a summary of connection information
func (c *Config) GetConnectionInfo() string {
	return fmt.Sprintf(
		"GCP Project: %s, AWS Region: %s, Batch Size: %d, Workers: %d",
		c.GCP.ProjectID,
		c.AWS.Region,
		c.Migration.BatchSize,
		c.Migration.MaxWorkers,
	)
}

// helper functions

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvOrDefaultInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		// Try to parse the integer value
		if intValue := parseInt(value); intValue > 0 {
			return intValue
		}
	}
	return defaultValue
}

func getEnvOrDefaultBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		return value == "true" || value == "1" || value == "yes"
	}
	return defaultValue
}

func parseInt(s string) int {
	result := 0
	for _, char := range s {
		if char < '0' || char > '9' {
			return 0
		}
		result = result*10 + int(char-'0')
	}
	return result
}
