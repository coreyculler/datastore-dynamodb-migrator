package s3store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// Client wraps AWS S3 client and implements interfaces.S3Client-like API
type Client struct {
	client *s3.Client
	cfg    aws.Config
}

func NewClient(ctx context.Context) (*Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config for S3: %w", err)
	}
	return &Client{client: s3.NewFromConfig(cfg), cfg: cfg}, nil
}

func (c *Client) PutJSON(ctx context.Context, bucket string, key string, data interface{}, dryRun bool) (string, error) {
	path := fmt.Sprintf("s3://%s/%s", bucket, key)
	if dryRun {
		fmt.Printf("🔍 DRY RUN: Would upload JSON to %s\n", path)
		return path, nil
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON for %s: %w", path, err)
	}
	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(payload),
		ContentType: aws.String("application/json"),
		ACL:         types.ObjectCannedACLPrivate,
	}
	_, err = c.client.PutObject(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to PutObject %s: %w", path, err)
	}
	return path, nil
}

// EnsureBucket checks whether the bucket exists and creates it if it is missing.
// In dryRun mode, it only prints what it would do.
func (c *Client) EnsureBucket(ctx context.Context, bucket string, dryRun bool) error {
	if bucket == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}

	if dryRun {
		fmt.Printf("🔍 DRY RUN: Would ensure S3 bucket exists: %s (create if missing)\n", bucket)
		return nil
	}

	// HeadBucket is the fastest way to check existence and permissions.
	_, err := c.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err == nil {
		return nil
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		if code == "NotFound" || code == "404" || code == "NoSuchBucket" {
			// Create bucket in client's configured region
			region := c.cfg.Region
			createInput := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
			// For some regions, you must specify LocationConstraint; us-east-1 is special-case
			if region != "us-east-1" && region != "" {
				createInput.CreateBucketConfiguration = &types.CreateBucketConfiguration{
					LocationConstraint: types.BucketLocationConstraint(region),
				}
			}
			_, createErr := c.client.CreateBucket(ctx, createInput)
			if createErr != nil {
				return fmt.Errorf("failed to create bucket %s: %w", bucket, createErr)
			}
			// Wait until bucket exists
			waiter := s3.NewBucketExistsWaiter(c.client)
			if werr := waiter.Wait(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)}, 2*time.Minute); werr != nil {
				return fmt.Errorf("bucket %s creation not confirmed: %w", bucket, werr)
			}
			fmt.Printf("🪣 Created S3 bucket: %s\n", bucket)
			return nil
		}
	}

	// If it's not a NotFound, return the original error (permissions or others)
	return fmt.Errorf("failed to check bucket %s: %w", bucket, err)
}

func (c *Client) Close() error { return nil }
