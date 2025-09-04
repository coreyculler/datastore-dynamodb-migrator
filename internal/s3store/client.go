package s3store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func (c *Client) Close() error { return nil }
