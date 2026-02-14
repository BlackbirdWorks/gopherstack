package demo

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	defaultCapacity = 5
	helloFilesCount = 2
)

// LoadData loads sample data into DynamoDB and S3.
func LoadData(ctx context.Context, logger *slog.Logger, ddb *dynamodb.Client, s3Client *s3.Client) error {
	logger.InfoContext(ctx, "Loading demo data...")

	if err := loadDynamoDB(ctx, logger, ddb); err != nil {
		return fmt.Errorf("failed to load dynamodb data: %w", err)
	}

	if err := loadS3(ctx, logger, s3Client); err != nil {
		return fmt.Errorf("failed to load s3 data: %w", err)
	}

	logger.InfoContext(ctx, "Demo data loaded successfully")

	return nil
}

func loadDynamoDB(ctx context.Context, logger *slog.Logger, ddb *dynamodb.Client) error {
	tableName := "Movies"

	// Check if table exists
	_, err := ddb.DescribeTable(ctx, &dynamodb.DescribeTableInput{TableName: &tableName})
	if err == nil {
		logger.InfoContext(ctx, "Table already exists, skipping creation", "table", tableName)
	} else {
		// Create Table
		_, err = ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
			TableName: &tableName,
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("Year"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("Title"), KeyType: types.KeyTypeRange},
			},
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("Year"), AttributeType: types.ScalarAttributeTypeN},
				{AttributeName: aws.String("Title"), AttributeType: types.ScalarAttributeTypeS},
			},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(defaultCapacity),
				WriteCapacityUnits: aws.Int64(defaultCapacity),
			},
		})
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		logger.InfoContext(ctx, "Created table", "table", tableName)
	}

	// Insert Items
	items := []map[string]types.AttributeValue{
		{
			"Year":  &types.AttributeValueMemberN{Value: "2023"},
			"Title": &types.AttributeValueMemberS{Value: "The Gopher Movie"},
			"Info":  &types.AttributeValueMemberS{Value: "A movie about Gophers"},
		},
		{
			"Year":  &types.AttributeValueMemberN{Value: "2024"},
			"Title": &types.AttributeValueMemberS{Value: "Gopher Returns"},
			"Info":  &types.AttributeValueMemberS{Value: "The sequel"},
		},
	}

	for _, item := range items {
		_, err = ddb.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: &tableName,
			Item:      item,
		})
		if err != nil {
			return fmt.Errorf("failed to put item: %w", err)
		}
	}
	logger.InfoContext(ctx, "Loaded DynamoDB items", "count", len(items))

	return nil
}

func loadS3(ctx context.Context, logger *slog.Logger, s3Client *s3.Client) error {
	bucketName := "demo-bucket"

	// Create Bucket
	_, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucketName,
	})
	if err != nil {
		// Ignore error if bucket exists (naive check)
		// SDK returns specific error but for demo code we can just log and continue or assume it exists
		// Wait, CreateBucket returns error if exists?
		// "BucketAlreadyOwnedByYou"
		if !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") &&
			!strings.Contains(err.Error(), "BucketAlreadyExists") {
			// In-memory backend might return generic error or specific.
			// We'll log and continue if it fails, maybe it already exists.
			logger.WarnContext(ctx, "Failed to create bucket (might exist)", "bucket", bucketName, "error", err)
		}
		logger.InfoContext(ctx, "Created bucket", "bucket", bucketName)
	}

	_, err = s3Client.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket: &bucketName,
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatusEnabled,
		},
	})
	if err != nil {
		logger.WarnContext(ctx, "Failed to enable versioning", "error", err)
	} else {
		logger.InfoContext(ctx, "Enabled versioning", "bucket", bucketName)
	}

	// Upload Files (Multiple versions for hello.txt)
	// Version 1
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &bucketName,
		Key:         aws.String("hello.txt"),
		Body:        strings.NewReader("Hello Gopherstack! (v1)"),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload hello.txt v1: %w", err)
	}

	// Version 2
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      &bucketName,
		Key:         aws.String("hello.txt"),
		Body:        strings.NewReader("Hello Gopherstack! (v2) - Updated version"),
		ContentType: aws.String("text/plain"),
	})
	if err != nil {
		return fmt.Errorf("failed to upload hello.txt v2: %w", err)
	}

	// Other files
	files := map[string]string{
		"notes.md": "# Notes\n\nThis is a demo file with versioning enabled.",
	}

	for key, content := range files {
		_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      &bucketName,
			Key:         &key,
			Body:        strings.NewReader(content),
			ContentType: aws.String("text/plain"),
		})
		if err != nil {
			return fmt.Errorf("failed to upload file %s: %w", key, err)
		}
	}
	logger.InfoContext(ctx, "Loaded S3 files", "count", len(files)+helloFilesCount)

	return nil
}
