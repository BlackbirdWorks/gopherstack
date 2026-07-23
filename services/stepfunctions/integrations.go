package stepfunctions

import (
	"context"
	"encoding/json"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	dynamodbpkg "github.com/blackbirdworks/gopherstack/services/dynamodb"
	s3pkg "github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

// sqsAdapter adapts sqs.StorageBackend to asl.SQSIntegration.
type sqsAdapter struct {
	backend sqs.StorageBackend
}

// NewSQSIntegration creates a new SQS integration adapter.
func NewSQSIntegration(backend sqs.StorageBackend) asl.SQSIntegration {
	return &sqsAdapter{backend: backend}
}

// SFNSendMessage implements asl.SQSIntegration.
func (a *sqsAdapter) SFNSendMessage(
	_ context.Context,
	queueURL, messageBody, groupID, deduplicationID string,
	delaySeconds int,
) (string, string, error) {
	out, err := a.backend.SendMessage(&sqs.SendMessageInput{
		QueueURL:               queueURL,
		MessageBody:            messageBody,
		MessageGroupID:         groupID,
		MessageDeduplicationID: deduplicationID,
		DelaySeconds:           delaySeconds,
	})
	if err != nil {
		return "", "", err
	}

	return out.MessageID, out.MD5OfBody, nil
}

// snsAdapter adapts sns.StorageBackend to asl.SNSIntegration.
type snsAdapter struct {
	backend sns.StorageBackend
}

// NewSNSIntegration creates a new SNS integration adapter.
func NewSNSIntegration(backend sns.StorageBackend) asl.SNSIntegration {
	return &snsAdapter{backend: backend}
}

// SFNPublish implements asl.SNSIntegration.
func (a *snsAdapter) SFNPublish(_ context.Context, topicARN, message, subject string) (string, error) {
	return a.backend.Publish(topicARN, message, subject, "", nil)
}

// dynamoDBAdapter adapts dynamodb.StorageBackend to asl.DynamoDBIntegration.
type dynamoDBAdapter struct {
	backend dynamodbpkg.StorageBackend
}

// Compile-time assertion: dynamoDBAdapter must implement asl.DynamoDBIntegration.
var _ asl.DynamoDBIntegration = (*dynamoDBAdapter)(nil)

// NewDynamoDBIntegration creates a new DynamoDB integration adapter.
func NewDynamoDBIntegration(backend dynamodbpkg.StorageBackend) asl.DynamoDBIntegration {
	return &dynamoDBAdapter{backend: backend}
}

// s3Adapter adapts s3.StorageBackend to asl.S3Reader, used to resolve Map
// state ItemReader (Distributed Map) items from S3 objects.
type s3Adapter struct {
	backend s3pkg.StorageBackend
}

// Compile-time assertion: s3Adapter must implement asl.S3Reader.
var _ asl.S3Reader = (*s3Adapter)(nil)

// NewS3Integration creates a new S3 integration adapter for Map state ItemReader.
func NewS3Integration(backend s3pkg.StorageBackend) asl.S3Reader {
	return &s3Adapter{backend: backend}
}

// GetObjectBytes implements asl.S3Reader.
func (a *s3Adapter) GetObjectBytes(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := a.backend.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// convertViaJSON converts a value to a target type by marshaling to JSON and back.
func convertViaJSON(input, target any) error {
	b, err := json.Marshal(input)
	if err != nil {
		return err
	}

	return json.Unmarshal(b, target)
}

// outputToAny converts a typed SDK output struct to an untyped map via JSON round-trip.
// This is used by all DynamoDB adapter methods to return an `any` value that the
// ASL executor can pass as the state's Result.
func outputToAny(out any) (any, error) {
	var result any
	if err := convertViaJSON(out, &result); err != nil {
		return nil, err
	}

	return result, nil
}

// SFNPutItem implements asl.DynamoDBIntegration.
func (a *dynamoDBAdapter) SFNPutItem(ctx context.Context, input any) (any, error) {
	var req awsdynamodb.PutItemInput
	if err := convertViaJSON(input, &req); err != nil {
		return nil, err
	}

	out, err := a.backend.PutItem(ctx, &req)
	if err != nil {
		return nil, err
	}

	return outputToAny(out)
}

// SFNGetItem implements asl.DynamoDBIntegration.
func (a *dynamoDBAdapter) SFNGetItem(ctx context.Context, input any) (any, error) {
	var req awsdynamodb.GetItemInput
	if err := convertViaJSON(input, &req); err != nil {
		return nil, err
	}

	out, err := a.backend.GetItem(ctx, &req)
	if err != nil {
		return nil, err
	}

	return outputToAny(out)
}

// SFNDeleteItem implements asl.DynamoDBIntegration.
func (a *dynamoDBAdapter) SFNDeleteItem(ctx context.Context, input any) (any, error) {
	var req awsdynamodb.DeleteItemInput
	if err := convertViaJSON(input, &req); err != nil {
		return nil, err
	}

	out, err := a.backend.DeleteItem(ctx, &req)
	if err != nil {
		return nil, err
	}

	return outputToAny(out)
}

// SFNUpdateItem implements asl.DynamoDBIntegration.
func (a *dynamoDBAdapter) SFNUpdateItem(ctx context.Context, input any) (any, error) {
	var req awsdynamodb.UpdateItemInput
	if err := convertViaJSON(input, &req); err != nil {
		return nil, err
	}

	out, err := a.backend.UpdateItem(ctx, &req)
	if err != nil {
		return nil, err
	}

	return outputToAny(out)
}

// SFNBatchExecuteStatement implements asl.DynamoDBIntegration.
func (a *dynamoDBAdapter) SFNBatchExecuteStatement(ctx context.Context, input any) (any, error) {
	var req awsdynamodb.BatchExecuteStatementInput
	if err := convertViaJSON(input, &req); err != nil {
		return nil, err
	}

	out, err := a.backend.BatchExecuteStatement(ctx, &req)
	if err != nil {
		return nil, err
	}

	return outputToAny(out)
}

// SFNBatchGetItem implements asl.DynamoDBIntegration.
func (a *dynamoDBAdapter) SFNBatchGetItem(ctx context.Context, input any) (any, error) {
	var req awsdynamodb.BatchGetItemInput
	if err := convertViaJSON(input, &req); err != nil {
		return nil, err
	}

	out, err := a.backend.BatchGetItem(ctx, &req)
	if err != nil {
		return nil, err
	}

	return outputToAny(out)
}

// SFNBatchWriteItem implements asl.DynamoDBIntegration.
func (a *dynamoDBAdapter) SFNBatchWriteItem(ctx context.Context, input any) (any, error) {
	var req awsdynamodb.BatchWriteItemInput
	if err := convertViaJSON(input, &req); err != nil {
		return nil, err
	}

	out, err := a.backend.BatchWriteItem(ctx, &req)
	if err != nil {
		return nil, err
	}

	return outputToAny(out)
}

// SFNCreateBackup implements asl.DynamoDBIntegration.
func (a *dynamoDBAdapter) SFNCreateBackup(ctx context.Context, input any) (any, error) {
	var req awsdynamodb.CreateBackupInput
	if err := convertViaJSON(input, &req); err != nil {
		return nil, err
	}

	out, err := a.backend.CreateBackup(ctx, &req)
	if err != nil {
		return nil, err
	}

	return outputToAny(out)
}

// SFNCreateGlobalTable implements asl.DynamoDBIntegration.
func (a *dynamoDBAdapter) SFNCreateGlobalTable(ctx context.Context, input any) (any, error) {
	var req awsdynamodb.CreateGlobalTableInput
	if err := convertViaJSON(input, &req); err != nil {
		return nil, err
	}

	out, err := a.backend.CreateGlobalTable(ctx, &req)
	if err != nil {
		return nil, err
	}

	return outputToAny(out)
}

// SFNCreateTable implements asl.DynamoDBIntegration.
func (a *dynamoDBAdapter) SFNCreateTable(ctx context.Context, input any) (any, error) {
	var req awsdynamodb.CreateTableInput
	if err := convertViaJSON(input, &req); err != nil {
		return nil, err
	}

	out, err := a.backend.CreateTable(ctx, &req)
	if err != nil {
		return nil, err
	}

	return outputToAny(out)
}

// SFNDeleteBackup implements asl.DynamoDBIntegration.
func (a *dynamoDBAdapter) SFNDeleteBackup(ctx context.Context, input any) (any, error) {
	var req awsdynamodb.DeleteBackupInput
	if err := convertViaJSON(input, &req); err != nil {
		return nil, err
	}

	out, err := a.backend.DeleteBackup(ctx, &req)
	if err != nil {
		return nil, err
	}

	return outputToAny(out)
}

// SFNDeleteResourcePolicy implements asl.DynamoDBIntegration.
func (a *dynamoDBAdapter) SFNDeleteResourcePolicy(ctx context.Context, input any) (any, error) {
	var req awsdynamodb.DeleteResourcePolicyInput
	if err := convertViaJSON(input, &req); err != nil {
		return nil, err
	}

	out, err := a.backend.DeleteResourcePolicy(ctx, &req)
	if err != nil {
		return nil, err
	}

	return outputToAny(out)
}

// SFNDeleteTable implements asl.DynamoDBIntegration.
func (a *dynamoDBAdapter) SFNDeleteTable(ctx context.Context, input any) (any, error) {
	var req awsdynamodb.DeleteTableInput
	if err := convertViaJSON(input, &req); err != nil {
		return nil, err
	}

	out, err := a.backend.DeleteTable(ctx, &req)
	if err != nil {
		return nil, err
	}

	return outputToAny(out)
}
