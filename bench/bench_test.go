// Package bench_test contains benchmarks for core Gopherstack services.
// Benchmarks use in-process backends directly — no HTTP overhead — to give
// a pure measure of the storage / CPU cost of each operation.
//
// Run all benchmarks:
//
//	go test -bench=. -benchmem ./bench/
//
// Run a single service:
//
//	go test -bench=BenchmarkS3 -benchmem ./bench/
package bench_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"github.com/blackbirdworks/gopherstack/kms"
	"github.com/blackbirdworks/gopherstack/s3"
	"github.com/blackbirdworks/gopherstack/secretsmanager"
	"github.com/blackbirdworks/gopherstack/sqs"

	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// S3 benchmarks
// ----------------------------------------------------------------------------

func BenchmarkS3_PutObject(b *testing.B) {
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	_, setupErr := backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("bench")})
	require.NoError(b, setupErr)

	data := bytes.Repeat([]byte("x"), 1024) // 1 KiB payload

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		_, err := backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
			Bucket:   aws.String("bench"),
			Key:      aws.String(fmt.Sprintf("key-%d", i)),
			Body:     bytes.NewReader(data),
			Metadata: map[string]string{},
		})
		require.NoError(b, err)
	}
}

func BenchmarkS3_GetObject(b *testing.B) {
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	_, setupErr := backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("bench")})
	require.NoError(b, setupErr)

	data := bytes.Repeat([]byte("x"), 1024)

	for i := range 1000 {
		_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
			Bucket:   aws.String("bench"),
			Key:      aws.String(fmt.Sprintf("key-%d", i)),
			Body:     bytes.NewReader(data),
			Metadata: map[string]string{},
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		_, err := backend.GetObject(b.Context(), &sdk_s3.GetObjectInput{
			Bucket: aws.String("bench"),
			Key:    aws.String(fmt.Sprintf("key-%d", i%1000)),
		})
		require.NoError(b, err)
	}
}

func BenchmarkS3_ListObjectsV2(b *testing.B) {
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
	_, setupErr := backend.CreateBucket(b.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("bench")})
	require.NoError(b, setupErr)

	data := bytes.Repeat([]byte("x"), 64)

	for i := range 1000 {
		_, _ = backend.PutObject(b.Context(), &sdk_s3.PutObjectInput{
			Bucket:   aws.String("bench"),
			Key:      aws.String(fmt.Sprintf("key-%04d", i)),
			Body:     bytes.NewReader(data),
			Metadata: map[string]string{},
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := backend.ListObjectsV2(b.Context(), &sdk_s3.ListObjectsV2Input{
			Bucket: aws.String("bench"),
		})
		require.NoError(b, err)
	}
}

// ----------------------------------------------------------------------------
// SQS benchmarks
// ----------------------------------------------------------------------------

func BenchmarkSQS_SendMessage(b *testing.B) {
	backend := sqs.NewInMemoryBackend()
	out, setupErr := backend.CreateQueue(&sqs.CreateQueueInput{QueueName: "bench-queue"})
	require.NoError(b, setupErr)

	queueURL := out.QueueURL

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		_, err := backend.SendMessage(&sqs.SendMessageInput{
			QueueURL:    queueURL,
			MessageBody: fmt.Sprintf("message body %d", i),
		})
		require.NoError(b, err)
	}
}

func BenchmarkSQS_ReceiveMessage(b *testing.B) {
	backend := sqs.NewInMemoryBackend()
	out, setupErr := backend.CreateQueue(&sqs.CreateQueueInput{QueueName: "bench-queue"})
	require.NoError(b, setupErr)

	queueURL := out.QueueURL

	for i := range 1000 {
		_, _ = backend.SendMessage(&sqs.SendMessageInput{
			QueueURL:    queueURL,
			MessageBody: fmt.Sprintf("msg-%d", i),
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := backend.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueURL:            queueURL,
			MaxNumberOfMessages: 10,
		})
		require.NoError(b, err)
	}
}

// ----------------------------------------------------------------------------
// DynamoDB benchmarks
// ----------------------------------------------------------------------------

func setupDynamoDB(b *testing.B) *dynamodb.InMemoryDB {
	b.Helper()

	db := dynamodb.NewInMemoryDB()
	sdkInput := models.ToSDKCreateTableInput(&models.CreateTableInput{
		TableName: "BenchTable",
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
		},
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "id", KeyType: "HASH"},
		},
		ProvisionedThroughput: map[string]any{
			"ReadCapacityUnits":  int64(5),
			"WriteCapacityUnits": int64(5),
		},
	})
	_, err := db.CreateTable(context.Background(), sdkInput)
	require.NoError(b, err)

	return db
}

func BenchmarkDynamoDB_PutItem(b *testing.B) {
	db := setupDynamoDB(b)

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		input := models.PutItemInput{
			TableName: "BenchTable",
			Item:      map[string]any{"id": map[string]any{"S": strconv.Itoa(i)}},
		}
		sdkInput, err := models.ToSDKPutItemInput(&input)
		require.NoError(b, err)
		_, err = db.PutItem(context.Background(), sdkInput)
		require.NoError(b, err)
	}
}

func BenchmarkDynamoDB_GetItem(b *testing.B) {
	db := setupDynamoDB(b)

	for i := range 1000 {
		input := models.PutItemInput{
			TableName: "BenchTable",
			Item:      map[string]any{"id": map[string]any{"S": strconv.Itoa(i)}},
		}
		sdkInput, _ := models.ToSDKPutItemInput(&input)
		_, _ = db.PutItem(context.Background(), sdkInput)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		input := models.GetItemInput{
			TableName: "BenchTable",
			Key:       map[string]any{"id": map[string]any{"S": strconv.Itoa(i % 1000)}},
		}
		sdkInput, err := models.ToSDKGetItemInput(&input)
		require.NoError(b, err)
		_, err = db.GetItem(context.Background(), sdkInput)
		require.NoError(b, err)
	}
}

// ----------------------------------------------------------------------------
// KMS benchmarks
// ----------------------------------------------------------------------------

func BenchmarkKMS_Encrypt(b *testing.B) {
	backend := kms.NewInMemoryBackend()
	out, setupErr := backend.CreateKey(&kms.CreateKeyInput{Description: "bench"})
	require.NoError(b, setupErr)

	plaintext := bytes.Repeat([]byte("a"), 32)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := backend.Encrypt(&kms.EncryptInput{
			KeyID:     out.KeyMetadata.KeyID,
			Plaintext: plaintext,
		})
		require.NoError(b, err)
	}
}

func BenchmarkKMS_Decrypt(b *testing.B) {
	backend := kms.NewInMemoryBackend()
	out, setupErr := backend.CreateKey(&kms.CreateKeyInput{Description: "bench"})
	require.NoError(b, setupErr)

	plaintext := bytes.Repeat([]byte("a"), 32)
	enc, setupErr2 := backend.Encrypt(&kms.EncryptInput{KeyID: out.KeyMetadata.KeyID, Plaintext: plaintext})
	require.NoError(b, setupErr2)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := backend.Decrypt(&kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob})
		require.NoError(b, err)
	}
}

// ----------------------------------------------------------------------------
// Secrets Manager benchmarks
// ----------------------------------------------------------------------------

func BenchmarkSecretsManager_CreateSecret(b *testing.B) {
	backend := secretsmanager.NewInMemoryBackend()

	b.ResetTimer()
	b.ReportAllocs()

	for i := range b.N {
		_, err := backend.CreateSecret(&secretsmanager.CreateSecretInput{
			Name:         fmt.Sprintf("bench-secret-%d", i),
			SecretString: `{"key":"value"}`,
		})
		require.NoError(b, err)
	}
}

func BenchmarkSecretsManager_GetSecretValue(b *testing.B) {
	backend := secretsmanager.NewInMemoryBackend()
	_, setupErr := backend.CreateSecret(&secretsmanager.CreateSecretInput{
		Name:         "bench-secret",
		SecretString: `{"key":"value"}`,
	})
	require.NoError(b, setupErr)

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		_, err := backend.GetSecretValue(&secretsmanager.GetSecretValueInput{
			SecretID: "bench-secret",
		})
		require.NoError(b, err)
	}
}
