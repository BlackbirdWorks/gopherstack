package integration_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// startPurgeContainer starts a Gopherstack container with AUTO_PURGE_TTL set.
func startPurgeContainer(t *testing.T, ttl string) (testcontainers.Container, string) {
	t.Helper()
	ctx := t.Context()

	dockerfile := "Dockerfile"
	if _, err := os.Stat("../../bin/gopherstack"); err == nil {
		dockerfile = "Dockerfile.test"
	}

	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    "../../",
			Dockerfile: dockerfile,
		},
		Env: map[string]string{
			"AUTO_PURGE_TTL": ttl,
		},
		ExposedPorts: []string{"8000/tcp"},
		WaitingFor: wait.ForHTTP("/_gopherstack/health").
			WithStatusCodeMatcher(func(status int) bool { return status == 200 }).
			WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = container.Terminate(ctx)
	})

	mappedPort, err := container.MappedPort(ctx, "8000")
	require.NoError(t, err)

	return container, "http://localhost:" + mappedPort.Port()
}

func TestIntegration_AutoPurgeTTL_SupportsGranularPurge(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// 1. Start Gopherstack with 20s TTL
	_, ep := startPurgeContainer(t, "20s")

	cfg, err := awsconfig.LoadDefaultConfig(
		t.Context(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(ep)
		o.UsePathStyle = true
	})
	ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(ep)
	})
	sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(ep)
	})
	snsClient := sns.NewFromConfig(cfg, func(o *sns.Options) {
		o.BaseEndpoint = aws.String(ep)
	})
	iamClient := iam.NewFromConfig(cfg, func(o *iam.Options) {
		o.BaseEndpoint = aws.String(ep)
	})
	lambdaClient := lambda.NewFromConfig(cfg, func(o *lambda.Options) {
		o.BaseEndpoint = aws.String(ep)
	})

	ctx := t.Context()

	// 2. Create "old" resources
	bucketOld := "old-bucket"
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucketOld})
	require.NoError(t, err)

	tableOld := "old-table"
	_, err = ddbClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &tableOld,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{
			AttributeName: aws.String("id"),
			AttributeType: ddbtypes.ScalarAttributeTypeS,
		}},
		KeySchema: []ddbtypes.KeySchemaElement{{
			AttributeName: aws.String("id"),
			KeyType:       ddbtypes.KeyTypeHash,
		}},
		ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	require.NoError(t, err)

	queueOld := "old-queue"
	_, err = sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &queueOld})
	require.NoError(t, err)

	topicOld := "old-topic"
	_, err = snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: &topicOld})
	require.NoError(t, err)

	userOld := "old-user"
	_, err = iamClient.CreateUser(ctx, &iam.CreateUserInput{UserName: &userOld})
	require.NoError(t, err)

	funcOld := "old-func"
	_, err = lambdaClient.CreateFunction(ctx, &lambda.CreateFunctionInput{
		FunctionName: &funcOld,
		Role:         aws.String("arn:aws:iam::000000000000:role/dummy"),
		Code:         &lambdatypes.FunctionCode{ImageUri: aws.String("dummy")},
		PackageType:  lambdatypes.PackageTypeImage,
	})
	require.NoError(t, err)

	// 3. Wait for TTL to pass (20s + buffer)
	t.Log("Waiting for resources to expire...")
	time.Sleep(22 * time.Second)

	// 4. Create "new" resources
	bucketNew := "new-bucket"
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucketNew})
	require.NoError(t, err)

	tableNew := "new-table"
	_, err = ddbClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: &tableNew,
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{
			AttributeName: aws.String("id"),
			AttributeType: ddbtypes.ScalarAttributeTypeS,
		}},
		KeySchema: []ddbtypes.KeySchemaElement{{
			AttributeName: aws.String("id"),
			KeyType:       ddbtypes.KeyTypeHash,
		}},
		ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
	})
	require.NoError(t, err)

	queueNew := "new-queue"
	_, err = sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: &queueNew})
	require.NoError(t, err)

	topicNew := "new-topic"
	_, err = snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: &topicNew})
	require.NoError(t, err)

	userNew := "new-user"
	_, err = iamClient.CreateUser(ctx, &iam.CreateUserInput{UserName: &userNew})
	require.NoError(t, err)

	funcNew := "new-func"
	_, err = lambdaClient.CreateFunction(ctx, &lambda.CreateFunctionInput{
		FunctionName: &funcNew,
		Role:         aws.String("arn:aws:iam::000000000000:role/dummy"),
		Code:         &lambdatypes.FunctionCode{ImageUri: aws.String("dummy")},
		PackageType:  lambdatypes.PackageTypeImage,
	})
	require.NoError(t, err)

	// 5. Poll until old S3 bucket is purged or deadline is reached (TTL ticker fires every 20s).
	t.Log("Waiting for purge cycle...")
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		result, listErr := s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
		if listErr == nil {
			found := false
			for _, b := range result.Buckets {
				if *b.Name == bucketOld {
					found = true

					break
				}
			}
			if !found {
				break
			}
		}
		time.Sleep(2 * time.Second)
	}

	// 6. Verify old are gone, new remain
	// S3
	buckets, err := s3Client.ListBuckets(ctx, &s3.ListBucketsInput{})
	require.NoError(t, err)
	foundOldS3, foundNewS3 := false, false
	for _, b := range buckets.Buckets {
		if *b.Name == bucketOld {
			foundOldS3 = true
		}
		if *b.Name == bucketNew {
			foundNewS3 = true
		}
	}
	assert.False(t, foundOldS3, "old bucket should be purged")
	assert.True(t, foundNewS3, "new bucket should remain")

	// DynamoDB
	tables, err := ddbClient.ListTables(ctx, &dynamodb.ListTablesInput{})
	require.NoError(t, err)
	foundOldDDB, foundNewDDB := false, false
	for _, tName := range tables.TableNames {
		if tName == tableOld {
			foundOldDDB = true
		}
		if tName == tableNew {
			foundNewDDB = true
		}
	}
	assert.False(t, foundOldDDB, "old table should be purged")
	assert.True(t, foundNewDDB, "new table should remain")

	// SQS
	queues, err := sqsClient.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	foundOldSQS, foundNewSQS := false, false
	for _, qURL := range queues.QueueUrls {
		if strings.Contains(qURL, queueOld) {
			foundOldSQS = true
		}
		if strings.Contains(qURL, queueNew) {
			foundNewSQS = true
		}
	}
	assert.False(t, foundOldSQS, "old queue should be purged")
	assert.True(t, foundNewSQS, "new queue should remain")

	// SNS
	snsTopics, err := snsClient.ListTopics(ctx, &sns.ListTopicsInput{})
	require.NoError(t, err)
	foundOldSNS, foundNewSNS := false, false
	for _, tp := range snsTopics.Topics {
		if strings.Contains(*tp.TopicArn, topicOld) {
			foundOldSNS = true
		}
		if strings.Contains(*tp.TopicArn, topicNew) {
			foundNewSNS = true
		}
	}
	assert.False(t, foundOldSNS, "old topic should be purged")
	assert.True(t, foundNewSNS, "new topic should remain")

	// IAM
	iamUsers, err := iamClient.ListUsers(ctx, &iam.ListUsersInput{})
	require.NoError(t, err)
	foundOldIAM, foundNewIAM := false, false
	for _, u := range iamUsers.Users {
		if *u.UserName == userOld {
			foundOldIAM = true
		}
		if *u.UserName == userNew {
			foundNewIAM = true
		}
	}
	assert.False(t, foundOldIAM, "old user should be purged")
	assert.True(t, foundNewIAM, "new user should remain")

	// Lambda
	lambdaFuncs, err := lambdaClient.ListFunctions(ctx, &lambda.ListFunctionsInput{})
	require.NoError(t, err)
	foundOldLambda, foundNewLambda := false, false
	for _, f := range lambdaFuncs.Functions {
		if *f.FunctionName == funcOld {
			foundOldLambda = true
		}
		if *f.FunctionName == funcNew {
			foundNewLambda = true
		}
	}
	assert.False(t, foundOldLambda, "old function should be purged")
	assert.True(t, foundNewLambda, "new function should remain")
}
