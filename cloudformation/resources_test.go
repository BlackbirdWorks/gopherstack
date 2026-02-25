package cloudformation_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cloudformation"
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	kmsbackend "github.com/blackbirdworks/gopherstack/kms"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	smbackend "github.com/blackbirdworks/gopherstack/secretsmanager"
	snsbackend "github.com/blackbirdworks/gopherstack/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// newServiceBackends creates a ServiceBackends with all real in-memory backends.
func newServiceBackends() *cloudformation.ServiceBackends {
	return &cloudformation.ServiceBackends{
		DynamoDB:       ddbbackend.NewHandler(ddbbackend.NewInMemoryDB(), slog.Default()),
		S3:             s3backend.NewHandler(s3backend.NewInMemoryBackend(nil), slog.Default()),
		SQS:            sqsbackend.NewHandler(sqsbackend.NewInMemoryBackend(), slog.Default()),
		SNS:            snsbackend.NewHandler(snsbackend.NewInMemoryBackend(), slog.Default()),
		SSM:            ssmbackend.NewHandler(ssmbackend.NewInMemoryBackend(), slog.Default()),
		KMS:            kmsbackend.NewHandler(kmsbackend.NewInMemoryBackend(), slog.Default()),
		SecretsManager: smbackend.NewHandler(smbackend.NewInMemoryBackend(), slog.Default()),
		AccountID:      "000000000000",
		Region:         "us-east-1",
	}
}

func TestNewInMemoryBackend(t *testing.T) {
	t.Parallel()
	b := cloudformation.NewInMemoryBackend()
	require.NotNil(t, b)
	all := b.ListAll()
	assert.Empty(t, all)
}

// ---- ResourceCreator with nil backends (stub path) -------------------------

func TestResourceCreator_NilBackends_Create(t *testing.T) {
	t.Parallel()
	rc := cloudformation.NewResourceCreator(nil)
	physID, err := rc.Create(context.Background(), "MyBucket", "AWS::S3::Bucket", nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, "MyBucket")
}

func TestResourceCreator_NilBackends_Delete(t *testing.T) {
	t.Parallel()
	rc := cloudformation.NewResourceCreator(nil)
	err := rc.Delete(context.Background(), "AWS::S3::Bucket", "my-bucket", nil)
	require.NoError(t, err)
}

func TestResourceCreator_NilBackends_DefaultResource(t *testing.T) {
	t.Parallel()
	rc := cloudformation.NewResourceCreator(nil)
	physID, err := rc.Create(context.Background(), "MyRole", "AWS::IAM::Role", nil, nil, nil)
	require.NoError(t, err)
	// nil backends path: returns logicalID + "-" + uuid[:8]
	assert.Contains(t, physID, "MyRole-")
	assert.NotEqual(t, "MyRole-stub", physID)
}

// ---- ResourceCreator with real backends ------------------------------------

func TestResourceCreator_S3Bucket(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{"BucketName": "test-cfn-bucket"}
	physID, err := rc.Create(context.Background(), "MyBucket", "AWS::S3::Bucket", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "test-cfn-bucket", physID)

	err = rc.Delete(context.Background(), "AWS::S3::Bucket", physID, props)
	require.NoError(t, err)
}

func TestResourceCreator_S3Bucket_AutoName(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	physID, err := rc.Create(context.Background(), "MyBucket", "AWS::S3::Bucket", map[string]any{}, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, "mybucket")
}

func TestResourceCreator_DynamoDBTable(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"TableName": "cfn-test-table",
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "id", "AttributeType": "S"},
		},
		"KeySchema": []any{
			map[string]any{"AttributeName": "id", "KeyType": "HASH"},
		},
		"ProvisionedThroughput": map[string]any{
			"ReadCapacityUnits":  float64(5),
			"WriteCapacityUnits": float64(5),
		},
	}
	physID, err := rc.Create(context.Background(), "MyTable", "AWS::DynamoDB::Table", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "cfn-test-table", physID)

	err = rc.Delete(context.Background(), "AWS::DynamoDB::Table", physID, props)
	require.NoError(t, err)
}

func TestResourceCreator_DynamoDBTable_PAYPerRequest(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"TableName":   "cfn-ondemand-table",
		"BillingMode": "PAY_PER_REQUEST",
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "pk", "AttributeType": "S"},
		},
		"KeySchema": []any{
			map[string]any{"AttributeName": "pk", "KeyType": "HASH"},
		},
	}
	physID, err := rc.Create(context.Background(), "OnDemandTable", "AWS::DynamoDB::Table", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "cfn-ondemand-table", physID)
}

func TestResourceCreator_DynamoDBTable_DefaultName(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "id", "AttributeType": "N"},
		},
		"KeySchema": []any{
			map[string]any{"AttributeName": "id", "KeyType": "HASH"},
		},
	}
	physID, err := rc.Create(context.Background(), "MyTable", "AWS::DynamoDB::Table", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "MyTable", physID)
}

func TestResourceCreator_SQSQueue(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{"QueueName": "cfn-test-queue"}
	physID, err := rc.Create(context.Background(), "MyQueue", "AWS::SQS::Queue", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)

	err = rc.Delete(context.Background(), "AWS::SQS::Queue", physID, props)
	require.NoError(t, err)
}

func TestResourceCreator_SQSQueue_DefaultName(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"VisibilityTimeout": "30",
	}
	physID, err := rc.Create(context.Background(), "MyDefaultQueue", "AWS::SQS::Queue", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)
}

func TestResourceCreator_SQSQueue_FIFO(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"QueueName": "cfn-fifo-queue",
		"FifoQueue": true,
	}
	physID, err := rc.Create(context.Background(), "MyFIFOQueue", "AWS::SQS::Queue", props, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, ".fifo")
}

func TestResourceCreator_SNSTopic(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{"TopicName": "cfn-test-topic"}
	physID, err := rc.Create(context.Background(), "MyTopic", "AWS::SNS::Topic", props, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, "cfn-test-topic")

	err = rc.Delete(context.Background(), "AWS::SNS::Topic", physID, props)
	require.NoError(t, err)
}

func TestResourceCreator_SNSTopic_DefaultName(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{}
	physID, err := rc.Create(context.Background(), "MyDefaultTopic", "AWS::SNS::Topic", props, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, "MyDefaultTopic")
}

func TestResourceCreator_SSMParameter(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"Name":  "/cfn/test-param",
		"Type":  "String",
		"Value": "hello",
	}
	physID, err := rc.Create(context.Background(), "MyParam", "AWS::SSM::Parameter", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "/cfn/test-param", physID)

	err = rc.Delete(context.Background(), "AWS::SSM::Parameter", physID, props)
	require.NoError(t, err)
}

func TestResourceCreator_SSMParameter_DefaultName(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{"Value": "val"}
	physID, err := rc.Create(context.Background(), "MySSMParam", "AWS::SSM::Parameter", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "/MySSMParam", physID)
}

func TestResourceCreator_KMSKey(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{"Description": "cfn test key"}
	physID, err := rc.Create(context.Background(), "MyKey", "AWS::KMS::Key", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)

	err = rc.Delete(context.Background(), "AWS::KMS::Key", physID, props)
	require.NoError(t, err)
}

func TestResourceCreator_SecretsManagerSecret(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"Name":         "cfn-test-secret",
		"SecretString": `{"key":"value"}`,
	}
	physID, err := rc.Create(context.Background(), "MySecret", "AWS::SecretsManager::Secret", props, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, "cfn-test-secret")

	err = rc.Delete(context.Background(), "AWS::SecretsManager::Secret", physID, props)
	require.NoError(t, err)
}

func TestResourceCreator_SecretsManagerSecret_DefaultName(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{"SecretString": "secret"}
	physID, err := rc.Create(context.Background(), "MyDefaultSecret", "AWS::SecretsManager::Secret", props, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, "MyDefaultSecret")
}

func TestResourceCreator_UnknownType_Create(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	physID, err := rc.Create(context.Background(), "MyRole", "AWS::IAM::Role", map[string]any{}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "MyRole-stub", physID)
}

func TestResourceCreator_UnknownType_Delete(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	err := rc.Delete(context.Background(), "AWS::IAM::Role", "some-role", map[string]any{})
	require.NoError(t, err)
}

// ---- DynamoDB attribute type parsing ----------------------------------------

func TestResourceCreator_DynamoDBTable_BinaryAttributeType(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"TableName": "cfn-binary-table",
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "id", "AttributeType": "B"},
		},
		"KeySchema": []any{
			map[string]any{"AttributeName": "id", "KeyType": "HASH"},
		},
	}
	physID, err := rc.Create(context.Background(), "BinaryTable", "AWS::DynamoDB::Table", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "cfn-binary-table", physID)
}

func TestResourceCreator_DynamoDBTable_RangeKey(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"TableName": "cfn-range-table",
		"AttributeDefinitions": []any{
			map[string]any{"AttributeName": "pk", "AttributeType": "S"},
			map[string]any{"AttributeName": "sk", "AttributeType": "S"},
		},
		"KeySchema": []any{
			map[string]any{"AttributeName": "pk", "KeyType": "HASH"},
			map[string]any{"AttributeName": "sk", "KeyType": "RANGE"},
		},
	}
	physID, err := rc.Create(context.Background(), "RangeTable", "AWS::DynamoDB::Table", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "cfn-range-table", physID)
}

// ---- Provider.Init with BackendsProvider ------------------------------------

// mockBackendsProvider implements BackendsProvider to test Provider.Init.
type mockBackendsProvider struct {
	ddb *ddbbackend.DynamoDBHandler
	s3h *s3backend.S3Handler
	sqs *sqsbackend.Handler
	sns *snsbackend.Handler
	ssm *ssmbackend.Handler
	kms *kmsbackend.Handler
	sm  *smbackend.Handler
}

func newMockBackendsProvider() *mockBackendsProvider {
	return &mockBackendsProvider{
		ddb: ddbbackend.NewHandler(ddbbackend.NewInMemoryDB(), slog.Default()),
		s3h: s3backend.NewHandler(s3backend.NewInMemoryBackend(nil), slog.Default()),
		sqs: sqsbackend.NewHandler(sqsbackend.NewInMemoryBackend(), slog.Default()),
		sns: snsbackend.NewHandler(snsbackend.NewInMemoryBackend(), slog.Default()),
		ssm: ssmbackend.NewHandler(ssmbackend.NewInMemoryBackend(), slog.Default()),
		kms: kmsbackend.NewHandler(kmsbackend.NewInMemoryBackend(), slog.Default()),
		sm:  smbackend.NewHandler(smbackend.NewInMemoryBackend(), slog.Default()),
	}
}

func (m *mockBackendsProvider) GetDynamoDBHandler() service.Registerable       { return m.ddb }
func (m *mockBackendsProvider) GetS3Handler() service.Registerable             { return m.s3h }
func (m *mockBackendsProvider) GetSQSHandler() service.Registerable            { return m.sqs }
func (m *mockBackendsProvider) GetSNSHandler() service.Registerable            { return m.sns }
func (m *mockBackendsProvider) GetSSMHandler() service.Registerable            { return m.ssm }
func (m *mockBackendsProvider) GetKMSHandler() service.Registerable            { return m.kms }
func (m *mockBackendsProvider) GetSecretsManagerHandler() service.Registerable { return m.sm }
func (m *mockBackendsProvider) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: "111111111111", Region: "eu-west-1"}
}

func TestProvider_Init_WithBackendsProvider(t *testing.T) {
	t.Parallel()
	p := &cloudformation.Provider{}
	appCtx := &service.AppContext{
		Logger: slog.Default(),
		Config: newMockBackendsProvider(),
	}
	svc, err := p.Init(appCtx)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.Equal(t, "CloudFormation", svc.Name())
}

// mockConfigProvider implements only config.Provider (no BackendsProvider).
type mockConfigProvider struct{}

func (m *mockConfigProvider) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: "222222222222", Region: "ap-southeast-1"}
}

func TestProvider_Init_WithConfigProvider(t *testing.T) {
	t.Parallel()
	p := &cloudformation.Provider{}
	appCtx := &service.AppContext{
		Logger: slog.Default(),
		Config: &mockConfigProvider{},
	}
	svc, err := p.Init(appCtx)
	require.NoError(t, err)
	require.NotNil(t, svc)
	assert.Equal(t, "CloudFormation", svc.Name())
}

// ---- Stack creation with real resource provisioning --------------------------

func TestBackend_CreateStack_WithRealResources(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)

	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {"BucketName": "real-cfn-bucket"}
			}
		}
	}`
	stack, err := backend.CreateStack(context.Background(), "real-stack", tmpl, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

	err = backend.DeleteStack(context.Background(), "real-stack")
	require.NoError(t, err)
}

func TestBackend_CreateStack_WithSSMAndSNS(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)

	tmpl := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyParam": {
				"Type": "AWS::SSM::Parameter",
				"Properties": {
					"Name": "/test/cfn/param",
					"Type": "String",
					"Value": "test-value"
				}
			},
			"MyTopic": {
				"Type": "AWS::SNS::Topic",
				"Properties": {"TopicName": "real-cfn-topic"}
			}
		}
	}`
	stack, err := backend.CreateStack(context.Background(), "ssm-sns-stack", tmpl, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)
}

func TestBackend_UpdateStack_WithNewResource(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)

	tmpl1 := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {"BucketName": "upd-cfn-bucket"}
			}
		}
	}`
	_, err := backend.CreateStack(context.Background(), "upd-real-stack", tmpl1, nil, nil)
	require.NoError(t, err)

	// Update with an additional resource
	tmpl2 := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"MyBucket": {
				"Type": "AWS::S3::Bucket",
				"Properties": {"BucketName": "upd-cfn-bucket"}
			},
			"MyQueue": {
				"Type": "AWS::SQS::Queue",
				"Properties": {"QueueName": "upd-cfn-queue"}
			}
		}
	}`
	updated, err := backend.UpdateStack(context.Background(), "upd-real-stack", tmpl2, nil)
	require.NoError(t, err)
	assert.Equal(t, "UPDATE_COMPLETE", updated.StackStatus)
}

// ---- SQS MessageRetentionPeriod attribute ----------------------------------

func TestResourceCreator_SQSQueue_WithAttributes(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"QueueName":              "attr-queue",
		"VisibilityTimeout":      "45",
		"MessageRetentionPeriod": "86400",
	}
	physID, err := rc.Create(context.Background(), "AttrQueue", "AWS::SQS::Queue", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)
}
