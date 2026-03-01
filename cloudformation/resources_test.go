package cloudformation_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apigwbackend "github.com/blackbirdworks/gopherstack/apigateway"
	"github.com/blackbirdworks/gopherstack/cloudformation"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/cloudwatchlogs"
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	ebbackend "github.com/blackbirdworks/gopherstack/eventbridge"
	kmsbackend "github.com/blackbirdworks/gopherstack/kms"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	smbackend "github.com/blackbirdworks/gopherstack/secretsmanager"
	snsbackend "github.com/blackbirdworks/gopherstack/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
	sfnbackend "github.com/blackbirdworks/gopherstack/stepfunctions"

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
	physID, err := rc.Create(t.Context(), "MyBucket", "AWS::S3::Bucket", nil, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, "MyBucket")
}

func TestResourceCreator_NilBackends_Delete(t *testing.T) {
	t.Parallel()
	rc := cloudformation.NewResourceCreator(nil)
	err := rc.Delete(t.Context(), "AWS::S3::Bucket", "my-bucket", nil)
	require.NoError(t, err)
}

func TestResourceCreator_NilBackends_DefaultResource(t *testing.T) {
	t.Parallel()
	rc := cloudformation.NewResourceCreator(nil)
	physID, err := rc.Create(t.Context(), "MyRole", "AWS::IAM::Role", nil, nil, nil)
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
	physID, err := rc.Create(t.Context(), "MyBucket", "AWS::S3::Bucket", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "test-cfn-bucket", physID)

	err = rc.Delete(t.Context(), "AWS::S3::Bucket", physID, props)
	require.NoError(t, err)
}

func TestResourceCreator_S3Bucket_AutoName(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	physID, err := rc.Create(t.Context(), "MyBucket", "AWS::S3::Bucket", map[string]any{}, nil, nil)
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
	physID, err := rc.Create(t.Context(), "MyTable", "AWS::DynamoDB::Table", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "cfn-test-table", physID)

	err = rc.Delete(t.Context(), "AWS::DynamoDB::Table", physID, props)
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
	physID, err := rc.Create(t.Context(), "OnDemandTable", "AWS::DynamoDB::Table", props, nil, nil)
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
	physID, err := rc.Create(t.Context(), "MyTable", "AWS::DynamoDB::Table", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "MyTable", physID)
}

func TestResourceCreator_SQSQueue(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{"QueueName": "cfn-test-queue"}
	physID, err := rc.Create(t.Context(), "MyQueue", "AWS::SQS::Queue", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)

	err = rc.Delete(t.Context(), "AWS::SQS::Queue", physID, props)
	require.NoError(t, err)
}

func TestResourceCreator_SQSQueue_DefaultName(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{
		"VisibilityTimeout": "30",
	}
	physID, err := rc.Create(t.Context(), "MyDefaultQueue", "AWS::SQS::Queue", props, nil, nil)
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
	physID, err := rc.Create(t.Context(), "MyFIFOQueue", "AWS::SQS::Queue", props, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, ".fifo")
}

func TestResourceCreator_SNSTopic(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{"TopicName": "cfn-test-topic"}
	physID, err := rc.Create(t.Context(), "MyTopic", "AWS::SNS::Topic", props, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, "cfn-test-topic")

	err = rc.Delete(t.Context(), "AWS::SNS::Topic", physID, props)
	require.NoError(t, err)
}

func TestResourceCreator_SNSTopic_DefaultName(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{}
	physID, err := rc.Create(t.Context(), "MyDefaultTopic", "AWS::SNS::Topic", props, nil, nil)
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
	physID, err := rc.Create(t.Context(), "MyParam", "AWS::SSM::Parameter", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "/cfn/test-param", physID)

	err = rc.Delete(t.Context(), "AWS::SSM::Parameter", physID, props)
	require.NoError(t, err)
}

func TestResourceCreator_SSMParameter_DefaultName(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{"Value": "val"}
	physID, err := rc.Create(t.Context(), "MySSMParam", "AWS::SSM::Parameter", props, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "/MySSMParam", physID)
}

func TestResourceCreator_KMSKey(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{"Description": "cfn test key"}
	physID, err := rc.Create(t.Context(), "MyKey", "AWS::KMS::Key", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)

	err = rc.Delete(t.Context(), "AWS::KMS::Key", physID, props)
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
	physID, err := rc.Create(t.Context(), "MySecret", "AWS::SecretsManager::Secret", props, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, "cfn-test-secret")

	err = rc.Delete(t.Context(), "AWS::SecretsManager::Secret", physID, props)
	require.NoError(t, err)
}

func TestResourceCreator_SecretsManagerSecret_DefaultName(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	props := map[string]any{"SecretString": "secret"}
	physID, err := rc.Create(t.Context(), "MyDefaultSecret", "AWS::SecretsManager::Secret", props, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, "MyDefaultSecret")
}

func TestResourceCreator_UnknownType_Create(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	physID, err := rc.Create(t.Context(), "MyRole", "AWS::IAM::Role", map[string]any{}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "MyRole-stub", physID)
}

func TestResourceCreator_UnknownType_Delete(t *testing.T) {
	t.Parallel()
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	err := rc.Delete(t.Context(), "AWS::IAM::Role", "some-role", map[string]any{})
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
	physID, err := rc.Create(t.Context(), "BinaryTable", "AWS::DynamoDB::Table", props, nil, nil)
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
	physID, err := rc.Create(t.Context(), "RangeTable", "AWS::DynamoDB::Table", props, nil, nil)
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
func (m *mockBackendsProvider) GetLambdaHandler() service.Registerable         { return nil }
func (m *mockBackendsProvider) GetEventBridgeHandler() service.Registerable    { return nil }
func (m *mockBackendsProvider) GetStepFunctionsHandler() service.Registerable  { return nil }
func (m *mockBackendsProvider) GetCloudWatchLogsHandler() service.Registerable { return nil }
func (m *mockBackendsProvider) GetAPIGatewayHandler() service.Registerable     { return nil }
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
	stack, err := backend.CreateStack(t.Context(), "real-stack", tmpl, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_COMPLETE", stack.StackStatus)

	err = backend.DeleteStack(t.Context(), "real-stack")
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
	stack, err := backend.CreateStack(t.Context(), "ssm-sns-stack", tmpl, nil, nil)
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
	_, err := backend.CreateStack(t.Context(), "upd-real-stack", tmpl1, nil, nil)
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
	updated, err := backend.UpdateStack(t.Context(), "upd-real-stack", tmpl2, nil)
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
	physID, err := rc.Create(t.Context(), "AttrQueue", "AWS::SQS::Queue", props, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)
}

// newExtendedServiceBackends creates a ServiceBackends with all backends including extended types.
func newExtendedServiceBackends() *cloudformation.ServiceBackends {
	b := newServiceBackends()
	b.EventBridge = ebbackend.NewHandler(
		ebbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"), slog.Default())
	b.StepFunctions = sfnbackend.NewHandler(
		sfnbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"), slog.Default())
	b.CloudWatchLogs = cwlogsbackend.NewHandler(
		cwlogsbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"), slog.Default())
	b.APIGateway = apigwbackend.NewHandler(apigwbackend.NewInMemoryBackend(), slog.Default())

	return b
}

// ---- Extended resource types (Lambda nil-backend stub path) ----------------

func TestResourceCreator_Lambda_NilBackend_Stub(t *testing.T) {
	t.Parallel()
	// newServiceBackends() leaves Lambda=nil, so createLambdaFunction returns a stub.
	backends := newServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	physID, err := rc.Create(t.Context(), "MyFunction", "AWS::Lambda::Function",
		map[string]any{"FunctionName": "my-fn", "Runtime": "python3.12", "Handler": "index.handler"},
		nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "MyFunction-stub", physID)

	// delete also goes through the nil path
	err = rc.Delete(t.Context(), "AWS::Lambda::Function", "my-fn", nil)
	require.NoError(t, err)
}

// ---- Extended resource types with real backends ----------------------------

func TestResourceCreator_EventBridgeRule_RealBackend(t *testing.T) {
	t.Parallel()
	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	physID, err := rc.Create(t.Context(), "MyRule", "AWS::Events::Rule",
		map[string]any{
			"Name":         "my-cfn-rule",
			"EventPattern": `{"source":["aws.s3"]}`,
			"State":        "ENABLED",
		}, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, "my-cfn-rule")

	err = rc.Delete(t.Context(), "AWS::Events::Rule", physID, nil)
	require.NoError(t, err)
}

func TestResourceCreator_StepFunctionsStateMachine_RealBackend(t *testing.T) {
	t.Parallel()
	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	definition := `{"Comment":"test","StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`
	physID, err := rc.Create(t.Context(), "MyStateMachine", "AWS::StepFunctions::StateMachine",
		map[string]any{
			"StateMachineName": "cfn-sm",
			"DefinitionString": definition,
			"RoleArn":          "arn:aws:iam::000000000000:role/sfn-role",
		}, nil, nil)
	require.NoError(t, err)
	assert.Contains(t, physID, "cfn-sm")

	err = rc.Delete(t.Context(), "AWS::StepFunctions::StateMachine", physID, nil)
	require.NoError(t, err)
}

func TestResourceCreator_CloudWatchLogGroup_RealBackend(t *testing.T) {
	t.Parallel()
	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	physID, err := rc.Create(t.Context(), "MyLogGroup", "AWS::Logs::LogGroup",
		map[string]any{"LogGroupName": "/cfn/my-log-group"},
		nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "/cfn/my-log-group", physID)

	err = rc.Delete(t.Context(), "AWS::Logs::LogGroup", physID, nil)
	require.NoError(t, err)
}

func TestResourceCreator_APIGatewayRestAPI_RealBackend(t *testing.T) {
	t.Parallel()
	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	physID, err := rc.Create(t.Context(), "MyAPI", "AWS::ApiGateway::RestApi",
		map[string]any{"Name": "cfn-rest-api", "Description": "created by cfn"},
		nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)

	err = rc.Delete(t.Context(), "AWS::ApiGateway::RestApi", physID, nil)
	require.NoError(t, err)
}

func TestResourceCreator_ExtendedResource_DefaultStub(t *testing.T) {
	t.Parallel()
	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Unknown extended resource type → returns logicalID + "-stub"
	physID, err := rc.Create(t.Context(), "MyWhatever", "AWS::Whatever::Thing", nil, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "MyWhatever-stub", physID)

	err = rc.Delete(t.Context(), "AWS::Whatever::Thing", "whatever-id", nil)
	require.NoError(t, err)
}
