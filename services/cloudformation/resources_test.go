package cloudformation_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	smbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// newServiceBackends creates a ServiceBackends with all real in-memory backends.
func newServiceBackends() *cloudformation.ServiceBackends {
	return &cloudformation.ServiceBackends{
		DynamoDB:       ddbbackend.NewHandler(ddbbackend.NewInMemoryDB()),
		S3:             s3backend.NewHandler(s3backend.NewInMemoryBackend(nil)),
		SQS:            sqsbackend.NewHandler(sqsbackend.NewInMemoryBackend()),
		SNS:            snsbackend.NewHandler(snsbackend.NewInMemoryBackend()),
		SSM:            ssmbackend.NewHandler(ssmbackend.NewInMemoryBackend()),
		KMS:            kmsbackend.NewHandler(kmsbackend.NewInMemoryBackend()),
		SecretsManager: smbackend.NewHandler(smbackend.NewInMemoryBackend()),
		AccountID:      "000000000000",
		Region:         "us-east-1",
	}
}

// ---- NewInMemoryBackend -----------------------------------------------------

func TestNewInMemoryBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantEmpty bool
	}{
		{
			name:      "empty_on_creation",
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudformation.NewInMemoryBackend()
			require.NotNil(t, b)

			all := b.ListAll()

			if tt.wantEmpty {
				assert.Empty(t, all)
			}
		})
	}
}

// ---- ResourceCreator: nil backends ------------------------------------------

func TestResourceCreator_NilBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
		physID       string
		wantContains string
		wantNotEq    string
		isDelete     bool
	}{
		{
			name:         "create_s3",
			logicalID:    "MyBucket",
			resourceType: "AWS::S3::Bucket",
			wantContains: "MyBucket",
		},
		{
			name:         "delete_s3",
			resourceType: "AWS::S3::Bucket",
			physID:       "my-bucket",
			isDelete:     true,
		},
		{
			name:         "create_default_resource",
			logicalID:    "MyRole",
			resourceType: "AWS::IAM::Role",
			wantContains: "MyRole-",
			wantNotEq:    "MyRole-stub",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := cloudformation.NewResourceCreator(nil)

			if tt.isDelete {
				err := rc.Delete(t.Context(), tt.resourceType, tt.physID, tt.props)
				require.NoError(t, err)

				return
			}

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)

			if tt.wantContains != "" {
				assert.Contains(t, physID, tt.wantContains)
			}

			if tt.wantNotEq != "" {
				assert.NotEqual(t, tt.wantNotEq, physID)
			}
		})
	}
}

// ---- ResourceCreator: S3 Bucket ---------------------------------------------

func TestResourceCreator_S3Bucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		logicalID    string
		props        map[string]any
		wantPhysID   string
		wantContains string
		doDelete     bool
	}{
		{
			name:       "explicit_name",
			logicalID:  "MyBucket",
			props:      map[string]any{"BucketName": "test-cfn-bucket"},
			wantPhysID: "test-cfn-bucket",
			doDelete:   true,
		},
		{
			name:         "auto_name",
			logicalID:    "MyBucket",
			props:        map[string]any{},
			wantContains: "mybucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::S3::Bucket", tt.props, nil, nil)
			require.NoError(t, err)

			if tt.wantPhysID != "" {
				assert.Equal(t, tt.wantPhysID, physID)
			}

			if tt.wantContains != "" {
				assert.Contains(t, physID, tt.wantContains)
			}

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::S3::Bucket", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

// ---- ResourceCreator: DynamoDB Table ----------------------------------------

func TestResourceCreator_DynamoDBTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		logicalID  string
		props      map[string]any
		wantPhysID string
		doDelete   bool
	}{
		{
			name:      "provisioned_throughput",
			logicalID: "MyTable",
			props: map[string]any{
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
			},
			wantPhysID: "cfn-test-table",
			doDelete:   true,
		},
		{
			name:      "pay_per_request",
			logicalID: "OnDemandTable",
			props: map[string]any{
				"TableName":   "cfn-ondemand-table",
				"BillingMode": "PAY_PER_REQUEST",
				"AttributeDefinitions": []any{
					map[string]any{"AttributeName": "pk", "AttributeType": "S"},
				},
				"KeySchema": []any{
					map[string]any{"AttributeName": "pk", "KeyType": "HASH"},
				},
			},
			wantPhysID: "cfn-ondemand-table",
		},
		{
			name:      "default_name",
			logicalID: "MyTable",
			props: map[string]any{
				"AttributeDefinitions": []any{
					map[string]any{"AttributeName": "id", "AttributeType": "N"},
				},
				"KeySchema": []any{
					map[string]any{"AttributeName": "id", "KeyType": "HASH"},
				},
			},
			wantPhysID: "MyTable",
		},
		{
			name:      "binary_attribute_type",
			logicalID: "BinaryTable",
			props: map[string]any{
				"TableName": "cfn-binary-table",
				"AttributeDefinitions": []any{
					map[string]any{"AttributeName": "id", "AttributeType": "B"},
				},
				"KeySchema": []any{
					map[string]any{"AttributeName": "id", "KeyType": "HASH"},
				},
			},
			wantPhysID: "cfn-binary-table",
		},
		{
			name:      "range_key",
			logicalID: "RangeTable",
			props: map[string]any{
				"TableName": "cfn-range-table",
				"AttributeDefinitions": []any{
					map[string]any{"AttributeName": "pk", "AttributeType": "S"},
					map[string]any{"AttributeName": "sk", "AttributeType": "S"},
				},
				"KeySchema": []any{
					map[string]any{"AttributeName": "pk", "KeyType": "HASH"},
					map[string]any{"AttributeName": "sk", "KeyType": "RANGE"},
				},
			},
			wantPhysID: "cfn-range-table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::DynamoDB::Table", tt.props, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhysID, physID)

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::DynamoDB::Table", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

// ---- ResourceCreator: SQS Queue ---------------------------------------------

func TestResourceCreator_SQSQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		logicalID    string
		props        map[string]any
		wantContains string
		wantNotEmpty bool
		doDelete     bool
	}{
		{
			name:         "explicit_name",
			logicalID:    "MyQueue",
			props:        map[string]any{"QueueName": "cfn-test-queue"},
			wantNotEmpty: true,
			doDelete:     true,
		},
		{
			name:         "default_name",
			logicalID:    "MyDefaultQueue",
			props:        map[string]any{"VisibilityTimeout": "30"},
			wantNotEmpty: true,
		},
		{
			name:         "fifo",
			logicalID:    "MyFIFOQueue",
			props:        map[string]any{"QueueName": "cfn-fifo-queue", "FifoQueue": true},
			wantContains: ".fifo",
		},
		{
			name:      "with_attributes",
			logicalID: "AttrQueue",
			props: map[string]any{
				"QueueName":              "attr-queue",
				"VisibilityTimeout":      "45",
				"MessageRetentionPeriod": "86400",
			},
			wantNotEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::SQS::Queue", tt.props, nil, nil)
			require.NoError(t, err)

			if tt.wantNotEmpty {
				assert.NotEmpty(t, physID)
			}

			if tt.wantContains != "" {
				assert.Contains(t, physID, tt.wantContains)
			}

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::SQS::Queue", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

// ---- ResourceCreator: SNS Topic ---------------------------------------------

func TestResourceCreator_SNSTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		logicalID    string
		props        map[string]any
		wantContains string
		doDelete     bool
	}{
		{
			name:         "explicit_name",
			logicalID:    "MyTopic",
			props:        map[string]any{"TopicName": "cfn-test-topic"},
			wantContains: "cfn-test-topic",
			doDelete:     true,
		},
		{
			name:         "default_name",
			logicalID:    "MyDefaultTopic",
			props:        map[string]any{},
			wantContains: "MyDefaultTopic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::SNS::Topic", tt.props, nil, nil)
			require.NoError(t, err)
			assert.Contains(t, physID, tt.wantContains)

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::SNS::Topic", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

// ---- ResourceCreator: SSM Parameter -----------------------------------------

func TestResourceCreator_SSMParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		logicalID  string
		props      map[string]any
		wantPhysID string
		doDelete   bool
	}{
		{
			name:      "explicit_name",
			logicalID: "MyParam",
			props: map[string]any{
				"Name":  "/cfn/test-param",
				"Type":  "String",
				"Value": "hello",
			},
			wantPhysID: "/cfn/test-param",
			doDelete:   true,
		},
		{
			name:       "default_name",
			logicalID:  "MySSMParam",
			props:      map[string]any{"Value": "val"},
			wantPhysID: "/MySSMParam",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::SSM::Parameter", tt.props, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhysID, physID)

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::SSM::Parameter", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

// ---- ResourceCreator: KMS Key -----------------------------------------------

func TestResourceCreator_KMSKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props     map[string]any
		name      string
		logicalID string
		doDelete  bool
	}{
		{
			name:      "with_description",
			logicalID: "MyKey",
			props:     map[string]any{"Description": "cfn test key"},
			doDelete:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::KMS::Key", tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::KMS::Key", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

// ---- ResourceCreator: SecretsManager Secret ---------------------------------

func TestResourceCreator_SecretsManagerSecret(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		logicalID    string
		props        map[string]any
		wantContains string
		doDelete     bool
	}{
		{
			name:      "explicit_name",
			logicalID: "MySecret",
			props: map[string]any{
				"Name":         "cfn-test-secret",
				"SecretString": `{"key":"value"}`,
			},
			wantContains: "cfn-test-secret",
			doDelete:     true,
		},
		{
			name:         "default_name",
			logicalID:    "MyDefaultSecret",
			props:        map[string]any{"SecretString": "secret"},
			wantContains: "MyDefaultSecret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::SecretsManager::Secret", tt.props, nil, nil)
			require.NoError(t, err)
			assert.Contains(t, physID, tt.wantContains)

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::SecretsManager::Secret", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

// ---- ResourceCreator: unknown type ------------------------------------------

func TestResourceCreator_UnknownType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
		physID       string
		wantPhysID   string
		isDelete     bool
	}{
		{
			name:         "create_returns_stub",
			logicalID:    "MyRole",
			resourceType: "AWS::IAM::Role",
			props:        map[string]any{},
			wantPhysID:   "MyRole-stub",
		},
		{
			name:         "delete_no_error",
			resourceType: "AWS::IAM::Role",
			physID:       "some-role",
			props:        map[string]any{},
			isDelete:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			if tt.isDelete {
				err := rc.Delete(t.Context(), tt.resourceType, tt.physID, tt.props)
				require.NoError(t, err)

				return
			}

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhysID, physID)
		})
	}
}

// ---- Backend: CreateStack with real resource provisioning -------------------

func TestBackend_CreateStack_RealResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stackName  string
		template   string
		wantStatus string
	}{
		{
			name:      "s3_bucket",
			stackName: "real-stack",
			template: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyBucket": {
"Type": "AWS::S3::Bucket",
"Properties": {"BucketName": "real-cfn-bucket"}
}
}
}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:      "ssm_and_sns",
			stackName: "ssm-sns-stack",
			template: `{
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
}`,
			wantStatus: "CREATE_COMPLETE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			creator := cloudformation.NewResourceCreator(backends)
			backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)

			stack, err := backend.CreateStack(t.Context(), tt.stackName, tt.template, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, stack.StackStatus)

			if tt.stackName == "real-stack" {
				err = backend.DeleteStack(t.Context(), tt.stackName)
				require.NoError(t, err)
			}
		})
	}
}

// ---- Backend: UpdateStack with new resource ---------------------------------

func TestBackend_UpdateStack_WithNewResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stackName  string
		tmpl1      string
		tmpl2      string
		wantStatus string
	}{
		{
			name:      "add_sqs_queue",
			stackName: "upd-real-stack",
			tmpl1: `{
"AWSTemplateFormatVersion": "2010-09-09",
"Resources": {
"MyBucket": {
"Type": "AWS::S3::Bucket",
"Properties": {"BucketName": "upd-cfn-bucket"}
}
}
}`,
			tmpl2: `{
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
}`,
			wantStatus: "UPDATE_COMPLETE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			creator := cloudformation.NewResourceCreator(backends)
			backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)

			_, err := backend.CreateStack(t.Context(), tt.stackName, tt.tmpl1, nil, nil)
			require.NoError(t, err)

			updated, err := backend.UpdateStack(t.Context(), tt.stackName, tt.tmpl2, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, updated.StackStatus)
		})
	}
}

// ---- ResourceCreator: Lambda nil-backend stub and extended types helpers ----

// newExtendedServiceBackends creates a ServiceBackends with all backends including extended types.
func newExtendedServiceBackends() *cloudformation.ServiceBackends {
	b := newServiceBackends()
	b.EventBridge = ebbackend.NewHandler(
		ebbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"))
	b.StepFunctions = sfnbackend.NewHandler(
		sfnbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"))
	b.CloudWatchLogs = cwlogsbackend.NewHandler(
		cwlogsbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"))
	b.APIGateway = apigwbackend.NewHandler(apigwbackend.NewInMemoryBackend())

	return b
}

// ---- ResourceCreator: Lambda nil-backend stub -------------------------------

func TestResourceCreator_Lambda_NilBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props      map[string]any
		name       string
		logicalID  string
		physID     string
		wantPhysID string
		isDelete   bool
	}{
		{
			name:      "create_returns_stub",
			logicalID: "MyFunction",
			props: map[string]any{
				"FunctionName": "my-fn",
				"Runtime":      "python3.12",
				"Handler":      "index.handler",
			},
			wantPhysID: "MyFunction-stub",
		},
		{
			name:     "delete_nil_path",
			physID:   "my-fn",
			isDelete: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// newServiceBackends() leaves Lambda=nil, so Lambda functions use stub path.
			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			if tt.isDelete {
				err := rc.Delete(t.Context(), "AWS::Lambda::Function", tt.physID, nil)
				require.NoError(t, err)

				return
			}

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::Lambda::Function", tt.props, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhysID, physID)
		})
	}
}

// ---- ResourceCreator: extended types with real backends ---------------------

func TestResourceCreator_ExtendedTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
		physID       string
		wantPhysID   string
		wantContains string
		isDelete     bool
		wantNotEmpty bool
	}{
		{
			name:         "event_bridge_rule",
			logicalID:    "MyRule",
			resourceType: "AWS::Events::Rule",
			props: map[string]any{
				"Name":         "my-cfn-rule",
				"EventPattern": `{"source":["aws.s3"]}`,
				"State":        "ENABLED",
			},
			wantContains: "my-cfn-rule",
		},
		{
			name:         "step_functions_state_machine",
			logicalID:    "MyStateMachine",
			resourceType: "AWS::StepFunctions::StateMachine",
			props: map[string]any{
				"StateMachineName": "cfn-sm",
				"DefinitionString": `{"Comment":"test","StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}`,
				"RoleArn":          "arn:aws:iam::000000000000:role/sfn-role",
			},
			wantContains: "cfn-sm",
		},
		{
			name:         "cloudwatch_log_group",
			logicalID:    "MyLogGroup",
			resourceType: "AWS::Logs::LogGroup",
			props:        map[string]any{"LogGroupName": "/cfn/my-log-group"},
			wantPhysID:   "/cfn/my-log-group",
		},
		{
			name:         "api_gateway_rest_api",
			logicalID:    "MyAPI",
			resourceType: "AWS::ApiGateway::RestApi",
			props:        map[string]any{"Name": "cfn-rest-api", "Description": "created by cfn"},
			wantNotEmpty: true,
		},
		{
			name:         "default_stub",
			logicalID:    "MyWhatever",
			resourceType: "AWS::Whatever::Thing",
			props:        nil,
			wantPhysID:   "MyWhatever-stub",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)

			if tt.wantPhysID != "" {
				assert.Equal(t, tt.wantPhysID, physID)
			}

			if tt.wantContains != "" {
				assert.Contains(t, physID, tt.wantContains)
			}

			if tt.wantNotEmpty {
				assert.NotEmpty(t, physID)
			}

			// delete test (skip for default_stub since it uses a generic type)
			if tt.resourceType != "AWS::Whatever::Thing" {
				err = rc.Delete(t.Context(), tt.resourceType, physID, tt.props)
				require.NoError(t, err)
			} else {
				err = rc.Delete(t.Context(), tt.resourceType, "whatever-id", nil)
				require.NoError(t, err)
			}
		})
	}
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
		ddb: ddbbackend.NewHandler(ddbbackend.NewInMemoryDB()),
		s3h: s3backend.NewHandler(s3backend.NewInMemoryBackend(nil)),
		sqs: sqsbackend.NewHandler(sqsbackend.NewInMemoryBackend()),
		sns: snsbackend.NewHandler(snsbackend.NewInMemoryBackend()),
		ssm: ssmbackend.NewHandler(ssmbackend.NewInMemoryBackend()),
		kms: kmsbackend.NewHandler(kmsbackend.NewInMemoryBackend()),
		sm:  smbackend.NewHandler(smbackend.NewInMemoryBackend()),
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

// mockConfigProvider implements only config.Provider (no BackendsProvider).
type mockConfigProvider struct{}

func (m *mockConfigProvider) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: "222222222222", Region: "ap-southeast-1"}
}

func TestProvider_Init_WithConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  any
		wantSvc string
	}{
		{
			name:    "with_backends_provider",
			config:  newMockBackendsProvider(),
			wantSvc: "CloudFormation",
		},
		{
			name:    "with_config_provider",
			config:  &mockConfigProvider{},
			wantSvc: "CloudFormation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &cloudformation.Provider{}
			appCtx := &service.AppContext{
				Logger: slog.Default(),
				Config: tt.config,
			}
			svc, err := p.Init(appCtx)
			require.NoError(t, err)
			require.NotNil(t, svc)
			assert.Equal(t, tt.wantSvc, svc.Name())
		})
	}
}
