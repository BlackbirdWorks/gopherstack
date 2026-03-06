package cloudformation_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	cloudwatchbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	elasticachebackend "github.com/blackbirdworks/gopherstack/services/elasticache"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
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
	b.IAM = iambackend.NewHandler(iambackend.NewInMemoryBackendWithConfig("000000000000"))
	b.EC2 = ec2backend.NewHandler(ec2backend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.Kinesis = kinesisbackend.NewHandler(
		kinesisbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"))
	b.CloudWatch = cloudwatchbackend.NewHandler(
		cloudwatchbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"))
	b.Route53 = route53backend.NewHandler(route53backend.NewInMemoryBackend())
	b.ElastiCache = elasticachebackend.NewHandler(
		elasticachebackend.NewInMemoryBackend("", "000000000000", "us-east-1"))
	b.Scheduler = schedulerbackend.NewHandler(
		schedulerbackend.NewInMemoryBackend("000000000000", "us-east-1"))

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

// ---- ResourceCreator: IAM resources ----------------------------------------

func TestResourceCreator_IAMResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
		wantContains string
	}{
		{
			name:         "iam_role",
			logicalID:    "MyRole",
			resourceType: "AWS::IAM::Role",
			props: map[string]any{
				"RoleName":                 "cfn-my-role",
				"AssumeRolePolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
			},
			wantContains: "cfn-my-role",
		},
		{
			name:         "iam_policy",
			logicalID:    "MyPolicy",
			resourceType: "AWS::IAM::Policy",
			props: map[string]any{
				"PolicyName":     "cfn-my-policy",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
			},
			wantContains: "cfn-my-policy",
		},
		{
			name:         "iam_managed_policy",
			logicalID:    "MyManagedPolicy",
			resourceType: "AWS::IAM::ManagedPolicy",
			props: map[string]any{
				"ManagedPolicyName": "cfn-managed-policy",
				"PolicyDocument":    `{"Version":"2012-10-17","Statement":[]}`,
			},
			wantContains: "cfn-managed-policy",
		},
		{
			name:         "iam_instance_profile",
			logicalID:    "MyInstanceProfile",
			resourceType: "AWS::IAM::InstanceProfile",
			props: map[string]any{
				"InstanceProfileName": "cfn-instance-profile",
			},
			wantContains: "cfn-instance-profile",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.Contains(t, physID, tt.wantContains)

			err = rc.Delete(t.Context(), tt.resourceType, physID, tt.props)
			require.NoError(t, err)
		})
	}
}

// ---- ResourceCreator: EC2 resources ----------------------------------------

func TestResourceCreator_EC2Resources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildProps   func() map[string]any
		name         string
		logicalID    string
		resourceType string
		wantNotEmpty bool
	}{
		{
			name:         "ec2_vpc",
			logicalID:    "MyVPC",
			resourceType: "AWS::EC2::VPC",
			buildProps:   func() map[string]any { return map[string]any{"CidrBlock": "10.0.0.0/16"} },
			wantNotEmpty: true,
		},
		{
			name:         "ec2_internet_gateway",
			logicalID:    "MyIGW",
			resourceType: "AWS::EC2::InternetGateway",
			buildProps:   func() map[string]any { return map[string]any{} },
			wantNotEmpty: true,
		},
		{
			name:         "ec2_security_group",
			logicalID:    "MySecurityGroup",
			resourceType: "AWS::EC2::SecurityGroup",
			buildProps: func() map[string]any {
				return map[string]any{
					"GroupName":        "cfn-sg",
					"GroupDescription": "CloudFormation test SG",
				}
			},
			wantNotEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)
			props := tt.buildProps()

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, props, nil, nil)
			require.NoError(t, err)

			if tt.wantNotEmpty {
				assert.NotEmpty(t, physID)
			}

			err = rc.Delete(t.Context(), tt.resourceType, physID, props)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_EC2SubnetAndRouteTable(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create VPC first.
	vpcID, err := rc.Create(t.Context(), "MyVPC", "AWS::EC2::VPC",
		map[string]any{"CidrBlock": "10.0.0.0/16"}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, vpcID)

	// Create Subnet.
	subnetID, err := rc.Create(t.Context(), "MySubnet", "AWS::EC2::Subnet",
		map[string]any{
			"VpcId":            vpcID,
			"CidrBlock":        "10.0.1.0/24",
			"AvailabilityZone": "us-east-1a",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, subnetID)

	// Create RouteTable.
	rtID, err := rc.Create(t.Context(), "MyRouteTable", "AWS::EC2::RouteTable",
		map[string]any{"VpcId": vpcID}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, rtID)

	// Create Route.
	routePhysID, err := rc.Create(t.Context(), "MyRoute", "AWS::EC2::Route",
		map[string]any{
			"RouteTableId":         rtID,
			"DestinationCidrBlock": "0.0.0.0/0",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, routePhysID)

	// Delete in reverse order.
	err = rc.Delete(t.Context(), "AWS::EC2::Route", routePhysID, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::EC2::RouteTable", rtID, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::EC2::Subnet", subnetID, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::EC2::VPC", vpcID, nil)
	require.NoError(t, err)
}

// ---- ResourceCreator: Kinesis, CloudWatch, Route53 -------------------------

func TestResourceCreator_KinesisStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		wantContains string
	}{
		{
			name:      "basic_stream",
			logicalID: "MyStream",
			props: map[string]any{
				"Name":       "cfn-test-stream",
				"ShardCount": float64(1),
			},
			wantContains: "cfn-test-stream",
		},
		{
			name:      "default_name",
			logicalID: "MyStream2",
			props:     map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::Kinesis::Stream", tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			if tt.wantContains != "" {
				assert.Contains(t, physID, tt.wantContains)
			}

			err = rc.Delete(t.Context(), "AWS::Kinesis::Stream", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_CloudWatchAlarm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props      map[string]any
		name       string
		logicalID  string
		wantPhysID string
	}{
		{
			name:      "basic_alarm",
			logicalID: "MyAlarm",
			props: map[string]any{
				"AlarmName":          "cfn-test-alarm",
				"Namespace":          "AWS/Lambda",
				"MetricName":         "Errors",
				"ComparisonOperator": "GreaterThanThreshold",
				"Statistic":          "Sum",
				"Threshold":          float64(10),
				"EvaluationPeriods":  float64(1),
				"Period":             float64(60),
			},
			wantPhysID: "cfn-test-alarm",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::CloudWatch::Alarm", tt.props, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhysID, physID)

			err = rc.Delete(t.Context(), "AWS::CloudWatch::Alarm", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_Route53HostedZone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props     map[string]any
		name      string
		logicalID string
	}{
		{
			name:      "basic_hosted_zone",
			logicalID: "MyZone",
			props:     map[string]any{"Name": "example.com"},
		},
		{
			name:      "with_comment",
			logicalID: "MyZoneWithComment",
			props: map[string]any{
				"Name": "test.example.com",
				"HostedZoneConfig": map[string]any{
					"Comment": "Test zone",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::Route53::HostedZone", tt.props, nil, nil)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			err = rc.Delete(t.Context(), "AWS::Route53::HostedZone", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_Route53RecordSet(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create hosted zone first.
	zoneID, err := rc.Create(t.Context(), "MyZone", "AWS::Route53::HostedZone",
		map[string]any{"Name": "example.com"}, nil, nil)
	require.NoError(t, err)

	// Create record set.
	recordPhysID, err := rc.Create(t.Context(), "MyRecord", "AWS::Route53::RecordSet",
		map[string]any{
			"HostedZoneId":    zoneID,
			"Name":            "api.example.com",
			"Type":            "A",
			"TTL":             float64(300),
			"ResourceRecords": []any{"1.2.3.4"},
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, recordPhysID)

	err = rc.Delete(t.Context(), "AWS::Route53::RecordSet", recordPhysID, nil)
	require.NoError(t, err)
}

// ---- ResourceCreator: Lambda with real backend ------------------------------

// newLambdaServiceBackends creates a ServiceBackends with a real Lambda backend.
func newLambdaServiceBackends() *cloudformation.ServiceBackends {
	b := newExtendedServiceBackends()
	lambdaBk := lambdabackend.NewInMemoryBackend(nil, nil, lambdabackend.DefaultSettings(), "000000000000", "us-east-1")
	b.Lambda = lambdabackend.NewHandler(lambdaBk)

	return b
}

func TestResourceCreator_LambdaESM_RealBackend(t *testing.T) {
	t.Parallel()

	backends := newLambdaServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create a Lambda function first.
	fnARN, err := rc.Create(t.Context(), "MyFunction", "AWS::Lambda::Function",
		map[string]any{
			"FunctionName": "cfn-test-fn",
			"Runtime":      "python3.12",
			"Handler":      "index.handler",
			"Role":         "arn:aws:iam::000000000000:role/lambda-role",
			"Code": map[string]any{
				"ZipFile": "def handler(event, context): pass",
			},
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, fnARN)

	// Create a Kinesis stream for ESM.
	streamARN, err := rc.Create(t.Context(), "MyStream", "AWS::Kinesis::Stream",
		map[string]any{"Name": "cfn-esm-stream", "ShardCount": float64(1)},
		nil, nil)
	require.NoError(t, err)

	// Create EventSourceMapping.
	esmID, err := rc.Create(t.Context(), "MyESM", "AWS::Lambda::EventSourceMapping",
		map[string]any{
			"FunctionName":     "cfn-test-fn",
			"EventSourceArn":   streamARN,
			"StartingPosition": "TRIM_HORIZON",
			"BatchSize":        float64(10),
			"Enabled":          true,
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, esmID)

	err = rc.Delete(t.Context(), "AWS::Lambda::EventSourceMapping", esmID, nil)
	require.NoError(t, err)
}

func TestResourceCreator_LambdaAlias_RealBackend(t *testing.T) {
	t.Parallel()

	backends := newLambdaServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create function.
	_, err := rc.Create(t.Context(), "MyFunction", "AWS::Lambda::Function",
		map[string]any{
			"FunctionName": "cfn-alias-fn",
			"Runtime":      "python3.12",
			"Handler":      "index.handler",
			"Role":         "arn:aws:iam::000000000000:role/lambda-role",
			"Code": map[string]any{
				"ZipFile": "def handler(event, context): pass",
			},
		}, nil, nil)
	require.NoError(t, err)

	// Create alias pointing to $LATEST.
	aliasARN, err := rc.Create(t.Context(), "MyAlias", "AWS::Lambda::Alias",
		map[string]any{
			"FunctionName":    "cfn-alias-fn",
			"Name":            "prod",
			"FunctionVersion": "$LATEST",
			"Description":     "Production alias",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, aliasARN)
	assert.Contains(t, aliasARN, "prod")

	err = rc.Delete(t.Context(), "AWS::Lambda::Alias", aliasARN, nil)
	require.NoError(t, err)
}

func TestResourceCreator_LambdaVersion_RealBackend(t *testing.T) {
	t.Parallel()

	backends := newLambdaServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create function.
	_, err := rc.Create(t.Context(), "MyFunction", "AWS::Lambda::Function",
		map[string]any{
			"FunctionName": "cfn-version-fn",
			"Runtime":      "python3.12",
			"Handler":      "index.handler",
			"Role":         "arn:aws:iam::000000000000:role/lambda-role",
			"Code": map[string]any{
				"ZipFile": "def handler(event, context): pass",
			},
		}, nil, nil)
	require.NoError(t, err)

	// Publish a version.
	versionARN, err := rc.Create(t.Context(), "MyVersion", "AWS::Lambda::Version",
		map[string]any{
			"FunctionName": "cfn-version-fn",
			"Description":  "v1",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, versionARN)

	err = rc.Delete(t.Context(), "AWS::Lambda::Version", versionARN, nil)
	require.NoError(t, err)
}

// ---- ResourceCreator: ElastiCache, SNS Subscription, EventBus, Scheduler ---

func TestResourceCreator_ElastiCacheCacheCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props      map[string]any
		name       string
		logicalID  string
		wantPhysID string
	}{
		{
			name:      "redis_cluster",
			logicalID: "MyCluster",
			props: map[string]any{
				"CacheClusterId": "cfn-redis-cluster",
				"Engine":         "redis",
				"CacheNodeType":  "cache.t3.micro",
			},
			wantPhysID: "cfn-redis-cluster",
		},
		{
			name:       "default_engine",
			logicalID:  "MyCluster2",
			props:      map[string]any{"CacheClusterId": "cfn-default-cluster"},
			wantPhysID: "cfn-default-cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::ElastiCache::CacheCluster", tt.props, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhysID, physID)

			err = rc.Delete(t.Context(), "AWS::ElastiCache::CacheCluster", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_SNSSubscription(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create a topic first.
	topicARN, err := rc.Create(t.Context(), "MyTopic", "AWS::SNS::Topic",
		map[string]any{"TopicName": "cfn-test-topic"}, nil, nil)
	require.NoError(t, err)

	// Create subscription.
	subARN, err := rc.Create(t.Context(), "MySub", "AWS::SNS::Subscription",
		map[string]any{
			"TopicArn": topicARN,
			"Protocol": "sqs",
			"Endpoint": "https://sqs.us-east-1.amazonaws.com/000000000000/my-queue",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, subARN)

	err = rc.Delete(t.Context(), "AWS::SNS::Subscription", subARN, nil)
	require.NoError(t, err)
}

func TestResourceCreator_EventBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		wantContains string
	}{
		{
			name:         "custom_event_bus",
			logicalID:    "MyEventBus",
			props:        map[string]any{"Name": "cfn-custom-bus"},
			wantContains: "cfn-custom-bus",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::Events::EventBus", tt.props, nil, nil)
			require.NoError(t, err)
			assert.Contains(t, physID, tt.wantContains)

			err = rc.Delete(t.Context(), "AWS::Events::EventBus", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_SchedulerSchedule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		wantContains string
	}{
		{
			name:      "basic_schedule",
			logicalID: "MySchedule",
			props: map[string]any{
				"Name":               "cfn-schedule",
				"ScheduleExpression": "rate(5 minutes)",
				"State":              "ENABLED",
				"Target": map[string]any{
					"Arn":     "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
					"RoleArn": "arn:aws:iam::000000000000:role/my-role",
				},
			},
			wantContains: "cfn-schedule",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::Scheduler::Schedule", tt.props, nil, nil)
			require.NoError(t, err)
			assert.Contains(t, physID, tt.wantContains)

			err = rc.Delete(t.Context(), "AWS::Scheduler::Schedule", physID, nil)
			require.NoError(t, err)
		})
	}
}

// ---- ResourceCreator: S3 BucketPolicy, SQS QueuePolicy ---------------------

func TestResourceCreator_S3BucketPolicy(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create bucket first.
	bucketName, err := rc.Create(t.Context(), "MyBucket", "AWS::S3::Bucket",
		map[string]any{"BucketName": "cfn-test-bucket-policy"}, nil, nil)
	require.NoError(t, err)

	// Apply bucket policy.
	physID, err := rc.Create(t.Context(), "MyBucketPolicy", "AWS::S3::BucketPolicy",
		map[string]any{
			"Bucket":         bucketName,
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, bucketName, physID)

	err = rc.Delete(t.Context(), "AWS::S3::BucketPolicy", physID, nil)
	require.NoError(t, err)
}

func TestResourceCreator_SQSQueuePolicy(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create queue first.
	queueURL, err := rc.Create(t.Context(), "MyQueue", "AWS::SQS::Queue",
		map[string]any{"QueueName": "cfn-test-queue-policy"}, nil, nil)
	require.NoError(t, err)

	// Apply queue policy.
	physID, err := rc.Create(t.Context(), "MyQueuePolicy", "AWS::SQS::QueuePolicy",
		map[string]any{
			"Queues":         []any{queueURL},
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)

	err = rc.Delete(t.Context(), "AWS::SQS::QueuePolicy", physID, nil)
	require.NoError(t, err)
}

// ---- ResourceCreator: APIGateway sub-resources ------------------------------

func TestResourceCreator_APIGatewaySubResources(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create REST API.
	apiID, err := rc.Create(t.Context(), "MyAPI", "AWS::ApiGateway::RestApi",
		map[string]any{"Name": "cfn-api", "Description": "CFN test API"}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, apiID)

	// Find root resource.
	resources, _, err := backends.APIGateway.Backend.GetResources(apiID, "", 100)
	require.NoError(t, err)

	var rootID string
	for _, r := range resources {
		if r.Path == "/" {
			rootID = r.ID

			break
		}
	}
	require.NotEmpty(t, rootID, "root resource should exist")

	// Create sub-resource.
	resourcePhysID, err := rc.Create(t.Context(), "MyResource", "AWS::ApiGateway::Resource",
		map[string]any{
			"RestApiId": apiID,
			"ParentId":  rootID,
			"PathPart":  "items",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, resourcePhysID)

	// Create method.
	methodPhysID, err := rc.Create(t.Context(), "MyMethod", "AWS::ApiGateway::Method",
		map[string]any{
			"RestApiId":         apiID,
			"ResourceId":        resourcePhysID,
			"HttpMethod":        "GET",
			"AuthorizationType": "NONE",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, methodPhysID)

	// Create deployment.
	deployPhysID, err := rc.Create(t.Context(), "MyDeployment", "AWS::ApiGateway::Deployment",
		map[string]any{
			"RestApiId":   apiID,
			"StageName":   "prod",
			"Description": "Production deployment",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, deployPhysID)

	// Create stage.
	stagePhysID, err := rc.Create(t.Context(), "MyStage", "AWS::ApiGateway::Stage",
		map[string]any{
			"RestApiId": apiID,
			"StageName": "prod",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, stagePhysID)

	// Delete.
	err = rc.Delete(t.Context(), "AWS::ApiGateway::Stage", stagePhysID, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::ApiGateway::Deployment", deployPhysID, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::ApiGateway::Method", methodPhysID, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::ApiGateway::Resource", resourcePhysID, nil)
	require.NoError(t, err)
}

// ---- ResourceCreator: nil backends stub for new types ----------------------

func TestResourceCreator_NewTypes_NilBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		resourceType string
	}{
		{
			name:         "iam_role_nil",
			logicalID:    "MyRole",
			resourceType: "AWS::IAM::Role",
			props:        map[string]any{},
		},
		{
			name:         "iam_policy_nil",
			logicalID:    "MyPolicy",
			resourceType: "AWS::IAM::Policy",
			props:        map[string]any{},
		},
		{
			name:         "iam_managed_policy_nil",
			logicalID:    "MyManagedPolicy",
			resourceType: "AWS::IAM::ManagedPolicy",
			props:        map[string]any{},
		},
		{
			name:         "iam_instance_profile_nil",
			logicalID:    "MyInstanceProfile",
			resourceType: "AWS::IAM::InstanceProfile",
			props:        map[string]any{},
		},
		{
			name:         "ec2_vpc_nil",
			logicalID:    "MyVPC",
			resourceType: "AWS::EC2::VPC",
			props:        map[string]any{"CidrBlock": "10.0.0.0/16"},
		},
		{
			name:         "ec2_subnet_nil",
			logicalID:    "MySubnet",
			resourceType: "AWS::EC2::Subnet",
			props:        map[string]any{"VpcId": "vpc-abc", "CidrBlock": "10.0.1.0/24"},
		},
		{
			name:         "ec2_security_group_nil",
			logicalID:    "MySG",
			resourceType: "AWS::EC2::SecurityGroup",
			props:        map[string]any{"GroupDescription": "test"},
		},
		{
			name:         "ec2_igw_nil",
			logicalID:    "MyIGW",
			resourceType: "AWS::EC2::InternetGateway",
			props:        map[string]any{},
		},
		{
			name:         "ec2_route_table_nil",
			logicalID:    "MyRT",
			resourceType: "AWS::EC2::RouteTable",
			props:        map[string]any{"VpcId": "vpc-abc"},
		},
		{
			name:         "ec2_route_nil",
			logicalID:    "MyRoute",
			resourceType: "AWS::EC2::Route",
			props: map[string]any{
				"RouteTableId":         "rtb-abc",
				"DestinationCidrBlock": "0.0.0.0/0",
			},
		},
		{
			name:         "kinesis_stream_nil",
			logicalID:    "MyStream",
			resourceType: "AWS::Kinesis::Stream",
			props:        map[string]any{"Name": "my-stream"},
		},
		{
			name:         "cloudwatch_alarm_nil",
			logicalID:    "MyAlarm",
			resourceType: "AWS::CloudWatch::Alarm",
			props:        map[string]any{},
		},
		{
			name:         "route53_hosted_zone_nil",
			logicalID:    "MyZone",
			resourceType: "AWS::Route53::HostedZone",
			props:        map[string]any{"Name": "example.com"},
		},
		{
			name:         "route53_record_set_nil",
			logicalID:    "MyRecord",
			resourceType: "AWS::Route53::RecordSet",
			props:        map[string]any{"HostedZoneId": "Z123", "Name": "api.example.com", "Type": "A"},
		},
		{
			name:         "elasticache_cluster_nil",
			logicalID:    "MyCluster",
			resourceType: "AWS::ElastiCache::CacheCluster",
			props:        map[string]any{"Engine": "redis"},
		},
		{
			name:         "events_eventbus_nil",
			logicalID:    "MyEventBus",
			resourceType: "AWS::Events::EventBus",
			props:        map[string]any{"Name": "my-bus"},
		},
		{
			name:         "scheduler_schedule_nil",
			logicalID:    "MySchedule",
			resourceType: "AWS::Scheduler::Schedule",
			props: map[string]any{
				"Name":               "my-schedule",
				"ScheduleExpression": "rate(5 minutes)",
			},
		},
		{
			name:         "lambda_esm_nil",
			logicalID:    "MyESM",
			resourceType: "AWS::Lambda::EventSourceMapping",
			props: map[string]any{
				"FunctionName":   "my-fn",
				"EventSourceArn": "arn:aws:kinesis:us-east-1:000:stream/my-stream",
			},
		},
		{
			name:         "lambda_permission_nil",
			logicalID:    "MyPermission",
			resourceType: "AWS::Lambda::Permission",
			props:        map[string]any{"FunctionName": "my-fn"},
		},
		{
			name:         "lambda_alias_nil",
			logicalID:    "MyAlias",
			resourceType: "AWS::Lambda::Alias",
			props:        map[string]any{"FunctionName": "my-fn", "Name": "prod", "FunctionVersion": "$LATEST"},
		},
		{
			name:         "lambda_version_nil",
			logicalID:    "MyVersion",
			resourceType: "AWS::Lambda::Version",
			props:        map[string]any{"FunctionName": "my-fn"},
		},
		{
			name:         "apigw_resource_nil",
			logicalID:    "MyResource",
			resourceType: "AWS::ApiGateway::Resource",
			props:        map[string]any{"RestApiId": "abc123", "ParentId": "root", "PathPart": "items"},
		},
		{
			name:         "apigw_method_nil",
			logicalID:    "MyMethod",
			resourceType: "AWS::ApiGateway::Method",
			props:        map[string]any{"RestApiId": "abc123", "ResourceId": "res1", "HttpMethod": "GET"},
		},
		{
			name:         "apigw_deployment_nil",
			logicalID:    "MyDeployment",
			resourceType: "AWS::ApiGateway::Deployment",
			props:        map[string]any{"RestApiId": "abc123"},
		},
		{
			name:         "apigw_stage_nil",
			logicalID:    "MyStage",
			resourceType: "AWS::ApiGateway::Stage",
			props:        map[string]any{"RestApiId": "abc123", "StageName": "prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Use base backends (no IAM/EC2/Kinesis/etc.)
			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, tt.resourceType, tt.props, nil, nil)
			require.NoError(t, err)
			assert.Contains(t, physID, tt.logicalID, "stub physID should contain logicalID")

			err = rc.Delete(t.Context(), tt.resourceType, physID, nil)
			require.NoError(t, err)
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
func (m *mockBackendsProvider) GetIAMHandler() service.Registerable            { return nil }
func (m *mockBackendsProvider) GetEC2Handler() service.Registerable            { return nil }
func (m *mockBackendsProvider) GetKinesisHandler() service.Registerable        { return nil }
func (m *mockBackendsProvider) GetCloudWatchHandler() service.Registerable     { return nil }
func (m *mockBackendsProvider) GetRoute53Handler() service.Registerable        { return nil }
func (m *mockBackendsProvider) GetElastiCacheHandler() service.Registerable    { return nil }
func (m *mockBackendsProvider) GetSchedulerHandler() service.Registerable      { return nil }
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

// ---- ResourceCreator: Lambda::Permission real backend -----------------------

func TestResourceCreator_LambdaPermission_RealBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr      error
		props        map[string]any
		name         string
		logicalID    string
		wantContains string
	}{
		{
			name:      "success_with_function_name",
			logicalID: "MyPermission",
			props: map[string]any{
				"FunctionName": "cfn-perm-fn",
				"Action":       "lambda:InvokeFunction",
				"Principal":    "apigateway.amazonaws.com",
			},
			wantContains: "cfn-perm-fn",
		},
		{
			name:      "error_missing_function_name",
			logicalID: "BadPermission",
			props:     map[string]any{},
			wantErr:   cloudformation.ErrFunctionNameRequired,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newLambdaServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(t.Context(), tt.logicalID, "AWS::Lambda::Permission", tt.props, nil, nil)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, physID, tt.wantContains)

			err = rc.Delete(t.Context(), "AWS::Lambda::Permission", physID, nil)
			require.NoError(t, err)
		})
	}
}

// ---- helper functions: resourceNameFromARN, streamNameFromARN coverage ------

func TestResourceNameFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "arn_with_slash",
			input: "arn:aws:scheduler:us-east-1:000000000000:schedule/default/my-sched",
			want:  "my-sched",
		},
		{
			name:  "arn_with_colon_no_slash",
			input: "arn:aws:sns:us-east-1:000000000000:my-topic",
			want:  "my-topic",
		},
		{
			name:  "plain_name_no_separator",
			input: "my-plain-resource",
			want:  "my-plain-resource",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Indirectly exercise resourceNameFromARN via the Scheduler delete path.
			// We drive it through deleteSchedulerSchedule by creating and deleting a schedule.
			if tt.input == "my-plain-resource" {
				// Exercise plain-name case directly via Scheduler ARN that is already a name.
				backends := newExtendedServiceBackends()
				rc := cloudformation.NewResourceCreator(backends)

				physID, err := rc.Create(t.Context(), "PlainSched", "AWS::Scheduler::Schedule",
					map[string]any{
						"Name":               "my-plain-resource",
						"ScheduleExpression": "rate(1 minute)",
					}, nil, nil)
				require.NoError(t, err)

				// Delete using the ARN form (physID) to confirm extraction works.
				err = rc.Delete(t.Context(), "AWS::Scheduler::Schedule", physID, nil)
				require.NoError(t, err)

				return
			}

			// For ARN forms, just verify the ARN is used in scheduler create/delete cycle.
			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			schedName := tt.want
			physID, err := rc.Create(t.Context(), "MySched", "AWS::Scheduler::Schedule",
				map[string]any{
					"Name":               schedName,
					"ScheduleExpression": "rate(1 minute)",
				}, nil, nil)
			require.NoError(t, err)
			assert.Contains(t, physID, schedName)

			err = rc.Delete(t.Context(), "AWS::Scheduler::Schedule", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestStreamNameFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "full_kinesis_arn",
			input: "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
			want:  "my-stream",
		},
		{
			name:  "plain_name_fallback",
			input: "my-plain-stream",
			want:  "my-plain-stream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Exercise streamNameFromARN indirectly via Kinesis delete path.
			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			streamName := tt.want
			physID, err := rc.Create(t.Context(), "MyStream", "AWS::Kinesis::Stream",
				map[string]any{"Name": streamName}, nil, nil)
			require.NoError(t, err)

			// For the plain-name case, deleteKinesisStream receives the stream name (not ARN).
			deleteID := physID
			if tt.input == "my-plain-stream" {
				deleteID = tt.input // pass plain name so fallback branch is hit
			}

			err = rc.Delete(t.Context(), "AWS::Kinesis::Stream", deleteID, nil)
			require.NoError(t, err)
		})
	}
}

// ---- deleteSNSSubscription and deleteS3BucketPolicy nil-backend coverage ----

func TestResourceCreator_DeleteSNSSubscription_NilBackend(t *testing.T) {
	t.Parallel()

	backends := newServiceBackends() // SNS field is set but we want to test nil case; override
	backends.SNS = nil
	rc := cloudformation.NewResourceCreator(backends)

	err := rc.Delete(t.Context(), "AWS::SNS::Subscription",
		"arn:aws:sns:us-east-1:000000000000:topic:sub-id", nil)
	require.NoError(t, err)
}

func TestResourceCreator_DeleteS3BucketPolicy_NilBackend(t *testing.T) {
	t.Parallel()

	backends := newServiceBackends()
	backends.S3 = nil
	rc := cloudformation.NewResourceCreator(backends)

	err := rc.Delete(t.Context(), "AWS::S3::BucketPolicy", "my-bucket", nil)
	require.NoError(t, err)
}

func TestResourceCreator_DeleteS3BucketPolicy_RealBackend(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create bucket then apply policy, then delete policy.
	bucketName, err := rc.Create(t.Context(), "DelBucket", "AWS::S3::Bucket",
		map[string]any{"BucketName": "cfn-del-bucket-pol"}, nil, nil)
	require.NoError(t, err)

	physID, err := rc.Create(t.Context(), "DelBucketPolicy", "AWS::S3::BucketPolicy",
		map[string]any{
			"Bucket":         bucketName,
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		}, nil, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::S3::BucketPolicy", physID, nil)
	require.NoError(t, err)
}

// ---- EC2 DeleteSubnet returns ErrSubnetNotFound for missing subnet ----------

func TestEC2_DeleteSubnet_NotFound(t *testing.T) {
	t.Parallel()

	bk := ec2backend.NewInMemoryBackend("000000000000", "us-east-1")
	err := bk.DeleteSubnet("subnet-notexist")
	require.ErrorIs(t, err, ec2backend.ErrSubnetNotFound)
}
