package main

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudControlDelegatesToServiceBackends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		patchedVal any
		verify     func(t *testing.T, fx *sfnFixture, id string, present bool)
		name       string
		typeName   string
		desired    string
		wantID     string
		patch      string
		patchedKey string
	}{
		{
			name: "s3_bucket", typeName: "AWS::S3::Bucket", desired: `{"BucketName":"cc-delegated-bucket"}`,
			wantID: "cc-delegated-bucket",
			patch:  `[{"op":"add","path":"/Tags","value":[{"Key":"env","Value":"test"}]}]`,
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true }).HeadBucket(
					t.Context(), &s3.HeadBucketInput{Bucket: aws.String(id)},
				)
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "sqs_queue", typeName: "AWS::SQS::Queue", desired: `{"QueueName":"cc-delegated-queue"}`,
			patch:      `[{"op":"add","path":"/VisibilityTimeout","value":77}]`,
			patchedKey: "VisibilityTimeout", patchedVal: float64(77),
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := sqs.NewFromConfig(fx.cfg).GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
					QueueUrl: aws.String(id),
				})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "dynamodb_table", typeName: "AWS::DynamoDB::Table",
			desired: `{"TableName":"cc-delegated-table","BillingMode":"PROVISIONED",` +
				`"ProvisionedThroughput":{"ReadCapacityUnits":5,"WriteCapacityUnits":5},` +
				`"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],` +
				`"AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}`,
			wantID:     "cc-delegated-table",
			patch:      `[{"op":"replace","path":"/ProvisionedThroughput/ReadCapacityUnits","value":10}]`,
			patchedKey: "ProvisionedThroughput",
			patchedVal: map[string]any{"ReadCapacityUnits": 10, "WriteCapacityUnits": 5},
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := dynamodb.NewFromConfig(fx.cfg).DescribeTable(t.Context(), &dynamodb.DescribeTableInput{
					TableName: aws.String(id),
				})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "log_group", typeName: "AWS::Logs::LogGroup",
			desired: `{"LogGroupName":"/cc/delegated","RetentionInDays":7}`, wantID: "/cc/delegated",
			patch:      `[{"op":"replace","path":"/RetentionInDays","value":30}]`,
			patchedKey: "RetentionInDays", patchedVal: float64(30),
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				out, err := cloudwatchlogs.NewFromConfig(fx.cfg).DescribeLogGroups(
					t.Context(), &cloudwatchlogs.DescribeLogGroupsInput{LogGroupNamePrefix: aws.String(id)},
				)
				require.NoError(t, err)
				assert.Equal(t, present, len(out.LogGroups) == 1)
			},
		},
		{
			name: "sns_topic", typeName: "AWS::SNS::Topic", desired: `{"TopicName":"cc-topic"}`,
			patch:      `[{"op":"add","path":"/DisplayName","value":"hello"}]`,
			patchedKey: "DisplayName", patchedVal: "hello",
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := sns.NewFromConfig(fx.cfg).GetTopicAttributes(t.Context(), &sns.GetTopicAttributesInput{
					TopicArn: aws.String(id),
				})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "iam_role", typeName: "AWS::IAM::Role",
			desired: `{"RoleName":"cc-role","AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[` +
				`{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}}`,
			wantID: "cc-role", patch: `[{"op":"add","path":"/Description","value":"cc role"}]`,
			patchedKey: "Description", patchedVal: "cc role",
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := iam.NewFromConfig(fx.cfg).GetRole(t.Context(), &iam.GetRoleInput{RoleName: aws.String(id)})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "kms_key", typeName: "AWS::KMS::Key", desired: `{"Description":"cc key"}`,
			patch:      `[{"op":"replace","path":"/Description","value":"cc key v2"}]`,
			patchedKey: "Description", patchedVal: "cc key v2",
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				out, err := kms.NewFromConfig(fx.cfg).
					DescribeKey(t.Context(), &kms.DescribeKeyInput{KeyId: aws.String(id)})
				require.NoError(t, err)
				assert.Equal(t, present, out.KeyMetadata.KeyState != kmstypes.KeyStatePendingDeletion)
			},
		},
		{
			name:       "secret",
			typeName:   "AWS::SecretsManager::Secret",
			desired:    `{"Name":"cc-secret","SecretString":"s3cret"}`,
			patch:      `[{"op":"add","path":"/Description","value":"cc secret"}]`,
			patchedKey: "Description",
			patchedVal: "cc secret",
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := secretsmanager.NewFromConfig(fx.cfg).DescribeSecret(
					t.Context(), &secretsmanager.DescribeSecretInput{SecretId: aws.String(id)},
				)
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "ssm_parameter", typeName: "AWS::SSM::Parameter",
			desired: `{"Name":"/cc/param","Type":"String","Value":"v1"}`, wantID: "/cc/param",
			patch:      `[{"op":"replace","path":"/Value","value":"v2"}]`,
			patchedKey: "Value", patchedVal: "v2",
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := ssm.NewFromConfig(fx.cfg).
					GetParameter(t.Context(), &ssm.GetParameterInput{Name: aws.String(id)})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "ecr_repository", typeName: "AWS::ECR::Repository", desired: `{"RepositoryName":"cc-repo"}`,
			wantID: "cc-repo", patch: `[{"op":"replace","path":"/ImageTagMutability","value":"IMMUTABLE"}]`,
			patchedKey: "ImageTagMutability", patchedVal: "IMMUTABLE",
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := ecr.NewFromConfig(fx.cfg).DescribeRepositories(t.Context(), &ecr.DescribeRepositoriesInput{
					RepositoryNames: []string{id},
				})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "kinesis_stream", typeName: "AWS::Kinesis::Stream", desired: `{"Name":"cc-stream","ShardCount":1}`,
			wantID: "cc-stream", patch: `[{"op":"replace","path":"/RetentionPeriodHours","value":48}]`,
			patchedKey: "RetentionPeriodHours", patchedVal: 48,
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := kinesis.NewFromConfig(fx.cfg).DescribeStreamSummary(
					t.Context(), &kinesis.DescribeStreamSummaryInput{StreamName: aws.String(id)},
				)
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "event_bus", typeName: "AWS::Events::EventBus", desired: `{"Name":"cc-bus"}`, wantID: "cc-bus",
			patch:      `[{"op":"add","path":"/Description","value":"cc bus"}]`,
			patchedKey: "Description", patchedVal: "cc bus",
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := eventbridge.NewFromConfig(fx.cfg).DescribeEventBus(
					t.Context(), &eventbridge.DescribeEventBusInput{Name: aws.String(id)},
				)
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "lambda_function", typeName: "AWS::Lambda::Function",
			desired: `{"FunctionName":"cc-fn","Runtime":"python3.12","Handler":"index.handler",` +
				`"Role":"arn:aws:iam::000000000000:role/cc-fn","Code":{"ZipFile":"def handler(e, c):\n  return 1\n"}}`,
			wantID: "cc-fn", patch: `[{"op":"add","path":"/Description","value":"cc fn"}]`,
			patchedKey: "Description", patchedVal: "cc fn",
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := lambda.NewFromConfig(fx.cfg).GetFunction(t.Context(), &lambda.GetFunctionInput{
					FunctionName: aws.String(id),
				})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name: "state_machine", typeName: "AWS::StepFunctions::StateMachine",
			desired: `{"StateMachineName":"cc-sm","RoleArn":"arn:aws:iam::000000000000:role/cc-sfn",` +
				`"DefinitionString":"{\"StartAt\":\"P\",\"States\":{\"P\":{\"Type\":\"Pass\",\"End\":true}}}"}`,
			patch:      `[{"op":"replace","path":"/RoleArn","value":"arn:aws:iam::000000000000:role/cc-sfn2"}]`,
			patchedKey: "RoleArn", patchedVal: "arn:aws:iam::000000000000:role/cc-sfn2",
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := sfn.NewFromConfig(fx.cfg).DescribeStateMachine(t.Context(), &sfn.DescribeStateMachineInput{
					StateMachineArn: aws.String(id),
				})
				assert.Equal(t, present, err == nil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			cc := cloudcontrol.NewFromConfig(fx.cfg)

			created, err := cc.CreateResource(t.Context(), &cloudcontrol.CreateResourceInput{
				TypeName: aws.String(tt.typeName), DesiredState: aws.String(tt.desired),
			})
			require.NoError(t, err)

			id := aws.ToString(created.ProgressEvent.Identifier)
			require.NotEmpty(t, id)

			if tt.wantID != "" {
				assert.Equal(t, tt.wantID, id)
			}

			tt.verify(t, fx, id, true)

			got, err := cc.GetResource(t.Context(), &cloudcontrol.GetResourceInput{
				TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
			})
			require.NoError(t, err)
			assert.Equal(t, id, aws.ToString(got.ResourceDescription.Identifier))

			listed, err := cc.ListResources(t.Context(), &cloudcontrol.ListResourcesInput{
				TypeName: aws.String(tt.typeName),
			})
			require.NoError(t, err)

			var listedIDs []string
			for _, d := range listed.ResourceDescriptions {
				listedIDs = append(listedIDs, aws.ToString(d.Identifier))
			}

			assert.Contains(t, listedIDs, id)

			_, err = cc.UpdateResource(t.Context(), &cloudcontrol.UpdateResourceInput{
				TypeName: aws.String(tt.typeName), Identifier: aws.String(id), PatchDocument: aws.String(tt.patch),
			})
			require.NoError(t, err)

			if tt.patchedKey != "" {
				after, getErr := cc.GetResource(t.Context(), &cloudcontrol.GetResourceInput{
					TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
				})
				require.NoError(t, getErr)
				assert.JSONEq(t, `{"`+tt.patchedKey+`":`+jsonScalar(tt.patchedVal)+`}`,
					subsetJSON(t, aws.ToString(after.ResourceDescription.Properties), tt.patchedKey))
			}

			_, err = cc.DeleteResource(t.Context(), &cloudcontrol.DeleteResourceInput{
				TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
			})
			require.NoError(t, err)

			tt.verify(t, fx, id, false)

			_, err = cc.GetResource(t.Context(), &cloudcontrol.GetResourceInput{
				TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
			})
			require.Error(t, err)
		})
	}
}

func jsonScalar(v any) string {
	b, _ := json.Marshal(v)

	return string(b)
}

func subsetJSON(t *testing.T, doc, key string) string {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(doc), &m))

	b, err := json.Marshal(map[string]any{key: m[key]})
	require.NoError(t, err)

	return string(b)
}
