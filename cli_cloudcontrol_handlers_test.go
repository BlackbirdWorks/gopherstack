package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/cloudcontrol"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/route53"
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
		patchedVal     any
		verify         func(t *testing.T, fx *sfnFixture, id string, present bool)
		name           string
		typeName       string
		desired        string
		wantID         string
		patch          string
		patchedKey     string
		immutablePatch string
		wantKeys       []string
		absentKeys     []string
	}{
		{
			name:     "s3_bucket",
			typeName: "AWS::S3::Bucket",
			desired:  `{"BucketName":"cc-delegated-bucket"}`,
			wantID:   "cc-delegated-bucket",
			patch:    `[{"op":"add","path":"/Tags","value":[{"Key":"env","Value":"test"}]}]`,
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true }).HeadBucket(
					t.Context(), &s3.HeadBucketInput{Bucket: aws.String(id)},
				)
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name:       "sqs_queue",
			typeName:   "AWS::SQS::Queue",
			desired:    `{"QueueName":"cc-delegated-queue"}`,
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
			name:       "sns_topic",
			typeName:   "AWS::SNS::Topic",
			desired:    `{"TopicName":"cc-topic"}`,
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
			name:       "kms_key",
			typeName:   "AWS::KMS::Key",
			desired:    `{"Description":"cc key"}`,
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
			name:     "ecr_repository",
			typeName: "AWS::ECR::Repository",
			desired:  `{"RepositoryName":"cc-repo"}`,
			wantID:   "cc-repo", patch: `[{"op":"replace","path":"/ImageTagMutability","value":"IMMUTABLE"}]`,
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
			name:     "kinesis_stream",
			typeName: "AWS::Kinesis::Stream",
			desired:  `{"Name":"cc-stream","ShardCount":1}`,
			wantID:   "cc-stream", patch: `[{"op":"replace","path":"/RetentionPeriodHours","value":48}]`,
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
		{
			immutablePatch: `[{"op":"replace","path":"/CidrBlock","value":"10.99.0.0/16"}]`,
			wantKeys:       []string{"VpcId", "CidrBlockAssociations", "DefaultSecurityGroup", "DefaultNetworkAcl"},
			name:           "ec2_vpc",
			typeName:       "AWS::EC2::VPC",
			desired:        `{"CidrBlock":"10.20.0.0/16"}`,
			patch:          `[{"op":"add","path":"/EnableDnsHostnames","value":true}]`,
			patchedKey:     "EnableDnsHostnames", patchedVal: true,
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				out, err := ec2.NewFromConfig(fx.cfg).
					DescribeVpcs(t.Context(), &ec2.DescribeVpcsInput{VpcIds: []string{id}})
				assert.Equal(t, present, err == nil && len(out.Vpcs) == 1)
			},
		},
		{
			immutablePatch: `[{"op":"replace","path":"/CidrBlock","value":"10.30.9.0/24"}]`,
			name:           "ec2_subnet",
			typeName:       "AWS::EC2::Subnet",
			desired:        `{"VpcId":"{{vpc}}","CidrBlock":"10.30.1.0/24"}`,
			patch:          `[{"op":"add","path":"/MapPublicIpOnLaunch","value":true}]`,
			patchedKey:     "MapPublicIpOnLaunch", patchedVal: true,
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				out, err := ec2.NewFromConfig(fx.cfg).
					DescribeSubnets(t.Context(), &ec2.DescribeSubnetsInput{SubnetIds: []string{id}})
				assert.Equal(t, present, err == nil && len(out.Subnets) == 1)
			},
		},
		{
			immutablePatch: `[{"op":"replace","path":"/GroupDescription","value":"other"}]`,
			wantKeys:       []string{"Id", "GroupId"},
			name:           "ec2_security_group", typeName: "AWS::EC2::SecurityGroup",
			desired: `{"GroupDescription":"cc sg","GroupName":"cc-sg","SecurityGroupIngress":` +
				`[{"IpProtocol":"tcp","FromPort":80,"ToPort":80,"CidrIp":"10.0.0.0/8"}]}`,
			patch: `[{"op":"add","path":"/SecurityGroupIngress/-","value":` +
				`{"IpProtocol":"tcp","FromPort":443,"ToPort":443,"CidrIp":"10.0.0.0/8"}}]`,
			patchedKey: "SecurityGroupIngress",
			patchedVal: []any{
				map[string]any{"IpProtocol": "tcp", "FromPort": 80, "ToPort": 80, "CidrIp": "10.0.0.0/8"},
				map[string]any{"IpProtocol": "tcp", "FromPort": 443, "ToPort": 443, "CidrIp": "10.0.0.0/8"},
			},
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				out, err := ec2.NewFromConfig(fx.cfg).DescribeSecurityGroups(
					t.Context(), &ec2.DescribeSecurityGroupsInput{GroupIds: []string{id}},
				)
				assert.Equal(t, present, err == nil && len(out.SecurityGroups) == 1)
			},
		},
		{
			name:       "ecs_cluster",
			typeName:   "AWS::ECS::Cluster",
			desired:    `{"ClusterName":"cc-cluster"}`,
			wantID:     "cc-cluster",
			patch:      `[{"op":"add","path":"/ClusterSettings","value":[{"Name":"containerInsights","Value":"enabled"}]}]`,
			patchedKey: "ClusterSettings",
			patchedVal: []any{map[string]any{"Name": "containerInsights", "Value": "enabled"}},
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				out, err := ecs.NewFromConfig(fx.cfg).
					DescribeClusters(t.Context(), &ecs.DescribeClustersInput{Clusters: []string{id}})
				require.NoError(t, err)
				assert.Equal(t, present, len(out.Clusters) == 1 && aws.ToString(out.Clusters[0].Status) == "ACTIVE")
			},
		},
		{
			name:           "elbv2_target_group",
			immutablePatch: `[{"op":"replace","path":"/Name","value":"other-tg"}]`,
			wantKeys:       []string{"TargetGroupArn", "TargetGroupName", "TargetGroupFullName"},
			typeName:       "AWS::ElasticLoadBalancingV2::TargetGroup",
			desired:        `{"Name":"cc-tg","Protocol":"HTTP","Port":80,"VpcId":"{{vpc}}"}`,
			patch:          `[{"op":"add","path":"/HealthCheckPath","value":"/health"}]`,
			patchedKey:     "HealthCheckPath",
			patchedVal:     "/health",
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				out, err := elbv2.NewFromConfig(fx.cfg).DescribeTargetGroups(
					t.Context(), &elbv2.DescribeTargetGroupsInput{TargetGroupArns: []string{id}},
				)
				assert.Equal(t, present, err == nil && len(out.TargetGroups) == 1)
			},
		},
		{
			immutablePatch: `[{"op":"replace","path":"/Name","value":"other.example.com"}]`,
			wantKeys:       []string{"Id"},
			name:           "route53_hosted_zone",
			typeName:       "AWS::Route53::HostedZone",
			desired:        `{"Name":"cc-example.com"}`,
			patch:          `[{"op":"add","path":"/HostedZoneConfig","value":{"Comment":"cc zone"}}]`,
			patchedKey:     "HostedZoneConfig", patchedVal: map[string]any{"Comment": "cc zone"},
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := route53.NewFromConfig(fx.cfg).
					GetHostedZone(t.Context(), &route53.GetHostedZoneInput{Id: aws.String(id)})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			immutablePatch: `[{"op":"replace","path":"/AlarmName","value":"other"}]`,
			wantKeys:       []string{"Arn"},
			name:           "cloudwatch_alarm", typeName: "AWS::CloudWatch::Alarm",
			desired: `{"AlarmName":"cc-alarm","ComparisonOperator":"GreaterThanThreshold","EvaluationPeriods":1,` +
				`"MetricName":"CPUUtilization","Namespace":"AWS/EC2","Period":60,"Statistic":"Average","Threshold":80}`,
			wantID: "cc-alarm", patch: `[{"op":"replace","path":"/Threshold","value":90}]`,
			patchedKey: "Threshold", patchedVal: float64(90),
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				out, err := cloudwatch.NewFromConfig(fx.cfg).DescribeAlarms(
					t.Context(), &cloudwatch.DescribeAlarmsInput{AlarmNames: []string{id}},
				)
				require.NoError(t, err)
				assert.Equal(t, present, len(out.MetricAlarms) == 1)
			},
		},
		{
			immutablePatch: `[{"op":"replace","path":"/UserPoolName","value":"other"}]`,
			wantKeys:       []string{"UserPoolId", "Arn", "ProviderName", "ProviderURL"},
			absentKeys:     []string{"Id"},
			name:           "cognito_user_pool",
			typeName:       "AWS::Cognito::UserPool",
			desired:        `{"UserPoolName":"cc-pool"}`,
			patch:          `[{"op":"add","path":"/MfaConfiguration","value":"OFF"}]`,
			patchedKey:     "MfaConfiguration", patchedVal: "OFF",
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := cognitoidentityprovider.NewFromConfig(fx.cfg).DescribeUserPool(
					t.Context(), &cognitoidentityprovider.DescribeUserPoolInput{UserPoolId: aws.String(id)},
				)
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name:       "apigateway_rest_api",
			typeName:   "AWS::ApiGateway::RestApi",
			desired:    `{"Name":"cc-api"}`,
			patch:      `[{"op":"add","path":"/Description","value":"cc api"}]`,
			patchedKey: "Description", patchedVal: "cc api",
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := apigateway.NewFromConfig(fx.cfg).
					GetRestApi(t.Context(), &apigateway.GetRestApiInput{RestApiId: aws.String(id)})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			immutablePatch: `[{"op":"replace","path":"/PerformanceMode","value":"maxIO"}]`,
			wantKeys:       []string{"FileSystemId", "Arn"},
			name:           "efs_file_system",
			typeName:       "AWS::EFS::FileSystem",
			desired:        `{"PerformanceMode":"generalPurpose","Encrypted":true}`,
			patch:          `[{"op":"add","path":"/BackupPolicy","value":{"Status":"ENABLED"}}]`,
			patchedKey:     "BackupPolicy",
			patchedVal:     map[string]any{"Status": "ENABLED"},
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				out, err := efs.NewFromConfig(fx.cfg).
					DescribeFileSystems(t.Context(), &efs.DescribeFileSystemsInput{FileSystemId: aws.String(id)})
				assert.Equal(t, present, err == nil && len(out.FileSystems) == 1)
			},
		},
		{
			immutablePatch: `[{"op":"replace","path":"/DatabaseName","value":"other_db"}]`,
			wantKeys:       []string{"CatalogId", "DatabaseName"},
			absentKeys:     []string{"Tags"},
			name:           "glue_database",
			typeName:       "AWS::Glue::Database",
			desired:        `{"CatalogId":"000000000000","DatabaseInput":{"Name":"cc_db"}}`,
			wantID:         "cc_db",
			patch:          `[{"op":"add","path":"/DatabaseInput/Description","value":"cc db"}]`,
			patchedKey:     "DatabaseInput",
			patchedVal:     map[string]any{"Name": "cc_db", "Description": "cc db"},
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := glue.NewFromConfig(fx.cfg).
					GetDatabase(t.Context(), &glue.GetDatabaseInput{Name: aws.String(id)})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name:       "ecs_cluster_service_connect",
			typeName:   "AWS::ECS::Cluster",
			desired:    `{"ClusterName":"cc-cluster-sc","ServiceConnectDefaults":{"Namespace":"ns-one"}}`,
			wantID:     "cc-cluster-sc",
			patch:      `[{"op":"replace","path":"/ServiceConnectDefaults/Namespace","value":"ns-two"}]`,
			patchedKey: "ServiceConnectDefaults", patchedVal: map[string]any{"Namespace": "ns-two"},
			immutablePatch: `[{"op":"replace","path":"/ClusterName","value":"other"}]`,
			wantKeys:       []string{"Arn", "ServiceConnectDefaults"},
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				out, err := ecs.NewFromConfig(fx.cfg).
					DescribeClusters(t.Context(), &ecs.DescribeClustersInput{Clusters: []string{id}})
				require.NoError(t, err)
				assert.Equal(t, present, len(out.Clusters) == 1 && aws.ToString(out.Clusters[0].Status) == "ACTIVE")
			},
		},
		{
			name:       "route53_private_zone_vpcs",
			typeName:   "AWS::Route53::HostedZone",
			desired:    `{"Name":"cc-private.example.com","VPCs":[{"VPCId":"{{vpc}}","VPCRegion":"{{region}}"}]}`,
			patch:      `[{"op":"replace","path":"/VPCs","value":[{"VPCId":"{{vpc2}}","VPCRegion":"{{region}}"}]}]`,
			patchedKey: "VPCs", patchedVal: []any{map[string]any{"VPCId": "{{vpc2}}", "VPCRegion": "{{region}}"}},
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := route53.NewFromConfig(fx.cfg).
					GetHostedZone(t.Context(), &route53.GetHostedZoneInput{Id: aws.String(id)})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name:       "apigateway_rest_api_security_policy",
			typeName:   "AWS::ApiGateway::RestApi",
			desired:    `{"Name":"cc-api-sp","EndpointAccessMode":"BASIC"}`,
			patch:      `[{"op":"add","path":"/SecurityPolicy","value":"TLS_1_2"}]`,
			patchedKey: "SecurityPolicy", patchedVal: "TLS_1_2",
			wantKeys: []string{"RestApiId", "RootResourceId", "EndpointAccessMode"},
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := apigateway.NewFromConfig(fx.cfg).
					GetRestApi(t.Context(), &apigateway.GetRestApiInput{RestApiId: aws.String(id)})
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name:     "cognito_user_pool_attribute_update_settings",
			typeName: "AWS::Cognito::UserPool",
			desired:  `{"UserPoolName":"cc-pool-us"}`,
			patch: `[{"op":"add","path":"/UserAttributeUpdateSettings",` +
				`"value":{"AttributesRequireVerificationBeforeUpdate":["email"]}}]`,
			patchedKey: "UserAttributeUpdateSettings",
			patchedVal: map[string]any{"AttributesRequireVerificationBeforeUpdate": []any{"email"}},
			wantKeys:   []string{"UserPoolTier"},
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				_, err := cognitoidentityprovider.NewFromConfig(fx.cfg).DescribeUserPool(
					t.Context(), &cognitoidentityprovider.DescribeUserPoolInput{UserPoolId: aws.String(id)},
				)
				assert.Equal(t, present, err == nil)
			},
		},
		{
			name:       "efs_file_system_protection",
			typeName:   "AWS::EFS::FileSystem",
			desired:    `{"PerformanceMode":"generalPurpose"}`,
			patch:      `[{"op":"add","path":"/FileSystemProtection","value":{"ReplicationOverwriteProtection":"DISABLED"}}]`,
			patchedKey: "FileSystemProtection",
			patchedVal: map[string]any{"ReplicationOverwriteProtection": "DISABLED"},
			verify: func(t *testing.T, fx *sfnFixture, id string, present bool) {
				t.Helper()

				out, err := efs.NewFromConfig(fx.cfg).
					DescribeFileSystems(t.Context(), &efs.DescribeFileSystemsInput{FileSystemId: aws.String(id)})
				assert.Equal(t, present, err == nil && len(out.FileSystems) == 1)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			cc := cloudcontrol.NewFromConfig(fx.cfg)
			desired := tt.desired

			patch := tt.patch
			vpc2 := ""

			if strings.Contains(desired, "{{vpc}}") {
				vpc, vpcErr := ec2.NewFromConfig(fx.cfg).CreateVpc(t.Context(), &ec2.CreateVpcInput{
					CidrBlock: aws.String("10.30.0.0/16"),
				})
				require.NoError(t, vpcErr)

				desired = strings.ReplaceAll(desired, "{{vpc}}", aws.ToString(vpc.Vpc.VpcId))
			}

			if strings.Contains(patch, "{{vpc2}}") {
				vpc, vpcErr := ec2.NewFromConfig(fx.cfg).CreateVpc(t.Context(), &ec2.CreateVpcInput{
					CidrBlock: aws.String("10.31.0.0/16"),
				})
				require.NoError(t, vpcErr)

				vpc2 = aws.ToString(vpc.Vpc.VpcId)
				patch = strings.ReplaceAll(patch, "{{vpc2}}", vpc2)
			}

			desired = strings.ReplaceAll(desired, "{{region}}", fx.cfg.Region)
			patch = strings.ReplaceAll(patch, "{{region}}", fx.cfg.Region)

			created, err := cc.CreateResource(t.Context(), &cloudcontrol.CreateResourceInput{
				TypeName: aws.String(tt.typeName), DesiredState: aws.String(desired),
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

			var props map[string]any
			require.NoError(t, json.Unmarshal([]byte(aws.ToString(got.ResourceDescription.Properties)), &props))

			for _, k := range tt.wantKeys {
				assert.Contains(t, props, k)
				assert.NotEmpty(t, props[k], k)
			}

			for _, k := range tt.absentKeys {
				assert.NotContains(t, props, k)
			}

			if tt.immutablePatch != "" {
				_, immErr := cc.UpdateResource(t.Context(), &cloudcontrol.UpdateResourceInput{
					TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
					PatchDocument: aws.String(tt.immutablePatch),
				})
				require.Error(t, immErr)
			}

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
				TypeName: aws.String(tt.typeName), Identifier: aws.String(id), PatchDocument: aws.String(patch),
			})
			require.NoError(t, err)

			if tt.patchedKey != "" {
				after, getErr := cc.GetResource(t.Context(), &cloudcontrol.GetResourceInput{
					TypeName: aws.String(tt.typeName), Identifier: aws.String(id),
				})
				require.NoError(t, getErr)
				repl := strings.NewReplacer("{{vpc2}}", vpc2, "{{region}}", fx.cfg.Region)
				want := repl.Replace(jsonScalar(tt.patchedVal))
				assert.JSONEq(t, `{"`+tt.patchedKey+`":`+want+`}`,
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
