package main

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
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

func ccStorageCases() []ccDelegationCase {
	return []ccDelegationCase{
		{
			name:     "s3_bucket",
			typeName: "AWS::S3::Bucket",
			desired:  `{"BucketName":"cc-delegated-bucket"}`,
			wantID:   "cc-delegated-bucket",
			patch:    `[{"op":"add","path":"/Tags","value":[{"Key":"env","Value":"test"}]}]`,
			verify:   ccVerifyS3Bucket,
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
			verify:     ccVerifyDynamodbTable,
		},
		{
			name:     "ecr_repository",
			typeName: "AWS::ECR::Repository",
			desired:  `{"RepositoryName":"cc-repo"}`,
			wantID:   "cc-repo", patch: `[{"op":"replace","path":"/ImageTagMutability","value":"IMMUTABLE"}]`,
			patchedKey: "ImageTagMutability", patchedVal: "IMMUTABLE",
			verify: ccVerifyEcrRepository,
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
			verify:         ccVerifyGlueDatabase,
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
			verify:         ccVerifyEfsFileSystem,
		},
		{
			name:       "efs_file_system_protection",
			typeName:   "AWS::EFS::FileSystem",
			desired:    `{"PerformanceMode":"generalPurpose"}`,
			patch:      `[{"op":"add","path":"/FileSystemProtection","value":{"ReplicationOverwriteProtection":"DISABLED"}}]`,
			patchedKey: "FileSystemProtection",
			patchedVal: map[string]any{"ReplicationOverwriteProtection": "DISABLED"},
			verify:     ccVerifyEfsFileSystemProtection,
		},
		{
			name:     "efs_file_system_replication",
			typeName: "AWS::EFS::FileSystem",
			desired: `{"PerformanceMode":"generalPurpose",` +
				`"ReplicationConfiguration":{"Destinations":[{"Region":"{{region}}"}]}}`,
			patch:         `[{"op":"replace","path":"/ReplicationConfiguration/Destinations/0/Region","value":"eu-west-1"}]`,
			wantKeys:      []string{"ReplicationConfiguration"},
			verify:        ccVerifyEfsFileSystemReplication,
			verifyPatched: ccVerifyPatchedEfsFileSystemReplication,
		},
	}
}

func ccMessagingCases() []ccDelegationCase {
	return []ccDelegationCase{
		{
			name:       "sqs_queue",
			typeName:   "AWS::SQS::Queue",
			desired:    `{"QueueName":"cc-delegated-queue"}`,
			patch:      `[{"op":"add","path":"/VisibilityTimeout","value":77}]`,
			patchedKey: "VisibilityTimeout", patchedVal: float64(77),
			verify: ccVerifySqsQueue,
		},
		{
			name:       "sns_topic",
			typeName:   "AWS::SNS::Topic",
			desired:    `{"TopicName":"cc-topic"}`,
			patch:      `[{"op":"add","path":"/DisplayName","value":"hello"}]`,
			patchedKey: "DisplayName", patchedVal: "hello",
			verify: ccVerifySnsTopic,
		},
		{
			name:     "kinesis_stream",
			typeName: "AWS::Kinesis::Stream",
			desired:  `{"Name":"cc-stream","ShardCount":1}`,
			wantID:   "cc-stream", patch: `[{"op":"replace","path":"/RetentionPeriodHours","value":48}]`,
			patchedKey: "RetentionPeriodHours", patchedVal: 48,
			verify: ccVerifyKinesisStream,
		},
		{
			name: "event_bus", typeName: "AWS::Events::EventBus", desired: `{"Name":"cc-bus"}`, wantID: "cc-bus",
			patch:      `[{"op":"add","path":"/Description","value":"cc bus"}]`,
			patchedKey: "Description", patchedVal: "cc bus",
			verify: ccVerifyEventBus,
		},
		{
			name: "log_group", typeName: "AWS::Logs::LogGroup",
			desired: `{"LogGroupName":"/cc/delegated","RetentionInDays":7}`, wantID: "/cc/delegated",
			patch:      `[{"op":"replace","path":"/RetentionInDays","value":30}]`,
			patchedKey: "RetentionInDays", patchedVal: float64(30),
			verify: ccVerifyLogGroup,
		},
		{
			immutablePatch: `[{"op":"replace","path":"/AlarmName","value":"other"}]`,
			wantKeys:       []string{"Arn"},
			name:           "cloudwatch_alarm", typeName: "AWS::CloudWatch::Alarm",
			desired: `{"AlarmName":"cc-alarm","ComparisonOperator":"GreaterThanThreshold","EvaluationPeriods":1,` +
				`"MetricName":"CPUUtilization","Namespace":"AWS/EC2","Period":60,"Statistic":"Average","Threshold":80}`,
			wantID: "cc-alarm", patch: `[{"op":"replace","path":"/Threshold","value":90}]`,
			patchedKey: "Threshold", patchedVal: float64(90),
			verify: ccVerifyCloudwatchAlarm,
		},
		{
			name:     "cloudwatch_alarm_window_warmup",
			typeName: "AWS::CloudWatch::Alarm",
			desired: `{"AlarmName":"cc-alarm-window","ComparisonOperator":"GreaterThanThreshold","EvaluationPeriods":1,` +
				`"MetricName":"CPUUtilization","Namespace":"AWS/EC2","Period":60,"Statistic":"Average","Threshold":80,` +
				`"EvaluationWindow":{"WallClockWindow":{"Timezone":"UTC"}},` +
				`"WarmUpConfiguration":{"WarmUpPeriodDurationInMinutes":5,` +
				`"OnlyStartEvaluatingAfterWarmUpPeriodEnds":true}}`,
			wantID:     "cc-alarm-window",
			wantKeys:   []string{"EvaluationWindow", "WarmUpConfiguration"},
			patch:      `[{"op":"replace","path":"/WarmUpConfiguration/WarmUpPeriodDurationInMinutes","value":10}]`,
			patchedKey: "WarmUpConfiguration",
			patchedVal: map[string]any{
				"WarmUpPeriodDurationInMinutes": 10, "OnlyStartEvaluatingAfterWarmUpPeriodEnds": true,
			},
			verify: ccVerifyCloudwatchAlarmWindowWarmup,
		},
	}
}

func ccSecurityCases() []ccDelegationCase {
	return []ccDelegationCase{
		{
			name: "iam_role", typeName: "AWS::IAM::Role",
			desired: `{"RoleName":"cc-role","AssumeRolePolicyDocument":{"Version":"2012-10-17","Statement":[` +
				`{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}}`,
			wantID: "cc-role", patch: `[{"op":"add","path":"/Description","value":"cc role"}]`,
			patchedKey: "Description", patchedVal: "cc role",
			verify: ccVerifyIamRole,
		},
		{
			name:       "kms_key",
			typeName:   "AWS::KMS::Key",
			desired:    `{"Description":"cc key"}`,
			patch:      `[{"op":"replace","path":"/Description","value":"cc key v2"}]`,
			patchedKey: "Description", patchedVal: "cc key v2",
			verify: ccVerifyKmsKey,
		},
		{
			name:       "secret",
			typeName:   "AWS::SecretsManager::Secret",
			desired:    `{"Name":"cc-secret","SecretString":"s3cret"}`,
			patch:      `[{"op":"add","path":"/Description","value":"cc secret"}]`,
			patchedKey: "Description",
			patchedVal: "cc secret",
			verify:     ccVerifySecret,
		},
		{
			name: "ssm_parameter", typeName: "AWS::SSM::Parameter",
			desired: `{"Name":"/cc/param","Type":"String","Value":"v1"}`, wantID: "/cc/param",
			patch:      `[{"op":"replace","path":"/Value","value":"v2"}]`,
			patchedKey: "Value", patchedVal: "v2",
			verify: ccVerifySsmParameter,
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
			verify: ccVerifyCognitoUserPool,
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
			verify:     ccVerifyCognitoUserPoolAttributeUpdateSettings,
		},
		{
			name:     "cognito_user_pool_mfa_webauthn_issuer_key",
			typeName: "AWS::Cognito::UserPool",
			desired: `{"UserPoolName":"cc-pool-mfa","MfaConfiguration":"OPTIONAL",` +
				`"EnabledMfas":["SOFTWARE_TOKEN_MFA"],"WebAuthnRelyingPartyID":"auth.example.com",` +
				`"WebAuthnUserVerification":"preferred","IssuerConfiguration":{"Type":"UPDATED"},` +
				`"KeyConfiguration":{"KeyType":"AWS_OWNED_KEY"}}`,
			patch:      `[{"op":"replace","path":"/WebAuthnUserVerification","value":"required"}]`,
			patchedKey: "WebAuthnUserVerification", patchedVal: "required",
			wantKeys: []string{
				"EnabledMfas", "WebAuthnRelyingPartyID", "WebAuthnUserVerification", "IssuerConfiguration",
				"KeyConfiguration",
			},
			verify:        ccVerifyCognitoUserPoolMfaWebauthnIssuerKey,
			verifyPatched: ccVerifyPatchedCognitoUserPoolMfaWebauthnIssuerKey,
		},
	}
}

func ccNetworkCases() []ccDelegationCase {
	return []ccDelegationCase{
		{
			immutablePatch: `[{"op":"replace","path":"/CidrBlock","value":"10.99.0.0/16"}]`,
			wantKeys:       []string{"VpcId", "CidrBlockAssociations", "DefaultSecurityGroup", "DefaultNetworkAcl"},
			name:           "ec2_vpc",
			typeName:       "AWS::EC2::VPC",
			desired:        `{"CidrBlock":"10.20.0.0/16"}`,
			patch:          `[{"op":"add","path":"/EnableDnsHostnames","value":true}]`,
			patchedKey:     "EnableDnsHostnames", patchedVal: true,
			verify: ccVerifyEc2Vpc,
		},
		{
			immutablePatch: `[{"op":"replace","path":"/CidrBlock","value":"10.30.9.0/24"}]`,
			name:           "ec2_subnet",
			typeName:       "AWS::EC2::Subnet",
			desired:        `{"VpcId":"{{vpc}}","CidrBlock":"10.30.1.0/24"}`,
			patch:          `[{"op":"add","path":"/MapPublicIpOnLaunch","value":true}]`,
			patchedKey:     "MapPublicIpOnLaunch", patchedVal: true,
			verify: ccVerifyEc2Subnet,
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
			verify: ccVerifyEc2SecurityGroup,
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
			verify:         ccVerifyElbv2TargetGroup,
		},
		{
			name:     "elbv2_target_group_targets_attributes",
			typeName: "AWS::ElasticLoadBalancingV2::TargetGroup",
			desired: `{"Name":"cc-tg-targets","Protocol":"HTTP","Port":80,"VpcId":"{{vpc}}","TargetType":"ip",` +
				`"TargetControlPort":8443,"Targets":[{"Id":"10.0.0.5","Port":80}],` +
				`"TargetGroupAttributes":[{"Key":"deregistration_delay.timeout_seconds","Value":"30"}]}`,
			patch: `[{"op":"replace","path":"/Targets","value":[{"Id":"10.0.0.6","Port":80}]},` +
				`{"op":"replace","path":"/TargetGroupAttributes","value":` +
				`[{"Key":"deregistration_delay.timeout_seconds","Value":"60"}]}]`,
			patchedKey: "Targets", patchedVal: []any{map[string]any{"Id": "10.0.0.6", "Port": 80}},
			wantKeys:       []string{"Targets", "TargetGroupAttributes", "TargetControlPort"},
			immutablePatch: `[{"op":"replace","path":"/TargetControlPort","value":9443}]`,
			verify:         ccVerifyElbv2TargetGroupTargetsAttributes,
			verifyPatched:  ccVerifyPatchedElbv2TargetGroupTargetsAttributes,
		},
		{
			immutablePatch: `[{"op":"replace","path":"/Name","value":"other.example.com"}]`,
			wantKeys:       []string{"Id"},
			name:           "route53_hosted_zone",
			typeName:       "AWS::Route53::HostedZone",
			desired:        `{"Name":"cc-example.com"}`,
			patch:          `[{"op":"add","path":"/HostedZoneConfig","value":{"Comment":"cc zone"}}]`,
			patchedKey:     "HostedZoneConfig", patchedVal: map[string]any{"Comment": "cc zone"},
			verify: ccVerifyRoute53HostedZone,
		},
		{
			name:       "route53_private_zone_vpcs",
			typeName:   "AWS::Route53::HostedZone",
			desired:    `{"Name":"cc-private.example.com","VPCs":[{"VPCId":"{{vpc}}","VPCRegion":"{{region}}"}]}`,
			patch:      `[{"op":"replace","path":"/VPCs","value":[{"VPCId":"{{vpc2}}","VPCRegion":"{{region}}"}]}]`,
			patchedKey: "VPCs", patchedVal: []any{map[string]any{"VPCId": "{{vpc2}}", "VPCRegion": "{{region}}"}},
			verify: ccVerifyRoute53PrivateZoneVpcs,
		},
		{
			name:     "route53_hosted_zone_features_query_logging",
			typeName: "AWS::Route53::HostedZone",
			desired: `{"Name":"cc-features.example.com","HostedZoneFeatures":{"EnableAcceleratedRecovery":true},` +
				`"QueryLoggingConfig":{"CloudWatchLogsLogGroupArn":` +
				`"arn:aws:logs:us-east-1:000000000000:log-group:/aws/route53/cc-features"}}`,
			patch: `[{"op":"replace","path":"/QueryLoggingConfig/CloudWatchLogsLogGroupArn","value":` +
				`"arn:aws:logs:us-east-1:000000000000:log-group:/aws/route53/cc-other"},` +
				`{"op":"replace","path":"/HostedZoneFeatures/EnableAcceleratedRecovery","value":false}]`,
			patchedKey: "QueryLoggingConfig",
			patchedVal: map[string]any{
				"CloudWatchLogsLogGroupArn": "arn:aws:logs:us-east-1:000000000000:log-group:/aws/route53/cc-other",
			},
			wantKeys:      []string{"HostedZoneFeatures", "QueryLoggingConfig"},
			verify:        ccVerifyRoute53HostedZoneFeaturesQueryLogging,
			verifyPatched: ccVerifyPatchedRoute53HostedZoneFeaturesQueryLogging,
		},
	}
}

func ccComputeCases() []ccDelegationCase {
	return []ccDelegationCase{
		{
			name: "lambda_function", typeName: "AWS::Lambda::Function",
			desired: `{"FunctionName":"cc-fn","Runtime":"python3.12","Handler":"index.handler",` +
				`"Role":"arn:aws:iam::000000000000:role/cc-fn","Code":{"ZipFile":"def handler(e, c):\n  return 1\n"}}`,
			wantID: "cc-fn", patch: `[{"op":"add","path":"/Description","value":"cc fn"}]`,
			patchedKey: "Description", patchedVal: "cc fn",
			verify: ccVerifyLambdaFunction,
		},
		{
			name: "state_machine", typeName: "AWS::StepFunctions::StateMachine",
			desired: `{"StateMachineName":"cc-sm","RoleArn":"arn:aws:iam::000000000000:role/cc-sfn",` +
				`"DefinitionString":"{\"StartAt\":\"P\",\"States\":{\"P\":{\"Type\":\"Pass\",\"End\":true}}}"}`,
			patch:      `[{"op":"replace","path":"/RoleArn","value":"arn:aws:iam::000000000000:role/cc-sfn2"}]`,
			patchedKey: "RoleArn", patchedVal: "arn:aws:iam::000000000000:role/cc-sfn2",
			verify: ccVerifyStateMachine,
		},
		{
			name:       "ecs_cluster",
			typeName:   "AWS::ECS::Cluster",
			desired:    `{"ClusterName":"cc-cluster"}`,
			wantID:     "cc-cluster",
			patch:      `[{"op":"add","path":"/ClusterSettings","value":[{"Name":"containerInsights","Value":"enabled"}]}]`,
			patchedKey: "ClusterSettings",
			patchedVal: []any{map[string]any{"Name": "containerInsights", "Value": "enabled"}},
			verify:     ccVerifyEcsCluster,
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
			verify:         ccVerifyEcsClusterServiceConnect,
		},
	}
}

func ccAppCases() []ccDelegationCase {
	return []ccDelegationCase{
		{
			name:       "apigateway_rest_api",
			typeName:   "AWS::ApiGateway::RestApi",
			desired:    `{"Name":"cc-api"}`,
			patch:      `[{"op":"add","path":"/Description","value":"cc api"}]`,
			patchedKey: "Description", patchedVal: "cc api",
			verify: ccVerifyApigatewayRestAPI,
		},
		{
			name:       "apigateway_rest_api_security_policy",
			typeName:   "AWS::ApiGateway::RestApi",
			desired:    `{"Name":"cc-api-sp","EndpointAccessMode":"BASIC"}`,
			patch:      `[{"op":"add","path":"/SecurityPolicy","value":"TLS_1_2"}]`,
			patchedKey: "SecurityPolicy", patchedVal: "TLS_1_2",
			wantKeys: []string{"RestApiId", "RootResourceId", "EndpointAccessMode"},
			verify:   ccVerifyApigatewayRestAPISecurityPolicy,
		},
		{
			name:     "apigateway_rest_api_version",
			typeName: "AWS::ApiGateway::RestApi",
			desired:  `{"Name":"cc-api-ver","Version":"v7"}`,
			patch:    `[{"op":"add","path":"/Description","value":"cc api"}]`,
			wantKeys: []string{"Version"}, patchedKey: "Version", patchedVal: "v7",
			verify: ccVerifyApigatewayRestAPIVersion,
		},
	}
}

func ccVerifyS3Bucket(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true }).HeadBucket(
		t.Context(), &s3.HeadBucketInput{Bucket: aws.String(id)},
	)
	assert.Equal(t, present, err == nil)
}

func ccVerifySqsQueue(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := sqs.NewFromConfig(fx.cfg).GetQueueAttributes(t.Context(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(id),
	})
	assert.Equal(t, present, err == nil)
}

func ccVerifyDynamodbTable(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := dynamodb.NewFromConfig(fx.cfg).DescribeTable(t.Context(), &dynamodb.DescribeTableInput{
		TableName: aws.String(id),
	})
	assert.Equal(t, present, err == nil)
}

func ccVerifyLogGroup(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	out, err := cloudwatchlogs.NewFromConfig(fx.cfg).DescribeLogGroups(
		t.Context(), &cloudwatchlogs.DescribeLogGroupsInput{LogGroupNamePrefix: aws.String(id)},
	)
	require.NoError(t, err)
	assert.Equal(t, present, len(out.LogGroups) == 1)
}

func ccVerifySnsTopic(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := sns.NewFromConfig(fx.cfg).GetTopicAttributes(t.Context(), &sns.GetTopicAttributesInput{
		TopicArn: aws.String(id),
	})
	assert.Equal(t, present, err == nil)
}

func ccVerifyIamRole(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := iam.NewFromConfig(fx.cfg).GetRole(t.Context(), &iam.GetRoleInput{RoleName: aws.String(id)})
	assert.Equal(t, present, err == nil)
}

func ccVerifyKmsKey(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	out, err := kms.NewFromConfig(fx.cfg).
		DescribeKey(t.Context(), &kms.DescribeKeyInput{KeyId: aws.String(id)})
	require.NoError(t, err)
	assert.Equal(t, present, out.KeyMetadata.KeyState != kmstypes.KeyStatePendingDeletion)
}

func ccVerifySecret(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := secretsmanager.NewFromConfig(fx.cfg).DescribeSecret(
		t.Context(), &secretsmanager.DescribeSecretInput{SecretId: aws.String(id)},
	)
	assert.Equal(t, present, err == nil)
}

func ccVerifySsmParameter(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := ssm.NewFromConfig(fx.cfg).
		GetParameter(t.Context(), &ssm.GetParameterInput{Name: aws.String(id)})
	assert.Equal(t, present, err == nil)
}

func ccVerifyEcrRepository(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := ecr.NewFromConfig(fx.cfg).DescribeRepositories(t.Context(), &ecr.DescribeRepositoriesInput{
		RepositoryNames: []string{id},
	})
	assert.Equal(t, present, err == nil)
}

func ccVerifyKinesisStream(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := kinesis.NewFromConfig(fx.cfg).DescribeStreamSummary(
		t.Context(), &kinesis.DescribeStreamSummaryInput{StreamName: aws.String(id)},
	)
	assert.Equal(t, present, err == nil)
}

func ccVerifyEventBus(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := eventbridge.NewFromConfig(fx.cfg).DescribeEventBus(
		t.Context(), &eventbridge.DescribeEventBusInput{Name: aws.String(id)},
	)
	assert.Equal(t, present, err == nil)
}

func ccVerifyLambdaFunction(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := lambda.NewFromConfig(fx.cfg).GetFunction(t.Context(), &lambda.GetFunctionInput{
		FunctionName: aws.String(id),
	})
	assert.Equal(t, present, err == nil)
}

func ccVerifyStateMachine(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := sfn.NewFromConfig(fx.cfg).DescribeStateMachine(t.Context(), &sfn.DescribeStateMachineInput{
		StateMachineArn: aws.String(id),
	})
	assert.Equal(t, present, err == nil)
}

func ccVerifyEc2Vpc(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	out, err := ec2.NewFromConfig(fx.cfg).
		DescribeVpcs(t.Context(), &ec2.DescribeVpcsInput{VpcIds: []string{id}})
	assert.Equal(t, present, err == nil && len(out.Vpcs) == 1)
}

func ccVerifyEc2Subnet(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	out, err := ec2.NewFromConfig(fx.cfg).
		DescribeSubnets(t.Context(), &ec2.DescribeSubnetsInput{SubnetIds: []string{id}})
	assert.Equal(t, present, err == nil && len(out.Subnets) == 1)
}

func ccVerifyEc2SecurityGroup(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	out, err := ec2.NewFromConfig(fx.cfg).DescribeSecurityGroups(
		t.Context(), &ec2.DescribeSecurityGroupsInput{GroupIds: []string{id}},
	)
	assert.Equal(t, present, err == nil && len(out.SecurityGroups) == 1)
}

func ccVerifyEcsCluster(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	out, err := ecs.NewFromConfig(fx.cfg).
		DescribeClusters(t.Context(), &ecs.DescribeClustersInput{Clusters: []string{id}})
	require.NoError(t, err)
	assert.Equal(t, present, len(out.Clusters) == 1 && aws.ToString(out.Clusters[0].Status) == "ACTIVE")
}

func ccVerifyElbv2TargetGroup(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	out, err := elbv2.NewFromConfig(fx.cfg).DescribeTargetGroups(
		t.Context(), &elbv2.DescribeTargetGroupsInput{TargetGroupArns: []string{id}},
	)
	assert.Equal(t, present, err == nil && len(out.TargetGroups) == 1)
}

func ccVerifyRoute53HostedZone(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := route53.NewFromConfig(fx.cfg).
		GetHostedZone(t.Context(), &route53.GetHostedZoneInput{Id: aws.String(id)})
	assert.Equal(t, present, err == nil)
}

func ccVerifyCloudwatchAlarm(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	out, err := cloudwatch.NewFromConfig(fx.cfg).DescribeAlarms(
		t.Context(), &cloudwatch.DescribeAlarmsInput{AlarmNames: []string{id}},
	)
	require.NoError(t, err)
	assert.Equal(t, present, len(out.MetricAlarms) == 1)
}

func ccVerifyCognitoUserPool(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := cognitoidentityprovider.NewFromConfig(fx.cfg).DescribeUserPool(
		t.Context(), &cognitoidentityprovider.DescribeUserPoolInput{UserPoolId: aws.String(id)},
	)
	assert.Equal(t, present, err == nil)
}

func ccVerifyApigatewayRestAPI(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := apigateway.NewFromConfig(fx.cfg).
		GetRestApi(t.Context(), &apigateway.GetRestApiInput{RestApiId: aws.String(id)})
	assert.Equal(t, present, err == nil)
}

func ccVerifyEfsFileSystem(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	out, err := efs.NewFromConfig(fx.cfg).
		DescribeFileSystems(t.Context(), &efs.DescribeFileSystemsInput{FileSystemId: aws.String(id)})
	assert.Equal(t, present, err == nil && len(out.FileSystems) == 1)
}

func ccVerifyGlueDatabase(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := glue.NewFromConfig(fx.cfg).
		GetDatabase(t.Context(), &glue.GetDatabaseInput{Name: aws.String(id)})
	assert.Equal(t, present, err == nil)
}

func ccVerifyEcsClusterServiceConnect(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	out, err := ecs.NewFromConfig(fx.cfg).
		DescribeClusters(t.Context(), &ecs.DescribeClustersInput{Clusters: []string{id}})
	require.NoError(t, err)
	assert.Equal(t, present, len(out.Clusters) == 1 && aws.ToString(out.Clusters[0].Status) == "ACTIVE")
}

func ccVerifyRoute53PrivateZoneVpcs(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := route53.NewFromConfig(fx.cfg).
		GetHostedZone(t.Context(), &route53.GetHostedZoneInput{Id: aws.String(id)})
	assert.Equal(t, present, err == nil)
}

func ccVerifyApigatewayRestAPISecurityPolicy(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := apigateway.NewFromConfig(fx.cfg).
		GetRestApi(t.Context(), &apigateway.GetRestApiInput{RestApiId: aws.String(id)})
	assert.Equal(t, present, err == nil)
}

func ccVerifyCognitoUserPoolAttributeUpdateSettings(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	_, err := cognitoidentityprovider.NewFromConfig(fx.cfg).DescribeUserPool(
		t.Context(), &cognitoidentityprovider.DescribeUserPoolInput{UserPoolId: aws.String(id)},
	)
	assert.Equal(t, present, err == nil)
}

func ccVerifyEfsFileSystemProtection(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	out, err := efs.NewFromConfig(fx.cfg).
		DescribeFileSystems(t.Context(), &efs.DescribeFileSystemsInput{FileSystemId: aws.String(id)})
	assert.Equal(t, present, err == nil && len(out.FileSystems) == 1)
}

func ccVerifyElbv2TargetGroupTargetsAttributes(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	if !present {
		return
	}

	c := elbv2.NewFromConfig(fx.cfg)
	health, err := c.DescribeTargetHealth(t.Context(), &elbv2.DescribeTargetHealthInput{
		TargetGroupArn: aws.String(id),
	})
	require.NoError(t, err)
	require.Len(t, health.TargetHealthDescriptions, 1)
	assert.Equal(t, "10.0.0.5", aws.ToString(health.TargetHealthDescriptions[0].Target.Id))

	groups, err := c.DescribeTargetGroups(t.Context(), &elbv2.DescribeTargetGroupsInput{
		TargetGroupArns: []string{id},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 8443, aws.ToInt32(groups.TargetGroups[0].TargetControlPort))
}

func ccVerifyPatchedElbv2TargetGroupTargetsAttributes(t *testing.T, fx *sfnFixture, id string) {
	t.Helper()

	attrs, err := elbv2.NewFromConfig(fx.cfg).DescribeTargetGroupAttributes(
		t.Context(), &elbv2.DescribeTargetGroupAttributesInput{TargetGroupArn: aws.String(id)},
	)
	require.NoError(t, err)

	got := map[string]string{}
	for _, a := range attrs.Attributes {
		got[aws.ToString(a.Key)] = aws.ToString(a.Value)
	}

	assert.Equal(t, "60", got["deregistration_delay.timeout_seconds"])
}

func ccVerifyRoute53HostedZoneFeaturesQueryLogging(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	if !present {
		return
	}

	c := route53.NewFromConfig(fx.cfg)
	z, err := c.GetHostedZone(t.Context(), &route53.GetHostedZoneInput{Id: aws.String(id)})
	require.NoError(t, err)
	require.NotNil(t, z.HostedZone.Features)
	assert.Equal(t, "ENABLED", string(z.HostedZone.Features.AcceleratedRecoveryStatus))

	logs, err := c.ListQueryLoggingConfigs(t.Context(), &route53.ListQueryLoggingConfigsInput{
		HostedZoneId: aws.String(id),
	})
	require.NoError(t, err)
	assert.Len(t, logs.QueryLoggingConfigs, 1)
}

func ccVerifyPatchedRoute53HostedZoneFeaturesQueryLogging(t *testing.T, fx *sfnFixture, id string) {
	t.Helper()

	z, err := route53.NewFromConfig(fx.cfg).GetHostedZone(t.Context(), &route53.GetHostedZoneInput{
		Id: aws.String(id),
	})
	require.NoError(t, err)
	assert.Equal(t, "DISABLED", string(z.HostedZone.Features.AcceleratedRecoveryStatus))
}

func ccVerifyCloudwatchAlarmWindowWarmup(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	if !present {
		return
	}

	out, err := cloudwatch.NewFromConfig(fx.cfg).DescribeAlarms(
		t.Context(), &cloudwatch.DescribeAlarmsInput{AlarmNames: []string{id}},
	)
	require.NoError(t, err)
	require.Len(t, out.MetricAlarms, 1)
	assert.NotNil(t, out.MetricAlarms[0].EvaluationWindow)
	warm := out.MetricAlarms[0].WarmUpConfiguration
	require.NotNil(t, warm)
	assert.EqualValues(t, 5, aws.ToInt32(warm.WarmUpPeriodDurationInMinutes))
}

func ccVerifyCognitoUserPoolMfaWebauthnIssuerKey(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	if !present {
		return
	}

	c := cognitoidentityprovider.NewFromConfig(fx.cfg)
	mfa, err := c.GetUserPoolMfaConfig(t.Context(), &cognitoidentityprovider.GetUserPoolMfaConfigInput{
		UserPoolId: aws.String(id),
	})
	require.NoError(t, err)
	require.NotNil(t, mfa.SoftwareTokenMfaConfiguration)
	assert.True(t, mfa.SoftwareTokenMfaConfiguration.Enabled)
	require.NotNil(t, mfa.WebAuthnConfiguration)
	assert.Equal(t, "auth.example.com", aws.ToString(mfa.WebAuthnConfiguration.RelyingPartyId))

	pool, err := c.DescribeUserPool(t.Context(), &cognitoidentityprovider.DescribeUserPoolInput{
		UserPoolId: aws.String(id),
	})
	require.NoError(t, err)
	require.NotNil(t, pool.UserPool.IssuerConfiguration)
	assert.Equal(t, "UPDATED", string(pool.UserPool.IssuerConfiguration.Type))
	require.NotNil(t, pool.UserPool.KeyConfiguration)
}

func ccVerifyPatchedCognitoUserPoolMfaWebauthnIssuerKey(t *testing.T, fx *sfnFixture, id string) {
	t.Helper()

	mfa, err := cognitoidentityprovider.NewFromConfig(fx.cfg).GetUserPoolMfaConfig(
		t.Context(), &cognitoidentityprovider.GetUserPoolMfaConfigInput{UserPoolId: aws.String(id)},
	)
	require.NoError(t, err)
	assert.Equal(t, "required", string(mfa.WebAuthnConfiguration.UserVerification))
}

func ccVerifyEfsFileSystemReplication(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	if !present {
		return
	}

	out, err := efs.NewFromConfig(fx.cfg).DescribeReplicationConfigurations(
		t.Context(), &efs.DescribeReplicationConfigurationsInput{FileSystemId: aws.String(id)},
	)
	require.NoError(t, err)
	require.Len(t, out.Replications, 1)
	assert.Equal(t, fx.cfg.Region, aws.ToString(out.Replications[0].Destinations[0].Region))
}

func ccVerifyPatchedEfsFileSystemReplication(t *testing.T, fx *sfnFixture, id string) {
	t.Helper()

	out, err := efs.NewFromConfig(fx.cfg).DescribeReplicationConfigurations(
		t.Context(), &efs.DescribeReplicationConfigurationsInput{FileSystemId: aws.String(id)},
	)
	require.NoError(t, err)
	require.Len(t, out.Replications, 1)
	assert.Equal(t, "eu-west-1", aws.ToString(out.Replications[0].Destinations[0].Region))
}

func ccVerifyApigatewayRestAPIVersion(t *testing.T, fx *sfnFixture, id string, present bool) {
	t.Helper()

	out, err := apigateway.NewFromConfig(fx.cfg).
		GetRestApi(t.Context(), &apigateway.GetRestApiInput{RestApiId: aws.String(id)})
	if present {
		require.NoError(t, err)
		assert.Equal(t, "v7", aws.ToString(out.Version))

		return
	}

	assert.Error(t, err)
}
