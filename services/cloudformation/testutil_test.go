package cloudformation_test

import (
	"testing"

	acmbackend "github.com/blackbirdworks/gopherstack/services/acm"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	appautoscalingbackend "github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	cloudwatchbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	elasticachebackend "github.com/blackbirdworks/gopherstack/services/elasticache"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	route53resolverbackend "github.com/blackbirdworks/gopherstack/services/route53resolver"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
	smbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
	swfbackend "github.com/blackbirdworks/gopherstack/services/swf"

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

// newExtendedServiceBackends creates a ServiceBackends with all backends including extended types.
func newExtendedServiceBackends() *cloudformation.ServiceBackends {
	b := newServiceBackends()
	b.EventBridge = ebbackend.NewHandler(
		ebbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"),
	)
	b.StepFunctions = sfnbackend.NewHandler(
		sfnbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"),
	)
	b.CloudWatchLogs = cwlogsbackend.NewHandler(
		cwlogsbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"),
	)
	b.APIGateway = apigwbackend.NewHandler(apigwbackend.NewInMemoryBackend())
	b.IAM = iambackend.NewHandler(iambackend.NewInMemoryBackendWithConfig("000000000000"))
	b.EC2 = ec2backend.NewHandler(ec2backend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.Kinesis = kinesisbackend.NewHandler(
		kinesisbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"),
	)
	b.CloudWatch = cloudwatchbackend.NewHandler(
		cloudwatchbackend.NewInMemoryBackendWithConfig("000000000000", "us-east-1"),
	)
	b.Route53 = route53backend.NewHandler(route53backend.NewInMemoryBackend())
	b.ElastiCache = elasticachebackend.NewHandler(
		elasticachebackend.NewInMemoryBackend("", "000000000000", "us-east-1", nil),
	)
	b.Scheduler = schedulerbackend.NewHandler(
		schedulerbackend.NewInMemoryBackend("000000000000", "us-east-1"),
	)

	return b
}

// newAdditionalServiceBackends creates a ServiceBackends with all phase 2 backends (RDS, ECS, etc.).
func newAdditionalServiceBackends() *cloudformation.ServiceBackends {
	b := newExtendedServiceBackends()
	b.RDS = rdsbackend.NewHandler(rdsbackend.NewInMemoryBackend("000000000000", "us-east-1"))
	b.ECS = ecsbackend.NewHandler(
		ecsbackend.NewInMemoryBackend("000000000000", "us-east-1", nil),
	)
	b.ECR = ecrbackend.NewHandler(
		ecrbackend.NewInMemoryBackend("000000000000", "us-east-1", ""), nil,
	)
	b.Redshift = redshiftbackend.NewHandler(
		redshiftbackend.NewInMemoryBackend("000000000000", "us-east-1"),
	)
	b.OpenSearch = opensearchbackend.NewHandler(
		opensearchbackend.NewInMemoryBackend("000000000000", "us-east-1"),
	)
	b.Firehose = firehosebackend.NewHandler(
		firehosebackend.NewInMemoryBackend("000000000000", "us-east-1"),
	)
	b.Route53Resolver = route53resolverbackend.NewHandler(
		route53resolverbackend.NewInMemoryBackend("000000000000", "us-east-1"),
	)
	b.SWF = swfbackend.NewHandler(swfbackend.NewInMemoryBackend())
	b.AppSync = appsyncbackend.NewHandler(
		appsyncbackend.NewInMemoryBackend("000000000000", "us-east-1", ""),
	)
	b.SES = sesbackend.NewHandler(sesbackend.NewInMemoryBackend())
	b.ACM = acmbackend.NewHandler(
		acmbackend.NewInMemoryBackend("000000000000", "us-east-1"),
	)
	b.CognitoIDP = cognitoidpbackend.NewHandler(
		cognitoidpbackend.NewInMemoryBackend("000000000000", "us-east-1", ""), "us-east-1",
	)

	return b
}

// newLambdaServiceBackends creates a ServiceBackends with a real Lambda backend.
func newLambdaServiceBackends() *cloudformation.ServiceBackends {
	b := newExtendedServiceBackends()
	lambdaBk := lambdabackend.NewInMemoryBackend(
		nil,
		nil,
		lambdabackend.DefaultSettings(),
		"000000000000",
		"us-east-1",
	)
	b.Lambda = lambdabackend.NewHandler(lambdaBk)

	return b
}

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

func (m *mockBackendsProvider) GetDynamoDBHandler() service.Registerable { return m.ddb }

func (m *mockBackendsProvider) GetS3Handler() service.Registerable { return m.s3h }

func (m *mockBackendsProvider) GetSQSHandler() service.Registerable { return m.sqs }

func (m *mockBackendsProvider) GetSNSHandler() service.Registerable { return m.sns }

func (m *mockBackendsProvider) GetSSMHandler() service.Registerable { return m.ssm }

func (m *mockBackendsProvider) GetKMSHandler() service.Registerable { return m.kms }

func (m *mockBackendsProvider) GetSecretsManagerHandler() service.Registerable { return m.sm }

func (m *mockBackendsProvider) GetLambdaHandler() service.Registerable { return nil }

func (m *mockBackendsProvider) GetEventBridgeHandler() service.Registerable { return nil }

func (m *mockBackendsProvider) GetStepFunctionsHandler() service.Registerable { return nil }

func (m *mockBackendsProvider) GetCloudWatchLogsHandler() service.Registerable { return nil }

func (m *mockBackendsProvider) GetAPIGatewayHandler() service.Registerable { return nil }

func (m *mockBackendsProvider) GetIAMHandler() service.Registerable { return nil }

func (m *mockBackendsProvider) GetEC2Handler() service.Registerable { return nil }

func (m *mockBackendsProvider) GetKinesisHandler() service.Registerable { return nil }

func (m *mockBackendsProvider) GetCloudWatchHandler() service.Registerable { return nil }

func (m *mockBackendsProvider) GetRoute53Handler() service.Registerable { return nil }

func (m *mockBackendsProvider) GetElastiCacheHandler() service.Registerable { return nil }

func (m *mockBackendsProvider) GetSchedulerHandler() service.Registerable { return nil }

func (m *mockBackendsProvider) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig("111111111111", "eu-west-1", 0, 0, false, 0)
}

// mockConfigProvider implements only config.Provider (no BackendsProvider).
type mockConfigProvider struct{}

func (m *mockConfigProvider) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig("222222222222", "ap-southeast-1", 0, 0, false, 0)
}

// newExtraServiceBackends creates a ServiceBackends with all phase-5 backends populated.
func newExtraServiceBackends(t *testing.T) *cloudformation.ServiceBackends {
	t.Helper()

	b := newMoreTypesServiceBackends(t)
	b.AppAutoScaling = appautoscalingbackend.NewHandler(
		appautoscalingbackend.NewInMemoryBackend("000000000000", "us-east-1"),
	)

	return b
}
