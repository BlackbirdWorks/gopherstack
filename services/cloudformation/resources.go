package cloudformation

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	acmbackend "github.com/blackbirdworks/gopherstack/services/acm"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
	cloudtrailbackend "github.com/blackbirdworks/gopherstack/services/cloudtrail"
	cloudwatchbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	codepipelinebackend "github.com/blackbirdworks/gopherstack/services/codepipeline"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	elasticachebackend "github.com/blackbirdworks/gopherstack/services/elasticache"
	emrbackend "github.com/blackbirdworks/gopherstack/services/emr"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	gluebackend "github.com/blackbirdworks/gopherstack/services/glue"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	kafkabackend "github.com/blackbirdworks/gopherstack/services/kafka"
	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	pipesbackend "github.com/blackbirdworks/gopherstack/services/pipes"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	route53resolverbackend "github.com/blackbirdworks/gopherstack/services/route53resolver"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
	swfbackend "github.com/blackbirdworks/gopherstack/services/swf"
	transferbackend "github.com/blackbirdworks/gopherstack/services/transfer"
)

// ServiceBackends holds references to all service backends.
type ServiceBackends struct {
	DynamoDB        *ddbbackend.DynamoDBHandler
	S3              *s3backend.S3Handler
	SQS             *sqsbackend.Handler
	SNS             *snsbackend.Handler
	SSM             *ssmbackend.Handler
	KMS             *kmsbackend.Handler
	SecretsManager  *secretsmanagerbackend.Handler
	Lambda          *lambdabackend.Handler
	EventBridge     *ebbackend.Handler
	StepFunctions   *sfnbackend.Handler
	CloudWatchLogs  *cwlogsbackend.Handler
	APIGateway      *apigwbackend.Handler
	IAM             *iambackend.Handler
	EC2             *ec2backend.Handler
	Kinesis         *kinesisbackend.Handler
	CloudWatch      *cloudwatchbackend.Handler
	Route53         *route53backend.Handler
	ElastiCache     *elasticachebackend.Handler
	Scheduler       *schedulerbackend.Handler
	RDS             *rdsbackend.Handler
	ECS             *ecsbackend.Handler
	ECR             *ecrbackend.Handler
	Redshift        *redshiftbackend.Handler
	OpenSearch      *opensearchbackend.Handler
	Firehose        *firehosebackend.Handler
	Route53Resolver *route53resolverbackend.Handler
	SWF             *swfbackend.Handler
	AppSync         *appsyncbackend.Handler
	SES             *sesbackend.Handler
	ACM             *acmbackend.Handler
	CognitoIDP      *cognitoidpbackend.Handler
	// Phase-3 backends
	EKS          *eksbackend.Handler
	EFS          *efsbackend.Handler
	Batch        *batchbackend.Handler
	CloudFront   *cloudfrontbackend.Handler
	Autoscaling  *autoscalingbackend.Handler
	APIGatewayV2 *apigatewayv2backend.Handler
	CodeBuild    *codebuildbackend.Handler
	Glue         *gluebackend.Handler
	DocDB        *docdbbackend.Handler
	Neptune      *neptunebackend.Handler
	Kafka        *kafkabackend.Handler
	Transfer     *transferbackend.Handler
	CloudTrail   *cloudtrailbackend.Handler
	CodePipeline *codepipelinebackend.Handler
	IoT          *iotbackend.Handler
	Pipes        *pipesbackend.Handler
	EMR          *emrbackend.Handler
	AccountID    string
	Region       string
}

// ResourceCreator creates and deletes cloud resources.
type ResourceCreator struct {
	backends *ServiceBackends
}

// NewResourceCreator returns a ResourceCreator backed by the given services.
func NewResourceCreator(backends *ServiceBackends) *ResourceCreator {
	return &ResourceCreator{backends: backends}
}

// Create creates a resource and returns its physical ID.
func (rc *ResourceCreator) Create(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params map[string]string,
	physicalIDs map[string]string,
) (string, error) {
	if rc == nil || rc.backends == nil {
		return logicalID + "-" + uuid.New().String()[:8], nil
	}

	if id, handled, err := rc.createCoreResource(ctx, logicalID, resourceType, props, params, physicalIDs); handled {
		return id, err
	}

	return rc.createExtendedResource(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// createCoreResource handles the original 7 core AWS resource types.
func (rc *ResourceCreator) createCoreResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params map[string]string,
	physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::S3::Bucket":
		id, err := rc.createS3Bucket(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::DynamoDB::Table":
		id, err := rc.createDynamoDBTable(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::SQS::Queue":
		id, err := rc.createSQSQueue(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::SNS::Topic":
		id, err := rc.createSNSTopic(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::SSM::Parameter":
		id, err := rc.createSSMParameter(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::KMS::Key":
		id, err := rc.createKMSKey(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::SecretsManager::Secret":
		id, err := rc.createSecretsManagerSecret(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// createExtendedResource handles extended AWS resource types (Lambda, EventBridge, etc.).
func (rc *ResourceCreator) createExtendedResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params map[string]string,
	physicalIDs map[string]string,
) (string, error) {
	if physID, handled, err := rc.createInfraResource(
		ctx,
		logicalID,
		resourceType,
		props,
		params,
		physicalIDs,
	); handled {
		return physID, err
	}

	return rc.createServiceResource(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// createInfraResource handles Lambda, EventBridge, StepFunctions, Logs, and APIGateway resources.
func (rc *ResourceCreator) createInfraResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if physID, handled, err := rc.createLambdaResources(
		ctx,
		logicalID,
		resourceType,
		props,
		params,
		physicalIDs,
	); handled {
		return physID, true, err
	}

	return rc.createPlatformResources(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// createLambdaResources handles AWS::Lambda::* CloudFormation resource creation.
func (rc *ResourceCreator) createLambdaResources(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::Lambda::Function":
		physID, err := rc.createLambdaFunction(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Lambda::EventSourceMapping":
		physID, err := rc.createLambdaEventSourceMapping(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Lambda::Permission":
		physID, err := rc.createLambdaPermission(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Lambda::Alias":
		physID, err := rc.createLambdaAlias(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Lambda::Version":
		physID, err := rc.createLambdaVersion(logicalID, props, params, physicalIDs)

		return physID, true, err
	default:
		return "", false, nil
	}
}

// createPlatformResources handles EventBridge, StepFunctions, Logs, and APIGateway resource creation.
func (rc *ResourceCreator) createPlatformResources(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::Events::Rule":
		physID, err := rc.createEventBridgeRule(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Events::EventBus":
		physID, err := rc.createEventBus(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::StepFunctions::StateMachine":
		physID, err := rc.createStepFunctionsStateMachine(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Logs::LogGroup":
		physID, err := rc.createCloudWatchLogGroup(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ApiGateway::RestApi":
		physID, err := rc.createAPIGatewayRestAPI(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ApiGateway::Resource":
		physID, err := rc.createAPIGatewayResource(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ApiGateway::Method":
		physID, err := rc.createAPIGatewayMethod(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ApiGateway::Deployment":
		physID, err := rc.createAPIGatewayDeployment(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ApiGateway::Stage":
		physID, err := rc.createAPIGatewayStage(logicalID, props, params, physicalIDs)

		return physID, true, err
	default:
		return "", false, nil
	}
}

// createServiceResource handles IAM, EC2, Kinesis, CloudWatch, Route53, ElastiCache,
// SNS/SQS/S3 policies, and Scheduler resources.
func (rc *ResourceCreator) createServiceResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if physID, handled, err := rc.createIAMEC2Resource(logicalID, resourceType, props, params, physicalIDs); handled {
		return physID, err
	}

	return rc.createDataPlatformResource(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// createIAMEC2Resource handles IAM and EC2 CloudFormation resource creation.
func (rc *ResourceCreator) createIAMEC2Resource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::IAM::Role":
		physID, err := rc.createIAMRole(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::IAM::Policy":
		physID, err := rc.createIAMPolicy(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::IAM::ManagedPolicy":
		physID, err := rc.createIAMManagedPolicy(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::IAM::InstanceProfile":
		physID, err := rc.createIAMInstanceProfile(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EC2::SecurityGroup":
		physID, err := rc.createEC2SecurityGroup(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EC2::VPC":
		physID, err := rc.createEC2VPC(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EC2::Subnet":
		physID, err := rc.createEC2Subnet(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EC2::InternetGateway":
		physID, err := rc.createEC2InternetGateway(logicalID)

		return physID, true, err
	case "AWS::EC2::RouteTable":
		physID, err := rc.createEC2RouteTable(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EC2::Route":
		physID, err := rc.createEC2Route(logicalID, props, params, physicalIDs)

		return physID, true, err
	default:
		return "", false, nil
	}
}

// createDataPlatformResource handles Kinesis, CloudWatch, Route53, ElastiCache,
// SNS/SQS/S3 policies, and Scheduler CloudFormation resource creation.
func (rc *ResourceCreator) createDataPlatformResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	switch resourceType {
	case "AWS::Kinesis::Stream":
		return rc.createKinesisStream(logicalID, props, params, physicalIDs)
	case "AWS::CloudWatch::Alarm":
		return rc.createCloudWatchAlarm(logicalID, props, params, physicalIDs)
	case "AWS::CloudWatch::CompositeAlarm":
		return rc.createCloudWatchCompositeAlarm(logicalID, props, params, physicalIDs)
	case "AWS::Route53::HostedZone":
		return rc.createRoute53HostedZone(logicalID, props, params, physicalIDs)
	case "AWS::Route53::RecordSet":
		return rc.createRoute53RecordSet(logicalID, props, params, physicalIDs)
	case "AWS::Route53::HealthCheck":
		return rc.createRoute53HealthCheck(logicalID, props, params, physicalIDs)
	case "AWS::ElastiCache::CacheCluster":
		return rc.createElastiCacheCacheCluster(logicalID, props, params, physicalIDs)
	case "AWS::ElastiCache::ReplicationGroup":
		return rc.createElastiCacheReplicationGroup(logicalID, props, params, physicalIDs)
	case "AWS::ElastiCache::SubnetGroup":
		return rc.createElastiCacheSubnetGroup(logicalID, props, params, physicalIDs)
	case "AWS::SNS::Subscription":
		return rc.createSNSSubscription(logicalID, props, params, physicalIDs)
	case "AWS::SQS::QueuePolicy":
		return rc.createSQSQueuePolicy(logicalID, props, params, physicalIDs)
	case "AWS::S3::BucketPolicy":
		return rc.createS3BucketPolicy(ctx, logicalID, props, params, physicalIDs)
	case "AWS::Scheduler::Schedule":
		return rc.createSchedulerSchedule(logicalID, props, params, physicalIDs)
	default:
		return rc.createNewServiceResource(ctx, logicalID, resourceType, props, params, physicalIDs)
	}
}

// createNewServiceResource handles RDS, ECS, ECR, Redshift, OpenSearch, Firehose,
// Route53Resolver, SWF, AppSync, SES, ACM, Cognito, and EC2 extended resources.
func (rc *ResourceCreator) createNewServiceResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if physID, handled, err := rc.createRDSResource(logicalID, resourceType, props, params, physicalIDs); handled {
		return physID, err
	}

	if physID, handled, err := rc.createContainerResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); handled {
		return physID, err
	}

	return rc.createMiscServiceResource(logicalID, resourceType, props, params, physicalIDs)
}

// createRDSResource handles AWS::RDS::* resource creation.
func (rc *ResourceCreator) createRDSResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::RDS::DBInstance":
		physID, err := rc.createRDSDBInstance(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::RDS::DBSubnetGroup":
		physID, err := rc.createRDSDBSubnetGroup(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::RDS::DBParameterGroup":
		physID, err := rc.createRDSDBParameterGroup(logicalID, props, params, physicalIDs)

		return physID, true, err
	default:
		return "", false, nil
	}
}

// createContainerResource handles AWS::ECS::*, AWS::ECR::*, and AWS::Lambda::Layer* resource creation.
func (rc *ResourceCreator) createContainerResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::ECS::Cluster":
		physID, err := rc.createECSCluster(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ECS::TaskDefinition":
		physID, err := rc.createECSTaskDefinition(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ECS::Service":
		physID, err := rc.createECSService(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ECR::Repository":
		physID, err := rc.createECRRepository(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Lambda::LayerVersion":
		physID, err := rc.createLambdaLayerVersion(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Lambda::LayerVersionPermission":
		physID, err := rc.createLambdaLayerVersionPermission(logicalID, props, params, physicalIDs)

		return physID, true, err
	default:
		return "", false, nil
	}
}

// createMiscServiceResource handles Redshift, OpenSearch, Firehose, Route53Resolver, SWF, AppSync,
// SES, ACM, Cognito, extended EC2, and phase-3 resource creation.
func (rc *ResourceCreator) createMiscServiceResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if physID, ok, err := rc.createMiscLegacyResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return physID, err
	}

	if physID, ok, err := rc.createPhase3ComputeResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return physID, err
	}

	return rc.createPhase3DataResource(logicalID, resourceType, props, params, physicalIDs)
}

// createMiscLegacyResource handles Redshift, OpenSearch, Firehose, Route53Resolver, SWF, AppSync,
// SES, ACM, Cognito, and EC2 NatGateway/EIP resource creation.
func (rc *ResourceCreator) createMiscLegacyResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::Redshift::Cluster":
		physID, err := rc.createRedshiftCluster(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::OpenSearch::Domain":
		physID, err := rc.createOpenSearchDomain(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Firehose::DeliveryStream":
		physID, err := rc.createFirehoseDeliveryStream(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Route53Resolver::ResolverEndpoint":
		physID, err := rc.createRoute53ResolverEndpoint(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Route53Resolver::ResolverRule":
		physID, err := rc.createRoute53ResolverRule(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::SWF::Domain":
		physID, err := rc.createSWFDomain(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::AppSync::GraphQLApi":
		physID, err := rc.createAppSyncGraphQLAPI(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::SES::EmailIdentity":
		physID, err := rc.createSESEmailIdentity(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ACM::Certificate":
		physID, err := rc.createACMCertificate(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Cognito::UserPool":
		physID, err := rc.createCognitoUserPool(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Cognito::UserPoolClient":
		physID, err := rc.createCognitoUserPoolClient(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EC2::NatGateway":
		physID, err := rc.createEC2NatGateway(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EC2::EIP":
		physID, err := rc.createEC2EIP(logicalID)

		return physID, true, err
	default:
		return "", false, nil
	}
}

// createPhase3ComputeResource handles EKS, EFS, Batch, CloudFront, AutoScaling,
// ApiGatewayV2, CodeBuild, and Glue resource creation.
func (rc *ResourceCreator) createPhase3ComputeResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if physID, ok, err := rc.createPhase3InfraResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return physID, true, err
	}

	return rc.createPhase3AppServiceResource(logicalID, resourceType, props, params, physicalIDs)
}

// createPhase3InfraResource handles EKS, EFS, Batch, and CloudFront resource creation.
func (rc *ResourceCreator) createPhase3InfraResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::EKS::Cluster":
		physID, err := rc.createEKSCluster(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EKS::Nodegroup":
		physID, err := rc.createEKSNodegroup(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EFS::FileSystem":
		physID, err := rc.createEFSFileSystem(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EFS::MountTarget":
		physID, err := rc.createEFSMountTarget(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Batch::ComputeEnvironment":
		physID, err := rc.createBatchComputeEnvironment(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Batch::JobQueue":
		physID, err := rc.createBatchJobQueue(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Batch::JobDefinition":
		physID, err := rc.createBatchJobDefinition(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::CloudFront::Distribution":
		physID, err := rc.createCloudFrontDistribution(logicalID, props, params, physicalIDs)

		return physID, true, err
	default:
		return "", false, nil
	}
}

// createPhase3AppServiceResource handles AutoScaling, ApiGatewayV2, CodeBuild, and Glue resource creation.
func (rc *ResourceCreator) createPhase3AppServiceResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::AutoScaling::AutoScalingGroup":
		physID, err := rc.createAutoScalingGroup(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::AutoScaling::LaunchConfiguration":
		physID, err := rc.createLaunchConfiguration(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ApiGatewayV2::Api":
		physID, err := rc.createAPIGatewayV2API(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ApiGatewayV2::Stage":
		physID, err := rc.createAPIGatewayV2Stage(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::CodeBuild::Project":
		physID, err := rc.createCodeBuildProject(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Glue::Database":
		physID, err := rc.createGlueDatabase(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Glue::Job":
		physID, err := rc.createGlueJob(logicalID, props, params, physicalIDs)

		return physID, true, err
	default:
		return "", false, nil
	}
}

// createPhase3DataResource handles DocDB, Neptune, MSK, Transfer, CloudTrail,
// CodePipeline, IoT, Pipes, EMR, and CloudWatch Dashboard resource creation.
func (rc *ResourceCreator) createPhase3DataResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	switch resourceType {
	case "AWS::DocDB::DBCluster":
		return rc.createDocDBCluster(logicalID, props, params, physicalIDs)
	case "AWS::DocDB::DBInstance":
		return rc.createDocDBInstance(logicalID, props, params, physicalIDs)
	case "AWS::Neptune::DBCluster":
		return rc.createNeptuneCluster(logicalID, props, params, physicalIDs)
	case "AWS::Neptune::DBInstance":
		return rc.createNeptuneInstance(logicalID, props, params, physicalIDs)
	case "AWS::MSK::Cluster":
		return rc.createMSKCluster(logicalID, props, params, physicalIDs)
	case "AWS::Transfer::Server":
		return rc.createTransferServer(logicalID, props, params, physicalIDs)
	case "AWS::CloudTrail::Trail":
		return rc.createCloudTrailTrail(logicalID, props, params, physicalIDs)
	case "AWS::CodePipeline::Pipeline":
		return rc.createCodePipelinePipeline(logicalID, props, params, physicalIDs)
	case "AWS::IoT::Thing":
		return rc.createIoTThing(logicalID, props, params, physicalIDs)
	case "AWS::IoT::TopicRule":
		return rc.createIoTTopicRule(logicalID, props, params, physicalIDs)
	case "AWS::Pipes::Pipe":
		return rc.createPipesPipe(logicalID, props, params, physicalIDs)
	case "AWS::EMR::Cluster":
		return rc.createEMRCluster(logicalID, props, params, physicalIDs)
	case "AWS::CloudWatch::Dashboard":
		return rc.createCloudWatchDashboard(logicalID, props, params, physicalIDs)
	default:
		return logicalID + "-stub", nil
	}
}

// Delete deletes a resource by type and physical ID.
func (rc *ResourceCreator) Delete(
	ctx context.Context,
	resourceType, physicalID string,
	_ map[string]any,
) error {
	if rc == nil || rc.backends == nil {
		return nil
	}

	if handled, err := rc.deleteCoreResource(ctx, resourceType, physicalID); handled {
		return err
	}

	return rc.deleteExtendedResource(ctx, resourceType, physicalID)
}

// deleteCoreResource handles deletion of the original 7 core AWS resource types.
func (rc *ResourceCreator) deleteCoreResource(ctx context.Context, resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::S3::Bucket":
		return true, rc.deleteS3Bucket(ctx, physicalID)
	case "AWS::DynamoDB::Table":
		return true, rc.deleteDynamoDBTable(ctx, physicalID)
	case "AWS::SQS::Queue":
		return true, rc.deleteSQSQueue(ctx, physicalID)
	case "AWS::SNS::Topic":
		return true, rc.deleteSNSTopic(ctx, physicalID)
	case "AWS::SSM::Parameter":
		return true, rc.deleteSSMParameter(ctx, physicalID)
	case "AWS::KMS::Key":
		return true, rc.deleteKMSKey(ctx, physicalID)
	case "AWS::SecretsManager::Secret":
		return true, rc.deleteSecretsManagerSecret(ctx, physicalID)
	default:
		return false, nil
	}
}

// deleteExtendedResource handles deletion of extended AWS resource types (Lambda, EventBridge, etc.).
func (rc *ResourceCreator) deleteExtendedResource(ctx context.Context, resourceType, physicalID string) error {
	if handled, err := rc.deleteInfraResource(ctx, resourceType, physicalID); handled {
		return err
	}

	return rc.deleteServiceResource(ctx, resourceType, physicalID)
}

// deleteInfraResource handles Lambda, EventBridge, StepFunctions, Logs, and APIGateway deletions.
func (rc *ResourceCreator) deleteInfraResource(ctx context.Context, resourceType, physicalID string) (bool, error) {
	if handled, err := rc.deleteLambdaResource(resourceType, physicalID); handled {
		return true, err
	}

	return rc.deletePlatformResource(ctx, resourceType, physicalID)
}

// deleteLambdaResource handles Lambda and Lambda-adjacent resource deletions.
func (rc *ResourceCreator) deleteLambdaResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::Lambda::Function":
		return true, rc.deleteLambdaFunction(physicalID)
	case "AWS::Lambda::EventSourceMapping":
		return true, rc.deleteLambdaEventSourceMapping(physicalID)
	case "AWS::Lambda::Permission":
		return true, rc.deleteLambdaPermission(physicalID)
	case "AWS::Lambda::Alias":
		return true, rc.deleteLambdaAlias(physicalID)
	case "AWS::Lambda::Version":
		return true, nil // versions are immutable; deletion is a no-op
	default:
		return false, nil
	}
}

// deletePlatformResource handles EventBridge, StepFunctions, Logs, and APIGateway deletions.
func (rc *ResourceCreator) deletePlatformResource(ctx context.Context, resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::Events::Rule":
		return true, rc.deleteEventBridgeRule(ctx, physicalID)
	case "AWS::Events::EventBus":
		return true, rc.deleteEventBus(physicalID)
	case "AWS::StepFunctions::StateMachine":
		return true, rc.deleteStepFunctionsStateMachine(ctx, physicalID)
	case "AWS::Logs::LogGroup":
		return true, rc.deleteCloudWatchLogGroup(physicalID)
	case "AWS::ApiGateway::RestApi":
		return true, rc.deleteAPIGatewayRestAPI(ctx, physicalID)
	case "AWS::ApiGateway::Resource":
		return true, rc.deleteAPIGatewayResource(physicalID)
	case "AWS::ApiGateway::Method":
		return true, rc.deleteAPIGatewayMethod(physicalID)
	case "AWS::ApiGateway::Deployment":
		return true, rc.deleteAPIGatewayDeployment(physicalID)
	case "AWS::ApiGateway::Stage":
		return true, rc.deleteAPIGatewayStage(physicalID)
	default:
		return false, nil
	}
}

// deleteServiceResource handles IAM, EC2, Kinesis, CloudWatch, Route53, ElastiCache,
// SNS/SQS/S3 policies, and Scheduler resource deletions.
func (rc *ResourceCreator) deleteServiceResource(ctx context.Context, resourceType, physicalID string) error {
	if handled, err := rc.deleteIAMEC2Resource(resourceType, physicalID); handled {
		return err
	}

	return rc.deleteDataPlatformResource(ctx, resourceType, physicalID)
}

// deleteIAMEC2Resource handles IAM and EC2 resource deletions.
func (rc *ResourceCreator) deleteIAMEC2Resource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::IAM::Role":
		return true, rc.deleteIAMRole(physicalID)
	case "AWS::IAM::Policy", "AWS::IAM::ManagedPolicy":
		return true, rc.deleteIAMPolicy(physicalID)
	case "AWS::IAM::InstanceProfile":
		return true, rc.deleteIAMInstanceProfile(physicalID)
	case "AWS::EC2::SecurityGroup":
		return true, rc.deleteEC2SecurityGroup(physicalID)
	case "AWS::EC2::VPC":
		return true, rc.deleteEC2VPC(physicalID)
	case "AWS::EC2::Subnet":
		return true, rc.deleteEC2Subnet(physicalID)
	case "AWS::EC2::InternetGateway":
		return true, rc.deleteEC2InternetGateway(physicalID)
	case "AWS::EC2::RouteTable":
		return true, rc.deleteEC2RouteTable(physicalID)
	case "AWS::EC2::Route":
		return true, nil // routes are deleted with their route table
	default:
		return false, nil
	}
}

// deleteDataPlatformResource handles Kinesis, CloudWatch, Route53, ElastiCache,
// SNS/SQS/S3 policies, and Scheduler resource deletions.
func (rc *ResourceCreator) deleteDataPlatformResource(ctx context.Context, resourceType, physicalID string) error {
	switch resourceType {
	case "AWS::Kinesis::Stream":
		return rc.deleteKinesisStream(physicalID)
	case "AWS::CloudWatch::Alarm", "AWS::CloudWatch::CompositeAlarm":
		return rc.deleteCloudWatchAlarm(physicalID)
	case "AWS::Route53::HostedZone":
		return rc.deleteRoute53HostedZone(physicalID)
	case "AWS::Route53::RecordSet":
		return nil // record sets are deleted with the hosted zone
	case "AWS::Route53::HealthCheck":
		return rc.deleteRoute53HealthCheck(physicalID)
	case "AWS::ElastiCache::CacheCluster":
		return rc.deleteElastiCacheCacheCluster(ctx, physicalID)
	case "AWS::ElastiCache::ReplicationGroup":
		return rc.deleteElastiCacheReplicationGroup(ctx, physicalID)
	case "AWS::ElastiCache::SubnetGroup":
		return rc.deleteElastiCacheSubnetGroup(physicalID)
	case "AWS::SNS::Subscription":
		return rc.deleteSNSSubscription(physicalID)
	case "AWS::SQS::QueuePolicy":
		return nil // queue policies are soft resources; deletion is a no-op
	case "AWS::S3::BucketPolicy":
		return rc.deleteS3BucketPolicy(ctx, physicalID)
	case "AWS::Scheduler::Schedule":
		return rc.deleteSchedulerSchedule(physicalID)
	default:
		return rc.deleteNewServiceResource(physicalID, resourceType)
	}
}

// deleteNewServiceResource handles RDS, ECS, ECR, Redshift, OpenSearch, Firehose,
// Route53Resolver, SWF, AppSync, SES, ACM, Cognito, extended EC2, and phase-3 resource deletions.
func (rc *ResourceCreator) deleteNewServiceResource(physicalID, resourceType string) error {
	if handled, err := rc.deleteComputeStorageResource(physicalID, resourceType); handled {
		return err
	}

	if handled, err := rc.deletePhase3ComputeResource(physicalID, resourceType); handled {
		return err
	}

	return rc.deleteAppNetworkResource(physicalID, resourceType)
}

// deleteComputeStorageResource handles RDS, ECS, ECR, Lambda layer, Redshift, and OpenSearch deletions.
func (rc *ResourceCreator) deleteComputeStorageResource(physicalID, resourceType string) (bool, error) {
	switch resourceType {
	case "AWS::RDS::DBInstance":
		return true, rc.deleteRDSDBInstance(physicalID)
	case "AWS::RDS::DBSubnetGroup":
		return true, rc.deleteRDSDBSubnetGroup(physicalID)
	case "AWS::RDS::DBParameterGroup":
		return true, rc.deleteRDSDBParameterGroup(physicalID)
	case "AWS::ECS::Cluster":
		return true, rc.deleteECSCluster(physicalID)
	case "AWS::ECS::TaskDefinition":
		return true, rc.deleteECSTaskDefinition(physicalID)
	case "AWS::ECS::Service":
		return true, rc.deleteECSService(physicalID)
	case "AWS::ECR::Repository":
		return true, rc.deleteECRRepository(physicalID)
	case "AWS::Lambda::LayerVersion":
		return true, rc.deleteLambdaLayerVersion(physicalID)
	case "AWS::Lambda::LayerVersionPermission":
		return true, rc.deleteLambdaLayerVersionPermission(physicalID)
	case "AWS::Redshift::Cluster":
		return true, rc.deleteRedshiftCluster(physicalID)
	case "AWS::OpenSearch::Domain":
		return true, rc.deleteOpenSearchDomain(physicalID)
	default:
		return false, nil
	}
}

// deleteAppNetworkResource handles Firehose, Route53Resolver, SWF, AppSync, SES, ACM,
// Cognito, extended EC2, and phase-3 data/managed service resource deletions.
func (rc *ResourceCreator) deleteAppNetworkResource(physicalID, resourceType string) error {
	switch resourceType {
	case "AWS::Firehose::DeliveryStream":
		return rc.deleteFirehoseDeliveryStream(physicalID)
	case "AWS::Route53Resolver::ResolverEndpoint":
		return rc.deleteRoute53ResolverEndpoint(physicalID)
	case "AWS::Route53Resolver::ResolverRule":
		return rc.deleteRoute53ResolverRule(physicalID)
	case "AWS::SWF::Domain":
		return rc.deleteSWFDomain(physicalID)
	case "AWS::AppSync::GraphQLApi":
		return rc.deleteAppSyncGraphQLAPI(physicalID)
	case "AWS::SES::EmailIdentity":
		return rc.deleteSESEmailIdentity(physicalID)
	case "AWS::ACM::Certificate":
		return rc.deleteACMCertificate(physicalID)
	case "AWS::Cognito::UserPool":
		return rc.deleteCognitoUserPool(physicalID)
	case "AWS::Cognito::UserPoolClient":
		return rc.deleteCognitoUserPoolClient(physicalID)
	case "AWS::EC2::NatGateway":
		return rc.deleteEC2NatGateway(physicalID)
	case "AWS::EC2::EIP":
		return rc.deleteEC2EIP(physicalID)
	default:
		return rc.deletePhase3DataResource(physicalID, resourceType)
	}
}

func resolve(v any, params, physicalIDs map[string]string) string {
	return ResolveValue(v, params, physicalIDs)
}

func strProp(props map[string]any, key string, params, physicalIDs map[string]string) string {
	return resolve(props[key], params, physicalIDs)
}

func (rc *ResourceCreator) createS3Bucket(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.S3 == nil {
		return logicalID + "-stub", nil
	}
	bucketName := strProp(props, "BucketName", params, physicalIDs)
	if bucketName == "" {
		bucketName = strings.ToLower(logicalID) + "-" + uuid.New().String()[:8]
	}
	_, err := rc.backends.S3.Backend.CreateBucket(ctx, &awss3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to create S3 bucket %s: %w", bucketName, err)
	}

	return bucketName, nil
}

func (rc *ResourceCreator) deleteS3Bucket(ctx context.Context, physicalID string) error {
	if rc.backends.S3 == nil {
		return nil
	}
	_, err := rc.backends.S3.Backend.DeleteBucket(ctx, &awss3.DeleteBucketInput{
		Bucket: aws.String(physicalID),
	})

	return err
}

func (rc *ResourceCreator) createDynamoDBTable(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.DynamoDB == nil {
		return logicalID + "-stub", nil
	}
	tableName := strProp(props, "TableName", params, physicalIDs)
	if tableName == "" {
		tableName = logicalID
	}
	attrDefs := parseDDBAttributeDefinitions(props, params, physicalIDs)
	keySchema := parseDDBKeySchema(props, params, physicalIDs)
	billingMode := strProp(props, "BillingMode", params, physicalIDs)
	input := &awsddb.CreateTableInput{
		TableName:            aws.String(tableName),
		AttributeDefinitions: attrDefs,
		KeySchema:            keySchema,
	}
	if billingMode == "PAY_PER_REQUEST" {
		input.BillingMode = ddbtypes.BillingModePayPerRequest
	} else {
		input.ProvisionedThroughput = parseDDBProvisionedThroughput(props)
	}
	_, err := rc.backends.DynamoDB.Backend.CreateTable(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to create DynamoDB table %s: %w", tableName, err)
	}

	return tableName, nil
}

func (rc *ResourceCreator) deleteDynamoDBTable(ctx context.Context, physicalID string) error {
	if rc.backends.DynamoDB == nil {
		return nil
	}
	_, err := rc.backends.DynamoDB.Backend.DeleteTable(ctx, &awsddb.DeleteTableInput{
		TableName: aws.String(physicalID),
	})

	return err
}

func parseDDBAttributeDefinitions(
	props map[string]any,
	params, physicalIDs map[string]string,
) []ddbtypes.AttributeDefinition {
	rawList, _ := props["AttributeDefinitions"].([]any)
	defs := make([]ddbtypes.AttributeDefinition, 0, len(rawList))
	for _, item := range rawList {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := resolve(m["AttributeName"], params, physicalIDs)
		attrType := resolve(m["AttributeType"], params, physicalIDs)
		var at ddbtypes.ScalarAttributeType
		switch attrType {
		case "N":
			at = ddbtypes.ScalarAttributeTypeN
		case "B":
			at = ddbtypes.ScalarAttributeTypeB
		default:
			at = ddbtypes.ScalarAttributeTypeS
		}
		defs = append(defs, ddbtypes.AttributeDefinition{
			AttributeName: aws.String(name),
			AttributeType: at,
		})
	}

	return defs
}

func parseDDBKeySchema(props map[string]any, params, physicalIDs map[string]string) []ddbtypes.KeySchemaElement {
	rawList, _ := props["KeySchema"].([]any)
	schema := make([]ddbtypes.KeySchemaElement, 0, len(rawList))
	for _, item := range rawList {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := resolve(m["AttributeName"], params, physicalIDs)
		keyType := resolve(m["KeyType"], params, physicalIDs)
		var kt ddbtypes.KeyType
		switch strings.ToUpper(keyType) {
		case "RANGE":
			kt = ddbtypes.KeyTypeRange
		default:
			kt = ddbtypes.KeyTypeHash
		}
		schema = append(schema, ddbtypes.KeySchemaElement{
			AttributeName: aws.String(name),
			KeyType:       kt,
		})
	}

	return schema
}

const (
	defaultCapacityUnits     = int64(5)
	kmsMinDeletionWindowDays = 7
	boolTrue                 = "true"
)

func parseDDBProvisionedThroughput(props map[string]any) *ddbtypes.ProvisionedThroughput {
	pt, _ := props["ProvisionedThroughput"].(map[string]any)
	rcu := defaultCapacityUnits
	wcu := defaultCapacityUnits
	if pt != nil {
		if v, ok := pt["ReadCapacityUnits"].(float64); ok {
			rcu = int64(v)
		}
		if v, ok := pt["WriteCapacityUnits"].(float64); ok {
			wcu = int64(v)
		}
	}

	return &ddbtypes.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(rcu),
		WriteCapacityUnits: aws.Int64(wcu),
	}
}

func (rc *ResourceCreator) createSQSQueue(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SQS == nil {
		return logicalID + "-stub", nil
	}
	queueName := strProp(props, "QueueName", params, physicalIDs)
	if queueName == "" {
		queueName = logicalID
	}
	attrs := map[string]string{}
	if vt := strProp(props, "VisibilityTimeout", params, physicalIDs); vt != "" {
		attrs["VisibilityTimeout"] = vt
	}
	if mrt := strProp(props, "MessageRetentionPeriod", params, physicalIDs); mrt != "" {
		attrs["MessageRetentionPeriod"] = mrt
	}
	if isFIFO, _ := props["FifoQueue"].(bool); isFIFO {
		queueName = strings.TrimSuffix(queueName, ".fifo") + ".fifo"
		attrs["FifoQueue"] = boolTrue
	}
	out, err := rc.backends.SQS.Backend.CreateQueue(&sqsbackend.CreateQueueInput{
		QueueName:  queueName,
		Attributes: attrs,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create SQS queue %s: %w", queueName, err)
	}

	return out.QueueURL, nil
}

func (rc *ResourceCreator) deleteSQSQueue(_ context.Context, physicalID string) error {
	if rc.backends.SQS == nil {
		return nil
	}

	return rc.backends.SQS.Backend.DeleteQueue(&sqsbackend.DeleteQueueInput{QueueURL: physicalID})
}

func (rc *ResourceCreator) createSNSTopic(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SNS == nil {
		return logicalID + "-stub", nil
	}
	topicName := strProp(props, "TopicName", params, physicalIDs)
	if topicName == "" {
		topicName = logicalID
	}
	attrs := map[string]string{}
	if isFIFO, _ := props["FifoTopic"].(bool); isFIFO {
		attrs["FifoTopic"] = boolTrue
	}
	topic, err := rc.backends.SNS.Backend.CreateTopic(topicName, attrs)
	if err != nil {
		return "", fmt.Errorf("failed to create SNS topic %s: %w", topicName, err)
	}

	return topic.TopicArn, nil
}

func (rc *ResourceCreator) deleteSNSTopic(_ context.Context, physicalID string) error {
	if rc.backends.SNS == nil {
		return nil
	}

	return rc.backends.SNS.Backend.DeleteTopic(physicalID)
}

func (rc *ResourceCreator) createSSMParameter(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SSM == nil {
		return logicalID + "-stub", nil
	}
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = "/" + logicalID
	}
	paramType := strProp(props, "Type", params, physicalIDs)
	if paramType == "" {
		paramType = "String"
	}
	value := strProp(props, "Value", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)
	_, err := rc.backends.SSM.Backend.PutParameter(&ssmbackend.PutParameterInput{
		Name:        name,
		Type:        paramType,
		Value:       value,
		Description: description,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create SSM parameter %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteSSMParameter(_ context.Context, physicalID string) error {
	if rc.backends.SSM == nil {
		return nil
	}
	_, err := rc.backends.SSM.Backend.DeleteParameter(&ssmbackend.DeleteParameterInput{Name: physicalID})

	return err
}

func (rc *ResourceCreator) createKMSKey(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.KMS == nil {
		return logicalID + "-stub", nil
	}
	description := strProp(props, "Description", params, physicalIDs)
	out, err := rc.backends.KMS.Backend.CreateKey(&kmsbackend.CreateKeyInput{
		Description: description,
		KeyUsage:    "ENCRYPT_DECRYPT",
	})
	if err != nil {
		return "", fmt.Errorf("failed to create KMS key: %w", err)
	}

	return out.KeyMetadata.KeyID, nil
}

func (rc *ResourceCreator) deleteKMSKey(_ context.Context, physicalID string) error {
	if rc.backends.KMS == nil {
		return nil
	}
	_, err := rc.backends.KMS.Backend.ScheduleKeyDeletion(&kmsbackend.ScheduleKeyDeletionInput{
		KeyID:               physicalID,
		PendingWindowInDays: kmsMinDeletionWindowDays,
	})

	return err
}

func (rc *ResourceCreator) createSecretsManagerSecret(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.SecretsManager == nil {
		return logicalID + "-stub", nil
	}
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}
	description := strProp(props, "Description", params, physicalIDs)
	secretString := strProp(props, "SecretString", params, physicalIDs)
	out, err := rc.backends.SecretsManager.Backend.CreateSecret(&secretsmanagerbackend.CreateSecretInput{
		Name:         name,
		Description:  description,
		SecretString: secretString,
	})
	if err != nil {
		return "", fmt.Errorf("failed to create secret %s: %w", name, err)
	}

	return out.ARN, nil
}

func (rc *ResourceCreator) deleteSecretsManagerSecret(_ context.Context, physicalID string) error {
	if rc.backends.SecretsManager == nil {
		return nil
	}
	_, err := rc.backends.SecretsManager.Backend.DeleteSecret(&secretsmanagerbackend.DeleteSecretInput{
		SecretID:                   physicalID,
		ForceDeleteWithoutRecovery: true,
	})

	return err
}

// createLambdaFunction creates a Lambda function from CloudFormation template properties.
func (rc *ResourceCreator) createLambdaFunction(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Lambda == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "FunctionName", params, physicalIDs)
	if name == "" {
		name = logicalID + "-" + uuid.New().String()[:8]
	}

	runtime := strProp(props, "Runtime", params, physicalIDs)
	handler := strProp(props, "Handler", params, physicalIDs)
	role := strProp(props, "Role", params, physicalIDs)

	fn := &lambdabackend.FunctionConfiguration{
		FunctionName: name,
		Runtime:      runtime,
		Handler:      handler,
		Role:         role,
	}

	if err := rc.backends.Lambda.Backend.CreateFunction(fn); err != nil {
		return "", fmt.Errorf("create Lambda function: %w", err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteLambdaFunction(name string) error {
	if rc.backends.Lambda == nil {
		return nil
	}

	return rc.backends.Lambda.Backend.DeleteFunction(name)
}

// createEventBridgeRule creates an EventBridge rule from CloudFormation template properties.
func (rc *ResourceCreator) createEventBridgeRule(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EventBridge == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	eventBusName := strProp(props, "EventBusName", params, physicalIDs)
	if eventBusName == "" {
		eventBusName = "default"
	}

	pattern := strProp(props, "EventPattern", params, physicalIDs)
	schedExpr := strProp(props, "ScheduleExpression", params, physicalIDs)
	state := strProp(props, "State", params, physicalIDs)
	if state == "" {
		state = "ENABLED"
	}

	input := ebbackend.PutRuleInput{
		Name:               name,
		EventBusName:       eventBusName,
		EventPattern:       pattern,
		ScheduleExpression: schedExpr,
		State:              state,
	}

	rule, err := rc.backends.EventBridge.Backend.PutRule(input)
	if err != nil {
		return "", fmt.Errorf("create EventBridge rule: %w", err)
	}

	return rule.Arn, nil
}

func (rc *ResourceCreator) deleteEventBridgeRule(_ context.Context, physicalID string) error {
	if rc.backends.EventBridge == nil {
		return nil
	}
	// physicalID is the rule ARN; extract name from it
	parts := strings.Split(physicalID, "/")
	name := parts[len(parts)-1]

	return rc.backends.EventBridge.Backend.DeleteRule(name, "default")
}

// createStepFunctionsStateMachine creates a Step Functions state machine.
func (rc *ResourceCreator) createStepFunctionsStateMachine(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.StepFunctions == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "StateMachineName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	definition := strProp(props, "DefinitionString", params, physicalIDs)
	roleArn := strProp(props, "RoleArn", params, physicalIDs)
	smType := strProp(props, "StateMachineType", params, physicalIDs)
	if smType == "" {
		smType = "STANDARD"
	}

	sm, err := rc.backends.StepFunctions.Backend.CreateStateMachine(name, definition, roleArn, smType)
	if err != nil {
		return "", fmt.Errorf("create StepFunctions state machine: %w", err)
	}

	return sm.StateMachineArn, nil
}

func (rc *ResourceCreator) deleteStepFunctionsStateMachine(_ context.Context, arn string) error {
	if rc.backends.StepFunctions == nil {
		return nil
	}

	return rc.backends.StepFunctions.Backend.DeleteStateMachine(arn)
}

// createCloudWatchLogGroup creates a CloudWatch Logs log group.
func (rc *ResourceCreator) createCloudWatchLogGroup(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatchLogs == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "LogGroupName", params, physicalIDs)
	if name == "" {
		name = "/aws/cfn/" + logicalID
	}

	_, err := rc.backends.CloudWatchLogs.Backend.CreateLogGroup(name)
	if err != nil {
		return "", fmt.Errorf("create CloudWatch Logs log group: %w", err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteCloudWatchLogGroup(name string) error {
	if rc.backends.CloudWatchLogs == nil {
		return nil
	}

	return rc.backends.CloudWatchLogs.Backend.DeleteLogGroup(name)
}

// createAPIGatewayRestAPI creates an API Gateway REST API.
func (rc *ResourceCreator) createAPIGatewayRestAPI(
	_ context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	description := strProp(props, "Description", params, physicalIDs)

	api, err := rc.backends.APIGateway.Backend.CreateRestAPI(name, description, nil)
	if err != nil {
		return "", fmt.Errorf("create API Gateway REST API: %w", err)
	}

	return api.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayRestAPI(_ context.Context, apiID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteRestAPI(apiID)
}

// NewDynamicRefResolver returns a DynamicRefResolver backed by the SSM and SecretsManager
// handlers in the given ServiceBackends. Returns nil when backends is nil.
func NewDynamicRefResolver(backends *ServiceBackends) DynamicRefResolver {
	if backends == nil {
		return nil
	}

	return &serviceBackendsResolver{
		ssm: backends.SSM,
		sm:  backends.SecretsManager,
	}
}

// serviceBackendsResolver implements DynamicRefResolver using real service backends.
type serviceBackendsResolver struct {
	ssm *ssmbackend.Handler
	sm  *secretsmanagerbackend.Handler
}

// ResolveSSMParameter retrieves a plain-text (String / StringList) SSM parameter.
func (r *serviceBackendsResolver) ResolveSSMParameter(name string) (string, error) {
	if r.ssm == nil {
		return "", fmt.Errorf("%w: SSM backend is not available", ErrDynamicRefFailed)
	}

	out, err := r.ssm.Backend.GetParameter(&ssmbackend.GetParameterInput{Name: name})
	if err != nil {
		return "", err
	}

	return out.Parameter.Value, nil
}

// ResolveSSMSecureParameter retrieves a SecureString SSM parameter with decryption.
func (r *serviceBackendsResolver) ResolveSSMSecureParameter(name string) (string, error) {
	if r.ssm == nil {
		return "", fmt.Errorf("%w: SSM backend is not available", ErrDynamicRefFailed)
	}

	out, err := r.ssm.Backend.GetParameter(&ssmbackend.GetParameterInput{Name: name, WithDecryption: true})
	if err != nil {
		return "", err
	}

	return out.Parameter.Value, nil
}

// ResolveSecret retrieves a Secrets Manager secret value.
// When jsonKey is non-empty the secret string is parsed as JSON and the key is extracted.
func (r *serviceBackendsResolver) ResolveSecret(secretID, jsonKey string) (string, error) {
	if r.sm == nil {
		return "", fmt.Errorf("%w: SecretsManager backend is not available", ErrDynamicRefFailed)
	}

	out, err := r.sm.Backend.GetSecretValue(&secretsmanagerbackend.GetSecretValueInput{SecretID: secretID})
	if err != nil {
		return "", err
	}

	if jsonKey == "" {
		return out.SecretString, nil
	}

	return resolveJSONKey(out.SecretString, jsonKey)
}
