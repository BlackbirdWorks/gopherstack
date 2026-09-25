package cloudformation

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/ctxval"
	acmbackend "github.com/blackbirdworks/gopherstack/services/acm"
	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	athenabackend "github.com/blackbirdworks/gopherstack/services/athena"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	awsconfigbackend "github.com/blackbirdworks/gopherstack/services/awsconfig"
	batchbackend "github.com/blackbirdworks/gopherstack/services/batch"
	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
	cloudtrailbackend "github.com/blackbirdworks/gopherstack/services/cloudtrail"
	cloudwatchbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	codeartifactbackend "github.com/blackbirdworks/gopherstack/services/codeartifact"
	codebuildbackend "github.com/blackbirdworks/gopherstack/services/codebuild"
	codedeploybackend "github.com/blackbirdworks/gopherstack/services/codedeploy"
	codepipelinebackend "github.com/blackbirdworks/gopherstack/services/codepipeline"
	cognitoidentitybackend "github.com/blackbirdworks/gopherstack/services/cognitoidentity"
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
	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	servicediscoverybackend "github.com/blackbirdworks/gopherstack/services/servicediscovery"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
	swfbackend "github.com/blackbirdworks/gopherstack/services/swf"
	transferbackend "github.com/blackbirdworks/gopherstack/services/transfer"

	appautoscalingbackend "github.com/blackbirdworks/gopherstack/services/applicationautoscaling"
	backupbackend "github.com/blackbirdworks/gopherstack/services/backup"
	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	"github.com/blackbirdworks/gopherstack/services/memorydb"
	wafv2backend "github.com/blackbirdworks/gopherstack/services/wafv2"
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
	CognitoIdentity *cognitoidentitybackend.Handler
	// Phase-3 backends
	EKS            *eksbackend.Handler
	EFS            *efsbackend.Handler
	Batch          *batchbackend.Handler
	CloudFront     *cloudfrontbackend.Handler
	Autoscaling    *autoscalingbackend.Handler
	APIGatewayV2   *apigatewayv2backend.Handler
	CodeBuild      *codebuildbackend.Handler
	Glue           *gluebackend.Handler
	DocDB          *docdbbackend.Handler
	Neptune        *neptunebackend.Handler
	Kafka          *kafkabackend.Handler
	Transfer       *transferbackend.Handler
	CloudTrail     *cloudtrailbackend.Handler
	CodePipeline   *codepipelinebackend.Handler
	IoT            *iotbackend.Handler
	Pipes          *pipesbackend.Handler
	EMR            *emrbackend.Handler
	MemoryDB       *memorydb.Handler
	BedrockRuntime *bedrockruntime.Handler
	// Phase-4 backends
	ELBv2  *elbv2backend.Handler
	WAFv2  *wafv2backend.Handler
	Backup *backupbackend.Handler
	// Phase-5 backends
	AppAutoScaling *appautoscalingbackend.Handler
	// CFN extensibility
	WaitConditions *WaitConditionStore
	MacroRegistry  *MacroRegistry
	// ResilienceHub is declared as a local interface, not a concrete
	// *resiliencehub.Handler, to avoid an import cycle -- see
	// ResilienceHubBackend's doc comment in resources_resiliencehub.go.
	ResilienceHub    ResilienceHubBackend
	ServiceDiscovery *servicediscoverybackend.Handler
	CodeDeploy       *codedeploybackend.Handler
	AWSConfig        *awsconfigbackend.Handler
	SageMaker        *sagemakerbackend.Handler
	Athena           *athenabackend.Handler
	CodeArtifact     *codeartifactbackend.Handler
	AccountID        string
	Region           string
}

// NestedStackCreator is a callback used to create and delete nested CloudFormation stacks.
type NestedStackCreator interface {
	CreateNestedStack(
		ctx context.Context,
		name, templateURL, templateBody string,
		params []Parameter,
		parentID string,
	) (string, error)
	DeleteNestedStack(ctx context.Context, stackID string) error
}

// parentStackIDKey carries the StackId of the stack currently being
// provisioned, set by provisionResources/createUpdateResource before calling
// ResourceCreator.Create. createNestedStack reads it to populate a nested
// stack's ParentId (gopherstack-pbv1) -- threaded via context rather than a
// new Create parameter because Create's signature is shared by every
// resource type and called directly from ~30 test files.
//
//nolint:gochecknoglobals // ctxval key, not mutable runtime state -- see awsmeta.Key/logger.Key for the same pattern
var parentStackIDKey = ctxval.NewKey[string]("cfnParentStackID")

// ResourceCreator creates and deletes cloud resources.
type ResourceCreator struct {
	backends           *ServiceBackends
	nestedStackCreator NestedStackCreator
	createHook         func(resourceType string) error // used by tests to inject creation errors
	deleteHook         func(resourceType string)       // used by tests to observe deletion calls
}

// NewResourceCreator returns a ResourceCreator backed by the given services.
func NewResourceCreator(backends *ServiceBackends) *ResourceCreator {
	return &ResourceCreator{backends: backends}
}

// resourceCreatorFunc is a per-resource-type create method, expressed as a
// method expression (e.g. (*ResourceCreator).createIoTPolicy) so a family of
// same-shaped create* methods can be dispatched from a lookup table instead
// of a switch -- avoids the family of near-identical "case X: id, err :=
// rc.createX(...); return id, true, err" switches the dupl linter flags once
// two families reach the same case count.
type resourceCreatorFunc func(
	*ResourceCreator, string, map[string]any, map[string]string, map[string]string,
) (string, error)

// dispatchCreate looks up resourceType in fns and calls the matching create
// method, returning handled=false when resourceType isn't in fns.
func dispatchCreate(
	rc *ResourceCreator,
	fns map[string]resourceCreatorFunc,
	resourceType, logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	fn, ok := fns[resourceType]
	if !ok {
		return "", false, nil
	}

	id, err := fn(rc, logicalID, props, params, physicalIDs)

	return id, true, err
}

// WithNestedStackCreator sets the callback used to create/delete nested stacks.
func (rc *ResourceCreator) WithNestedStackCreator(nsc NestedStackCreator) {
	rc.nestedStackCreator = nsc
}

// Create creates a resource and returns its physical ID.
func (rc *ResourceCreator) Create(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params map[string]string,
	physicalIDs map[string]string,
) (string, error) {
	if rc == nil {
		return logicalID + "-" + uuid.New().String()[:8], nil
	}

	if rc.createHook != nil {
		if err := rc.createHook(resourceType); err != nil {
			return "", err
		}
	}

	// Handle nested stacks regardless of whether service backends are configured.
	if resourceType == cfnStackType {
		return rc.createNestedStack(ctx, logicalID, props, params)
	}

	// Handle CFN extensibility types (CustomResource, WaitCondition, Macro).
	if isCFNExtensibilityType(resourceType) {
		if rc.backends == nil {
			return logicalID + "-" + uuid.New().String()[:8], nil
		}

		return rc.createCFNExtensibilityResource(
			ctx,
			logicalID,
			resourceType,
			props,
			params,
			physicalIDs,
		)
	}

	if rc.backends == nil {
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
	case resTypeS3Bucket:
		id, err := rc.createS3Bucket(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeDynamoDBTable:
		id, err := rc.createDynamoDBTable(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSQSQueue:
		id, err := rc.createSQSQueue(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSNSTopic:
		id, err := rc.createSNSTopic(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::SSM::Parameter":
		id, err := rc.createSSMParameter(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeKMSKey:
		id, err := rc.createKMSKey(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSecret:
		id, err := rc.createSecretsManagerSecret(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

// createNestedStack provisions a nested AWS::CloudFormation::Stack resource.
func (rc *ResourceCreator) createNestedStack(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params map[string]string,
) (string, error) {
	if rc.nestedStackCreator == nil {
		// No nested stack creator wired; return a stub physical ID.
		return "arn:aws:cloudformation:us-east-1:000000000000:stack/" + logicalID + "/stub", nil
	}

	templateURL, _ := props["TemplateURL"].(string)
	templateBody, _ := props["TemplateBody"].(string)

	// Extract nested stack parameters from Properties.Parameters map.
	nestedParams := resolveNestedParams(props, params)

	parentID, _ := parentStackIDKey.Get(ctx)

	// Use logicalID as the child stack name.
	return rc.nestedStackCreator.CreateNestedStack(
		ctx,
		logicalID,
		templateURL,
		templateBody,
		nestedParams,
		parentID,
	)
}

// resolveNestedParams extracts CloudFormation stack parameters from a resource property map,
// resolving Ref references against the caller's parameter set.
func resolveNestedParams(props map[string]any, params map[string]string) []Parameter {
	rawParams, ok := props["Parameters"].(map[string]any)
	if !ok {
		return nil
	}
	out := make([]Parameter, 0, len(rawParams))
	for k, v := range rawParams {
		out = append(out, Parameter{ParameterKey: k, ParameterValue: resolveParamValue(v, params)})
	}

	return out
}

// resolveParamValue converts a template parameter value to a string, resolving Ref references.
func resolveParamValue(v any, params map[string]string) string {
	if v == nil {
		return ""
	}
	if ref, ok := v.(map[string]any); ok {
		if refName, isRef := ref["Ref"].(string); isRef {
			if resolved, found := params[refName]; found {
				return resolved
			}

			return refName
		}
	}

	return fmt.Sprintf("%v", v)
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
	case resTypeLambdaFunction:
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
		physID, err := rc.createEventBus(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeStepFunctionsStateMachine:
		physID, err := rc.createStepFunctionsStateMachine(
			ctx,
			logicalID,
			props,
			params,
			physicalIDs,
		)

		return physID, true, err
	case "AWS::StepFunctions::StateMachineVersion":
		physID, err := rc.createSFNStateMachineVersion(props, params, physicalIDs)

		return physID, true, err
	case "AWS::StepFunctions::StateMachineAlias":
		physID, err := rc.createSFNStateMachineAlias(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeLogGroup:
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
	if physID, ok, err := rc.createIAMCoreResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return physID, true, err
	}

	return rc.createEC2CoreResource(logicalID, resourceType, props, params, physicalIDs)
}

// createIAMCoreResource handles AWS::IAM::* resource creation.
func (rc *ResourceCreator) createIAMCoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeIAMRole:
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
	case "AWS::IAM::User":
		physID, err := rc.createIAMUser(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::IAM::Group":
		physID, err := rc.createIAMGroup(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeIAMOIDCProvider:
		physID, err := rc.createIAMOIDCProvider(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeIAMAccessKey:
		physID, err := rc.createIAMAccessKey(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeIAMServiceLinkedRole:
		physID, err := rc.createIAMServiceLinkedRole(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeIAMUserToGroupAddition:
		physID, err := rc.createIAMUserToGroupAddition(logicalID, props, params, physicalIDs)

		return physID, true, err
	default:

		return "", false, nil
	}
}

// createEC2CoreResource handles AWS::EC2::* resource creation.
func (rc *ResourceCreator) createEC2CoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::EC2::SecurityGroup":
		physID, err := rc.createEC2SecurityGroup(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeEC2VPC:
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
	case resTypeEC2Instance:
		physID, err := rc.createEC2Instance(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EC2::VPCGatewayAttachment":
		physID, err := rc.createEC2VPCGatewayAttachment(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EC2::SubnetRouteTableAssociation":
		physID, err := rc.createEC2SubnetRouteTableAssociation(
			logicalID,
			props,
			params,
			physicalIDs,
		)

		return physID, true, err
	case resTypeEC2LaunchTemplate:
		physID, err := rc.createEC2LaunchTemplate(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeEC2VPCEndpoint:
		physID, err := rc.createEC2VPCEndpoint(logicalID, props, params, physicalIDs)

		return physID, true, err
	default:

		return rc.createEC2TGWResource(logicalID, resourceType, props, params, physicalIDs)
	}
}

// createEC2TGWResource handles AWS::EC2::TransitGateway* resource creation
// (split out of createEC2CoreResource to keep its cyclomatic complexity down).
func (rc *ResourceCreator) createEC2TGWResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeEC2TransitGateway:
		physID, err := rc.createEC2TransitGateway(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeEC2TGWAttachment:
		physID, err := rc.createEC2TGWAttachment(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeEC2TGWRouteTable:
		physID, err := rc.createEC2TGWRouteTable(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeEC2TGWRoute:
		physID, err := rc.createEC2TGWRoute(logicalID, props, params, physicalIDs)

		return physID, true, err
	default:

		return rc.createEC2MoreResource(logicalID, resourceType, props, params, physicalIDs)
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

		return rc.createKinesisStream(ctx, logicalID, props, params, physicalIDs)
	case resTypeKinesisStreamConsumer:

		return rc.createKinesisStreamConsumer(ctx, logicalID, props, params, physicalIDs)
	case "AWS::CloudWatch::Alarm":

		return rc.createCloudWatchAlarm(logicalID, props, params, physicalIDs)
	case "AWS::CloudWatch::CompositeAlarm":

		return rc.createCloudWatchCompositeAlarm(logicalID, props, params, physicalIDs)
	case resTypeRoute53HostedZone:

		return rc.createRoute53HostedZone(logicalID, props, params, physicalIDs)
	case resTypeRoute53RecordSet:

		return rc.createRoute53RecordSet(logicalID, props, params, physicalIDs)
	case resTypeRoute53RecordSetGroup:

		return rc.createRoute53RecordSetGroup(logicalID, props, params, physicalIDs)
	case "AWS::Route53::HealthCheck":

		return rc.createRoute53HealthCheck(logicalID, props, params, physicalIDs)
	case "AWS::SNS::Subscription":

		return rc.createSNSSubscription(logicalID, props, params, physicalIDs)
	case "AWS::S3::BucketPolicy":

		return rc.createS3BucketPolicy(ctx, logicalID, props, params, physicalIDs)
	case "AWS::Scheduler::Schedule":

		return rc.createSchedulerSchedule(ctx, logicalID, props, params, physicalIDs)
	default:
		if physID, handled, err := rc.createElastiCacheResource(
			ctx, logicalID, resourceType, props, params, physicalIDs,
		); handled {
			return physID, err
		}

		if physID, handled, err := rc.createQueueTopicPolicyResource(
			logicalID, resourceType, props, params, physicalIDs,
		); handled {
			return physID, err
		}

		return rc.createNewServiceResource(ctx, logicalID, resourceType, props, params, physicalIDs)
	}
}

// createElastiCacheResource handles AWS::ElastiCache::* resource creation
// (split out of createDataPlatformResource to keep its cyclomatic complexity down).
func (rc *ResourceCreator) createElastiCacheResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::ElastiCache::CacheCluster":
		physID, err := rc.createElastiCacheCacheCluster(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ElastiCache::ReplicationGroup":
		physID, err := rc.createElastiCacheReplicationGroup(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ElastiCache::SubnetGroup":
		physID, err := rc.createElastiCacheSubnetGroup(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	default:

		return "", false, nil
	}
}

// createQueueTopicPolicyResource handles SQS/SNS policy resource creation
// (split out of createDataPlatformResource to keep its cyclomatic complexity down).
func (rc *ResourceCreator) createQueueTopicPolicyResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::SQS::QueuePolicy":
		physID, err := rc.createSQSQueuePolicy(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeSQSQueueInlinePolicy:
		physID, err := rc.createSQSQueueInlinePolicy(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeSNSTopicInlinePolicy:
		physID, err := rc.createSNSTopicInlinePolicy(logicalID, props, params, physicalIDs)

		return physID, true, err
	default:

		return "", false, nil
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

	if physID, handled, err := rc.createExtraResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); handled {
		return physID, err
	}

	if physID, handled, err := rc.createSupplementalResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); handled {
		return physID, err
	}

	return rc.createMiscServiceResource(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// createRDSResource handles AWS::RDS::* resource creation.
func (rc *ResourceCreator) createRDSResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeRDSDB:
		physID, err := rc.createRDSDBInstance(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::RDS::DBSubnetGroup":
		physID, err := rc.createRDSDBSubnetGroup(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::RDS::DBParameterGroup":
		physID, err := rc.createRDSDBParameterGroup(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeRDSDBProxy:
		physID, err := rc.createRDSDBProxy(logicalID, props, params, physicalIDs)

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
	case resTypeECSCluster:
		physID, err := rc.createECSCluster(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ECS::TaskDefinition":
		physID, err := rc.createECSTaskDefinition(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ECS::Service":
		physID, err := rc.createECSService(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ECS::CapacityProvider":
		physID, err := rc.createECSCapacityProvider(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ECS::ClusterCapacityProviderAssociations":
		physID, err := rc.createECSClusterCapacityProviderAssociations(props, params, physicalIDs)

		return physID, true, err
	case resTypeECSTaskSet:
		physID, err := rc.createECSTaskSet(props, params, physicalIDs)

		return physID, true, err
	case "AWS::ECS::PrimaryTaskSet":
		physID, err := rc.createECSPrimaryTaskSet(props, params, physicalIDs)

		return physID, true, err
	case "AWS::ECR::Repository":
		physID, err := rc.createECRRepository(ctx, logicalID, props, params, physicalIDs)

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
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if physID, ok, err := rc.createMiscLegacyResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return physID, err
	}

	if physID, ok, err := rc.createPhase3ComputeResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return physID, err
	}

	return rc.createPhase3DataResource(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// createMiscLegacyResource handles Redshift, OpenSearch, Firehose, Route53Resolver, SWF, AppSync,
// SES, ACM, Cognito, and EC2 NatGateway/EIP resource creation.
func (rc *ResourceCreator) createMiscLegacyResource(
	ctx context.Context,
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
	case resTypeFirehoseDeliveryStream, resTypeFirehoseDeliveryStreamAlias:
		// AWS::KinesisFirehose::DeliveryStream is the real CFN type name (the CFN
		// spec has no "AWS::Firehose::DeliveryStream"); the old name is kept as an
		// alias since existing tests/templates in this repo use it.
		physID, err := rc.createFirehoseDeliveryStream(
			ctx,
			logicalID,
			props,
			params,
			physicalIDs,
		)

		return physID, true, err
	case "AWS::Route53Resolver::ResolverEndpoint":
		physID, err := rc.createRoute53ResolverEndpoint(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Route53Resolver::ResolverRule":
		physID, err := rc.createRoute53ResolverRule(ctx, logicalID, props, params, physicalIDs)

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
		physID, err := rc.createACMCertificate(ctx, logicalID, props, params, physicalIDs)

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
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if physID, ok, err := rc.createPhase3InfraResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return physID, true, err
	}

	return rc.createPhase3AppServiceResource(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// createPhase3InfraResource handles EKS, EFS, Batch, and CloudFront resource creation.
func (rc *ResourceCreator) createPhase3InfraResource(
	ctx context.Context,
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
	case resTypeEKSFargateProfile:
		physID, err := rc.createEKSFargateProfile(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeEKSAddon:
		physID, err := rc.createEKSAddon(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeEKSAccessEntry:
		physID, err := rc.createEKSAccessEntry(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeEKSPodIdentityAssociation:
		physID, err := rc.createEKSPodIdentityAssociation(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeEKSIdentityProviderConfig:
		physID, err := rc.createEKSIdentityProviderConfig(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EFS::FileSystem":
		physID, err := rc.createEFSFileSystem(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::EFS::MountTarget":
		physID, err := rc.createEFSMountTarget(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Batch::ComputeEnvironment":
		physID, err := rc.createBatchComputeEnvironment(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Batch::JobQueue":
		physID, err := rc.createBatchJobQueue(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::Batch::JobDefinition":
		physID, err := rc.createBatchJobDefinition(ctx, logicalID, props, params, physicalIDs)

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
	ctx context.Context,
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
	case resTypeASGScalingPolicy:
		physID, err := rc.createASGScalingPolicy(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeASGScheduledActn:
		physID, err := rc.createASGScheduledAction(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeASGLifecycleHook:
		physID, err := rc.createASGLifecycleHook(logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ApiGatewayV2::Api":
		physID, err := rc.createAPIGatewayV2API(ctx, logicalID, props, params, physicalIDs)

		return physID, true, err
	case "AWS::ApiGatewayV2::Stage":
		physID, err := rc.createAPIGatewayV2Stage(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeAPIGatewayV2Integ:
		physID, err := rc.createAPIGatewayV2Integration(logicalID, props, params, physicalIDs)

		return physID, true, err
	case resTypeAPIGatewayV2Route:
		physID, err := rc.createAPIGatewayV2Route(logicalID, props, params, physicalIDs)

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
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	switch resourceType {
	case "AWS::DocDB::DBCluster":

		return rc.createDocDBCluster(ctx, logicalID, props, params, physicalIDs)
	case "AWS::DocDB::DBInstance":

		return rc.createDocDBInstance(ctx, logicalID, props, params, physicalIDs)
	case "AWS::Neptune::DBCluster":

		return rc.createNeptuneCluster(ctx, logicalID, props, params, physicalIDs)
	case "AWS::Neptune::DBInstance":

		return rc.createNeptuneInstance(ctx, logicalID, props, params, physicalIDs)
	case "AWS::MSK::Cluster":

		return rc.createMSKCluster(ctx, logicalID, props, params, physicalIDs)
	case "AWS::Transfer::Server":

		return rc.createTransferServer(logicalID, props, params, physicalIDs)
	case "AWS::CloudTrail::Trail":

		return rc.createCloudTrailTrail(logicalID, props, params, physicalIDs)
	case "AWS::CodePipeline::Pipeline":

		return rc.createCodePipelinePipeline(ctx, logicalID, props, params, physicalIDs)
	case "AWS::IoT::Thing":

		return rc.createIoTThing(logicalID, props, params, physicalIDs)
	case "AWS::IoT::TopicRule":

		return rc.createIoTTopicRule(logicalID, props, params, physicalIDs)
	case "AWS::Pipes::Pipe":

		return rc.createPipesPipe(ctx, logicalID, props, params, physicalIDs)
	case "AWS::EMR::Cluster":

		return rc.createEMRCluster(ctx, logicalID, props, params, physicalIDs)
	case "AWS::CloudWatch::Dashboard":

		return rc.createCloudWatchDashboard(logicalID, props, params, physicalIDs)
	default:

		return rc.createPhase4Resource(ctx, logicalID, resourceType, props, params, physicalIDs)
	}
}

// createELBv2Resource handles ELBv2 resource creation.
func (rc *ResourceCreator) createELBv2Resource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	switch resourceType {
	case resTypeELBv2LB:
		return rc.createELBv2LoadBalancer(logicalID, props, params, physicalIDs)
	case resTypeELBv2TargetGroup:
		return rc.createELBv2TargetGroup(logicalID, props, params, physicalIDs)
	default:
		return rc.createELBv2Listener(logicalID, props, params, physicalIDs)
	}
}

// createPhase4Resource handles ELBv2, WAFv2, Backup, and RDS cluster resource creation.
func (rc *ResourceCreator) createPhase4Resource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	switch resourceType {
	case resTypeELBv2LB,
		resTypeELBv2TargetGroup,
		"AWS::ElasticLoadBalancingV2::Listener":

		return rc.createELBv2Resource(logicalID, resourceType, props, params, physicalIDs)
	case "AWS::WAFv2::WebACL":

		return rc.createWAFv2WebACL(ctx, logicalID, props, params, physicalIDs)
	case "AWS::WAFv2::IPSet":

		return rc.createWAFv2IPSet(ctx, logicalID, props, params, physicalIDs)
	case "AWS::WAFv2::RuleGroup":

		return rc.createWAFv2RuleGroup(ctx, logicalID, props, params, physicalIDs)
	case "AWS::Backup::BackupVault":

		return rc.createBackupVault(logicalID, props, params, physicalIDs)
	case "AWS::Backup::BackupPlan":

		return rc.createBackupPlan(logicalID, props, params, physicalIDs)
	case "AWS::Backup::BackupSelection":

		return rc.createBackupSelection(logicalID, props, params, physicalIDs)
	case "AWS::RDS::DBCluster":

		return rc.createRDSDBCluster(logicalID, props, params, physicalIDs)
	case "AWS::RDS::DBClusterParameterGroup":

		return rc.createRDSDBClusterParameterGroup(logicalID, props, params, physicalIDs)
	default:

		id, handled, err := rc.createExtraResource(
			ctx,
			logicalID,
			resourceType,
			props,
			params,
			physicalIDs,
		)
		if err != nil {
			return "", err
		}
		if !handled {
			id, handled, err = rc.createSupplementalResource(
				ctx,
				logicalID,
				resourceType,
				props,
				params,
				physicalIDs,
			)
			if err != nil {
				return "", err
			}
			if !handled {
				return logicalID + "-stub", nil
			}
		}

		return id, nil
	}
}

// Update sends an Update lifecycle event to CFN extensibility resource types
// (Custom::*, AWS::CloudFormation::CustomResource). For other resource types it is
// a no-op — the backend's updateResources handles them via property overwrite.
func (rc *ResourceCreator) Update(
	ctx context.Context,
	logicalID, resourceType, physicalID string,
	newProps, oldProps map[string]any,
) error {
	if rc == nil {
		return nil
	}

	if !isCFNExtensibilityType(resourceType) {
		return nil
	}

	if rc.backends == nil {
		return nil
	}

	_, err := rc.updateCFNExtensibilityResource(
		ctx,
		logicalID,
		resourceType,
		physicalID,
		newProps,
		oldProps,
	)

	return err
}

// Delete deletes a resource by type and physical ID.
// stackPhysicalIDsSnapshot builds a logicalID -> physicalID map from a
// stack's live resources, letting Delete resolve a {"Ref": "OtherResource"}
// property to the real physical ID (see deleteSDInstance / deleteIAMAccessKey
// / deleteIAMUserToGroupAddition).
func stackPhysicalIDsSnapshot(resources map[string]*StackResource) map[string]string {
	out := make(map[string]string, len(resources))
	for logicalID, res := range resources {
		out[logicalID] = res.PhysicalID
	}

	return out
}

// deleteResolveContext builds the physicalIDs map used to resolve Fn::GetAtt
// (and Fn::Sub ${Logical.Attr}) in a resource's Properties during deletion
// and rollback: stackPhysicalIDsSnapshot's plain logicalID->PhysicalID, each
// live resource's declared Type (from StackResource.Type -- always accurate,
// unlike create-time resolution's physIDResourceTypeKey side channel), the
// account/region/stack name, and the stack's persisted GetAtt attribute
// stash for values not derivable from PhysicalID+Type alone (see
// extractAttrStash / Stack.ResourceAttrs). Without this, delete-time
// property resolution only sees {logicalID: PhysicalID} and a sibling
// Fn::GetAtt in a resource's own Properties silently resolves to that
// resource's physical ID instead of the requested attribute
// (gopherstack-9e44r).
func (b *InMemoryBackend) deleteResolveContext(stack *Stack) map[string]string {
	resources := b.resources[stack.StackID]
	out := stackPhysicalIDsSnapshot(resources)

	for logicalID, res := range resources {
		out[physIDResourceTypeKey(logicalID)] = res.Type
	}

	out[physIDAccountIDKey] = b.accountID
	out[physIDRegionKey] = b.region
	out[physIDStackNameKey] = stack.StackName

	maps.Copy(out, stack.ResourceAttrs)

	return out
}

func (rc *ResourceCreator) Delete(
	ctx context.Context,
	resourceType, physicalID string,
	props map[string]any,
	stackPhysicalIDs map[string]string,
) error {
	if rc == nil {
		return nil
	}

	if rc.deleteHook != nil {
		rc.deleteHook(resourceType)
	}

	// Handle nested stack deletion regardless of service backends.
	if resourceType == cfnStackType {
		if rc.nestedStackCreator != nil {
			return rc.nestedStackCreator.DeleteNestedStack(ctx, physicalID)
		}

		return nil
	}

	// Handle CFN extensibility type deletions.
	if isCFNExtensibilityType(resourceType) {
		if rc.backends == nil {
			return nil
		}

		handled, err := rc.deleteCFNExtensibilityResource(ctx, "", resourceType, physicalID, props)
		if handled {
			return err
		}
	}

	if rc.backends == nil {
		return nil
	}

	// AWS::ServiceDiscovery::Instance needs its own props (ServiceId) that
	// aren't threaded through the prop-less create*Resource chain below --
	// same reason IAM::AccessKey/UserToGroupAddition are special-cased.
	// stackPhysicalIDs (a snapshot of every logical ID -> physical ID in the
	// stack) lets a {"Ref": "OtherResource"} property resolve to the real
	// physical ID here, not just the logical ID literal.
	if resourceType == resTypeSDInstance {
		return rc.deleteSDInstance(props, stackPhysicalIDs, physicalID)
	}

	// AWS::EKS::AccessEntry's Ref is PrincipalArn and
	// AWS::EKS::PodIdentityAssociation's Ref is the association ID -- neither
	// embeds ClusterName, so both need props the same way.
	if handled, err := rc.deletePropsBasedResource(ctx, resourceType, physicalID, props, stackPhysicalIDs); handled {
		return err
	}

	if handled, err := rc.deleteCoreResource(ctx, resourceType, physicalID); handled {
		return err
	}

	return rc.deleteExtendedResource(ctx, resourceType, physicalID, props, stackPhysicalIDs)
}

// deletePropsBasedResource handles deletion for resource types whose delete
// call needs the resource's own CFN properties (not just its physical ID),
// e.g. because a required identifier (WindowId, UserPoolId, RestApiId, ...)
// is a sibling property rather than embedded in the Ref value.
func (rc *ResourceCreator) deletePropsBasedResource(
	ctx context.Context,
	resourceType, physicalID string,
	props map[string]any, stackPhysicalIDs map[string]string,
) (bool, error) {
	switch resourceType {
	case resTypeEKSAccessEntry:
		return true, rc.deleteEKSAccessEntry(props, stackPhysicalIDs, physicalID)
	case resTypeEKSPodIdentityAssociation:
		return true, rc.deleteEKSPodIdentityAssociation(props, stackPhysicalIDs, physicalID)
	case resTypeCognitoUserPoolResourceServer:
		return true, rc.deleteCognitoUserPoolResourceServer(props, stackPhysicalIDs, physicalID)
	case resTypeCognitoUserPoolIdentityProvider:
		return true, rc.deleteCognitoUserPoolIdentityProvider(props, stackPhysicalIDs, physicalID)
	case resTypeSSMMaintenanceWindowTarget:
		return true, rc.deleteSSMMaintenanceWindowTarget(ctx, props, stackPhysicalIDs, physicalID)
	case resTypeSSMMaintenanceWindowTask:
		return true, rc.deleteSSMMaintenanceWindowTask(ctx, props, stackPhysicalIDs, physicalID)
	case resTypeSSMResourcePolicy:
		return true, rc.deleteSSMResourcePolicy(ctx, props, stackPhysicalIDs, physicalID)
	case resTypeCWAnomalyDetector:
		return true, rc.deleteCWAnomalyDetector(props, stackPhysicalIDs)
	case "AWS::ApiGateway::DocumentationPart":
		return true, rc.deleteAPIGatewayDocumentationPart(props, stackPhysicalIDs, physicalID)
	case "AWS::ApiGateway::DocumentationVersion":
		return true, rc.deleteAPIGatewayDocumentationVersion(props, stackPhysicalIDs, physicalID)
	case resTypeConfigStoredQuery:
		return true, rc.deleteConfigStoredQuery(props, stackPhysicalIDs)
	case resTypeCodeArtifactRepository:
		return true, rc.deleteCodeArtifactRepository(ctx, props, stackPhysicalIDs)
	case resTypeCodeArtifactPackageGroup:
		return true, rc.deleteCodeArtifactPackageGroup(ctx, props, stackPhysicalIDs)
	default:
		return rc.deleteMorePropsBasedResource(resourceType, props, stackPhysicalIDs)
	}
}

// deleteCoreResource handles deletion of the original 7 core AWS resource types.
func (rc *ResourceCreator) deleteCoreResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case resTypeS3Bucket:

		return true, rc.deleteS3Bucket(ctx, physicalID)
	case resTypeDynamoDBTable:

		return true, rc.deleteDynamoDBTable(ctx, physicalID)
	case resTypeSQSQueue:

		return true, rc.deleteSQSQueue(ctx, physicalID)
	case resTypeSNSTopic:

		return true, rc.deleteSNSTopic(ctx, physicalID)
	case "AWS::SSM::Parameter":

		return true, rc.deleteSSMParameter(ctx, physicalID)
	case resTypeKMSKey:

		return true, rc.deleteKMSKey(ctx, physicalID)
	case resTypeSecret:

		return true, rc.deleteSecretsManagerSecret(ctx, physicalID)
	default:

		return false, nil
	}
}

// deleteExtendedResource handles deletion of extended AWS resource types (Lambda, EventBridge, etc.).
func (rc *ResourceCreator) deleteExtendedResource(
	ctx context.Context,
	resourceType, physicalID string,
	props map[string]any,
	stackPhysicalIDs map[string]string,
) error {
	if handled, err := rc.deleteInfraResource(ctx, resourceType, physicalID); handled {
		return err
	}

	return rc.deleteServiceResource(ctx, resourceType, physicalID, props, stackPhysicalIDs)
}

// deleteInfraResource handles Lambda, EventBridge, StepFunctions, Logs, and APIGateway deletions.
func (rc *ResourceCreator) deleteInfraResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deleteLambdaResource(resourceType, physicalID); handled {
		return true, err
	}

	return rc.deletePlatformResource(ctx, resourceType, physicalID)
}

// deleteLambdaResource handles Lambda and Lambda-adjacent resource deletions.
func (rc *ResourceCreator) deleteLambdaResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeLambdaFunction:

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
func (rc *ResourceCreator) deletePlatformResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case "AWS::Events::Rule":

		return true, rc.deleteEventBridgeRule(ctx, physicalID)
	case "AWS::Events::EventBus":

		return true, rc.deleteEventBus(ctx, physicalID)
	case resTypeStepFunctionsStateMachine:

		return true, rc.deleteStepFunctionsStateMachine(ctx, physicalID)
	case "AWS::StepFunctions::StateMachineVersion":

		return true, rc.deleteSFNStateMachineVersion(physicalID)
	case "AWS::StepFunctions::StateMachineAlias":

		return true, rc.deleteSFNStateMachineAlias(physicalID)
	case resTypeLogGroup:

		return true, rc.deleteCloudWatchLogGroup(ctx, physicalID)
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
func (rc *ResourceCreator) deleteServiceResource(
	ctx context.Context,
	resourceType, physicalID string,
	props map[string]any,
	stackPhysicalIDs map[string]string,
) error {
	// These types need the resource's props (UserName / GroupName+Users /
	// ApplicationName) that aren't threaded through the prop-less
	// deleteIAMEC2Resource chain.
	switch resourceType {
	case resTypeIAMAccessKey:
		return rc.deleteIAMAccessKey(props, stackPhysicalIDs, physicalID)
	case resTypeIAMUserToGroupAddition:
		return rc.deleteIAMUserToGroupAddition(props, stackPhysicalIDs)
	case resTypeCodeDeployDeploymentGroup:
		return rc.deleteCodeDeployDeploymentGroup(props, stackPhysicalIDs, physicalID)
	}

	if handled, err := rc.deleteIAMEC2Resource(resourceType, physicalID); handled {
		return err
	}

	return rc.deleteDataPlatformResource(ctx, resourceType, physicalID, props)
}

// deleteIAMEC2Resource handles IAM and EC2 resource deletions.
func (rc *ResourceCreator) deleteIAMEC2Resource(resourceType, physicalID string) (bool, error) {
	if handled, err := rc.deleteIAMCoreResource(resourceType, physicalID); handled {
		return true, err
	}

	return rc.deleteEC2CoreResource(resourceType, physicalID)
}

// deleteIAMCoreResource handles AWS::IAM::* resource deletions.
func (rc *ResourceCreator) deleteIAMCoreResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeIAMRole:

		return true, rc.deleteIAMRole(physicalID)
	case "AWS::IAM::Policy", "AWS::IAM::ManagedPolicy":

		return true, rc.deleteIAMPolicy(physicalID)
	case "AWS::IAM::InstanceProfile":

		return true, rc.deleteIAMInstanceProfile(physicalID)
	case "AWS::IAM::User":

		return true, rc.deleteIAMUser(physicalID)
	case "AWS::IAM::Group":

		return true, rc.deleteIAMGroup(physicalID)
	case resTypeIAMOIDCProvider:

		return true, rc.deleteIAMOIDCProvider(physicalID)
	case resTypeIAMServiceLinkedRole:

		return true, rc.deleteIAMServiceLinkedRole(physicalID)
	default:

		return false, nil
	}
}

// deleteEC2CoreResource handles AWS::EC2::* resource deletions.
func (rc *ResourceCreator) deleteEC2CoreResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::EC2::SecurityGroup":

		return true, rc.deleteEC2SecurityGroup(physicalID)
	case resTypeEC2VPC:

		return true, rc.deleteEC2VPC(physicalID)
	case "AWS::EC2::Subnet":

		return true, rc.deleteEC2Subnet(physicalID)
	case "AWS::EC2::InternetGateway":

		return true, rc.deleteEC2InternetGateway(physicalID)
	case "AWS::EC2::RouteTable":

		return true, rc.deleteEC2RouteTable(physicalID)
	case "AWS::EC2::Route":

		return true, nil // routes are deleted with their route table
	case resTypeEC2Instance:

		return true, rc.deleteEC2Instance(physicalID)
	case "AWS::EC2::VPCGatewayAttachment":

		return true, rc.deleteEC2VPCGatewayAttachment(physicalID)
	case "AWS::EC2::SubnetRouteTableAssociation":

		return true, rc.deleteEC2SubnetRouteTableAssociation(physicalID)
	case resTypeEC2LaunchTemplate:

		return true, rc.deleteEC2LaunchTemplate(physicalID)
	case resTypeEC2VPCEndpoint:

		return true, rc.deleteEC2VPCEndpoint(physicalID)
	default:

		return rc.deleteEC2TGWResource(resourceType, physicalID)
	}
}

// deleteEC2TGWResource handles AWS::EC2::TransitGateway* resource deletion
// (split out of deleteEC2CoreResource to keep its cyclomatic complexity down).
func (rc *ResourceCreator) deleteEC2TGWResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeEC2TransitGateway:

		return true, rc.deleteEC2TransitGateway(physicalID)
	case resTypeEC2TGWAttachment:

		return true, rc.deleteEC2TGWAttachment(physicalID)
	case resTypeEC2TGWRouteTable:

		return true, rc.deleteEC2TGWRouteTable(physicalID)
	case resTypeEC2TGWRoute:

		return true, rc.deleteEC2TGWRoute(physicalID)
	default:

		return rc.deleteEC2MoreResource(resourceType, physicalID)
	}
}

// deleteRoute53Resource handles Route53 resource deletions.
func (rc *ResourceCreator) deleteRoute53Resource(resourceType, physicalID string) error {
	switch resourceType {
	case resTypeRoute53HostedZone:
		return rc.deleteRoute53HostedZone(physicalID)
	case resTypeRoute53RecordSet, resTypeRoute53RecordSetGroup:
		return nil // record sets are deleted with the hosted zone
	default:
		return rc.deleteRoute53HealthCheck(physicalID)
	}
}

// deleteDataPlatformResource handles Kinesis, CloudWatch, Route53, ElastiCache,
// SNS/SQS/S3 policies, and Scheduler resource deletions.
func (rc *ResourceCreator) deleteDataPlatformResource(
	ctx context.Context,
	resourceType, physicalID string,
	props map[string]any,
) error {
	if handled, err := rc.deleteKinesisFamilyResource(ctx, resourceType, physicalID); handled {
		return err
	}

	switch resourceType {
	case "AWS::CloudWatch::Alarm", "AWS::CloudWatch::CompositeAlarm":

		return rc.deleteCloudWatchAlarm(physicalID)
	case resTypeRoute53HostedZone, resTypeRoute53RecordSet, resTypeRoute53RecordSetGroup, "AWS::Route53::HealthCheck":

		return rc.deleteRoute53Resource(resourceType, physicalID)
	case "AWS::ElastiCache::CacheCluster":

		return rc.deleteElastiCacheCacheCluster(ctx, physicalID)
	case "AWS::ElastiCache::ReplicationGroup":

		return rc.deleteElastiCacheReplicationGroup(ctx, physicalID)
	case "AWS::ElastiCache::SubnetGroup":

		return rc.deleteElastiCacheSubnetGroup(ctx, physicalID)
	case "AWS::SNS::Subscription":

		return rc.deleteSNSSubscription(physicalID)
	case "AWS::SQS::QueuePolicy", resTypeSQSQueueInlinePolicy:

		return nil // queue policies are soft resources; deletion is a no-op
	case resTypeSNSTopicInlinePolicy:

		return nil // topic policy is an attribute on the topic; removed with the topic
	case "AWS::S3::BucketPolicy":

		return rc.deleteS3BucketPolicy(ctx, physicalID)
	case "AWS::Scheduler::Schedule":

		return rc.deleteSchedulerSchedule(ctx, physicalID)
	default:
		if handled, err := rc.deleteExtraResource(ctx, resourceType, physicalID); handled {
			return err
		}

		if handled, err := rc.deleteSupplementalResource(ctx, resourceType, physicalID); handled {
			return err
		}

		return rc.deleteNewServiceResource(ctx, physicalID, resourceType, props)
	}
}

// deleteNewServiceResource handles RDS, ECS, ECR, Redshift, OpenSearch, Firehose,
// Route53Resolver, SWF, AppSync, SES, ACM, Cognito, extended EC2, and phase-3 resource deletions.
func (rc *ResourceCreator) deleteNewServiceResource(
	ctx context.Context,
	physicalID, resourceType string,
	props map[string]any,
) error {
	if handled, err := rc.deleteComputeStorageResource(ctx, physicalID, resourceType, props); handled {
		return err
	}

	if handled, err := rc.deleteComputePlatformResource(ctx, physicalID, resourceType); handled {
		return err
	}

	return rc.deleteAppNetworkResource(ctx, physicalID, resourceType)
}

// deleteComputeStorageResource handles RDS, ECS, ECR, Lambda layer, Redshift, and OpenSearch deletions.
func (rc *ResourceCreator) deleteComputeStorageResource(
	ctx context.Context,
	physicalID, resourceType string,
	props map[string]any,
) (bool, error) {
	switch resourceType {
	case resTypeRDSDB:

		return true, rc.deleteRDSDBInstance(physicalID)
	case "AWS::RDS::DBSubnetGroup":

		return true, rc.deleteRDSDBSubnetGroup(physicalID)
	case "AWS::RDS::DBParameterGroup":

		return true, rc.deleteRDSDBParameterGroup(physicalID)
	case resTypeRDSDBProxy:

		return true, rc.deleteRDSDBProxy(physicalID)
	case resTypeECSCluster:

		return true, rc.deleteECSCluster(physicalID)
	case "AWS::ECS::TaskDefinition":

		return true, rc.deleteECSTaskDefinition(physicalID)
	case "AWS::ECS::Service":

		return true, rc.deleteECSService(physicalID)
	case "AWS::ECR::Repository":

		return true, rc.deleteECRRepository(ctx, physicalID, props)
	case "AWS::Lambda::LayerVersion":

		return true, rc.deleteLambdaLayerVersion(physicalID)
	case "AWS::Lambda::LayerVersionPermission":

		return true, rc.deleteLambdaLayerVersionPermission(physicalID)
	case "AWS::Redshift::Cluster":

		return true, rc.deleteRedshiftCluster(physicalID)
	case "AWS::OpenSearch::Domain":

		return true, rc.deleteOpenSearchDomain(physicalID)
	}

	return rc.deleteECSDeploymentResource(resourceType, physicalID)
}

// deleteECSDeploymentResource handles AWS::ECS::CapacityProvider,
// ClusterCapacityProviderAssociations, TaskSet, and PrimaryTaskSet deletions
// (split out of deleteComputeStorageResource to keep its cyclomatic complexity down).
func (rc *ResourceCreator) deleteECSDeploymentResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::ECS::CapacityProvider":

		return true, rc.deleteECSCapacityProvider(physicalID)
	case "AWS::ECS::ClusterCapacityProviderAssociations":

		return true, rc.deleteECSClusterCapacityProviderAssociations(physicalID)
	case resTypeECSTaskSet:

		return true, rc.deleteECSTaskSet(physicalID)
	case "AWS::ECS::PrimaryTaskSet":
		// No independent lifecycle to tear down: it only points at a task
		// set the sibling AWS::ECS::TaskSet resource already owns deletion of.
		return true, nil
	default:

		return false, nil
	}
}

// deleteAppNetworkResource handles Firehose, Route53Resolver, SWF, AppSync, SES, ACM,
// Cognito, extended EC2, and phase-3 data/managed service resource deletions.
func (rc *ResourceCreator) deleteAppNetworkResource(ctx context.Context, physicalID, resourceType string) error {
	switch resourceType {
	case resTypeFirehoseDeliveryStream, resTypeFirehoseDeliveryStreamAlias:

		return rc.deleteFirehoseDeliveryStream(ctx, physicalID)
	case "AWS::Route53Resolver::ResolverEndpoint":

		return rc.deleteRoute53ResolverEndpoint(ctx, physicalID)
	case "AWS::Route53Resolver::ResolverRule":

		return rc.deleteRoute53ResolverRule(ctx, physicalID)
	case "AWS::SWF::Domain":

		return rc.deleteSWFDomain(physicalID)
	case "AWS::AppSync::GraphQLApi":

		return rc.deleteAppSyncGraphQLAPI(physicalID)
	case "AWS::SES::EmailIdentity":

		return rc.deleteSESEmailIdentity(physicalID)
	case "AWS::ACM::Certificate":

		return rc.deleteACMCertificate(ctx, physicalID)
	case "AWS::Cognito::UserPool":

		return rc.deleteCognitoUserPool(physicalID)
	case "AWS::Cognito::UserPoolClient":

		return rc.deleteCognitoUserPoolClient(physicalID)
	case "AWS::EC2::NatGateway":

		return rc.deleteEC2NatGateway(physicalID)
	case "AWS::EC2::EIP":

		return rc.deleteEC2EIP(physicalID)
	default:

		return rc.deleteManagedDataResource(ctx, physicalID, resourceType)
	}
}

// createSupplementalResource handles APIGW v1 supplemental, APIGW v2 supplemental,
// Events ApiDestination/EventBusPolicy, KMS ReplicaKey, Cognito
// IdentityPool/Group/Domain, EC2 VPCPeering/NetworkAcl/KeyPair/SGRule/FlowLog,
// ELBv2 ListenerRule, and Lambda EventInvokeConfig/Url resource creation.
// Returns handled=false when resourceType is none of the above.
func (rc *ResourceCreator) createSupplementalResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createAPIGatewayV1SupplementalResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createAPIGatewayV2SupplementalResource(
		ctx,
		logicalID,
		resourceType,
		props,
		params,
		physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createEventsSupplementalResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createKMSSupplementalResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createCognitoSupplementalResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createEC2SupplementalResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createELBv2SupplementalResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createLambdaSupplementalResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createRDSSupplementalResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createSSMMoreResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createCloudWatchMoreResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createAPIGatewayMoreResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createIoTMoreResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createAWSConfigResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}

	return rc.createMoreSupplementalResource(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// createMoreSupplementalResource is createSupplementalResource's overflow
// table for resource type groups added after its own gocognit budget was
// spent: IAM inline policies/certs, ECR, ElastiCache, Neptune, DocDB,
// Backup, Glue, and the CodeBuild/Kinesis/Lambda misc group.
func (rc *ResourceCreator) createMoreSupplementalResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createIAMMoreResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createECRMoreResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createElastiCacheMoreResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createNeptuneMoreResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createDocDBMoreResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createBackupMoreResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createGlueMoreResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createMiscMoreResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createSageMakerResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createAthenaResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createWAFv2AssociationResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createMemoryDBResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}

	return rc.createNewerSupplementalResource(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// createNewerSupplementalResource is createMoreSupplementalResource's own
// overflow table, for resource type groups added once its budget was spent
// too: Lambda CodeSigningConfig, Events Endpoint, Scheduler ScheduleGroup,
// AppSync DomainName/GraphQLSchema/ChannelNamespace, the Route53Resolver
// firewall/query-logging/outpost family, CloudTrail EventDataStore/Channel,
// the CloudWatch Logs delivery/integration/anomaly-detector/scheduled-query
// family, and CodeArtifact.
func (rc *ResourceCreator) createNewerSupplementalResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createLambdaCSCResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createEventsEndpointResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createSchedulerGroupResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createAppSyncMoreResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createRoute53ResolverMoreResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createCloudTrailMoreResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createLogsMoreResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}
	if id, ok, err := rc.createCodeArtifactResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}

	return "", false, nil
}

// deleteSupplementalResource handles deletion for the supplemental resource types
// described in createSupplementalResource.
func (rc *ResourceCreator) deleteSupplementalResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deleteAPIGatewayV1SupplementalResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteAPIGatewayV2SupplementalResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteEventsSupplementalResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteKMSSupplementalResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteCognitoSupplementalResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteEC2SupplementalResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteELBv2SupplementalResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteLambdaSupplementalResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteRDSSupplementalResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteSSMMoreResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteCloudWatchMoreResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteAPIGatewayMoreResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteIoTMoreResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteAWSConfigResource(resourceType, physicalID); handled {
		return true, err
	}

	return rc.deleteMoreSupplementalResource(ctx, resourceType, physicalID)
}

// deleteMoreSupplementalResource is deleteSupplementalResource's overflow
// table, mirroring createMoreSupplementalResource.
func (rc *ResourceCreator) deleteMoreSupplementalResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deleteIAMMoreResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteECRMoreResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteElastiCacheMoreResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteNeptuneMoreResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteDocDBMoreResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteBackupMoreResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteGlueMoreResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteMiscMoreResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteSageMakerResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteAthenaResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteWAFv2AssociationResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteMemoryDBResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	return rc.deleteNewerSupplementalResource(ctx, resourceType, physicalID)
}

// deleteNewerSupplementalResource is deleteMoreSupplementalResource's own
// overflow table, mirroring createNewerSupplementalResource.
func (rc *ResourceCreator) deleteNewerSupplementalResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deleteLambdaCSCResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteEventsEndpointResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteSchedulerGroupResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteAppSyncMoreResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteRoute53ResolverMoreResource(ctx, resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteCloudTrailMoreResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteLogsMoreResource(resourceType, physicalID); handled {
		return true, err
	}
	if handled, err := rc.deleteCodeArtifactResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	return false, nil
}

// deleteComputePlatformResource handles EKS, EFS, Batch, CloudFront, AutoScaling,
// ApiGatewayV2, CodeBuild, and Glue resource deletions.
func (rc *ResourceCreator) deleteComputePlatformResource(
	ctx context.Context,
	physicalID, resourceType string,
) (bool, error) {
	if handled, err := rc.deleteContainerPlatformResource(ctx, physicalID, resourceType); handled {
		return true, err
	}

	return rc.deleteAppPlatformResource(ctx, physicalID, resourceType)
}

// deleteContainerPlatformResource handles EKS, EFS, and Batch deletions.
func (rc *ResourceCreator) deleteContainerPlatformResource(
	ctx context.Context,
	physicalID, resourceType string,
) (bool, error) {
	switch resourceType {
	case "AWS::EKS::Cluster":
		return true, rc.deleteEKSCluster(physicalID)
	case "AWS::EKS::Nodegroup":
		return true, rc.deleteEKSNodegroup(physicalID)
	case resTypeEKSFargateProfile:
		return true, rc.deleteEKSFargateProfile(physicalID)
	case resTypeEKSAddon:
		return true, rc.deleteEKSAddon(physicalID)
	case resTypeEKSIdentityProviderConfig:
		return true, rc.deleteEKSIdentityProviderConfig(physicalID)
	case "AWS::EFS::FileSystem":
		return true, rc.deleteEFSFileSystem(ctx, physicalID)
	case "AWS::EFS::MountTarget":
		return true, rc.deleteEFSMountTarget(ctx, physicalID)
	case "AWS::Batch::ComputeEnvironment":
		return true, rc.deleteBatchComputeEnvironment(ctx, physicalID)
	case "AWS::Batch::JobQueue":
		return true, rc.deleteBatchJobQueue(ctx, physicalID)
	case "AWS::Batch::JobDefinition":
		return true, rc.deleteBatchJobDefinition(ctx, physicalID)
	}

	return false, nil
}

// deleteAppPlatformResource handles CloudFront, AutoScaling, ApiGatewayV2, CodeBuild, and Glue deletions.
func (rc *ResourceCreator) deleteAppPlatformResource(_ context.Context, physicalID, resourceType string) (bool, error) {
	switch resourceType {
	case "AWS::CloudFront::Distribution":
		return true, rc.deleteCloudFrontDistribution(physicalID)
	case "AWS::AutoScaling::AutoScalingGroup":
		return true, rc.deleteAutoScalingGroup(physicalID)
	case "AWS::AutoScaling::LaunchConfiguration":
		return true, rc.deleteLaunchConfiguration(physicalID)
	case resTypeASGScalingPolicy:
		return true, rc.deleteASGScalingPolicy(physicalID)
	case resTypeASGScheduledActn:
		return true, rc.deleteASGScheduledAction(physicalID)
	case resTypeASGLifecycleHook:
		return true, rc.deleteASGLifecycleHook(physicalID)
	case "AWS::ApiGatewayV2::Api":
		return true, rc.deleteAPIGatewayV2API(physicalID)
	case "AWS::ApiGatewayV2::Stage":
		return true, rc.deleteAPIGatewayV2Stage(physicalID)
	case resTypeAPIGatewayV2Integ:
		return true, rc.deleteAPIGatewayV2Integration(physicalID)
	case resTypeAPIGatewayV2Route:
		return true, rc.deleteAPIGatewayV2Route(physicalID)
	case "AWS::CodeBuild::Project":
		return true, rc.deleteCodeBuildProject(physicalID)
	case "AWS::Glue::Database":
		return true, rc.deleteGlueDatabase(physicalID)
	case "AWS::Glue::Job":
		return true, rc.deleteGlueJob(physicalID)
	}

	return false, nil
}

// deleteManagedDataResource handles DocDB, Neptune, MSK, Transfer, CloudTrail,
// CodePipeline, IoT, Pipes, EMR, and CloudWatch Dashboard deletions, falling through
// to deleteNetworkSecurityResource for ELBv2/WAFv2/Backup/RDS-cluster types.
func (rc *ResourceCreator) deleteManagedDataResource(ctx context.Context, physicalID, resourceType string) error {
	switch resourceType {
	case "AWS::DocDB::DBCluster":
		return rc.deleteDocDBCluster(ctx, physicalID)
	case "AWS::DocDB::DBInstance":
		return rc.deleteDocDBInstance(ctx, physicalID)
	case "AWS::Neptune::DBCluster":
		return rc.deleteNeptuneCluster(ctx, physicalID)
	case "AWS::Neptune::DBInstance":
		return rc.deleteNeptuneInstance(ctx, physicalID)
	case "AWS::MSK::Cluster":
		return rc.deleteMSKCluster(ctx, physicalID)
	case "AWS::Transfer::Server":
		return rc.deleteTransferServer(physicalID)
	case "AWS::CloudTrail::Trail":
		return rc.deleteCloudTrailTrail(physicalID)
	case "AWS::CodePipeline::Pipeline":
		return rc.deleteCodePipelinePipeline(ctx, physicalID)
	case "AWS::IoT::Thing":
		return rc.deleteIoTThing(physicalID)
	case "AWS::IoT::TopicRule":
		return rc.deleteIoTTopicRule(physicalID)
	case "AWS::Pipes::Pipe":
		return rc.deletePipesPipe(ctx, physicalID)
	case "AWS::EMR::Cluster":
		return rc.deleteEMRCluster(ctx, physicalID)
	case "AWS::CloudWatch::Dashboard":
		return rc.deleteCloudWatchDashboard(physicalID)
	default:
		return rc.deleteNetworkSecurityResource(ctx, physicalID, resourceType)
	}
}

// deleteNetworkSecurityResource handles ELBv2, WAFv2, Backup, and RDS cluster resource deletions.
func (rc *ResourceCreator) deleteNetworkSecurityResource(ctx context.Context, physicalID, resourceType string) error {
	switch resourceType {
	case resTypeELBv2LB:
		return rc.deleteELBv2LoadBalancer(physicalID)
	case resTypeELBv2TargetGroup:
		return rc.deleteELBv2TargetGroup(physicalID)
	case "AWS::ElasticLoadBalancingV2::Listener":
		return rc.deleteELBv2Listener(physicalID)
	case "AWS::WAFv2::WebACL":
		return rc.deleteWAFv2WebACL(ctx, physicalID)
	case "AWS::WAFv2::IPSet":
		return rc.deleteWAFv2IPSet(ctx, physicalID)
	case "AWS::WAFv2::RuleGroup":
		return rc.deleteWAFv2RuleGroup(ctx, physicalID)
	case "AWS::Backup::BackupVault":
		return rc.deleteBackupVault(physicalID)
	case "AWS::Backup::BackupPlan":
		return rc.deleteBackupPlan(physicalID)
	case "AWS::Backup::BackupSelection":
		return rc.deleteBackupSelection(physicalID)
	case "AWS::RDS::DBCluster":
		return rc.deleteRDSDBCluster(physicalID)
	case "AWS::RDS::DBClusterParameterGroup":
		return rc.deleteRDSDBClusterParameterGroup(physicalID)
	default:
		_, err := rc.deleteExtraResource(ctx, resourceType, physicalID)

		return err
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

func parseDDBKeySchema(
	props map[string]any,
	params, physicalIDs map[string]string,
) []ddbtypes.KeySchemaElement {
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
	defaultEventBusName      = "default"
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
	ctx context.Context,
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
	_, err := rc.backends.SSM.Backend.PutParameter(ctx, &ssmbackend.PutParameterInput{
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

func (rc *ResourceCreator) deleteSSMParameter(ctx context.Context, physicalID string) error {
	if rc.backends.SSM == nil {
		return nil
	}
	_, err := rc.backends.SSM.Backend.DeleteParameter(
		ctx,
		&ssmbackend.DeleteParameterInput{Name: physicalID},
	)

	return err
}

func (rc *ResourceCreator) createKMSKey(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.KMS == nil {
		return logicalID + "-stub", nil
	}
	description := strProp(props, "Description", params, physicalIDs)
	out, err := rc.backends.KMS.Backend.CreateKey(ctx, &kmsbackend.CreateKeyInput{
		Description: description,
		KeyUsage:    "ENCRYPT_DECRYPT",
	})
	if err != nil {
		return "", fmt.Errorf("failed to create KMS key: %w", err)
	}

	return out.KeyMetadata.KeyID, nil
}

func (rc *ResourceCreator) deleteKMSKey(ctx context.Context, physicalID string) error {
	if rc.backends.KMS == nil {
		return nil
	}
	_, err := rc.backends.KMS.Backend.ScheduleKeyDeletion(ctx, &kmsbackend.ScheduleKeyDeletionInput{
		KeyID:               physicalID,
		PendingWindowInDays: kmsMinDeletionWindowDays,
	})

	return err
}

func (rc *ResourceCreator) createSecretsManagerSecret(
	ctx context.Context,
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
	out, err := rc.backends.SecretsManager.Backend.CreateSecret(
		ctx,
		&secretsmanagerbackend.CreateSecretInput{
			Name:         name,
			Description:  description,
			SecretString: secretString,
		})
	if err != nil {
		return "", fmt.Errorf("failed to create secret %s: %w", name, err)
	}

	return out.ARN, nil
}

func (rc *ResourceCreator) deleteSecretsManagerSecret(ctx context.Context, physicalID string) error {
	if rc.backends.SecretsManager == nil {
		return nil
	}
	_, err := rc.backends.SecretsManager.Backend.DeleteSecret(
		ctx,
		&secretsmanagerbackend.DeleteSecretInput{
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
	ctx context.Context,
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
		eventBusName = defaultEventBusName
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

	rule, err := rc.backends.EventBridge.Backend.PutRule(ctx, input)
	if err != nil {
		return "", fmt.Errorf("create EventBridge rule: %w", err)
	}

	return rule.Arn, nil
}

func (rc *ResourceCreator) deleteEventBridgeRule(ctx context.Context, physicalID string) error {
	if rc.backends.EventBridge == nil {
		return nil
	}
	// physicalID is the rule ARN; extract name from it
	parts := strings.Split(physicalID, "/")
	name := parts[len(parts)-1]

	return rc.backends.EventBridge.Backend.DeleteRule(
		ctx,
		name,
		defaultEventBusName,
	)
}

// createStepFunctionsStateMachine creates a Step Functions state machine.
func (rc *ResourceCreator) createStepFunctionsStateMachine(
	ctx context.Context,
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

	sm, err := rc.backends.StepFunctions.Backend.CreateStateMachine(
		ctx,
		name,
		definition,
		roleArn,
		smType,
	)
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
	ctx context.Context,
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

	_, err := rc.backends.CloudWatchLogs.Backend.CreateLogGroup(ctx, name, "", "")
	if err != nil {
		return "", fmt.Errorf("create CloudWatch Logs log group: %w", err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteCloudWatchLogGroup(ctx context.Context, name string) error {
	if rc.backends.CloudWatchLogs == nil {
		return nil
	}

	return rc.backends.CloudWatchLogs.Backend.DeleteLogGroup(ctx, name)
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

	api, err := rc.backends.APIGateway.Backend.CreateRestAPI(apigwbackend.CreateRestAPIInput{
		Name:        name,
		Description: description,
	})
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
func (r *serviceBackendsResolver) ResolveSSMParameter(ctx context.Context, name string) (string, error) {
	if r.ssm == nil {
		return "", fmt.Errorf("%w: SSM backend is not available", ErrDynamicRefFailed)
	}

	out, err := r.ssm.Backend.GetParameter(
		ctx,
		&ssmbackend.GetParameterInput{Name: name},
	)
	if err != nil {
		return "", err
	}

	return out.Parameter.Value, nil
}

// ResolveSSMSecureParameter retrieves a SecureString SSM parameter with decryption.
func (r *serviceBackendsResolver) ResolveSSMSecureParameter(ctx context.Context, name string) (string, error) {
	if r.ssm == nil {
		return "", fmt.Errorf("%w: SSM backend is not available", ErrDynamicRefFailed)
	}

	out, err := r.ssm.Backend.GetParameter(
		ctx,
		&ssmbackend.GetParameterInput{Name: name, WithDecryption: true},
	)
	if err != nil {
		return "", err
	}

	return out.Parameter.Value, nil
}

// ResolveSecret retrieves a Secrets Manager secret value.
// When jsonKey is non-empty the secret string is parsed as JSON and the key is extracted.
func (r *serviceBackendsResolver) ResolveSecret(ctx context.Context, secretID, jsonKey string) (string, error) {
	if r.sm == nil {
		return "", fmt.Errorf("%w: SecretsManager backend is not available", ErrDynamicRefFailed)
	}

	out, err := r.sm.Backend.GetSecretValue(
		ctx,
		&secretsmanagerbackend.GetSecretValueInput{SecretID: secretID})
	if err != nil {
		return "", err
	}

	if jsonKey == "" {
		return out.SecretString, nil
	}

	return resolveJSONKey(out.SecretString, jsonKey)
}

// isCFNExtensibilityType reports whether resourceType is a CFN extensibility type
// (CustomResource, Custom::*, WaitCondition, WaitConditionHandle, Macro).
func isCFNExtensibilityType(resourceType string) bool {
	switch resourceType {
	case cfnTypeCustomResource,
		cfnTypeWaitCondition,
		cfnTypeWaitConditionHandle,
		cfnTypeMacro:
		return true
	}

	return strings.HasPrefix(resourceType, "Custom::")
}

// createExtraResource handles phase-5 resource types added for §K CloudFormation
// resource-type coverage. It returns handled=false when resourceType is not a phase-5 type
// so the caller can fall through to the remaining dispatch chain.
func (rc *ResourceCreator) createExtraResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if physID, handled, err := rc.createExtraLogsResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); handled {
		return physID, true, err
	}

	if physID, handled, err := rc.createExtraNetworkResource(
		logicalID, resourceType, props, params, physicalIDs,
	); handled {
		return physID, true, err
	}

	if id, ok, err := rc.createAppAutoScalingResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createSecretsManagerSupplementalResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}

	if id, ok, err := rc.createSSMSupplementalResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createDynamoDBSupplementalResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}

	if id, ok, err := rc.createGlueSupplementalResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createAppSyncSupplementalResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createResilienceHubSupplementalResource(
		logicalID, resourceType, props, params, physicalIDs,
	); ok {
		return id, true, err
	}

	if id, ok, err := rc.createServiceDiscoveryResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createCodeDeployResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	return rc.createExtraPlatformResource(ctx, logicalID, resourceType, props, params, physicalIDs)
}

// createServiceDiscoveryResource handles AWS::ServiceDiscovery::* resource creation.
func (rc *ResourceCreator) createServiceDiscoveryResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeSDPrivateDNSNamespace:
		id, err := rc.createSDPrivateDNSNamespace(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSDHTTPNamespace:
		id, err := rc.createSDHTTPNamespace(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSDPublicDNSNamespace:
		id, err := rc.createSDPublicDNSNamespace(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSDService:
		id, err := rc.createSDService(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSDInstance:
		id, err := rc.createSDInstance(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

// deleteServiceDiscoveryResource handles AWS::ServiceDiscovery::* resource
// deletions (excluding Instance, which is special-cased in Delete for props access).
func (rc *ResourceCreator) deleteServiceDiscoveryResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeSDPrivateDNSNamespace, resTypeSDHTTPNamespace, resTypeSDPublicDNSNamespace:

		return true, rc.deleteSDNamespace(physicalID)
	case resTypeSDService:

		return true, rc.deleteSDService(physicalID)
	default:

		return false, nil
	}
}

// deleteExtraResource handles deletion for phase-5 resource types.
func (rc *ResourceCreator) deleteExtraResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deleteExtraLogsResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deleteExtraNetworkResource(resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deleteAppAutoScalingResource(resourceType, physicalID); handled {
		return true, err
	}

	if rc.deleteSecretsManagerSupplementalResource(resourceType, physicalID) {
		return true, nil
	}

	if handled, err := rc.deleteSSMSupplementalResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deleteDynamoDBSupplementalResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deleteGlueSupplementalResource(resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deleteAppSyncSupplementalResource(resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deleteResilienceHubSupplementalResource(resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deleteServiceDiscoveryResource(resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deleteCodeDeployResource(resourceType, physicalID); handled {
		return true, err
	}

	return rc.deleteExtraPlatformResource(ctx, resourceType, physicalID)
}

func (rc *ResourceCreator) createExtraPlatformResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if physID, handled, err := rc.createExtraAPIGatewayV2Resource(
		logicalID, resourceType, props, params, physicalIDs,
	); handled {
		return physID, true, err
	}

	if physID, handled, err := rc.createExtraMessagingResource(
		ctx, logicalID, resourceType, props, params, physicalIDs,
	); handled {
		return physID, true, err
	}

	switch resourceType {
	case resTypeKMSAlias:
		id, err := rc.createKMSAlias(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeSSMDocument:
		id, err := rc.createSSMDocument(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::SecretsManager::ResourcePolicy":
		id, err := rc.createSecretsManagerResourcePolicy(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	}

	return rc.createCloudFrontResource(logicalID, resourceType, props, params, physicalIDs)
}

func (rc *ResourceCreator) deleteExtraPlatformResource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deleteExtraAPIGatewayV2Resource(resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deleteExtraMessagingResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	switch resourceType {
	case resTypeKMSAlias:

		return true, rc.deleteKMSAlias(ctx, physicalID)
	case resTypeSSMDocument:

		return true, rc.deleteSSMDocument(ctx, physicalID)
	case "AWS::SecretsManager::ResourcePolicy":

		return true, rc.deleteSecretsManagerResourcePolicy(ctx, physicalID)
	}

	return rc.deleteCloudFrontResource(resourceType, physicalID)
}

// intProp reads an integer-valued property, accepting JSON numbers (float64) and ints.
func intProp(props map[string]any, key string) int {
	return int(int64Val(props[key]))
}

// int64Val converts a JSON-decoded numeric value to int64. CloudFormation templates may carry
// numbers as float64 (JSON), int, or string. Returns 0 when the value is absent or unparseable.
func int64Val(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int:
		return int64(n)
	case int64:
		return n
	case json.Number:
		i, err := n.Int64()
		if err == nil {
			return i
		}
	}

	return 0
}

// strSliceProp resolves a property that is expected to be a list of strings (or refs).
func strSliceProp(v any, params, physicalIDs map[string]string) []string {
	list, ok := v.([]any)
	if !ok {
		return nil
	}

	out := make([]string, 0, len(list))
	for _, item := range list {
		if s := resolve(item, params, physicalIDs); s != "" {
			out = append(out, s)
		}
	}

	return out
}
