package dashboard

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"

	"github.com/blackbirdworks/gopherstack/services/amplify"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
	"github.com/blackbirdworks/gopherstack/services/athena"
	"github.com/blackbirdworks/gopherstack/services/autoscaling"
	"github.com/blackbirdworks/gopherstack/services/backup"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/blackbirdworks/gopherstack/services/ecr"
	"github.com/blackbirdworks/gopherstack/services/ecs"
	"github.com/blackbirdworks/gopherstack/services/efs"
	"github.com/blackbirdworks/gopherstack/services/eks"
	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/blackbirdworks/gopherstack/services/kms"
	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/blackbirdworks/gopherstack/services/route53"
	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
	"github.com/blackbirdworks/gopherstack/services/ses"
	"github.com/blackbirdworks/gopherstack/services/sesv2"
	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/blackbirdworks/gopherstack/services/ssm"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

var errConfigManagerNotImplemented = errors.New("app context config does not implement ConfigManager")

// Provider implements the service.Provider interface for the Dashboard service.
type Provider struct{}

// Name returns the name of the service provider.
func (p *Provider) Name() string {
	return "Dashboard"
}

// Init initializes the Dashboard service.
// It extracts the fault store and config manager from the AppContext and
// returns a new DashboardHandler instance.
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	// Extract the main config to get the fault store and other system components.
	// Since the orchestrator puts the *CLI or *TestStack etc into Config, we need
	// to ensure it satisfies what we need.
	cfg, ok := ctx.Config.(ConfigManager)
	if !ok {
		return nil, errConfigManagerNotImplemented
	}

	// We also need access to the fault store for the chaos dashboard.
	// In Gopherstack, the fault store is typically passed through the individual
	// service configs or global config.
	var faultStore *chaos.FaultStore
	var globalCfg *config.GlobalConfig

	// Try to extract fault store from known config types if they exist.
	// This is a safety measure to ensure we don't break different stack types.
	if c, hasFaultStore := ctx.Config.(interface{ GetFaultStore() *chaos.FaultStore }); hasFaultStore {
		faultStore = c.GetFaultStore()
	}
	if c, hasGlobalConfig := ctx.Config.(interface{ GetGlobalConfig() *config.GlobalConfig }); hasGlobalConfig {
		globalCfg = c.GetGlobalConfig()
	}

	conf := Config{
		FaultStore:    faultStore,
		ConfigManager: cfg,
		GlobalConfig:  globalCfg,
		Logger:        ctx.Logger,
	}

	// If the config provides service handlers, link them.
	if ap, hasAWSSDKProvider := ctx.Config.(AWSSDKProvider); hasAWSSDKProvider {
		linkHandlers(&conf, ap)
	}

	h := NewHandler(conf)

	return h, nil
}

func linkHandlers(conf *Config, ap AWSSDKProvider) {
	conf.DDBOps, _ = getHandler[*dynamodb.DynamoDBHandler](ap.GetDynamoDBHandler())
	conf.S3Ops, _ = getHandler[*s3.S3Handler](ap.GetS3Handler())
	conf.SSMOps, _ = getHandler[*ssm.Handler](ap.GetSSMHandler())
	conf.IAMOps, _ = getHandler[*iam.Handler](ap.GetIAMHandler())
	conf.STSOps, _ = getHandler[*sts.Handler](ap.GetSTSHandler())
	conf.SNSOps, _ = getHandler[*sns.Handler](ap.GetSNSHandler())
	conf.SQSOps, _ = getHandler[*sqs.Handler](ap.GetSQSHandler())
	conf.KMSOps, _ = getHandler[*kms.Handler](ap.GetKMSHandler())
	conf.SecretsManagerOps, _ = getHandler[*secretsmanager.Handler](ap.GetSecretsManagerHandler())
	conf.LambdaOps, _ = getHandler[*lambda.Handler](ap.GetLambdaHandler())
	conf.EventBridgeOps, _ = getHandler[*eventbridge.Handler](ap.GetEventBridgeHandler())
	conf.APIGatewayOps, _ = getHandler[*apigateway.Handler](ap.GetAPIGatewayHandler())
	conf.CloudWatchLogsOps, _ = getHandler[*cloudwatchlogs.Handler](ap.GetCloudWatchLogsHandler())
	conf.StepFunctionsOps, _ = getHandler[*stepfunctions.Handler](ap.GetStepFunctionsHandler())
	conf.CloudWatchOps, _ = getHandler[*cloudwatch.Handler](ap.GetCloudWatchHandler())
	conf.CloudFormationOps, _ = getHandler[*cloudformation.Handler](ap.GetCloudFormationHandler())
	conf.KinesisOps, _ = getHandler[*kinesis.Handler](ap.GetKinesisHandler())
	// Skipping ElastiCache as it's not strictly needed for legacy rest seeding
	conf.Route53Ops, _ = getHandler[*route53.Handler](ap.GetRoute53Handler())
	conf.SESOps, _ = getHandler[*ses.Handler](ap.GetSESHandler())
	conf.SESv2Ops, _ = getHandler[*sesv2.Handler](ap.GetSESv2Handler())
	conf.EC2Ops, _ = getHandler[*ec2.Handler](ap.GetEC2Handler())
	conf.ECROps, _ = getHandler[*ecr.Handler](ap.GetECRHandler())
	conf.ECSOps, _ = getHandler[*ecs.Handler](ap.GetECSHandler())
	conf.EFSOps, _ = getHandler[*efs.Handler](ap.GetEFSHandler())
	conf.EKSOps, _ = getHandler[*eks.Handler](ap.GetEKSHandler())
	conf.IoTOps, _ = getHandler[*iot.Handler](ap.GetIoTHandler())
	conf.APIGatewayManagementAPIOps, _ = getHandler[*apigatewaymanagementapi.Handler](
		ap.GetAPIGatewayManagementAPIHandler(),
	)
	conf.AppConfigDataOps, _ = getHandler[*appconfigdata.Handler](ap.GetAppConfigDataHandler())
	conf.AmplifyOps, _ = getHandler[*amplify.Handler](ap.GetAmplifyHandler())
	conf.APIGatewayV2Ops, _ = getHandler[*apigatewayv2.Handler](ap.GetAPIGatewayV2Handler())
	conf.AppConfigOps, _ = getHandler[*appconfig.Handler](ap.GetAppConfigHandler())
	conf.AthenaOps, _ = getHandler[*athena.Handler](ap.GetAthenaHandler())
	conf.AutoscalingOps, _ = getHandler[*autoscaling.Handler](ap.GetAutoscalingHandler())
	conf.BackupOps, _ = getHandler[*backup.Handler](ap.GetBackupHandler())
	conf.CloudTrailOps, _ = getHandler[*cloudtrail.Handler](ap.GetCloudTrailHandler())
}

func getHandler[T any](h service.Registerable) (T, bool) {
	if h == nil {
		var zero T

		return zero, false
	}
	v, ok := h.(T)

	return v, ok
}
