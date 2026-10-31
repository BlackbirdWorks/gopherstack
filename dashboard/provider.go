package dashboard

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"

	"github.com/blackbirdworks/gopherstack/services/amplify"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
	"github.com/blackbirdworks/gopherstack/services/appconfigdata"
	"github.com/blackbirdworks/gopherstack/services/athena"
	"github.com/blackbirdworks/gopherstack/services/autoscaling"
	"github.com/blackbirdworks/gopherstack/services/azureappconfig"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azureeventgrid"
	"github.com/blackbirdworks/gopherstack/services/azureeventhubs"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
	"github.com/blackbirdworks/gopherstack/services/azurekeyvault"
	"github.com/blackbirdworks/gopherstack/services/azuremonitor"
	"github.com/blackbirdworks/gopherstack/services/azurequeue"
	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
	"github.com/blackbirdworks/gopherstack/services/azuretable"
	"github.com/blackbirdworks/gopherstack/services/backup"
	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/blackbirdworks/gopherstack/services/ecr"
	"github.com/blackbirdworks/gopherstack/services/ecs"
	"github.com/blackbirdworks/gopherstack/services/efs"
	"github.com/blackbirdworks/gopherstack/services/eks"
	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/blackbirdworks/gopherstack/services/gcpapigateway"
	"github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
	"github.com/blackbirdworks/gopherstack/services/gcpbatch"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery"
	"github.com/blackbirdworks/gopherstack/services/gcpbigtable"
	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
	"github.com/blackbirdworks/gopherstack/services/gcpcloudtasks"
	"github.com/blackbirdworks/gopherstack/services/gcpcompute"
	"github.com/blackbirdworks/gopherstack/services/gcpcontainer"
	"github.com/blackbirdworks/gopherstack/services/gcpdatastore"
	"github.com/blackbirdworks/gopherstack/services/gcpdns"
	"github.com/blackbirdworks/gopherstack/services/gcpeventarc"
	"github.com/blackbirdworks/gopherstack/services/gcpfirestore"
	"github.com/blackbirdworks/gopherstack/services/gcpfunctions"
	"github.com/blackbirdworks/gopherstack/services/gcpiam"
	"github.com/blackbirdworks/gopherstack/services/gcpidentitytoolkit"
	"github.com/blackbirdworks/gopherstack/services/gcpkms"
	"github.com/blackbirdworks/gopherstack/services/gcplogging"
	"github.com/blackbirdworks/gopherstack/services/gcpmonitoring"
	"github.com/blackbirdworks/gopherstack/services/gcppubsub"
	"github.com/blackbirdworks/gopherstack/services/gcpredis"
	"github.com/blackbirdworks/gopherstack/services/gcprun"
	"github.com/blackbirdworks/gopherstack/services/gcpscheduler"
	"github.com/blackbirdworks/gopherstack/services/gcpsecretmanager"
	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
	"github.com/blackbirdworks/gopherstack/services/gcpsql"
	"github.com/blackbirdworks/gopherstack/services/gcpstorage"
	"github.com/blackbirdworks/gopherstack/services/gcpworkflows"
	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/iot"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/blackbirdworks/gopherstack/services/kms"
	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/blackbirdworks/gopherstack/services/ociapigateway"
	"github.com/blackbirdworks/gopherstack/services/ocicontainerengine"
	"github.com/blackbirdworks/gopherstack/services/ocicore"
	"github.com/blackbirdworks/gopherstack/services/ocievents"
	"github.com/blackbirdworks/gopherstack/services/ocifunctions"
	"github.com/blackbirdworks/gopherstack/services/ociidentity"
	"github.com/blackbirdworks/gopherstack/services/ociloadbalancer"
	"github.com/blackbirdworks/gopherstack/services/ocilogging"
	"github.com/blackbirdworks/gopherstack/services/ocimonitoring"
	"github.com/blackbirdworks/gopherstack/services/ocimysql"
	"github.com/blackbirdworks/gopherstack/services/ocinosql"
	"github.com/blackbirdworks/gopherstack/services/ocinotifications"
	"github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
	"github.com/blackbirdworks/gopherstack/services/ociqueue"
	"github.com/blackbirdworks/gopherstack/services/ocistreaming"
	"github.com/blackbirdworks/gopherstack/services/ocivault"
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

	var runtimePortAlloc *portalloc.Allocator
	if c, hasPortAllocator := ctx.Config.(interface{ GetPortAllocator() *portalloc.Allocator }); hasPortAllocator {
		runtimePortAlloc = c.GetPortAllocator()
	}

	conf := Config{
		FaultStore:    faultStore,
		ConfigManager: cfg,
		GlobalConfig:  globalCfg,
		PortAlloc:     runtimePortAlloc,
		Logger:        ctx.Logger,
	}

	// If the config provides service handlers, link them.
	if ap, hasAWSSDKProvider := ctx.Config.(AWSSDKProvider); hasAWSSDKProvider {
		linkHandlers(&conf, ap)
	}

	if ap, hasARM := ctx.Config.(interface{ GetAzureARMHandler() service.Registerable }); hasARM {
		conf.AzureARMOps, _ = getHandler[*azurearm.Handler](ap.GetAzureARMHandler())
	}

	if bp, hasBlob := ctx.Config.(interface{ GetAzureBlobHandler() service.Registerable }); hasBlob {
		conf.AzureBlobOps, _ = getHandler[*azureblob.Handler](bp.GetAzureBlobHandler())
	}

	wireGCPOps(ctx, &conf)

	if qp, hasQueue := ctx.Config.(interface{ GetAzureQueueHandler() service.Registerable }); hasQueue {
		conf.AzureQueueOps, _ = getHandler[*azurequeue.Handler](qp.GetAzureQueueHandler())
	}

	if tp, hasTable := ctx.Config.(interface{ GetAzureTableHandler() service.Registerable }); hasTable {
		conf.AzureTableOps, _ = getHandler[*azuretable.Handler](tp.GetAzureTableHandler())
	}

	if sp, hasBus := ctx.Config.(interface{ GetAzureServiceBusHandler() service.Registerable }); hasBus {
		conf.AzureServiceBusOps, _ = getHandler[*azureservicebus.Handler](sp.GetAzureServiceBusHandler())
	}

	if kp, hasKV := ctx.Config.(interface{ GetAzureKeyVaultHandler() service.Registerable }); hasKV {
		conf.AzureKeyVaultOps, _ = getHandler[*azurekeyvault.Handler](kp.GetAzureKeyVaultHandler())
	}

	if cp, hasCosmos := ctx.Config.(interface{ GetCosmosDBHandler() service.Registerable }); hasCosmos {
		conf.AzureCosmosOps, _ = getHandler[*cosmosdb.Handler](cp.GetCosmosDBHandler())
	}

	linkCloudExtras(&conf, ctx.Config)

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

// wireGCPOps resolves the in-process GCP backends the dashboard proxies call.
func wireGCPOps(ctx *service.AppContext, conf *Config) {
	if gp, hasGCS := ctx.Config.(interface{ GetGCPStorageHandler() service.Registerable }); hasGCS {
		conf.GCPStorageOps, _ = getHandler[*gcpstorage.Handler](gp.GetGCPStorageHandler())
	}

	if gp, hasBQ := ctx.Config.(interface{ GetGCPBigQueryHandler() service.Registerable }); hasBQ {
		conf.GCPBigQueryOps, _ = getHandler[*gcpbigquery.Handler](gp.GetGCPBigQueryHandler())
	}

	if gp, hasPubSub := ctx.Config.(interface{ GetGCPPubSubHandler() service.Registerable }); hasPubSub {
		conf.GCPPubSubOps, _ = getHandler[*gcppubsub.Handler](gp.GetGCPPubSubHandler())
	}

	if gp, hasFS := ctx.Config.(interface{ GetGCPFirestoreHandler() service.Registerable }); hasFS {
		conf.GCPFirestoreOps, _ = getHandler[*gcpfirestore.Handler](gp.GetGCPFirestoreHandler())
	}

	if gp, hasSM := ctx.Config.(interface{ GetGCPSecretManagerHandler() service.Registerable }); hasSM {
		conf.GCPSecretManagerOps, _ = getHandler[*gcpsecretmanager.Handler](gp.GetGCPSecretManagerHandler())
	}

	if gp, hasKMS := ctx.Config.(interface{ GetGCPKMSHandler() service.Registerable }); hasKMS {
		conf.GCPKMSOps, _ = getHandler[*gcpkms.Handler](gp.GetGCPKMSHandler())
	}

	if gp, hasCT := ctx.Config.(interface{ GetGCPCloudTasksHandler() service.Registerable }); hasCT {
		conf.GCPCloudTasksOps, _ = getHandler[*gcpcloudtasks.Handler](gp.GetGCPCloudTasksHandler())
	}

	if gp, hasCS := ctx.Config.(interface{ GetGCPSchedulerHandler() service.Registerable }); hasCS {
		conf.GCPSchedulerOps, _ = getHandler[*gcpscheduler.Handler](gp.GetGCPSchedulerHandler())
	}

	wireGCPExplorerOps(ctx, conf)

	if gp, ok := ctx.Config.(interface{ GetGCPCloudBuildHandler() service.Registerable }); ok {
		conf.GCPCloudBuildOps, _ = getHandler[*gcpcloudbuild.Handler](gp.GetGCPCloudBuildHandler())
	}

	if gp, ok := ctx.Config.(interface{ GetGCPRedisHandler() service.Registerable }); ok {
		conf.GCPRedisOps, _ = getHandler[*gcpredis.Handler](gp.GetGCPRedisHandler())
	}

	if gp, ok := ctx.Config.(interface{ GetGCPMonitoringHandler() service.Registerable }); ok {
		conf.GCPMonitoringOps, _ = getHandler[*gcpmonitoring.Handler](gp.GetGCPMonitoringHandler())
	}

	if gp, hasRun := ctx.Config.(interface{ GetGCPRunHandler() service.Registerable }); hasRun {
		conf.GCPRunOps, _ = getHandler[*gcprun.Handler](gp.GetGCPRunHandler())
	}

	if gp, hasFn := ctx.Config.(interface{ GetGCPFunctionsHandler() service.Registerable }); hasFn {
		conf.GCPFunctionsOps, _ = getHandler[*gcpfunctions.Handler](gp.GetGCPFunctionsHandler())
	}

	if gp, hasIAM := ctx.Config.(interface{ GetGCPIAMHandler() service.Registerable }); hasIAM {
		conf.GCPIAMOps, _ = getHandler[*gcpiam.Handler](gp.GetGCPIAMHandler())
	}
}

// linkCloudExtras resolves the Event Hubs, Event Grid, App Configuration, Cloud SQL and
// Cloud Logging handlers the dashboard proxies call in-process.
func linkCloudExtras(conf *Config, cfg any) {
	if p, ok := cfg.(interface{ GetAzureEventHubsHandler() service.Registerable }); ok {
		conf.AzureEventHubsOps, _ = getHandler[*azureeventhubs.Handler](p.GetAzureEventHubsHandler())
	}

	if p, ok := cfg.(interface{ GetAzureEventGridHandler() service.Registerable }); ok {
		conf.AzureEventGridOps, _ = getHandler[*azureeventgrid.Handler](p.GetAzureEventGridHandler())
	}

	if p, ok := cfg.(interface{ GetAzureFunctionsHandler() service.Registerable }); ok {
		conf.AzureFunctionsOps, _ = getHandler[*azurefunctions.Handler](p.GetAzureFunctionsHandler())
	}

	linkConsoleExtras(conf, cfg)

	if p, ok := cfg.(interface{ GetAzureAppConfigHandler() service.Registerable }); ok {
		conf.AzureAppConfigOps, _ = getHandler[*azureappconfig.Handler](p.GetAzureAppConfigHandler())
	}

	if p, ok := cfg.(interface{ GetGCPSQLHandler() service.Registerable }); ok {
		conf.GCPSQLOps, _ = getHandler[*gcpsql.Handler](p.GetGCPSQLHandler())
	}

	if p, ok := cfg.(interface{ GetGCPLoggingHandler() service.Registerable }); ok {
		conf.GCPLoggingOps, _ = getHandler[*gcplogging.Handler](p.GetGCPLoggingHandler())
	}
}

func wireGCPExplorerOps(ctx *service.AppContext, conf *Config) {
	if gp, ok := ctx.Config.(interface{ GetGCPBigtableHandler() service.Registerable }); ok {
		conf.GCPBigtableOps, _ = getHandler[*gcpbigtable.Handler](gp.GetGCPBigtableHandler())
	}

	if gp, ok := ctx.Config.(interface{ GetGCPEventarcHandler() service.Registerable }); ok {
		conf.GCPEventarcOps, _ = getHandler[*gcpeventarc.Handler](gp.GetGCPEventarcHandler())
	}

	if gp, ok := ctx.Config.(interface{ GetGCPSpannerHandler() service.Registerable }); ok {
		conf.GCPSpannerOps, _ = getHandler[*gcpspanner.Handler](gp.GetGCPSpannerHandler())
	}

	if gp, ok := ctx.Config.(interface{ GetGCPArtifactRegistryHandler() service.Registerable }); ok {
		conf.GCPArtifactRegistryOps, _ = getHandler[*gcpartifactregistry.Handler](gp.GetGCPArtifactRegistryHandler())
	}
}

// linkConsoleExtras resolves the Workflows, Cloud DNS, Azure Files and Oracle Cloud
// handlers the dashboard proxies call in-process.
func linkConsoleExtras(conf *Config, cfg any) {
	if p, ok := cfg.(interface{ GetGCPWorkflowsHandler() service.Registerable }); ok {
		conf.GCPWorkflowsOps, _ = getHandler[*gcpworkflows.Handler](p.GetGCPWorkflowsHandler())
	}

	if p, ok := cfg.(interface{ GetGCPDNSHandler() service.Registerable }); ok {
		conf.GCPDNSOps, _ = getHandler[*gcpdns.Handler](p.GetGCPDNSHandler())
	}

	if p, ok := cfg.(interface{ GetGCPDatastoreHandler() service.Registerable }); ok {
		conf.GCPDatastoreOps, _ = getHandler[*gcpdatastore.Handler](p.GetGCPDatastoreHandler())
	}

	if p, ok := cfg.(interface{ GetGCPComputeHandler() service.Registerable }); ok {
		conf.GCPComputeOps, _ = getHandler[*gcpcompute.Handler](p.GetGCPComputeHandler())
	}

	if p, ok := cfg.(interface{ GetAzureFileHandler() service.Registerable }); ok {
		conf.AzureFileOps, _ = getHandler[*azurefile.Handler](p.GetAzureFileHandler())
	}

	if p, ok := cfg.(interface{ GetOCIObjectStorageHandler() service.Registerable }); ok {
		conf.OCIObjectStorageOps, _ = getHandler[*ociobjectstorage.Handler](p.GetOCIObjectStorageHandler())
	}

	if p, ok := cfg.(interface{ GetOCIVaultHandler() service.Registerable }); ok {
		conf.OCIVaultOps, _ = getHandler[*ocivault.Handler](p.GetOCIVaultHandler())
	}

	if p, ok := cfg.(interface{ GetOCIStreamingHandler() service.Registerable }); ok {
		conf.OCIStreamingOps, _ = getHandler[*ocistreaming.Handler](p.GetOCIStreamingHandler())
	}

	if p, ok := cfg.(interface{ GetOCIIdentityHandler() service.Registerable }); ok {
		conf.OCIIdentityOps, _ = getHandler[*ociidentity.Handler](p.GetOCIIdentityHandler())
	}

	if p, ok := cfg.(interface{ GetOCICoreHandler() service.Registerable }); ok {
		conf.OCICoreOps, _ = getHandler[*ocicore.Handler](p.GetOCICoreHandler())
	}

	linkContainerQueueExtras(conf, cfg)

	if p, ok := cfg.(interface{ GetOCIFunctionsHandler() service.Registerable }); ok {
		conf.OCIFunctionsOps, _ = getHandler[*ocifunctions.Handler](p.GetOCIFunctionsHandler())
	}

	if p, ok := cfg.(interface{ GetAzureMonitorHandler() service.Registerable }); ok {
		conf.AzureMonitorOps, _ = getHandler[*azuremonitor.Handler](p.GetAzureMonitorHandler())
	}
}

// linkContainerQueueExtras links the Kubernetes Engine, Queue and Notifications handlers used by their proxies.
func linkContainerQueueExtras(conf *Config, cfg any) {
	if p, ok := cfg.(interface{ GetGCPContainerHandler() service.Registerable }); ok {
		conf.GCPContainerOps, _ = getHandler[*gcpcontainer.Handler](p.GetGCPContainerHandler())
	}

	if p, ok := cfg.(interface{ GetOCIQueueHandler() service.Registerable }); ok {
		conf.OCIQueueOps, _ = getHandler[*ociqueue.Handler](p.GetOCIQueueHandler())
	}

	if p, ok := cfg.(interface{ GetGCPIdentityToolkitHandler() service.Registerable }); ok {
		conf.GCPIdentityToolkitOps, _ = getHandler[*gcpidentitytoolkit.Handler](p.GetGCPIdentityToolkitHandler())
	}

	if p, ok := cfg.(interface{ GetGCPAPIGatewayHandler() service.Registerable }); ok {
		conf.GCPAPIGatewayOps, _ = getHandler[*gcpapigateway.Handler](p.GetGCPAPIGatewayHandler())
	}

	if p, ok := cfg.(interface{ GetGCPBatchHandler() service.Registerable }); ok {
		conf.GCPBatchOps, _ = getHandler[*gcpbatch.Handler](p.GetGCPBatchHandler())
	}

	if p, ok := cfg.(interface{ GetOCIContainerEngineHandler() service.Registerable }); ok {
		conf.OCIContainerEngineOps, _ = getHandler[*ocicontainerengine.Handler](p.GetOCIContainerEngineHandler())
	}

	linkOCIObservabilityExtras(conf, cfg)

	if p, ok := cfg.(interface{ GetOCINotificationsHandler() service.Registerable }); ok {
		conf.OCINotificationsOps, _ = getHandler[*ocinotifications.Handler](p.GetOCINotificationsHandler())
	}
}

// linkOCIObservabilityExtras links the MySQL HeatWave, Logging and Monitoring handlers used by their proxies.
func linkOCIObservabilityExtras(conf *Config, cfg any) {
	if p, ok := cfg.(interface{ GetOCIMySQLHandler() service.Registerable }); ok {
		conf.OCIMySQLOps, _ = getHandler[*ocimysql.Handler](p.GetOCIMySQLHandler())
	}

	if p, ok := cfg.(interface{ GetOCILoggingHandler() service.Registerable }); ok {
		conf.OCILoggingOps, _ = getHandler[*ocilogging.Handler](p.GetOCILoggingHandler())
	}

	if p, ok := cfg.(interface{ GetOCIMonitoringHandler() service.Registerable }); ok {
		conf.OCIMonitoringOps, _ = getHandler[*ocimonitoring.Handler](p.GetOCIMonitoringHandler())
	}

	if p, ok := cfg.(interface{ GetOCINoSQLHandler() service.Registerable }); ok {
		conf.OCINoSQLOps, _ = getHandler[*ocinosql.Handler](p.GetOCINoSQLHandler())
	}

	if p, ok := cfg.(interface{ GetOCIAPIGatewayHandler() service.Registerable }); ok {
		conf.OCIAPIGatewayOps, _ = getHandler[*ociapigateway.Handler](p.GetOCIAPIGatewayHandler())
	}

	if p, ok := cfg.(interface{ GetOCILoadBalancerHandler() service.Registerable }); ok {
		conf.OCILoadBalancerOps, _ = getHandler[*ociloadbalancer.Handler](p.GetOCILoadBalancerHandler())
	}

	if p, ok := cfg.(interface{ GetOCIEventsHandler() service.Registerable }); ok {
		conf.OCIEventsOps, _ = getHandler[*ocievents.Handler](p.GetOCIEventsHandler())
	}
}
