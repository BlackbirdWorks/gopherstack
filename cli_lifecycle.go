package main

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	apprunnerbackend "github.com/blackbirdworks/gopherstack/services/apprunner"
	awsconfigbackend "github.com/blackbirdworks/gopherstack/services/awsconfig"
	bedrockbackend "github.com/blackbirdworks/gopherstack/services/bedrock"
	bedrockagentbackend "github.com/blackbirdworks/gopherstack/services/bedrockagent"
	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
	directoryservicebackend "github.com/blackbirdworks/gopherstack/services/directoryservice"
	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	elasticachebackend "github.com/blackbirdworks/gopherstack/services/elasticache"
	elasticbeanstalkbackend "github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
	inspector2backend "github.com/blackbirdworks/gopherstack/services/inspector2"
	kinesisanalyticsv2backend "github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	medialivebackend "github.com/blackbirdworks/gopherstack/services/medialive"
	mediastorebackend "github.com/blackbirdworks/gopherstack/services/mediastore"
	memorydbbackend "github.com/blackbirdworks/gopherstack/services/memorydb"
	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	quicksightbackend "github.com/blackbirdworks/gopherstack/services/quicksight"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	route53resolverbackend "github.com/blackbirdworks/gopherstack/services/route53resolver"
	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	workspacesbackend "github.com/blackbirdworks/gopherstack/services/workspaces"
)

// LifecycleSettings holds the opt-in dwell times for transitional resource
// states (Creating, Processing, Pending, InProgress). Every default is 0, so
// resources settle instantly unless a client-visible window is requested.
// A per-service value of 0 falls back to Delay.
type LifecycleSettings struct {
	Delay                        time.Duration `name:"delay"                          env:"GOPHERSTACK_LIFECYCLE_DELAY"          default:"0s" help:"Default dwell in transitional states for every service below; 0 settles instantly."`             //nolint:lll // config struct tags are intentionally verbose
	OpenSearchProcessing         time.Duration `name:"opensearch-processing"          env:"OPENSEARCH_PROCESSING_DELAY"          default:"0s" help:"OpenSearch domain Processing window."`                                                           //nolint:lll // config struct tags are intentionally verbose
	ElastiCache                  time.Duration `name:"elasticache"                    env:"ELASTICACHE_LIFECYCLE_DELAY"          default:"0s" help:"ElastiCache creating dwell."`                                                                    //nolint:lll // config struct tags are intentionally verbose
	MemoryDB                     time.Duration `name:"memorydb"                       env:"MEMORYDB_LIFECYCLE_DELAY"             default:"0s" help:"MemoryDB cluster creating dwell."`                                                               //nolint:lll // config struct tags are intentionally verbose
	LambdaActivation             time.Duration `name:"lambda-activation"              env:"LAMBDA_ACTIVATION_DELAY"              default:"0s" help:"Lambda function Pending window."`                                                                //nolint:lll // config struct tags are intentionally verbose
	LambdaProvisionedConcurrency time.Duration `name:"lambda-provisioned-concurrency" env:"LAMBDA_PROVISIONED_CONCURRENCY_DELAY" default:"0s" help:"Lambda provisioned concurrency IN_PROGRESS window."`                                             //nolint:lll // config struct tags are intentionally verbose
	ECSStart                     time.Duration `name:"ecs-start"                      env:"ECS_START_DELAY"                      default:"0s" help:"ECS per-phase task start delay."`                                                                //nolint:lll // config struct tags are intentionally verbose
	ECSStop                      time.Duration `name:"ecs-stop"                       env:"ECS_STOP_DELAY"                       default:"0s" help:"ECS per-phase task stop delay."`                                                                 //nolint:lll // config struct tags are intentionally verbose
	MediaStore                   time.Duration `name:"mediastore"                     env:"MEDIASTORE_ACTIVATION_DELAY"          default:"0s" help:"MediaStore container CREATING/DELETING window."`                                                 //nolint:lll // config struct tags are intentionally verbose
	EFS                          time.Duration `name:"efs"                            env:"EFS_ACTIVATION_DELAY"                 default:"0s" help:"EFS file system creating window."`                                                               //nolint:lll // config struct tags are intentionally verbose
	Redshift                     time.Duration `name:"redshift"                       env:"REDSHIFT_ACTIVATION_DELAY"            default:"0s" help:"Redshift cluster creating window."`                                                              //nolint:lll // config struct tags are intentionally verbose
	SSMCommand                   time.Duration `name:"ssm-command"                    env:"SSM_COMMAND_EXEC_DELAY"               default:"0s" help:"SSM SendCommand InProgress window."`                                                             //nolint:lll // config struct tags are intentionally verbose
	CloudFront                   time.Duration `name:"cloudfront"                     env:"CLOUDFRONT_DEPLOY_DELAY"              default:"0s" help:"CloudFront distribution InProgress window; 0 keeps the built-in 100ms."`                         //nolint:lll // config struct tags are intentionally verbose
	SSMAutomation                time.Duration `name:"ssm-automation"                 env:"SSM_AUTOMATION_EXEC_DELAY"            default:"0s" help:"SSM automation execution InProgress window."`                                                    //nolint:lll // config struct tags are intentionally verbose
	DocDB                        time.Duration `name:"docdb"                          env:"DOCDB_LIFECYCLE_DELAY"                default:"0s" help:"DocumentDB cluster and instance creating dwell."`                                                //nolint:lll // config struct tags are intentionally verbose
	Neptune                      time.Duration `name:"neptune"                        env:"NEPTUNE_LIFECYCLE_DELAY"              default:"0s" help:"Neptune cluster and instance creating dwell."`                                                   //nolint:lll // config struct tags are intentionally verbose
	AWSConfig                    time.Duration `name:"awsconfig"                      env:"AWSCONFIG_LIFECYCLE_DELAY"            default:"0s" help:"AWS Config conformance pack CREATE_IN_PROGRESS window."`                                         //nolint:lll // config struct tags are intentionally verbose
	Inspector2                   time.Duration `name:"inspector2"                     env:"INSPECTOR2_LIFECYCLE_DELAY"           default:"0s" help:"Inspector2 ENABLING/DISABLING window."`                                                          //nolint:lll // config struct tags are intentionally verbose
	WorkSpaces                   time.Duration `name:"workspaces"                     env:"WORKSPACES_LIFECYCLE_DELAY"           default:"0s" help:"WorkSpaces PENDING and directory REGISTERING window."`                                           //nolint:lll // config struct tags are intentionally verbose
	QuickSight                   time.Duration `name:"quicksight"                     env:"QUICKSIGHT_CREATION_DELAY"            default:"0s" help:"QuickSight dashboard, analysis and data source CREATION_IN_PROGRESS window."`                    //nolint:lll // config struct tags are intentionally verbose
	AppRunner                    time.Duration `name:"apprunner"                      env:"APPRUNNER_OPERATION_DELAY"            default:"0s" help:"App Runner service OPERATION_IN_PROGRESS window after each operation."`                          //nolint:lll // config struct tags are intentionally verbose
	KinesisAnalyticsV2           time.Duration `name:"kinesisanalyticsv2"             env:"KINESISANALYTICSV2_LIFECYCLE_DELAY"   default:"0s" help:"Managed Flink application STARTING/STOPPING/UPDATING window."`                                   //nolint:lll // config struct tags are intentionally verbose
	QuickSightIngestion          time.Duration `name:"quicksight-ingestion"           env:"QUICKSIGHT_INGESTION_DELAY"           default:"0s" help:"QuickSight manual ingestion RUNNING window; 0 keeps the built-in 1s; ignores the global delay."` //nolint:lll // config struct tags are intentionally verbose
	SageMaker                    time.Duration `name:"sagemaker"                      env:"SAGEMAKER_LIFECYCLE_DELAY"            default:"0s" help:"SageMaker transitional-state dwell; 0 keeps the built-in dwell."`                                //nolint:lll // config struct tags are intentionally verbose
	Bedrock                      time.Duration `name:"bedrock"                        env:"BEDROCK_JOB_COMPLETION_DELAY"         default:"0s" help:"Bedrock job InProgress window; 0 keeps the built-in window."`                                    //nolint:lll // config struct tags are intentionally verbose
	BedrockAgent                 time.Duration `name:"bedrockagent"                   env:"BEDROCKAGENT_LIFECYCLE_DELAY"         default:"0s" help:"Bedrock Agents CREATING/PREPARING/UPDATING/ingestion window."`                                   //nolint:lll // config struct tags are intentionally verbose
	ElasticBeanstalk             time.Duration `name:"elasticbeanstalk"               env:"ELASTICBEANSTALK_LIFECYCLE_DELAY"     default:"0s" help:"Elastic Beanstalk environment Launching/Updating/Terminating window."`                           //nolint:lll // config struct tags are intentionally verbose
	Route53Resolver              time.Duration `name:"route53resolver"                env:"ROUTE53RESOLVER_LIFECYCLE_DELAY"      default:"0s" help:"Route 53 Resolver endpoint CREATING/UPDATING and rule association CREATING window."`             //nolint:lll // config struct tags are intentionally verbose
	DirectoryService             time.Duration `name:"directoryservice"               env:"DIRECTORYSERVICE_LIFECYCLE_DELAY"     default:"0s" help:"Directory Service transitional-state window; 0 keeps the built-in sub-second dwell."`            //nolint:lll // config struct tags are intentionally verbose
	MediaLive                    time.Duration `name:"medialive"                      env:"MEDIALIVE_LIFECYCLE_DELAY"            default:"0s" help:"MediaLive channel/multiplex CREATING/STARTING/STOPPING/DELETING window."`                        //nolint:lll // config struct tags are intentionally verbose
}

func (l LifecycleSettings) effective(specific time.Duration) time.Duration {
	if specific > 0 {
		return specific
	}

	return l.Delay
}

// lifecycleApplier sets one service's dwell on its backend; it is a no-op if the handler or backend type differs.
type lifecycleApplier func(reg service.Registerable, l LifecycleSettings)

func applyOpenSearch(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*opensearchbackend.Handler); ok {
		if bk, isBk := h.Backend.(*opensearchbackend.InMemoryBackend); isBk {
			bk.SetProcessingDelay(l.effective(l.OpenSearchProcessing))
		}
	}
}

func applyElastiCache(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*elasticachebackend.Handler); ok {
		if bk, isBk := h.Backend.(*elasticachebackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.ElastiCache))
		}
	}
}

func applyMemoryDB(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*memorydbbackend.Handler); ok {
		if bk, isBk := h.Backend.(*memorydbbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.MemoryDB))
		}
	}
}

func applyLambda(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*lambdabackend.Handler); ok {
		if bk, isBk := h.Backend.(*lambdabackend.InMemoryBackend); isBk {
			bk.SetActivationDelay(l.effective(l.LambdaActivation))
			bk.SetProvisionedConcurrencyDelay(l.effective(l.LambdaProvisionedConcurrency))
		}
	}
}

func applyECS(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*ecsbackend.Handler); ok {
		if bk, isBk := h.Backend.(*ecsbackend.InMemoryBackend); isBk {
			bk.SetStartDelay(l.effective(l.ECSStart))
			bk.SetStopDelay(l.effective(l.ECSStop))
		}
	}
}

func applyMediaStore(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*mediastorebackend.Handler); ok {
		if bk, isBk := h.Backend.(*mediastorebackend.InMemoryBackend); isBk {
			bk.SetActivationDelay(l.effective(l.MediaStore))
		}
	}
}

func applyEFS(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*efsbackend.Handler); ok {
		h.Backend.SetFileSystemActivationDelay(l.effective(l.EFS))
	}
}

func applyRedshift(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*redshiftbackend.Handler); ok {
		if bk, isBk := h.Backend.(*redshiftbackend.InMemoryBackend); isBk {
			bk.SetClusterActivationDelay(l.effective(l.Redshift))
		}
	}
}

func applyCloudFront(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*cloudfrontbackend.Handler); ok {
		h.Backend.SetDistributionDeployDelay(l.effective(l.CloudFront))
	}
}

func applySSM(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*ssmbackend.Handler); ok {
		if bk, isBk := h.Backend.(*ssmbackend.InMemoryBackend); isBk {
			bk.WithCommandExecDelay(l.effective(l.SSMCommand))
			bk.WithAutomationExecDelay(l.effective(l.SSMAutomation))
		}
	}
}

func applyDocDB(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*docdbbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.effective(l.DocDB))
	}
}

func applyNeptune(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*neptunebackend.Handler); ok {
		if bk, isBk := h.Backend.(*neptunebackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.Neptune))
		}
	}
}

func applyAWSConfig(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*awsconfigbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.effective(l.AWSConfig))
	}
}

func applyInspector2(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*inspector2backend.Handler); ok {
		if bk, isBk := h.Backend.(*inspector2backend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.Inspector2))
		}
	}
}

func applyWorkSpaces(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*workspacesbackend.Handler); ok {
		if bk, isBk := h.Backend.(*workspacesbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.WorkSpaces))
		}
	}
}

func applyAppRunner(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*apprunnerbackend.Handler); ok {
		if bk, isBk := h.Backend.(*apprunnerbackend.InMemoryBackend); isBk {
			bk.SetOperationDelay(l.effective(l.AppRunner))
		}
	}
}

func applyKinesisAnalyticsV2(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*kinesisanalyticsv2backend.Handler); ok {
		if bk, isBk := h.Backend.(*kinesisanalyticsv2backend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.KinesisAnalyticsV2))
		}
	}
}

func applyQuickSight(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*quicksightbackend.Handler); ok {
		if bk, isBk := h.Backend.(*quicksightbackend.InMemoryBackend); isBk {
			bk.SetCreationDelay(l.effective(l.QuickSight))

			if l.QuickSightIngestion > 0 {
				bk.SetIngestionDelay(l.QuickSightIngestion)
			}
		}
	}
}

func applySageMaker(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*sagemakerbackend.Handler); ok {
		if d := l.effective(l.SageMaker); d > 0 {
			h.Backend.SetLifecycleDelay(d)
		}
	}
}

func applyBedrock(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*bedrockbackend.Handler); ok {
		if d := l.effective(l.Bedrock); d > 0 {
			h.Backend.SetJobCompletionDelay(d)
		}
	}
}

func applyBedrockAgent(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*bedrockagentbackend.Handler); ok {
		if bk, isBk := h.Backend.(*bedrockagentbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.BedrockAgent))
		}
	}
}

func applyElasticBeanstalk(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*elasticbeanstalkbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.effective(l.ElasticBeanstalk))
	}
}

func applyRoute53Resolver(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*route53resolverbackend.Handler); ok {
		if bk, isBk := h.Backend.(*route53resolverbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.Route53Resolver))
		}
	}
}

func applyDirectoryService(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*directoryservicebackend.Handler); ok {
		if bk, isBk := h.Backend.(*directoryservicebackend.InMemoryBackend); isBk {
			if d := l.effective(l.DirectoryService); d > 0 {
				bk.SetLifecycleDelay(d)
			}
		}
	}
}

func applyMediaLive(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*medialivebackend.Handler); ok {
		if bk, isBk := h.Backend.(*medialivebackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.MediaLive))
		}
	}
}

// wireLifecycleDelays applies the configured dwell times to the already
// initialized service backends; zero values leave services instant.
func wireLifecycleDelays(byName map[string]service.Registerable, l LifecycleSettings) {
	appliers := map[string]lifecycleApplier{
		"OpenSearch":         applyOpenSearch,
		"ElastiCache":        applyElastiCache,
		"MemoryDB":           applyMemoryDB,
		"Lambda":             applyLambda,
		"ECS":                applyECS,
		"MediaStore":         applyMediaStore,
		"EFS":                applyEFS,
		"Redshift":           applyRedshift,
		"CloudFront":         applyCloudFront,
		"SSM":                applySSM,
		"DocDB":              applyDocDB,
		"Neptune":            applyNeptune,
		"AWSConfig":          applyAWSConfig,
		"Inspector2":         applyInspector2,
		"WorkSpaces":         applyWorkSpaces,
		"QuickSight":         applyQuickSight,
		"AppRunner":          applyAppRunner,
		"KinesisAnalyticsV2": applyKinesisAnalyticsV2,
		"SageMaker":          applySageMaker,
		"Bedrock":            applyBedrock,
		"BedrockAgent":       applyBedrockAgent,
		"Elasticbeanstalk":   applyElasticBeanstalk,
		"Route53Resolver":    applyRoute53Resolver,
		"DirectoryService":   applyDirectoryService,
		"MediaLive":          applyMediaLive,
	}

	for name, apply := range appliers {
		if reg, ok := byName[name]; ok {
			apply(reg, l)
		}
	}
}
