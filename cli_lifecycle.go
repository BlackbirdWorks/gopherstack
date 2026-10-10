package main

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	elasticachebackend "github.com/blackbirdworks/gopherstack/services/elasticache"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	mediastorebackend "github.com/blackbirdworks/gopherstack/services/mediastore"
	memorydbbackend "github.com/blackbirdworks/gopherstack/services/memorydb"
	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
)

// LifecycleSettings holds the opt-in dwell times for transitional resource
// states (Creating, Processing, Pending, InProgress). Every default is 0, so
// resources settle instantly unless a client-visible window is requested.
// A per-service value of 0 falls back to Delay.
type LifecycleSettings struct {
	Delay                        time.Duration `name:"delay"                          env:"GOPHERSTACK_LIFECYCLE_DELAY"          default:"0s" help:"Default dwell in transitional states for every service below; 0 settles instantly."` //nolint:lll // config struct tags are intentionally verbose
	OpenSearchProcessing         time.Duration `name:"opensearch-processing"          env:"OPENSEARCH_PROCESSING_DELAY"          default:"0s" help:"OpenSearch domain Processing window."`                                               //nolint:lll // config struct tags are intentionally verbose
	ElastiCache                  time.Duration `name:"elasticache"                    env:"ELASTICACHE_LIFECYCLE_DELAY"          default:"0s" help:"ElastiCache creating dwell."`                                                        //nolint:lll // config struct tags are intentionally verbose
	MemoryDB                     time.Duration `name:"memorydb"                       env:"MEMORYDB_LIFECYCLE_DELAY"             default:"0s" help:"MemoryDB cluster creating dwell."`                                                   //nolint:lll // config struct tags are intentionally verbose
	LambdaActivation             time.Duration `name:"lambda-activation"              env:"LAMBDA_ACTIVATION_DELAY"              default:"0s" help:"Lambda function Pending window."`                                                    //nolint:lll // config struct tags are intentionally verbose
	LambdaProvisionedConcurrency time.Duration `name:"lambda-provisioned-concurrency" env:"LAMBDA_PROVISIONED_CONCURRENCY_DELAY" default:"0s" help:"Lambda provisioned concurrency IN_PROGRESS window."`                                 //nolint:lll // config struct tags are intentionally verbose
	ECSStart                     time.Duration `name:"ecs-start"                      env:"ECS_START_DELAY"                      default:"0s" help:"ECS per-phase task start delay."`                                                    //nolint:lll // config struct tags are intentionally verbose
	ECSStop                      time.Duration `name:"ecs-stop"                       env:"ECS_STOP_DELAY"                       default:"0s" help:"ECS per-phase task stop delay."`                                                     //nolint:lll // config struct tags are intentionally verbose
	MediaStore                   time.Duration `name:"mediastore"                     env:"MEDIASTORE_ACTIVATION_DELAY"          default:"0s" help:"MediaStore container CREATING/DELETING window."`                                     //nolint:lll // config struct tags are intentionally verbose
	EFS                          time.Duration `name:"efs"                            env:"EFS_ACTIVATION_DELAY"                 default:"0s" help:"EFS file system creating window."`                                                   //nolint:lll // config struct tags are intentionally verbose
	Redshift                     time.Duration `name:"redshift"                       env:"REDSHIFT_ACTIVATION_DELAY"            default:"0s" help:"Redshift cluster creating window."`                                                  //nolint:lll // config struct tags are intentionally verbose
	SSMCommand                   time.Duration `name:"ssm-command"                    env:"SSM_COMMAND_EXEC_DELAY"               default:"0s" help:"SSM SendCommand InProgress window."`                                                 //nolint:lll // config struct tags are intentionally verbose
	CloudFront                   time.Duration `name:"cloudfront"                     env:"CLOUDFRONT_DEPLOY_DELAY"              default:"0s" help:"CloudFront distribution InProgress window; 0 keeps the built-in 100ms."`             //nolint:lll // config struct tags are intentionally verbose
	SSMAutomation                time.Duration `name:"ssm-automation"                 env:"SSM_AUTOMATION_EXEC_DELAY"            default:"0s" help:"SSM automation execution InProgress window."`                                        //nolint:lll // config struct tags are intentionally verbose
	DocDB                        time.Duration `name:"docdb"                          env:"DOCDB_LIFECYCLE_DELAY"                default:"0s" help:"DocumentDB cluster and instance creating dwell."`                                    //nolint:lll // config struct tags are intentionally verbose
	Neptune                      time.Duration `name:"neptune"                        env:"NEPTUNE_LIFECYCLE_DELAY"              default:"0s" help:"Neptune cluster and instance creating dwell."`                                       //nolint:lll // config struct tags are intentionally verbose
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

// wireLifecycleDelays applies the configured dwell times to the already
// initialized service backends; zero values leave services instant.
func wireLifecycleDelays(byName map[string]service.Registerable, l LifecycleSettings) {
	appliers := map[string]lifecycleApplier{
		"OpenSearch":  applyOpenSearch,
		"ElastiCache": applyElastiCache,
		"MemoryDB":    applyMemoryDB,
		"Lambda":      applyLambda,
		"ECS":         applyECS,
		"MediaStore":  applyMediaStore,
		"EFS":         applyEFS,
		"Redshift":    applyRedshift,
		"CloudFront":  applyCloudFront,
		"SSM":         applySSM,
		"DocDB":       applyDocDB,
		"Neptune":     applyNeptune,
	}

	for name, apply := range appliers {
		if reg, ok := byName[name]; ok {
			apply(reg, l)
		}
	}
}
