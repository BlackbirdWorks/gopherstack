package app

import (
	"github.com/blackbirdworks/gopherstack/internal/cliconfig"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	accessanalyzerbackend "github.com/blackbirdworks/gopherstack/services/accessanalyzer"
	accountbackend "github.com/blackbirdworks/gopherstack/services/account"
	acmpcabackend "github.com/blackbirdworks/gopherstack/services/acmpca"
	ampbackend "github.com/blackbirdworks/gopherstack/services/amp"
	apprunnerbackend "github.com/blackbirdworks/gopherstack/services/apprunner"
	appstreambackend "github.com/blackbirdworks/gopherstack/services/appstream"
	arczonalshiftbackend "github.com/blackbirdworks/gopherstack/services/arczonalshift"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	awsconfigbackend "github.com/blackbirdworks/gopherstack/services/awsconfig"
	backupbackend "github.com/blackbirdworks/gopherstack/services/backup"
	bedrockbackend "github.com/blackbirdworks/gopherstack/services/bedrock"
	bedrockagentbackend "github.com/blackbirdworks/gopherstack/services/bedrockagent"
	bedrockagentcorecontrolbackend "github.com/blackbirdworks/gopherstack/services/bedrockagentcorecontrol"
	cleanroomsbackend "github.com/blackbirdworks/gopherstack/services/cleanrooms"
	cloudcontrolbackend "github.com/blackbirdworks/gopherstack/services/cloudcontrol"
	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
	codeconnectionsbackend "github.com/blackbirdworks/gopherstack/services/codeconnections"
	codestarconnectionsbackend "github.com/blackbirdworks/gopherstack/services/codestarconnections"
	comprehendbackend "github.com/blackbirdworks/gopherstack/services/comprehend"
	databrewbackend "github.com/blackbirdworks/gopherstack/services/databrew"
	daxbackend "github.com/blackbirdworks/gopherstack/services/dax"
	detectivebackend "github.com/blackbirdworks/gopherstack/services/detective"
	directoryservicebackend "github.com/blackbirdworks/gopherstack/services/directoryservice"
	docdbbackend "github.com/blackbirdworks/gopherstack/services/docdb"
	dsqlbackend "github.com/blackbirdworks/gopherstack/services/dsql"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	elasticachebackend "github.com/blackbirdworks/gopherstack/services/elasticache"
	elasticbeanstalkbackend "github.com/blackbirdworks/gopherstack/services/elasticbeanstalk"
	fisbackend "github.com/blackbirdworks/gopherstack/services/fis"
	forecastbackend "github.com/blackbirdworks/gopherstack/services/forecast"
	fsxbackend "github.com/blackbirdworks/gopherstack/services/fsx"
	globalacceleratorbackend "github.com/blackbirdworks/gopherstack/services/globalaccelerator"
	grafanabackend "github.com/blackbirdworks/gopherstack/services/grafana"
	inspector2backend "github.com/blackbirdworks/gopherstack/services/inspector2"
	iotanalyticsbackend "github.com/blackbirdworks/gopherstack/services/iotanalytics"
	iotwirelessbackend "github.com/blackbirdworks/gopherstack/services/iotwireless"
	kafkabackend "github.com/blackbirdworks/gopherstack/services/kafka"
	kafkaconnectbackend "github.com/blackbirdworks/gopherstack/services/kafkaconnect"
	kinesisanalyticsv2backend "github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	macie2backend "github.com/blackbirdworks/gopherstack/services/macie2"
	managedblockchainbackend "github.com/blackbirdworks/gopherstack/services/managedblockchain"
	medialivebackend "github.com/blackbirdworks/gopherstack/services/medialive"
	mediapackagebackend "github.com/blackbirdworks/gopherstack/services/mediapackage"
	mediastorebackend "github.com/blackbirdworks/gopherstack/services/mediastore"
	memorydbbackend "github.com/blackbirdworks/gopherstack/services/memorydb"
	mgnbackend "github.com/blackbirdworks/gopherstack/services/mgn"
	mwaabackend "github.com/blackbirdworks/gopherstack/services/mwaa"
	neptunebackend "github.com/blackbirdworks/gopherstack/services/neptune"
	networkfirewallbackend "github.com/blackbirdworks/gopherstack/services/networkfirewall"
	networkmanagerbackend "github.com/blackbirdworks/gopherstack/services/networkmanager"
	networkmonitorbackend "github.com/blackbirdworks/gopherstack/services/networkmonitor"
	omicsbackend "github.com/blackbirdworks/gopherstack/services/omics"
	opensearchbackend "github.com/blackbirdworks/gopherstack/services/opensearch"
	opsworksbackend "github.com/blackbirdworks/gopherstack/services/opsworks"
	organizationsbackend "github.com/blackbirdworks/gopherstack/services/organizations"
	outpostsbackend "github.com/blackbirdworks/gopherstack/services/outposts"
	personalizebackend "github.com/blackbirdworks/gopherstack/services/personalize"
	pinpointbackend "github.com/blackbirdworks/gopherstack/services/pinpoint"
	pollybackend "github.com/blackbirdworks/gopherstack/services/polly"
	quicksightbackend "github.com/blackbirdworks/gopherstack/services/quicksight"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	redshiftdatabackend "github.com/blackbirdworks/gopherstack/services/redshiftdata"
	rekognitionbackend "github.com/blackbirdworks/gopherstack/services/rekognition"
	resiliencehubbackend "github.com/blackbirdworks/gopherstack/services/resiliencehub"
	resourcegroupsbackend "github.com/blackbirdworks/gopherstack/services/resourcegroups"
	recoverycontrolbackend "github.com/blackbirdworks/gopherstack/services/route53recoverycontrolconfig"
	route53resolverbackend "github.com/blackbirdworks/gopherstack/services/route53resolver"
	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
	serverlessrepobackend "github.com/blackbirdworks/gopherstack/services/serverlessrepo"
	servicequotasbackend "github.com/blackbirdworks/gopherstack/services/servicequotas"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	supportbackend "github.com/blackbirdworks/gopherstack/services/support"
	textractbackend "github.com/blackbirdworks/gopherstack/services/textract"
	timestreaminfluxdbbackend "github.com/blackbirdworks/gopherstack/services/timestreaminfluxdb"
	transcribebackend "github.com/blackbirdworks/gopherstack/services/transcribe"
	translatebackend "github.com/blackbirdworks/gopherstack/services/translate"
	vpclatticebackend "github.com/blackbirdworks/gopherstack/services/vpclattice"
	workmailbackend "github.com/blackbirdworks/gopherstack/services/workmail"
	workspacesbackend "github.com/blackbirdworks/gopherstack/services/workspaces"
)

// lifecycleApplier sets one service's dwell on its backend; it is a no-op if the handler or backend type differs.
type lifecycleApplier func(reg service.Registerable, l cliconfig.LifecycleSettings)

func applyOpenSearch(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*opensearchbackend.Handler); ok {
		if bk, isBk := h.Backend.(*opensearchbackend.InMemoryBackend); isBk {
			bk.SetProcessingDelay(l.Effective(l.OpenSearchProcessing))
		}
	}
}

func applyElastiCache(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*elasticachebackend.Handler); ok {
		if bk, isBk := h.Backend.(*elasticachebackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.ElastiCache))
		}
	}
}

func applyMemoryDB(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*memorydbbackend.Handler); ok {
		if bk, isBk := h.Backend.(*memorydbbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.MemoryDB))
		}
	}
}

func applyLambda(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*lambdabackend.Handler); ok {
		if bk, isBk := h.Backend.(*lambdabackend.InMemoryBackend); isBk {
			bk.SetActivationDelay(l.Effective(l.LambdaActivation))
			bk.SetProvisionedConcurrencyDelay(l.Effective(l.LambdaProvisionedConcurrency))
		}
	}
}

func applyECS(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*ecsbackend.Handler); ok {
		if bk, isBk := h.Backend.(*ecsbackend.InMemoryBackend); isBk {
			bk.SetStartDelay(l.Effective(l.ECSStart))
			bk.SetStopDelay(l.Effective(l.ECSStop))
		}
	}
}

func applyMediaStore(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*mediastorebackend.Handler); ok {
		if bk, isBk := h.Backend.(*mediastorebackend.InMemoryBackend); isBk {
			bk.SetActivationDelay(l.Effective(l.MediaStore))
		}
	}
}

func applyEFS(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*efsbackend.Handler); ok {
		h.Backend.SetFileSystemActivationDelay(l.Effective(l.EFS))
	}
}

func applyRedshift(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*redshiftbackend.Handler); ok {
		if bk, isBk := h.Backend.(*redshiftbackend.InMemoryBackend); isBk {
			bk.SetClusterActivationDelay(l.Effective(l.Redshift))
		}
	}
}

func applyRedshiftData(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*redshiftdatabackend.Handler); ok {
		if bk, isBk := h.Backend.(*redshiftdatabackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.RedshiftData))
		}
	}
}

func applyVPCLattice(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*vpclatticebackend.Handler); ok {
		if bk, isBk := h.Backend.(*vpclatticebackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.VPCLattice))
		}
	}
}

func applyKafkaConnect(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*kafkaconnectbackend.Handler); ok {
		if bk, isBk := h.Backend.(*kafkaconnectbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.KafkaConnect))
		}
	}
}

func applyGrafana(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*grafanabackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.Grafana))
	}
}

func applyDAX(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*daxbackend.Handler); ok {
		if bk, isBk := h.Backend.(*daxbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.DAX))
		}
	}
}

func applyPinpoint(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*pinpointbackend.Handler); ok {
		if bk, isBk := h.Backend.(*pinpointbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.Pinpoint))
		}
	}
}

func applyAutoscaling(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*autoscalingbackend.Handler); ok {
		if bk, isBk := h.Backend.(*autoscalingbackend.InMemoryBackend); isBk {
			bk.SetLaunchDelay(l.Effective(l.Autoscaling))
		}
	}
}

func applyACMPCA(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*acmpcabackend.Handler); ok {
		h.Backend.SetAuditReportDelay(l.Effective(l.ACMPCA))
	}
}

func applyAccessAnalyzer(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*accessanalyzerbackend.Handler); ok {
		if bk, isBk := h.Backend.(*accessanalyzerbackend.InMemoryBackend); isBk {
			bk.SetAnalyzerCreateDelay(l.Effective(l.AccessAnalyzer))
		}
	}
}

func applyAppStream(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*appstreambackend.Handler); ok {
		if bk, isBk := h.Backend.(*appstreambackend.InMemoryBackend); isBk {
			bk.SetStateTransitionDelay(l.Effective(l.AppStream))
		}
	}
}

func applyFIS(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*fisbackend.Handler); ok {
		if bk, isBk := h.Backend.(*fisbackend.InMemoryBackend); isBk {
			if d := l.Effective(l.FIS); d > 0 {
				bk.SetLifecycleDelay(d)
			}
		}
	}
}

func applyDSQL(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*dsqlbackend.Handler); ok {
		if bk, isBk := h.Backend.(*dsqlbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.DSQL))
		}
	}
}

func applyNetworkFirewall(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*networkfirewallbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.NetworkFirewall))
	}
}

func applyGlobalAccelerator(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*globalacceleratorbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.GlobalAccelerator))
	}
}

func applyBedrockAgentCoreControl(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*bedrockagentcorecontrolbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.BedrockAgentCore))
	}
}

func applyAMP(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*ampbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.AMP))
	}
}

func applyTimestreamInfluxDB(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*timestreaminfluxdbbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.TimestreamInfluxDB))
	}
}

func applyServiceQuotas(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*servicequotasbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.ServiceQuotas))
	}
}

func applyOrganizations(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*organizationsbackend.Handler); ok {
		if bk, isBk := h.Backend.(*organizationsbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.Organizations))
		}
	}
}

func applyBackup(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*backupbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.Backup))
	}
}

func applyMacie2(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*macie2backend.Handler); ok {
		if bk, isBk := h.Backend.(*macie2backend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.Macie2))
		}
	}
}

func applyDetective(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*detectivebackend.Handler); ok {
		if bk, isBk := h.Backend.(*detectivebackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.Detective))
		}
	}
}

func applyWorkMail(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*workmailbackend.Handler); ok {
		if bk, isBk := h.Backend.(*workmailbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.WorkMail))
		}
	}
}

func applyPersonalize(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*personalizebackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.Personalize))
	}
}

func applyOpsWorks(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*opsworksbackend.Handler); ok {
		if bk, isBk := h.Backend.(*opsworksbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.OpsWorks))
		}
	}
}

func applyOutposts(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*outpostsbackend.Handler); ok {
		if d := l.Effective(l.Outposts); d > 0 {
			h.Backend.SetTransitionDelay(d)
		}
	}
}

func applySupport(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*supportbackend.Handler); ok {
		if bk, isBk := h.Backend.(*supportbackend.InMemoryBackend); isBk {
			bk.SetCaseProgressDelay(l.Effective(l.Support))
		}
	}
}

func applyMWAA(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*mwaabackend.Handler); ok {
		if bk, isBk := h.Backend.(*mwaabackend.InMemoryBackend); isBk {
			bk.SetTransitionDelay(l.Effective(l.MWAA))
		}
	}
}

func applyOmics(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*omicsbackend.Handler); ok {
		if bk, isBk := h.Backend.(*omicsbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.Omics))
		}
	}
}

func applyResilienceHub(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*resiliencehubbackend.Handler); ok {
		h.Backend.SetTransitionDelay(l.Effective(l.ResilienceHub))
	}
}

func applyDataBrew(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*databrewbackend.Handler); ok {
		if bk, isBk := h.Backend.(*databrewbackend.InMemoryBackend); isBk {
			bk.SetJobRunDelay(l.Effective(l.DataBrew))
		}
	}
}

func applyMGN(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*mgnbackend.Handler); ok {
		h.Backend.SetTransitionDelay(l.Effective(l.MGN))
	}
}

func applyEKS(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*eksbackend.Handler); ok {
		if d := l.Effective(l.EKS); d > 0 {
			h.Backend.SetLifecycleDelay(d)
		}
	}
}

func applyCloudControl(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*cloudcontrolbackend.Handler); ok {
		h.Backend.SetOperationDelay(l.Effective(l.CloudControl))
	}
}

func applyMediaPackage(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*mediapackagebackend.Handler); ok {
		if bk, isBk := h.Backend.(*mediapackagebackend.InMemoryBackend); isBk {
			if d := l.Effective(l.MediaPackage); d > 0 {
				bk.SetHarvestJobDuration(d)
			}
		}
	}
}

func applyCloudFront(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*cloudfrontbackend.Handler); ok {
		h.Backend.SetDistributionDeployDelay(l.Effective(l.CloudFront))
	}
}

func applySSM(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*ssmbackend.Handler); ok {
		if bk, isBk := h.Backend.(*ssmbackend.InMemoryBackend); isBk {
			bk.WithCommandExecDelay(l.Effective(l.SSMCommand))
			bk.WithAutomationExecDelay(l.Effective(l.SSMAutomation))
		}
	}
}

func applyDocDB(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*docdbbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.DocDB))
	}
}

func applyNeptune(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*neptunebackend.Handler); ok {
		if bk, isBk := h.Backend.(*neptunebackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.Neptune))
		}
	}
}

func applyKafka(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*kafkabackend.Handler); ok {
		if bk, isBk := h.Backend.(*kafkabackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.Kafka))
		}
	}
}

func applyAWSConfig(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*awsconfigbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.AWSConfig))
	}
}

func applyInspector2(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*inspector2backend.Handler); ok {
		if bk, isBk := h.Backend.(*inspector2backend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.Inspector2))
		}
	}
}

func applyWorkSpaces(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*workspacesbackend.Handler); ok {
		if bk, isBk := h.Backend.(*workspacesbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.WorkSpaces))
		}
	}
}

func applyFSx(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*fsxbackend.Handler); ok {
		if bk, isBk := h.Backend.(*fsxbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.FSx))
		}
	}
}

func applyNetworkManager(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*networkmanagerbackend.Handler); ok {
		if d := l.Effective(l.NetworkManager); d > 0 {
			h.Backend.SetLifecycleDelay(d)
		}
	}
}

func applyAccount(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*accountbackend.Handler); ok {
		if bk, isBk := h.Backend.(*accountbackend.InMemoryBackend); isBk {
			if d := l.Effective(l.Account); d > 0 {
				bk.SetLifecycleDelay(d)
			}
		}
	}
}

func applyAppRunner(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*apprunnerbackend.Handler); ok {
		if bk, isBk := h.Backend.(*apprunnerbackend.InMemoryBackend); isBk {
			bk.SetOperationDelay(l.Effective(l.AppRunner))
		}
	}
}

func applyKinesisAnalyticsV2(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*kinesisanalyticsv2backend.Handler); ok {
		if bk, isBk := h.Backend.(*kinesisanalyticsv2backend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.KinesisAnalyticsV2))
		}
	}
}

func applyQuickSight(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*quicksightbackend.Handler); ok {
		if bk, isBk := h.Backend.(*quicksightbackend.InMemoryBackend); isBk {
			bk.SetCreationDelay(l.Effective(l.QuickSight))

			if l.QuickSightIngestion > 0 {
				bk.SetIngestionDelay(l.QuickSightIngestion)
			}
		}
	}
}

func applySageMaker(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*sagemakerbackend.Handler); ok {
		if d := l.Effective(l.SageMaker); d > 0 {
			h.Backend.SetLifecycleDelay(d)
		}
	}
}

func applyBedrock(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*bedrockbackend.Handler); ok {
		if d := l.Effective(l.Bedrock); d > 0 {
			h.Backend.SetJobCompletionDelay(d)
		}
	}
}

func applyBedrockAgent(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*bedrockagentbackend.Handler); ok {
		if bk, isBk := h.Backend.(*bedrockagentbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.BedrockAgent))
		}
	}
}

func applyElasticBeanstalk(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*elasticbeanstalkbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.ElasticBeanstalk))
	}
}

func applyRoute53Resolver(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*route53resolverbackend.Handler); ok {
		if bk, isBk := h.Backend.(*route53resolverbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.Route53Resolver))
		}
	}
}

func applyDirectoryService(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*directoryservicebackend.Handler); ok {
		if bk, isBk := h.Backend.(*directoryservicebackend.InMemoryBackend); isBk {
			if d := l.Effective(l.DirectoryService); d > 0 {
				bk.SetLifecycleDelay(d)
			}
		}
	}
}

func applyMediaLive(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*medialivebackend.Handler); ok {
		if bk, isBk := h.Backend.(*medialivebackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.MediaLive))
		}
	}
}

func applyComprehend(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*comprehendbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.Comprehend))
	}
}

func applyRekognition(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*rekognitionbackend.Handler); ok {
		if bk, isBk := h.Backend.(*rekognitionbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.Rekognition))
		}
	}
}

func applyTextract(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*textractbackend.Handler); ok {
		if bk, isBk := h.Backend.(*textractbackend.InMemoryBackend); isBk {
			if d := l.Textract; d > 0 {
				bk.SetAsyncJobDelay(d)
			}
		}
	}
}

func applyTranslate(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*translatebackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.Translate))
	}
}

func applyPolly(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*pollybackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.Effective(l.Polly))
	}
}

func applyTranscribe(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*transcribebackend.Handler); ok {
		if bk, isBk := h.Backend.(*transcribebackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.Transcribe))
		}
	}
}

func applyIoTAnalytics(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*iotanalyticsbackend.Handler); ok {
		if bk, isBk := h.Backend.(*iotanalyticsbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.IoTAnalytics))
		}
	}
}

func applyIoTWireless(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*iotwirelessbackend.Handler); ok {
		if bk, isBk := h.Backend.(*iotwirelessbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.IoTWireless))
		}
	}
}

func applyForecast(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*forecastbackend.Handler); ok {
		if bk := h.Backend; bk != nil {
			bk.SetCreateDelay(l.Effective(l.Forecast))
		}
	}
}

func applyNetworkMonitor(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*networkmonitorbackend.Handler); ok {
		if bk, isBk := h.Backend.(*networkmonitorbackend.InMemoryBackend); isBk {
			bk.SetActivationDelay(l.Effective(l.NetworkMonitor))
		}
	}
}

func applyManagedBlockchain(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*managedblockchainbackend.Handler); ok {
		if bk, isBk := h.Backend.(*managedblockchainbackend.InMemoryBackend); isBk {
			bk.SetCreateDelay(l.Effective(l.ManagedBlockchain))
		}
	}
}

func applyCleanRooms(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*cleanroomsbackend.Handler); ok {
		if bk, isBk := h.Backend.(*cleanroomsbackend.InMemoryBackend); isBk {
			bk.SetProtectedWorkDelay(l.Effective(l.CleanRooms))
		}
	}
}

func applyResourceGroups(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*resourcegroupsbackend.Handler); ok {
		if bk, isBk := h.Backend.(*resourcegroupsbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.Effective(l.ResourceGroups))
		}
	}
}

func applyServerlessRepo(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*serverlessrepobackend.Handler); ok {
		if bk, isBk := h.Backend.(*serverlessrepobackend.InMemoryBackend); isBk {
			bk.SetTemplatePrepareDelay(l.Effective(l.ServerlessRepo))
		}
	}
}

func applyCodeConnections(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*codeconnectionsbackend.Handler); ok {
		if bk := h.Backend; bk != nil {
			d := l.Effective(l.CodeConnections)
			bk.SetConnectionHandshakeDelay(d)
			bk.SetHostSetupDelay(d)
		}
	}
}

func applyCodeStarConnections(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*codestarconnectionsbackend.Handler); ok {
		if bk := h.Backend; bk != nil {
			d := l.Effective(l.CodeStarConnections)
			if d > 0 {
				bk.SetConnectionHandshakeDelay(d)
				bk.SetHostSetupDelay(d)
			}
		}
	}
}

func applyRoute53Recovery(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*recoverycontrolbackend.Handler); ok {
		h.Backend.SetActivationDelay(l.Effective(l.Route53Recovery))
	}
}

func applyARCZonalShift(reg service.Registerable, l cliconfig.LifecycleSettings) {
	if h, ok := reg.(*arczonalshiftbackend.Handler); ok {
		h.Backend.SetPracticeRunDuration(l.ARCZonalShift)
	}
}

// wireLifecycleDelays applies the configured dwell times to the already
// initialized service backends; zero values leave services instant.
func wireLifecycleDelays(byName map[string]service.Registerable, l cliconfig.LifecycleSettings) {
	appliers := map[string]lifecycleApplier{
		"OpenSearch":   applyOpenSearch,
		"ElastiCache":  applyElastiCache,
		"MemoryDB":     applyMemoryDB,
		"Lambda":       applyLambda,
		"ECS":          applyECS,
		"MediaStore":   applyMediaStore,
		"EFS":          applyEFS,
		"Redshift":     applyRedshift,
		"RedshiftData": applyRedshiftData,
		"RedshiftServerless": func(r service.Registerable, l cliconfig.LifecycleSettings) {
			applyRedshiftServerless(r, l.Effective(l.RedshiftServerless))
		},
		"DAX":                 applyDAX,
		"VPCLattice":          applyVPCLattice,
		"KafkaConnect":        applyKafkaConnect,
		"Grafana":             applyGrafana,
		"ACMPCA":              applyACMPCA,
		"AccessAnalyzer":      applyAccessAnalyzer,
		"AppStream":           applyAppStream,
		"FIS":                 applyFIS,
		"DSQL":                applyDSQL,
		"GlobalAccelerator":   applyGlobalAccelerator,
		"AMP":                 applyAMP,
		"TimestreamInfluxDB":  applyTimestreamInfluxDB,
		"ServiceQuotas":       applyServiceQuotas,
		"NetworkFirewall":     applyNetworkFirewall,
		"WorkMail":            applyWorkMail,
		"Personalize":         applyPersonalize,
		"OpsWorks":            applyOpsWorks,
		"Outposts":            applyOutposts,
		"MWAA":                applyMWAA,
		"Omics":               applyOmics,
		"ResilienceHub":       applyResilienceHub,
		"DataBrew":            applyDataBrew,
		"MGN":                 applyMGN,
		"Support":             applySupport,
		"Detective":           applyDetective,
		"Macie2":              applyMacie2,
		"Organizations":       applyOrganizations,
		"Backup":              applyBackup,
		"Pinpoint":            applyPinpoint,
		"Autoscaling":         applyAutoscaling,
		"EKS":                 applyEKS,
		"CloudControl":        applyCloudControl,
		"MediaPackage":        applyMediaPackage,
		"CloudFront":          applyCloudFront,
		"SSM":                 applySSM,
		"DocDB":               applyDocDB,
		"Neptune":             applyNeptune,
		"Kafka":               applyKafka,
		"AWSConfig":           applyAWSConfig,
		"Inspector2":          applyInspector2,
		"WorkSpaces":          applyWorkSpaces,
		"QuickSight":          applyQuickSight,
		"AppRunner":           applyAppRunner,
		"KinesisAnalyticsV2":  applyKinesisAnalyticsV2,
		"SageMaker":           applySageMaker,
		"Bedrock":             applyBedrock,
		"BedrockAgent":        applyBedrockAgent,
		"Elasticbeanstalk":    applyElasticBeanstalk,
		"Route53Resolver":     applyRoute53Resolver,
		"DirectoryService":    applyDirectoryService,
		"MediaLive":           applyMediaLive,
		"FSx":                 applyFSx,
		"NetworkManager":      applyNetworkManager,
		"Account":             applyAccount,
		"IoTAnalytics":        applyIoTAnalytics,
		"IoTWireless":         applyIoTWireless,
		"Comprehend":          applyComprehend,
		"Rekognition":         applyRekognition,
		"Textract":            applyTextract,
		"Translate":           applyTranslate,
		"Polly":               applyPolly,
		"Transcribe":          applyTranscribe,
		"Forecast":            applyForecast,
		"NetworkMonitor":      applyNetworkMonitor,
		"ManagedBlockchain":   applyManagedBlockchain,
		"CleanRooms":          applyCleanRooms,
		"ResourceGroups":      applyResourceGroups,
		"ServerlessRepo":      applyServerlessRepo,
		"CodeConnections":     applyCodeConnections,
		"CodeStarConnections": applyCodeStarConnections,

		"Route53RecoveryControlConfig": applyRoute53Recovery,
		"ARCZonalShift":                applyARCZonalShift,
		"BedrockAgentCoreControl":      applyBedrockAgentCoreControl,
	}

	for name, apply := range appliers {
		if reg, ok := byName[name]; ok {
			apply(reg, l)
		}
	}
}
