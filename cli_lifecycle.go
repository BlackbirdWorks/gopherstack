package main

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	accessanalyzerbackend "github.com/blackbirdworks/gopherstack/services/accessanalyzer"
	accountbackend "github.com/blackbirdworks/gopherstack/services/account"
	acmpcabackend "github.com/blackbirdworks/gopherstack/services/acmpca"
	apprunnerbackend "github.com/blackbirdworks/gopherstack/services/apprunner"
	appstreambackend "github.com/blackbirdworks/gopherstack/services/appstream"
	arczonalshiftbackend "github.com/blackbirdworks/gopherstack/services/arczonalshift"
	autoscalingbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	awsconfigbackend "github.com/blackbirdworks/gopherstack/services/awsconfig"
	bedrockbackend "github.com/blackbirdworks/gopherstack/services/bedrock"
	bedrockagentbackend "github.com/blackbirdworks/gopherstack/services/bedrockagent"
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
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	supportbackend "github.com/blackbirdworks/gopherstack/services/support"
	textractbackend "github.com/blackbirdworks/gopherstack/services/textract"
	transcribebackend "github.com/blackbirdworks/gopherstack/services/transcribe"
	translatebackend "github.com/blackbirdworks/gopherstack/services/translate"
	vpclatticebackend "github.com/blackbirdworks/gopherstack/services/vpclattice"
	workmailbackend "github.com/blackbirdworks/gopherstack/services/workmail"
	workspacesbackend "github.com/blackbirdworks/gopherstack/services/workspaces"
)

// LifecycleSettings holds the opt-in dwell times for transitional resource
// states (Creating, Processing, Pending, InProgress). Every default is 0, so
// resources settle instantly unless a client-visible window is requested.
// A per-service value of 0 falls back to Delay.
type LifecycleSettings struct {
	Delay                        time.Duration `name:"delay"                          env:"GOPHERSTACK_LIFECYCLE_DELAY"          default:"0s" help:"Default dwell in transitional states for every service below; 0 settles instantly."`                  //nolint:lll // config struct tags are intentionally verbose
	OpenSearchProcessing         time.Duration `name:"opensearch-processing"          env:"OPENSEARCH_PROCESSING_DELAY"          default:"0s" help:"OpenSearch domain Processing window."`                                                                //nolint:lll // config struct tags are intentionally verbose
	ElastiCache                  time.Duration `name:"elasticache"                    env:"ELASTICACHE_LIFECYCLE_DELAY"          default:"0s" help:"ElastiCache creating dwell."`                                                                         //nolint:lll // config struct tags are intentionally verbose
	MemoryDB                     time.Duration `name:"memorydb"                       env:"MEMORYDB_LIFECYCLE_DELAY"             default:"0s" help:"MemoryDB cluster creating dwell."`                                                                    //nolint:lll // config struct tags are intentionally verbose
	LambdaActivation             time.Duration `name:"lambda-activation"              env:"LAMBDA_ACTIVATION_DELAY"              default:"0s" help:"Lambda function Pending window."`                                                                     //nolint:lll // config struct tags are intentionally verbose
	LambdaProvisionedConcurrency time.Duration `name:"lambda-provisioned-concurrency" env:"LAMBDA_PROVISIONED_CONCURRENCY_DELAY" default:"0s" help:"Lambda provisioned concurrency IN_PROGRESS window."`                                                  //nolint:lll // config struct tags are intentionally verbose
	ECSStart                     time.Duration `name:"ecs-start"                      env:"ECS_START_DELAY"                      default:"0s" help:"ECS per-phase task start delay."`                                                                     //nolint:lll // config struct tags are intentionally verbose
	ECSStop                      time.Duration `name:"ecs-stop"                       env:"ECS_STOP_DELAY"                       default:"0s" help:"ECS per-phase task stop delay."`                                                                      //nolint:lll // config struct tags are intentionally verbose
	MediaStore                   time.Duration `name:"mediastore"                     env:"MEDIASTORE_ACTIVATION_DELAY"          default:"0s" help:"MediaStore container CREATING/DELETING window."`                                                      //nolint:lll // config struct tags are intentionally verbose
	EFS                          time.Duration `name:"efs"                            env:"EFS_ACTIVATION_DELAY"                 default:"0s" help:"EFS file system creating window."`                                                                    //nolint:lll // config struct tags are intentionally verbose
	Redshift                     time.Duration `name:"redshift"                       env:"REDSHIFT_ACTIVATION_DELAY"            default:"0s" help:"Redshift cluster creating window."`                                                                   //nolint:lll // config struct tags are intentionally verbose
	SSMCommand                   time.Duration `name:"ssm-command"                    env:"SSM_COMMAND_EXEC_DELAY"               default:"0s" help:"SSM SendCommand InProgress window."`                                                                  //nolint:lll // config struct tags are intentionally verbose
	CloudFront                   time.Duration `name:"cloudfront"                     env:"CLOUDFRONT_DEPLOY_DELAY"              default:"0s" help:"CloudFront distribution InProgress window; 0 keeps the built-in 100ms."`                              //nolint:lll // config struct tags are intentionally verbose
	SSMAutomation                time.Duration `name:"ssm-automation"                 env:"SSM_AUTOMATION_EXEC_DELAY"            default:"0s" help:"SSM automation execution InProgress window."`                                                         //nolint:lll // config struct tags are intentionally verbose
	DocDB                        time.Duration `name:"docdb"                          env:"DOCDB_LIFECYCLE_DELAY"                default:"0s" help:"DocumentDB cluster and instance creating dwell."`                                                     //nolint:lll // config struct tags are intentionally verbose
	Neptune                      time.Duration `name:"neptune"                        env:"NEPTUNE_LIFECYCLE_DELAY"              default:"0s" help:"Neptune cluster and instance creating dwell."`                                                        //nolint:lll // config struct tags are intentionally verbose
	Kafka                        time.Duration `name:"kafka"                          env:"KAFKA_LIFECYCLE_DELAY"                default:"0s" help:"MSK cluster creating and update window."`                                                             //nolint:lll // config struct tags are intentionally verbose
	AWSConfig                    time.Duration `name:"awsconfig"                      env:"AWSCONFIG_LIFECYCLE_DELAY"            default:"0s" help:"AWS Config conformance pack CREATE_IN_PROGRESS window."`                                              //nolint:lll // config struct tags are intentionally verbose
	Inspector2                   time.Duration `name:"inspector2"                     env:"INSPECTOR2_LIFECYCLE_DELAY"           default:"0s" help:"Inspector2 ENABLING/DISABLING window."`                                                               //nolint:lll // config struct tags are intentionally verbose
	WorkSpaces                   time.Duration `name:"workspaces"                     env:"WORKSPACES_LIFECYCLE_DELAY"           default:"0s" help:"WorkSpaces PENDING and directory REGISTERING window."`                                                //nolint:lll // config struct tags are intentionally verbose
	QuickSight                   time.Duration `name:"quicksight"                     env:"QUICKSIGHT_CREATION_DELAY"            default:"0s" help:"QuickSight dashboard, analysis and data source CREATION_IN_PROGRESS window."`                         //nolint:lll // config struct tags are intentionally verbose
	AppRunner                    time.Duration `name:"apprunner"                      env:"APPRUNNER_OPERATION_DELAY"            default:"0s" help:"App Runner service OPERATION_IN_PROGRESS window after each operation."`                               //nolint:lll // config struct tags are intentionally verbose
	KinesisAnalyticsV2           time.Duration `name:"kinesisanalyticsv2"             env:"KINESISANALYTICSV2_LIFECYCLE_DELAY"   default:"0s" help:"Managed Flink application STARTING/STOPPING/UPDATING window."`                                        //nolint:lll // config struct tags are intentionally verbose
	QuickSightIngestion          time.Duration `name:"quicksight-ingestion"           env:"QUICKSIGHT_INGESTION_DELAY"           default:"0s" help:"QuickSight manual ingestion RUNNING window; 0 keeps the built-in 1s; ignores the global delay."`      //nolint:lll // config struct tags are intentionally verbose
	SageMaker                    time.Duration `name:"sagemaker"                      env:"SAGEMAKER_LIFECYCLE_DELAY"            default:"0s" help:"SageMaker transitional-state dwell; 0 keeps the built-in dwell."`                                     //nolint:lll // config struct tags are intentionally verbose
	Bedrock                      time.Duration `name:"bedrock"                        env:"BEDROCK_JOB_COMPLETION_DELAY"         default:"0s" help:"Bedrock job InProgress window; 0 keeps the built-in window."`                                         //nolint:lll // config struct tags are intentionally verbose
	BedrockAgent                 time.Duration `name:"bedrockagent"                   env:"BEDROCKAGENT_LIFECYCLE_DELAY"         default:"0s" help:"Bedrock Agents CREATING/PREPARING/UPDATING/ingestion window."`                                        //nolint:lll // config struct tags are intentionally verbose
	ElasticBeanstalk             time.Duration `name:"elasticbeanstalk"               env:"ELASTICBEANSTALK_LIFECYCLE_DELAY"     default:"0s" help:"Elastic Beanstalk environment Launching/Updating/Terminating window."`                                //nolint:lll // config struct tags are intentionally verbose
	Route53Resolver              time.Duration `name:"route53resolver"                env:"ROUTE53RESOLVER_LIFECYCLE_DELAY"      default:"0s" help:"Route 53 Resolver endpoint CREATING/UPDATING and rule association CREATING window."`                  //nolint:lll // config struct tags are intentionally verbose
	DirectoryService             time.Duration `name:"directoryservice"               env:"DIRECTORYSERVICE_LIFECYCLE_DELAY"     default:"0s" help:"Directory Service transitional-state window; 0 keeps the built-in sub-second dwell."`                 //nolint:lll // config struct tags are intentionally verbose
	MediaLive                    time.Duration `name:"medialive"                      env:"MEDIALIVE_LIFECYCLE_DELAY"            default:"0s" help:"MediaLive channel/multiplex CREATING/STARTING/STOPPING/DELETING window."`                             //nolint:lll // config struct tags are intentionally verbose
	FSx                          time.Duration `name:"fsx"                            env:"FSX_LIFECYCLE_DELAY"                  default:"0s" help:"FSx resource CREATING window."`                                                                       //nolint:lll // config struct tags are intentionally verbose
	NetworkManager               time.Duration `name:"networkmanager"                 env:"NETWORKMANAGER_LIFECYCLE_DELAY"       default:"0s" help:"Network Manager PENDING/DELETING window (built-in 100ms)."`                                           //nolint:lll // config struct tags are intentionally verbose
	Comprehend                   time.Duration `name:"comprehend"                     env:"COMPREHEND_LIFECYCLE_DELAY"           default:"0s" help:"Comprehend async job and classifier/recognizer training stage window."`                               //nolint:lll // config struct tags are intentionally verbose
	Rekognition                  time.Duration `name:"rekognition"                    env:"REKOGNITION_LIFECYCLE_DELAY"          default:"0s" help:"Rekognition stream processor, project version and dataset transitional window."`                      //nolint:lll // config struct tags are intentionally verbose
	Textract                     time.Duration `name:"textract"                       env:"TEXTRACT_ASYNC_JOB_DELAY"             default:"0s" help:"Textract async job IN_PROGRESS window; 0 keeps the built-in 200ms; ignores the global delay."`        //nolint:lll // config struct tags are intentionally verbose
	Translate                    time.Duration `name:"translate"                      env:"TRANSLATE_LIFECYCLE_DELAY"            default:"0s" help:"Translate batch job and parallel data stage window."`                                                 //nolint:lll // config struct tags are intentionally verbose
	Polly                        time.Duration `name:"polly"                          env:"POLLY_LIFECYCLE_DELAY"                default:"0s" help:"Polly speech synthesis task stage window."`                                                           //nolint:lll // config struct tags are intentionally verbose
	Transcribe                   time.Duration `name:"transcribe"                     env:"TRANSCRIBE_LIFECYCLE_DELAY"           default:"0s" help:"Transcribe job, vocabulary and language model IN_PROGRESS/PENDING window."`                           //nolint:lll // config struct tags are intentionally verbose
	IoTAnalytics                 time.Duration `name:"iotanalytics"                   env:"IOTANALYTICS_LIFECYCLE_DELAY"         default:"0s" help:"IoT Analytics channel/datastore/dataset CREATING and dataset content CREATING window."`               //nolint:lll // config struct tags are intentionally verbose
	IoTWireless                  time.Duration `name:"iotwireless"                    env:"IOTWIRELESS_LIFECYCLE_DELAY"          default:"0s" help:"IoT Wireless FUOTA task FuotaSession_Waiting/In_FuotaSession stage window."`                          //nolint:lll // config struct tags are intentionally verbose
	Organizations                time.Duration `name:"organizations"                  env:"ORGANIZATIONS_LIFECYCLE_DELAY"        default:"0s" help:"Organizations CreateAccount/CreateGovCloudAccount IN_PROGRESS window."`                               //nolint:lll // config struct tags are intentionally verbose
	Account                      time.Duration `name:"account"                        env:"ACCOUNT_LIFECYCLE_DELAY"              default:"0s" help:"Account region ENABLING/DISABLING window (built-in 100ms)."`                                          //nolint:lll // config struct tags are intentionally verbose
	RedshiftData                 time.Duration `name:"redshiftdata"                   env:"REDSHIFTDATA_LIFECYCLE_DELAY"         default:"0s" help:"Redshift Data statement SUBMITTED/STARTED window before FINISHED."`                                   //nolint:lll // config struct tags are intentionally verbose
	RedshiftServerless           time.Duration `name:"redshiftserverless"             env:"REDSHIFTSERVERLESS_LIFECYCLE_DELAY"   default:"0s" help:"Redshift Serverless workgroup CREATING/MODIFYING/DELETING and namespace MODIFYING/DELETING window."`  //nolint:lll // config struct tags are intentionally verbose
	DAX                          time.Duration `name:"dax"                            env:"DAX_LIFECYCLE_DELAY"                  default:"0s" help:"DAX cluster and node creating/modifying/deleting window (built-in 1s)."`                              //nolint:lll // config struct tags are intentionally verbose
	Pinpoint                     time.Duration `name:"pinpoint"                       env:"PINPOINT_LIFECYCLE_DELAY"             default:"0s" help:"Pinpoint export/import job CREATED/PROCESSING window before COMPLETED."`                              //nolint:lll // config struct tags are intentionally verbose
	Autoscaling                  time.Duration `name:"autoscaling"                    env:"AUTOSCALING_LAUNCH_DELAY"             default:"0s" help:"Auto Scaling instance Pending window before InService."`                                              //nolint:lll // config struct tags are intentionally verbose
	VPCLattice                   time.Duration `name:"vpclattice"                     env:"VPCLATTICE_LIFECYCLE_DELAY"           default:"0s" help:"VPC Lattice service/target group/association CREATE_IN_PROGRESS and target INITIAL window."`          //nolint:lll // config struct tags are intentionally verbose
	KafkaConnect                 time.Duration `name:"kafkaconnect"                   env:"KAFKACONNECT_LIFECYCLE_DELAY"         default:"0s" help:"MSK Connect connector/plugin transitional window (built-in 500ms create, 300ms delete)."`             //nolint:lll // config struct tags are intentionally verbose
	Grafana                      time.Duration `name:"grafana"                        env:"GRAFANA_LIFECYCLE_DELAY"              default:"0s" help:"Grafana workspace CREATING/UPDATING transitional window (built-in 100ms)."`                           //nolint:lll // config struct tags are intentionally verbose
	ACMPCA                       time.Duration `name:"acmpca"                         env:"ACMPCA_AUDIT_REPORT_DELAY"            default:"0s" help:"ACM PCA audit report CREATING window."`                                                               //nolint:lll // config struct tags are intentionally verbose
	AccessAnalyzer               time.Duration `name:"accessanalyzer"                 env:"ACCESSANALYZER_LIFECYCLE_DELAY"       default:"0s" help:"IAM Access Analyzer analyzer CREATING window."`                                                       //nolint:lll // config struct tags are intentionally verbose
	AppStream                    time.Duration `name:"appstream"                      env:"APPSTREAM_LIFECYCLE_DELAY"            default:"0s" help:"AppStream fleet and image builder STARTING/STOPPING/PENDING window."`                                 //nolint:lll // config struct tags are intentionally verbose
	FIS                          time.Duration `name:"fis"                            env:"FIS_LIFECYCLE_DELAY"                  default:"0s" help:"FIS experiment state step delay (built-in 10ms)."`                                                    //nolint:lll // config struct tags are intentionally verbose
	DSQL                         time.Duration `name:"dsql"                           env:"DSQL_LIFECYCLE_DELAY"                 default:"0s" help:"Aurora DSQL cluster and stream transitional window (built-in 750ms/500ms)."`                          //nolint:lll // config struct tags are intentionally verbose
	Macie2                       time.Duration `name:"macie2"                         env:"MACIE2_LIFECYCLE_DELAY"               default:"0s" help:"Macie one-time classification job RUNNING window (built-in 5s)."`                                     //nolint:lll // config struct tags are intentionally verbose
	Detective                    time.Duration `name:"detective"                      env:"DETECTIVE_LIFECYCLE_DELAY"            default:"0s" help:"Detective investigation RUNNING window."`                                                             //nolint:lll // config struct tags are intentionally verbose
	WorkMail                     time.Duration `name:"workmail"                       env:"WORKMAIL_LIFECYCLE_DELAY"             default:"0s" help:"WorkMail transitional window."`                                                                       //nolint:lll // config struct tags are intentionally verbose
	Personalize                  time.Duration `name:"personalize"                    env:"PERSONALIZE_LIFECYCLE_DELAY"          default:"0s" help:"Personalize resource CREATE PENDING/IN PROGRESS window."`                                             //nolint:lll // config struct tags are intentionally verbose
	OpsWorks                     time.Duration `name:"opsworks"                       env:"OPSWORKS_LIFECYCLE_DELAY"             default:"0s" help:"OpsWorks instance transitional window."`                                                              //nolint:lll // config struct tags are intentionally verbose
	Outposts                     time.Duration `name:"outposts"                       env:"OUTPOSTS_TRANSITION_DELAY"            default:"0s" help:"Outposts transitional window (built-in 100ms)."`                                                      //nolint:lll // config struct tags are intentionally verbose
	Support                      time.Duration `name:"support"                        env:"SUPPORT_CASE_PROGRESS_DELAY"          default:"0s" help:"Support case progress window."`                                                                       //nolint:lll // config struct tags are intentionally verbose
	MWAA                         time.Duration `name:"mwaa"                           env:"MWAA_LIFECYCLE_DELAY"                 default:"0s" help:"MWAA environment CREATING/UPDATING/DELETING window."`                                                 //nolint:lll // config struct tags are intentionally verbose
	Omics                        time.Duration `name:"omics"                          env:"OMICS_LIFECYCLE_DELAY"                default:"0s" help:"HealthOmics workflow/store/share/run and import/export/activation job per-hop window."`               //nolint:lll // config struct tags are intentionally verbose
	ResilienceHub                time.Duration `name:"resiliencehub"                  env:"RESILIENCEHUB_LIFECYCLE_DELAY"        default:"0s" help:"Resilience Hub assessment/import/resolution per-hop window (built-in 100ms)."`                        //nolint:lll // config struct tags are intentionally verbose
	DataBrew                     time.Duration `name:"databrew"                       env:"DATABREW_LIFECYCLE_DELAY"             default:"0s" help:"DataBrew job run STARTING-to-SUCCEEDED window (built-in 100ms)."`                                     //nolint:lll // config struct tags are intentionally verbose
	MGN                          time.Duration `name:"mgn"                            env:"MGN_LIFECYCLE_DELAY"                  default:"0s" help:"Application Migration Service replication/job/import per-hop window (built-in 100ms)."`               //nolint:lll // config struct tags are intentionally verbose
	Forecast                     time.Duration `name:"forecast"                       env:"FORECAST_LIFECYCLE_DELAY"             default:"0s" help:"Forecast resource CREATE_PENDING/CREATE_IN_PROGRESS window."`                                         //nolint:lll // config struct tags are intentionally verbose
	NetworkMonitor               time.Duration `name:"networkmonitor"                 env:"NETWORKMONITOR_LIFECYCLE_DELAY"       default:"0s" help:"Network Monitor monitor and probe PENDING window."`                                                   //nolint:lll // config struct tags are intentionally verbose
	ManagedBlockchain            time.Duration `name:"managedblockchain"              env:"MANAGEDBLOCKCHAIN_LIFECYCLE_DELAY"    default:"0s" help:"Managed Blockchain network/member/node CREATING window."`                                             //nolint:lll // config struct tags are intentionally verbose
	CleanRooms                   time.Duration `name:"cleanrooms"                     env:"CLEANROOMS_LIFECYCLE_DELAY"           default:"0s" help:"Clean Rooms protected query and job SUBMITTED window."`                                               //nolint:lll // config struct tags are intentionally verbose
	ResourceGroups               time.Duration `name:"resourcegroups"                 env:"RESOURCEGROUPS_LIFECYCLE_DELAY"       default:"0s" help:"Resource Groups transitional window."`                                                                //nolint:lll // config struct tags are intentionally verbose
	ServerlessRepo               time.Duration `name:"serverlessrepo"                 env:"SERVERLESSREPO_LIFECYCLE_DELAY"       default:"0s" help:"Serverless Application Repository template PREPARING window."`                                        //nolint:lll // config struct tags are intentionally verbose
	CodeConnections              time.Duration `name:"codeconnections"                env:"CODECONNECTIONS_LIFECYCLE_DELAY"      default:"0s" help:"CodeConnections connection handshake and host setup PENDING window."`                                 //nolint:lll // config struct tags are intentionally verbose
	CodeStarConnections          time.Duration `name:"codestarconnections"            env:"CODESTARCONNECTIONS_LIFECYCLE_DELAY"  default:"0s" help:"CodeStar Connections connection handshake and host setup PENDING window; hosts stay PENDING when 0."` //nolint:lll // config struct tags are intentionally verbose
	CloudControl                 time.Duration `name:"cloudcontrol"                   env:"CLOUDCONTROL_OPERATION_DELAY"         default:"0s" help:"Cloud Control Create/Update/DeleteResource IN_PROGRESS window."`                                      //nolint:lll // config struct tags are intentionally verbose
	MediaPackage                 time.Duration `name:"mediapackage"                   env:"MEDIAPACKAGE_HARVEST_JOB_DELAY"       default:"0s" help:"MediaPackage harvest job IN_PROGRESS window; 0 keeps the built-in 1s."`                               //nolint:lll // config struct tags are intentionally verbose
	Route53Recovery              time.Duration `name:"route53-recovery"               env:"ROUTE53_RECOVERY_LIFECYCLE_DELAY"     default:"0s" help:"Route 53 ARC control plane PENDING/PENDING_DELETION window."`                                         //nolint:lll // config struct tags are intentionally verbose
	ARCZonalShift                time.Duration `name:"arczonalshift"                  env:"ARCZONALSHIFT_PRACTICE_RUN_DURATION"  default:"0s" help:"ARC zonal shift practice run ACTIVE window; 0 keeps the built-in 30m; ignores the global delay."`     //nolint:lll // config struct tags are intentionally verbose
	EKS                          time.Duration `name:"eks"                            env:"EKS_LIFECYCLE_DELAY"                  default:"0s" help:"EKS transitional-state window (built-in 100ms)."`                                                     //nolint:lll // config struct tags are intentionally verbose
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

func applyRedshiftData(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*redshiftdatabackend.Handler); ok {
		if bk, isBk := h.Backend.(*redshiftdatabackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.RedshiftData))
		}
	}
}

func applyVPCLattice(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*vpclatticebackend.Handler); ok {
		if bk, isBk := h.Backend.(*vpclatticebackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.VPCLattice))
		}
	}
}

func applyKafkaConnect(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*kafkaconnectbackend.Handler); ok {
		if bk, isBk := h.Backend.(*kafkaconnectbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.KafkaConnect))
		}
	}
}

func applyGrafana(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*grafanabackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.effective(l.Grafana))
	}
}

func applyDAX(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*daxbackend.Handler); ok {
		if bk, isBk := h.Backend.(*daxbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.DAX))
		}
	}
}

func applyPinpoint(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*pinpointbackend.Handler); ok {
		if bk, isBk := h.Backend.(*pinpointbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.Pinpoint))
		}
	}
}

func applyAutoscaling(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*autoscalingbackend.Handler); ok {
		if bk, isBk := h.Backend.(*autoscalingbackend.InMemoryBackend); isBk {
			bk.SetLaunchDelay(l.effective(l.Autoscaling))
		}
	}
}

func applyACMPCA(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*acmpcabackend.Handler); ok {
		h.Backend.SetAuditReportDelay(l.effective(l.ACMPCA))
	}
}

func applyAccessAnalyzer(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*accessanalyzerbackend.Handler); ok {
		if bk, isBk := h.Backend.(*accessanalyzerbackend.InMemoryBackend); isBk {
			bk.SetAnalyzerCreateDelay(l.effective(l.AccessAnalyzer))
		}
	}
}

func applyAppStream(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*appstreambackend.Handler); ok {
		if bk, isBk := h.Backend.(*appstreambackend.InMemoryBackend); isBk {
			bk.SetStateTransitionDelay(l.effective(l.AppStream))
		}
	}
}

func applyFIS(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*fisbackend.Handler); ok {
		if bk, isBk := h.Backend.(*fisbackend.InMemoryBackend); isBk {
			if d := l.effective(l.FIS); d > 0 {
				bk.SetLifecycleDelay(d)
			}
		}
	}
}

func applyDSQL(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*dsqlbackend.Handler); ok {
		if bk, isBk := h.Backend.(*dsqlbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.DSQL))
		}
	}
}

func applyOrganizations(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*organizationsbackend.Handler); ok {
		if bk, isBk := h.Backend.(*organizationsbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.Organizations))
		}
	}
}

func applyMacie2(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*macie2backend.Handler); ok {
		if bk, isBk := h.Backend.(*macie2backend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.Macie2))
		}
	}
}

func applyDetective(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*detectivebackend.Handler); ok {
		if bk, isBk := h.Backend.(*detectivebackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.Detective))
		}
	}
}

func applyWorkMail(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*workmailbackend.Handler); ok {
		if bk, isBk := h.Backend.(*workmailbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.WorkMail))
		}
	}
}

func applyPersonalize(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*personalizebackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.effective(l.Personalize))
	}
}

func applyOpsWorks(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*opsworksbackend.Handler); ok {
		if bk, isBk := h.Backend.(*opsworksbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.OpsWorks))
		}
	}
}

func applyOutposts(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*outpostsbackend.Handler); ok {
		if d := l.effective(l.Outposts); d > 0 {
			h.Backend.SetTransitionDelay(d)
		}
	}
}

func applySupport(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*supportbackend.Handler); ok {
		if bk, isBk := h.Backend.(*supportbackend.InMemoryBackend); isBk {
			bk.SetCaseProgressDelay(l.effective(l.Support))
		}
	}
}

func applyMWAA(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*mwaabackend.Handler); ok {
		if bk, isBk := h.Backend.(*mwaabackend.InMemoryBackend); isBk {
			bk.SetTransitionDelay(l.effective(l.MWAA))
		}
	}
}

func applyOmics(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*omicsbackend.Handler); ok {
		if bk, isBk := h.Backend.(*omicsbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.Omics))
		}
	}
}

func applyResilienceHub(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*resiliencehubbackend.Handler); ok {
		h.Backend.SetTransitionDelay(l.effective(l.ResilienceHub))
	}
}

func applyDataBrew(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*databrewbackend.Handler); ok {
		if bk, isBk := h.Backend.(*databrewbackend.InMemoryBackend); isBk {
			bk.SetJobRunDelay(l.effective(l.DataBrew))
		}
	}
}

func applyMGN(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*mgnbackend.Handler); ok {
		h.Backend.SetTransitionDelay(l.effective(l.MGN))
	}
}

func applyEKS(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*eksbackend.Handler); ok {
		if d := l.effective(l.EKS); d > 0 {
			h.Backend.SetLifecycleDelay(d)
		}
	}
}

func applyCloudControl(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*cloudcontrolbackend.Handler); ok {
		h.Backend.SetOperationDelay(l.effective(l.CloudControl))
	}
}

func applyMediaPackage(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*mediapackagebackend.Handler); ok {
		if bk, isBk := h.Backend.(*mediapackagebackend.InMemoryBackend); isBk {
			if d := l.effective(l.MediaPackage); d > 0 {
				bk.SetHarvestJobDuration(d)
			}
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

func applyKafka(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*kafkabackend.Handler); ok {
		if bk, isBk := h.Backend.(*kafkabackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.Kafka))
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

func applyFSx(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*fsxbackend.Handler); ok {
		if bk, isBk := h.Backend.(*fsxbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.FSx))
		}
	}
}

func applyNetworkManager(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*networkmanagerbackend.Handler); ok {
		if d := l.effective(l.NetworkManager); d > 0 {
			h.Backend.SetLifecycleDelay(d)
		}
	}
}

func applyAccount(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*accountbackend.Handler); ok {
		if bk, isBk := h.Backend.(*accountbackend.InMemoryBackend); isBk {
			if d := l.effective(l.Account); d > 0 {
				bk.SetLifecycleDelay(d)
			}
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

func applyComprehend(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*comprehendbackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.effective(l.Comprehend))
	}
}

func applyRekognition(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*rekognitionbackend.Handler); ok {
		if bk, isBk := h.Backend.(*rekognitionbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.Rekognition))
		}
	}
}

func applyTextract(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*textractbackend.Handler); ok {
		if bk, isBk := h.Backend.(*textractbackend.InMemoryBackend); isBk {
			if d := l.Textract; d > 0 {
				bk.SetAsyncJobDelay(d)
			}
		}
	}
}

func applyTranslate(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*translatebackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.effective(l.Translate))
	}
}

func applyPolly(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*pollybackend.Handler); ok {
		h.Backend.SetLifecycleDelay(l.effective(l.Polly))
	}
}

func applyTranscribe(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*transcribebackend.Handler); ok {
		if bk, isBk := h.Backend.(*transcribebackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.Transcribe))
		}
	}
}

func applyIoTAnalytics(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*iotanalyticsbackend.Handler); ok {
		if bk, isBk := h.Backend.(*iotanalyticsbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.IoTAnalytics))
		}
	}
}

func applyIoTWireless(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*iotwirelessbackend.Handler); ok {
		if bk, isBk := h.Backend.(*iotwirelessbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.IoTWireless))
		}
	}
}

func applyForecast(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*forecastbackend.Handler); ok {
		if bk := h.Backend; bk != nil {
			bk.SetCreateDelay(l.effective(l.Forecast))
		}
	}
}

func applyNetworkMonitor(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*networkmonitorbackend.Handler); ok {
		if bk, isBk := h.Backend.(*networkmonitorbackend.InMemoryBackend); isBk {
			bk.SetActivationDelay(l.effective(l.NetworkMonitor))
		}
	}
}

func applyManagedBlockchain(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*managedblockchainbackend.Handler); ok {
		if bk, isBk := h.Backend.(*managedblockchainbackend.InMemoryBackend); isBk {
			bk.SetCreateDelay(l.effective(l.ManagedBlockchain))
		}
	}
}

func applyCleanRooms(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*cleanroomsbackend.Handler); ok {
		if bk, isBk := h.Backend.(*cleanroomsbackend.InMemoryBackend); isBk {
			bk.SetProtectedWorkDelay(l.effective(l.CleanRooms))
		}
	}
}

func applyResourceGroups(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*resourcegroupsbackend.Handler); ok {
		if bk, isBk := h.Backend.(*resourcegroupsbackend.InMemoryBackend); isBk {
			bk.SetLifecycleDelay(l.effective(l.ResourceGroups))
		}
	}
}

func applyServerlessRepo(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*serverlessrepobackend.Handler); ok {
		if bk, isBk := h.Backend.(*serverlessrepobackend.InMemoryBackend); isBk {
			bk.SetTemplatePrepareDelay(l.effective(l.ServerlessRepo))
		}
	}
}

func applyCodeConnections(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*codeconnectionsbackend.Handler); ok {
		if bk := h.Backend; bk != nil {
			d := l.effective(l.CodeConnections)
			bk.SetConnectionHandshakeDelay(d)
			bk.SetHostSetupDelay(d)
		}
	}
}

func applyCodeStarConnections(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*codestarconnectionsbackend.Handler); ok {
		if bk := h.Backend; bk != nil {
			d := l.effective(l.CodeStarConnections)
			if d > 0 {
				bk.SetConnectionHandshakeDelay(d)
				bk.SetHostSetupDelay(d)
			}
		}
	}
}

func applyRoute53Recovery(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*recoverycontrolbackend.Handler); ok {
		h.Backend.SetActivationDelay(l.effective(l.Route53Recovery))
	}
}

func applyARCZonalShift(reg service.Registerable, l LifecycleSettings) {
	if h, ok := reg.(*arczonalshiftbackend.Handler); ok {
		h.Backend.SetPracticeRunDuration(l.ARCZonalShift)
	}
}

// wireLifecycleDelays applies the configured dwell times to the already
// initialized service backends; zero values leave services instant.
func wireLifecycleDelays(byName map[string]service.Registerable, l LifecycleSettings) {
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
		"RedshiftServerless": func(r service.Registerable, l LifecycleSettings) {
			applyRedshiftServerless(r, l.effective(l.RedshiftServerless))
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
	}

	for name, apply := range appliers {
		if reg, ok := byName[name]; ok {
			apply(reg, l)
		}
	}
}
