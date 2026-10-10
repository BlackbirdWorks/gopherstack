package main

import (
	"archive/zip"
	"bytes"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apprunner"
	apprunnertypes "github.com/aws/aws-sdk-go-v2/service/apprunner/types"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagent"
	batypes "github.com/aws/aws-sdk-go-v2/service/bedrockagent/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
	cftypes "github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	configservice "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/elasticbeanstalk/types"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go-v2/service/inspector2"
	inspector2types "github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2"
	kav2types "github.com/aws/aws-sdk-go-v2/service/kinesisanalyticsv2/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/medialive"
	mltypes "github.com/aws/aws-sdk-go-v2/service/medialive/types"
	"github.com/aws/aws-sdk-go-v2/service/mediastore"
	"github.com/aws/aws-sdk-go-v2/service/memorydb"
	"github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/quicksight"
	qstypes "github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/route53resolver"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53resolver/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/workspaces"
	wstypes "github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	lifecycleLongDelay  = time.Hour
	lifecycleShortDelay = 300 * time.Millisecond
)

type lifecycleKnob struct {
	set        func(l *LifecycleSettings, d time.Duration)
	start      func(t *testing.T, cfg aws.Config) func() bool
	name       string
	skipGlobal bool
}

func zipBytes(t *testing.T) []byte {
	t.Helper()

	var buf bytes.Buffer

	w := zip.NewWriter(&buf)
	f, err := w.Create("index.py")
	require.NoError(t, err)
	_, err = f.Write([]byte("def handler(e, c):\n    return {}\n"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return buf.Bytes()
}

func lambdaCreateInput(t *testing.T, name string) *lambda.CreateFunctionInput {
	t.Helper()

	return &lambda.CreateFunctionInput{
		FunctionName: aws.String(name),
		Runtime:      lambdatypes.RuntimePython312,
		Handler:      aws.String("index.handler"),
		Role:         aws.String("arn:aws:iam::000000000000:role/r"),
		Code:         &lambdatypes.FunctionCode{ZipFile: zipBytes(t)},
	}
}

func lifecycleKnobs() []lifecycleKnob {
	return []lifecycleKnob{
		{
			name: "elasticsearch",
			set:  func(*LifecycleSettings, time.Duration) {},
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := elasticsearchservice.NewFromConfig(cfg)
				_, err := c.CreateElasticsearchDomain(t.Context(), &elasticsearchservice.CreateElasticsearchDomainInput{
					DomainName: aws.String("lcres"),
				})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeElasticsearchDomain(
						t.Context(),
						&elasticsearchservice.DescribeElasticsearchDomainInput{DomainName: aws.String("lcres")},
					)
					require.NoError(t, dErr)

					return aws.ToBool(out.DomainStatus.Processing)
				}
			},
		},
		{
			name: "opensearch",
			set:  func(l *LifecycleSettings, d time.Duration) { l.OpenSearchProcessing = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := opensearch.NewFromConfig(cfg)
				_, err := c.CreateDomain(t.Context(), &opensearch.CreateDomainInput{DomainName: aws.String("lcres")})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeDomain(
						t.Context(),
						&opensearch.DescribeDomainInput{DomainName: aws.String("lcres")},
					)
					require.NoError(t, dErr)

					return aws.ToBool(out.DomainStatus.Processing)
				}
			},
		},
		{
			name: "elasticache",
			set:  func(l *LifecycleSettings, d time.Duration) { l.ElastiCache = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := elasticache.NewFromConfig(cfg)
				_, err := c.CreateCacheCluster(t.Context(), &elasticache.CreateCacheClusterInput{
					CacheClusterId: aws.String("lcres"),
					Engine:         aws.String("redis"),
					CacheNodeType:  aws.String("cache.t3.micro"),
					NumCacheNodes:  aws.Int32(1),
				})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeCacheClusters(t.Context(), &elasticache.DescribeCacheClustersInput{
						CacheClusterId: aws.String("lcres"),
					})
					require.NoError(t, dErr)
					require.Len(t, out.CacheClusters, 1)

					return aws.ToString(out.CacheClusters[0].CacheClusterStatus) == "creating"
				}
			},
		},
		{
			name: "memorydb",
			set:  func(l *LifecycleSettings, d time.Duration) { l.MemoryDB = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := memorydb.NewFromConfig(cfg)
				_, err := c.CreateCluster(t.Context(), &memorydb.CreateClusterInput{
					ClusterName: aws.String("lcres"),
					NodeType:    aws.String("db.r6g.large"),
					ACLName:     aws.String("open-access"),
				})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeClusters(t.Context(), &memorydb.DescribeClustersInput{
						ClusterName: aws.String("lcres"),
					})
					require.NoError(t, dErr)
					require.Len(t, out.Clusters, 1)

					return aws.ToString(out.Clusters[0].Status) == "creating"
				}
			},
		},
		{
			name: "docdb",
			set:  func(l *LifecycleSettings, d time.Duration) { l.DocDB = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := docdb.NewFromConfig(cfg)
				_, err := c.CreateDBCluster(t.Context(), &docdb.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("lcres"),
					Engine:              aws.String("docdb"),
					MasterUsername:      aws.String("admin"),
					MasterUserPassword:  aws.String("password123"),
				})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeDBClusters(t.Context(), &docdb.DescribeDBClustersInput{
						DBClusterIdentifier: aws.String("lcres"),
					})
					require.NoError(t, dErr)
					require.Len(t, out.DBClusters, 1)

					return aws.ToString(out.DBClusters[0].Status) == "creating"
				}
			},
		},
		{
			name: "neptune",
			set:  func(l *LifecycleSettings, d time.Duration) { l.Neptune = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := neptune.NewFromConfig(cfg)
				_, err := c.CreateDBCluster(t.Context(), &neptune.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("lcres"),
					Engine:              aws.String("neptune"),
				})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeDBClusters(t.Context(), &neptune.DescribeDBClustersInput{
						DBClusterIdentifier: aws.String("lcres"),
					})
					require.NoError(t, dErr)
					require.Len(t, out.DBClusters, 1)

					return aws.ToString(out.DBClusters[0].Status) == "creating"
				}
			},
		},
		{
			name: "lambda_activation",
			set:  func(l *LifecycleSettings, d time.Duration) { l.LambdaActivation = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := lambda.NewFromConfig(cfg)
				_, err := c.CreateFunction(t.Context(), lambdaCreateInput(t, "lcres"))
				require.NoError(t, err)

				return func() bool {
					out, gErr := c.GetFunctionConfiguration(t.Context(), &lambda.GetFunctionConfigurationInput{
						FunctionName: aws.String("lcres"),
					})
					require.NoError(t, gErr)

					return out.State == lambdatypes.StatePending
				}
			},
		},
		{
			name:       "lambda_provisioned_concurrency",
			set:        func(l *LifecycleSettings, d time.Duration) { l.LambdaProvisionedConcurrency = d },
			skipGlobal: true,
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := lambda.NewFromConfig(cfg)
				_, err := c.CreateFunction(t.Context(), lambdaCreateInput(t, "lcres"))
				require.NoError(t, err)
				ver, err := c.PublishVersion(
					t.Context(),
					&lambda.PublishVersionInput{FunctionName: aws.String("lcres")},
				)
				require.NoError(t, err)
				_, err = c.PutProvisionedConcurrencyConfig(t.Context(), &lambda.PutProvisionedConcurrencyConfigInput{
					FunctionName:                    aws.String("lcres"),
					Qualifier:                       ver.Version,
					ProvisionedConcurrentExecutions: aws.Int32(1),
				})
				require.NoError(t, err)

				return func() bool {
					out, gErr := c.GetProvisionedConcurrencyConfig(
						t.Context(),
						&lambda.GetProvisionedConcurrencyConfigInput{
							FunctionName: aws.String("lcres"),
							Qualifier:    ver.Version,
						},
					)
					require.NoError(t, gErr)

					return out.Status == lambdatypes.ProvisionedConcurrencyStatusEnumInProgress
				}
			},
		},
		{
			name: "mediastore",
			set:  func(l *LifecycleSettings, d time.Duration) { l.MediaStore = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := mediastore.NewFromConfig(cfg)
				_, err := c.CreateContainer(
					t.Context(),
					&mediastore.CreateContainerInput{ContainerName: aws.String("lcres")},
				)
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeContainer(t.Context(), &mediastore.DescribeContainerInput{
						ContainerName: aws.String("lcres"),
					})
					require.NoError(t, dErr)

					return string(out.Container.Status) == "CREATING"
				}
			},
		},
		{
			name: "efs",
			set:  func(l *LifecycleSettings, d time.Duration) { l.EFS = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := efs.NewFromConfig(cfg)
				fs, err := c.CreateFileSystem(
					t.Context(),
					&efs.CreateFileSystemInput{CreationToken: aws.String("lcres")},
				)
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeFileSystems(
						t.Context(),
						&efs.DescribeFileSystemsInput{FileSystemId: fs.FileSystemId},
					)
					require.NoError(t, dErr)
					require.Len(t, out.FileSystems, 1)

					return string(out.FileSystems[0].LifeCycleState) == "creating"
				}
			},
		},
		{
			name: "redshift",
			set:  func(l *LifecycleSettings, d time.Duration) { l.Redshift = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := redshift.NewFromConfig(cfg)
				_, err := c.CreateCluster(t.Context(), &redshift.CreateClusterInput{
					ClusterIdentifier:  aws.String("lcres"),
					NodeType:           aws.String("dc2.large"),
					MasterUsername:     aws.String("admin"),
					MasterUserPassword: aws.String("Passw0rdPassw0rd"),
				})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeClusters(t.Context(), &redshift.DescribeClustersInput{
						ClusterIdentifier: aws.String("lcres"),
					})
					require.NoError(t, dErr)
					require.Len(t, out.Clusters, 1)

					return aws.ToString(out.Clusters[0].ClusterStatus) == "creating"
				}
			},
		},
		{
			name: "ssm_command",
			set:  func(l *LifecycleSettings, d time.Duration) { l.SSMCommand = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := ssm.NewFromConfig(cfg)
				out, err := c.SendCommand(t.Context(), &ssm.SendCommandInput{
					DocumentName: aws.String("AWS-RunShellScript"),
					InstanceIds:  []string{"i-0123456789abcdef0"},
					Parameters:   map[string][]string{"commands": {"echo hi"}},
				})
				require.NoError(t, err)

				return func() bool {
					inv, gErr := c.GetCommandInvocation(t.Context(), &ssm.GetCommandInvocationInput{
						CommandId:  out.Command.CommandId,
						InstanceId: aws.String("i-0123456789abcdef0"),
					})
					require.NoError(t, gErr)

					return string(inv.Status) == "InProgress"
				}
			},
		},
		{
			name: "ssm_automation",
			set:  func(l *LifecycleSettings, d time.Duration) { l.SSMAutomation = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := ssm.NewFromConfig(cfg)
				out, err := c.StartAutomationExecution(t.Context(), &ssm.StartAutomationExecutionInput{
					DocumentName: aws.String("AWS-DoSomething"),
				})
				require.NoError(t, err)

				return func() bool {
					got, gErr := c.GetAutomationExecution(t.Context(), &ssm.GetAutomationExecutionInput{
						AutomationExecutionId: out.AutomationExecutionId,
					})
					require.NoError(t, gErr)

					return string(got.AutomationExecution.AutomationExecutionStatus) == "InProgress"
				}
			},
		},
		{
			name: "ecs_start",
			set:  func(l *LifecycleSettings, d time.Duration) { l.ECSStart = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := ecs.NewFromConfig(cfg)
				task := ecsRunTask(t, c)

				return func() bool { return ecsTaskStatus(t, c, task) != "RUNNING" }
			},
		},
		{
			name:       "ecs_stop",
			set:        func(l *LifecycleSettings, d time.Duration) { l.ECSStop = d },
			skipGlobal: true,
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := ecs.NewFromConfig(cfg)
				task := ecsRunTask(t, c)
				_, err := c.StopTask(
					t.Context(),
					&ecs.StopTaskInput{Cluster: aws.String("lcres"), Task: aws.String(task)},
				)
				require.NoError(t, err)

				return func() bool { return ecsTaskStatus(t, c, task) != "STOPPED" }
			},
		},
		{
			name: "awsconfig",
			set:  func(l *LifecycleSettings, d time.Duration) { l.AWSConfig = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := configservice.NewFromConfig(cfg)
				_, err := c.PutConformancePack(t.Context(), &configservice.PutConformancePackInput{
					ConformancePackName: aws.String("lcres"),
					TemplateBody:        aws.String("Resources: {}"),
				})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeConformancePackStatus(
						t.Context(),
						&configservice.DescribeConformancePackStatusInput{ConformancePackNames: []string{"lcres"}},
					)
					require.NoError(t, dErr)
					require.Len(t, out.ConformancePackStatusDetails, 1)

					return string(out.ConformancePackStatusDetails[0].ConformancePackState) == "CREATE_IN_PROGRESS"
				}
			},
		},
		{
			name: "inspector2",
			set:  func(l *LifecycleSettings, d time.Duration) { l.Inspector2 = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := inspector2.NewFromConfig(cfg)
				_, err := c.Enable(t.Context(), &inspector2.EnableInput{
					ResourceTypes: []inspector2types.ResourceScanType{inspector2types.ResourceScanTypeEc2},
				})
				require.NoError(t, err)

				return func() bool {
					out, gErr := c.BatchGetAccountStatus(t.Context(), &inspector2.BatchGetAccountStatusInput{})
					require.NoError(t, gErr)
					require.Len(t, out.Accounts, 1)

					return out.Accounts[0].State.Status == inspector2types.StatusEnabling
				}
			},
		},
		{
			name: "apprunner",
			set:  func(l *LifecycleSettings, d time.Duration) { l.AppRunner = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := apprunner.NewFromConfig(cfg)
				out, err := c.CreateService(t.Context(), &apprunner.CreateServiceInput{
					ServiceName: aws.String("lcres"),
					SourceConfiguration: &apprunnertypes.SourceConfiguration{
						ImageRepository: &apprunnertypes.ImageRepository{
							ImageIdentifier:     aws.String("public.ecr.aws/x/y:1"),
							ImageRepositoryType: apprunnertypes.ImageRepositoryTypeEcrPublic,
						},
					},
				})
				require.NoError(t, err)

				return func() bool {
					d, dErr := c.DescribeService(t.Context(), &apprunner.DescribeServiceInput{
						ServiceArn: out.Service.ServiceArn,
					})
					require.NoError(t, dErr)

					return d.Service.Status == apprunnertypes.ServiceStatusOperationInProgress
				}
			},
		},
		{
			name: "kinesisanalyticsv2",
			set:  func(l *LifecycleSettings, d time.Duration) { l.KinesisAnalyticsV2 = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := kinesisanalyticsv2.NewFromConfig(cfg)
				_, err := c.CreateApplication(t.Context(), &kinesisanalyticsv2.CreateApplicationInput{
					ApplicationName:      aws.String("lcres"),
					RuntimeEnvironment:   kav2types.RuntimeEnvironmentFlink118,
					ServiceExecutionRole: aws.String("arn:aws:iam::000000000000:role/r"),
				})
				require.NoError(t, err)
				_, err = c.StartApplication(t.Context(), &kinesisanalyticsv2.StartApplicationInput{
					ApplicationName: aws.String("lcres"),
				})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeApplication(t.Context(), &kinesisanalyticsv2.DescribeApplicationInput{
						ApplicationName: aws.String("lcres"),
					})
					require.NoError(t, dErr)

					return out.ApplicationDetail.ApplicationStatus == kav2types.ApplicationStatusStarting
				}
			},
		},
		{
			name: "workspaces",
			set:  func(l *LifecycleSettings, d time.Duration) { l.WorkSpaces = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := workspaces.NewFromConfig(cfg)
				_, err := c.RegisterWorkspaceDirectory(t.Context(), &workspaces.RegisterWorkspaceDirectoryInput{
					DirectoryId:            aws.String("d-00000000"),
					WorkspaceDirectoryName: aws.String("lcres"),
				})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeWorkspaceDirectories(
						t.Context(),
						&workspaces.DescribeWorkspaceDirectoriesInput{},
					)
					require.NoError(t, dErr)
					require.Len(t, out.Directories, 1)

					return out.Directories[0].State == wstypes.WorkspaceDirectoryStateRegistering
				}
			},
		},
		{
			name: "quicksight",
			set:  func(l *LifecycleSettings, d time.Duration) { l.QuickSight = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := quicksight.NewFromConfig(cfg)
				_, err := c.CreateAnalysis(t.Context(), &quicksight.CreateAnalysisInput{
					AwsAccountId: aws.String("000000000000"),
					AnalysisId:   aws.String("lcres"),
					Name:         aws.String("lcres"),
				})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeAnalysis(t.Context(), &quicksight.DescribeAnalysisInput{
						AwsAccountId: aws.String("000000000000"),
						AnalysisId:   aws.String("lcres"),
					})
					require.NoError(t, dErr)

					return out.Analysis.Status == qstypes.ResourceStatusCreationInProgress
				}
			},
		},
		{
			name: "bedrockagent",
			set:  func(l *LifecycleSettings, d time.Duration) { l.BedrockAgent = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := bedrockagent.NewFromConfig(cfg)
				created, err := c.CreateAgent(t.Context(), &bedrockagent.CreateAgentInput{
					AgentName:            aws.String("lcres"),
					AgentResourceRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
					FoundationModel:      aws.String("anthropic.claude-v2"),
				})
				require.NoError(t, err)
				_, err = c.PrepareAgent(t.Context(), &bedrockagent.PrepareAgentInput{AgentId: created.Agent.AgentId})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.GetAgent(t.Context(), &bedrockagent.GetAgentInput{
						AgentId: created.Agent.AgentId,
					})
					require.NoError(t, dErr)

					return out.Agent.AgentStatus == batypes.AgentStatusPreparing
				}
			},
		},
		{
			name: "elasticbeanstalk",
			set:  func(l *LifecycleSettings, d time.Duration) { l.ElasticBeanstalk = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := elasticbeanstalk.NewFromConfig(cfg)
				_, err := c.CreateApplication(t.Context(), &elasticbeanstalk.CreateApplicationInput{
					ApplicationName: aws.String("lcres"),
				})
				require.NoError(t, err)
				_, err = c.CreateEnvironment(t.Context(), &elasticbeanstalk.CreateEnvironmentInput{
					ApplicationName: aws.String("lcres"),
					EnvironmentName: aws.String("lcres-env"),
				})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeEnvironments(t.Context(), &elasticbeanstalk.DescribeEnvironmentsInput{})
					require.NoError(t, dErr)
					require.Len(t, out.Environments, 1)

					return out.Environments[0].Status == ebtypes.EnvironmentStatusLaunching
				}
			},
		},
		{
			name: "route53resolver",
			set:  func(l *LifecycleSettings, d time.Duration) { l.Route53Resolver = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := route53resolver.NewFromConfig(cfg)
				created, err := c.CreateResolverEndpoint(t.Context(), &route53resolver.CreateResolverEndpointInput{
					CreatorRequestId: aws.String("lcres"),
					Name:             aws.String("lcres"),
					Direction:        r53types.ResolverEndpointDirectionInbound,
					SecurityGroupIds: []string{"sg-1"},
					IpAddresses: []r53types.IpAddressRequest{
						{SubnetId: aws.String("subnet-1")},
						{SubnetId: aws.String("subnet-2")},
					},
				})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.GetResolverEndpoint(t.Context(), &route53resolver.GetResolverEndpointInput{
						ResolverEndpointId: created.ResolverEndpoint.Id,
					})
					require.NoError(t, dErr)

					return out.ResolverEndpoint.Status == r53types.ResolverEndpointStatusCreating
				}
			},
		},
		{
			name: "medialive",
			set:  func(l *LifecycleSettings, d time.Duration) { l.MediaLive = d },
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := medialive.NewFromConfig(cfg)
				created, err := c.CreateChannel(t.Context(), &medialive.CreateChannelInput{Name: aws.String("lcres")})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeChannel(t.Context(), &medialive.DescribeChannelInput{
						ChannelId: created.Channel.Id,
					})
					require.NoError(t, dErr)

					return out.State == mltypes.ChannelStateCreating
				}
			},
		},
		{
			name: "dynamodb",
			set:  func(*LifecycleSettings, time.Duration) {},
			start: func(t *testing.T, cfg aws.Config) func() bool {
				t.Helper()

				c := dynamodb.NewFromConfig(cfg)
				_, err := c.CreateTable(t.Context(), &dynamodb.CreateTableInput{
					TableName:   aws.String("lcres"),
					BillingMode: ddbtypes.BillingModePayPerRequest,
					AttributeDefinitions: []ddbtypes.AttributeDefinition{
						{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
					},
					KeySchema: []ddbtypes.KeySchemaElement{
						{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
					},
				})
				require.NoError(t, err)

				return func() bool {
					out, dErr := c.DescribeTable(
						t.Context(),
						&dynamodb.DescribeTableInput{TableName: aws.String("lcres")},
					)
					require.NoError(t, dErr)

					return out.Table.TableStatus == ddbtypes.TableStatusCreating
				}
			},
		},
	}
}

func TestLifecycleDelayWiring(t *testing.T) {
	t.Parallel()

	for _, k := range lifecycleKnobs() {
		t.Run(k.name, func(t *testing.T) {
			t.Parallel()

			t.Run("default_settles", func(t *testing.T) {
				t.Parallel()

				poll := k.start(t, newWiredSDKConfig(t, CLI{}))
				assert.False(t, poll())
			})

			t.Run("global_delay_transitional", func(t *testing.T) {
				t.Parallel()

				if k.skipGlobal {
					t.Skip("global delay also delays a prerequisite")
				}

				poll := k.start(t, newWiredSDKConfig(t, CLI{Lifecycle: LifecycleSettings{Delay: lifecycleLongDelay}}))
				assert.True(t, poll())
			})

			t.Run("specific_delay_transitional", func(t *testing.T) {
				t.Parallel()

				cli := CLI{}
				k.set(&cli.Lifecycle, lifecycleLongDelay)
				cli.ElasticsearchProcessingDelay = esDelayFor(k.name, lifecycleLongDelay)
				cli.DynamoDB.CreateDelay = ddbDelayFor(k.name, lifecycleLongDelay)

				poll := k.start(t, newWiredSDKConfig(t, cli))
				assert.True(t, poll())
			})

			t.Run("settles", func(t *testing.T) {
				t.Parallel()

				cli := CLI{}
				k.set(&cli.Lifecycle, lifecycleShortDelay)
				cli.ElasticsearchProcessingDelay = esDelayFor(k.name, lifecycleShortDelay)
				cli.DynamoDB.CreateDelay = ddbDelayFor(k.name, lifecycleShortDelay)

				poll := k.start(t, newWiredSDKConfig(t, cli))
				require.Eventually(t, func() bool { return !poll() }, 15*time.Second, 50*time.Millisecond)
			})
		})
	}
}

func esDelayFor(name string, d time.Duration) time.Duration {
	if name == "elasticsearch" {
		return d
	}

	return 0
}

func ddbDelayFor(name string, d time.Duration) time.Duration {
	if name == "dynamodb" {
		return d
	}

	return 0
}

func ecsRunTask(t *testing.T, c *ecs.Client) string {
	t.Helper()

	_, err := c.CreateCluster(t.Context(), &ecs.CreateClusterInput{ClusterName: aws.String("lcres")})
	require.NoError(t, err)
	td, err := c.RegisterTaskDefinition(t.Context(), &ecs.RegisterTaskDefinitionInput{
		Family: aws.String("lcres"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{
			{Name: aws.String("c"), Image: aws.String("busybox"), Memory: aws.Int32(64)},
		},
	})
	require.NoError(t, err)
	run, err := c.RunTask(t.Context(), &ecs.RunTaskInput{
		Cluster:        aws.String("lcres"),
		TaskDefinition: td.TaskDefinition.TaskDefinitionArn,
	})
	require.NoError(t, err)
	require.Len(t, run.Tasks, 1)

	return aws.ToString(run.Tasks[0].TaskArn)
}

func ecsTaskStatus(t *testing.T, c *ecs.Client, task string) string {
	t.Helper()

	out, err := c.DescribeTasks(
		t.Context(),
		&ecs.DescribeTasksInput{Cluster: aws.String("lcres"), Tasks: []string{task}},
	)
	require.NoError(t, err)
	require.Len(t, out.Tasks, 1)

	return aws.ToString(out.Tasks[0].LastStatus)
}

func TestLifecycleDelayCloudFront(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		life       LifecycleSettings
		wantHeld   bool
		wantSettle bool
	}{
		{name: "default", life: LifecycleSettings{}, wantSettle: true},
		{name: "specific", life: LifecycleSettings{CloudFront: lifecycleLongDelay}, wantHeld: true},
		{name: "global", life: LifecycleSettings{Delay: lifecycleLongDelay}, wantHeld: true},
		{name: "short", life: LifecycleSettings{CloudFront: lifecycleShortDelay}, wantSettle: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := cloudfront.NewFromConfig(newWiredSDKConfig(t, CLI{Lifecycle: tt.life}))
			out, err := c.CreateDistribution(t.Context(), &cloudfront.CreateDistributionInput{
				DistributionConfig: &cftypes.DistributionConfig{
					CallerReference: aws.String("lc"),
					Comment:         aws.String("lc"),
					Enabled:         aws.Bool(true),
					Origins: &cftypes.Origins{
						Quantity: aws.Int32(1),
						Items: []cftypes.Origin{{
							Id:         aws.String("o"),
							DomainName: aws.String("example.com"),
							CustomOriginConfig: &cftypes.CustomOriginConfig{
								HTTPPort:             aws.Int32(80),
								HTTPSPort:            aws.Int32(443),
								OriginProtocolPolicy: cftypes.OriginProtocolPolicyHttpOnly,
							},
						}},
					},
					DefaultCacheBehavior: &cftypes.DefaultCacheBehavior{
						TargetOriginId:       aws.String("o"),
						ViewerProtocolPolicy: cftypes.ViewerProtocolPolicyAllowAll,
						CachePolicyId:        aws.String("658327ea-f89d-4fab-a63d-7e88639e58f6"),
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, "InProgress", aws.ToString(out.Distribution.Status))

			status := func() string {
				got, gErr := c.GetDistribution(t.Context(), &cloudfront.GetDistributionInput{Id: out.Distribution.Id})
				require.NoError(t, gErr)

				return aws.ToString(got.Distribution.Status)
			}

			if tt.wantHeld {
				tick := time.NewTicker(50 * time.Millisecond)
				defer tick.Stop()

				for range 14 {
					<-tick.C
					require.Equal(t, "InProgress", status())
				}

				return
			}

			require.Eventually(t, func() bool { return status() == "Deployed" }, 15*time.Second, 50*time.Millisecond)
		})
	}
}

func TestLifecycleDelayQuickSightIngestion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		life       LifecycleSettings
		wantHeld   bool
		wantSettle bool
	}{
		{name: "explicit holds", life: LifecycleSettings{QuickSightIngestion: lifecycleLongDelay}, wantHeld: true},
		{name: "global ignored", life: LifecycleSettings{Delay: lifecycleLongDelay}, wantSettle: true},
		{name: "short settles", life: LifecycleSettings{QuickSightIngestion: lifecycleShortDelay}, wantSettle: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := quicksight.NewFromConfig(newWiredSDKConfig(t, CLI{Lifecycle: tt.life}))
			_, err := c.CreateDataSet(t.Context(), &quicksight.CreateDataSetInput{
				AwsAccountId: aws.String("000000000000"),
				DataSetId:    aws.String("lcds"),
				Name:         aws.String("lcds"),
				ImportMode:   qstypes.DataSetImportModeSpice,
				PhysicalTableMap: map[string]qstypes.PhysicalTable{
					"pt1": &qstypes.PhysicalTableMemberRelationalTable{Value: qstypes.RelationalTable{
						DataSourceArn: aws.String("arn:aws:quicksight:us-east-1:000000000000:datasource/ds1"),
						Name:          aws.String("table1"),
						InputColumns: []qstypes.InputColumn{
							{Name: aws.String("col1"), Type: qstypes.InputColumnDataTypeString},
						},
					}},
				},
			})
			require.NoError(t, err)

			_, err = c.CreateIngestion(t.Context(), &quicksight.CreateIngestionInput{
				AwsAccountId: aws.String("000000000000"),
				DataSetId:    aws.String("lcds"),
				IngestionId:  aws.String("i1"),
			})
			require.NoError(t, err)

			running := func() bool {
				out, dErr := c.DescribeIngestion(t.Context(), &quicksight.DescribeIngestionInput{
					AwsAccountId: aws.String("000000000000"),
					DataSetId:    aws.String("lcds"),
					IngestionId:  aws.String("i1"),
				})
				require.NoError(t, dErr)

				return out.Ingestion.IngestionStatus == qstypes.IngestionStatusRunning
			}

			if tt.wantHeld {
				assert.True(t, running())
			}

			if tt.wantSettle {
				require.Eventually(t, func() bool { return !running() }, 15*time.Second, 50*time.Millisecond)
			}
		})
	}
}
