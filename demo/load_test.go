package demo_test

import (
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	codepipelinesvc "github.com/aws/aws-sdk-go-v2/service/codepipeline"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/demo"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	codepipelinebackend "github.com/blackbirdworks/gopherstack/services/codepipeline"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ecsbackend "github.com/blackbirdworks/gopherstack/services/ecs"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

func TestLoadData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantDDBCount int32
		wantS3Count  int32
	}{
		{
			name:         "loads demo data into all backends successfully",
			wantDDBCount: 2,
			wantS3Count:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Setup Backends
			ddbBackend := ddbbackend.NewInMemoryDB()
			ddbHandler := ddbbackend.NewHandler(ddbBackend)
			s3Backend := s3backend.NewInMemoryBackend(&s3backend.GzipCompressor{})
			s3Handler := s3backend.NewHandler(s3Backend)

			// Setup Echo server with service registry
			e := echo.New()
			e.Pre(logger.EchoMiddleware(slog.Default()))

			registry := service.NewRegistry()
			_ = registry.Register(ddbHandler)
			_ = registry.Register(s3Handler)
			_ = registry.Register(sqsbackend.NewHandler(sqsbackend.NewInMemoryBackend()))
			_ = registry.Register(snsbackend.NewHandler(snsbackend.NewInMemoryBackend()))
			_ = registry.Register(iambackend.NewHandler(iambackend.NewInMemoryBackend()))
			_ = registry.Register(ssmbackend.NewHandler(ssmbackend.NewInMemoryBackend()))
			_ = registry.Register(stsbackend.NewHandler(stsbackend.NewInMemoryBackend()))
			_ = registry.Register(kmsbackend.NewHandler(kmsbackend.NewInMemoryBackend()))
			_ = registry.Register(
				secretsmanagerbackend.NewHandler(secretsmanagerbackend.NewInMemoryBackend()),
			)

			router := service.NewServiceRouter(registry)
			e.Use(router.RouteHandler())

			// Setup Client using Echo's HTTP server
			inMemClient := &dashboard.InMemClient{Handler: e}

			// Setup AWS Config
			cfg, err := awscfg.LoadDefaultConfig(
				t.Context(),
				awscfg.WithRegion("us-east-1"),
				awscfg.WithCredentialsProvider(
					credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
				),
				awscfg.WithHTTPClient(inMemClient),
			)
			require.NoError(t, err)

			ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
				o.BaseEndpoint = aws.String("http://local")
			})
			s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
				o.UsePathStyle = true
				o.BaseEndpoint = aws.String("http://local")
			})

			// Run LoadData
			loadClients := &demo.Clients{
				DynamoDB: ddbClient,
				S3:       s3Client,
				SQS:      sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = aws.String("http://local") }),
				SNS:      sns.NewFromConfig(cfg, func(o *sns.Options) { o.BaseEndpoint = aws.String("http://local") }),
				IAM:      iam.NewFromConfig(cfg, func(o *iam.Options) { o.BaseEndpoint = aws.String("http://local") }),
				SSM:      ssm.NewFromConfig(cfg, func(o *ssm.Options) { o.BaseEndpoint = aws.String("http://local") }),
				KMS:      kms.NewFromConfig(cfg, func(o *kms.Options) { o.BaseEndpoint = aws.String("http://local") }),
				SecretsManager: secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
					o.BaseEndpoint = aws.String("http://local")
				}),
			}
			err = demo.LoadData(t.Context(), loadClients)
			require.NoError(t, err)

			// Verify DynamoDB
			tableName := "Movies"
			items, err := ddbClient.Scan(t.Context(), &dynamodb.ScanInput{TableName: &tableName})
			require.NoError(t, err)
			assert.Equal(t, tt.wantDDBCount, items.Count)

			// Verify S3
			bucketName := "demo-bucket"
			objects, err := s3Client.ListObjectsV2(
				t.Context(),
				&s3.ListObjectsV2Input{Bucket: &bucketName},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantS3Count, *objects.KeyCount)
		})
	}
}

func TestLoadECS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		wantTaskDefFamily   string
		wantClusterNames    []string
		wantTaskDefRevision int32
	}{
		{
			name:                "seeds two demo clusters and one task definition",
			wantClusterNames:    []string{"demo-cluster", "production-cluster"},
			wantTaskDefFamily:   "demo-web",
			wantTaskDefRevision: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Set up all required in-memory backends (LoadData calls DynamoDB, S3,
			// SQS, SNS, IAM, SSM, KMS, SecretsManager unconditionally).
			ddbHandler := ddbbackend.NewHandler(ddbbackend.NewInMemoryDB())
			s3Handler := s3backend.NewHandler(s3backend.NewInMemoryBackend(nil))
			ecsHandler := ecsbackend.NewHandler(
				ecsbackend.NewInMemoryBackend(
					config.DefaultAccountID, config.DefaultRegion, ecsbackend.NewNoopRunner(),
				),
			)

			// Setup Echo server with service registry.
			e := echo.New()
			e.Pre(logger.EchoMiddleware(slog.Default()))

			registry := service.NewRegistry()
			_ = registry.Register(ddbHandler)
			_ = registry.Register(s3Handler)
			_ = registry.Register(sqsbackend.NewHandler(sqsbackend.NewInMemoryBackend()))
			_ = registry.Register(snsbackend.NewHandler(snsbackend.NewInMemoryBackend()))
			_ = registry.Register(iambackend.NewHandler(iambackend.NewInMemoryBackend()))
			_ = registry.Register(ssmbackend.NewHandler(ssmbackend.NewInMemoryBackend()))
			_ = registry.Register(stsbackend.NewHandler(stsbackend.NewInMemoryBackend()))
			_ = registry.Register(kmsbackend.NewHandler(kmsbackend.NewInMemoryBackend()))
			_ = registry.Register(secretsmanagerbackend.NewHandler(secretsmanagerbackend.NewInMemoryBackend()))
			_ = registry.Register(ecsHandler)

			router := service.NewServiceRouter(registry)
			e.Use(router.RouteHandler())

			// Setup AWS SDK clients routed through in-memory Echo.
			inMemClient := &dashboard.InMemClient{Handler: e}

			cfg, err := awscfg.LoadDefaultConfig(
				t.Context(),
				awscfg.WithRegion(config.DefaultRegion),
				awscfg.WithCredentialsProvider(
					credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
				),
				awscfg.WithHTTPClient(inMemClient),
			)
			require.NoError(t, err)

			ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
				o.BaseEndpoint = aws.String("http://local")
			})
			s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
				o.UsePathStyle = true
				o.BaseEndpoint = aws.String("http://local")
			})
			ecsClient := ecs.NewFromConfig(cfg, func(o *ecs.Options) {
				o.BaseEndpoint = aws.String("http://local")
			})

			loadClients := &demo.Clients{
				DynamoDB: ddbClient,
				S3:       s3Client,
				SQS: sqs.NewFromConfig(
					cfg,
					func(o *sqs.Options) { o.BaseEndpoint = aws.String("http://local") },
				),
				SNS: sns.NewFromConfig(
					cfg,
					func(o *sns.Options) { o.BaseEndpoint = aws.String("http://local") },
				),
				IAM: iam.NewFromConfig(
					cfg,
					func(o *iam.Options) { o.BaseEndpoint = aws.String("http://local") },
				),
				SSM: ssm.NewFromConfig(
					cfg,
					func(o *ssm.Options) { o.BaseEndpoint = aws.String("http://local") },
				),
				KMS: kms.NewFromConfig(
					cfg,
					func(o *kms.Options) { o.BaseEndpoint = aws.String("http://local") },
				),
				SecretsManager: secretsmanager.NewFromConfig(
					cfg,
					func(o *secretsmanager.Options) { o.BaseEndpoint = aws.String("http://local") },
				),
				ECS: ecsClient,
			}
			err = demo.LoadData(t.Context(), loadClients)
			require.NoError(t, err)

			// Verify clusters.
			out, err := ecsClient.DescribeClusters(t.Context(), &ecs.DescribeClustersInput{
				Clusters: tt.wantClusterNames,
			})
			require.NoError(t, err)
			require.Len(t, out.Clusters, len(tt.wantClusterNames))

			gotNames := make([]string, 0, len(out.Clusters))
			for _, cl := range out.Clusters {
				gotNames = append(gotNames, aws.ToString(cl.ClusterName))
			}
			assert.ElementsMatch(t, tt.wantClusterNames, gotNames)

			// Verify task definition.
			tdOut, err := ecsClient.DescribeTaskDefinition(t.Context(), &ecs.DescribeTaskDefinitionInput{
				TaskDefinition: aws.String(tt.wantTaskDefFamily),
			})
			require.NoError(t, err)
			require.NotNil(t, tdOut.TaskDefinition)
			assert.Equal(t, tt.wantTaskDefFamily, aws.ToString(tdOut.TaskDefinition.Family))
			assert.Equal(t, tt.wantTaskDefRevision, tdOut.TaskDefinition.Revision)
		})
	}
}

func TestLoadCodePipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		wantPipelineLen int
	}{
		{
			name:            "seeds two demo pipelines",
			wantPipelineLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ddbHandler := ddbbackend.NewHandler(ddbbackend.NewInMemoryDB())
			s3Handler := s3backend.NewHandler(s3backend.NewInMemoryBackend(&s3backend.GzipCompressor{}))
			cpHandler := codepipelinebackend.NewHandler(
				codepipelinebackend.NewInMemoryBackend(
					config.DefaultAccountID, config.DefaultRegion,
				),
			)

			e := echo.New()
			e.Pre(logger.EchoMiddleware(slog.Default()))

			registry := service.NewRegistry()
			_ = registry.Register(ddbHandler)
			_ = registry.Register(s3Handler)
			_ = registry.Register(sqsbackend.NewHandler(sqsbackend.NewInMemoryBackend()))
			_ = registry.Register(snsbackend.NewHandler(snsbackend.NewInMemoryBackend()))
			_ = registry.Register(iambackend.NewHandler(iambackend.NewInMemoryBackend()))
			_ = registry.Register(ssmbackend.NewHandler(ssmbackend.NewInMemoryBackend()))
			_ = registry.Register(stsbackend.NewHandler(stsbackend.NewInMemoryBackend()))
			_ = registry.Register(kmsbackend.NewHandler(kmsbackend.NewInMemoryBackend()))
			_ = registry.Register(secretsmanagerbackend.NewHandler(secretsmanagerbackend.NewInMemoryBackend()))
			_ = registry.Register(cpHandler)

			router := service.NewServiceRouter(registry)
			e.Use(router.RouteHandler())

			inMemClient := &dashboard.InMemClient{Handler: e}

			cfg, err := awscfg.LoadDefaultConfig(
				t.Context(),
				awscfg.WithRegion(config.DefaultRegion),
				awscfg.WithCredentialsProvider(
					credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
				),
				awscfg.WithHTTPClient(inMemClient),
			)
			require.NoError(t, err)

			ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
				o.BaseEndpoint = aws.String("http://local")
			})
			cpClient := codepipelinesvc.NewFromConfig(cfg, func(o *codepipelinesvc.Options) {
				o.BaseEndpoint = aws.String("http://local")
			})

			loadClients := &demo.Clients{
				DynamoDB: ddbClient,
				S3: s3.NewFromConfig(cfg, func(o *s3.Options) {
					o.UsePathStyle = true
					o.BaseEndpoint = aws.String("http://local")
				}),
				SQS: sqs.NewFromConfig(cfg, func(o *sqs.Options) {
					o.BaseEndpoint = aws.String("http://local")
				}),
				SNS: sns.NewFromConfig(cfg, func(o *sns.Options) {
					o.BaseEndpoint = aws.String("http://local")
				}),
				IAM: iam.NewFromConfig(cfg, func(o *iam.Options) {
					o.BaseEndpoint = aws.String("http://local")
				}),
				SSM: ssm.NewFromConfig(cfg, func(o *ssm.Options) {
					o.BaseEndpoint = aws.String("http://local")
				}),
				KMS: kms.NewFromConfig(cfg, func(o *kms.Options) {
					o.BaseEndpoint = aws.String("http://local")
				}),
				SecretsManager: secretsmanager.NewFromConfig(cfg, func(o *secretsmanager.Options) {
					o.BaseEndpoint = aws.String("http://local")
				}),
				CodePipeline: cpClient,
			}

			err = demo.LoadData(t.Context(), loadClients)
			require.NoError(t, err)

			pipelines := cpHandler.Backend.ListPipelines()
			assert.Len(t, pipelines, tt.wantPipelineLen)
		})
	}
}
