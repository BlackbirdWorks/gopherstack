package demo_test

import (
	"log/slog"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
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
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	iambackend "github.com/blackbirdworks/gopherstack/iam"
	kmsbackend "github.com/blackbirdworks/gopherstack/kms"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/secretsmanager"
	snsbackend "github.com/blackbirdworks/gopherstack/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
	stsbackend "github.com/blackbirdworks/gopherstack/sts"
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

			registry := service.NewRegistry(slog.Default())
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
			cfg, err := config.LoadDefaultConfig(
				t.Context(),
				config.WithRegion("us-east-1"),
				config.WithCredentialsProvider(
					credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
				),
				config.WithHTTPClient(inMemClient),
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
			err = demo.LoadData(t.Context(), slog.Default(), loadClients)
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
