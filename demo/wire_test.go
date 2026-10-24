package demo_test

import (
	"context"
	"log/slog"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/iam"
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
	"github.com/blackbirdworks/gopherstack/internal/awswire/kms"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	amplifybackend "github.com/blackbirdworks/gopherstack/services/amplify"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	codedeploybackend "github.com/blackbirdworks/gopherstack/services/codedeploy"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	eksbackend "github.com/blackbirdworks/gopherstack/services/eks"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	iotbackend "github.com/blackbirdworks/gopherstack/services/iot"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

func TestLoadWireServices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		handler  func(ctx context.Context) service.Registerable
		name     string
		key      string
		listOp   string
		listKey  string
		wantSize int
	}{
		{
			name: "ecr", key: "ecr", listOp: "DescribeRepositories", listKey: "Repositories", wantSize: 3,
			handler: func(context.Context) service.Registerable {
				return ecrbackend.NewHandler(ecrbackend.NewInMemoryBackend(
					config.DefaultAccountID, config.DefaultRegion, "http://local"), nil)
			},
		},
		{
			name: "appsync", key: "appsync", listOp: "ListGraphqlApis", listKey: "GraphqlApis", wantSize: 2,
			handler: func(context.Context) service.Registerable {
				return appsyncbackend.NewHandler(appsyncbackend.NewInMemoryBackend(
					config.DefaultAccountID, config.DefaultRegion, "http://local"))
			},
		},
		{
			name: "amplify", key: "amplify", listOp: "ListApps", listKey: "Apps", wantSize: 2,
			handler: func(context.Context) service.Registerable {
				return amplifybackend.NewHandler(amplifybackend.NewInMemoryBackend(
					config.DefaultAccountID, config.DefaultRegion))
			},
		},
		{
			name: "eks", key: "eks", listOp: "ListClusters", listKey: "Clusters", wantSize: 2,
			handler: func(ctx context.Context) service.Registerable {
				return eksbackend.NewHandler(eksbackend.NewInMemoryBackend(
					ctx, config.DefaultAccountID, config.DefaultRegion))
			},
		},
		{
			name: "iot", key: "iot", listOp: "ListThings", listKey: "Things", wantSize: 3,
			handler: func(context.Context) service.Registerable {
				return iotbackend.NewHandler(iotbackend.NewInMemoryBackendWithConfig(
					config.DefaultAccountID, config.DefaultRegion), nil)
			},
		},
		{
			name: "codedeploy", key: "codedeploy", listOp: "ListApplications", listKey: "Applications", wantSize: 3,
			handler: func(context.Context) service.Registerable {
				return codedeploybackend.NewHandler(codedeploybackend.NewInMemoryBackend(
					config.DefaultAccountID, config.DefaultRegion))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			e.Pre(logger.EchoMiddleware(slog.Default()))

			registry := service.NewRegistry()
			for _, h := range []service.Registerable{
				ddbbackend.NewHandler(ddbbackend.NewInMemoryDB()),
				s3backend.NewHandler(s3backend.NewInMemoryBackend(nil)),
				sqsbackend.NewHandler(sqsbackend.NewInMemoryBackend()),
				snsbackend.NewHandler(snsbackend.NewInMemoryBackend()),
				iambackend.NewHandler(iambackend.NewInMemoryBackend()),
				ssmbackend.NewHandler(ssmbackend.NewInMemoryBackend()),
				stsbackend.NewHandler(stsbackend.NewInMemoryBackend()),
				kmsbackend.NewHandler(kmsbackend.NewInMemoryBackend()),
				secretsmanagerbackend.NewHandler(secretsmanagerbackend.NewInMemoryBackend()),
				tt.handler(t.Context()),
			} {
				require.NoError(t, registry.Register(h))
			}

			e.Use(service.NewServiceRouter(registry).RouteHandler())

			cfg, err := awscfg.LoadDefaultConfig(
				t.Context(),
				awscfg.WithRegion(config.DefaultRegion),
				awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
				awscfg.WithHTTPClient(&dashboard.InMemClient{Handler: e}),
			)
			require.NoError(t, err)

			caller, err := demo.NewWireCaller(tt.key, http.Handler(e), cfg.Credentials, cfg.Region)
			require.NoError(t, err)

			clients := wireDemoClients(cfg)
			switch tt.key {
			case "ecr":
				clients.ECR = caller
			case "appsync":
				clients.AppSync = caller
			case "amplify":
				clients.Amplify = caller
			case "eks":
				clients.EKS = caller
			case "iot":
				clients.IoT = caller
			case "codedeploy":
				clients.CodeDeploy = caller
			}

			require.NoError(t, demo.LoadData(t.Context(), clients))

			out, err := caller.Call(t.Context(), tt.listOp, map[string]any{})
			require.NoError(t, err)

			items, _ := out[tt.listKey].([]any)
			assert.Len(t, items, tt.wantSize)
		})
	}
}

func TestNewWireCallerUnknownService(t *testing.T) {
	t.Parallel()

	_, err := demo.NewWireCaller("nope", http.NotFoundHandler(), nil, config.DefaultRegion)
	require.Error(t, err)
}

func wireDemoClients(cfg aws.Config) *demo.Clients {
	return &demo.Clients{
		DynamoDB: dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			o.BaseEndpoint = aws.String("http://local")
		}),
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
	}
}
