package mq_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mqsdk "github.com/aws/aws-sdk-go-v2/service/mq"
	mqtypes "github.com/aws/aws-sdk-go-v2/service/mq/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mq"
)

const mqTagsRTRegion = "us-east-1"

// newTestMQClient stands up the real aws-sdk-go-v2 mq client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestMQClient(t *testing.T, h *mq.Handler) *mqsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(mqTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return mqsdk.NewFromConfig(cfg, func(o *mqsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every mq Create op whose real
// Input struct accepts Tags (mq@v1.39.4: api_op_CreateBroker.go:187,
// api_op_CreateConfiguration.go:60) through the real SDK client and asserts
// ListTags sees what was supplied at creation (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *mqsdk.Client) string
		name  string
	}{
		{
			name: "broker",
			setup: func(t *testing.T, client *mqsdk.Client) string {
				t.Helper()

				out, err := client.CreateBroker(t.Context(), &mqsdk.CreateBrokerInput{
					BrokerName:         aws.String("tagged-broker"),
					EngineType:         "ACTIVEMQ",
					EngineVersion:      aws.String("5.15.14"),
					HostInstanceType:   aws.String("mq.t3.micro"),
					DeploymentMode:     "SINGLE_INSTANCE",
					PubliclyAccessible: aws.Bool(false),
					Users: []mqtypes.User{
						{Username: aws.String("admin"), Password: aws.String("supersecretpassword1")},
					},
					Tags: map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return aws.ToString(out.BrokerArn)
			},
		},
		{
			name: "configuration",
			setup: func(t *testing.T, client *mqsdk.Client) string {
				t.Helper()

				out, err := client.CreateConfiguration(t.Context(), &mqsdk.CreateConfigurationInput{
					Name:          aws.String("tagged-config"),
					EngineType:    "ACTIVEMQ",
					EngineVersion: aws.String("5.15.14"),
					Tags:          map[string]string{"env": "test"},
				})
				require.NoError(t, err)

				return aws.ToString(out.Arn)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := mq.NewInMemoryBackend("000000000000", mqTagsRTRegion)
			client := newTestMQClient(t, mq.NewHandler(backend))

			resourceARN := tt.setup(t, client)
			require.NotEmpty(t, resourceARN)

			got, err := client.ListTags(t.Context(), &mqsdk.ListTagsInput{ResourceArn: aws.String(resourceARN)})
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, "test", got.Tags["env"])
		})
	}
}
