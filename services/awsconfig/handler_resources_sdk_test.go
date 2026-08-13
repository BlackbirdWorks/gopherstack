package awsconfig_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	configservicesdk "github.com/aws/aws-sdk-go-v2/service/configservice"
	"github.com/aws/aws-sdk-go-v2/service/configservice/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// newTestAWSConfigSDKClient stands up the real aws-sdk-go-v2 configservice
// client against an httptest server running this package's Handler.
func newTestAWSConfigSDKClient(t *testing.T, h *awsconfig.Handler) *configservicesdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return configservicesdk.NewFromConfig(cfg, func(o *configservicesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetAggregateResourceConfig_RoundTrip drives GetAggregateResourceConfig
// through a real SDK client and proves each distinct ResourceIdentifier
// resolves to the matching resource, instead of the pre-fix bug where the
// op decoded into an *emptyInput and always returned "the first resource
// config found" regardless of what was requested (gopherstack-h910).
func TestGetAggregateResourceConfig_RoundTrip(t *testing.T) {
	t.Parallel()

	h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
	client := newTestAWSConfigSDKClient(t, h)

	_, err := client.PutConfigurationAggregator(t.Context(), &configservicesdk.PutConfigurationAggregatorInput{
		ConfigurationAggregatorName: aws.String("my-aggregator"),
	})
	require.NoError(t, err)

	_, err = client.PutResourceConfig(t.Context(), &configservicesdk.PutResourceConfigInput{
		ResourceType:    aws.String("AWS::EC2::Instance"),
		ResourceId:      aws.String("i-first"),
		Configuration:   aws.String(`{"a":1}`),
		SchemaVersionId: aws.String("1.0"),
	})
	require.NoError(t, err)

	_, err = client.PutResourceConfig(t.Context(), &configservicesdk.PutResourceConfigInput{
		ResourceType:    aws.String("AWS::S3::Bucket"),
		ResourceId:      aws.String("my-bucket"),
		Configuration:   aws.String(`{"a":2}`),
		SchemaVersionId: aws.String("1.0"),
	})
	require.NoError(t, err)

	t.Run("distinct_requests_return_distinct_items", func(t *testing.T) {
		t.Parallel()

		out1, getErr1 := client.GetAggregateResourceConfig(
			t.Context(),
			&configservicesdk.GetAggregateResourceConfigInput{
				ConfigurationAggregatorName: aws.String("my-aggregator"),
				ResourceIdentifier: &types.AggregateResourceIdentifier{
					ResourceType:    types.ResourceType("AWS::EC2::Instance"),
					ResourceId:      aws.String("i-first"),
					SourceAccountId: aws.String("000000000000"),
					SourceRegion:    aws.String("us-east-1"),
				},
			},
		)
		require.NoError(t, getErr1)
		require.NotNil(t, out1.ConfigurationItem)
		assert.Equal(t, "i-first", aws.ToString(out1.ConfigurationItem.ResourceId))

		out2, getErr2 := client.GetAggregateResourceConfig(
			t.Context(),
			&configservicesdk.GetAggregateResourceConfigInput{
				ConfigurationAggregatorName: aws.String("my-aggregator"),
				ResourceIdentifier: &types.AggregateResourceIdentifier{
					ResourceType:    types.ResourceType("AWS::S3::Bucket"),
					ResourceId:      aws.String("my-bucket"),
					SourceAccountId: aws.String("000000000000"),
					SourceRegion:    aws.String("us-east-1"),
				},
			},
		)
		require.NoError(t, getErr2)
		require.NotNil(t, out2.ConfigurationItem)
		assert.Equal(t, "my-bucket", aws.ToString(out2.ConfigurationItem.ResourceId))
	})

	t.Run("unknown_aggregator_errors", func(t *testing.T) {
		t.Parallel()

		_, getErr := client.GetAggregateResourceConfig(t.Context(), &configservicesdk.GetAggregateResourceConfigInput{
			ConfigurationAggregatorName: aws.String("no-such-aggregator"),
			ResourceIdentifier: &types.AggregateResourceIdentifier{
				ResourceType:    types.ResourceType("AWS::EC2::Instance"),
				ResourceId:      aws.String("i-first"),
				SourceAccountId: aws.String("000000000000"),
				SourceRegion:    aws.String("us-east-1"),
			},
		})
		require.Error(t, getErr)
		assert.Contains(t, getErr.Error(), "NoSuchConfigurationAggregatorException")
	})

	t.Run("undiscovered_resource_errors", func(t *testing.T) {
		t.Parallel()

		_, getErr := client.GetAggregateResourceConfig(t.Context(), &configservicesdk.GetAggregateResourceConfigInput{
			ConfigurationAggregatorName: aws.String("my-aggregator"),
			ResourceIdentifier: &types.AggregateResourceIdentifier{
				ResourceType:    types.ResourceType("AWS::EC2::Instance"),
				ResourceId:      aws.String("i-does-not-exist"),
				SourceAccountId: aws.String("000000000000"),
				SourceRegion:    aws.String("us-east-1"),
			},
		})
		require.Error(t, getErr)
		assert.Contains(t, getErr.Error(), "ResourceNotDiscoveredException")
	})
}

// TestPutResourceConfig_RequiresSchemaVersionID proves SchemaVersionId is a
// required member of PutResourceConfigInput, not silently dropped
// (gopherstack-h910).
func TestPutResourceConfig_RequiresSchemaVersionID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		schemaVersionID string
		name            string
		wantErr         bool
	}{
		{name: "missing_schema_version_id_errors", schemaVersionID: "", wantErr: true},
		{name: "present_schema_version_id_succeeds", schemaVersionID: "1.0", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := awsconfig.NewHandler(awsconfig.NewInMemoryBackend())
			client := newTestAWSConfigSDKClient(t, h)

			_, err := client.PutResourceConfig(t.Context(), &configservicesdk.PutResourceConfigInput{
				ResourceType:    aws.String("AWS::EC2::Instance"),
				ResourceId:      aws.String("i-abc"),
				Configuration:   aws.String(`{}`),
				SchemaVersionId: aws.String(tt.schemaVersionID),
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")

				return
			}

			require.NoError(t, err)
		})
	}
}
