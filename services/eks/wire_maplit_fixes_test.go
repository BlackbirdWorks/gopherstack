package eks_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

// newTestEKSClient stands up the real aws-sdk-go-v2 eks client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestEKSClient(t *testing.T, h *eks.Handler) *ekssdk.Client {
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

	return ekssdk.NewFromConfig(cfg, func(o *ekssdk.Options) {
		o.BaseEndpoint = &srv.URL
	})
}

// TestDescribeAddonConfiguration_SchemaDecodesAsString drives
// DescribeAddonConfiguration through the real eks client.
// ConfigurationSchema deserializes from a plain JSON string
// (deserializers.go, awsRestjson1_deserializeOpDocumentDescribeAddonConfigurationOutput,
// case "configurationSchema": value.(string)) -- gopherstack previously
// emitted a nested JSON object there, which failed every real client's
// decode outright.
func TestDescribeAddonConfiguration_SchemaDecodesAsString(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := eks.NewHandler(b)
	client := newTestEKSClient(t, h)

	out, err := client.DescribeAddonConfiguration(t.Context(), &ekssdk.DescribeAddonConfigurationInput{
		AddonName:    aws.String("vpc-cni"),
		AddonVersion: aws.String("v1.18.5-eksbuild.1"),
	})
	require.NoError(t, err, "real SDK client must decode DescribeAddonConfiguration without error")
	require.NotNil(t, out.ConfigurationSchema)
	assert.Contains(t, *out.ConfigurationSchema, `"type"`)
}

// TestDescribeClusterVersions_SupportDatesDecodeAsEpoch drives
// DescribeClusterVersions through the real eks client.
// EndOfStandardSupportDate/EndOfExtendedSupportDate deserialize from a
// json.Number via smithytime.ParseEpochSeconds -- gopherstack previously
// emitted "YYYY-MM-DD" date strings there.
func TestDescribeClusterVersions_SupportDatesDecodeAsEpoch(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := eks.NewHandler(b)
	client := newTestEKSClient(t, h)

	out, err := client.DescribeClusterVersions(t.Context(), &ekssdk.DescribeClusterVersionsInput{})
	require.NoError(t, err, "real SDK client must decode DescribeClusterVersions without error")
	require.NotEmpty(t, out.ClusterVersions)

	for _, v := range out.ClusterVersions {
		assert.NotNil(t, v.EndOfStandardSupportDate)
		assert.NotNil(t, v.EndOfExtendedSupportDate)
	}
}

// TestAssociateEncryptionConfig_ParamsDecodeAsArray drives
// AssociateEncryptionConfig through the real eks client. Update.Params
// deserializes as an array of {type, value} pairs (deserializers.go,
// awsRestjson1_deserializeDocumentUpdate, case "params":
// awsRestjson1_deserializeDocumentUpdateParams) -- gopherstack previously
// emitted a nested {"encryptionConfig": ...} object there.
func TestAssociateEncryptionConfig_ParamsDecodeAsArray(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	h := eks.NewHandler(b)
	client := newTestEKSClient(t, h)

	_, err := b.CreateCluster("my-cluster", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	out, err := client.AssociateEncryptionConfig(t.Context(), &ekssdk.AssociateEncryptionConfigInput{
		ClusterName: aws.String("my-cluster"),
		EncryptionConfig: []ekstypes.EncryptionConfig{
			{
				Provider:  &ekstypes.Provider{KeyArn: aws.String("arn:aws:kms:us-east-1:123456789012:key/abc")},
				Resources: []string{"secrets"},
			},
		},
	})
	require.NoError(t, err, "real SDK client must decode AssociateEncryptionConfig without error")
	require.NotNil(t, out.Update)
	require.NotEmpty(t, out.Update.Params, "Update.Params must carry the applied encryption config")
	assert.Equal(t, ekstypes.UpdateParamTypeEncryptionConfig, out.Update.Params[0].Type)
}
