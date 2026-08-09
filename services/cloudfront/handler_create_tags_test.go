package cloudfront_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// newTestCloudFrontClient stands up the real aws-sdk-go-v2 CloudFront client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestCloudFrontClient(t *testing.T, h *cloudfront.Handler) *cfsdk.Client {
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

	return cfsdk.NewFromConfig(cfg, func(o *cfsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives CreateFunction, CreateKeyValueStore, and
// CreateVpcOrigin through the real SDK client and asserts ListTagsForResource
// sees what was supplied at creation (gopherstack-2mwl). All three real Input
// structs accept Tags *types.Tags (cloudfront@v1.67.4: api_op_CreateFunction.go,
// api_op_CreateKeyValueStore.go, api_op_CreateVpcOrigin.go), but gopherstack's
// tag registry (taggableTags/taggableARNs in tags.go) had no case at all for
// these three resource kinds -- a missing-case shape identical to memorydb's,
// on top of the Create handlers never decoding the Tags element in the first
// place. Both the tag-registry gap and the decode-drop are fixed together
// here. Asserted through a real SDK client because CloudFront is REST-XML and
// a wrong element name (Items>Tag vs Items>member) would pass a handler-level
// assertion on the same map -- verified correct against
// awsRestxml_deserializeDocumentTagList, cloudfront@v1.67.4 deserializers.go:57546.
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := &types.Tags{Items: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}}

	requireTags := func(t *testing.T, client *cfsdk.Client, resourceARN *string) {
		t.Helper()
		out, err := client.ListTagsForResource(t.Context(), &cfsdk.ListTagsForResourceInput{
			Resource: resourceARN,
		})
		require.NoError(t, err)
		require.Len(t, out.Tags.Items, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags.Items[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Tags.Items[0].Value))
	}

	t.Run("createfunction", func(t *testing.T) {
		t.Parallel()

		h := cloudfront.NewHandler(
			cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1"),
		)
		client := newTestCloudFrontClient(t, h)

		out, err := client.CreateFunction(t.Context(), &cfsdk.CreateFunctionInput{
			Name:         aws.String("tagged-fn"),
			FunctionCode: []byte("function handler(event) { return event.request; }"),
			FunctionConfig: &types.FunctionConfig{
				Comment: aws.String("test"),
				Runtime: types.FunctionRuntimeCloudfrontJs20,
			},
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.FunctionSummary.FunctionMetadata.FunctionARN)
	})

	t.Run("createkeyvaluestore", func(t *testing.T) {
		t.Parallel()

		h := cloudfront.NewHandler(
			cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1"),
		)
		client := newTestCloudFrontClient(t, h)

		out, err := client.CreateKeyValueStore(t.Context(), &cfsdk.CreateKeyValueStoreInput{
			Name:    aws.String("tagged-kvs"),
			Comment: aws.String("test"),
			Tags:    tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.KeyValueStore.ARN)
	})

	t.Run("createvpcorigin", func(t *testing.T) {
		t.Parallel()

		h := cloudfront.NewHandler(
			cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1"),
		)
		client := newTestCloudFrontClient(t, h)

		out, err := client.CreateVpcOrigin(t.Context(), &cfsdk.CreateVpcOriginInput{
			VpcOriginEndpointConfig: &types.VpcOriginEndpointConfig{
				Name: aws.String("tagged-vpc-origin"),
				Arn: aws.String(
					"arn:aws:ec2:us-east-1:123456789012:vpc-endpoint/vpce-0123456789abcdef0",
				),
				HTTPPort:             aws.Int32(80),
				HTTPSPort:            aws.Int32(443),
				OriginProtocolPolicy: types.OriginProtocolPolicyHttpOnly,
			},
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.VpcOrigin.Arn)
	})

	t.Run("createtruststore", func(t *testing.T) {
		t.Parallel()

		h := cloudfront.NewHandler(
			cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1"),
		)
		client := newTestCloudFrontClient(t, h)

		out, err := client.CreateTrustStore(t.Context(), &cfsdk.CreateTrustStoreInput{
			Name: aws.String("tagged-trust-store"),
			CaCertificatesBundleSource: &types.CaCertificatesBundleSourceMemberCaCertificatesBundleS3Location{
				Value: types.CaCertificatesBundleS3Location{
					Bucket: aws.String("bucket"),
					Key:    aws.String("bundle.pem"),
					Region: aws.String("us-east-1"),
				},
			},
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.TrustStore.Arn)
	})

	t.Run("createanycastiplist", func(t *testing.T) {
		t.Parallel()

		h := cloudfront.NewHandler(
			cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1"),
		)
		client := newTestCloudFrontClient(t, h)

		out, err := client.CreateAnycastIpList(t.Context(), &cfsdk.CreateAnycastIpListInput{
			Name:    aws.String("tagged-anycast-list"),
			IpCount: aws.Int32(1),
			Tags:    tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.AnycastIpList.Arn)
	})

	t.Run("createconnectiongroup", func(t *testing.T) {
		t.Parallel()

		h := cloudfront.NewHandler(
			cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1"),
		)
		client := newTestCloudFrontClient(t, h)

		out, err := client.CreateConnectionGroup(t.Context(), &cfsdk.CreateConnectionGroupInput{
			Name: aws.String("tagged-connection-group"),
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.ConnectionGroup.Arn)
	})

	t.Run("createconnectionfunction", func(t *testing.T) {
		t.Parallel()

		h := cloudfront.NewHandler(
			cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1"),
		)
		client := newTestCloudFrontClient(t, h)

		out, err := client.CreateConnectionFunction(t.Context(), &cfsdk.CreateConnectionFunctionInput{
			Name:                   aws.String("tagged-connection-fn"),
			ConnectionFunctionCode: []byte("function handler(event) { return event.request; }"),
			ConnectionFunctionConfig: &types.FunctionConfig{
				Comment: aws.String("test"),
				Runtime: types.FunctionRuntimeCloudfrontJs20,
			},
			Tags: tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.ConnectionFunctionSummary.ConnectionFunctionArn)
	})
}
