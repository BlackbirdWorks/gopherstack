package elasticsearch_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elasticsearchsdk "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

const rtTestRegion = "us-east-1"

// newTestElasticsearchClient stands up the real aws-sdk-go-v2 Elasticsearch
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
// Round-tripping through the genuine SDK serializer/deserializer -- rather
// than decoding the raw JSON body with ad-hoc structs, as most other tests
// in this package do -- is what actually proves a response is
// wire-compatible, matching the pattern services/codedeploy's
// handler_sdk_roundtrip_test.go uses.
func newTestElasticsearchClient(t *testing.T, h *elasticsearch.Handler) *elasticsearchsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return elasticsearchsdk.NewFromConfig(cfg, func(o *elasticsearchsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_CancelDomainConfigChange proves CancelDomainConfigChangeOutput
// carries its own shape (CancelledChangeIds/CancelledChangeProperties/DryRun) rather
// than the unrelated DescribeElasticsearchDomainConfig response
// ({"DomainConfig": {...}}) it was previously built from -- a borrowed-shape bug
// invisible to a hand-decoded-JSON test because restjson1 silently ignores unknown
// top-level keys. DryRun is the cleanest observable: the old handler never read the
// request body at all, so a real client's DryRun=true was dropped and the response
// never had a "DryRun" key, leaving output.DryRun permanently nil regardless of what
// the caller asked for.
func Test_SDKRoundTrip_CancelDomainConfigChange(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
	h := elasticsearch.NewHandler(backend)
	client := newTestElasticsearchClient(t, h)
	ctx := t.Context()

	const domainName = "rt-cancel-config-domain"

	_, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{
		DomainName: aws.String(domainName),
	})
	require.NoError(t, err, "CreateElasticsearchDomain should succeed")

	out, err := client.CancelDomainConfigChange(ctx, &elasticsearchsdk.CancelDomainConfigChangeInput{
		DomainName: aws.String(domainName),
		DryRun:     aws.Bool(true),
	})
	require.NoError(t, err, "CancelDomainConfigChange should succeed")

	require.NotNil(t, out.DryRun, "DryRun must round-trip through the response, not be silently dropped")
	assert.True(t, *out.DryRun, "DryRun should echo the request value")
	assert.NotNil(t, out.CancelledChangeIds, "CancelledChangeIds should be an empty list, not absent")
	assert.Empty(t, out.CancelledChangeIds, "no config change is ever pending in this synchronous backend")
	assert.NotNil(t, out.CancelledChangeProperties, "CancelledChangeProperties should be an empty list, not absent")
}

// Test_SDKRoundTrip_CreateVpcEndpoint_VpcOptions proves CreateVpcEndpoint's request
// VpcOptions decodes as the real types.VPCOptions shape ({SecurityGroupIds,SubnetIds},
// both string lists) rather than the flat map[string]string gopherstack previously
// parsed it as. A real SDK client always serializes VpcOptions this way -- the old
// shape made json.Unmarshal fail on every array-valued field, so CreateVpcEndpoint
// with real security groups or subnets always 400'd. Also proves the response
// VpcOptions (types.VPCDerivedInfo, same two fields) round-trips back out.
func Test_SDKRoundTrip_CreateVpcEndpoint_VpcOptions(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)
	h := elasticsearch.NewHandler(backend)
	client := newTestElasticsearchClient(t, h)
	ctx := t.Context()

	const domainName = "rt-vpcendpoint-domain"

	createOut, err := client.CreateElasticsearchDomain(ctx, &elasticsearchsdk.CreateElasticsearchDomainInput{
		DomainName: aws.String(domainName),
	})
	require.NoError(t, err, "CreateElasticsearchDomain should succeed")

	out, err := client.CreateVpcEndpoint(ctx, &elasticsearchsdk.CreateVpcEndpointInput{
		DomainArn: createOut.DomainStatus.ARN,
		VpcOptions: &types.VPCOptions{
			SecurityGroupIds: []string{"sg-0123456789abcdef0"},
			SubnetIds:        []string{"subnet-0123456789abcdef0"},
		},
	})
	require.NoError(t, err, "CreateVpcEndpoint should succeed with a real VpcOptions request body")

	require.NotNil(t, out.VpcEndpoint)
	require.NotNil(t, out.VpcEndpoint.VpcOptions, "VpcOptions should round-trip on the response")
	assert.Equal(t, []string{"sg-0123456789abcdef0"}, out.VpcEndpoint.VpcOptions.SecurityGroupIds)
	assert.Equal(t, []string{"subnet-0123456789abcdef0"}, out.VpcEndpoint.VpcOptions.SubnetIds)
}
