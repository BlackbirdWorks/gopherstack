package codeconnections_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	codeconnectionssdk "github.com/aws/aws-sdk-go-v2/service/codeconnections"
	codeconnectionstypes "github.com/aws/aws-sdk-go-v2/service/codeconnections/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

const vpcRoundTripRegion = "us-east-1"

// newTestCodeConnectionsClient stands up the real aws-sdk-go-v2
// codeconnections client against an httptest server running this package's
// Handler, wired through the same pkgs/service registry/router used in
// production. This is what proves wire compatibility (JSON shape, field
// names) rather than calling h.Handler() directly.
func newTestCodeConnectionsClient(t *testing.T, h *codeconnections.Handler) *codeconnectionssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(vpcRoundTripRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return codeconnectionssdk.NewFromConfig(cfg, func(o *codeconnectionssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetHost_VpcConfigurationRoundTrip drives CreateHost/GetHost/ListHosts/
// UpdateHost through the real aws-sdk-go-v2 codeconnections client and
// asserts VpcConfiguration comes back populated on every op whose real
// Output struct carries it (GetHostOutput, ListHostsOutput's Host items).
// VpcConfiguration was previously omitted entirely from GetHost's response;
// this proves it now round-trips through the real SDK deserializer.
func TestGetHost_VpcConfigurationRoundTrip(t *testing.T) {
	t.Parallel()

	backend := codeconnections.NewInMemoryBackend("123456789012", vpcRoundTripRegion)
	h := codeconnections.NewHandler(backend)
	client := newTestCodeConnectionsClient(t, h)

	vpcCfg := &codeconnectionstypes.VpcConfiguration{
		VpcId:            aws.String("vpc-0123456789abcdef0"),
		SubnetIds:        []string{"subnet-aaa", "subnet-bbb"},
		SecurityGroupIds: []string{"sg-ccc"},
		TlsCertificate:   aws.String("-----BEGIN CERTIFICATE-----fake-----END CERTIFICATE-----"),
	}

	createOut, err := client.CreateHost(t.Context(), &codeconnectionssdk.CreateHostInput{
		Name:             aws.String("vpc-host"),
		ProviderType:     codeconnectionstypes.ProviderTypeGithubEnterpriseServer,
		ProviderEndpoint: aws.String("https://ghe.example.com"),
		VpcConfiguration: vpcCfg,
	})
	require.NoError(t, err)
	hostArn := aws.ToString(createOut.HostArn)

	getOut, err := client.GetHost(t.Context(), &codeconnectionssdk.GetHostInput{HostArn: aws.String(hostArn)})
	require.NoError(t, err)
	require.NotNil(t, getOut.VpcConfiguration, "GetHost must return the VpcConfiguration set at creation")
	assert.Equal(t, aws.ToString(vpcCfg.VpcId), aws.ToString(getOut.VpcConfiguration.VpcId))
	assert.Equal(t, vpcCfg.SubnetIds, getOut.VpcConfiguration.SubnetIds)
	assert.Equal(t, vpcCfg.SecurityGroupIds, getOut.VpcConfiguration.SecurityGroupIds)
	assert.Equal(t, aws.ToString(vpcCfg.TlsCertificate), aws.ToString(getOut.VpcConfiguration.TlsCertificate))

	listOut, err := client.ListHosts(t.Context(), &codeconnectionssdk.ListHostsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.Hosts, 1)
	require.NotNil(
		t,
		listOut.Hosts[0].VpcConfiguration,
		"ListHosts item must return the VpcConfiguration set at creation",
	)
	assert.Equal(t, aws.ToString(vpcCfg.VpcId), aws.ToString(listOut.Hosts[0].VpcConfiguration.VpcId))

	newVpcCfg := &codeconnectionstypes.VpcConfiguration{
		VpcId:            aws.String("vpc-fedcba9876543210f"),
		SubnetIds:        []string{"subnet-ccc"},
		SecurityGroupIds: []string{"sg-ddd"},
	}

	_, err = client.UpdateHost(t.Context(), &codeconnectionssdk.UpdateHostInput{
		HostArn:          aws.String(hostArn),
		VpcConfiguration: newVpcCfg,
	})
	require.NoError(t, err)

	getOut2, err := client.GetHost(t.Context(), &codeconnectionssdk.GetHostInput{HostArn: aws.String(hostArn)})
	require.NoError(t, err)
	require.NotNil(t, getOut2.VpcConfiguration)
	assert.Equal(t, aws.ToString(newVpcCfg.VpcId), aws.ToString(getOut2.VpcConfiguration.VpcId))
}

// TestGetHost_RawShapeExcludesFabricatedFields is a raw-body assertion (the
// typed GetHostOutput struct has no HostArn/StatusMessage/Tags fields to
// bind, so the SDK client can't observe their absence -- doJSON reads the
// wire body directly). Confirms the previously fabricated members are gone.
func TestGetHost_RawShapeExcludesFabricatedFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	hostArn := createHost(t, h, "raw-shape-host", "GitHubEnterpriseServer", "https://ghe.example.com")

	rec := doJSON(t, h, "GetHost", map[string]any{"HostArn": hostArn})
	require.Equal(t, 200, rec.Code)

	resp := parseResp(t, rec)
	for _, field := range []string{"HostArn", "StatusMessage", "Tags"} {
		_, present := resp[field]
		assert.Falsef(t, present, "GetHost response must not include a %s member", field)
	}

	for _, field := range []string{"Name", "ProviderEndpoint", "ProviderType", "Status"} {
		assert.NotEmptyf(t, resp[field], "GetHost response must include %s", field)
	}
}
