package servicediscovery_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdsdk "github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/servicediscovery"
)

// gopherstack-3gbe: Cloud Map's discovery family carries the same
// client-side host-prefix rewrite Omics has (gopherstack-keee). Two ops,
// one literal prefix -- "data-" (DiscoverInstances, DiscoverInstancesRevision)
// -- confirmed by grepping servicediscovery@v1.43.4's api_op_*.go for
// `req.URL.Host = "..." + req.URL.Host`, matching gopherstack-3gbe's filing
// exactly.
//
// Handler.RouteMatcher (handler.go:151) matches on the X-Amz-Target header
// prefix "Route53AutoNaming_v20170314.", never Host or Path, so the rewrite
// can't create a routing collision here at all -- header-based dispatch is
// inherently immune to the path-collision class this bug family could
// otherwise cause. Same conclusion as Omics: no gopherstack routing/auth
// code needs to change, the gap is a pure client-side DNS/dial failure.
//
// servicediscovery already has a real-SDK-client round trip
// (handler_create_tags_test.go), but it never exercises DiscoverInstances or
// DiscoverInstancesRevision, so the host-prefix reachability of this family
// specifically had never been proven either way before this test.
const (
	sdHostPrefixAccountID = "000000000000"
	sdHostPrefixRegion    = "us-east-1"
)

func dialToRealAddr(realAddr string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				var d net.Dialer

				return d.DialContext(ctx, network, realAddr)
			},
		},
	}
}

func newServiceDiscoveryHostPrefixTestClient(t *testing.T, redialFix bool) *sdsdk.Client {
	t.Helper()

	backend := servicediscovery.NewInMemoryBackend(sdHostPrefixAccountID, sdHostPrefixRegion)
	h := servicediscovery.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfgOpts := []func(*awscfg.LoadOptions) error{
		awscfg.WithRegion(sdHostPrefixRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	}
	if redialFix {
		cfgOpts = append(cfgOpts, awscfg.WithHTTPClient(dialToRealAddr(srv.Listener.Addr().String())))
	}

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), cfgOpts...)
	require.NoError(t, err)

	return sdsdk.NewFromConfig(cfg, func(o *sdsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestSDKRoundTrip_HostPrefix_Unreachable_BeforeFix drives an unmodified SDK
// client through both "data-" prefixed ops and proves neither can dial.
func TestSDKRoundTrip_HostPrefix_Unreachable_BeforeFix(t *testing.T) {
	t.Parallel()

	cases := []struct {
		probe func(ctx context.Context, client *sdsdk.Client) error
		name  string
	}{
		{
			name: "discover_instances",
			probe: func(ctx context.Context, client *sdsdk.Client) error {
				_, err := client.DiscoverInstances(ctx, &sdsdk.DiscoverInstancesInput{
					NamespaceName: aws.String("unreachable-probe"),
					ServiceName:   aws.String("unreachable-probe"),
				})

				return err
			},
		},
		{
			name: "discover_instances_revision",
			probe: func(ctx context.Context, client *sdsdk.Client) error {
				_, err := client.DiscoverInstancesRevision(ctx, &sdsdk.DiscoverInstancesRevisionInput{
					NamespaceName: aws.String("unreachable-probe"),
					ServiceName:   aws.String("unreachable-probe"),
				})

				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newServiceDiscoveryHostPrefixTestClient(t, false)

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			err := tc.probe(ctx, client)
			require.Error(t, err, "expected the unmodified client to fail to dial the data- rewritten host")
			t.Logf("data- unmodified-client error (expected): %v", err)
		})
	}
}

// TestSDKRoundTrip_HostPrefix_Reachable_AfterFix drives CreateHttpNamespace
// -> CreateService -> RegisterInstance -> DiscoverInstances/
// DiscoverInstancesRevision through the real SDK client with a
// redial-to-the-real-listener transport, proving gopherstack survives the
// real, un-disabled "data-" rewrite and decodes correct values.
func TestSDKRoundTrip_HostPrefix_Reachable_AfterFix(t *testing.T) {
	t.Parallel()

	client := newServiceDiscoveryHostPrefixTestClient(t, true)

	ns, err := client.CreateHttpNamespace(t.Context(), &sdsdk.CreateHttpNamespaceInput{
		Name: aws.String("keee-data-ns"),
	})
	require.NoError(t, err)

	nsOp, err := client.GetOperation(t.Context(), &sdsdk.GetOperationInput{OperationId: ns.OperationId})
	require.NoError(t, err)
	nsID := nsOp.Operation.Targets["NAMESPACE"]
	require.NotEmpty(t, nsID)

	svc, err := client.CreateService(t.Context(), &sdsdk.CreateServiceInput{
		Name:        aws.String("keee-data-svc"),
		NamespaceId: aws.String(nsID),
	})
	require.NoError(t, err)
	svcID := aws.ToString(svc.Service.Id)

	_, err = client.RegisterInstance(t.Context(), &sdsdk.RegisterInstanceInput{
		ServiceId:  aws.String(svcID),
		InstanceId: aws.String("keee-instance-1"),
		Attributes: map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1"},
	})
	require.NoError(t, err)

	discovered, err := client.DiscoverInstances(t.Context(), &sdsdk.DiscoverInstancesInput{
		NamespaceName: aws.String("keee-data-ns"),
		ServiceName:   aws.String("keee-data-svc"),
	})
	require.NoError(t, err)
	require.Len(t, discovered.Instances, 1)
	assert.Equal(t, "keee-instance-1", aws.ToString(discovered.Instances[0].InstanceId))
	assert.Equal(t, "10.0.0.1", discovered.Instances[0].Attributes["AWS_INSTANCE_IPV4"])

	revision, err := client.DiscoverInstancesRevision(t.Context(), &sdsdk.DiscoverInstancesRevisionInput{
		NamespaceName: aws.String("keee-data-ns"),
		ServiceName:   aws.String("keee-data-svc"),
	})
	require.NoError(t, err)
	assert.Positive(t, aws.ToInt64(revision.InstancesRevision))
}
