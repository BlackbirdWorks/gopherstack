package networkmonitor_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	networkmonitorsdk "github.com/aws/aws-sdk-go-v2/service/networkmonitor"
	"github.com/aws/aws-sdk-go-v2/service/networkmonitor/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/networkmonitor"
)

const networkmonitorTagsRTRegion = "us-east-1"

// newTestNetworkMonitorClient stands up the real aws-sdk-go-v2
// CloudWatchNetworkMonitor client against an httptest server running this
// package's Handler, wired through the same pkgs/service registry/router
// used in production.
func newTestNetworkMonitorClient(t *testing.T, h *networkmonitor.Handler) *networkmonitorsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(networkmonitorTagsRTRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return networkmonitorsdk.NewFromConfig(cfg, func(o *networkmonitorsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOpsWithTags_RoundTrip drives every networkmonitor Create* op
// whose real Input struct accepts Tags (networkmonitor@v1.16.4:
// api_op_CreateMonitor.go, api_op_CreateProbe.go, both
// `Tags map[string]string`) through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *networkmonitorsdk.Client) string
		name  string
	}{
		{
			name: "monitor",
			setup: func(t *testing.T, client *networkmonitorsdk.Client) string {
				t.Helper()
				out, err := client.CreateMonitor(t.Context(), &networkmonitorsdk.CreateMonitorInput{
					MonitorName: aws.String("tagged-monitor"),
					Tags:        map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.MonitorArn)
			},
		},
		{
			name: "probe",
			setup: func(t *testing.T, client *networkmonitorsdk.Client) string {
				t.Helper()
				_, err := client.CreateMonitor(t.Context(), &networkmonitorsdk.CreateMonitorInput{
					MonitorName: aws.String("monitor-for-probe"),
				})
				require.NoError(t, err)

				out, err := client.CreateProbe(t.Context(), &networkmonitorsdk.CreateProbeInput{
					MonitorName: aws.String("monitor-for-probe"),
					Probe: &types.ProbeInput{
						Destination:     aws.String("10.0.0.1"),
						DestinationPort: aws.Int32(80),
						Protocol:        types.ProtocolTcp,
						SourceArn:       aws.String("arn:aws:ec2:us-east-1:000000000000:subnet/subnet-tagtest"),
					},
					Tags: map[string]string{"env": "prod"},
				})
				require.NoError(t, err)

				return aws.ToString(out.ProbeArn)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := networkmonitor.NewInMemoryBackend(networkmonitorTagsRTRegion, "000000000000")
			client := newTestNetworkMonitorClient(t, networkmonitor.NewHandler(backend))

			arn := tc.setup(t, client)
			require.NotEmpty(t, arn)

			got, err := client.ListTagsForResource(t.Context(), &networkmonitorsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(arn),
			})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"env": "prod"}, got.Tags)
		})
	}
}
