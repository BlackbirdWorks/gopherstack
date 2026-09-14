package networkmonitor_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmonitorsdk "github.com/aws/aws-sdk-go-v2/service/networkmonitor"
	"github.com/aws/aws-sdk-go-v2/service/networkmonitor/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/networkmonitor"
)

// TestRealClient_MonitorProbe drives networkmonitor's typed-coverage-blind
// ops (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_MonitorProbe(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "monitor lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				backend := networkmonitor.NewInMemoryBackend("us-east-1", "123456789012")
				client := newTestNetworkMonitorClient(t, networkmonitor.NewHandler(backend))
				ctx := t.Context()

				_, err := client.CreateMonitor(ctx, &networkmonitorsdk.CreateMonitorInput{
					MonitorName:       aws.String("s15-monitor"),
					AggregationPeriod: aws.Int64(30),
				})
				require.NoError(t, err)

				getOut, err := client.GetMonitor(ctx, &networkmonitorsdk.GetMonitorInput{
					MonitorName: aws.String("s15-monitor"),
				})
				require.NoError(t, err)
				assert.Equal(t, "s15-monitor", aws.ToString(getOut.MonitorName))
				assert.EqualValues(t, 30, aws.ToInt64(getOut.AggregationPeriod))
				assert.NotEmpty(t, aws.ToString(getOut.MonitorArn))

				updOut, err := client.UpdateMonitor(ctx, &networkmonitorsdk.UpdateMonitorInput{
					MonitorName:       aws.String("s15-monitor"),
					AggregationPeriod: aws.Int64(60),
				})
				require.NoError(t, err)
				assert.EqualValues(t, 60, aws.ToInt64(updOut.AggregationPeriod))

				_, err = client.CreateMonitor(ctx, &networkmonitorsdk.CreateMonitorInput{
					MonitorName:       aws.String("s15-monitor-2"),
					AggregationPeriod: aws.Int64(30),
				})
				require.NoError(t, err)

				listOut, err := client.ListMonitors(ctx, &networkmonitorsdk.ListMonitorsInput{})
				require.NoError(t, err)
				names := map[string]bool{}
				for _, m := range listOut.Monitors {
					names[aws.ToString(m.MonitorName)] = true
				}
				assert.True(t, names["s15-monitor"])
				assert.True(t, names["s15-monitor-2"])
			},
		},
		{
			name: "probe lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				backend := networkmonitor.NewInMemoryBackend("us-east-1", "123456789012")
				client := newTestNetworkMonitorClient(t, networkmonitor.NewHandler(backend))
				ctx := t.Context()

				_, err := client.CreateMonitor(ctx, &networkmonitorsdk.CreateMonitorInput{
					MonitorName:       aws.String("s15-probe-monitor"),
					AggregationPeriod: aws.Int64(30),
				})
				require.NoError(t, err)

				createOut, err := client.CreateProbe(ctx, &networkmonitorsdk.CreateProbeInput{
					MonitorName: aws.String("s15-probe-monitor"),
					Probe: &types.ProbeInput{
						Destination:     aws.String("10.0.0.1"),
						Protocol:        types.ProtocolTcp,
						SourceArn:       aws.String("arn:aws:ec2:us-east-1:123456789012:subnet/subnet-0abc123"),
						DestinationPort: aws.Int32(443),
					},
				})
				require.NoError(t, err)
				probeID := aws.ToString(createOut.ProbeId)
				require.NotEmpty(t, probeID)

				getOut, err := client.GetProbe(ctx, &networkmonitorsdk.GetProbeInput{
					MonitorName: aws.String("s15-probe-monitor"),
					ProbeId:     aws.String(probeID),
				})
				require.NoError(t, err)
				assert.Equal(t, "10.0.0.1", aws.ToString(getOut.Destination))
				assert.Equal(t, types.ProtocolTcp, getOut.Protocol)
				assert.EqualValues(t, 443, aws.ToInt32(getOut.DestinationPort))

				updOut, err := client.UpdateProbe(ctx, &networkmonitorsdk.UpdateProbeInput{
					MonitorName:     aws.String("s15-probe-monitor"),
					ProbeId:         aws.String(probeID),
					Destination:     aws.String("10.0.0.2"),
					DestinationPort: aws.Int32(8443),
					Protocol:        types.ProtocolTcp,
				})
				require.NoError(t, err)
				assert.Equal(t, "10.0.0.2", aws.ToString(updOut.Destination))
				assert.EqualValues(t, 8443, aws.ToInt32(updOut.DestinationPort))

				_, err = client.DeleteProbe(ctx, &networkmonitorsdk.DeleteProbeInput{
					MonitorName: aws.String("s15-probe-monitor"),
					ProbeId:     aws.String(probeID),
				})
				require.NoError(t, err)

				_, err = client.GetProbe(ctx, &networkmonitorsdk.GetProbeInput{
					MonitorName: aws.String("s15-probe-monitor"),
					ProbeId:     aws.String(probeID),
				})
				require.Error(t, err)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
