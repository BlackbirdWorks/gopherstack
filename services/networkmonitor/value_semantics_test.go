package networkmonitor_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmonitorsdk "github.com/aws/aws-sdk-go-v2/service/networkmonitor"
	"github.com/aws/aws-sdk-go-v2/service/networkmonitor/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProbe_PartialUpdateKeepsOmittedFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		update   *networkmonitorsdk.UpdateProbeInput
		wantProt types.Protocol
		name     string
		wantDest string
		wantPort int32
		wantSize int32
	}{
		{
			name:     "destination only",
			update:   &networkmonitorsdk.UpdateProbeInput{Destination: aws.String("10.0.0.9")},
			wantDest: "10.0.0.9", wantPort: 80, wantSize: 100, wantProt: types.ProtocolTcp,
		},
		{
			name:     "packet size only",
			update:   &networkmonitorsdk.UpdateProbeInput{PacketSize: aws.Int32(200)},
			wantDest: "10.0.0.1", wantPort: 80, wantSize: 200, wantProt: types.ProtocolTcp,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestNetworkMonitorClient(t, newTestHandler(t))

			_, err := client.CreateMonitor(ctx, &networkmonitorsdk.CreateMonitorInput{
				MonitorName: aws.String("vs-mon"),
			})
			require.NoError(t, err)

			created, err := client.CreateProbe(ctx, &networkmonitorsdk.CreateProbeInput{
				MonitorName: aws.String("vs-mon"),
				Probe: &types.ProbeInput{
					SourceArn:   aws.String("arn:aws:ec2:us-east-1:123456789012:subnet/subnet-1"),
					Destination: aws.String("10.0.0.1"), Protocol: types.ProtocolTcp,
					DestinationPort: aws.Int32(80), PacketSize: aws.Int32(100),
				},
			})
			require.NoError(t, err)

			tt.update.MonitorName = aws.String("vs-mon")
			tt.update.ProbeId = created.ProbeId
			_, err = client.UpdateProbe(ctx, tt.update)
			require.NoError(t, err)

			got, err := client.GetProbe(ctx, &networkmonitorsdk.GetProbeInput{
				MonitorName: aws.String("vs-mon"), ProbeId: created.ProbeId,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantDest, aws.ToString(got.Destination))
			assert.Equal(t, tt.wantPort, aws.ToInt32(got.DestinationPort))
			assert.Equal(t, tt.wantSize, aws.ToInt32(got.PacketSize))
			assert.Equal(t, tt.wantProt, got.Protocol)
			assert.Equal(t, created.CreatedAt, got.CreatedAt)
			assert.Equal(t, types.ProbeStateActive, got.State)
		})
	}
}
