package networkmonitor_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmonitorsdk "github.com/aws/aws-sdk-go-v2/service/networkmonitor"
	"github.com/aws/aws-sdk-go-v2/service/networkmonitor/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/blackbirdworks/gopherstack/services/networkmonitor"
)

type ec2Siblings struct{ ec2 service.Registerable }

func (f *ec2Siblings) GetEC2Handler() service.Registerable { return f.ec2 }

func TestProbe_VpcIDFromSubnet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		subnetArn func(subnetID string) string
		name      string
		wired     bool
		wantVpc   bool
	}{
		{
			name:      "known subnet",
			subnetArn: func(id string) string { return "arn:aws:ec2:us-east-1:123456789012:subnet/" + id },
			wired:     true,
			wantVpc:   true,
		},
		{
			name:      "unknown subnet",
			subnetArn: func(string) string { return "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-nope" },
			wired:     true,
		},
		{
			name:      "ec2 not wired",
			subnetArn: func(id string) string { return "arn:aws:ec2:us-east-1:123456789012:subnet/" + id },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ec2Bk := ec2backend.NewInMemoryBackend("123456789012", "us-east-1")
			vpc, err := ec2Bk.CreateVpc("10.1.0.0/16", "")
			require.NoError(t, err)
			subnet, err := ec2Bk.CreateSubnet(vpc.ID, "10.1.0.0/24", "us-east-1a")
			require.NoError(t, err)

			backend := networkmonitor.NewInMemoryBackend("us-east-1", "123456789012")
			if tt.wired {
				backend.SetAppConfig(&ec2Siblings{ec2: ec2backend.NewHandler(ec2Bk)})
			}

			client := newTestNetworkMonitorClient(t, networkmonitor.NewHandler(backend))
			ctx := t.Context()
			sourceArn := tt.subnetArn(subnet.ID)

			_, err = client.CreateMonitor(ctx, &networkmonitorsdk.CreateMonitorInput{
				MonitorName: aws.String("m1"),
				Probes: []types.CreateMonitorProbeInput{{
					Destination: aws.String("10.0.0.1"), Protocol: types.ProtocolIcmp, SourceArn: aws.String(sourceArn),
				}},
			})
			require.NoError(t, err)

			created, err := client.CreateProbe(ctx, &networkmonitorsdk.CreateProbeInput{
				MonitorName: aws.String("m1"),
				Probe: &types.ProbeInput{
					Destination: aws.String("10.0.0.2"), Protocol: types.ProtocolIcmp, SourceArn: aws.String(sourceArn),
				},
			})
			require.NoError(t, err)

			got, err := client.GetProbe(ctx, &networkmonitorsdk.GetProbeInput{
				MonitorName: aws.String("m1"), ProbeId: created.ProbeId,
			})
			require.NoError(t, err)

			mon, err := client.GetMonitor(ctx, &networkmonitorsdk.GetMonitorInput{MonitorName: aws.String("m1")})
			require.NoError(t, err)
			require.Len(t, mon.Probes, 2)

			vpcs := []*string{created.VpcId, got.VpcId, mon.Probes[0].VpcId, mon.Probes[1].VpcId}
			for _, v := range vpcs {
				if tt.wantVpc {
					assert.Equal(t, vpc.ID, aws.ToString(v))
				} else {
					assert.Empty(t, aws.ToString(v))
				}
			}
		})
	}
}
