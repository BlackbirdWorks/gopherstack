package networkfirewall_test

import (
	"testing"
	"testing/synctest"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeTGWs map[string]string

func (f fakeTGWs) TransitGatewayOwner(id string) (string, bool) {
	owner, ok := f[id]

	return owner, ok
}

func (f *fixture) tgwFirewall(t *testing.T, name, tgw string, zones ...string) (*sdk.CreateFirewallOutput, error) {
	t.Helper()

	pol := f.policy(t, "pol-"+name)
	mappings := make([]types.AvailabilityZoneMapping, len(zones))

	for i, z := range zones {
		mappings[i] = types.AvailabilityZoneMapping{AvailabilityZone: aws.String(z)}
	}

	return f.client.CreateFirewall(t.Context(), &sdk.CreateFirewallInput{
		FirewallName:             aws.String(name),
		FirewallPolicyArn:        pol.FirewallPolicyResponse.FirewallPolicyArn,
		TransitGatewayId:         aws.String(tgw),
		AvailabilityZoneMappings: mappings,
	})
}

func TestTransitGatewayFirewalls(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "same account attachment becomes ready", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			created, err := f.tgwFirewall(t, "fw", "tgw-1", "us-east-1a", "us-east-1b")
			require.NoError(t, err)
			assert.Equal(t, "tgw-1", aws.ToString(created.Firewall.TransitGatewayId))
			assert.Equal(t, testAccount, aws.ToString(created.Firewall.TransitGatewayOwnerAccountId))
			assert.Len(t, created.Firewall.AvailabilityZoneMappings, 2)
			assert.Empty(t, created.Firewall.SubnetMappings)
			attach := created.FirewallStatus.TransitGatewayAttachmentSyncState
			require.NotNil(t, attach)
			assert.Equal(t, types.TransitGatewayAttachmentStatusCreating, attach.TransitGatewayAttachmentStatus)

			require.Eventually(t, func() bool {
				d, derr := f.client.DescribeFirewall(ctx, &sdk.DescribeFirewallInput{FirewallName: aws.String("fw")})

				return derr == nil && d.FirewallStatus.Status == types.FirewallStatusValueReady &&
					d.FirewallStatus.TransitGatewayAttachmentSyncState.TransitGatewayAttachmentStatus ==
						types.TransitGatewayAttachmentStatusReady
			}, 5e9, 10e6)

			d, err := f.client.DescribeFirewall(ctx, &sdk.DescribeFirewallInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)
			assert.Len(t, d.FirewallStatus.SyncStates, 2)

			meta, err := f.client.DescribeFirewallMetadata(ctx, &sdk.DescribeFirewallMetadataInput{
				FirewallArn: created.Firewall.FirewallArn,
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(attach.AttachmentId), aws.ToString(meta.TransitGatewayAttachmentId))
			assert.Contains(t, meta.SupportedAvailabilityZones, "us-east-1a")

			_, err = f.client.AcceptNetworkFirewallTransitGatewayAttachment(
				ctx,
				&sdk.AcceptNetworkFirewallTransitGatewayAttachmentInput{
					TransitGatewayAttachmentId: attach.AttachmentId,
				},
			)
			assert.Equal(t, "InvalidOperationException", errCode(t, err))

			_, err = f.client.AssociateSubnets(ctx, &sdk.AssociateSubnetsInput{
				FirewallName:   aws.String("fw"),
				SubnetMappings: []types.SubnetMapping{{SubnetId: aws.String("subnet-1")}},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))
		}},
		{name: "availability zone changes", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			_, err := f.tgwFirewall(t, "fw", "tgw-1", "us-east-1a")
			require.NoError(t, err)

			added, err := f.client.AssociateAvailabilityZones(ctx, &sdk.AssociateAvailabilityZonesInput{
				FirewallName:             aws.String("fw"),
				AvailabilityZoneMappings: []types.AvailabilityZoneMapping{{AvailabilityZone: aws.String("us-east-1b")}},
			})
			require.NoError(t, err)
			assert.Len(t, added.AvailabilityZoneMappings, 2)

			_, err = f.client.AssociateAvailabilityZones(ctx, &sdk.AssociateAvailabilityZonesInput{
				FirewallName:             aws.String("fw"),
				AvailabilityZoneMappings: []types.AvailabilityZoneMapping{{AvailabilityZone: aws.String("us-east-1b")}},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			prot, err := f.client.UpdateAvailabilityZoneChangeProtection(
				ctx,
				&sdk.UpdateAvailabilityZoneChangeProtectionInput{
					FirewallName: aws.String("fw"), AvailabilityZoneChangeProtection: true,
				},
			)
			require.NoError(t, err)
			assert.True(t, prot.AvailabilityZoneChangeProtection)

			_, err = f.client.DisassociateAvailabilityZones(ctx, &sdk.DisassociateAvailabilityZonesInput{
				FirewallName:             aws.String("fw"),
				AvailabilityZoneMappings: []types.AvailabilityZoneMapping{{AvailabilityZone: aws.String("us-east-1b")}},
			})
			assert.Equal(t, "InvalidOperationException", errCode(t, err))

			_, err = f.client.UpdateAvailabilityZoneChangeProtection(
				ctx,
				&sdk.UpdateAvailabilityZoneChangeProtectionInput{
					FirewallName: aws.String("fw"),
				},
			)
			require.NoError(t, err)

			removed, err := f.client.DisassociateAvailabilityZones(ctx, &sdk.DisassociateAvailabilityZonesInput{
				FirewallName:             aws.String("fw"),
				AvailabilityZoneMappings: []types.AvailabilityZoneMapping{{AvailabilityZone: aws.String("us-east-1b")}},
			})
			require.NoError(t, err)
			require.Len(t, removed.AvailabilityZoneMappings, 1)

			_, err = f.client.DisassociateAvailabilityZones(ctx, &sdk.DisassociateAvailabilityZonesInput{
				FirewallName:             aws.String("fw"),
				AvailabilityZoneMappings: []types.AvailabilityZoneMapping{{AvailabilityZone: aws.String("us-east-1a")}},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			d, err := f.client.DescribeFirewall(ctx, &sdk.DescribeFirewallInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)
			assert.Len(t, d.Firewall.AvailabilityZoneMappings, 1)
			assert.False(t, d.Firewall.AvailabilityZoneChangeProtection)
		}},
		{name: "cross account accept and delete", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			f.handler.Backend.SetTransitGatewayResolver(fakeTGWs{"tgw-x": "999999999999", "tgw-own": testAccount})

			created, err := f.tgwFirewall(t, "fw", "tgw-x", "us-east-1a")
			require.NoError(t, err)

			attachID := created.FirewallStatus.TransitGatewayAttachmentSyncState.AttachmentId
			assert.Equal(t, types.TransitGatewayAttachmentStatusPendingAcceptance,
				created.FirewallStatus.TransitGatewayAttachmentSyncState.TransitGatewayAttachmentStatus)
			assert.Equal(t, "999999999999", aws.ToString(created.Firewall.TransitGatewayOwnerAccountId))
			assert.Equal(t, types.FirewallStatusValueProvisioning, created.FirewallStatus.Status)

			_, err = f.client.DeleteNetworkFirewallTransitGatewayAttachment(ctx,
				&sdk.DeleteNetworkFirewallTransitGatewayAttachmentInput{TransitGatewayAttachmentId: attachID})
			assert.Equal(t, "InvalidOperationException", errCode(t, err))

			acc, err := f.client.AcceptNetworkFirewallTransitGatewayAttachment(ctx,
				&sdk.AcceptNetworkFirewallTransitGatewayAttachmentInput{TransitGatewayAttachmentId: attachID})
			require.NoError(t, err)
			assert.Equal(t, types.TransitGatewayAttachmentStatusCreating, acc.TransitGatewayAttachmentStatus)

			require.Eventually(t, func() bool {
				d, derr := f.client.DescribeFirewall(ctx, &sdk.DescribeFirewallInput{FirewallName: aws.String("fw")})

				return derr == nil && d.FirewallStatus.Status == types.FirewallStatusValueReady
			}, 5e9, 10e6)

			del, err := f.client.DeleteNetworkFirewallTransitGatewayAttachment(ctx,
				&sdk.DeleteNetworkFirewallTransitGatewayAttachmentInput{TransitGatewayAttachmentId: attachID})
			require.NoError(t, err)
			assert.Equal(t, types.TransitGatewayAttachmentStatusDeleting, del.TransitGatewayAttachmentStatus)

			_, err = f.client.AcceptNetworkFirewallTransitGatewayAttachment(
				ctx,
				&sdk.AcceptNetworkFirewallTransitGatewayAttachmentInput{
					TransitGatewayAttachmentId: aws.String("tgw-attach-ghost"),
				},
			)
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))
		}},
		{name: "reject", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			f.handler.Backend.SetTransitGatewayResolver(fakeTGWs{"tgw-x": "999999999999"})

			created, err := f.tgwFirewall(t, "fw", "tgw-x", "us-east-1a")
			require.NoError(t, err)

			rej, err := f.client.RejectNetworkFirewallTransitGatewayAttachment(ctx,
				&sdk.RejectNetworkFirewallTransitGatewayAttachmentInput{
					TransitGatewayAttachmentId: created.FirewallStatus.TransitGatewayAttachmentSyncState.AttachmentId,
				})
			require.NoError(t, err)
			assert.Equal(t, types.TransitGatewayAttachmentStatusRejecting, rej.TransitGatewayAttachmentStatus)

			require.Eventually(t, func() bool {
				d, derr := f.client.DescribeFirewall(ctx, &sdk.DescribeFirewallInput{FirewallName: aws.String("fw")})

				return derr == nil && d.FirewallStatus.Status == types.FirewallStatusValueFailed &&
					d.FirewallStatus.TransitGatewayAttachmentSyncState.TransitGatewayAttachmentStatus ==
						types.TransitGatewayAttachmentStatusRejected
			}, 5e9, 10e6)
		}},
		{name: "validation", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			f.handler.Backend.SetTransitGatewayResolver(fakeTGWs{"tgw-1": testAccount})

			_, err := f.tgwFirewall(t, "a", "tgw-ghost", "us-east-1a")
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.tgwFirewall(t, "b", "tgw-1")
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.tgwFirewall(t, "c", "tgw-1", "us-east-1a", "us-east-1a")
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			pol := f.policy(t, "pol")
			fw := f.firewall(t, "vpcfw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))

			_, err = f.client.AssociateAvailabilityZones(ctx, &sdk.AssociateAvailabilityZonesInput{
				FirewallArn:              fw.Firewall.FirewallArn,
				AvailabilityZoneMappings: []types.AvailabilityZoneMapping{{AvailabilityZone: aws.String("us-east-1b")}},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.CreateFirewall(ctx, &sdk.CreateFirewallInput{
				FirewallName: aws.String("d"), FirewallPolicyArn: pol.FirewallPolicyResponse.FirewallPolicyArn,
				VpcId: aws.String(testVpc), SubnetMappings: []types.SubnetMapping{{SubnetId: aws.String("subnet-1")}},
				AvailabilityZoneMappings: []types.AvailabilityZoneMapping{{AvailabilityZone: aws.String("us-east-1a")}},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))
		}},
		{name: "proxy settings", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			pol := f.policy(t, "pol")
			polARN := pol.FirewallPolicyResponse.FirewallPolicyArn

			created, err := f.client.CreateFirewall(ctx, &sdk.CreateFirewallInput{
				FirewallName: aws.String("fw"), FirewallPolicyArn: polARN, VpcId: aws.String(testVpc),
				SubnetMappings: []types.SubnetMapping{{SubnetId: aws.String("subnet-1")}},
				ProxySettings: &types.ProxySettings{ListenerProperties: []types.ListenerProperty{
					{Port: aws.Int32(8080), Type: types.ListenerPropertyTypeHttp},
				}},
			})
			require.NoError(t, err)
			require.Len(t, created.Firewall.ProxySettings.ListenerProperties, 1)

			upd, err := f.client.UpdateProxySettings(ctx, &sdk.UpdateProxySettingsInput{
				FirewallName: aws.String("fw"),
				ProxySettings: &types.ProxySettings{ListenerProperties: []types.ListenerProperty{
					{Port: aws.Int32(8443), Type: types.ListenerPropertyTypeHttps},
				}},
			})
			require.NoError(t, err)
			assert.EqualValues(t, 8443, aws.ToInt32(upd.ProxySettings.ListenerProperties[0].Port))

			d, err := f.client.DescribeFirewall(ctx, &sdk.DescribeFirewallInput{FirewallName: aws.String("fw")})
			require.NoError(t, err)
			assert.EqualValues(t, 8443, aws.ToInt32(d.Firewall.ProxySettings.ListenerProperties[0].Port))

			_, err = f.client.UpdateProxySettings(ctx, &sdk.UpdateProxySettingsInput{
				FirewallName: aws.String("fw"),
				ProxySettings: &types.ProxySettings{ListenerProperties: []types.ListenerProperty{
					{Port: aws.Int32(0), Type: types.ListenerPropertyTypeHttp},
				}},
			})
			assert.Equal(t, "InvalidRequestException", errCode(t, err))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				tt.run(t, newFixture(t))
			})
		})
	}
}
