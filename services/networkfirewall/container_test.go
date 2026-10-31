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

func eksConfig(cluster string) []types.ContainerMonitoringConfiguration {
	return []types.ContainerMonitoringConfiguration{{
		ClusterArn: aws.String(cluster),
		AttributeFilters: []types.ContainerAttribute{
			{Key: aws.String("namespace"), Value: aws.String("prod")},
		},
	}}
}

func TestContainerAssociations(t *testing.T) {
	t.Parallel()

	const cluster = "arn:aws:eks:us-east-1:123456789012:cluster/c1"

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{name: "lifecycle", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			created, err := f.client.CreateContainerAssociation(ctx, &sdk.CreateContainerAssociationInput{
				ContainerAssociationName: aws.String("ca"), Type: types.ContainerMonitoringTypeEks,
				ContainerMonitoringConfigurations: eksConfig(cluster),
				Description:                       aws.String("d"),
				Tags:                              []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
			})
			require.NoError(t, err)
			assert.Equal(t, types.ContainerAssociationStatusCreating, created.Status)
			assert.Contains(t, aws.ToString(created.ContainerAssociationArn), ":container-association/ca")

			require.Eventually(t, func() bool {
				d, derr := f.client.DescribeContainerAssociation(ctx, &sdk.DescribeContainerAssociationInput{
					ContainerAssociationName: aws.String("ca"),
				})

				return derr == nil && d.Status == types.ContainerAssociationStatusActive
			}, 5e9, 10e6)

			d, err := f.client.DescribeContainerAssociation(ctx, &sdk.DescribeContainerAssociationInput{
				ContainerAssociationArn: created.ContainerAssociationArn,
			})
			require.NoError(t, err)
			assert.Equal(t, types.ContainerMonitoringTypeEks, d.Type)
			assert.NotNil(t, d.LastUpdatedTime)
			require.Len(t, d.ContainerMonitoringConfigurations, 1)
			assert.Equal(t, "prod", aws.ToString(d.ContainerMonitoringConfigurations[0].AttributeFilters[0].Value))
			require.Len(t, d.Tags, 1)

			upd, err := f.client.UpdateContainerAssociation(ctx, &sdk.UpdateContainerAssociationInput{
				ContainerAssociationName: aws.String("ca"), Type: types.ContainerMonitoringTypeEcs,
				UpdateToken:                       d.UpdateToken,
				ContainerMonitoringConfigurations: eksConfig("arn:aws:ecs:us-east-1:123456789012:cluster/c2"),
				Description:                       aws.String("new"),
			})
			require.NoError(t, err)
			assert.Equal(t, types.ContainerAssociationStatusUpdating, upd.Status)
			assert.Equal(t, types.ContainerMonitoringTypeEcs, upd.Type)
			assert.NotEqual(t, aws.ToString(d.UpdateToken), aws.ToString(upd.UpdateToken))

			_, err = f.client.UpdateContainerAssociation(ctx, &sdk.UpdateContainerAssociationInput{
				ContainerAssociationName: aws.String("ca"), Type: types.ContainerMonitoringTypeEcs,
				UpdateToken: d.UpdateToken, ContainerMonitoringConfigurations: eksConfig(cluster),
			})
			assert.Equal(t, "InvalidTokenException", errCode(t, err))

			list, err := f.client.ListContainerAssociations(ctx, &sdk.ListContainerAssociationsInput{})
			require.NoError(t, err)
			require.Len(t, list.ContainerAssociations, 1)
			assert.Equal(t, "ca", aws.ToString(list.ContainerAssociations[0].Name))

			del, err := f.client.DeleteContainerAssociation(ctx, &sdk.DeleteContainerAssociationInput{
				ContainerAssociationName: aws.String("ca"),
			})
			require.NoError(t, err)
			assert.Equal(t, types.ContainerAssociationStatusDeleting, del.Status)

			require.Eventually(t, func() bool {
				_, derr := f.client.DescribeContainerAssociation(ctx, &sdk.DescribeContainerAssociationInput{
					ContainerAssociationName: aws.String("ca"),
				})

				return derr != nil
			}, 5e9, 10e6)
		}},
		{name: "validation", run: func(t *testing.T, f *fixture) {
			t.Helper()

			ctx := t.Context()
			in := &sdk.CreateContainerAssociationInput{
				ContainerAssociationName: aws.String("ca"), Type: types.ContainerMonitoringTypeEks,
				ContainerMonitoringConfigurations: eksConfig(cluster),
			}
			_, err := f.client.CreateContainerAssociation(ctx, in)
			require.NoError(t, err)

			_, err = f.client.CreateContainerAssociation(ctx, in)
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			in.ContainerAssociationName = aws.String("other")
			in.Type = "DOCKER"
			_, err = f.client.CreateContainerAssociation(ctx, in)
			assert.Equal(t, "InvalidRequestException", errCode(t, err))

			_, err = f.client.DescribeContainerAssociation(ctx, &sdk.DescribeContainerAssociationInput{
				ContainerAssociationName: aws.String("ghost"),
			})
			assert.Equal(t, "ResourceNotFoundException", errCode(t, err))
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

func TestSnapshotRestoreNewResources(t *testing.T) {
	t.Parallel()

	src := newFixture(t)
	ctx := t.Context()

	src.proxyRuleGroup(t, "rg", &types.ProxyRulesByRequestPhase{
		PreDNS: []types.ProxyRule{domainRule("a", types.ProxyRulePhaseActionAllow)},
	})
	cfg := src.proxyConfig(t, "rg")

	_, err := src.client.CreateProxy(ctx, &sdk.CreateProxyInput{
		ProxyName: aws.String("px"), NatGatewayId: aws.String("nat-1"), ProxyConfigurationName: aws.String("cfg"),
		TlsInterceptProperties: &types.TlsInterceptPropertiesRequest{TlsInterceptMode: types.TlsInterceptModeDisabled},
	})
	require.NoError(t, err)

	created, err := src.tgwFirewall(t, "tgwfw", "tgw-1", "us-east-1a")
	require.NoError(t, err)

	pol := src.policy(t, "pol")
	fw := src.firewall(t, "fw", aws.ToString(pol.FirewallPolicyResponse.FirewallPolicyArn))
	assoc := src.endpointAssoc(t, fw.Firewall.FirewallArn)

	op, err := src.client.StartFlowCapture(ctx, &sdk.StartFlowCaptureInput{
		FirewallArn: fw.Firewall.FirewallArn, AvailabilityZone: aws.String(testRegion + "a"),
		FlowFilters: []types.FlowFilter{},
	})
	require.NoError(t, err)

	_, err = src.client.CreateContainerAssociation(ctx, &sdk.CreateContainerAssociationInput{
		ContainerAssociationName: aws.String("ca"), Type: types.ContainerMonitoringTypeEks,
		ContainerMonitoringConfigurations: eksConfig("arn:aws:eks:us-east-1:123456789012:cluster/c1"),
	})
	require.NoError(t, err)

	snap := src.handler.Snapshot(ctx)
	require.NotEmpty(t, snap)

	dst := newFixture(t)
	require.NoError(t, dst.handler.Restore(ctx, snap))

	d, err := dst.client.DescribeProxyConfiguration(
		ctx,
		&sdk.DescribeProxyConfigurationInput{ProxyConfigurationName: aws.String("cfg")},
	)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(cfg.UpdateToken), aws.ToString(d.UpdateToken))
	require.Len(t, d.ProxyConfiguration.RuleGroups, 1)

	_, err = dst.client.DescribeProxyRule(ctx, &sdk.DescribeProxyRuleInput{
		ProxyRuleGroupName: aws.String("rg"), ProxyRuleName: aws.String("a"),
	})
	require.NoError(t, err)

	_, err = dst.client.DescribeProxy(ctx, &sdk.DescribeProxyInput{ProxyName: aws.String("px")})
	require.NoError(t, err)

	tgw, err := dst.client.DescribeFirewall(ctx, &sdk.DescribeFirewallInput{FirewallName: aws.String("tgwfw")})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.FirewallStatus.TransitGatewayAttachmentSyncState.AttachmentId),
		aws.ToString(tgw.FirewallStatus.TransitGatewayAttachmentSyncState.AttachmentId))

	_, err = dst.client.DescribeVpcEndpointAssociation(ctx, &sdk.DescribeVpcEndpointAssociationInput{
		VpcEndpointAssociationArn: assoc.VpcEndpointAssociation.VpcEndpointAssociationArn,
	})
	require.NoError(t, err)

	_, err = dst.client.DescribeFlowOperation(ctx, &sdk.DescribeFlowOperationInput{
		FirewallArn: fw.Firewall.FirewallArn, FlowOperationId: op.FlowOperationId,
	})
	require.NoError(t, err)

	_, err = dst.client.DescribeContainerAssociation(ctx, &sdk.DescribeContainerAssociationInput{
		ContainerAssociationName: aws.String("ca"),
	})
	require.NoError(t, err)
}
