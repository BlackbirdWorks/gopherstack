package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSearchTransitGatewayMulticastGroups_ProviderFilters(t *testing.T) {
	t.Parallel()

	const groupIP, eni = "224.0.1.1", "eni-aaa"

	tests := []struct {
		filters    map[string]string
		name       string
		wantMember bool
		wantSource bool
		wantLen    int
	}{
		{
			name: "member_finder",
			filters: map[string]string{
				"group-ip-address": groupIP, "is-group-member": "true", "is-group-source": "false",
			},
			wantLen:    1,
			wantMember: true,
		},
		{
			name: "source_finder",
			filters: map[string]string{
				"group-ip-address": groupIP, "is-group-member": "false", "is-group-source": "true",
			},
			wantLen:    1,
			wantSource: true,
		},
		{name: "unknown_filter_ignored", filters: map[string]string{"bogus-filter": "x"}, wantLen: 2},
		{name: "other_group", filters: map[string]string{"group-ip-address": "224.0.9.9"}, wantLen: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestBackendAndClient(t)
			ctx := t.Context()

			tgw, err := client.CreateTransitGateway(ctx, &ec2sdk.CreateTransitGatewayInput{})
			require.NoError(t, err)

			dom, err := client.CreateTransitGatewayMulticastDomain(
				ctx, &ec2sdk.CreateTransitGatewayMulticastDomainInput{
					TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
				})
			require.NoError(t, err)

			domainID := dom.TransitGatewayMulticastDomain.TransitGatewayMulticastDomainId

			_, err = client.RegisterTransitGatewayMulticastGroupMembers(ctx,
				&ec2sdk.RegisterTransitGatewayMulticastGroupMembersInput{
					TransitGatewayMulticastDomainId: domainID, GroupIpAddress: aws.String(groupIP),
					NetworkInterfaceIds: []string{eni},
				})
			require.NoError(t, err)

			_, err = client.RegisterTransitGatewayMulticastGroupSources(ctx,
				&ec2sdk.RegisterTransitGatewayMulticastGroupSourcesInput{
					TransitGatewayMulticastDomainId: domainID, GroupIpAddress: aws.String(groupIP),
					NetworkInterfaceIds: []string{eni},
				})
			require.NoError(t, err)

			in := &ec2sdk.SearchTransitGatewayMulticastGroupsInput{TransitGatewayMulticastDomainId: domainID}
			for k, v := range tt.filters {
				in.Filters = append(in.Filters, types.Filter{Name: aws.String(k), Values: []string{v}})
			}

			out, err := client.SearchTransitGatewayMulticastGroups(ctx, in)
			require.NoError(t, err)
			require.Len(t, out.MulticastGroups, tt.wantLen)

			if tt.wantLen == 1 {
				g := out.MulticastGroups[0]
				assert.Equal(t, eni, aws.ToString(g.NetworkInterfaceId))
				assert.Equal(t, tt.wantMember, aws.ToBool(g.GroupMember))
				assert.Equal(t, tt.wantSource, aws.ToBool(g.GroupSource))
			}
		})
	}
}
