package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tcpOpts(lo, hi int) *core.TcpOptions {
	return &core.TcpOptions{DestinationPortRange: &core.PortRange{Min: &lo, Max: &hi}}
}

func TestSecurityListRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "sl")
	icmpType := 8

	tests := []struct {
		name     string
		ingress  []core.IngressSecurityRule
		egress   []core.EgressSecurityRule
		wantCode string
	}{
		{"tcp", []core.IngressSecurityRule{{Protocol: new("6"), Source: new("0.0.0.0/0"), TcpOptions: tcpOpts(80, 443)}}, nil, ""},
		{"icmp", []core.IngressSecurityRule{{Protocol: new("1"), Source: new("10.0.0.0/8"),
			IcmpOptions: &core.IcmpOptions{Type: &icmpType}}}, nil, ""},
		{"service egress", nil, []core.EgressSecurityRule{{Protocol: new("all"), Destination: new("oci-iad-objectstorage"),
			DestinationType: core.EgressSecurityRuleDestinationTypeServiceCidrBlock}}, ""},
		{"bad protocol", []core.IngressSecurityRule{{Protocol: new("tcp"), Source: new("0.0.0.0/0")}}, nil, "InvalidParameter"},
		{"bad cidr", []core.IngressSecurityRule{{Protocol: new("6"), Source: new("everyone")}}, nil, "InvalidParameter"},
		{"tcp options on udp", []core.IngressSecurityRule{{Protocol: new("17"), Source: new("0.0.0.0/0"),
			TcpOptions: tcpOpts(1, 2)}}, nil, "InvalidParameter"},
		{"inverted ports", []core.IngressSecurityRule{{Protocol: new("6"), Source: new("0.0.0.0/0"),
			TcpOptions: tcpOpts(90, 80)}}, nil, "InvalidParameter"},
		{"unknown service", nil, []core.EgressSecurityRule{{Protocol: new("all"), Destination: new("nope"),
			DestinationType: core.EgressSecurityRuleDestinationTypeServiceCidrBlock}}, "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.ingress == nil {
				tt.ingress = []core.IngressSecurityRule{}
			}

			if tt.egress == nil {
				tt.egress = []core.EgressSecurityRule{}
			}

			r, err := e.client.CreateSecurityList(ctx, core.CreateSecurityListRequest{
				CreateSecurityListDetails: core.CreateSecurityListDetails{
					CompartmentId: new(testComp), VcnId: v.Id, IngressSecurityRules: tt.ingress, EgressSecurityRules: tt.egress,
				},
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode())

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, *r.DisplayName)

			u, err := e.client.UpdateSecurityList(ctx, core.UpdateSecurityListRequest{SecurityListId: r.Id,
				UpdateSecurityListDetails: core.UpdateSecurityListDetails{
					EgressSecurityRules: []core.EgressSecurityRule{{Protocol: new("all"), Destination: new("0.0.0.0/0")}},
				}})
			require.NoError(t, err)
			assert.Len(t, u.EgressSecurityRules, 1)
			assert.Len(t, u.IngressSecurityRules, len(tt.ingress))
		})
	}
}

func TestNetworkSecurityGroup(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "nsg")
	other := e.mkVcn(t, "10.9.0.0/16", "nsgother")

	mk := func(vcn *string) core.NetworkSecurityGroup {
		r, err := e.client.CreateNetworkSecurityGroup(ctx, core.CreateNetworkSecurityGroupRequest{
			CreateNetworkSecurityGroupDetails: core.CreateNetworkSecurityGroupDetails{CompartmentId: new(testComp), VcnId: vcn},
		})
		require.NoError(t, err)

		return r.NetworkSecurityGroup
	}

	a, b, foreign := mk(v.Id), mk(v.Id), mk(other.Id)

	add := func(d ...core.AddSecurityRuleDetails) (core.AddNetworkSecurityGroupSecurityRulesResponse, error) {
		return e.client.AddNetworkSecurityGroupSecurityRules(ctx, core.AddNetworkSecurityGroupSecurityRulesRequest{
			NetworkSecurityGroupId:                      a.Id,
			AddNetworkSecurityGroupSecurityRulesDetails: core.AddNetworkSecurityGroupSecurityRulesDetails{SecurityRules: d},
		})
	}

	tests := []struct {
		name     string
		rule     core.AddSecurityRuleDetails
		wantCode string
	}{
		{"ingress cidr", core.AddSecurityRuleDetails{Direction: core.AddSecurityRuleDetailsDirectionIngress,
			Protocol: new("6"), Source: new("0.0.0.0/0"), TcpOptions: tcpOpts(22, 22)}, ""},
		{"ingress nsg", core.AddSecurityRuleDetails{Direction: core.AddSecurityRuleDetailsDirectionIngress,
			Protocol: new("all"), Source: b.Id, SourceType: core.AddSecurityRuleDetailsSourceTypeNetworkSecurityGroup}, ""},
		{"egress service", core.AddSecurityRuleDetails{Direction: core.AddSecurityRuleDetailsDirectionEgress,
			Protocol: new("all"), Destination: new("oci-iad-objectstorage"),
			DestinationType: core.AddSecurityRuleDetailsDestinationTypeServiceCidrBlock}, ""},
		{"foreign nsg", core.AddSecurityRuleDetails{Direction: core.AddSecurityRuleDetailsDirectionIngress,
			Protocol: new("all"), Source: foreign.Id, SourceType: core.AddSecurityRuleDetailsSourceTypeNetworkSecurityGroup},
			"InvalidParameter"},
		{"missing source", core.AddSecurityRuleDetails{Direction: core.AddSecurityRuleDetailsDirectionIngress,
			Protocol: new("6")}, "MissingParameter"},
		{"bad protocol", core.AddSecurityRuleDetails{Direction: core.AddSecurityRuleDetailsDirectionEgress,
			Protocol: new("999"), Destination: new("0.0.0.0/0")}, "InvalidParameter"},
	}

	var ids []string

	for _, tt := range tests {
		r, err := add(tt.rule)
		if tt.wantCode != "" {
			assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode(), tt.name)

			continue
		}

		require.NoError(t, err, tt.name)
		require.Len(t, r.SecurityRules, 1)
		assert.True(t, *r.SecurityRules[0].IsValid)
		ids = append(ids, *r.SecurityRules[0].Id)
	}

	l, err := e.client.ListNetworkSecurityGroupSecurityRules(ctx, core.ListNetworkSecurityGroupSecurityRulesRequest{
		NetworkSecurityGroupId: a.Id})
	require.NoError(t, err)
	require.Len(t, l.Items, 3)
	assert.Equal(t, ids[2], *l.Items[0].Id, "newest first")

	l, err = e.client.ListNetworkSecurityGroupSecurityRules(ctx, core.ListNetworkSecurityGroupSecurityRulesRequest{
		NetworkSecurityGroupId: a.Id, Direction: core.ListNetworkSecurityGroupSecurityRulesDirectionIngress, Limit: new(1)})
	require.NoError(t, err)
	assert.Len(t, l.Items, 1)
	assert.NotNil(t, l.OpcNextPage)

	up, err := e.client.UpdateNetworkSecurityGroupSecurityRules(ctx, core.UpdateNetworkSecurityGroupSecurityRulesRequest{
		NetworkSecurityGroupId: a.Id,
		UpdateNetworkSecurityGroupSecurityRulesDetails: core.UpdateNetworkSecurityGroupSecurityRulesDetails{
			SecurityRules: []core.UpdateSecurityRuleDetails{{
				Id: &ids[0], Direction: core.UpdateSecurityRuleDetailsDirectionIngress, Protocol: new("17"),
				Source: new("10.0.0.0/8"), Description: new("udp now"),
			}},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "17", *up.SecurityRules[0].Protocol)
	assert.Nil(t, up.SecurityRules[0].TcpOptions)

	_, err = e.client.UpdateNetworkSecurityGroupSecurityRules(ctx, core.UpdateNetworkSecurityGroupSecurityRulesRequest{
		NetworkSecurityGroupId: a.Id,
		UpdateNetworkSecurityGroupSecurityRulesDetails: core.UpdateNetworkSecurityGroupSecurityRulesDetails{
			SecurityRules: []core.UpdateSecurityRuleDetails{{Id: new("nope"),
				Direction: core.UpdateSecurityRuleDetailsDirectionIngress, Protocol: new("6"), Source: new("0.0.0.0/0")}},
		}})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.DeleteNetworkSecurityGroup(ctx, core.DeleteNetworkSecurityGroupRequest{NetworkSecurityGroupId: b.Id})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "b is a rule source of a")

	_, err = e.client.RemoveNetworkSecurityGroupSecurityRules(ctx, core.RemoveNetworkSecurityGroupSecurityRulesRequest{
		NetworkSecurityGroupId: a.Id,
		RemoveNetworkSecurityGroupSecurityRulesDetails: core.RemoveNetworkSecurityGroupSecurityRulesDetails{
			SecurityRuleIds: []string{ids[1], "missing"}},
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.RemoveNetworkSecurityGroupSecurityRules(ctx, core.RemoveNetworkSecurityGroupSecurityRulesRequest{
		NetworkSecurityGroupId: a.Id,
		RemoveNetworkSecurityGroupSecurityRulesDetails: core.RemoveNetworkSecurityGroupSecurityRulesDetails{
			SecurityRuleIds: []string{ids[1]}},
	})
	require.NoError(t, err)

	l, err = e.client.ListNetworkSecurityGroupSecurityRules(ctx, core.ListNetworkSecurityGroupSecurityRulesRequest{
		NetworkSecurityGroupId: a.Id})
	require.NoError(t, err)
	assert.Len(t, l.Items, 2)

	_, err = e.client.DeleteNetworkSecurityGroup(ctx, core.DeleteNetworkSecurityGroupRequest{NetworkSecurityGroupId: b.Id})
	require.NoError(t, err)

	vn, err := e.client.ListNetworkSecurityGroupVnics(ctx, core.ListNetworkSecurityGroupVnicsRequest{NetworkSecurityGroupId: a.Id})
	require.NoError(t, err)
	assert.Empty(t, vn.Items)

	_, err = e.client.ListNetworkSecurityGroupVnics(ctx, core.ListNetworkSecurityGroupVnicsRequest{
		NetworkSecurityGroupId: new("ocid1.networksecuritygroup.oc1.iad.nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
