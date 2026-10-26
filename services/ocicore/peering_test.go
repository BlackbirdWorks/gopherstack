package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) mkLpg(t *testing.T, vcn core.Vcn) core.LocalPeeringGateway {
	t.Helper()

	r, err := e.client.CreateLocalPeeringGateway(t.Context(), core.CreateLocalPeeringGatewayRequest{
		CreateLocalPeeringGatewayDetails: core.CreateLocalPeeringGatewayDetails{CompartmentId: new(testComp), VcnId: vcn.Id},
	})
	require.NoError(t, err)

	return r.LocalPeeringGateway
}

func TestLocalPeeringGateway(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	a, b, c := e.mkVcn(t, "10.0.0.0/16", "la"), e.mkVcn(t, "10.1.0.0/16", "lb"), e.mkVcn(t, "10.0.0.0/24", "lc")
	la, la2, lb, lc := e.mkLpg(t, a), e.mkLpg(t, a), e.mkLpg(t, b), e.mkLpg(t, c)

	assert.Equal(t, core.LocalPeeringGatewayPeeringStatusNew, la.PeeringStatus)
	assert.False(t, *la.IsCrossTenancyPeering)

	connect := func(from, to *string) error {
		_, err := e.client.ConnectLocalPeeringGateways(ctx, core.ConnectLocalPeeringGatewaysRequest{
			LocalPeeringGatewayId: from, ConnectLocalPeeringGatewaysDetails: core.ConnectLocalPeeringGatewaysDetails{PeerId: to},
		})

		return err
	}

	tests := []struct {
		name     string
		from, to *string
		wantCode string
		status   int
	}{
		{"same vcn", la.Id, la2.Id, "InvalidParameter", http.StatusBadRequest},
		{"overlapping cidrs", la.Id, lc.Id, "InvalidParameter", http.StatusBadRequest},
		{"unknown peer", la.Id, new("ocid1.localpeeringgateway.oc1.iad.nope"), "NotAuthorizedOrNotFound", http.StatusNotFound},
		{"ok", la.Id, lb.Id, "", 0},
		{"already peered", la2.Id, lb.Id, "Conflict", http.StatusConflict},
	}

	for _, tt := range tests {
		err := connect(tt.from, tt.to)
		if tt.wantCode == "" {
			require.NoError(t, err, tt.name)

			continue
		}

		se := svcErr(t, err)
		assert.Equal(t, tt.wantCode, se.GetCode(), tt.name)
		assert.Equal(t, tt.status, se.GetHTTPStatusCode(), tt.name)
	}

	ga, err := e.client.GetLocalPeeringGateway(ctx, core.GetLocalPeeringGatewayRequest{LocalPeeringGatewayId: la.Id})
	require.NoError(t, err)
	assert.Equal(t, core.LocalPeeringGatewayPeeringStatusPeered, ga.PeeringStatus)
	assert.Equal(t, *lb.Id, *ga.PeerId)
	assert.Equal(t, "10.1.0.0/16", *ga.PeerAdvertisedCidr)

	gb, err := e.client.GetLocalPeeringGateway(ctx, core.GetLocalPeeringGatewayRequest{LocalPeeringGatewayId: lb.Id})
	require.NoError(t, err)
	assert.Equal(t, *la.Id, *gb.PeerId)

	_, err = e.client.DeleteLocalPeeringGateway(ctx, core.DeleteLocalPeeringGatewayRequest{LocalPeeringGatewayId: la.Id})
	require.NoError(t, err)

	gb, err = e.client.GetLocalPeeringGateway(ctx, core.GetLocalPeeringGatewayRequest{LocalPeeringGatewayId: lb.Id})
	require.NoError(t, err)
	assert.Equal(t, core.LocalPeeringGatewayPeeringStatusRevoked, gb.PeeringStatus)
}

func TestDhcpOptions(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "dh")

	custom := core.DhcpDnsOption{ServerType: core.DhcpDnsOptionServerTypeCustomdnsserver, CustomDnsServers: []string{"8.8.8.8"}}
	search := core.DhcpSearchDomainOption{SearchDomainNames: []string{"example.com"}}

	tests := []struct {
		name     string
		opts     []core.DhcpOption
		wantCode string
	}{
		{"custom dns and search", []core.DhcpOption{custom, search}, ""},
		{"vcn local", []core.DhcpOption{core.DhcpDnsOption{ServerType: core.DhcpDnsOptionServerTypeVcnlocal}}, ""},
		{"empty", []core.DhcpOption{}, ""},
		{"custom without servers", []core.DhcpOption{core.DhcpDnsOption{
			ServerType: core.DhcpDnsOptionServerTypeCustomdnsserver}}, "InvalidParameter"},
		{"bad address", []core.DhcpOption{core.DhcpDnsOption{
			ServerType: core.DhcpDnsOptionServerTypeCustomdnsserver, CustomDnsServers: []string{"dns.local"}}}, "InvalidParameter"},
		{"too many servers", []core.DhcpOption{core.DhcpDnsOption{
			ServerType: core.DhcpDnsOptionServerTypeCustomdnsserver, CustomDnsServers: []string{"1.1.1.1", "1.0.0.1", "8.8.8.8", "8.8.4.4"}}},
			"InvalidParameter"},
		{"duplicate type", []core.DhcpOption{search, search}, "InvalidParameter"},
		{"two search domains", []core.DhcpOption{core.DhcpSearchDomainOption{SearchDomainNames: []string{"a.com", "b.com"}}},
			"InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, err := e.client.CreateDhcpOptions(ctx, core.CreateDhcpOptionsRequest{CreateDhcpDetails: core.CreateDhcpDetails{
				CompartmentId: new(testComp), VcnId: v.Id, Options: tt.opts,
			}})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode())

				return
			}

			require.NoError(t, err)
			assert.Len(t, r.Options, len(tt.opts))

			u, err := e.client.UpdateDhcpOptions(ctx, core.UpdateDhcpOptionsRequest{DhcpId: r.Id,
				UpdateDhcpDetails: core.UpdateDhcpDetails{DisplayName: new("n")}})
			require.NoError(t, err)
			assert.Len(t, u.Options, len(tt.opts))
		})
	}
}
