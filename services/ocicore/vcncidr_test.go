package ocicore_test

import (
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/oracle/oci-go-sdk/v65/workrequests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) getVcn(t *testing.T, v core.Vcn) core.Vcn {
	t.Helper()

	r, err := e.client.GetVcn(t.Context(), core.GetVcnRequest{VcnId: v.Id})
	require.NoError(t, err)

	return r.Vcn
}

func (e *env) workRequest(t *testing.T, id *string) workrequests.WorkRequest {
	t.Helper()
	require.NotNil(t, id)

	r, err := e.wr.GetWorkRequest(t.Context(), workrequests.GetWorkRequestRequest{WorkRequestId: id})
	require.NoError(t, err)

	return r.WorkRequest
}

func TestAddVcnCidr(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "add")
	peer := e.mkVcn(t, "10.5.0.0/16", "peer")
	lpgA, lpgB := e.mkLpg(t, v), e.mkLpg(t, peer)
	e.settle()
	_, err := e.client.ConnectLocalPeeringGateways(ctx, core.ConnectLocalPeeringGatewaysRequest{
		LocalPeeringGatewayId: lpgA.Id,
		PeerId:                lpgB.Id})
	require.NoError(t, err)

	tests := []struct {
		name     string
		cidr     string
		ifMatch  *string
		wantCode string
	}{
		{"ok", "10.1.0.0/16", nil, ""},
		{"overlap", "10.0.128.0/17", nil, "InvalidParameter"},
		{"overlaps peer", "10.5.0.0/24", nil, "InvalidParameter"},
		{"reserved", "169.254.0.0/16", nil, "InvalidParameter"},
		{"too small", "10.2.0.0/31", nil, "InvalidParameter"},
		{"not a network address", "10.2.0.1/24", nil, "InvalidParameter"},
		{"bad etag", "10.3.0.0/16", new("stale"), "NoEtagMatch"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ne := newEnv(t, false)
			nv := ne.mkVcn(t, "10.0.0.0/16", "case")
			np := ne.mkVcn(t, "10.5.0.0/16", "casepeer")
			a, b := ne.mkLpg(t, nv), ne.mkLpg(t, np)
			_, cerr := ne.client.ConnectLocalPeeringGateways(ctx, core.ConnectLocalPeeringGatewaysRequest{
				LocalPeeringGatewayId: a.Id,
				PeerId:                b.Id})
			require.NoError(t, cerr)
			ne.settle()

			r, aerr := ne.client.AddVcnCidr(ctx, core.AddVcnCidrRequest{VcnId: nv.Id, IfMatch: tt.ifMatch,
				CidrBlock: new(tt.cidr)})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, svcErr(t, aerr).GetCode())

				return
			}

			require.NoError(t, aerr)
			assert.NotEmpty(t, *r.OpcWorkRequestId)
		})
	}

	r, err := e.client.AddVcnCidr(ctx, core.AddVcnCidrRequest{VcnId: v.Id,
		CidrBlock: new("10.1.0.0/16")})
	require.NoError(t, err)

	got := e.getVcn(t, v)
	assert.Equal(t, core.VcnLifecycleStateUpdating, got.LifecycleState)
	assert.Equal(t, []string{"10.0.0.0/16", "10.1.0.0/16"}, got.CidrBlocks)

	wr := e.workRequest(t, r.OpcWorkRequestId)
	assert.Equal(t, "ADD_VCN_CIDR", *wr.OperationType)
	assert.Equal(t, workrequests.WorkRequestStatusAccepted, wr.Status)
	require.Len(t, wr.Resources, 1)
	assert.Equal(t, *v.Id, *wr.Resources[0].Identifier)

	_, err = e.client.CreateSubnet(ctx, core.CreateSubnetRequest{CompartmentId: new(testComp), VcnId: v.Id,
		CidrBlock: new("10.1.1.0/24")})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "no subnets while updating")

	_, err = e.client.CreateRouteTable(ctx, core.CreateRouteTableRequest{CompartmentId: new(testComp), VcnId: v.Id,
		RouteRules: []core.RouteRule{}})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "no route tables while updating")

	_, err = e.client.CreateLocalPeeringGateway(ctx, core.CreateLocalPeeringGatewayRequest{
		CompartmentId: new(testComp), VcnId: v.Id})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "no LPGs while updating")

	_, err = e.client.AddVcnCidr(ctx, core.AddVcnCidrRequest{VcnId: v.Id,
		CidrBlock: new("10.2.0.0/16")})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode(), "one change at a time")

	_, err = e.client.UpdateSubnet(ctx, core.UpdateSubnetRequest{SubnetId: e.mkSubnetNow(t, peer).Id,
		DisplayName: new("ok")})
	require.NoError(t, err, "other VCNs are unaffected")

	e.settle()

	got = e.getVcn(t, v)
	assert.Equal(t, core.VcnLifecycleStateAvailable, got.LifecycleState)

	wr = e.workRequest(t, r.OpcWorkRequestId)
	assert.Equal(t, workrequests.WorkRequestStatusSucceeded, wr.Status)
	assert.InDelta(t, 100, *wr.PercentComplete, 0.1)
	assert.NotNil(t, wr.TimeFinished)

	s := e.mkSubnet(t, got, "10.1.1.0/24")
	assert.Equal(t, "10.1.1.0/24", *s.CidrBlock)

	pg, err := e.client.GetLocalPeeringGateway(ctx, core.GetLocalPeeringGatewayRequest{LocalPeeringGatewayId: lpgB.Id})
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.0/16", "10.1.0.0/16"}, pg.PeerAdvertisedCidrDetails)

	_, err = e.client.AddVcnCidr(ctx, core.AddVcnCidrRequest{VcnId: peer.Id,
		CidrBlock: new("10.1.5.0/24")})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "the new block overlaps the peer's view")

	_, err = e.client.AddVcnCidr(ctx, core.AddVcnCidrRequest{VcnId: new("ocid1.vcn.oc1.iad.nope"),
		CidrBlock: new("10.9.0.0/16")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func (e *env) mkSubnetNow(t *testing.T, v core.Vcn) core.Subnet {
	t.Helper()

	return e.mkSubnet(t, v, "10.5.7.0/24")
}

func TestAddVcnCidrLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "limit")
	e.settle()

	for i := 1; i < 16; i++ {
		_, err := e.client.AddVcnCidr(ctx, core.AddVcnCidrRequest{VcnId: v.Id,
			CidrBlock: new("10." + strconv.Itoa(i) + ".0.0/16")})
		require.NoError(t, err)
		e.settle()
	}

	_, err := e.client.AddVcnCidr(ctx, core.AddVcnCidrRequest{VcnId: v.Id,
		CidrBlock: new("10.99.0.0/16")})
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())
}

func TestModifyVcnCidr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		orig     string
		updated  string
		wantCode string
		route    string
		subnet   string
		unknown  bool
	}{
		{name: "expand", orig: "10.0.0.0/24", updated: "10.0.0.0/23", subnet: "10.0.0.0/25"},
		{name: "shrink around subnet", orig: "10.0.0.0/16", updated: "10.0.0.0/20", subnet: "10.0.1.0/24"},
		{name: "subnet falls outside", orig: "10.0.0.0/16", updated: "10.0.0.0/20", subnet: "10.0.200.0/24",
			wantCode: "InvalidParameter"},
		{name: "route destination falls outside", orig: "10.0.0.0/16", updated: "10.0.0.0/20", route: "10.0.200.0/24",
			wantCode: "InvalidParameter"},
		{name: "route destination kept", orig: "10.0.0.0/16", updated: "10.0.0.0/17", route: "10.0.1.0/24"},
		{name: "same block", orig: "10.0.0.0/16", updated: "10.0.0.0/16", wantCode: "InvalidParameter"},
		{
			name:     "unknown original",
			orig:     "10.0.0.0/16",
			updated:  "10.0.0.0/20",
			wantCode: "InvalidParameter",
			unknown:  true,
		},
		{name: "overlaps other block", orig: "10.0.0.0/16", updated: "10.100.0.0/20", wantCode: "InvalidParameter"},
		{name: "too small", orig: "10.0.0.0/16", updated: "10.0.0.0/31", wantCode: "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			ctx := t.Context()
			v := e.mkVcn(t, tt.orig, "mod")
			e.settle()

			_, err := e.client.AddVcnCidr(ctx, core.AddVcnCidrRequest{VcnId: v.Id,
				CidrBlock: new("10.100.0.0/16")})
			require.NoError(t, err)
			e.settle()

			vcn := e.getVcn(t, v)

			if tt.subnet != "" {
				e.mkSubnet(t, vcn, tt.subnet)
			}

			if tt.route != "" {
				igw, ierr := e.client.CreateInternetGateway(ctx, core.CreateInternetGatewayRequest{
					CompartmentId: new(testComp), VcnId: v.Id, IsEnabled: new(true)})
				require.NoError(t, ierr)

				_, ierr = e.client.CreateRouteTable(ctx, core.CreateRouteTableRequest{CompartmentId: new(testComp),
					VcnId: v.Id, RouteRules: []core.RouteRule{{Destination: new(tt.route), NetworkEntityId: igw.Id}}})
				require.NoError(t, ierr)
			}

			orig := tt.orig
			if tt.unknown {
				orig = "10.7.0.0/16"
			}

			r, merr := e.client.ModifyVcnCidr(ctx, core.ModifyVcnCidrRequest{VcnId: v.Id,
				OriginalCidrBlock: new(orig), NewCidrBlock: new(tt.updated)})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, svcErr(t, merr).GetCode())

				return
			}

			require.NoError(t, merr)
			assert.Equal(t, "MODIFY_VCN_CIDR", *e.workRequest(t, r.OpcWorkRequestId).OperationType)
			assert.Contains(t, e.getVcn(t, v).CidrBlocks, tt.updated)
			assert.NotContains(t, e.getVcn(t, v).CidrBlocks, tt.orig)
		})
	}
}

func TestRemoveVcnCidr(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "rm")
	e.settle()

	_, err := e.client.AddVcnCidr(ctx, core.AddVcnCidrRequest{VcnId: v.Id,
		CidrBlock: new("10.1.0.0/16")})
	require.NoError(t, err)
	e.settle()

	vcn := e.getVcn(t, v)
	e.mkSubnet(t, vcn, "10.1.5.0/24")

	remove := func(cidr string) error {
		_, rerr := e.client.RemoveVcnCidr(ctx, core.RemoveVcnCidrRequest{VcnId: v.Id,
			CidrBlock: new(cidr)})

		return rerr
	}

	assert.Equal(t, "Conflict", svcErr(t, remove("10.1.0.0/16")).GetCode(), "a subnet uses the block")
	assert.Equal(t, "InvalidParameter", svcErr(t, remove("10.9.0.0/16")).GetCode(), "unknown block")

	require.NoError(t, remove("10.0.0.0/16"))
	e.settle()

	got := e.getVcn(t, v)
	assert.Equal(t, []string{"10.1.0.0/16"}, got.CidrBlocks)
	assert.Equal(t, "10.1.0.0/16", *got.CidrBlock)

	assert.Equal(t, "InvalidParameter", svcErr(t, remove("10.1.0.0/16")).GetCode(), "the last block stays")
}

func TestVcnIpv6Cidr(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "v6")
	e.settle()

	add := func(d core.AddVcnIpv6CidrDetails) (core.AddIpv6VcnCidrResponse, error) {
		return e.client.AddIpv6VcnCidr(ctx, core.AddIpv6VcnCidrRequest{VcnId: v.Id, AddVcnIpv6CidrDetails: d})
	}

	off := core.AddVcnIpv6CidrDetails{IsOracleGuaAllocationEnabled: new(false)}
	_, err := add(off)
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())

	_, err = add(core.AddVcnIpv6CidrDetails{Ipv6PrivateCidrBlock: new("2001:db8::/64")})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "not a ULA prefix")

	_, err = add(core.AddVcnIpv6CidrDetails{Byoipv6CidrDetail: &core.Byoipv6CidrDetails{}})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "no BYOIP")

	r, err := add(core.AddVcnIpv6CidrDetails{})
	require.NoError(t, err)
	assert.Equal(t, "ADD_VCN_IPV6_CIDR", *e.workRequest(t, r.OpcWorkRequestId).OperationType)
	e.settle()

	got := e.getVcn(t, v)
	require.Len(t, got.Ipv6CidrBlocks, 1)
	assert.Contains(t, got.Ipv6CidrBlocks[0], "/56")

	sl, err := e.client.GetSecurityList(ctx, core.GetSecurityListRequest{SecurityListId: v.DefaultSecurityListId})
	require.NoError(t, err)

	var v6Rules int

	for _, r := range sl.IngressSecurityRules {
		if *r.Source == "::/0" {
			v6Rules++
		}
	}

	assert.Equal(t, 2, v6Rules, "the IPv6 default ingress rules are added")

	_, err = add(core.AddVcnIpv6CidrDetails{})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "one Oracle-allocated prefix")

	_, err = add(core.AddVcnIpv6CidrDetails{Ipv6PrivateCidrBlock: new("fd00:1::/64")})
	require.NoError(t, err)
	e.settle()

	_, err = add(core.AddVcnIpv6CidrDetails{Ipv6PrivateCidrBlock: new("fd00:1::/64")})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "duplicate ULA")

	got = e.getVcn(t, v)
	assert.Equal(t, []string{"fd00:1::/64"}, got.Ipv6PrivateCidrBlocks)

	sub, err := e.client.CreateSubnet(ctx, core.CreateSubnetRequest{CompartmentId: new(testComp), VcnId: v.Id,
		CidrBlock: new("10.0.1.0/24"), Ipv6CidrBlock: new(hexSubnet(got.Ipv6CidrBlocks[0]))})
	require.NoError(t, err)

	remove := func(c string) error {
		_, rerr := e.client.RemoveIpv6VcnCidr(ctx, core.RemoveIpv6VcnCidrRequest{VcnId: v.Id,
			Ipv6CidrBlock: new(c)})

		return rerr
	}

	assert.Equal(t, "Conflict", svcErr(t, remove(got.Ipv6CidrBlocks[0])).GetCode(), "a subnet uses the prefix")
	assert.Equal(t, "InvalidParameter", svcErr(t, remove("fd00:9::/64")).GetCode())

	require.NoError(t, remove("fd00:1::/64"))
	e.settle()
	assert.Empty(t, e.getVcn(t, v).Ipv6PrivateCidrBlocks)

	assert.Equal(t, sub.Ipv6CidrBlocks[0], hexSubnet(got.Ipv6CidrBlocks[0]))
}

// hexSubnet returns the first /64 of a /56.
func hexSubnet(p56 string) string { return strings.TrimSuffix(p56, "::/56") + "::/64" }
