package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubnetCreate(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "sn")
	other := e.mkVcn(t, "10.9.0.0/16", "other")
	e.mkSubnet(t, v, "10.0.1.0/24")

	_, err := e.client.CreateSubnet(ctx, core.CreateSubnetRequest{CreateSubnetDetails: core.CreateSubnetDetails{
		CompartmentId: new(testComp), VcnId: v.Id, CidrBlock: new("10.0.20.0/24"), DnsLabel: new("web"),
	}})
	require.NoError(t, err)

	otherSL, err := e.client.CreateSecurityList(ctx, core.CreateSecurityListRequest{
		CompartmentId: new(testComp), VcnId: other.Id,
		IngressSecurityRules: []core.IngressSecurityRule{}, EgressSecurityRules: []core.EgressSecurityRule{},
	})
	require.NoError(t, err)

	tests := []struct {
		name     string
		wantCode string
		d        core.CreateSubnetDetails
		status   int
	}{
		{"ok", "", core.CreateSubnetDetails{CidrBlock: new("10.0.2.0/24"), DnsLabel: new("app")}, 0},
		{"ad specific", "", core.CreateSubnetDetails{CidrBlock: new("10.0.3.0/24"),
			AvailabilityDomain: new("Abcd:US-ASHBURN-AD-1")}, 0},
		{"private", "", core.CreateSubnetDetails{CidrBlock: new("10.0.4.0/24"), ProhibitPublicIpOnVnic: new(true)}, 0},
		{"outside vcn", "InvalidParameter", core.CreateSubnetDetails{CidrBlock: new("192.168.0.0/24")}, 400},
		{"overlap", "InvalidParameter", core.CreateSubnetDetails{CidrBlock: new("10.0.1.128/25")}, 400},
		{"missing cidr", "MissingParameter", core.CreateSubnetDetails{}, 400},
		{"too small", "InvalidParameter", core.CreateSubnetDetails{CidrBlock: new("10.0.5.0/31")}, 400},
		{
			"bad ad",
			"InvalidParameter",
			core.CreateSubnetDetails{CidrBlock: new("10.0.6.0/24"), AvailabilityDomain: new("AD-1")},
			400,
		},
		{
			"dup dns label",
			"Conflict",
			core.CreateSubnetDetails{CidrBlock: new("10.0.7.0/24"), DnsLabel: new("WEB")},
			409,
		},
		{"other vcn security list", "InvalidParameter", core.CreateSubnetDetails{CidrBlock: new("10.0.8.0/24"),
			SecurityListIds: []string{*otherSL.Id}}, 400},
		{"unknown security list", "NotAuthorizedOrNotFound", core.CreateSubnetDetails{CidrBlock: new("10.0.8.0/24"),
			SecurityListIds: []string{"ocid1.securitylist.oc1.iad.nope"}}, 404},
		{"private but ingress", "InvalidParameter", core.CreateSubnetDetails{CidrBlock: new("10.0.9.0/24"),
			ProhibitPublicIpOnVnic: new(true), ProhibitInternetIngress: new(false)}, 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.d.CompartmentId, tt.d.VcnId = new(testComp), v.Id
			r, cerr := e.client.CreateSubnet(ctx, core.CreateSubnetRequest{CreateSubnetDetails: tt.d})

			if tt.wantCode == "" {
				require.NoError(t, cerr)
				assert.Equal(t, *v.DefaultRouteTableId, *r.RouteTableId)
				assert.Equal(t, []string{*v.DefaultSecurityListId}, r.SecurityListIds)
				assert.Equal(t, *v.DefaultDhcpOptionsId, *r.DhcpOptionsId)
				assert.NotEmpty(t, *r.VirtualRouterMac)

				return
			}

			se := svcErr(t, cerr)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.wantCode, se.GetCode())
		})
	}
}

func TestSubnetDetails(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "det")

	r, err := e.client.CreateSubnet(ctx, core.CreateSubnetRequest{
		CompartmentId: new(testComp), VcnId: v.Id, CidrBlock: new("10.0.1.0/24"), DnsLabel: new("app"),
		ProhibitPublicIpOnVnic: new(true)})
	require.NoError(t, err)

	s := r.Subnet
	assert.Equal(t, "app.labdet.oraclevcn.com", *s.SubnetDomainName)
	assert.Equal(t, "10.0.1.1", *s.VirtualRouterIp)
	assert.True(t, *s.ProhibitPublicIpOnVnic)
	assert.True(t, *s.ProhibitInternetIngress)
	assert.Nil(t, s.AvailabilityDomain)
	assert.Equal(t, []string{"10.0.1.0/24"}, s.Ipv4CidrBlocks)

	nolabel, err := e.client.CreateVcn(ctx, core.CreateVcnRequest{
		CompartmentId: new(testComp), CidrBlocks: []string{"10.5.0.0/16"}})
	require.NoError(t, err)

	_, err = e.client.CreateSubnet(ctx, core.CreateSubnetRequest{
		CompartmentId: new(testComp), VcnId: nolabel.Id, CidrBlock: new("10.5.1.0/24"), DnsLabel: new("x")})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode())
}

func TestSubnetIPv6AndUpdate(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	vr, err := e.client.CreateVcn(ctx, core.CreateVcnRequest{
		CompartmentId: new(testComp), CidrBlocks: []string{"10.0.0.0/16"}, IsIpv6Enabled: new(true)})
	require.NoError(t, err)

	v6 := vr.Ipv6CidrBlocks[0]
	prefix := v6[:len(v6)-len("00::/56")]
	good := prefix + "01::/64"

	tests := []struct {
		name   string
		cidr   string
		v4     string
		wantOK bool
	}{
		{"inside prefix", good, "10.0.1.0/24", true},
		{"reused", good, "10.0.2.0/24", false},
		{"wrong size", prefix + "02::/65", "10.0.3.0/24", false},
		{"outside prefix", "2001:db8::/64", "10.0.4.0/24", false},
	}

	for _, tt := range tests {
		r, cerr := e.client.CreateSubnet(ctx, core.CreateSubnetRequest{
			CompartmentId: new(testComp), VcnId: vr.Id, CidrBlock: new(tt.v4), Ipv6CidrBlock: new(tt.cidr)})
		if tt.wantOK {
			require.NoError(t, cerr, tt.name)
			assert.Equal(t, tt.cidr, *r.Ipv6CidrBlock, tt.name)
		} else {
			assert.Equal(t, "InvalidParameter", svcErr(t, cerr).GetCode(), tt.name)
		}
	}

	s := e.mkSubnet(t, vr.Vcn, "10.0.9.0/24")
	rt, err := e.client.CreateRouteTable(
		ctx,
		core.CreateRouteTableRequest{
			CompartmentId: new(testComp), VcnId: vr.Id, RouteRules: []core.RouteRule{}},
	)
	require.NoError(t, err)

	u, err := e.client.UpdateSubnet(
		ctx,
		core.UpdateSubnetRequest{SubnetId: s.Id,
			RouteTableId: rt.Id, DisplayName: new("renamed"), FreeformTags: map[string]string{"k": "v"}},
	)
	require.NoError(t, err)
	assert.Equal(t, *rt.Id, *u.RouteTableId)
	assert.Equal(t, "renamed", *u.DisplayName)
	assert.Equal(t, "v", u.FreeformTags["k"])

	_, err = e.client.UpdateSubnet(
		ctx,
		core.UpdateSubnetRequest{SubnetId: s.Id,
			CidrBlock: new("10.0.8.0/24")},
	)
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.UpdateSubnet(
		ctx,
		core.UpdateSubnetRequest{SubnetId: s.Id,
			SecurityListIds: []string{"a", "b", "c", "d", "e", "f"}},
	)
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "at most five security lists")
}
