package ocicore_test

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouteTableRules(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "rt")
	other := e.mkVcn(t, "10.8.0.0/16", "rtother")
	svcs := e.services(t)

	igw, err := e.client.CreateInternetGateway(ctx, core.CreateInternetGatewayRequest{
		CreateInternetGatewayDetails: core.CreateInternetGatewayDetails{CompartmentId: new(testComp), VcnId: v.Id, IsEnabled: new(true)}})
	require.NoError(t, err)
	nat, err := e.client.CreateNatGateway(ctx, core.CreateNatGatewayRequest{
		CreateNatGatewayDetails: core.CreateNatGatewayDetails{CompartmentId: new(testComp), VcnId: v.Id}})
	require.NoError(t, err)
	sgw, err := e.client.CreateServiceGateway(ctx, core.CreateServiceGatewayRequest{
		CreateServiceGatewayDetails: core.CreateServiceGatewayDetails{CompartmentId: new(testComp), VcnId: v.Id,
			Services: []core.ServiceIdRequestDetails{{ServiceId: svcs[1].Id}}}})
	require.NoError(t, err)
	foreign, err := e.client.CreateInternetGateway(ctx, core.CreateInternetGatewayRequest{
		CreateInternetGatewayDetails: core.CreateInternetGatewayDetails{CompartmentId: new(testComp), VcnId: other.Id, IsEnabled: new(true)}})
	require.NoError(t, err)

	cidr := core.RouteRuleDestinationTypeCidrBlock
	svc := core.RouteRuleDestinationTypeServiceCidrBlock

	tests := []struct {
		name     string
		rules    []core.RouteRule
		wantCode string
	}{
		{"internet", []core.RouteRule{{Destination: new("0.0.0.0/0"), DestinationType: cidr, NetworkEntityId: igw.Id}}, ""},
		{"nat", []core.RouteRule{{Destination: new("0.0.0.0/0"), NetworkEntityId: nat.Id, Description: new("out")}}, ""},
		{"deprecated cidrBlock", []core.RouteRule{{CidrBlock: new("10.1.0.0/16"), NetworkEntityId: igw.Id}}, ""},
		{"service gateway", []core.RouteRule{{Destination: new("oci-iad-objectstorage"), DestinationType: svc,
			NetworkEntityId: sgw.Id}}, ""},
		{"drg", []core.RouteRule{{Destination: new("172.16.0.0/12"), NetworkEntityId: new("ocid1.drg.oc1.iad.x")}}, ""},
		{"service not attached", []core.RouteRule{{Destination: new("all-iad-services-in-oracle-services-network"),
			DestinationType: svc, NetworkEntityId: sgw.Id}}, "InvalidParameter"},
		{"cidr to service gateway", []core.RouteRule{{Destination: new("10.1.0.0/16"), DestinationType: cidr,
			NetworkEntityId: sgw.Id}}, "InvalidParameter"},
		{"service type to igw", []core.RouteRule{{Destination: new("oci-iad-objectstorage"), DestinationType: svc,
			NetworkEntityId: igw.Id}}, "InvalidParameter"},
		{"other vcn gateway", []core.RouteRule{{Destination: new("0.0.0.0/0"), NetworkEntityId: foreign.Id}}, "InvalidParameter"},
		{"unknown gateway", []core.RouteRule{{Destination: new("0.0.0.0/0"),
			NetworkEntityId: new("ocid1.internetgateway.oc1.iad.nope")}}, "NotAuthorizedOrNotFound"},
		{"bad target kind", []core.RouteRule{{Destination: new("0.0.0.0/0"), NetworkEntityId: new("ocid1.vcn.oc1.iad.x")}},
			"InvalidParameter"},
		{"bad destination", []core.RouteRule{{Destination: new("nonsense"), NetworkEntityId: igw.Id}}, "InvalidParameter"},
		{"duplicate destination", []core.RouteRule{
			{Destination: new("0.0.0.0/0"), NetworkEntityId: igw.Id},
			{Destination: new("0.0.0.0/0"), NetworkEntityId: nat.Id},
		}, "InvalidParameter"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, err := e.client.CreateRouteTable(ctx, core.CreateRouteTableRequest{CreateRouteTableDetails: core.CreateRouteTableDetails{
				CompartmentId: new(testComp), VcnId: v.Id, RouteRules: tt.rules,
			}})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode())

				return
			}

			require.NoError(t, err)
			require.Len(t, r.RouteRules, 1)
			assert.Equal(t, core.RouteRuleRouteTypeStatic, r.RouteRules[0].RouteType)
			assert.NotEmpty(t, *r.RouteRules[0].Destination)
		})
	}
}

func TestDeleteInUse(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "use")

	igw, err := e.client.CreateInternetGateway(ctx, core.CreateInternetGatewayRequest{
		CreateInternetGatewayDetails: core.CreateInternetGatewayDetails{CompartmentId: new(testComp), VcnId: v.Id, IsEnabled: new(true)}})
	require.NoError(t, err)
	rt, err := e.client.CreateRouteTable(ctx, core.CreateRouteTableRequest{CreateRouteTableDetails: core.CreateRouteTableDetails{
		CompartmentId: new(testComp), VcnId: v.Id,
		RouteRules: []core.RouteRule{{Destination: new("0.0.0.0/0"), NetworkEntityId: igw.Id}},
	}})
	require.NoError(t, err)
	sl, err := e.client.CreateSecurityList(ctx, core.CreateSecurityListRequest{CreateSecurityListDetails: core.CreateSecurityListDetails{
		CompartmentId: new(testComp), VcnId: v.Id, IngressSecurityRules: []core.IngressSecurityRule{},
		EgressSecurityRules: []core.EgressSecurityRule{}}})
	require.NoError(t, err)
	dh, err := e.client.CreateDhcpOptions(ctx, core.CreateDhcpOptionsRequest{CreateDhcpDetails: core.CreateDhcpDetails{
		CompartmentId: new(testComp), VcnId: v.Id, Options: []core.DhcpOption{core.DhcpDnsOption{
			ServerType: core.DhcpDnsOptionServerTypeVcnlocal}}}})
	require.NoError(t, err)
	sn, err := e.client.CreateSubnet(ctx, core.CreateSubnetRequest{CreateSubnetDetails: core.CreateSubnetDetails{
		CompartmentId: new(testComp), VcnId: v.Id, CidrBlock: new("10.0.1.0/24"), RouteTableId: rt.Id,
		SecurityListIds: []string{*sl.Id}, DhcpOptionsId: dh.Id}})
	require.NoError(t, err)

	del := map[string]func() error{
		"vcn": func() error { _, e2 := e.client.DeleteVcn(ctx, core.DeleteVcnRequest{VcnId: v.Id}); return e2 },
		"igw": func() error {
			_, e2 := e.client.DeleteInternetGateway(ctx, core.DeleteInternetGatewayRequest{IgId: igw.Id})
			return e2
		},
		"rt": func() error {
			_, e2 := e.client.DeleteRouteTable(ctx, core.DeleteRouteTableRequest{RtId: rt.Id})
			return e2
		},
		"sl": func() error {
			_, e2 := e.client.DeleteSecurityList(ctx, core.DeleteSecurityListRequest{SecurityListId: sl.Id})
			return e2
		},
		"dhcp": func() error {
			_, e2 := e.client.DeleteDhcpOptions(ctx, core.DeleteDhcpOptionsRequest{DhcpId: dh.Id})
			return e2
		},
		"defaultRT": func() error {
			_, e2 := e.client.DeleteRouteTable(ctx, core.DeleteRouteTableRequest{RtId: v.DefaultRouteTableId})
			return e2
		},
		"defaultSL": func() error {
			_, e2 := e.client.DeleteSecurityList(ctx, core.DeleteSecurityListRequest{SecurityListId: v.DefaultSecurityListId})
			return e2
		},
		"defaultDH": func() error {
			_, e2 := e.client.DeleteDhcpOptions(ctx, core.DeleteDhcpOptionsRequest{DhcpId: v.DefaultDhcpOptionsId})
			return e2
		},
	}

	for name, f := range del {
		se := svcErr(t, f())
		assert.Equal(t, 409, se.GetHTTPStatusCode(), name)
		assert.Equal(t, "Conflict", se.GetCode(), name)
	}

	steps := []struct {
		name string
		f    func() error
	}{
		{"subnet", func() error {
			_, e2 := e.client.DeleteSubnet(ctx, core.DeleteSubnetRequest{SubnetId: sn.Id})
			return e2
		}},
		{"sl", del["sl"]}, {"dhcp", del["dhcp"]}, {"rt", del["rt"]},
		{"igw", del["igw"]}, {"vcn", del["vcn"]},
	}

	e.clock.Advance(3e9)

	for _, s := range steps {
		if s.name == "sl" || s.name == "dhcp" || s.name == "rt" || s.name == "igw" {
			e.clock.Advance(3e9)
		}

		require.NoError(t, s.f(), s.name)
	}
}
