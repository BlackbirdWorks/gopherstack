package ocicore_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

func TestVcnCreate(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	tests := []struct {
		name     string
		details  core.CreateVcnDetails
		wantCode string
		status   int
	}{
		{"cidrBlocks", core.CreateVcnDetails{CidrBlocks: []string{"10.0.0.0/16"}}, "", 0},
		{"deprecated cidrBlock", core.CreateVcnDetails{CidrBlock: new("10.1.0.0/24")}, "", 0},
		{"two blocks", core.CreateVcnDetails{CidrBlocks: []string{"10.2.0.0/16", "10.3.0.0/16"}}, "", 0},
		{"missing cidr", core.CreateVcnDetails{}, "MissingParameter", http.StatusBadRequest},
		{"too large", core.CreateVcnDetails{CidrBlocks: []string{"10.0.0.0/15"}}, "InvalidParameter", 400},
		{"too small", core.CreateVcnDetails{CidrBlocks: []string{"10.0.0.0/31"}}, "InvalidParameter", 400},
		{"host bits", core.CreateVcnDetails{CidrBlocks: []string{"10.0.1.0/16"}}, "InvalidParameter", 400},
		{"ipv6 cidr", core.CreateVcnDetails{CidrBlocks: []string{"fd00::/48"}}, "InvalidParameter", 400},
		{"link local", core.CreateVcnDetails{CidrBlocks: []string{"169.254.0.0/16"}}, "InvalidParameter", 400},
		{
			"overlap",
			core.CreateVcnDetails{CidrBlocks: []string{"10.0.0.0/16", "10.0.128.0/17"}},
			"InvalidParameter",
			400,
		},
		{
			"bad label",
			core.CreateVcnDetails{CidrBlocks: []string{"10.4.0.0/16"}, DnsLabel: new("1abc")},
			"InvalidParameter",
			400,
		},
		{"long label", core.CreateVcnDetails{CidrBlocks: []string{"10.4.0.0/16"}, DnsLabel: new("abcdefghijklmnop")},
			"InvalidParameter", 400},
		{"cidrBlock not in list", core.CreateVcnDetails{CidrBlock: new("10.5.0.0/16"),
			CidrBlocks: []string{"10.6.0.0/16"}}, "InvalidParameter", 400},
		{"bad ula", core.CreateVcnDetails{CidrBlocks: []string{"10.7.0.0/16"},
			Ipv6PrivateCidrBlocks: []string{"2001:db8::/64"}}, "InvalidParameter", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.details.CompartmentId = new(testComp)
			r, err := e.client.CreateVcn(t.Context(), core.CreateVcnRequest{CreateVcnDetails: tt.details})

			if tt.wantCode == "" {
				require.NoError(t, err)
				assert.Equal(t, core.VcnLifecycleStateProvisioning, r.LifecycleState)
				assert.NotEmpty(t, r.CidrBlocks)
				assert.Equal(t, r.CidrBlocks[0], *r.CidrBlock)
				assert.NotNil(t, r.Etag)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, tt.status, se.GetHTTPStatusCode())
			assert.Equal(t, tt.wantCode, se.GetCode())
		})
	}
}

func TestVcnDefaults(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	r, err := e.client.CreateVcn(ctx, core.CreateVcnRequest{
		CompartmentId: new(testComp), CidrBlocks: []string{"10.0.0.0/16"}, DisplayName: new("main"),
		DnsLabel: new("Main"), IsIpv6Enabled: new(true)})
	require.NoError(t, err)

	v := r.Vcn
	assert.Equal(t, "main.oraclevcn.com", *v.VcnDomainName)
	assert.Len(t, v.Ipv6CidrBlocks, 1)
	assert.Contains(t, v.Ipv6CidrBlocks[0], "/56")

	rt, err := e.client.GetRouteTable(ctx, core.GetRouteTableRequest{RtId: v.DefaultRouteTableId})
	require.NoError(t, err)
	assert.Equal(t, "Default Route Table for main", *rt.DisplayName)
	assert.Empty(t, rt.RouteRules)

	sl, err := e.client.GetSecurityList(ctx, core.GetSecurityListRequest{SecurityListId: v.DefaultSecurityListId})
	require.NoError(t, err)
	assert.Equal(t, "Default Security List for main", *sl.DisplayName)
	assert.Len(t, sl.IngressSecurityRules, 5)
	assert.Len(t, sl.EgressSecurityRules, 2)
	assert.Equal(t, "all", *sl.EgressSecurityRules[0].Protocol)
	assert.Equal(t, 22, *sl.IngressSecurityRules[0].TcpOptions.DestinationPortRange.Min)

	dh, err := e.client.GetDhcpOptions(ctx, core.GetDhcpOptionsRequest{DhcpId: v.DefaultDhcpOptionsId})
	require.NoError(t, err)
	assert.Equal(t, core.DhcpOptionsDomainNameTypeVcnDomain, dh.DomainNameType)
	require.Len(t, dh.Options, 2)

	dns, ok := dh.Options[0].(core.DhcpDnsOption)
	require.True(t, ok)
	assert.Equal(t, core.DhcpDnsOptionServerTypeVcnlocalplusinternet, dns.ServerType)

	sd, ok := dh.Options[1].(core.DhcpSearchDomainOption)
	require.True(t, ok)
	assert.Equal(t, []string{"main.oraclevcn.com"}, sd.SearchDomainNames)

	for _, id := range []*string{v.DefaultRouteTableId, v.DefaultSecurityListId, v.DefaultDhcpOptionsId} {
		assert.Contains(t, *id, "ocid1.")
	}
}

func TestVcnLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "life")

	got, err := e.client.GetVcn(ctx, core.GetVcnRequest{VcnId: v.Id})
	require.NoError(t, err)
	assert.Equal(t, core.VcnLifecycleStateProvisioning, got.LifecycleState)

	e.clock.Advance(3 * time.Second)

	got, err = e.client.GetVcn(ctx, core.GetVcnRequest{VcnId: v.Id})
	require.NoError(t, err)
	assert.Equal(t, core.VcnLifecycleStateAvailable, got.LifecycleState)

	_, err = e.client.DeleteVcn(ctx, core.DeleteVcnRequest{VcnId: v.Id})
	require.NoError(t, err)

	got, err = e.client.GetVcn(ctx, core.GetVcnRequest{VcnId: v.Id})
	require.NoError(t, err)
	assert.Equal(t, core.VcnLifecycleStateTerminating, got.LifecycleState)

	e.clock.Advance(3 * time.Second)

	got, err = e.client.GetVcn(ctx, core.GetVcnRequest{VcnId: v.Id})
	require.NoError(t, err)
	assert.Equal(t, core.VcnLifecycleStateTerminated, got.LifecycleState)

	rt, err := e.client.GetRouteTable(ctx, core.GetRouteTableRequest{RtId: v.DefaultRouteTableId})
	require.NoError(t, err)
	assert.Equal(t, core.RouteTableLifecycleStateTerminated, rt.LifecycleState)

	_, err = e.client.DeleteVcn(ctx, core.DeleteVcnRequest{VcnId: v.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.UpdateVcn(ctx, core.UpdateVcnRequest{VcnId: v.Id,
		DisplayName: new("x")})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

	e.clock.Advance(2 * time.Hour)

	_, err = e.client.GetVcn(ctx, core.GetVcnRequest{VcnId: v.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestEtagIfMatch(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "etag")

	tests := []struct {
		name    string
		ifMatch string
		status  int
	}{
		{"stale", "nope", http.StatusPreconditionFailed},
		{"current", *v.Id, 0},
	}

	got, err := e.client.GetVcn(ctx, core.GetVcnRequest{VcnId: v.Id})
	require.NoError(t, err)
	require.NotNil(t, got.Etag)
	tests[1].ifMatch = *got.Etag

	for _, tt := range tests {
		_, cerr := e.client.UpdateVcn(ctx, core.UpdateVcnRequest{
			VcnId: v.Id, IfMatch: &tt.ifMatch, DisplayName: new("renamed"),
		})
		if tt.status == 0 {
			require.NoError(t, cerr, tt.name)

			continue
		}

		se := svcErr(t, cerr)
		assert.Equal(t, tt.status, se.GetHTTPStatusCode(), tt.name)
		assert.Equal(t, "NoEtagMatch", se.GetCode(), tt.name)
	}

	after, err := e.client.GetVcn(ctx, core.GetVcnRequest{VcnId: v.Id})
	require.NoError(t, err)
	assert.Equal(t, "renamed", *after.DisplayName)
	assert.NotEqual(t, *got.Etag, *after.Etag)

	_, err = e.client.DeleteVcn(ctx, core.DeleteVcnRequest{VcnId: v.Id, IfMatch: new("stale")})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())
}

func TestVcnListing(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	for _, n := range []string{"c", "a", "b"} {
		e.clock.Advance(time.Second)
		e.mkVcn(t, "10.0.0.0/16", n)
	}

	names := func(r core.ListVcnsResponse) []string {
		out := make([]string, 0, len(r.Items))
		for _, v := range r.Items {
			out = append(out, *v.DisplayName)
		}

		return out
	}

	r, err := e.client.ListVcns(ctx, core.ListVcnsRequest{CompartmentId: new(testComp)})
	require.NoError(t, err)
	assert.Equal(t, []string{"b", "a", "c"}, names(r), "default is newest first")

	r, err = e.client.ListVcns(ctx, core.ListVcnsRequest{CompartmentId: new(testComp),
		SortBy: core.ListVcnsSortByDisplayname})
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c"}, names(r))

	r, err = e.client.ListVcns(ctx, core.ListVcnsRequest{CompartmentId: new(testComp), Limit: new(2),
		SortBy: core.ListVcnsSortByDisplayname})
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, names(r))
	require.NotNil(t, r.OpcNextPage)

	r, err = e.client.ListVcns(ctx, core.ListVcnsRequest{CompartmentId: new(testComp), Limit: new(2),
		SortBy: core.ListVcnsSortByDisplayname, Page: r.OpcNextPage})
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, names(r))
	assert.Nil(t, r.OpcNextPage)

	r, err = e.client.ListVcns(ctx, core.ListVcnsRequest{CompartmentId: new(testComp), DisplayName: new("a")})
	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, names(r))

	r, err = e.client.ListVcns(ctx, core.ListVcnsRequest{CompartmentId: new(testComp),
		LifecycleState: core.VcnLifecycleStateAvailable})
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, names(r), "only the oldest has finished provisioning")

	r, err = e.client.ListVcns(ctx, core.ListVcnsRequest{CompartmentId: new("ocid1.compartment.oc1..other")})
	require.NoError(t, err)
	assert.Empty(t, names(r))

	resp, err := testclient.New(t).Get(e.base + "/20160918/vcns")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestChangeCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "mv")
	dest := "ocid1.compartment.oc1..dest"

	_, err := e.client.ChangeVcnCompartment(ctx, core.ChangeVcnCompartmentRequest{
		VcnId: v.Id, CompartmentId: &dest,
	})
	require.NoError(t, err)

	got, err := e.client.GetVcn(ctx, core.GetVcnRequest{VcnId: v.Id})
	require.NoError(t, err)
	assert.Equal(t, dest, *got.CompartmentId)

	_, err = e.client.ChangeVcnCompartment(ctx, core.ChangeVcnCompartmentRequest{
		VcnId: new(
			"ocid1.vcn.oc1.iad.nope",
		),
		CompartmentId: &dest,
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestCompartmentChecker(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.backend.SetCompartmentChecker(func(id string) bool { return id == testComp })

	_, err := e.client.CreateVcn(t.Context(), core.CreateVcnRequest{
		CompartmentId: new("ocid1.compartment.oc1..ghost"), CidrBlocks: []string{"10.0.0.0/16"}})
	se := svcErr(t, err)
	assert.Equal(t, http.StatusNotFound, se.GetHTTPStatusCode())
	assert.Equal(t, "NotAuthorizedOrNotFound", se.GetCode())

	e.mkVcn(t, "10.0.0.0/16", "ok")
}
