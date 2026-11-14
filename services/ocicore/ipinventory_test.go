package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) inventory(t *testing.T, d core.ListIpInventoryDetails) core.ListIpInventoryResponse {
	t.Helper()

	r, err := e.client.ListIpInventory(t.Context(), core.ListIpInventoryRequest{ListIpInventoryDetails: d})
	require.NoError(t, err)

	return r
}

func vcnNames(r core.ListIpInventoryResponse) []string {
	out := make([]string, 0, len(r.InventoryVcnCollection))
	for _, v := range r.InventoryVcnCollection {
		out = append(out, *v.VcnName)
	}

	return out
}

func TestSubnetCidrUtilization(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "util")
	s := e.mkSubnet(t, v, "10.0.1.0/29")
	e.settle()

	r, err := e.client.GetSubnetCidrUtilization(ctx, core.GetSubnetCidrUtilizationRequest{SubnetId: s.Id})
	require.NoError(t, err)
	require.Len(t, r.IpInventoryCidrUtilizationSummary, 1)
	assert.Equal(t, "10.0.1.0/29", *r.IpInventoryCidrUtilizationSummary[0].Cidr)
	assert.Equal(t, "Private_IPv4", *r.IpInventoryCidrUtilizationSummary[0].AddressType)
	assert.InDelta(t, 0, *r.IpInventoryCidrUtilizationSummary[0].Utilization, 0.001)
	assert.Equal(t, testComp, *r.CompartmentId)
	assert.Equal(t, 1, *r.OpcTotalItems)

	for range 2 {
		_, nerr := e.nic(t, s, "")
		require.NoError(t, nerr)
	}

	r, err = e.client.GetSubnetCidrUtilization(ctx, core.GetSubnetCidrUtilizationRequest{SubnetId: s.Id})
	require.NoError(t, err)
	assert.InDelta(t, 40, *r.IpInventoryCidrUtilizationSummary[0].Utilization, 0.001, "2 of 5 usable addresses")

	_, err = e.client.AddIpv4SubnetCidr(
		ctx,
		core.AddIpv4SubnetCidrRequest{SubnetId: s.Id, Ipv4CidrBlock: new("10.0.2.0/28")},
	)
	require.NoError(t, err)

	r, err = e.client.GetSubnetCidrUtilization(ctx, core.GetSubnetCidrUtilizationRequest{SubnetId: s.Id})
	require.NoError(t, err)
	require.Len(t, r.IpInventoryCidrUtilizationSummary, 2)
	assert.InDelta(t, 0, *r.IpInventoryCidrUtilizationSummary[1].Utilization, 0.001)

	_, err = e.client.GetSubnetCidrUtilization(ctx, core.GetSubnetCidrUtilizationRequest{
		SubnetId: new("ocid1.subnet.oc1.iad.nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestSubnetIpInventory(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.v6Fixture(t)

	inst, err := e.launch(t, f.subnet, func(d *core.LaunchInstanceDetails) {
		d.DisplayName = new("web")
		d.CreateVnicDetails.HostnameLabel = new("web1")
		d.CreateVnicDetails.AssignIpv6Ip = new(true)
	})
	require.NoError(t, err)
	e.settle()

	extra, err := e.nic(t, f.subnet, "")
	require.NoError(t, err)

	r, err := e.client.GetSubnetIpInventory(ctx, core.GetSubnetIpInventoryRequest{SubnetId: f.subnet.Id})
	require.NoError(t, err)
	assert.Equal(t, testComp, *r.CompartmentId)
	assert.NotNil(t, r.LastUpdatedTimestamp)

	byAddr := map[string]core.IpInventorySubnetResourceSummary{}
	for _, it := range r.IpInventorySubnetResourceSummary {
		byAddr[*it.IpAddress] = it
	}

	vnic := e.vnicOf(t, inst.Id)
	primary := byAddr[*vnic.PrivateIp]
	assert.Equal(t, "Private_IPv4", *primary.AddressType)
	assert.Equal(t, "10.0.1.0/24", *primary.ParentCidr)
	assert.Equal(t, "web", *primary.AssignedResourceName)
	assert.True(t, *primary.IsPrimary)
	assert.Equal(t, core.IpInventorySubnetResourceSummaryIpAddressLifetimeEphemeral, primary.IpAddressLifetime)
	assert.Equal(t, *vnic.PublicIp, *primary.AssociatedPublicIp)
	assert.Equal(t, core.IpInventorySubnetResourceSummaryPublicIpLifetimeEphemeral, primary.PublicIpLifetime)
	assert.Equal(t, core.IpInventorySubnetResourceSummaryAssociatedPublicIpPoolOracle, primary.AssociatedPublicIpPool)
	assert.Equal(t, "web1."+*f.subnet.SubnetDomainName, *primary.DnsHostName)

	secondary := byAddr[extra.IPAddress]
	assert.Equal(t, "Private_IPv4", *secondary.AddressType)
	assert.Nil(t, secondary.AssociatedPublicIp)

	v6 := byAddr[vnic.Ipv6Addresses[0]]
	assert.Equal(t, "Oracle_Allocated_GUA_IPv6", *v6.AddressType)
	assert.Equal(t, hexSubnet(f.vcn.Ipv6CidrBlocks[0]), *v6.ParentCidr)
	assert.Equal(t, "web", *v6.AssignedResourceName)

	util, err := e.client.GetSubnetCidrUtilization(ctx, core.GetSubnetCidrUtilizationRequest{SubnetId: f.subnet.Id})
	require.NoError(t, err)
	require.Len(t, util.IpInventoryCidrUtilizationSummary, 2)
	assert.Equal(t, "Oracle_Allocated_GUA_IPv6", *util.IpInventoryCidrUtilizationSummary[1].AddressType)

	_, err = e.client.GetSubnetIpInventory(
		ctx,
		core.GetSubnetIpInventoryRequest{SubnetId: new("ocid1.subnet.oc1.iad.nope")},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestListIpInventory(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	hot := e.mkVcn(t, "10.0.0.0/16", "hot")
	e.mkVcn(t, "10.0.0.0/20", "clash")
	e.mkVcn(t, "172.16.0.0/16", "calm")
	sn := e.mkSubnet(t, hot, "10.0.1.0/29")
	e.settle()

	for range 3 {
		_, err := e.nic(t, sn, "")
		require.NoError(t, err)
	}

	base := core.ListIpInventoryDetails{RegionList: []string{testRegion}, CompartmentList: []string{testComp}}

	r := e.inventory(t, base)
	assert.Equal(t, core.ListIpInventoryLifecycleStateDone, r.LifecycleState)
	assert.NotEmpty(t, *r.DataRequestId)
	assert.Equal(t, 3, *r.OpcTotalItems)
	assert.Equal(t, 3, *r.Count)
	assert.Equal(t, []string{"calm", "clash", "hot"}, vcnNames(r))

	var hotVcn core.InventoryVcnSummary

	for _, v := range r.InventoryVcnCollection {
		if *v.VcnName == "hot" {
			hotVcn = v
		}
	}

	assert.Equal(t, "VCN", string(hotVcn.ResourceType))
	assert.Equal(t, testRegion, *hotVcn.Region)
	assert.Equal(t, "labhot.oraclevcn.com", *hotVcn.DnsDomainName)
	require.Len(t, hotVcn.InventoryVcnCidrBlockCollection, 1)
	assert.InDelta(t, 0.01, *hotVcn.InventoryVcnCidrBlockCollection[0].Utilization, 0.005, "an /29 of a /16")
	require.Len(t, hotVcn.InventorySubnetcollection, 1)

	sub := hotVcn.InventorySubnetcollection[0]
	assert.Equal(t, "Subnet", string(sub.ResourceType))
	require.Len(t, sub.InventorySubnetCidrCollection, 1)
	assert.InDelta(t, 60, *sub.InventorySubnetCidrCollection[0].Utilization, 0.001, "3 of 5 usable")
	require.Len(t, sub.InventoryResourceSummary, 3)

	for _, res := range sub.InventoryResourceSummary {
		assert.Len(t, res.IpAddressCollection, 1)
		assert.NotEmpty(t, *res.MacAddress)
	}

	tests := []struct {
		name string
		mod  func(*core.ListIpInventoryDetails)
		want []string
	}{
		{
			"utilization filter",
			func(d *core.ListIpInventoryDetails) { d.Utilization = new(float32(0.005)) },
			[]string{"hot"},
		},
		{
			"overlapping only",
			func(d *core.ListIpInventoryDetails) { d.OverlappingVcnsOnly = new(true) },
			[]string{"clash", "hot"},
		},
		{"search keyword", func(d *core.ListIpInventoryDetails) { d.SearchKeyword = new("CALM") }, []string{"calm"}},
		{
			"search by address",
			func(d *core.ListIpInventoryDetails) { d.SearchKeyword = new("10.0.1.2") },
			[]string{"hot"},
		},
		{"sort descending", func(d *core.ListIpInventoryDetails) {
			d.SortBy = core.ListIpInventoryDetailsSortByDisplayname
			d.SortOrder = core.ListIpInventoryDetailsSortOrderDesc
		}, []string{"hot", "clash", "calm"}},
		{"sort by utilization", func(d *core.ListIpInventoryDetails) {
			d.SortBy = core.ListIpInventoryDetailsSortByUtilization
			d.SortOrder = core.ListIpInventoryDetailsSortOrderDesc
		}, []string{"hot", "clash", "calm"}},
		{"pagination", func(d *core.ListIpInventoryDetails) {
			d.PaginationOffset, d.PaginationLimit = new(1), new(1)
		}, []string{"clash"}},
		{"ipv6 only", func(d *core.ListIpInventoryDetails) {
			d.AddressTypeList = []core.AddressTypeEnum{core.AddressTypeUlaIPv6}
		}, []string{}},
		{"ipv4 only", func(d *core.ListIpInventoryDetails) {
			d.AddressTypeList = []core.AddressTypeEnum{core.AddressTypePrivateIPv4}
		}, []string{"calm", "clash", "hot"}},
		{
			"other region",
			func(d *core.ListIpInventoryDetails) { d.RegionList = []string{"eu-frankfurt-1"} },
			[]string{},
		},
		{
			"other compartment",
			func(d *core.ListIpInventoryDetails) { d.CompartmentList = []string{testComp + "x"} },
			[]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := base
			tt.mod(&d)
			assert.Equal(t, tt.want, vcnNames(e.inventory(t, d)))
		})
	}

	replay, err := e.client.GetResourceIpInventory(
		ctx,
		core.GetResourceIpInventoryRequest{DataRequestId: r.DataRequestId},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"calm", "clash", "hot"}, vcnNames(core.ListIpInventoryResponse{
		IpInventoryCollection: replay.IpInventoryCollection}))

	_, err = e.client.GetResourceIpInventory(ctx, core.GetResourceIpInventoryRequest{
		DataRequestId: new("ocid1.ipinventoryrequest.oc1.iad.nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestListIpInventoryRequestsPersist(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.mkVcn(t, "10.0.0.0/16", "keep")

	r := e.inventory(
		t,
		core.ListIpInventoryDetails{RegionList: []string{testRegion}, CompartmentList: []string{testComp}},
	)

	e2 := newEnv(t, false)
	require.NoError(t, e2.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	got, err := e2.client.GetResourceIpInventory(
		t.Context(),
		core.GetResourceIpInventoryRequest{DataRequestId: r.DataRequestId},
	)
	require.NoError(t, err)
	assert.Equal(t, 1, *got.Count)
}

func TestGetVcnOverlap(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	a := e.mkVcn(t, "10.0.0.0/16", "a")
	e.mkVcn(t, "10.0.128.0/17", "b")
	e.mkVcn(t, "192.168.0.0/16", "c")

	d := core.GetIpInventoryVcnOverlapDetails{RegionList: []string{testRegion}, CompartmentList: []string{testComp}}

	r, err := e.client.GetVcnOverlap(ctx, core.GetVcnOverlapRequest{VcnId: a.Id, GetVcnOverlapDetails: d})
	require.NoError(t, err)
	assert.Equal(t, 1, *r.OverlapCount)
	require.Len(t, r.IpInventoryVcnOverlapSummary, 1)

	o := r.IpInventoryVcnOverlapSummary[0]
	assert.Equal(t, "b", *o.OverlappingVcnName)
	assert.Equal(t, "10.0.128.0/17", *o.OverlappingCidr)
	assert.Equal(t, "10.0.0.0/16", *o.Cidr)
	assert.Equal(t, core.GetVcnOverlapLifecycleStateDone, r.LifecycleState)
	assert.NotEmpty(t, *r.DataRequestId)

	d.CompartmentList = []string{testComp + "x"}

	r, err = e.client.GetVcnOverlap(ctx, core.GetVcnOverlapRequest{VcnId: a.Id, GetVcnOverlapDetails: d})
	require.NoError(t, err)
	assert.Zero(t, *r.OverlapCount)

	_, err = e.client.GetVcnOverlap(
		ctx,
		core.GetVcnOverlapRequest{VcnId: new("ocid1.vcn.oc1.iad.nope"), GetVcnOverlapDetails: d},
	)
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}
