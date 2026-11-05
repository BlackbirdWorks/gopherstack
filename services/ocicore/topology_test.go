package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
)

type relKey struct {
	typ string
	id1 string
	id2 string
}

func relsOf(t *testing.T, rels []core.TopologyEntityRelationship) map[relKey]core.TopologyEntityRelationship {
	t.Helper()

	out := map[relKey]core.TopologyEntityRelationship{}

	for _, r := range rels {
		switch v := r.(type) {
		case core.TopologyContainsEntityRelationship:
			out[relKey{"CONTAINS", *v.Id1, *v.Id2}] = v
		case core.TopologyAssociatedWithEntityRelationship:
			out[relKey{"ASSOCIATED_WITH", *v.Id1, *v.Id2}] = v
		case core.TopologyRoutesToEntityRelationship:
			out[relKey{"ROUTES_TO", *v.Id1, *v.Id2}] = v
		default:
			require.Failf(t, "unexpected relationship", "%T", r)
		}
	}

	return out
}

func entityIDs(t *testing.T, entities []any) map[string]string {
	t.Helper()

	out := map[string]string{}

	for _, e := range entities {
		m, ok := e.(map[string]any)
		require.True(t, ok, "%T", e)

		id, _ := m["id"].(string)
		typ, _ := m["entityType"].(string)
		out[id] = typ
	}

	return out
}

func (e *env) rawStatus(t *testing.T, url, ifNoneMatch string) int {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	require.NoError(t, err)
	req.Header.Set("If-None-Match", ifNoneMatch)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	return resp.StatusCode
}

type topoFix struct {
	subnet core.Subnet
	vcn    core.Vcn
	rt     core.RouteTable
	igw    core.InternetGateway
	nsg    core.NetworkSecurityGroup
	drg    core.Drg
	inst   core.Instance
}

func (e *env) topoFixture(t *testing.T) topoFix {
	t.Helper()

	ctx := t.Context()
	f := e.fixture(t)

	igw, err := e.client.CreateInternetGateway(ctx, core.CreateInternetGatewayRequest{CompartmentId: new(testComp),
		VcnId: f.vcn.Id, IsEnabled: new(true)})
	require.NoError(t, err)

	drg := e.mkDrg(t, "topo")
	e.attachVcn(t, drg, f.vcn)

	rt, err := e.client.CreateRouteTable(
		ctx,
		core.CreateRouteTableRequest{CompartmentId: new(testComp), VcnId: f.vcn.Id,
			RouteRules: []core.RouteRule{
				{Destination: new("0.0.0.0/0"), NetworkEntityId: igw.Id},
				{Destination: new("172.16.0.0/12"), NetworkEntityId: drg.Id},
			}},
	)
	require.NoError(t, err)

	nsg, err := e.client.CreateNetworkSecurityGroup(ctx, core.CreateNetworkSecurityGroupRequest{
		CompartmentId: new(testComp), VcnId: f.vcn.Id})
	require.NoError(t, err)

	e.settle()

	_, err = e.client.UpdateSubnet(ctx, core.UpdateSubnetRequest{SubnetId: f.pub.Id, RouteTableId: rt.Id})
	require.NoError(t, err)

	inst, err := e.launch(t, f.pub, func(d *core.LaunchInstanceDetails) {
		d.CreateVnicDetails.NsgIds = []string{*nsg.Id}
	})
	require.NoError(t, err)
	e.settle()

	return topoFix{
		vcn:    f.vcn,
		subnet: f.pub,
		igw:    igw.InternetGateway,
		rt:     rt.RouteTable,
		nsg:    nsg.NetworkSecurityGroup,
		inst:   inst,
		drg:    drg,
	}
}

func TestVcnTopology(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.topoFixture(t)
	vnic := e.vnicOf(t, f.inst.Id)

	r, err := e.client.GetVcnTopology(ctx, core.GetVcnTopologyRequest{CompartmentId: new(testComp), VcnId: f.vcn.Id})
	require.NoError(t, err)

	top := r.VcnTopology
	assert.Equal(t, *f.vcn.Id, *top.VcnId)
	assert.NotEmpty(t, *r.Etag)
	assert.Empty(t, top.LimitedEntities)
	assert.NotNil(t, top.TimeCreated)

	ents := entityIDs(t, top.Entities)
	for id, typ := range map[string]string{
		*f.vcn.Id: "vcn", *f.subnet.Id: "subnet", *f.igw.Id: "internetgateway", *f.rt.Id: "routetable",
		*f.nsg.Id: "networksecuritygroup", *f.inst.Id: "instance", *vnic.Id: "vnic", *f.drg.Id: "drg",
		*f.vcn.DefaultSecurityListId: "securitylist", *f.vcn.DefaultDhcpOptionsId: "dhcpoptions",
	} {
		assert.Equal(t, typ, ents[id], id)
	}

	rels := relsOf(t, top.Relationships)

	for _, want := range []relKey{
		{"CONTAINS", *f.vcn.Id, *f.subnet.Id}, {"CONTAINS", *f.vcn.Id, *f.igw.Id}, {"CONTAINS", *f.vcn.Id, *f.rt.Id},
		{"CONTAINS", *f.subnet.Id, *vnic.Id}, {"CONTAINS", *f.inst.Id, *vnic.Id},
		{"ASSOCIATED_WITH", *f.subnet.Id, *f.rt.Id}, {"ASSOCIATED_WITH", *f.subnet.Id, *f.vcn.DefaultSecurityListId},
		{"ASSOCIATED_WITH", *vnic.Id, *f.nsg.Id}, {"ASSOCIATED_WITH", *f.vcn.Id, *f.drg.Id},
		{"ROUTES_TO", *f.rt.Id, *f.igw.Id}, {"ROUTES_TO", *f.rt.Id, *f.drg.Id},
	} {
		assert.Contains(t, rels, want)
	}

	route, ok := rels[relKey{"ROUTES_TO", *f.rt.Id, *f.igw.Id}].(core.TopologyRoutesToEntityRelationship)
	require.True(t, ok)
	assert.Equal(t, "0.0.0.0/0", *route.RouteRuleDetails.Destination)
	assert.Equal(t, *f.rt.Id, *route.RouteRuleDetails.RouteTableId)
	assert.Equal(t, core.TopologyRoutesToRelationshipDetailsRouteTypeStatic, route.RouteRuleDetails.RouteType)

	via, ok := rels[relKey{"ASSOCIATED_WITH", *f.vcn.Id, *f.drg.Id}].(core.TopologyAssociatedWithEntityRelationship)
	require.True(t, ok)
	assert.Len(t, via.AssociatedWithDetails.Via, 1, "the DRG attachment")

	_, err = e.client.GetVcnTopology(ctx, core.GetVcnTopologyRequest{CompartmentId: new(testComp),
		VcnId: new("ocid1.vcn.oc1.iad.nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	url := e.base + "/20160918/vcnTopology?compartmentId=" + testComp + "&vcnId=" + *f.vcn.Id
	assert.Equal(t, http.StatusNotModified, e.rawStatus(t, url, *r.Etag), "unchanged topologies answer 304")
	assert.Equal(t, http.StatusOK, e.rawStatus(t, url, "other"))

	_, err = e.client.UpdateRouteTable(ctx, core.UpdateRouteTableRequest{RtId: f.rt.Id,
		RouteRules: []core.RouteRule{{Destination: new("0.0.0.0/0"), NetworkEntityId: f.igw.Id}}})
	require.NoError(t, err)

	changed, err := e.client.GetVcnTopology(
		ctx,
		core.GetVcnTopologyRequest{CompartmentId: new(testComp), VcnId: f.vcn.Id},
	)
	require.NoError(t, err)
	assert.NotEqual(t, *r.Etag, *changed.Etag)

	top2 := changed.VcnTopology
	assert.NotContains(t, relsOf(t, top2.Relationships), relKey{"ROUTES_TO", *f.rt.Id, *f.drg.Id})
}

func TestSubnetTopology(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.topoFixture(t)
	vnic := e.vnicOf(t, f.inst.Id)

	r, err := e.client.GetSubnetTopology(
		ctx,
		core.GetSubnetTopologyRequest{CompartmentId: new(testComp), SubnetId: f.subnet.Id},
	)
	require.NoError(t, err)

	top := r.SubnetTopology
	assert.Equal(t, *f.subnet.Id, *top.SubnetId)

	ents := entityIDs(t, top.Entities)
	assert.Contains(t, ents, *f.vcn.Id)
	assert.Contains(t, ents, *f.subnet.Id)
	assert.Contains(t, ents, *vnic.Id)
	assert.Contains(t, ents, *f.rt.Id)
	assert.Contains(t, ents, *f.igw.Id, "route targets are part of the subnet topology")
	assert.NotContains(t, ents, *f.nsg.Id+"x")

	rels := relsOf(t, top.Relationships)
	assert.Contains(t, rels, relKey{"CONTAINS", *f.vcn.Id, *f.subnet.Id})
	assert.Contains(t, rels, relKey{"ASSOCIATED_WITH", *f.subnet.Id, *f.rt.Id})

	_, err = e.client.GetSubnetTopology(ctx, core.GetSubnetTopologyRequest{CompartmentId: new(testComp),
		SubnetId: new("ocid1.subnet.oc1.iad.nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestNetworkingTopology(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.topoFixture(t)

	otherComp, err := e.client.CreateVcn(ctx, core.CreateVcnRequest{CompartmentId: new(testComp + "x"),
		CidrBlocks: []string{"192.168.0.0/16"}})
	require.NoError(t, err)

	second := e.mkVcn(t, "172.20.0.0/16", "second")

	r, err := e.client.GetNetworkingTopology(ctx, core.GetNetworkingTopologyRequest{CompartmentId: new(testComp)})
	require.NoError(t, err)

	top := r.NetworkingTopology

	ents := entityIDs(t, top.Entities)
	assert.Equal(t, "vcn", ents[*f.vcn.Id])
	assert.Equal(t, "vcn", ents[*second.Id])
	assert.NotContains(t, ents, *otherComp.Id, "other compartments are not part of the topology")

	empty, err := e.client.GetNetworkingTopology(
		ctx,
		core.GetNetworkingTopologyRequest{CompartmentId: new(testComp + "y")},
	)
	require.NoError(t, err)

	etop := empty.NetworkingTopology
	assert.Empty(t, etop.Entities)
	assert.Empty(t, etop.Relationships)
}
