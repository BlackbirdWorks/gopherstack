package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestLocalGateway_SeedAndDescribe(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	lg, err := bk.SeedLocalGateway(ec2.LocalGateway{OutpostArn: "arn:aws:outposts:us-east-1:000000000000:outpost/op-1"})
	require.NoError(t, err)
	assert.NotEmpty(t, lg.LocalGatewayID)
	assert.Equal(t, "000000000000", lg.OwnerID)
	assert.Equal(t, "available", lg.State)

	// Describe all.
	all := bk.DescribeLocalGateways(nil)
	require.Len(t, all, 1)
	assert.Equal(t, lg.LocalGatewayID, all[0].LocalGatewayID)

	// Describe filtered by ID.
	filtered := bk.DescribeLocalGateways([]string{lg.LocalGatewayID})
	require.Len(t, filtered, 1)

	// Describe filtered by unknown ID returns empty (not an error) — matches AWS shape.
	none := bk.DescribeLocalGateways([]string{"lgw-doesnotexist"})
	assert.Empty(t, none)
}

func TestLocalGateway_DescribeEmpty(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	assert.Empty(t, bk.DescribeLocalGateways(nil))
	assert.Empty(t, bk.DescribeLocalGatewayVirtualInterfaces(nil))
	assert.Empty(t, bk.DescribeLocalGatewayVirtualInterfaceGroups(nil))
	assert.Empty(t, bk.DescribeLocalGatewayRouteTables(nil))
	assert.Empty(t, bk.DescribeLocalGatewayRouteTableVpcAssociations(nil))
	assert.Empty(t, bk.DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations(nil))
}

func TestLocalGateway_SeedVirtualInterfaceAndGroup(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	lg, err := bk.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)

	group, err := bk.SeedLocalGatewayVirtualInterfaceGroup(ec2.LocalGatewayVirtualInterfaceGroup{
		LocalGatewayID: lg.LocalGatewayID,
		LocalBgpAsn:    64512,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, group.LocalGatewayVirtualInterfaceGroupID)
	assert.Equal(t, "available", group.ConfigurationState)

	vif, err := bk.SeedLocalGatewayVirtualInterface(ec2.LocalGatewayVirtualInterface{
		LocalGatewayID:                      lg.LocalGatewayID,
		LocalGatewayVirtualInterfaceGroupID: group.LocalGatewayVirtualInterfaceGroupID,
		Vlan:                                100,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, vif.LocalGatewayVirtualInterfaceID)

	vifs := bk.DescribeLocalGatewayVirtualInterfaces(nil)
	require.Len(t, vifs, 1)
	assert.Equal(t, vif.LocalGatewayVirtualInterfaceID, vifs[0].LocalGatewayVirtualInterfaceID)

	groups := bk.DescribeLocalGatewayVirtualInterfaceGroups([]string{group.LocalGatewayVirtualInterfaceGroupID})
	require.Len(t, groups, 1)
}

func TestLocalGateway_RouteTableLifecycle(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	// Creating a route table for an unseeded local gateway fails.
	_, err := bk.CreateLocalGatewayRouteTable("lgw-doesnotexist", "coip")
	require.Error(t, err)

	lg, err := bk.SeedLocalGateway(ec2.LocalGateway{OutpostArn: "arn:aws:outposts:us-east-1:000000000000:outpost/op-1"})
	require.NoError(t, err)

	rt, err := bk.CreateLocalGatewayRouteTable(lg.LocalGatewayID, "direct-vpc-routing")
	require.NoError(t, err)
	assert.NotEmpty(t, rt.LocalGatewayRouteTableID)
	assert.Equal(t, "direct-vpc-routing", rt.Mode)
	assert.Equal(t, "available", rt.State)
	assert.Equal(t, lg.OutpostArn, rt.OutpostArn)
	assert.Contains(t, rt.LocalGatewayRouteTableArn, rt.LocalGatewayRouteTableID)

	all := bk.DescribeLocalGatewayRouteTables(nil)
	require.Len(t, all, 1)

	filtered := bk.DescribeLocalGatewayRouteTables([]string{rt.LocalGatewayRouteTableID})
	require.Len(t, filtered, 1)

	require.NoError(t, bk.DeleteLocalGatewayRouteTable(rt.LocalGatewayRouteTableID))
	assert.Empty(t, bk.DescribeLocalGatewayRouteTables(nil))

	// Delete not found.
	err2 := bk.DeleteLocalGatewayRouteTable(rt.LocalGatewayRouteTableID)
	require.Error(t, err2)
	require.ErrorIs(t, err2, ec2.ErrLocalGatewayRouteTableNotFound)

	// Empty ID.
	require.Error(t, bk.DeleteLocalGatewayRouteTable(""))
}

func TestLocalGateway_RouteLifecycle(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	lg, err := bk.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)

	rt, err := bk.CreateLocalGatewayRouteTable(lg.LocalGatewayID, "coip")
	require.NoError(t, err)

	// Creating a route in a non-existent route table fails.
	_, err = bk.CreateLocalGatewayRoute("lgw-rtb-doesnotexist", "10.0.0.0/24", "", "", "eni-1")
	require.Error(t, err)

	route, err := bk.CreateLocalGatewayRoute(rt.LocalGatewayRouteTableID, "10.0.0.0/24", "", "", "eni-1")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.0/24", route.DestinationCidrBlock)
	assert.Equal(t, "eni-1", route.NetworkInterfaceID)
	assert.Equal(t, "static", route.Type)
	assert.Equal(t, "active", route.State)
	assert.Equal(t, rt.LocalGatewayRouteTableArn, route.LocalGatewayRouteTableArn)

	// Duplicate route fails.
	_, err = bk.CreateLocalGatewayRoute(rt.LocalGatewayRouteTableID, "10.0.0.0/24", "", "", "eni-2")
	require.Error(t, err)

	// Modify the route to point at a VIF group instead of the ENI.
	modified, err := bk.ModifyLocalGatewayRoute(
		rt.LocalGatewayRouteTableID, "10.0.0.0/24", "", "lgw-vif-grp-1", "",
	)
	require.NoError(t, err)
	assert.Equal(t, "lgw-vif-grp-1", modified.LocalGatewayVirtualInterfaceGroupID)
	assert.Empty(t, modified.NetworkInterfaceID)

	// Modify a route that doesn't exist fails.
	_, err = bk.ModifyLocalGatewayRoute(rt.LocalGatewayRouteTableID, "192.168.0.0/24", "", "", "eni-3")
	require.Error(t, err)

	// Search returns the route.
	found, err := bk.SearchLocalGatewayRoutes(rt.LocalGatewayRouteTableID, nil)
	require.NoError(t, err)
	require.Len(t, found, 1)

	// Search against an unknown route table fails.
	_, err = bk.SearchLocalGatewayRoutes("lgw-rtb-doesnotexist", nil)
	require.Error(t, err)

	// Delete the route.
	deleted, err := bk.DeleteLocalGatewayRoute(rt.LocalGatewayRouteTableID, "10.0.0.0/24", "")
	require.NoError(t, err)
	assert.Equal(t, "deleted", deleted.State)

	// Delete not found.
	_, err = bk.DeleteLocalGatewayRoute(rt.LocalGatewayRouteTableID, "10.0.0.0/24", "")
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrLocalGatewayRouteNotFound)
}

func TestLocalGateway_VpcAssociationLifecycle(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	lg, err := bk.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)

	rt, err := bk.CreateLocalGatewayRouteTable(lg.LocalGatewayID, "direct-vpc-routing")
	require.NoError(t, err)

	vpc, err := bk.CreateVpc("10.1.0.0/16")
	require.NoError(t, err)

	// VPC not found.
	_, err = bk.CreateLocalGatewayRouteTableVpcAssociation(rt.LocalGatewayRouteTableID, "vpc-doesnotexist")
	require.Error(t, err)

	// Route table not found.
	_, err = bk.CreateLocalGatewayRouteTableVpcAssociation("lgw-rtb-doesnotexist", vpc.ID)
	require.Error(t, err)

	assoc, err := bk.CreateLocalGatewayRouteTableVpcAssociation(rt.LocalGatewayRouteTableID, vpc.ID)
	require.NoError(t, err)
	assert.NotEmpty(t, assoc.LocalGatewayRouteTableVpcAssociationID)
	assert.Equal(t, "associated", assoc.State)
	assert.Equal(t, vpc.ID, assoc.VpcID)
	assert.Equal(t, lg.LocalGatewayID, assoc.LocalGatewayID)

	all := bk.DescribeLocalGatewayRouteTableVpcAssociations(nil)
	require.Len(t, all, 1)

	deleted, err := bk.DeleteLocalGatewayRouteTableVpcAssociation(assoc.LocalGatewayRouteTableVpcAssociationID)
	require.NoError(t, err)
	assert.Equal(t, "disassociated", deleted.State)
	assert.Empty(t, bk.DescribeLocalGatewayRouteTableVpcAssociations(nil))

	_, err = bk.DeleteLocalGatewayRouteTableVpcAssociation(assoc.LocalGatewayRouteTableVpcAssociationID)
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrLocalGatewayVpcAssociationNotFound)
}

func TestLocalGateway_VifGroupAssociationLifecycle(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	lg, err := bk.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)

	rt, err := bk.CreateLocalGatewayRouteTable(lg.LocalGatewayID, "coip")
	require.NoError(t, err)

	// Virtual interface group not found.
	_, err = bk.CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
		rt.LocalGatewayRouteTableID, "lgw-vif-grp-doesnotexist",
	)
	require.Error(t, err)

	group, err := bk.SeedLocalGatewayVirtualInterfaceGroup(ec2.LocalGatewayVirtualInterfaceGroup{
		LocalGatewayID: lg.LocalGatewayID,
	})
	require.NoError(t, err)

	// Route table not found.
	_, err = bk.CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
		"lgw-rtb-doesnotexist", group.LocalGatewayVirtualInterfaceGroupID,
	)
	require.Error(t, err)

	assoc, err := bk.CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
		rt.LocalGatewayRouteTableID, group.LocalGatewayVirtualInterfaceGroupID,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, assoc.LocalGatewayRouteTableVirtualInterfaceGroupAssociationID)
	assert.Equal(t, "associated", assoc.State)

	all := bk.DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations(nil)
	require.Len(t, all, 1)

	filtered := bk.DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations(
		[]string{assoc.LocalGatewayRouteTableVirtualInterfaceGroupAssociationID},
	)
	require.Len(t, filtered, 1)

	deleted, err := bk.DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
		assoc.LocalGatewayRouteTableVirtualInterfaceGroupAssociationID,
	)
	require.NoError(t, err)
	assert.Equal(t, "disassociated", deleted.State)

	_, err = bk.DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
		assoc.LocalGatewayRouteTableVirtualInterfaceGroupAssociationID,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrLocalGatewayVifGroupAssociationNotFound)
}

func TestLocalGateway_SnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	lg, err := bk.SeedLocalGateway(ec2.LocalGateway{OutpostArn: "arn:aws:outposts:us-east-1:000000000000:outpost/op-1"})
	require.NoError(t, err)

	group, err := bk.SeedLocalGatewayVirtualInterfaceGroup(ec2.LocalGatewayVirtualInterfaceGroup{
		LocalGatewayID: lg.LocalGatewayID,
	})
	require.NoError(t, err)

	_, err = bk.SeedLocalGatewayVirtualInterface(ec2.LocalGatewayVirtualInterface{
		LocalGatewayID:                      lg.LocalGatewayID,
		LocalGatewayVirtualInterfaceGroupID: group.LocalGatewayVirtualInterfaceGroupID,
	})
	require.NoError(t, err)

	rt, err := bk.CreateLocalGatewayRouteTable(lg.LocalGatewayID, "coip")
	require.NoError(t, err)

	_, err = bk.CreateLocalGatewayRoute(rt.LocalGatewayRouteTableID, "10.2.0.0/24", "", "", "eni-9")
	require.NoError(t, err)

	vpc, err := bk.CreateVpc("10.3.0.0/16")
	require.NoError(t, err)

	_, err = bk.CreateLocalGatewayRouteTableVpcAssociation(rt.LocalGatewayRouteTableID, vpc.ID)
	require.NoError(t, err)

	_, err = bk.CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation(
		rt.LocalGatewayRouteTableID, group.LocalGatewayVirtualInterfaceGroupID,
	)
	require.NoError(t, err)

	snap := bk.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := newTestBackend()
	require.NoError(t, restored.Restore(t.Context(), snap))

	assert.Len(t, restored.DescribeLocalGateways(nil), 1)
	assert.Len(t, restored.DescribeLocalGatewayVirtualInterfaces(nil), 1)
	assert.Len(t, restored.DescribeLocalGatewayVirtualInterfaceGroups(nil), 1)
	assert.Len(t, restored.DescribeLocalGatewayRouteTables(nil), 1)
	assert.Len(t, restored.DescribeLocalGatewayRouteTableVpcAssociations(nil), 1)
	assert.Len(t, restored.DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations(nil), 1)

	found, err := restored.SearchLocalGatewayRoutes(rt.LocalGatewayRouteTableID, nil)
	require.NoError(t, err)
	assert.Len(t, found, 1)
}

func TestLocalGatewayVirtualInterfaceGroup_CreateDelete(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	lg, err := bk.SeedLocalGateway(ec2.LocalGateway{})
	require.NoError(t, err)

	t.Run("create requires a known local gateway", func(t *testing.T) {
		t.Parallel()

		_, createErr := bk.CreateLocalGatewayVirtualInterfaceGroup("lgw-doesnotexist", 64512, 0, nil)
		require.Error(t, createErr)
	})

	group, err := bk.CreateLocalGatewayVirtualInterfaceGroup(
		lg.LocalGatewayID,
		64512,
		0,
		map[string]string{"Name": "grp"},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, group.LocalGatewayVirtualInterfaceGroupID)
	assert.Equal(t, lg.LocalGatewayID, group.LocalGatewayID)
	assert.Equal(t, "available", group.ConfigurationState)
	assert.Contains(t, group.LocalGatewayVirtualInterfaceGroupArn, group.LocalGatewayVirtualInterfaceGroupID)
	assert.Equal(t, "grp", group.Tags["Name"])

	vif, err := bk.CreateLocalGatewayVirtualInterface(ec2.LocalGatewayVirtualInterfaceParams{
		LocalGatewayVirtualInterfaceGroupID: group.LocalGatewayVirtualInterfaceGroupID,
		OutpostLagID:                        "lag-1",
		LocalAddress:                        "169.254.0.1",
		PeerAddress:                         "169.254.0.2",
		Vlan:                                100,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, vif.LocalGatewayVirtualInterfaceID)
	assert.Equal(t, lg.LocalGatewayID, vif.LocalGatewayID)
	assert.Equal(t, group.LocalGatewayVirtualInterfaceGroupID, vif.LocalGatewayVirtualInterfaceGroupID)
	assert.Contains(t, vif.LocalGatewayVirtualInterfaceArn, vif.LocalGatewayVirtualInterfaceID)

	groups := bk.DescribeLocalGatewayVirtualInterfaceGroups([]string{group.LocalGatewayVirtualInterfaceGroupID})
	require.Len(t, groups, 1)
	assert.Contains(t, groups[0].LocalGatewayVirtualInterfaceIDs, vif.LocalGatewayVirtualInterfaceID)

	_, err = bk.DeleteLocalGatewayVirtualInterfaceGroup(group.LocalGatewayVirtualInterfaceGroupID)
	require.Error(t, err)

	deletedVif, err := bk.DeleteLocalGatewayVirtualInterface(vif.LocalGatewayVirtualInterfaceID)
	require.NoError(t, err)
	assert.Equal(t, "deleted", deletedVif.ConfigurationState)
	assert.Empty(t, bk.DescribeLocalGatewayVirtualInterfaces([]string{vif.LocalGatewayVirtualInterfaceID}))

	groupsAfter := bk.DescribeLocalGatewayVirtualInterfaceGroups([]string{group.LocalGatewayVirtualInterfaceGroupID})
	require.Len(t, groupsAfter, 1)
	assert.NotContains(t, groupsAfter[0].LocalGatewayVirtualInterfaceIDs, vif.LocalGatewayVirtualInterfaceID)

	deletedGroup, err := bk.DeleteLocalGatewayVirtualInterfaceGroup(group.LocalGatewayVirtualInterfaceGroupID)
	require.NoError(t, err)
	assert.Equal(t, "deleted", deletedGroup.ConfigurationState)

	_, err = bk.DeleteLocalGatewayVirtualInterfaceGroup(group.LocalGatewayVirtualInterfaceGroupID)
	require.Error(t, err)

	_, err = bk.CreateLocalGatewayVirtualInterface(ec2.LocalGatewayVirtualInterfaceParams{
		LocalGatewayVirtualInterfaceGroupID: "lgw-vif-grp-doesnotexist",
		OutpostLagID:                        "lag-1",
		LocalAddress:                        "169.254.0.1",
		PeerAddress:                         "169.254.0.2",
	})
	require.Error(t, err)

	_, err = bk.DeleteLocalGatewayVirtualInterface("lgw-vif-doesnotexist")
	require.Error(t, err)
}
