package ec2_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Capacity Reservation ---- //nolint:godot // existing issue.
func TestBatch3_CapacityReservation(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var crID string

	t.Run("create reservation", func(t *testing.T) { //nolint:paralleltest // existing issue.
		cr, err := b.CreateCapacityReservation("t3.micro", "us-east-1a", 2)
		require.NoError(t, err)
		assert.NotEmpty(t, cr.CapacityReservationID)
		assert.Equal(t, "active", cr.State)
		assert.Equal(t, 2, cr.TotalInstanceCount)
		crID = cr.CapacityReservationID
	})

	t.Run("modify instance count", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyCapacityReservation(crID, 5))
	})

	t.Run("get groups returns empty", func(t *testing.T) { //nolint:paralleltest // existing issue.
		groups, err := b.GetGroupsForCapacityReservation(crID)
		require.NoError(t, err)
		assert.Empty(t, groups)
	})

	t.Run("cancel reservation", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.CancelCapacityReservation(crID))
	})

	t.Run("empty ID returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.CancelCapacityReservation(""))
	})
}

// ---- Instance Connect Endpoint ---- //nolint:godot // existing issue.
func TestBatch3_InstanceConnectEndpoint(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var epID string

	t.Run("create endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ep, err := b.CreateInstanceConnectEndpoint("subnet-default", nil, false)
		require.NoError(t, err)
		assert.NotEmpty(t, ep.InstanceConnectEndpointID)
		assert.Equal(t, "active", ep.State)
		epID = ep.InstanceConnectEndpointID
	})

	t.Run("describe returns created", func(t *testing.T) { //nolint:paralleltest // existing issue.
		eps := b.DescribeInstanceConnectEndpoints([]string{epID})
		require.Len(t, eps, 1)
		assert.Equal(t, "subnet-default", eps[0].SubnetID)
	})

	t.Run("modify preserveClientIP", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyInstanceConnectEndpoint(epID, true))
	})

	t.Run("delete endpoint", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteInstanceConnectEndpoint(epID))
		eps := b.DescribeInstanceConnectEndpoints(nil)
		assert.Empty(t, eps)
	})

	t.Run("unknown subnet returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateInstanceConnectEndpoint("subnet-missing", nil, false)
		require.Error(t, err)
	})
}

// ---- Instance Event Window ---- //nolint:godot // existing issue.
func TestBatch3_InstanceEventWindow(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	var ewID string

	t.Run("create window", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ew, err := b.CreateInstanceEventWindow("test-window", "0 4 * * 6,7")
		require.NoError(t, err)
		assert.NotEmpty(t, ew.InstanceEventWindowID)
		ewID = ew.InstanceEventWindowID
	})

	t.Run("describe returns window", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ews := b.DescribeInstanceEventWindows([]string{ewID})
		require.Len(t, ews, 1)
		assert.Equal(t, "0 4 * * 6,7", ews[0].CronExpression)
	})

	t.Run("modify window", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyInstanceEventWindow(ewID, "updated", "0 3 * * *"))
	})

	t.Run("delete window", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteInstanceEventWindow(ewID))
	})
}

// ---- Spot Datafeed ---- //nolint:godot // existing issue.
func TestBatch3_SpotDatafeed(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("describe returns nil when empty", func(t *testing.T) { //nolint:paralleltest // existing issue.
		assert.Nil(t, b.DescribeSpotDatafeedSubscription())
	})

	t.Run("create datafeed", func(t *testing.T) { //nolint:paralleltest // existing issue.
		feed, err := b.CreateSpotDatafeedSubscription("my-bucket", "spot/")
		require.NoError(t, err)
		assert.Equal(t, "my-bucket", feed.Bucket)
	})

	t.Run("describe returns feed", func(t *testing.T) { //nolint:paralleltest // existing issue.
		feed := b.DescribeSpotDatafeedSubscription()
		require.NotNil(t, feed)
		assert.Equal(t, "spot/", feed.Prefix)
	})

	t.Run("delete removes feed", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b.DeleteSpotDatafeedSubscription()
		assert.Nil(t, b.DescribeSpotDatafeedSubscription())
	})
}

// ---- Image lifecycle ---- //nolint:godot // existing issue.
func TestBatch3_RegisterImage(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("registers new AMI", func(t *testing.T) { //nolint:paralleltest // existing issue.
		img, err := b.RegisterImage("test-ami", "test AMI", "x86_64")
		require.NoError(t, err)
		assert.NotEmpty(t, img.ImageID)
		assert.Equal(t, "test-ami", img.Name)
	})

	t.Run("empty name returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.RegisterImage("", "", "")
		require.Error(t, err)
	})
}
func TestBatch3_ImportExportImage(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("import image creates task", func(t *testing.T) { //nolint:paralleltest // existing issue.
		task, err := b.ImportImage("test import", "x86_64", "Linux/UNIX")
		require.NoError(t, err)
		assert.NotEmpty(t, task.ImportTaskID)
		assert.Equal(t, "completed", task.Status)
	})

	t.Run("describe import tasks returns all", func(t *testing.T) { //nolint:paralleltest // existing issue.
		tasks := b.DescribeImportImageTasks(nil)
		require.Len(t, tasks, 1)
	})

	t.Run("export image creates task", func(t *testing.T) { //nolint:paralleltest // existing issue.
		taskID, err := b.ExportImage("ami-test", "test export")
		require.NoError(t, err)
		assert.NotEmpty(t, taskID)
	})
}

// ---- Snapshot recycle bin ---- //nolint:godot // existing issue.
func TestBatch3_SnapshotRecycleBin(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("list empty recycle bin", func(t *testing.T) { //nolint:paralleltest // existing issue.
		snaps := b.ListSnapshotsInRecycleBin(nil)
		assert.Empty(t, snaps)
	})

	t.Run("import snapshot creates task", func(t *testing.T) { //nolint:paralleltest // existing issue.
		task, err := b.ImportSnapshot("test import")
		require.NoError(t, err)
		assert.NotEmpty(t, task.ImportTaskID)
	})
}
func TestBatch3_RestoreSnapshotTier(t *testing.T) { //nolint:paralleltest // existing issue.
	t.Run("restore from archive to standard", func(t *testing.T) {
		b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		vol, _ := b.CreateVolume("us-east-1a", "gp2", 10)
		snap, _ := b.CreateSnapshot(vol.ID, "tier test")
		_ = b.ModifySnapshotTier(snap.SnapshotID, "archive")

		require.NoError(t, b.RestoreSnapshotTier(snap.SnapshotID))
		items := b.DescribeSnapshotTierStatus([]string{snap.SnapshotID})
		require.Len(t, items, 1)
		assert.Equal(t, "standard", items[0].StorageTier)
	})
}

// ---- Fast launch / snapshot restores ---- //nolint:godot // existing issue.
func TestBatch3_FastLaunch(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	imageID := "ami-testfast"

	t.Run("enable fast launch", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.EnableFastLaunch(imageID))
		items := b.DescribeFastLaunchImages([]string{imageID})
		require.Len(t, items, 1)
		assert.Equal(t, "enabled", items[0].State)
	})

	t.Run("disable fast launch", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DisableFastLaunch(imageID))
		items := b.DescribeFastLaunchImages([]string{imageID})
		assert.Empty(t, items)
	})
}
func TestBatch3_FastSnapshotRestores(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("enable and describe", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.EnableFastSnapshotRestores([]string{"snap-abc"}, []string{"us-east-1a"}))
		items := b.DescribeFastSnapshotRestores()
		require.Len(t, items, 1)
		assert.Equal(t, "snap-abc", items[0].SnapshotID)
	})

	t.Run("disable removes entry", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DisableFastSnapshotRestores([]string{"snap-abc"}, []string{"us-east-1a"}))
		items := b.DescribeFastSnapshotRestores()
		assert.Empty(t, items)
	})
}

// ---- Instance utilities ---- //nolint:godot // existing issue.
func TestBatch3_GetPasswordData(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	insts, _ := b.RunInstances("ami-test", "t3.micro", "", 1)

	t.Run("returns empty for instance", func(t *testing.T) { //nolint:paralleltest // existing issue.
		data, _, err := b.GetPasswordData(insts[0].ID)
		require.NoError(t, err)
		assert.Empty(t, data)
	})

	t.Run("unknown instance returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, _, err := b.GetPasswordData("i-missing")
		require.Error(t, err)
	})
}
func TestBatch3_GetSecurityGroupsForVpc(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("returns default SG for default VPC", func(t *testing.T) { //nolint:paralleltest // existing issue.
		sgs, err := b.GetSecurityGroupsForVpc("vpc-default")
		require.NoError(t, err)
		assert.NotEmpty(t, sgs)
	})

	t.Run("empty VPC returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.GetSecurityGroupsForVpc("")
		require.Error(t, err)
	})
}
func TestBatch3_ReplaceRoute(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	rt, _ := b.CreateRouteTable("vpc-default")
	igw, _ := b.CreateInternetGateway()
	_ = b.CreateRoute(rt.ID, "0.0.0.0/0", igw.ID, "")

	t.Run("replaces existing route", func(t *testing.T) {
		igw2, _ := b.CreateInternetGateway()
		require.NoError(t, b.ReplaceRoute(rt.ID, "0.0.0.0/0", igw2.ID, ""))
	})
}

func TestBatch3_GetInstanceTypesFromInstanceRequirements( //nolint:paralleltest // existing issue.
	t *testing.T,
) {
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	t.Run("returns static list", func(t *testing.T) {
		types := b.GetInstanceTypesFromInstanceRequirements()
		assert.NotEmpty(t, types)
	})
}

// ---- VPN ---- //nolint:godot // existing issue.
func TestBatch3_VpnConnectionRoute(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	// Create a VPN connection first
	gw, _ := b.CreateCustomerGateway("ipsec.1", "1.2.3.4", "65000")
	vgw, _ := b.CreateVpnGateway("ipsec.1")
	conn, setupErr := b.CreateVpnConnection("ipsec.1", gw.CustomerGatewayID, vgw.VpnGatewayID)
	require.NoError(t, setupErr)

	t.Run("create route", func(t *testing.T) { //nolint:paralleltest // existing issue.
		route, err := b.CreateVpnConnectionRoute(conn.VpnConnectionID, "192.168.0.0/24")
		require.NoError(t, err)
		assert.Equal(t, "active", route.State)
	})

	t.Run("delete route", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.DeleteVpnConnectionRoute(conn.VpnConnectionID, "192.168.0.0/24"))
	})

	t.Run("modify VPN connection", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.ModifyVpnConnection(conn.VpnConnectionID, ""))
	})
}

// ---- ModifyTransitGateway ---- //nolint:godot // existing issue.
func TestBatch3_ModifyTransitGateway(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	tgw, _ := b.CreateTransitGateway("")

	t.Run("modifies description", func(t *testing.T) {
		require.NoError(t, b.ModifyTransitGateway(tgw.ID, "updated description"))
	})
}

// ---- UpdateSecurityGroupRuleDescriptions ---- //nolint:godot // existing issue.
func TestBatch3_UpdateSGRuleDescriptions(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	sg, _ := b.CreateSecurityGroup("test-sg", "test", "vpc-default")

	t.Run("ingress update", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.UpdateSecurityGroupRuleDescriptionsIngress(sg.ID, nil))
	})

	t.Run("egress update", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.UpdateSecurityGroupRuleDescriptionsEgress(sg.ID, nil))
	})

	t.Run("unknown SG returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.UpdateSecurityGroupRuleDescriptionsIngress("sg-missing", nil))
	})
}

// ---- HTTP dispatch tests ---- //nolint:godot // existing issue.
func TestBatch3_HTTP_CreateCapacityReservation(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	h := ec2.NewHandler(b)

	_, err := ec2.ExportDispatch(h, url.Values{
		"Action":        []string{"CreateCapacityReservation"},
		"InstanceType":  []string{"t3.micro"},
		"InstanceCount": []string{"1"},
	})
	require.NoError(t, err)
}
func TestBatch3_HTTP_DescribeSpotDatafeedSubscription(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeSpotDatafeedSubscription"},
	})
	require.NoError(t, err)
}

func TestBatch3_HTTP_GetInstanceTypesFromInstanceRequirements( //nolint:paralleltest // existing issue.
	t *testing.T,
) {
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"GetInstanceTypesFromInstanceRequirements"},
	})
	require.NoError(t, err)
}
func TestBatch3_HTTP_DescribeFastSnapshotRestores(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"DescribeFastSnapshotRestores"},
	})
	require.NoError(t, err)
}
func TestBatch3_HTTP_ListSnapshotsInRecycleBin(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler()

	_, err := dispatchHandler(h, url.Values{
		"Action": []string{"ListSnapshotsInRecycleBin"},
	})
	require.NoError(t, err)
}
