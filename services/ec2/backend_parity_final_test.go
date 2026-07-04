package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- VerifiedAccess modify ----

func TestParityFinal_ModifyVerifiedAccessGroup(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	inst, err := b.CreateVerifiedAccessInstance("orig instance")
	require.NoError(t, err)
	other, err := b.CreateVerifiedAccessInstance("other instance")
	require.NoError(t, err)
	grp, err := b.CreateVerifiedAccessGroup(inst.VerifiedAccessInstanceID, "orig")
	require.NoError(t, err)

	updated, err := b.ModifyVerifiedAccessGroup(grp.VerifiedAccessGroupID, other.VerifiedAccessInstanceID, "updated")
	require.NoError(t, err)
	assert.Equal(t, "updated", updated.Description)
	assert.Equal(t, other.VerifiedAccessInstanceID, updated.VerifiedAccessInstanceID)

	// Mutation is real: a fresh describe reflects it.
	described := b.DescribeVerifiedAccessGroups([]string{grp.VerifiedAccessGroupID})
	require.Len(t, described, 1)
	assert.Equal(t, "updated", described[0].Description)

	_, err = b.ModifyVerifiedAccessGroup("vagr-missing", "", "x")
	require.ErrorIs(t, err, ec2.ErrVerifiedAccessGroupNotFound)

	_, err = b.ModifyVerifiedAccessGroup(grp.VerifiedAccessGroupID, "vai-missing", "x")
	require.ErrorIs(t, err, ec2.ErrVerifiedAccessInstanceNotFound)
}

func TestParityFinal_ModifyVerifiedAccessInstance(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	inst, err := b.CreateVerifiedAccessInstance("orig")
	require.NoError(t, err)

	updated, err := b.ModifyVerifiedAccessInstance(inst.VerifiedAccessInstanceID, "updated")
	require.NoError(t, err)
	assert.Equal(t, "updated", updated.Description)

	_, err = b.ModifyVerifiedAccessInstance("vai-missing", "x")
	require.ErrorIs(t, err, ec2.ErrVerifiedAccessInstanceNotFound)
}

func TestParityFinal_ModifyVerifiedAccessTrustProvider(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	tp, err := b.CreateVerifiedAccessTrustProvider("user", "orig")
	require.NoError(t, err)

	updated, err := b.ModifyVerifiedAccessTrustProvider(tp.VerifiedAccessTrustProviderID, "updated")
	require.NoError(t, err)
	assert.Equal(t, "updated", updated.Description)

	_, err = b.ModifyVerifiedAccessTrustProvider("vatp-missing", "x")
	require.ErrorIs(t, err, ec2.ErrVerifiedAccessTrustProviderNF)
}

// ---- Transit Gateway route propagation + unified attachment describe ----

func TestParityFinal_TransitGatewayRoutePropagation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	tgw, err := b.CreateTransitGateway("test tgw")
	require.NoError(t, err)
	rt, err := b.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)
	att, err := b.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-default", nil)
	require.NoError(t, err)

	// Before enabling, both getters return empty.
	props, err := b.GetTransitGatewayRouteTablePropagations(rt.RouteTableID)
	require.NoError(t, err)
	assert.Empty(t, props)

	attProps, err := b.GetTransitGatewayAttachmentPropagations(att.TransitGatewayAttachmentID)
	require.NoError(t, err)
	assert.Empty(t, attProps)

	prop, err := b.EnableTransitGatewayRouteTablePropagation(rt.RouteTableID, att.TransitGatewayAttachmentID)
	require.NoError(t, err)
	assert.Equal(t, "enabled", prop.State)
	assert.Equal(t, "vpc", prop.ResourceType)
	assert.Equal(t, "vpc-default", prop.ResourceID)

	props, err = b.GetTransitGatewayRouteTablePropagations(rt.RouteTableID)
	require.NoError(t, err)
	require.Len(t, props, 1)
	assert.Equal(t, att.TransitGatewayAttachmentID, props[0].TransitGatewayAttachmentID)

	attProps, err = b.GetTransitGatewayAttachmentPropagations(att.TransitGatewayAttachmentID)
	require.NoError(t, err)
	require.Len(t, attProps, 1)
	assert.Equal(t, rt.RouteTableID, attProps[0].TransitGatewayRouteTableID)
	assert.Equal(t, "enabled", attProps[0].State)

	disabled, err := b.DisableTransitGatewayRouteTablePropagation(rt.RouteTableID, att.TransitGatewayAttachmentID)
	require.NoError(t, err)
	assert.Equal(t, "disabled", disabled.State)

	props, err = b.GetTransitGatewayRouteTablePropagations(rt.RouteTableID)
	require.NoError(t, err)
	assert.Empty(t, props)

	_, err = b.DisableTransitGatewayRouteTablePropagation(rt.RouteTableID, att.TransitGatewayAttachmentID)
	require.ErrorIs(t, err, ec2.ErrTGWPropagationNotFound)

	_, err = b.EnableTransitGatewayRouteTablePropagation("tgw-rtb-missing", att.TransitGatewayAttachmentID)
	require.ErrorIs(t, err, ec2.ErrTGWRouteTableNotFound)

	_, err = b.EnableTransitGatewayRouteTablePropagation(rt.RouteTableID, "tgw-attach-missing")
	require.ErrorIs(t, err, ec2.ErrTGWAttachmentNotFound)
}

func TestParityFinal_DescribeTransitGatewayAttachments(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	tgw, err := b.CreateTransitGateway("test tgw")
	require.NoError(t, err)
	att, err := b.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-default", nil)
	require.NoError(t, err)

	all := b.DescribeTransitGatewayAttachments(nil)
	require.NotEmpty(t, all)

	found := b.DescribeTransitGatewayAttachments([]string{att.TransitGatewayAttachmentID})
	require.Len(t, found, 1)
	assert.Equal(t, "vpc", found[0].ResourceType)
	assert.Equal(t, "vpc-default", found[0].ResourceID)
	assert.Equal(t, tgw.ID, found[0].TransitGatewayID)
}

// ---- Capacity Reservation extras ----

func TestParityFinal_InterruptibleCapacityReservationAllocation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	cr, err := b.CreateCapacityReservation("m5.large", "us-east-1a", 10)
	require.NoError(t, err)

	alloc, err := b.CreateInterruptibleCapacityReservationAllocation(cr.CapacityReservationID, 4)
	require.NoError(t, err)
	assert.Equal(t, int32(4), alloc.TargetInstanceCount)
	assert.Equal(t, "active", alloc.Status)

	crs := b.DescribeCapacityReservations([]string{cr.CapacityReservationID})
	require.Len(t, crs, 1)
	assert.Equal(t, 6, crs[0].AvailableInstanceCount)

	updated, err := b.UpdateInterruptibleCapacityReservationAllocation(cr.CapacityReservationID, 6)
	require.NoError(t, err)
	assert.Equal(t, int32(6), updated.TargetInstanceCount)

	crs = b.DescribeCapacityReservations([]string{cr.CapacityReservationID})
	require.Len(t, crs, 1)
	assert.Equal(t, 4, crs[0].AvailableInstanceCount)

	_, err = b.CreateInterruptibleCapacityReservationAllocation(cr.CapacityReservationID, 100)
	require.ErrorIs(t, err, ec2.ErrCapacityReservationFull)

	_, err = b.UpdateInterruptibleCapacityReservationAllocation("cr-missing", 1)
	require.ErrorIs(t, err, ec2.ErrCapacityReservationNotFound)

	_, err = b.CreateInterruptibleCapacityReservationAllocation("cr-missing", 1)
	require.ErrorIs(t, err, ec2.ErrCapacityReservationNotFound)
}

func TestParityFinal_UpdateInterruptibleAllocationNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	cr, err := b.CreateCapacityReservation("m5.large", "us-east-1a", 10)
	require.NoError(t, err)

	_, err = b.UpdateInterruptibleCapacityReservationAllocation(cr.CapacityReservationID, 5)
	require.ErrorIs(t, err, ec2.ErrInterruptibleAllocationNotFound)
}

func TestParityFinal_GetCapacityReservationUsage(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	cr, err := b.CreateCapacityReservation("m5.large", "us-east-1a", 10)
	require.NoError(t, err)

	instances, err := b.RunInstances("ami-test", "m5.large", "", 2)
	require.NoError(t, err)

	for _, inst := range instances {
		_, modErr := b.ModifyInstanceCapacityReservationAttributes(inst.ID, "open", cr.CapacityReservationID, "")
		require.NoError(t, modErr)
	}

	usage, err := b.GetCapacityReservationUsage(cr.CapacityReservationID)
	require.NoError(t, err)
	assert.Equal(t, cr.CapacityReservationID, usage.CapacityReservationID)
	require.Len(t, usage.InstanceUsages, 1)
	assert.Equal(t, int32(2), usage.InstanceUsages[0].UsedInstanceCount)
	assert.False(t, usage.Interruptible)

	_, err = b.CreateInterruptibleCapacityReservationAllocation(cr.CapacityReservationID, 3)
	require.NoError(t, err)

	usage, err = b.GetCapacityReservationUsage(cr.CapacityReservationID)
	require.NoError(t, err)
	assert.True(t, usage.Interruptible)
	require.NotNil(t, usage.InterruptibleAllocation)
	assert.Equal(t, int32(3), usage.InterruptibleAllocation.TargetInstanceCount)

	_, err = b.GetCapacityReservationUsage("cr-missing")
	require.ErrorIs(t, err, ec2.ErrCapacityReservationNotFound)
}

func TestParityFinal_DescribeCapacityReservationTopology(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	cr, err := b.CreateCapacityReservation("m5.large", "us-east-1a", 10)
	require.NoError(t, err)

	topo := b.DescribeCapacityReservationTopology([]string{cr.CapacityReservationID})
	require.Len(t, topo, 1)
	assert.Equal(t, cr.CapacityReservationID, topo[0].CapacityReservationID)
	assert.Equal(t, "m5.large", topo[0].InstanceType)
	assert.Equal(t, "us-east-1a", topo[0].AvailabilityZone)

	all := b.DescribeCapacityReservationTopology(nil)
	assert.NotEmpty(t, all)
}

// ---- Address / VPC endpoint / NAT gateway misc ----

func TestParityFinal_MoveAddressToVpcAndDescribeMovingAddresses(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	addr, err := b.AllocateAddress()
	require.NoError(t, err)

	moved, err := b.MoveAddressToVpc(addr.PublicIP)
	require.NoError(t, err)
	assert.Equal(t, addr.AllocationID, moved.AllocationID)

	statuses := b.DescribeMovingAddresses(nil)
	require.Len(t, statuses, 1)
	assert.Equal(t, addr.PublicIP, statuses[0].PublicIP)
	assert.Equal(t, "movingToVpc", statuses[0].MoveStatus)

	filtered := b.DescribeMovingAddresses([]string{addr.PublicIP})
	require.Len(t, filtered, 1)

	none := b.DescribeMovingAddresses([]string{"1.2.3.4"})
	assert.Empty(t, none)

	_, err = b.MoveAddressToVpc("9.9.9.9")
	require.ErrorIs(t, err, ec2.ErrPublicIPNotFound)
}

func TestParityFinal_RejectVpcEndpointConnections(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	accepted, err := b.AcceptVpcEndpointConnections("vpce-svc-1", []string{"vpce-1"})
	require.NoError(t, err)
	require.Len(t, accepted, 1)

	unsuccessful, err := b.RejectVpcEndpointConnections("vpce-svc-1", []string{"vpce-1", "vpce-missing"})
	require.NoError(t, err)
	assert.Equal(t, []string{"vpce-missing"}, unsuccessful)

	conns := b.DescribeVpcEndpointConnections([]string{"vpce-svc-1"})
	require.Len(t, conns, 1)
	assert.Equal(t, "rejected", conns[0].State)

	_, err = b.RejectVpcEndpointConnections("", []string{"vpce-1"})
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = b.RejectVpcEndpointConnections("vpce-svc-1", nil)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestParityFinal_UnassignPrivateNatGatewayAddress(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	addr, err := b.AllocateAddress()
	require.NoError(t, err)
	nat, err := b.CreateNatGateway("subnet-default", addr.AllocationID)
	require.NoError(t, err)

	require.NoError(t, b.AssignPrivateNatGatewayAddress(nat.ID))
	require.NoError(t, b.AssignPrivateNatGatewayAddress(nat.ID))

	described := b.DescribeNatGateways([]string{nat.ID})
	require.Len(t, described, 1)
	require.Len(t, described[0].SecondaryPrivateIPs, 2)

	firstSecondary := described[0].SecondaryPrivateIPs[0]

	updated, err := b.UnassignPrivateNatGatewayAddress(nat.ID, []string{firstSecondary})
	require.NoError(t, err)
	assert.Len(t, updated.SecondaryPrivateIPs, 1)
	assert.NotContains(t, updated.SecondaryPrivateIPs, firstSecondary)

	_, err = b.UnassignPrivateNatGatewayAddress("nat-missing", []string{firstSecondary})
	require.ErrorIs(t, err, ec2.ErrNatGatewayNotFound)

	_, err = b.UnassignPrivateNatGatewayAddress(nat.ID, nil)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

// ---- Image extras ----

func TestParityFinal_CancelImageLaunchPermission(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	require.NoError(t, b.ModifyImageAttribute("ami-testimg", "launchPermission", "all"))
	require.NoError(t, b.CancelImageLaunchPermission("ami-testimg"))
	// Idempotent: calling again on an image with no stored permission is fine.
	require.NoError(t, b.CancelImageLaunchPermission("ami-testimg"))

	require.ErrorIs(t, b.CancelImageLaunchPermission(""), ec2.ErrInvalidParameter)
}

func TestParityFinal_DescribeImageReferences(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	instances, err := b.RunInstances("ami-refimg", "t3.micro", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)

	_, err = b.CreateLaunchTemplate("lt-name", "ami-refimg", "t3.micro")
	require.NoError(t, err)

	refs := b.DescribeImageReferences([]string{"ami-refimg"})
	require.Len(t, refs, 2)

	var sawInstance, sawLaunchTemplate bool

	for _, r := range refs {
		assert.Equal(t, "ami-refimg", r.ImageID)
		assert.NotEmpty(t, r.Arn)

		switch r.ResourceType {
		case "ec2:Instance":
			sawInstance = true
		case "ec2:LaunchTemplate":
			sawLaunchTemplate = true
		}
	}

	assert.True(t, sawInstance)
	assert.True(t, sawLaunchTemplate)

	assert.Empty(t, b.DescribeImageReferences([]string{"ami-unreferenced"}))
}

func TestParityFinal_GetImageAncestry(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	root, err := b.CreateImage(instances[0].ID, "root", "root image")
	require.NoError(t, err)

	child, err := b.CopyImage(root.ImageID, "child", "child image")
	require.NoError(t, err)

	grandchild, err := b.CopyImage(child.ImageID, "grandchild", "grandchild image")
	require.NoError(t, err)

	entries, err := b.GetImageAncestry(grandchild.ImageID)
	require.NoError(t, err)
	require.Len(t, entries, 3)
	assert.Equal(t, grandchild.ImageID, entries[0].ImageID)
	assert.Equal(t, child.ImageID, entries[0].SourceImageID)
	assert.Equal(t, child.ImageID, entries[1].ImageID)
	assert.Equal(t, root.ImageID, entries[1].SourceImageID)
	assert.Equal(t, root.ImageID, entries[2].ImageID)
	assert.Empty(t, entries[2].SourceImageID)

	_, err = b.GetImageAncestry("ami-missing")
	require.ErrorIs(t, err, ec2.ErrImageNotFound)
}

func TestParityFinal_GetFlowLogsIntegrationTemplate(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	fls, err := b.CreateFlowLogs([]string{"vpc-default"}, "ALL", "s3", "arn:aws:s3:::dest-bucket")
	require.NoError(t, err)
	require.Len(t, fls, 1)

	tmpl, err := b.GetFlowLogsIntegrationTemplate(fls[0].FlowLogID, "arn:aws:s3:::my-cfn-bucket")
	require.NoError(t, err)
	assert.Contains(t, tmpl, fls[0].FlowLogID)
	assert.Contains(t, tmpl, "arn:aws:s3:::my-cfn-bucket")
	assert.Contains(t, tmpl, "AWSTemplateFormatVersion")

	_, err = b.GetFlowLogsIntegrationTemplate("fl-missing", "arn:aws:s3:::x")
	require.ErrorIs(t, err, ec2.ErrFlowLogNotFound)

	_, err = b.GetFlowLogsIntegrationTemplate("", "arn:aws:s3:::x")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = b.GetFlowLogsIntegrationTemplate(fls[0].FlowLogID, "")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

// ---- Misc singletons ----

func TestParityFinal_GetSpotPlacementScores(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	scores, err := b.GetSpotPlacementScores([]string{"m5.large", "m5.xlarge"}, nil, false)
	require.NoError(t, err)
	require.NotEmpty(t, scores)

	for _, s := range scores {
		assert.NotEmpty(t, s.Region)
		assert.Empty(t, s.AvailabilityZoneID)
		assert.GreaterOrEqual(t, s.Score, int32(1))
		assert.LessOrEqual(t, s.Score, int32(10))
	}

	// Deterministic: same inputs produce the same scores.
	again, err := b.GetSpotPlacementScores([]string{"m5.large", "m5.xlarge"}, nil, false)
	require.NoError(t, err)
	assert.Equal(t, scores, again)

	azScores, err := b.GetSpotPlacementScores([]string{"m5.large"}, []string{"us-east-1"}, true)
	require.NoError(t, err)
	require.NotEmpty(t, azScores)

	for _, s := range azScores {
		assert.Empty(t, s.Region)
		assert.NotEmpty(t, s.AvailabilityZoneID)
	}

	_, err = b.GetSpotPlacementScores(nil, nil, false)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestParityFinal_SendDiagnosticInterrupt(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	instances, err := b.RunInstances("ami-test", "t3.micro", "", 1)
	require.NoError(t, err)

	require.NoError(t, b.SendDiagnosticInterrupt(instances[0].ID))
	require.ErrorIs(t, b.SendDiagnosticInterrupt("i-missing"), ec2.ErrInstanceNotFound)
	require.ErrorIs(t, b.SendDiagnosticInterrupt(""), ec2.ErrInvalidParameter)
}

func TestParityFinal_EnableReachabilityAnalyzerOrganizationSharing(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	assert.True(t, b.EnableReachabilityAnalyzerOrganizationSharing())
}

func TestParityFinal_DescribeElasticGpus(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	assert.Empty(t, b.DescribeElasticGpus(nil))
}
