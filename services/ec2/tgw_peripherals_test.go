package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Transit Gateway Policy Tables ----

func TestTGWPeripherals_PolicyTableLifecycle(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	tests := []struct {
		name    string
		tgwID   string
		wantErr bool
	}{
		{name: "empty transit gateway id", tgwID: "", wantErr: true},
		{name: "unknown transit gateway id", tgwID: "tgw-nonexistent", wantErr: true},
		{name: "valid transit gateway id", tgwID: tgw.ID, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pt, createErr := bk.CreateTransitGatewayPolicyTable(tt.tgwID)
			if tt.wantErr {
				require.Error(t, createErr)
				assert.Nil(t, pt)

				return
			}

			require.NoError(t, createErr)
			require.NotNil(t, pt)
			assert.Contains(t, pt.TransitGatewayPolicyTableID, "tgw-ptb-")
			assert.Equal(t, tt.tgwID, pt.TransitGatewayID)
			assert.Equal(t, "available", pt.State)
		})
	}

	pt, err := bk.CreateTransitGatewayPolicyTable(tgw.ID)
	require.NoError(t, err)

	described := bk.DescribeTransitGatewayPolicyTables([]string{pt.TransitGatewayPolicyTableID})
	require.Len(t, described, 1)
	assert.Equal(t, pt.TransitGatewayPolicyTableID, described[0].TransitGatewayPolicyTableID)

	// Unfiltered describe returns at least the tables we created.
	allDescribed := bk.DescribeTransitGatewayPolicyTables(nil)
	assert.GreaterOrEqual(t, len(allDescribed), 1)

	require.NoError(t, bk.DeleteTransitGatewayPolicyTable(pt.TransitGatewayPolicyTableID))
	assert.Empty(t, bk.DescribeTransitGatewayPolicyTables([]string{pt.TransitGatewayPolicyTableID}))

	err = bk.DeleteTransitGatewayPolicyTable(pt.TransitGatewayPolicyTableID)
	require.ErrorIs(t, err, ec2.ErrTGWPolicyTableNotFound)
}

func TestTGWPeripherals_PolicyTableAssociations(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	pt, err := bk.CreateTransitGatewayPolicyTable(tgw.ID)
	require.NoError(t, err)

	_, err = bk.AssociateTransitGatewayPolicyTable("tgw-ptb-nonexistent", "tgw-attach-1")
	require.ErrorIs(t, err, ec2.ErrTGWPolicyTableNotFound)

	assoc, err := bk.AssociateTransitGatewayPolicyTable(pt.TransitGatewayPolicyTableID, "tgw-attach-1")
	require.NoError(t, err)
	assert.Equal(t, "associated", assoc.State)
	assert.Equal(t, "vpc", assoc.ResourceType)
	assert.Equal(t, "tgw-attach-1", assoc.TransitGatewayAttachmentID)

	assocs := bk.GetTransitGatewayPolicyTableAssociations(pt.TransitGatewayPolicyTableID)
	require.Len(t, assocs, 1)
	assert.Equal(t, "tgw-attach-1", assocs[0].TransitGatewayAttachmentID)

	// Filtering by an unrelated policy table ID returns nothing.
	assert.Empty(t, bk.GetTransitGatewayPolicyTableAssociations("tgw-ptb-other"))

	disassoc, err := bk.DisassociateTransitGatewayPolicyTable(
		pt.TransitGatewayPolicyTableID,
		"tgw-attach-1",
	)
	require.NoError(t, err)
	assert.Equal(t, "disassociated", disassoc.State)
	assert.Empty(t, bk.GetTransitGatewayPolicyTableAssociations(pt.TransitGatewayPolicyTableID))

	_, err = bk.DisassociateTransitGatewayPolicyTable(pt.TransitGatewayPolicyTableID, "tgw-attach-1")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	// Deleting the policy table cascades to its associations.
	_, err = bk.AssociateTransitGatewayPolicyTable(pt.TransitGatewayPolicyTableID, "tgw-attach-2")
	require.NoError(t, err)
	require.NoError(t, bk.DeleteTransitGatewayPolicyTable(pt.TransitGatewayPolicyTableID))
	assert.Empty(t, bk.GetTransitGatewayPolicyTableAssociations(pt.TransitGatewayPolicyTableID))
}

func TestTGWPeripherals_PolicyTableEntriesValidation(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	_, err := bk.GetTransitGatewayPolicyTableEntries("")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.GetTransitGatewayPolicyTableEntries("tgw-ptb-nonexistent")
	require.ErrorIs(t, err, ec2.ErrTGWPolicyTableNotFound)

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	pt, err := bk.CreateTransitGatewayPolicyTable(tgw.ID)
	require.NoError(t, err)

	entries, err := bk.GetTransitGatewayPolicyTableEntries(pt.TransitGatewayPolicyTableID)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestTGWPeripherals_PolicyTableEntryLifecycle(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	pt, err := bk.CreateTransitGatewayPolicyTable(tgw.ID)
	require.NoError(t, err)

	rt, err := bk.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)

	otherRT, err := bk.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)

	// Missing/invalid required fields.
	_, err = bk.CreateTransitGatewayPolicyTableEntry(
		"", &ec2.TransitGatewayPolicyTableEntry{PolicyRuleNumber: 100, TargetRouteTableID: rt.RouteTableID},
	)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.CreateTransitGatewayPolicyTableEntry(
		pt.TransitGatewayPolicyTableID,
		&ec2.TransitGatewayPolicyTableEntry{TargetRouteTableID: rt.RouteTableID},
	)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.CreateTransitGatewayPolicyTableEntry(
		pt.TransitGatewayPolicyTableID,
		&ec2.TransitGatewayPolicyTableEntry{PolicyRuleNumber: 100},
	)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	// Unknown policy table / unknown target route table.
	_, err = bk.CreateTransitGatewayPolicyTableEntry(
		"tgw-ptb-nonexistent",
		&ec2.TransitGatewayPolicyTableEntry{PolicyRuleNumber: 100, TargetRouteTableID: rt.RouteTableID},
	)
	require.ErrorIs(t, err, ec2.ErrTGWPolicyTableNotFound)

	_, err = bk.CreateTransitGatewayPolicyTableEntry(
		pt.TransitGatewayPolicyTableID,
		&ec2.TransitGatewayPolicyTableEntry{PolicyRuleNumber: 100, TargetRouteTableID: "tgw-rtb-nonexistent"},
	)
	require.ErrorIs(t, err, ec2.ErrTGWRouteTableNotFound)

	// Create.
	entry, err := bk.CreateTransitGatewayPolicyTableEntry(
		pt.TransitGatewayPolicyTableID,
		&ec2.TransitGatewayPolicyTableEntry{
			PolicyRuleNumber:     100,
			TargetRouteTableID:   rt.RouteTableID,
			SourceCidrBlock:      "10.0.0.0/16",
			DestinationCidrBlock: "10.1.0.0/16",
			Protocol:             "6",
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "active", entry.State)
	assert.Equal(t, pt.TransitGatewayPolicyTableID, entry.TransitGatewayPolicyTableID)

	// Visible via Describe/Get.
	entries, err := bk.GetTransitGatewayPolicyTableEntries(pt.TransitGatewayPolicyTableID)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "10.0.0.0/16", entries[0].SourceCidrBlock)

	// Duplicate rule number on the same table overwrites (Put is keyed by
	// table+rule number, matching real AWS "one entry per rule number").
	_, err = bk.CreateTransitGatewayPolicyTableEntry(
		pt.TransitGatewayPolicyTableID,
		&ec2.TransitGatewayPolicyTableEntry{PolicyRuleNumber: 100, TargetRouteTableID: otherRT.RouteTableID},
	)
	require.NoError(t, err)

	entries, err = bk.GetTransitGatewayPolicyTableEntries(pt.TransitGatewayPolicyTableID)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, otherRT.RouteTableID, entries[0].TargetRouteTableID)

	// Modify: unset fields retain their current value.
	modified, err := bk.ModifyTransitGatewayPolicyTableEntry(
		pt.TransitGatewayPolicyTableID,
		100,
		&ec2.TransitGatewayPolicyTableEntry{TargetRouteTableID: rt.RouteTableID},
	)
	require.NoError(t, err)
	assert.Equal(t, rt.RouteTableID, modified.TargetRouteTableID)

	// Modify: unknown target route table is rejected without mutating state.
	_, err = bk.ModifyTransitGatewayPolicyTableEntry(
		pt.TransitGatewayPolicyTableID,
		100,
		&ec2.TransitGatewayPolicyTableEntry{TargetRouteTableID: "tgw-rtb-nonexistent"},
	)
	require.ErrorIs(t, err, ec2.ErrTGWRouteTableNotFound)

	entries, err = bk.GetTransitGatewayPolicyTableEntries(pt.TransitGatewayPolicyTableID)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, rt.RouteTableID, entries[0].TargetRouteTableID)

	// Modify: unknown rule number / unknown policy table.
	_, err = bk.ModifyTransitGatewayPolicyTableEntry(
		pt.TransitGatewayPolicyTableID,
		999,
		&ec2.TransitGatewayPolicyTableEntry{TargetRouteTableID: rt.RouteTableID},
	)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.ModifyTransitGatewayPolicyTableEntry(
		"tgw-ptb-nonexistent",
		100,
		&ec2.TransitGatewayPolicyTableEntry{TargetRouteTableID: rt.RouteTableID},
	)
	require.ErrorIs(t, err, ec2.ErrTGWPolicyTableNotFound)

	// Delete: unknown rule number / unknown policy table.
	_, err = bk.DeleteTransitGatewayPolicyTableEntry(pt.TransitGatewayPolicyTableID, 999)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.DeleteTransitGatewayPolicyTableEntry("tgw-ptb-nonexistent", 100)
	require.ErrorIs(t, err, ec2.ErrTGWPolicyTableNotFound)

	deleted, err := bk.DeleteTransitGatewayPolicyTableEntry(pt.TransitGatewayPolicyTableID, 100)
	require.NoError(t, err)
	assert.Equal(t, "deleted", deleted.State)

	entries, err = bk.GetTransitGatewayPolicyTableEntries(pt.TransitGatewayPolicyTableID)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestTGWPeripherals_PolicyTableEntrySnapshotRestore(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	pt, err := bk.CreateTransitGatewayPolicyTable(tgw.ID)
	require.NoError(t, err)

	rt, err := bk.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)

	_, err = bk.CreateTransitGatewayPolicyTableEntry(
		pt.TransitGatewayPolicyTableID,
		&ec2.TransitGatewayPolicyTableEntry{
			PolicyRuleNumber:     42,
			TargetRouteTableID:   rt.RouteTableID,
			SourceCidrBlock:      "192.168.0.0/16",
			DestinationCidrBlock: "172.16.0.0/12",
			Protocol:             "17",
		},
	)
	require.NoError(t, err)

	snap := bk.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	entries, err := restored.GetTransitGatewayPolicyTableEntries(pt.TransitGatewayPolicyTableID)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, 42, entries[0].PolicyRuleNumber)
	assert.Equal(t, rt.RouteTableID, entries[0].TargetRouteTableID)
	assert.Equal(t, "192.168.0.0/16", entries[0].SourceCidrBlock)
	assert.Equal(t, "172.16.0.0/12", entries[0].DestinationCidrBlock)
	assert.Equal(t, "17", entries[0].Protocol)
	assert.Equal(t, "active", entries[0].State)
}

func TestTGWPeripherals_DeletePolicyTableCascadesEntries(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	pt, err := bk.CreateTransitGatewayPolicyTable(tgw.ID)
	require.NoError(t, err)

	rt, err := bk.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)

	_, err = bk.CreateTransitGatewayPolicyTableEntry(
		pt.TransitGatewayPolicyTableID,
		&ec2.TransitGatewayPolicyTableEntry{PolicyRuleNumber: 1, TargetRouteTableID: rt.RouteTableID},
	)
	require.NoError(t, err)

	require.NoError(t, bk.DeleteTransitGatewayPolicyTable(pt.TransitGatewayPolicyTableID))

	// The policy table itself is gone, so Get now reports NotFound rather
	// than an empty entries list.
	_, err = bk.GetTransitGatewayPolicyTableEntries(pt.TransitGatewayPolicyTableID)
	require.ErrorIs(t, err, ec2.ErrTGWPolicyTableNotFound)
}

// ---- Transit Gateway Route Table Announcements ----

func TestTGWPeripherals_RouteTableAnnouncementLifecycle(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	rt, err := bk.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)

	peerAtt, err := bk.CreateTransitGatewayPeeringAttachment(tgw.ID, "tgw-peer-1", "")
	require.NoError(t, err)

	_, err = bk.CreateTransitGatewayRouteTableAnnouncement("tgw-rtb-nonexistent", peerAtt.TransitGatewayAttachmentID)
	require.ErrorIs(t, err, ec2.ErrTGWRouteTableNotFound)

	_, err = bk.CreateTransitGatewayRouteTableAnnouncement(rt.RouteTableID, "tgw-attach-nonexistent")
	require.ErrorIs(t, err, ec2.ErrTransitGatewayAttachmentNotFound)

	ann, err := bk.CreateTransitGatewayRouteTableAnnouncement(
		rt.RouteTableID,
		peerAtt.TransitGatewayAttachmentID,
	)
	require.NoError(t, err)
	assert.Contains(t, ann.TransitGatewayRouteTableAnnouncementID, "tgw-rtb-ann-")
	assert.Equal(t, "available", ann.State)
	assert.Equal(t, "outgoing", ann.AnnouncementDirection)
	assert.Equal(t, tgw.ID, ann.TransitGatewayID)
	assert.Equal(t, rt.RouteTableID, ann.TransitGatewayRouteTableID)
	assert.Equal(t, peerAtt.TransitGatewayAttachmentID, ann.PeeringAttachmentID)

	described := bk.DescribeTransitGatewayRouteTableAnnouncements(
		[]string{ann.TransitGatewayRouteTableAnnouncementID},
	)
	require.Len(t, described, 1)
	assert.Equal(t, ann.TransitGatewayRouteTableAnnouncementID, described[0].TransitGatewayRouteTableAnnouncementID)

	require.NoError(t, bk.DeleteTransitGatewayRouteTableAnnouncement(ann.TransitGatewayRouteTableAnnouncementID))
	assert.Empty(t, bk.DescribeTransitGatewayRouteTableAnnouncements(
		[]string{ann.TransitGatewayRouteTableAnnouncementID},
	))

	err = bk.DeleteTransitGatewayRouteTableAnnouncement(ann.TransitGatewayRouteTableAnnouncementID)
	require.ErrorIs(t, err, ec2.ErrTGWRouteTableAnnouncementNotFound)
}

// ---- Route table associations / propagations / search / export ----

func TestTGWPeripherals_GetRouteTableAssociations(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	rt, err := bk.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)

	_, err = bk.GetTransitGatewayRouteTableAssociations("tgw-rtb-nonexistent")
	require.ErrorIs(t, err, ec2.ErrTGWRouteTableNotFound)

	assocs, err := bk.GetTransitGatewayRouteTableAssociations(rt.RouteTableID)
	require.NoError(t, err)
	assert.Empty(t, assocs)

	att, err := bk.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-assoctest", nil)
	require.NoError(t, err)

	_, err = bk.AssociateTransitGatewayRouteTable(rt.RouteTableID, att.TransitGatewayAttachmentID)
	require.NoError(t, err)

	assocs, err = bk.GetTransitGatewayRouteTableAssociations(rt.RouteTableID)
	require.NoError(t, err)
	require.Len(t, assocs, 1)
	assert.Equal(t, att.TransitGatewayAttachmentID, assocs[0].TransitGatewayAttachmentID)
	assert.Equal(t, "vpc", assocs[0].ResourceType)
}

func TestTGWPeripherals_GetRouteTablePropagationsAlwaysEmpty(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	_, err := bk.GetTransitGatewayRouteTablePropagations("tgw-rtb-nonexistent")
	require.ErrorIs(t, err, ec2.ErrTGWRouteTableNotFound)

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	rt, err := bk.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)

	props, err := bk.GetTransitGatewayRouteTablePropagations(rt.RouteTableID)
	require.NoError(t, err)
	assert.Empty(t, props)
}

func TestTGWPeripherals_GetAttachmentPropagationsAlwaysEmpty(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	_, err = bk.GetTransitGatewayAttachmentPropagations("tgw-attach-nonexistent")
	require.ErrorIs(t, err, ec2.ErrTGWAttachmentNotFound)

	att, err := bk.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-default", nil)
	require.NoError(t, err)

	props, err := bk.GetTransitGatewayAttachmentPropagations(att.TransitGatewayAttachmentID)
	require.NoError(t, err)
	assert.Empty(t, props)
}

func TestTGWPeripherals_SearchTransitGatewayRoutes(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	rt, err := bk.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)

	// CreateTransitGatewayRoute validates the attachment exists (see
	// TestEC2Core_TransitGatewayRouteTables), so use real attachments rather
	// than fabricated "tgw-attach-N" strings.
	att1, err := bk.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-search1", nil)
	require.NoError(t, err)
	att2, err := bk.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-search2", nil)
	require.NoError(t, err)

	_, err = bk.CreateTransitGatewayRoute(rt.RouteTableID, "10.0.0.0/24", att1.TransitGatewayAttachmentID, false)
	require.NoError(t, err)
	_, err = bk.CreateTransitGatewayRoute(rt.RouteTableID, "10.0.1.0/24", att2.TransitGatewayAttachmentID, false)
	require.NoError(t, err)

	_, err = bk.SearchTransitGatewayRoutes("tgw-rtb-nonexistent", nil)
	require.ErrorIs(t, err, ec2.ErrTGWRouteTableNotFound)

	all, err := bk.SearchTransitGatewayRoutes(rt.RouteTableID, nil)
	require.NoError(t, err)
	require.Len(t, all, 2)

	exact, err := bk.SearchTransitGatewayRoutes(rt.RouteTableID, map[string][]string{
		"route-search.exact-match": {"10.0.0.0/24"},
	})
	require.NoError(t, err)
	require.Len(t, exact, 1)
	assert.Equal(t, "10.0.0.0/24", exact[0].DestinationCidrBlock)

	byAttachment, err := bk.SearchTransitGatewayRoutes(rt.RouteTableID, map[string][]string{
		"attachment.transit-gateway-attachment-id": {att2.TransitGatewayAttachmentID},
	})
	require.NoError(t, err)
	require.Len(t, byAttachment, 1)
	assert.Equal(t, "10.0.1.0/24", byAttachment[0].DestinationCidrBlock)

	noMatch, err := bk.SearchTransitGatewayRoutes(rt.RouteTableID, map[string][]string{
		"route-search.exact-match": {"10.9.9.0/24"},
	})
	require.NoError(t, err)
	assert.Empty(t, noMatch)
}

func TestTGWPeripherals_ExportTransitGatewayRoutes(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	rt, err := bk.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)

	_, err = bk.ExportTransitGatewayRoutes("", "my-bucket")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.ExportTransitGatewayRoutes(rt.RouteTableID, "")
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.ExportTransitGatewayRoutes("tgw-rtb-nonexistent", "my-bucket")
	require.ErrorIs(t, err, ec2.ErrTGWRouteTableNotFound)

	loc, err := bk.ExportTransitGatewayRoutes(rt.RouteTableID, "my-bucket")
	require.NoError(t, err)
	assert.Equal(
		t,
		"s3://my-bucket/VPCTransitGateway/TransitGatewayRouteTables/"+rt.RouteTableID+".csv",
		loc,
	)
}

// ---- Modifications ----

func TestTGWPeripherals_ModifyVpcAttachment(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	att, err := bk.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-default", nil)
	require.NoError(t, err)

	_, err = bk.ModifyTransitGatewayVpcAttachment("tgw-attach-nonexistent", nil, nil)
	require.ErrorIs(t, err, ec2.ErrTGWAttachmentNotFound)

	updated, err := bk.ModifyTransitGatewayVpcAttachment(
		att.TransitGatewayAttachmentID,
		[]string{"subnet-1", "subnet-2"},
		nil,
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"subnet-1", "subnet-2"}, updated.SubnetIDs)

	updated, err = bk.ModifyTransitGatewayVpcAttachment(
		att.TransitGatewayAttachmentID,
		[]string{"subnet-3"},
		[]string{"subnet-1"},
	)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"subnet-2", "subnet-3"}, updated.SubnetIDs)
}

func TestTGWPeripherals_ModifyMeteringPolicyAndGetEntries(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	policy, err := bk.CreateTransitGatewayMeteringPolicy(tgw.ID, []string{"tgw-attach-1"}, nil)
	require.NoError(t, err)

	_, err = bk.ModifyTransitGatewayMeteringPolicy("tgw-metering-policy-nonexistent", nil, nil)
	require.ErrorIs(t, err, ec2.ErrTGWMeteringPolicyNotFound)

	updated, err := bk.ModifyTransitGatewayMeteringPolicy(
		policy.ID,
		[]string{"tgw-attach-2"},
		[]string{"tgw-attach-1"},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"tgw-attach-2"}, updated.MiddleboxAttachmentIDs)

	_, err = bk.GetTransitGatewayMeteringPolicyEntries("tgw-metering-policy-nonexistent")
	require.ErrorIs(t, err, ec2.ErrTGWMeteringPolicyNotFound)

	entries, err := bk.GetTransitGatewayMeteringPolicyEntries(policy.ID)
	require.NoError(t, err)
	assert.Empty(t, entries)

	_, err = bk.CreateTransitGatewayMeteringPolicyEntry(policy.ID, &ec2.TransitGatewayMeteringPolicyEntry{
		PolicyRuleNumber:     1,
		DestinationCidrBlock: "10.0.0.0/24",
	})
	require.NoError(t, err)

	entries, err = bk.GetTransitGatewayMeteringPolicyEntries(policy.ID)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, 1, entries[0].PolicyRuleNumber)
}

func TestTGWPeripherals_ModifyPrefixListReference(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	ref, err := bk.CreateTransitGatewayPrefixListReference("tgw-rtb-111", "pl-abc123", false)
	require.NoError(t, err)
	assert.False(t, ref.Blackhole)

	_, err = bk.ModifyTransitGatewayPrefixListReference("tgw-rtb-999", "pl-nonexistent", "", true)
	require.ErrorIs(t, err, ec2.ErrTGWPrefixListRefNotFound)

	updated, err := bk.ModifyTransitGatewayPrefixListReference(
		"tgw-rtb-111",
		"pl-abc123",
		"tgw-attach-1",
		true,
	)
	require.NoError(t, err)
	assert.True(t, updated.Blackhole)
	assert.Equal(t, "tgw-attach-1", updated.TransitGatewayAttachmentID)

	refs, err := bk.GetTransitGatewayPrefixListReferences("tgw-rtb-111")
	require.NoError(t, err)
	require.Len(t, refs, 1)
	assert.True(t, refs[0].Blackhole)
}

// ---- Rejections ----

func TestTGWPeripherals_RejectVpcAttachment(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	att, err := bk.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-default", nil)
	require.NoError(t, err)

	_, err = bk.RejectTransitGatewayVpcAttachment("tgw-attach-nonexistent")
	require.ErrorIs(t, err, ec2.ErrTransitGatewayAttachmentNotFound)

	rejected, err := bk.RejectTransitGatewayVpcAttachment(att.TransitGatewayAttachmentID)
	require.NoError(t, err)
	assert.Equal(t, "rejected", rejected.State)

	described := bk.DescribeTransitGatewayVpcAttachments([]string{att.TransitGatewayAttachmentID})
	require.Len(t, described, 1)
	assert.Equal(t, "rejected", described[0].State)
}

func TestTGWPeripherals_RejectPeeringAttachment(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	att, err := bk.CreateTransitGatewayPeeringAttachment(tgw.ID, "tgw-peer-1", "")
	require.NoError(t, err)

	_, err = bk.RejectTransitGatewayPeeringAttachment("tgw-attach-nonexistent")
	require.ErrorIs(t, err, ec2.ErrTransitGatewayAttachmentNotFound)

	rejected, err := bk.RejectTransitGatewayPeeringAttachment(att.TransitGatewayAttachmentID)
	require.NoError(t, err)
	assert.Equal(t, "rejected", rejected.State)
}

func TestTGWPeripherals_RejectMulticastDomainAssociations(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	_, err := bk.RejectTransitGatewayMulticastDomainAssociations("", "tgw-attach-1", []string{"subnet-1"})
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.RejectTransitGatewayMulticastDomainAssociations("tgw-mcast-domain-1", "tgw-attach-1", nil)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	tgw, err := bk.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test-tgw"})
	require.NoError(t, err)

	domain, err := bk.CreateTransitGatewayMulticastDomain(tgw.ID, "", "", "", nil)
	require.NoError(t, err)

	_, err = bk.AssociateTransitGatewayMulticastDomain(domain.ID, "tgw-attach-1", []string{"subnet-1"})
	require.NoError(t, err)

	rejected, err := bk.RejectTransitGatewayMulticastDomainAssociations(
		domain.ID,
		"tgw-attach-1",
		[]string{"subnet-1"},
	)
	require.NoError(t, err)
	require.Len(t, rejected, 1)
	assert.Equal(t, "rejected", rejected[0].State)

	remaining := bk.GetTransitGatewayMulticastDomainAssociations(domain.ID)
	assert.Empty(t, remaining)
}
