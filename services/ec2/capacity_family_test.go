package ec2_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestCapacityReservationFleet_CreateDescribeModifyCancel(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	specs := []ec2.CapacityReservationFleetInstanceSpec{
		{InstanceType: "m5.xlarge", Priority: 1, Weight: 1},
		{InstanceType: "m5.2xlarge", Priority: 2, Weight: 2},
	}

	fleet, err := bk.CreateCapacityReservationFleet(specs, 10, "", "", "", nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, fleet.CapacityReservationFleetID)
	assert.Equal(t, "prioritized", fleet.AllocationStrategy)
	assert.Equal(t, "open", fleet.InstanceMatchCriteria)
	assert.Equal(t, "default", fleet.Tenancy)
	assert.Equal(t, "active", fleet.State)
	assert.Equal(t, int32(10), fleet.TotalTargetCapacity)
	assert.InDelta(t, float64(10), fleet.TotalFulfilledCapacity, 0.001)
	require.Len(t, fleet.InstanceTypeSpecifications, 2)
	// The lowest-priority (priority 1) spec should absorb all of the target
	// capacity since weight=1 covers it exactly.
	assert.Equal(t, int32(10), fleet.InstanceTypeSpecifications[0].TotalInstanceCount)
	assert.NotEmpty(t, fleet.InstanceTypeSpecifications[0].CapacityReservationID)
	assert.Equal(t, int32(0), fleet.InstanceTypeSpecifications[1].TotalInstanceCount)

	// The backing Capacity Reservation must have been created for real.
	crs := bk.DescribeCapacityReservations([]string{fleet.InstanceTypeSpecifications[0].CapacityReservationID})
	require.Len(t, crs, 1)
	assert.Equal(t, 10, crs[0].TotalInstanceCount)

	// Describe by ID.
	found := bk.DescribeCapacityReservationFleets([]string{fleet.CapacityReservationFleetID}, nil)
	require.Len(t, found, 1)
	assert.Equal(t, fleet.CapacityReservationFleetID, found[0].CapacityReservationFleetID)

	// Describe with a non-matching filter excludes it.
	none := bk.DescribeCapacityReservationFleets(nil, map[string][]string{"state": {"cancelled"}})
	assert.Empty(t, none)

	// Modify total target capacity resizes the primary spec and its CR.
	newCap := int32(20)
	require.NoError(t, bk.ModifyCapacityReservationFleet(fleet.CapacityReservationFleetID, &newCap, nil, false))

	updated := bk.DescribeCapacityReservationFleets([]string{fleet.CapacityReservationFleetID}, nil)
	require.Len(t, updated, 1)
	assert.Equal(t, int32(20), updated[0].TotalTargetCapacity)
	assert.Equal(t, int32(20), updated[0].InstanceTypeSpecifications[0].TotalInstanceCount)

	resizedCR := bk.DescribeCapacityReservations([]string{fleet.InstanceTypeSpecifications[0].CapacityReservationID})
	require.Len(t, resizedCR, 1)
	assert.Equal(t, 20, resizedCR[0].TotalInstanceCount)

	// Cancel: one real ID, one bogus ID.
	successful, failed := bk.CancelCapacityReservationFleets([]string{fleet.CapacityReservationFleetID, "crf-bogus"})
	require.Len(t, successful, 1)
	assert.Equal(t, "active", successful[0].PreviousState)
	assert.Equal(t, "cancelled", successful[0].CurrentState)
	require.Len(t, failed, 1)
	assert.Equal(t, "crf-bogus", failed[0].CapacityReservationFleetID)
	assert.NotEmpty(t, failed[0].ErrorCode)

	cancelledCR := bk.DescribeCapacityReservations([]string{fleet.InstanceTypeSpecifications[0].CapacityReservationID})
	require.Len(t, cancelledCR, 1)
	assert.Equal(t, "cancelled", cancelledCR[0].State)
}

func TestCapacityReservationFleet_CreateValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		specs   []ec2.CapacityReservationFleetInstanceSpec
		total   int32
	}{
		{
			name:    "no specs",
			specs:   nil,
			total:   5,
			wantErr: ec2.ErrInvalidParameter,
		},
		{
			name:    "zero target capacity",
			specs:   []ec2.CapacityReservationFleetInstanceSpec{{InstanceType: "m5.xlarge"}},
			total:   0,
			wantErr: ec2.ErrInvalidParameter,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			bk := newTestBackend()
			_, err := bk.CreateCapacityReservationFleet(tc.specs, tc.total, "", "", "", nil, nil)
			require.Error(t, err)
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestCapacityReservationFleet_ModifyNotFound(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()
	err := bk.ModifyCapacityReservationFleet("crf-doesnotexist", nil, nil, false)
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrCapacityReservationFleetNotFound)
}

func TestCapacityBlock_DescribeOfferingsAndPurchase(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	offerings, err := bk.DescribeCapacityBlockOfferings("p4d.24xlarge", 24, 2)
	require.NoError(t, err)
	require.Len(t, offerings, 2)
	assert.NotEmpty(t, offerings[0].CapacityBlockOfferingID)
	assert.Equal(t, "p4d.24xlarge", offerings[0].InstanceType)
	assert.Equal(t, int32(24), offerings[0].CapacityBlockDurationHours)
	assert.Equal(t, int32(2), offerings[0].InstanceCount)
	assert.NotEmpty(t, offerings[0].UpfrontPrice)

	block, cr, err := bk.PurchaseCapacityBlock(offerings[0].CapacityBlockOfferingID, "Linux/UNIX", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, block.CapacityBlockID)
	assert.Equal(t, "active", block.State)
	require.Len(t, block.CapacityReservationIDs, 1)
	assert.Equal(t, cr.CapacityReservationID, block.CapacityReservationIDs[0])
	assert.Equal(t, "Linux/UNIX", cr.InstancePlatform)
	assert.Equal(t, 2, cr.TotalInstanceCount)

	// The offering is consumed and cannot be purchased again.
	_, _, err = bk.PurchaseCapacityBlock(offerings[0].CapacityBlockOfferingID, "", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrCapacityBlockOfferingNotFound)

	// DescribeCapacityBlocks returns the purchased block.
	blocks := bk.DescribeCapacityBlocks([]string{block.CapacityBlockID}, nil)
	require.Len(t, blocks, 1)
	assert.Equal(t, block.CapacityBlockID, blocks[0].CapacityBlockID)

	// DescribeCapacityBlockStatus reports full capacity availability.
	statuses := bk.DescribeCapacityBlockStatus(nil, nil)
	require.Len(t, statuses, 1)
	assert.Equal(t, "ok", statuses[0].InterconnectStatus)
	assert.Equal(t, int32(2), statuses[0].TotalCapacity)
	assert.Equal(t, int32(2), statuses[0].TotalAvailableCapacity)
	assert.Equal(t, int32(0), statuses[0].TotalUnavailableCapacity)
}

func TestCapacityBlock_DescribeOfferingsValidation(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	_, err := bk.DescribeCapacityBlockOfferings("", 24, 1)
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = bk.DescribeCapacityBlockOfferings("p4d.24xlarge", 0, 1)
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestCapacityBlockExtension_DescribeAndPurchase(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	offerings, err := bk.DescribeCapacityBlockOfferings("trn1.32xlarge", 24, 1)
	require.NoError(t, err)
	require.NotEmpty(t, offerings)

	block, cr, err := bk.PurchaseCapacityBlock(offerings[0].CapacityBlockOfferingID, "", nil)
	require.NoError(t, err)

	extOfferings, err := bk.DescribeCapacityBlockExtensionOfferings(cr.CapacityReservationID, 12)
	require.NoError(t, err)
	require.Len(t, extOfferings, 1)
	assert.Equal(t, cr.CapacityReservationID, extOfferings[0].CapacityReservationID)
	assert.Equal(t, int32(12), extOfferings[0].CapacityBlockExtensionDurationHours)
	// The extension should pick up where the block's current end date leaves off.
	assert.WithinDuration(t, block.EndDate, extOfferings[0].CapacityBlockExtensionStartDate, time.Second)

	ext, err := bk.PurchaseCapacityBlockExtension(
		extOfferings[0].CapacityBlockExtensionOfferingID,
		cr.CapacityReservationID,
	)
	require.NoError(t, err)
	assert.Equal(t, "payment-succeeded", ext.CapacityBlockExtensionStatus)
	assert.Equal(t, cr.CapacityReservationID, ext.CapacityReservationID)

	// The Capacity Block's end date should be extended to match.
	blocks := bk.DescribeCapacityBlocks([]string{block.CapacityBlockID}, nil)
	require.Len(t, blocks, 1)
	assert.WithinDuration(t, ext.CapacityBlockExtensionEndDate, blocks[0].EndDate, time.Second)

	// History should now show the purchased extension.
	history := bk.DescribeCapacityBlockExtensionHistory([]string{cr.CapacityReservationID}, nil)
	require.Len(t, history, 1)
	assert.Equal(t, ext.CapacityBlockExtensionOfferingID, history[0].CapacityBlockExtensionOfferingID)

	// The offering is consumed.
	_, err = bk.PurchaseCapacityBlockExtension(
		extOfferings[0].CapacityBlockExtensionOfferingID,
		cr.CapacityReservationID,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrCapacityBlockExtensionOfferingNotFound)
}

func TestCapacityBlockExtension_DescribeOfferingsReservationNotFound(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	_, err := bk.DescribeCapacityBlockExtensionOfferings("cr-doesnotexist", 12)
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrCapacityReservationNotFound)
}

func TestCapacityReservationSplittingAndMove(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	src, err := bk.CreateCapacityReservation("m5.xlarge", "us-east-1a", 10, nil)
	require.NoError(t, err)

	dst, srcAfter, err := bk.CreateCapacityReservationBySplitting(src.CapacityReservationID, 4, nil)
	require.NoError(t, err)
	assert.Equal(t, 4, dst.TotalInstanceCount)
	assert.Equal(t, 6, srcAfter.TotalInstanceCount)
	assert.Equal(t, src.InstanceType, dst.InstanceType)
	assert.Equal(t, src.AvailabilityZone, dst.AvailabilityZone)

	// Move 2 instances from the (now 6-instance) source into the destination.
	dst2, src2, err := bk.MoveCapacityReservationInstances(src.CapacityReservationID, dst.CapacityReservationID, 2)
	require.NoError(t, err)
	assert.Equal(t, 6, dst2.TotalInstanceCount)
	assert.Equal(t, 4, src2.TotalInstanceCount)

	// Splitting more than available fails.
	_, _, err = bk.CreateCapacityReservationBySplitting(src.CapacityReservationID, 100, nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrCapacityReservationFull)

	// Moving from/to an unknown reservation fails.
	_, _, err = bk.MoveCapacityReservationInstances("cr-bogus", dst.CapacityReservationID, 1)
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrCapacityReservationNotFound)

	_, _, err = bk.MoveCapacityReservationInstances(src.CapacityReservationID, "cr-bogus", 1)
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrCapacityReservationNotFound)
}

func TestCapacityReservationBillingOwner_Lifecycle(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	cr, err := bk.CreateCapacityReservation("m5.xlarge", "us-east-1a", 1, nil)
	require.NoError(t, err)

	require.NoError(t, bk.AssociateCapacityReservationBillingOwner(cr.CapacityReservationID, "111111111111"))

	requests := bk.DescribeCapacityReservationBillingRequests([]string{cr.CapacityReservationID}, nil)
	require.Len(t, requests, 1)
	assert.Equal(t, "pending", requests[0].Status)
	assert.Equal(t, "111111111111", requests[0].UnusedReservationBillingOwnerID)

	// Wrong owner ID cannot disassociate.
	err = bk.DisassociateCapacityReservationBillingOwner(cr.CapacityReservationID, "222222222222")
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrCapacityReservationBillingRequestNotFound)

	require.NoError(t, bk.DisassociateCapacityReservationBillingOwner(cr.CapacityReservationID, "111111111111"))

	requests = bk.DescribeCapacityReservationBillingRequests(nil, map[string][]string{"status": {"revoked"}})
	require.Len(t, requests, 1)
	assert.Equal(t, "revoked", requests[0].Status)

	// Re-associate then reject.
	require.NoError(t, bk.AssociateCapacityReservationBillingOwner(cr.CapacityReservationID, "111111111111"))
	require.NoError(t, bk.RejectCapacityReservationBillingOwnership(cr.CapacityReservationID))

	requests = bk.DescribeCapacityReservationBillingRequests([]string{cr.CapacityReservationID}, nil)
	require.Len(t, requests, 1)
	assert.Equal(t, "rejected", requests[0].Status)

	err = bk.RejectCapacityReservationBillingOwnership("cr-doesnotexist")
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrCapacityReservationBillingRequestNotFound)
}

func TestCapacityManager_EnableDisableAttributes(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	attrs := bk.GetCapacityManagerAttributes()
	assert.Equal(t, "disabled", attrs.Status)
	assert.False(t, attrs.OrganizationsAccess)

	status, access := bk.EnableCapacityManager(true)
	assert.Equal(t, "enabled", status)
	assert.True(t, access)

	attrs = bk.GetCapacityManagerAttributes()
	assert.Equal(t, "enabled", attrs.Status)
	assert.True(t, attrs.OrganizationsAccess)
	assert.NotEmpty(t, attrs.IngestionStatus)

	status, access = bk.UpdateCapacityManagerOrganizationsAccess(false)
	assert.Equal(t, "enabled", status)
	assert.False(t, access)

	status, access = bk.DisableCapacityManager()
	assert.Equal(t, "disabled", status)
	assert.False(t, access)

	// No ingested metrics/dimensions are ever simulated, but the calls succeed
	// and return an empty, well-formed result set.
	assert.Empty(t, bk.GetCapacityManagerMetricData())
	assert.Empty(t, bk.GetCapacityManagerMetricDimensions())
}

func TestCapacityManager_DataExportCRUD(t *testing.T) {
	t.Parallel()

	bk := newTestBackend()

	_, err := bk.CreateCapacityManagerDataExport("", "my-bucket", "", "hourly", nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	export, err := bk.CreateCapacityManagerDataExport("parquet", "my-bucket", "prefix/", "hourly", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, export.CapacityManagerDataExportID)

	attrs := bk.GetCapacityManagerAttributes()
	assert.Equal(t, int32(1), attrs.DataExportCount)

	exports := bk.DescribeCapacityManagerDataExports(nil)
	require.Len(t, exports, 1)
	assert.Equal(t, "my-bucket", exports[0].S3BucketName)

	id, err := bk.DeleteCapacityManagerDataExport(export.CapacityManagerDataExportID)
	require.NoError(t, err)
	assert.Equal(t, export.CapacityManagerDataExportID, id)

	assert.Empty(t, bk.DescribeCapacityManagerDataExports(nil))

	_, err = bk.DeleteCapacityManagerDataExport(export.CapacityManagerDataExportID)
	require.Error(t, err)
	require.ErrorIs(t, err, ec2.ErrCapacityManagerDataExportNotFound)
}

func TestCapacityFamily_SnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	original := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	fleet, err := original.CreateCapacityReservationFleet(
		[]ec2.CapacityReservationFleetInstanceSpec{{InstanceType: "m5.xlarge", Weight: 1}},
		4, "", "", "", nil, nil,
	)
	require.NoError(t, err)

	offerings, err := original.DescribeCapacityBlockOfferings("p4d.24xlarge", 24, 1)
	require.NoError(t, err)
	block, cr, err := original.PurchaseCapacityBlock(offerings[0].CapacityBlockOfferingID, "Linux/UNIX", nil)
	require.NoError(t, err)

	require.NoError(t, original.AssociateCapacityReservationBillingOwner(cr.CapacityReservationID, "111111111111"))

	export, err := original.CreateCapacityManagerDataExport("parquet", "my-bucket", "", "hourly", nil)
	require.NoError(t, err)

	_, access := original.EnableCapacityManager(true)
	assert.True(t, access)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	restored := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(t.Context(), snap))

	fleets := restored.DescribeCapacityReservationFleets([]string{fleet.CapacityReservationFleetID}, nil)
	require.Len(t, fleets, 1)
	assert.Equal(t, int32(4), fleets[0].TotalTargetCapacity)

	blocks := restored.DescribeCapacityBlocks([]string{block.CapacityBlockID}, nil)
	require.Len(t, blocks, 1)

	requests := restored.DescribeCapacityReservationBillingRequests([]string{cr.CapacityReservationID}, nil)
	require.Len(t, requests, 1)
	assert.Equal(t, "pending", requests[0].Status)

	exports := restored.DescribeCapacityManagerDataExports([]string{export.CapacityManagerDataExportID})
	require.Len(t, exports, 1)

	attrs := restored.GetCapacityManagerAttributes()
	assert.Equal(t, "enabled", attrs.Status)
	assert.True(t, attrs.OrganizationsAccess)
}

func TestInterruptibleCapacityReservationAllocation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	cr, err := b.CreateCapacityReservation("m5.large", "us-east-1a", 10, nil)
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

func TestUpdateInterruptibleAllocationNotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	cr, err := b.CreateCapacityReservation("m5.large", "us-east-1a", 10, nil)
	require.NoError(t, err)

	_, err = b.UpdateInterruptibleCapacityReservationAllocation(cr.CapacityReservationID, 5)
	require.ErrorIs(t, err, ec2.ErrInterruptibleAllocationNotFound)
}

func TestGetCapacityReservationUsage(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	cr, err := b.CreateCapacityReservation("m5.large", "us-east-1a", 10, nil)
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

func TestDescribeCapacityReservationTopology(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	cr, err := b.CreateCapacityReservation("m5.large", "us-east-1a", 10, nil)
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
