package ec2_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// ---- Reserved Instances ----.
func TestReservedInstances(t *testing.T) { //nolint:paralleltest // existing issue.
	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")

	// Seed an offering for purchase
	b.SeedReservedInstancesOffering(
		"rio-test-offering-001",
		"t3.medium",
		"us-east-1a",
		"Linux/UNIX",
		"All Upfront",
		"standard",
		94608000,
		500.0,
		0.0,
	)

	t.Run("describe offerings returns seeded offering", func(t *testing.T) { //nolint:paralleltest // existing issue.
		offerings := b.DescribeReservedInstancesOfferings("", "", "", "")
		assert.NotEmpty(t, offerings)
	})

	t.Run("describe offerings by instance type", func(t *testing.T) { //nolint:paralleltest // existing issue.
		offerings := b.DescribeReservedInstancesOfferings("t3.medium", "", "", "")
		require.Len(t, offerings, 1)
		assert.Equal(t, "t3.medium", offerings[0].InstanceType)
	})

	t.Run("describe offerings by az", func(t *testing.T) { //nolint:paralleltest // existing issue.
		offerings := b.DescribeReservedInstancesOfferings("", "us-east-1a", "", "")
		require.Len(t, offerings, 1)
	})

	t.Run("describe offerings by product description", func(t *testing.T) { //nolint:paralleltest // existing issue.
		offerings := b.DescribeReservedInstancesOfferings("", "", "Linux/UNIX", "")
		require.Len(t, offerings, 1)
	})

	t.Run("describe offerings no match", func(t *testing.T) { //nolint:paralleltest // existing issue.
		offerings := b.DescribeReservedInstancesOfferings("m5.xlarge", "", "", "")
		assert.Empty(t, offerings)
	})

	var riID string

	t.Run("purchase offering", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ri, err := b.PurchaseReservedInstancesOffering("rio-test-offering-001", 3)
		require.NoError(t, err)
		assert.NotEmpty(t, ri.ReservedInstancesID)
		assert.Equal(t, "active", ri.State)
		assert.Equal(t, 3, ri.InstanceCount)
		riID = ri.ReservedInstancesID
	})

	t.Run("describe reserved instances", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ris := b.DescribeReservedInstances(nil)
		require.Len(t, ris, 1)
		assert.Equal(t, riID, ris[0].ReservedInstancesID)
	})

	t.Run("describe reserved instances by id", func(t *testing.T) { //nolint:paralleltest // existing issue.
		ris := b.DescribeReservedInstances([]string{riID})
		require.Len(t, ris, 1)
	})

	t.Run("purchase non-existent offering returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.PurchaseReservedInstancesOffering("rio-nonexistent", 1)
		require.Error(t, err)
	})

	var listingID string

	t.Run("create listing", func(t *testing.T) { //nolint:paralleltest // existing issue.
		schedules := []ec2.PriceScheduleEntry{{CurrencyCode: "USD", Price: 12.5, Term: 2}}
		l, err := b.CreateReservedInstancesListing(riID, 2, schedules)
		require.NoError(t, err)
		assert.NotEmpty(t, l.ReservedInstancesListingID)
		assert.Equal(t, "active", l.Status)
		require.Len(t, l.PriceSchedules, 1)
		assert.True(t, l.PriceSchedules[0].Active)
		require.Len(t, l.InstanceCounts, 1)
		assert.Equal(t, 2, l.InstanceCounts[0].InstanceCount)
		listingID = l.ReservedInstancesListingID
	})

	t.Run("create listing without schedules rejected", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CreateReservedInstancesListing(riID, 2, nil)
		require.Error(t, err)
	})

	t.Run("describe listings", func(t *testing.T) { //nolint:paralleltest // existing issue.
		listings := b.DescribeReservedInstancesListings(nil)
		require.Len(t, listings, 1)
		assert.Equal(t, listingID, listings[0].ReservedInstancesListingID)
	})

	t.Run("cancel listing", func(t *testing.T) { //nolint:paralleltest // existing issue.
		cancelled, err := b.CancelReservedInstancesListing(listingID)
		require.NoError(t, err)
		assert.Equal(t, "cancelled", cancelled.Status)
		listings := b.DescribeReservedInstancesListings([]string{listingID})
		require.Len(t, listings, 1)
		assert.Equal(t, "cancelled", listings[0].Status)
	})

	t.Run("cancel non-existent listing returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		_, err := b.CancelReservedInstancesListing("rsl-nonexistent")
		require.Error(t, err)
	})

	t.Run("modify reserved instances", func(t *testing.T) { //nolint:paralleltest // existing issue.
		targets := []ec2.ReservedInstancesConfigurationTarget{{InstanceType: "t3.large", InstanceCount: 3}}
		mod, err := b.ModifyReservedInstances([]string{riID}, targets)
		require.NoError(t, err)
		assert.NotEmpty(t, mod.ReservedInstancesModificationID)
		assert.Equal(t, "fulfilled", mod.Status)
		assert.Equal(t, []string{riID}, mod.ReservedInstancesIDs)
		require.Len(t, mod.ModificationResults, 1)
		assert.Equal(t, "t3.large", mod.ModificationResults[0].TargetConfiguration.InstanceType)
	})

	t.Run("describe modifications", func(t *testing.T) { //nolint:paralleltest // existing issue.
		mods := b.DescribeReservedInstancesModifications(nil)
		assert.NotEmpty(t, mods)
	})

	t.Run("delete queued: active RI not deleted", func(t *testing.T) { //nolint:paralleltest // existing issue.
		// Real AWS only ever deletes a Reserved Instance genuinely in the
		// "queued" state; this backend never produces one (no scheduled/
		// future-dated purchase mode -- see QueuedPurchaseDeletionResult's doc
		// comment), so an existing, active RI must be reported as failed and
		// left untouched, not silently deleted.
		results := b.DeleteQueuedReservedInstances([]string{riID})
		require.Len(t, results, 1)
		assert.True(t, results[0].Failed)
		assert.Equal(t, "reserved-instances-not-in-queued-state", results[0].ErrorCode)

		ris := b.DescribeReservedInstances([]string{riID})
		require.Len(t, ris, 1)
		assert.Equal(t, "active", ris[0].State)
	})

	t.Run("delete queued: unknown ID is id-invalid", func(t *testing.T) { //nolint:paralleltest // existing issue.
		results := b.DeleteQueuedReservedInstances([]string{"r-doesnotexist"})
		require.Len(t, results, 1)
		assert.True(t, results[0].Failed)
		assert.Equal(t, "reserved-instances-id-invalid", results[0].ErrorCode)
	})
}

// TestHandler_DeleteQueuedReservedInstances verifies the real
// DeleteQueuedReservedInstancesOutput wire shape (successfulQueuedPurchaseDeletionSet/
// failedQueuedPurchaseDeletionSet of <item> entries, confirmed against the installed
// SDK's deserializers) instead of the previous bare {Return: true}, and that the
// per-ID outcome matches AWS's real "only a queued reservation can be deleted here"
// semantics.
func TestHandler_DeleteQueuedReservedInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantBody    []string
		notWantBody []string
	}{
		{
			name: "unknown id reports reserved-instances-id-invalid on the failed set",
			wantBody: []string{
				"<failedQueuedPurchaseDeletionSet><item>",
				"<reservedInstancesId>r-doesnotexist</reservedInstancesId>",
				"<code>reserved-instances-id-invalid</code>",
			},
			notWantBody: []string{"successfulQueuedPurchaseDeletionSet><item>"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			vals := url.Values{}
			vals.Set("Action", "DeleteQueuedReservedInstances")
			vals.Set("Version", "2016-11-15")
			vals.Set("ReservedInstancesId.1", "r-doesnotexist")

			rec := postForm(t, h, vals.Encode())
			assert.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			for _, want := range tt.wantBody {
				assert.Contains(t, body, want)
			}

			for _, notWant := range tt.notWantBody {
				assert.NotContains(t, body, notWant)
			}
		})
	}
}

// TestHandler_GetReservedInstancesExchangeQuote_MissingIds covers the
// required ReservedInstanceIds param (api_op_GetReservedInstancesExchangeQuote.go:
// "This member is required"), mirroring AcceptReservedInstancesExchangeQuote's
// existing validation.
func TestHandler_GetReservedInstancesExchangeQuote_MissingIds(t *testing.T) {
	t.Parallel()

	h := newHandler()

	vals := url.Values{
		"Action":  {"GetReservedInstancesExchangeQuote"},
		"Version": {"2016-11-15"},
	}

	_, err := ec2.ExportDispatch(h, vals)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "InvalidParameterValue")
}

// TestHandler_ModifyReservedInstances covers ModifyReservedInstancesInput's
// two required members (api_op_ModifyReservedInstances.go: ReservedInstancesIds,
// TargetConfigurations). Before the fix, the handler only ever read index 1 of
// the ReservedInstancesConfigurationSetItemType.N wire array and the backend
// discarded every argument outright (ModifyReservedInstances(_ []string, _
// string, _ int)), fabricating a "fulfilled" modification for any input,
// including an unknown Reserved Instance ID and a request with no target
// configuration at all.
func TestHandler_ModifyReservedInstances(t *testing.T) {
	t.Parallel()

	t.Run("missing target configuration is rejected", func(t *testing.T) {
		t.Parallel()

		bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		h := ec2.NewHandler(bk)

		bk.SeedReservedInstancesOffering(
			"rio-mri-001", "t3.large", "us-east-1a", "Linux/UNIX", "No Upfront", "standard", 31536000, 0, 0.05,
		)
		ri, err := bk.PurchaseReservedInstancesOffering("rio-mri-001", 2)
		require.NoError(t, err)

		vals := url.Values{
			"Action":                {"ModifyReservedInstances"},
			"Version":               {"2016-11-15"},
			"ReservedInstancesId.1": {ri.ReservedInstancesID},
		}

		_, err = ec2.ExportDispatch(h, vals)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "InvalidParameterValue")
	})

	t.Run("unknown reserved instances id is rejected", func(t *testing.T) {
		t.Parallel()

		h := newHandler()

		vals := url.Values{
			"Action":                {"ModifyReservedInstances"},
			"Version":               {"2016-11-15"},
			"ReservedInstancesId.1": {"ri-doesnotexist"},
			"ReservedInstancesConfigurationSetItemType.1.InstanceType":  {"t3.large"},
			"ReservedInstancesConfigurationSetItemType.1.InstanceCount": {"1"},
		}

		_, err := ec2.ExportDispatch(h, vals)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "InvalidReservedInstancesId.NotFound")
	})

	t.Run("valid modification is honored and round-trips on describe", func(t *testing.T) {
		t.Parallel()

		bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		h := ec2.NewHandler(bk)

		bk.SeedReservedInstancesOffering(
			"rio-mri-002", "t3.large", "us-east-1a", "Linux/UNIX", "No Upfront", "standard", 31536000, 0, 0.05,
		)
		ri, err := bk.PurchaseReservedInstancesOffering("rio-mri-002", 2)
		require.NoError(t, err)

		vals := url.Values{
			"Action":                {"ModifyReservedInstances"},
			"Version":               {"2016-11-15"},
			"ReservedInstancesId.1": {ri.ReservedInstancesID},
			"ReservedInstancesConfigurationSetItemType.1.InstanceType":     {"t3.xlarge"},
			"ReservedInstancesConfigurationSetItemType.1.InstanceCount":    {"1"},
			"ReservedInstancesConfigurationSetItemType.1.AvailabilityZone": {"us-east-1b"},
			"ReservedInstancesConfigurationSetItemType.2.InstanceType":     {"t3.large"},
			"ReservedInstancesConfigurationSetItemType.2.InstanceCount":    {"1"},
		}

		body, err := ec2.ExportDispatch(h, vals)
		require.NoError(t, err)
		assert.Contains(t, body, "<reservedInstancesModificationId>")

		mods := bk.DescribeReservedInstancesModifications(nil)
		require.Len(t, mods, 1)
		assert.Equal(t, []string{ri.ReservedInstancesID}, mods[0].ReservedInstancesIDs)
		require.Len(t, mods[0].ModificationResults, 2)
		assert.Equal(t, "t3.xlarge", mods[0].ModificationResults[0].TargetConfiguration.InstanceType)
		assert.Equal(t, "us-east-1b", mods[0].ModificationResults[0].TargetConfiguration.AvailabilityZone)
		assert.Equal(t, "t3.large", mods[0].ModificationResults[1].TargetConfiguration.InstanceType)

		describeVals := url.Values{"Action": {"DescribeReservedInstancesModifications"}, "Version": {"2016-11-15"}}
		describeBody, err := ec2.ExportDispatch(h, describeVals)
		require.NoError(t, err)
		assert.Contains(t, describeBody, "<instanceType>t3.xlarge</instanceType>")
		assert.Contains(t, describeBody, "<reservedInstancesId>"+ri.ReservedInstancesID+"</reservedInstancesId>")
	})
}

// TestHandler_CreateReservedInstancesListing_PriceSchedulesRequired covers
// CreateReservedInstancesListingInput.PriceSchedules (api_op_
// CreateReservedInstancesListing.go: "This member is required"). Before the
// fix the handler never read PriceSchedules at all and the backend method
// discarded even InstanceCount, so ReservedInstancesListing never carried
// PriceSchedules or InstanceCounts for any caller.
func TestHandler_CreateReservedInstancesListing_PriceSchedulesRequired(t *testing.T) {
	t.Parallel()

	t.Run("missing price schedules is rejected", func(t *testing.T) {
		t.Parallel()

		bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		h := ec2.NewHandler(bk)

		bk.SeedReservedInstancesOffering(
			"rio-crl-001", "t3.medium", "us-east-1a", "Linux/UNIX", "All Upfront", "standard", 94608000, 500.0, 0.0,
		)
		ri, err := bk.PurchaseReservedInstancesOffering("rio-crl-001", 1)
		require.NoError(t, err)

		vals := url.Values{
			"Action":              {"CreateReservedInstancesListing"},
			"Version":             {"2016-11-15"},
			"ReservedInstancesId": {ri.ReservedInstancesID},
			"InstanceCount":       {"1"},
		}

		_, err = ec2.ExportDispatch(h, vals)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "InvalidParameterValue")
	})

	t.Run("valid price schedules are rendered on create and describe", func(t *testing.T) {
		t.Parallel()

		bk := ec2.NewInMemoryBackend("000000000000", "us-east-1")
		h := ec2.NewHandler(bk)

		bk.SeedReservedInstancesOffering(
			"rio-crl-002", "t3.medium", "us-east-1a", "Linux/UNIX", "All Upfront", "standard", 94608000, 500.0, 0.0,
		)
		ri, err := bk.PurchaseReservedInstancesOffering("rio-crl-002", 3)
		require.NoError(t, err)

		vals := url.Values{
			"Action":                        {"CreateReservedInstancesListing"},
			"Version":                       {"2016-11-15"},
			"ReservedInstancesId":           {ri.ReservedInstancesID},
			"InstanceCount":                 {"3"},
			"PriceSchedules.1.CurrencyCode": {"USD"},
			"PriceSchedules.1.Price":        {"25.5"},
			"PriceSchedules.1.Term":         {"3"},
			"PriceSchedules.2.CurrencyCode": {"USD"},
			"PriceSchedules.2.Price":        {"10"},
			"PriceSchedules.2.Term":         {"1"},
		}

		body, err := ec2.ExportDispatch(h, vals)
		require.NoError(t, err)
		assert.Contains(t, body, "<price>25.5</price>")
		assert.Contains(t, body, "<active>true</active>")
		assert.Contains(t, body, "<instanceCount>3</instanceCount>")

		listings := bk.DescribeReservedInstancesListings(nil)
		require.Len(t, listings, 1)
		require.Len(t, listings[0].PriceSchedules, 2)
		assert.True(t, listings[0].PriceSchedules[0].Active)
		assert.False(t, listings[0].PriceSchedules[1].Active)
		require.Len(t, listings[0].InstanceCounts, 1)
		assert.Equal(t, 3, listings[0].InstanceCounts[0].InstanceCount)
	})
}
