package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		94608000,
		500.0,
		0.0,
	)

	t.Run("describe offerings returns seeded offering", func(t *testing.T) { //nolint:paralleltest // existing issue.
		offerings := b.DescribeReservedInstancesOfferings("", "", "")
		assert.NotEmpty(t, offerings)
	})

	t.Run("describe offerings by instance type", func(t *testing.T) { //nolint:paralleltest // existing issue.
		offerings := b.DescribeReservedInstancesOfferings("t3.medium", "", "")
		require.Len(t, offerings, 1)
		assert.Equal(t, "t3.medium", offerings[0].InstanceType)
	})

	t.Run("describe offerings by az", func(t *testing.T) { //nolint:paralleltest // existing issue.
		offerings := b.DescribeReservedInstancesOfferings("", "us-east-1a", "")
		require.Len(t, offerings, 1)
	})

	t.Run("describe offerings by product description", func(t *testing.T) { //nolint:paralleltest // existing issue.
		offerings := b.DescribeReservedInstancesOfferings("", "", "Linux/UNIX")
		require.Len(t, offerings, 1)
	})

	t.Run("describe offerings no match", func(t *testing.T) { //nolint:paralleltest // existing issue.
		offerings := b.DescribeReservedInstancesOfferings("m5.xlarge", "", "")
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
		l, err := b.CreateReservedInstancesListing(riID, 2)
		require.NoError(t, err)
		assert.NotEmpty(t, l.ReservedInstancesListingID)
		assert.Equal(t, "active", l.Status)
		listingID = l.ReservedInstancesListingID
	})

	t.Run("describe listings", func(t *testing.T) { //nolint:paralleltest // existing issue.
		listings := b.DescribeReservedInstancesListings(nil)
		require.Len(t, listings, 1)
		assert.Equal(t, listingID, listings[0].ReservedInstancesListingID)
	})

	t.Run("cancel listing", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.NoError(t, b.CancelReservedInstancesListing(listingID))
		listings := b.DescribeReservedInstancesListings([]string{listingID})
		require.Len(t, listings, 1)
		assert.Equal(t, "cancelled", listings[0].Status)
	})

	t.Run("cancel non-existent listing returns error", func(t *testing.T) { //nolint:paralleltest // existing issue.
		require.Error(t, b.CancelReservedInstancesListing("rsl-nonexistent"))
	})

	t.Run("modify reserved instances", func(t *testing.T) { //nolint:paralleltest // existing issue.
		mod, err := b.ModifyReservedInstances([]string{riID}, "t3.large", 3)
		require.NoError(t, err)
		assert.NotEmpty(t, mod.ReservedInstancesModificationID)
		assert.Equal(t, "fulfilled", mod.Status)
	})

	t.Run("describe modifications", func(t *testing.T) { //nolint:paralleltest // existing issue.
		mods := b.DescribeReservedInstancesModifications(nil)
		assert.NotEmpty(t, mods)
	})

	t.Run("delete queued reserved instances", func(t *testing.T) { //nolint:paralleltest // existing issue.
		b.DeleteQueuedReservedInstances([]string{riID})
		ris := b.DescribeReservedInstances([]string{riID})
		assert.Empty(t, ris)
	})
}
