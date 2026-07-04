package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func TestBackend_ScheduledInstanceAvailability_ReturnsStaticCatalog(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	catalog := b.DescribeScheduledInstanceAvailability(nil, 0, 0)
	require.NotEmpty(t, catalog)
	assert.NotEmpty(t, catalog[0].PurchaseToken)
	assert.NotEmpty(t, catalog[0].InstanceType)
}

func TestBackend_ScheduledInstanceAvailability_FiltersByInstanceType(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	catalog := b.DescribeScheduledInstanceAvailability(nil, 0, 0)
	require.NotEmpty(t, catalog)

	want := catalog[0].InstanceType
	filtered := b.DescribeScheduledInstanceAvailability(map[string][]string{"instance-type": {want}}, 0, 0)
	require.NotEmpty(t, filtered)

	for _, e := range filtered {
		assert.Equal(t, want, e.InstanceType)
	}
}

func TestBackend_ScheduledInstanceAvailability_NoMatchReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	filtered := b.DescribeScheduledInstanceAvailability(
		map[string][]string{"instance-type": {"does-not-exist"}}, 0, 0,
	)
	assert.Empty(t, filtered)
}

func TestBackend_PurchaseScheduledInstances_UnknownTokenFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.PurchaseScheduledInstances(
		[]ec2.ScheduledInstancePurchaseRequest{{PurchaseToken: "not-a-real-token", InstanceCount: 1}},
	)
	require.Error(t, err)
}

func TestBackend_PurchaseScheduledInstances_ThenDescribeRoundTrips(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	catalog := b.DescribeScheduledInstanceAvailability(nil, 0, 0)
	require.NotEmpty(t, catalog)

	purchased, err := b.PurchaseScheduledInstances(
		[]ec2.ScheduledInstancePurchaseRequest{{PurchaseToken: catalog[0].PurchaseToken, InstanceCount: 2}},
	)
	require.NoError(t, err)
	require.Len(t, purchased, 1)
	assert.Contains(t, purchased[0].ScheduledInstanceID, "sci-")
	assert.Equal(t, int32(2), purchased[0].InstanceCount)

	described := b.DescribeScheduledInstances([]string{purchased[0].ScheduledInstanceID})
	require.Len(t, described, 1)
	assert.Equal(t, purchased[0].ScheduledInstanceID, described[0].ScheduledInstanceID)
}

func TestBackend_DescribeScheduledInstances_UnknownIDReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	assert.Empty(t, b.DescribeScheduledInstances([]string{"sci-doesnotexist"}))
}

// purchaseScheduledInstanceFixture purchases a fresh 2-instance schedule on a new backend,
// for use by a single RunScheduledInstances test.
func purchaseScheduledInstanceFixture(t *testing.T) (*ec2.InMemoryBackend, string) {
	t.Helper()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	catalog := b.DescribeScheduledInstanceAvailability(nil, 0, 0)
	require.NotEmpty(t, catalog)

	purchased, err := b.PurchaseScheduledInstances(
		[]ec2.ScheduledInstancePurchaseRequest{{PurchaseToken: catalog[0].PurchaseToken, InstanceCount: 2}},
	)
	require.NoError(t, err)
	require.Len(t, purchased, 1)

	return b, purchased[0].ScheduledInstanceID
}

func TestBackend_RunScheduledInstances_UnknownScheduleFails(t *testing.T) {
	t.Parallel()

	b, _ := purchaseScheduledInstanceFixture(t)

	_, err := b.RunScheduledInstances("sci-doesnotexist", "ami-123", "", 1)
	require.Error(t, err)
}

func TestBackend_RunScheduledInstances_LaunchesUpToPurchasedCount(t *testing.T) {
	t.Parallel()

	b, sciID := purchaseScheduledInstanceFixture(t)

	ids, err := b.RunScheduledInstances(sciID, "ami-123", "my-key", 2)
	require.NoError(t, err)
	require.Len(t, ids, 2)

	for _, id := range ids {
		assert.Contains(t, id, "i-")
	}
}

func TestBackend_RunScheduledInstances_BeyondPurchasedCountFails(t *testing.T) {
	t.Parallel()

	b, sciID := purchaseScheduledInstanceFixture(t)

	_, err := b.RunScheduledInstances(sciID, "ami-123", "", 2)
	require.NoError(t, err)

	_, err = b.RunScheduledInstances(sciID, "ami-123", "", 1)
	require.Error(t, err)
}
