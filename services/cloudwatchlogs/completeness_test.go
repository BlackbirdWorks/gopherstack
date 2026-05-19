package cloudwatchlogs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func newTestBackend(t *testing.T) *cloudwatchlogs.InMemoryBackend {
	t.Helper()
	b := cloudwatchlogs.NewInMemoryBackend()
	t.Cleanup(func() { b.Close() })
	return b
}

// ---- ResourcePolicy ----

func TestResourcePolicy_CRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	t.Run("PutAndDescribe", func(t *testing.T) {
		t.Parallel()

		bb := newTestBackend(t)

		p, err := bb.PutResourcePolicy("my-policy", `{"Version":"2012-10-17"}`)
		require.NoError(t, err)
		assert.Equal(t, "my-policy", p.PolicyName)

		policies := bb.DescribeResourcePolicies()
		assert.Len(t, policies, 1)
		assert.Equal(t, "my-policy", policies[0].PolicyName)
	})

	t.Run("Delete", func(t *testing.T) {
		t.Parallel()

		bb := newTestBackend(t)

		_, err := bb.PutResourcePolicy("del-policy", `{}`)
		require.NoError(t, err)

		err = bb.DeleteResourcePolicy("del-policy")
		require.NoError(t, err)

		policies := bb.DescribeResourcePolicies()
		assert.Empty(t, policies)
	})

	t.Run("DeleteNotFound", func(t *testing.T) {
		t.Parallel()

		err := b.DeleteResourcePolicy("ghost")
		require.Error(t, err)
	})

	t.Run("EmptyName", func(t *testing.T) {
		t.Parallel()

		_, err := b.PutResourcePolicy("", `{}`)
		require.Error(t, err)
	})
}

// ---- DeliveryDestination ----

func TestDeliveryDestination_CRUD(t *testing.T) {
	t.Parallel()

	t.Run("PutGetDescribeDelete", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		dest, err := b.PutDeliveryDestination("my-dest", "arn:aws:s3:::my-bucket", "JSON", nil)
		require.NoError(t, err)
		assert.Equal(t, "my-dest", dest.Name)
		assert.NotEmpty(t, dest.Arn)

		got, err := b.GetDeliveryDestination("my-dest")
		require.NoError(t, err)
		assert.Equal(t, "arn:aws:s3:::my-bucket", got.TargetArn)

		dests := b.DescribeDeliveryDestinations()
		assert.Len(t, dests, 1)

		err = b.DeleteDeliveryDestination("my-dest")
		require.NoError(t, err)

		_, err = b.GetDeliveryDestination("my-dest")
		require.Error(t, err)
	})

	t.Run("Policy", func(t *testing.T) {
		t.Parallel()

		b := newTestBackend(t)

		_, err := b.PutDeliveryDestination("policy-dest", "arn:aws:s3:::bucket", "JSON", nil)
		require.NoError(t, err)

		err = b.PutDeliveryDestinationPolicy("policy-dest", `{"Statement":[]}`)
		require.NoError(t, err)

		policy, err := b.GetDeliveryDestinationPolicy("policy-dest")
		require.NoError(t, err)
		assert.Contains(t, policy, "Statement")

		err = b.DeleteDeliveryDestinationPolicy("policy-dest")
		require.NoError(t, err)

		policy, err = b.GetDeliveryDestinationPolicy("policy-dest")
		require.NoError(t, err)
		assert.Empty(t, policy)
	})
}

// ---- DeliverySource ----

func TestDeliverySource_CRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	src, err := b.PutDeliverySource("my-src", "APPLICATION_LOGS", []string{"arn:aws:ec2:::instance/i-123"}, nil)
	require.NoError(t, err)
	assert.Equal(t, "my-src", src.Name)
	assert.NotEmpty(t, src.Arn)

	got, err := b.GetDeliverySource("my-src")
	require.NoError(t, err)
	assert.Equal(t, "APPLICATION_LOGS", got.LogType)

	srcs := b.DescribeDeliverySources()
	assert.Len(t, srcs, 1)

	err = b.DeleteDeliverySource("my-src")
	require.NoError(t, err)

	_, err = b.GetDeliverySource("my-src")
	require.Error(t, err)
}

// ---- Destination ----

func TestDestination_CRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	dest, err := b.PutDestination("my-dest", "arn:aws:kinesis:::stream/my-stream", "arn:aws:iam:::role/my-role")
	require.NoError(t, err)
	assert.Equal(t, "my-dest", dest.DestinationName)
	assert.NotEmpty(t, dest.Arn)

	// PutDestinationPolicy
	err = b.PutDestinationPolicy("my-dest", `{"Statement":[]}`)
	require.NoError(t, err)

	// DescribeDestinations
	dests := b.DescribeDestinations("")
	assert.Len(t, dests, 1)
	assert.Equal(t, `{"Statement":[]}`, dests[0].AccessPolicy)

	// Filter by prefix
	dests = b.DescribeDestinations("my")
	assert.Len(t, dests, 1)
	dests = b.DescribeDestinations("other")
	assert.Empty(t, dests)

	// Delete
	err = b.DeleteDestination("my-dest")
	require.NoError(t, err)

	dests = b.DescribeDestinations("")
	assert.Empty(t, dests)
}

// ---- IndexPolicy ----

func TestIndexPolicy_CRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	p, err := b.PutIndexPolicy("/aws/lambda/my-fn", `{"fields":["@message"]}`)
	require.NoError(t, err)
	assert.Equal(t, "/aws/lambda/my-fn", p.LogGroupIdentifier)

	policies := b.DescribeIndexPolicies()
	assert.Len(t, policies, 1)

	err = b.DeleteIndexPolicy("/aws/lambda/my-fn")
	require.NoError(t, err)

	policies = b.DescribeIndexPolicies()
	assert.Empty(t, policies)

	err = b.DeleteIndexPolicy("nonexistent")
	require.Error(t, err)
}

// ---- Transformer ----

func TestTransformer_CRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	processors := []map[string]any{
		{"parseJSON": map[string]any{}},
	}

	err := b.PutTransformer("/aws/lambda/my-fn", processors)
	require.NoError(t, err)

	tr, err := b.GetTransformer("/aws/lambda/my-fn")
	require.NoError(t, err)
	assert.Equal(t, "/aws/lambda/my-fn", tr.LogGroupIdentifier)
	assert.Len(t, tr.Processors, 1)

	err = b.DeleteTransformer("/aws/lambda/my-fn")
	require.NoError(t, err)

	_, err = b.GetTransformer("/aws/lambda/my-fn")
	require.Error(t, err)
}

// ---- Integration ----

func TestIntegration_CRUD(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	ig, err := b.PutIntegration("my-opensearch", "OPENSEARCH")
	require.NoError(t, err)
	assert.Equal(t, "my-opensearch", ig.Name)
	assert.Equal(t, "ACTIVE", ig.Status)

	got, err := b.GetIntegration("my-opensearch")
	require.NoError(t, err)
	assert.Equal(t, "OPENSEARCH", got.Type)

	igs := b.ListIntegrations()
	assert.Len(t, igs, 1)

	err = b.DeleteIntegration("my-opensearch")
	require.NoError(t, err)

	_, err = b.GetIntegration("my-opensearch")
	require.Error(t, err)
}

// ---- LogGroupDeletionProtection ----

func TestLogGroupDeletionProtection(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	assert.False(t, b.IsLogGroupDeletionProtected("/aws/lambda/my-fn"))

	err := b.SetLogGroupDeletionProtection("/aws/lambda/my-fn", true)
	require.NoError(t, err)
	assert.True(t, b.IsLogGroupDeletionProtected("/aws/lambda/my-fn"))

	err = b.SetLogGroupDeletionProtection("/aws/lambda/my-fn", false)
	require.NoError(t, err)
	assert.False(t, b.IsLogGroupDeletionProtected("/aws/lambda/my-fn"))
}

// ---- UpdateDeliveryConfiguration ----

func TestUpdateDeliveryConfiguration(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)

	_, err := b.CreateDelivery("src", "arn:aws:logs:::delivery-destination:dst", nil)
	require.NoError(t, err)

	deliveries, _, err := b.DescribeDeliveries(100, "")
	require.NoError(t, err)
	require.Len(t, deliveries, 1)

	deliveryID := deliveries[0].ID

	err = b.UpdateDeliveryConfiguration(deliveryID, ",", []string{"@timestamp", "@message"})
	require.NoError(t, err)

	// Verify update persisted.
	deliveries, _, err = b.DescribeDeliveries(100, "")
	require.NoError(t, err)
	assert.Equal(t, ",", deliveries[0].FieldDelimiter)
	assert.Equal(t, []string{"@timestamp", "@message"}, deliveries[0].RecordFields)
}
