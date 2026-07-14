package eventbridge_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutPartnerEvents_RecordsResults(t *testing.T) {
	t.Parallel()
	b := newBackend()

	entries := []eventbridge.EventEntry{
		{Source: "aws.partner/example.com/app", DetailType: "UserSignup", Detail: `{"userId":"u1"}`},
		{Source: "aws.partner/example.com/app", DetailType: "OrderPlaced", Detail: `{"orderId":"o1"}`},
	}

	results, err := b.PutPartnerEvents(context.Background(), entries)
	require.NoError(t, err)
	assert.Len(t, results, 2)
	for _, r := range results {
		assert.Empty(t, r.ErrorCode)
		assert.NotEmpty(t, r.EventID)
	}
}

func TestPartnerEventSource_CreatesEventSourceAsPending(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreatePartnerEventSource(context.Background(), "aws.partner/example.com/app", "123456789012")
	require.NoError(t, err)

	// The partner source should show up in ListEventSources as PENDING.
	sources, _, err := b.ListEventSources(context.Background(), "aws.partner/", "")
	require.NoError(t, err)
	require.Len(t, sources, 1)
	assert.Equal(t, "aws.partner/example.com/app", sources[0].Name)
	assert.Equal(t, "PENDING", sources[0].State)
}

func TestPartnerEventSource_ActivateTransitionsToPending(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreatePartnerEventSource(context.Background(), "aws.partner/saas.com/feed", "111111111111")
	require.NoError(t, err)

	// Event source should start as PENDING.
	src, err := b.DescribeEventSource(context.Background(), "aws.partner/saas.com/feed")
	require.NoError(t, err)
	assert.Equal(t, "PENDING", src.State)

	// Activate it → ACTIVE.
	err = b.ActivateEventSource(context.Background(), "aws.partner/saas.com/feed")
	require.NoError(t, err)

	src, err = b.DescribeEventSource(context.Background(), "aws.partner/saas.com/feed")
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", src.State)
}

func TestPartnerEventSource_DeactivateTransitionsToInactive(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreatePartnerEventSource(context.Background(), "aws.partner/saas.com/feed2", "222222222222")
	require.NoError(t, err)

	require.NoError(t, b.ActivateEventSource(context.Background(), "aws.partner/saas.com/feed2"))
	require.NoError(t, b.DeactivateEventSource(context.Background(), "aws.partner/saas.com/feed2"))

	src, err := b.DescribeEventSource(context.Background(), "aws.partner/saas.com/feed2")
	require.NoError(t, err)
	assert.Equal(t, "INACTIVE", src.State)
}

func TestPartnerEventSource_DeletePartnerSourceNotAffectEventSource(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreatePartnerEventSource(context.Background(), "aws.partner/example.com/gone", "333333333333")
	require.NoError(t, err)

	// Partner source deletion should NOT remove the event source record.
	err = b.DeletePartnerEventSource(context.Background(), "aws.partner/example.com/gone")
	require.NoError(t, err)

	// The event source persists for the customer.
	_, err = b.DescribeEventSource(context.Background(), "aws.partner/example.com/gone")
	require.NoError(t, err)
}

func TestPartnerEventSourceCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribePartnerEventSource(context.Background(), "missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("delete not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		require.ErrorIs(t, b.DeletePartnerEventSource(context.Background(), "missing"), eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		b.AddPartnerSourceInternal(&eventbridge.PartnerEventSource{
			Name: "aws.partner.test.123",
		})

		got, err := b.DescribePartnerEventSource(context.Background(), "aws.partner.test.123")
		require.NoError(t, err)
		assert.Equal(t, "aws.partner.test.123", got.Name)

		srcs, _, err := b.ListPartnerEventSources(context.Background(), "aws.partner", "")
		require.NoError(t, err)
		assert.Len(t, srcs, 1)

		require.NoError(t, b.DeletePartnerEventSource(context.Background(), "aws.partner.test.123"))
		_, err = b.DescribePartnerEventSource(context.Background(), "aws.partner.test.123")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})
}

func TestPutPartnerEvents(t *testing.T) {
	t.Parallel()
	b := newBackend()
	results, err := b.PutPartnerEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "aws.partner.test", DetailType: "Ping", Detail: "{}"},
	})
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Empty(t, results[0].ErrorCode)
}

// TestCreatePartnerEventSource_RoundTrip creates a partner event source and verifies ARN.
func TestCreatePartnerEventSource_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	src, err := b.CreatePartnerEventSource(context.Background(), "my-partner-source", "123456789012")
	require.NoError(t, err)

	assert.NotEmpty(t, src.Arn)
	assert.Equal(t, "my-partner-source", src.Name)
	assert.Equal(t, 1, b.PartnerSourceCount())
}
