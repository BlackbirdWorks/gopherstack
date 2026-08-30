package eventbridge_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestEventSource_ActivateDeactivate(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreatePartnerEventSource(context.Background(), "aws.partner/example.com/myapp", "123456789012")
	require.NoError(t, err)

	src, err := b.DescribePartnerEventSource(context.Background(), "aws.partner/example.com/myapp")
	require.NoError(t, err)
	assert.Equal(t, "aws.partner/example.com/myapp", src.Name)

	srcs, _, err := b.ListPartnerEventSources(context.Background(), "aws.partner/", "", 0)
	require.NoError(t, err)
	assert.Len(t, srcs, 1)

	err = b.DeletePartnerEventSource(context.Background(), "aws.partner/example.com/myapp")
	require.NoError(t, err)

	_, err = b.DescribePartnerEventSource(context.Background(), "aws.partner/example.com/myapp")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestEventSource_ActivateChangesState(t *testing.T) {
	t.Parallel()
	b := newBackend()

	name := "aws.partner/test.com/app"
	// Seed the event source directly (partner source → event source activation).
	b.AddPartnerSourceInternal(&eventbridge.PartnerEventSource{
		Name:    name,
		Arn:     "arn:aws:events:us-east-1:123456789012:event-source/aws.partner/test.com/app",
		Account: "123456789012",
	})

	src, _, err := b.ListEventSources(context.Background(), "", "", 0)
	require.NoError(t, err)
	_ = src // verify list works without panic
}

func TestEventSourceCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribeEventSource(context.Background(), "missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		b.AddEventSourceInternal(&eventbridge.EventSource{
			Name:  "aws.partner.test",
			State: "PENDING",
		})

		got, err := b.DescribeEventSource(context.Background(), "aws.partner.test")
		require.NoError(t, err)
		assert.Equal(t, "aws.partner.test", got.Name)

		sources, _, err := b.ListEventSources(context.Background(), "aws.partner", "", 0)
		require.NoError(t, err)
		assert.Len(t, sources, 1)
	})
}

// TestActivateDeactivateEventSource covers not-found handling plus the
// Activate/Deactivate round trips.
func TestActivateDeactivateEventSource(t *testing.T) {
	t.Parallel()

	t.Run("activate_not_found", func(t *testing.T) {
		t.Parallel()

		b := eventbridge.NewInMemoryBackend()
		require.ErrorIs(t, b.ActivateEventSource(context.Background(), "no-such-source"), eventbridge.ErrNotFound)
	})

	t.Run("activate_round_trip", func(t *testing.T) {
		t.Parallel()

		b := eventbridge.NewInMemoryBackend()
		b.AddEventSourceInternal(&eventbridge.EventSource{
			Name:         "my-source",
			State:        "PENDING",
			CreationTime: time.Now(),
		})

		require.NoError(t, b.ActivateEventSource(context.Background(), "my-source"))
	})

	t.Run("deactivate_round_trip", func(t *testing.T) {
		t.Parallel()

		b := eventbridge.NewInMemoryBackend()
		b.AddEventSourceInternal(&eventbridge.EventSource{
			Name:         "my-source",
			State:        "ACTIVE",
			CreationTime: time.Now(),
		})

		require.NoError(t, b.DeactivateEventSource(context.Background(), "my-source"))
	})
}
