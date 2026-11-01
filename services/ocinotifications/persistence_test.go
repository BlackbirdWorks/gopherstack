package ocinotifications_test

import (
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/ons"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotRestoreResumesDeliveries(t *testing.T) {
	t.Parallel()

	src := newEnv(t, envOpts{})
	topic := src.mkTopic(t, "persist")
	sink := newSink(t, 200, 500, 500)
	sub := src.subscribe(t, topic, "HTTPS", sink.srv.URL)
	src.confirm(t, sub, sink.next(t).header.Get("X-Oci-Ns-Confirmationurl"))
	email := src.subscribe(t, topic, "EMAIL", "a@example.com")

	res := src.publish(t, topic, "survives")
	sink.next(t)
	sink.next(t)

	require.Eventually(t, func() bool {
		for _, d := range src.deliveries(t, *sub.Id) {
			if d.MessageID == *res.MessageId {
				return d.Attempts == 2 && d.Status == "PENDING"
			}
		}

		return false
	}, 5*time.Second, 5*time.Millisecond)

	snap := src.backend.Snapshot(t.Context())

	dst := newEnv(t, envOpts{})
	dst.clock.ns.Store(src.clock.ns.Load())
	require.NoError(t, dst.backend.Restore(t.Context(), snap))

	got, err := dst.ctl.GetTopic(t.Context(), ons.GetTopicRequest{TopicId: topic.TopicId})
	require.NoError(t, err)
	assert.Equal(t, "persist", *got.Name)

	list, err := dst.dp.ListSubscriptions(
		t.Context(),
		ons.ListSubscriptionsRequest{CompartmentId: new(testCompartment)},
	)
	require.NoError(t, err)
	assert.Len(t, list.Items, 2)

	pending, err := dst.dp.GetSubscription(t.Context(), ons.GetSubscriptionRequest{SubscriptionId: email.Id})
	require.NoError(t, err)
	assert.Equal(t, ons.SubscriptionLifecycleStatePending, pending.LifecycleState)

	dst.clock.Advance(61 * time.Second)
	dst.backend.Kick()

	assert.Equal(t, "survives", sink.next(t).body)

	require.Eventually(t, func() bool {
		for _, d := range dst.deliveries(t, *sub.Id) {
			if d.MessageID == *res.MessageId {
				return d.Status == "DELIVERED"
			}
		}

		return false
	}, 5*time.Second, 5*time.Millisecond)
}

func TestRestoreGuards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload string
		wantErr bool
	}{
		{"null entry", `{"version":1,"topics":{"a":null}}`, true},
		{"old version", `{"version":99}`, false},
		{"empty", `{"version":1}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			err := e.backend.Restore(t.Context(), []byte(tt.payload))
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestReset(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "reset")
	e.handler.Reset()

	_, err := e.ctl.GetTopic(t.Context(), ons.GetTopicRequest{TopicId: topic.TopicId})
	require.Error(t, err)
}
