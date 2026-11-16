package gcpeventarc_test

import (
	"testing"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransportRebind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rebind func(t *testing.T, e *env, snap []byte, sub string)
		name   string
	}{
		{
			name: "new push base",
			rebind: func(_ *testing.T, e *env, _ []byte, _ string) {
				e.handler.Backend.SetPushBase("http://127.0.0.1:9")
			},
		},
		{
			name: "restore",
			rebind: func(t *testing.T, e *env, snap []byte, sub string) {
				t.Helper()
				e.handler.Backend.SetPushBase("http://127.0.0.1:9")
				stale := &pubsubpb.UpdateSubscriptionRequest{
					Subscription: &pubsubpb.Subscription{
						Name:       sub,
						PushConfig: &pubsubpb.PushConfig{PushEndpoint: "http://stale.invalid/push"},
					},
					UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"push_config"}},
				}
				_, err := e.pubsub.Backend.UpdateSubscription(stale)
				require.NoError(t, err)
				require.NoError(t, e.handler.Backend.Restore(t.Context(), snap))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			trig := e.createTrigger(t, "moved-trigger", pubsubTrigger("", runDestination("receiver", "")))
			sub := trig.GetTransport().GetPubsub().GetSubscription()
			snap := e.handler.Backend.Snapshot(t.Context())

			got, err := e.pubsub.Backend.GetSubscription(sub)
			require.NoError(t, err)
			before := got.GetPushConfig().GetPushEndpoint()

			tt.rebind(t, e, snap, sub)

			got, err = e.pubsub.Backend.GetSubscription(sub)
			require.NoError(t, err)
			assert.NotEqual(t, before, got.GetPushConfig().GetPushEndpoint())
			assert.Equal(t, "http://127.0.0.1:9/_eventarc/push/"+trig.GetName(), got.GetPushConfig().GetPushEndpoint())
		})
	}
}
