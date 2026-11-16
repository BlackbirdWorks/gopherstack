package gcpeventarc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransportRebind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rebind func(t *testing.T, e *env, snap []byte)
		name   string
	}{
		{
			name: "new push base",
			rebind: func(_ *testing.T, e *env, _ []byte) {
				e.handler.Backend.SetPushBase("http://127.0.0.1:9")
			},
		},
		{
			name: "restore",
			rebind: func(t *testing.T, e *env, snap []byte) {
				t.Helper()
				e.handler.Backend.SetPushBase("http://127.0.0.1:9")
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

			tt.rebind(t, e, snap)

			got, err = e.pubsub.Backend.GetSubscription(sub)
			require.NoError(t, err)
			assert.NotEqual(t, before, got.GetPushConfig().GetPushEndpoint())
			assert.Equal(t, "http://127.0.0.1:9/_eventarc/push/"+trig.GetName(), got.GetPushConfig().GetPushEndpoint())
		})
	}
}
