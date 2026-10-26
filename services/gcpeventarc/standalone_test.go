package gcpeventarc_test

import (
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/blackbirdworks/gopherstack/services/gcpeventarc"
	"github.com/blackbirdworks/gopherstack/services/gcppubsub"
)

func TestStandaloneBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		services gcpeventarc.Services
		pushBase string
		want     codes.Code
	}{
		{name: "no pubsub emulator still stores triggers", want: codes.OK},
		{
			name:     "pubsub without a running listener cannot deliver",
			services: gcpeventarc.Services{PubSub: gcppubsub.NewInMemoryBackend()},
			want:     codes.FailedPrecondition,
		},
		{
			name:     "pubsub with a listener provisions the transport",
			services: gcpeventarc.Services{PubSub: gcppubsub.NewInMemoryBackend()},
			pushBase: "http://127.0.0.1:1",
			want:     codes.OK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := gcpeventarc.NewInMemoryBackend()
			b.SetServices(tt.services)
			b.SetPushBase(tt.pushBase)

			op, err := b.CreateTrigger(&eventarcpb.CreateTriggerRequest{
				Parent: parent, TriggerId: "solo-trigger", Trigger: pubsubTrigger("", runDestination("receiver", "")),
			})
			assert.Equal(t, tt.want, code(err), "%v", err)

			if tt.want == codes.OK {
				require.NotNil(t, op)
				assert.True(t, op.GetDone())

				_, err = b.GetTrigger(parent + "/triggers/solo-trigger")
				require.NoError(t, err)
			}
		})
	}
}
