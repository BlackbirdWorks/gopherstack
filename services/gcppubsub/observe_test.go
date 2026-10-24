package gcppubsub_test

import (
	"testing"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcppubsub"
)

func TestObservePublishes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topic     string
		msgs      []*pubsubpb.PubsubMessage
		wantTopic string
		wantData  []string
		wantErr   bool
	}{
		{
			name: "each message", topic: proj + "/topics/orders", wantTopic: proj + "/topics/orders",
			msgs:     []*pubsubpb.PubsubMessage{{Data: []byte("a")}, {Data: []byte("b")}},
			wantData: []string{"a", "b"},
		},
		{
			name: "rejected publish", topic: proj + "/topics/missing", wantErr: true,
			msgs: []*pubsubpb.PubsubMessage{{Data: []byte("a")}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := gcppubsub.NewInMemoryBackend()
			_, err := b.CreateTopic(&pubsubpb.Topic{Name: proj + "/topics/orders"})
			require.NoError(t, err)

			var gotTopic string

			var gotData []string

			b.ObservePublishes(func(topic string, m *pubsubpb.PubsubMessage) {
				gotTopic = topic

				gotData = append(gotData, string(m.GetData()))

				assert.NotEmpty(t, m.GetMessageId())
				assert.NotNil(t, m.GetPublishTime())
			})

			_, err = b.Publish(&pubsubpb.PublishRequest{Topic: tt.topic, Messages: tt.msgs})
			if tt.wantErr {
				require.Error(t, err)
				assert.Empty(t, gotData)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantTopic, gotTopic)
			assert.Equal(t, tt.wantData, gotData)
		})
	}
}
