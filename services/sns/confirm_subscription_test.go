package sns_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

func newSNSBackend() *sns.InMemoryBackend {
	return sns.NewInMemoryBackend()
}

func TestConfirmSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		topicName string
		token     string
		subscribe bool
	}{
		{
			name:      "Success",
			topicName: "my-topic",
			subscribe: true,
			token:     "anytoken123",
		},
		{
			name:      "EmptyToken",
			topicName: "tok-topic",
			subscribe: true,
			token:     "",
			wantErr:   sns.ErrInvalidParameter,
		},
		{
			name:      "NoSubscription",
			topicName: "no-sub-topic",
			subscribe: false,
			token:     "token123",
			wantErr:   sns.ErrSubscriptionNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newSNSBackend()

			topic, err := b.CreateTopic(tt.topicName, nil)
			require.NoError(t, err)

			var sub *sns.Subscription
			if tt.subscribe {
				sub, err = b.Subscribe(topic.TopicArn, "https", "https://example.com/notify", "")
				require.NoError(t, err)
				assert.NotEmpty(t, sub.SubscriptionArn)
			}

			confirmed, err := b.ConfirmSubscription(topic.TopicArn, tt.token)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			require.NotNil(t, sub)
			assert.Equal(t, sub.SubscriptionArn, confirmed.SubscriptionArn)
			assert.False(t, confirmed.PendingConfirmation)
		})
	}
}
