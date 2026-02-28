package sns_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/sns"
)

func newSNSBackend() *sns.InMemoryBackend {
	return sns.NewInMemoryBackend()
}

func TestConfirmSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, b *sns.InMemoryBackend)
	}{
		{
			name: "Success",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				topic, err := b.CreateTopic("my-topic", nil)
				require.NoError(t, err)

				sub, err := b.Subscribe(topic.TopicArn, "https", "https://example.com/notify", "")
				require.NoError(t, err)
				assert.NotEmpty(t, sub.SubscriptionArn)

				// ConfirmSubscription with any non-empty token should succeed.
				confirmed, err := b.ConfirmSubscription(topic.TopicArn, "anytoken123")
				require.NoError(t, err)
				assert.Equal(t, sub.SubscriptionArn, confirmed.SubscriptionArn)
				assert.False(t, confirmed.PendingConfirmation)
			},
		},
		{
			name: "EmptyToken",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				topic, err := b.CreateTopic("tok-topic", nil)
				require.NoError(t, err)

				_, err = b.Subscribe(topic.TopicArn, "https", "https://example.com/notify", "")
				require.NoError(t, err)

				_, err = b.ConfirmSubscription(topic.TopicArn, "")
				require.ErrorIs(t, err, sns.ErrInvalidParameter)
			},
		},
		{
			name: "NoSubscription",
			run: func(t *testing.T, b *sns.InMemoryBackend) {
				topic, err := b.CreateTopic("no-sub-topic", nil)
				require.NoError(t, err)

				_, err = b.ConfirmSubscription(topic.TopicArn, "token123")
				require.ErrorIs(t, err, sns.ErrSubscriptionNotFound)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newSNSBackend()
			tt.run(t, b)
		})
	}
}
