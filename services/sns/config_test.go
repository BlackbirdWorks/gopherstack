package sns_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

func TestNewInMemoryBackendWithConfig_ARNUsesInjectedAccountAndRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
		topic     string
		wantARN   string
	}{
		{
			name:      "default account and region",
			accountID: "000000000000",
			region:    "us-east-1",
			topic:     "my-topic",
			wantARN:   "arn:aws:sns:us-east-1:000000000000:my-topic",
		},
		{
			name:      "custom account and region",
			accountID: "123456789012",
			region:    "eu-west-1",
			topic:     "my-topic",
			wantARN:   "arn:aws:sns:eu-west-1:123456789012:my-topic",
		},
		{
			name:      "us-west-2 region",
			accountID: "999999999999",
			region:    "us-west-2",
			topic:     "alerts",
			wantARN:   "arn:aws:sns:us-west-2:999999999999:alerts",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackendWithConfig(tc.accountID, tc.region)
			topic, err := b.CreateTopic(tc.topic, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.wantARN, topic.TopicArn)
		})
	}
}

func TestNewInMemoryBackend_UsesDefaultAccountAndRegion(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	topic, err := b.CreateTopic("default-topic", nil)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(topic.TopicArn, "arn:aws:sns:us-east-1:000000000000:"),
		"default backend should use account 000000000000 and region us-east-1, got: %s", topic.TopicArn)
}
