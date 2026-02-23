package sqs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/sqs"
)

func TestNewInMemoryBackendWithConfig_ARNUsesInjectedAccountAndRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
		queue     string
		wantARN   string
	}{
		{
			name:      "default account and region",
			accountID: "000000000000",
			region:    "us-east-1",
			queue:     "my-queue",
			wantARN:   "arn:aws:sqs:us-east-1:000000000000:my-queue",
		},
		{
			name:      "custom account and region",
			accountID: "123456789012",
			region:    "eu-west-1",
			queue:     "my-queue",
			wantARN:   "arn:aws:sqs:eu-west-1:123456789012:my-queue",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackendWithConfig(tc.accountID, tc.region)
			out, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: tc.queue,
				Endpoint:  "localhost:4566",
			})
			require.NoError(t, err)

			// QueueURL should embed account ID
			assert.Contains(t, out.QueueURL, tc.accountID,
				"queue URL should contain account ID %s, got: %s", tc.accountID, out.QueueURL)

			// QueueArn attribute should use the configured account and region
			attrs, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
				QueueURL:       out.QueueURL,
				AttributeNames: []string{"All"},
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantARN, attrs.Attributes["QueueArn"])
		})
	}
}
