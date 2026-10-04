package sns_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

func BenchmarkPublish_NoSignatureConsumers(b *testing.B) {
	backend := sns.NewInMemoryBackend()

	topic, err := backend.CreateTopic("bench-topic", nil)
	require.NoError(b, err)

	b.ReportAllocs()

	for b.Loop() {
		_, pubErr := backend.Publish(topic.TopicArn, "pgoload notification", "pgoload", "", nil)
		require.NoError(b, pubErr)
	}
}

func BenchmarkPublish_SQSSubscriber(b *testing.B) {
	backend := sns.NewInMemoryBackend()

	topic, err := backend.CreateTopic("bench-topic", nil)
	require.NoError(b, err)

	_, err = backend.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:bench-queue", "")
	require.NoError(b, err)

	b.ReportAllocs()

	for b.Loop() {
		_, pubErr := backend.Publish(topic.TopicArn, "pgoload notification", "pgoload", "", nil)
		require.NoError(b, pubErr)
	}
}
