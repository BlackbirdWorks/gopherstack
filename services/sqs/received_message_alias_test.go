package sqs_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// A message handed to a consumer is read by the handler after the queue lock is
// released; a later re-receive must not stamp a new receipt handle or counters
// into the same object.
func TestReceivedMessage_NotAliasedToReReceive(t *testing.T) {
	t.Parallel()

	b, clock := newClockedBackend(t)
	qURL := createTestQueue(t, b, "alias-q")

	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "body"})
	require.NoError(t, err)

	first, err := b.ReceiveMessage(
		&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1, VisibilityTimeout: 5},
	)
	require.NoError(t, err)
	require.Len(t, first.Messages, 1)

	held := first.Messages[0]
	handle, count := held.ReceiptHandle, held.Attributes["ApproximateReceiveCount"]

	clock.Advance(10 * time.Second)

	stop := make(chan struct{})

	var wg sync.WaitGroup

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = held.ReceiptHandle
				_ = held.Attributes["ApproximateReceiveCount"]
			}
		}
	})

	second, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
	close(stop)
	wg.Wait()

	require.NoError(t, err)
	require.Len(t, second.Messages, 1)
	assert.NotEqual(t, handle, second.Messages[0].ReceiptHandle)
	assert.Equal(t, handle, held.ReceiptHandle)
	assert.Equal(t, count, held.Attributes["ApproximateReceiveCount"])
}
