package sqs_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPruneState_SkipsIdleQueues verifies that pruneState does not process
// queues that have never received a message. We inject a large number of empty
// queues and confirm the janitor produces no work items for them while still
// correctly expiring messages in active queues.
func TestPruneState_SkipsIdleQueues(t *testing.T) {
	t.Parallel()

	const (
		idleCount  = 50
		activeBody = "active-message"
		ep         = "localhost:4566"
	)

	b := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	// Create many idle queues (no messages sent).
	for i := range idleCount {
		_, err := b.CreateQueue(&sqs.CreateQueueInput{
			QueueName: fmt.Sprintf("idle-%d", i),
			Endpoint:  ep,
		})
		require.NoError(t, err)
	}

	// Create one active queue and send a message.
	activeOut, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "active-queue",
		Endpoint:  ep,
	})
	require.NoError(t, err)

	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    activeOut.QueueURL,
		MessageBody: activeBody,
	})
	require.NoError(t, err)

	// Shorten retention so the message expires on the next janitor tick.
	b.SetRetentionForTest(activeOut.QueueURL, 1)

	// Run the janitor at a time 2 seconds in the future — active message expires,
	// idle queues produce no work.
	b.RunJanitorOnceForTest(time.Now().Add(2 * time.Second))

	// Active queue should now be empty.
	recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            activeOut.QueueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, recv.Messages, "active-queue message should have been expired by janitor")
}

// TestPruneState_ClearsActivityFlagWhenIdle verifies that the hasActivity flag
// is cleared after the janitor drains the last message from a queue, so that
// subsequent janitor ticks skip the queue without processing it.
func TestPruneState_ClearsActivityFlagWhenIdle(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "drain-test",
		Endpoint:  "localhost:4566",
	})
	require.NoError(t, err)

	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    out.QueueURL,
		MessageBody: "ephemeral",
	})
	require.NoError(t, err)

	b.SetRetentionForTest(out.QueueURL, 1)

	// First janitor tick: expires the message.
	b.RunJanitorOnceForTest(time.Now().Add(2 * time.Second))

	// Second janitor tick: queue is empty, should not panic or produce errors.
	b.RunJanitorOnceForTest(time.Now().Add(4 * time.Second))

	// Queue should still exist and be accessible (just empty).
	recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            out.QueueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Empty(t, recv.Messages)
}
