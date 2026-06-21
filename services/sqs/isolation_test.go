package sqs_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// TestSQSRegionIsolation proves that a same-named queue created in two different
// regions stays fully isolated: each region lists only its own queue, GetQueueURL
// is region-scoped, and deleting the queue in one region leaves the other region's
// queue intact.
//
// SQS isolates by a region-qualified map key (region + queue name) and stores the
// region on every queue; ListQueues/GetQueueURL filter by the request region
// threaded from SigV4. This test locks that behaviour in.
func TestSQSRegionIsolation(t *testing.T) {
	t.Parallel()

	const (
		east  = "us-east-1"
		west  = "us-west-2"
		queue = "shared-name"
	)

	b := sqs.NewInMemoryBackendWithConfig("000000000000", east)
	t.Cleanup(b.Close)

	// 1. Create a queue named "shared-name" in us-east-1.
	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: queue,
		Endpoint:  "localhost:4566",
		Region:    east,
	})
	require.NoError(t, err)

	// 2. Create a queue with the SAME NAME in us-west-2 — must NOT collide.
	_, err = b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: queue,
		Endpoint:  "localhost:4566",
		Region:    west,
	})
	require.NoError(t, err)

	// 3. Each region lists exactly its own queue.
	eastList, err := b.ListQueues(&sqs.ListQueuesInput{Region: east})
	require.NoError(t, err)
	require.Len(t, eastList.QueueURLs, 1)

	westList, err := b.ListQueues(&sqs.ListQueuesInput{Region: west})
	require.NoError(t, err)
	require.Len(t, westList.QueueURLs, 1)

	// 4. GetQueueURL resolves within the requested region.
	gotEast, err := b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: queue, Region: east})
	require.NoError(t, err)
	assert.NotEmpty(t, gotEast.QueueURL)

	gotWest, err := b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: queue, Region: west})
	require.NoError(t, err)
	assert.NotEmpty(t, gotWest.QueueURL)

	// 5. Deleting the us-east-1 queue leaves the us-west-2 queue intact.
	require.NoError(t, b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: gotEast.QueueURL, Region: east}))

	_, err = b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: queue, Region: east})
	require.Error(t, err)

	_, err = b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: queue, Region: west})
	require.NoError(t, err)

	eastAfter, err := b.ListQueues(&sqs.ListQueuesInput{Region: east})
	require.NoError(t, err)
	assert.Empty(t, eastAfter.QueueURLs)

	westAfter, err := b.ListQueues(&sqs.ListQueuesInput{Region: west})
	require.NoError(t, err)
	assert.Len(t, westAfter.QueueURLs, 1)
}
