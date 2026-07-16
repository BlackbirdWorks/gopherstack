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

// TestLookupQueueByURL_RegionIsolation verifies that lookupQueueByURL respects
// region boundaries and never returns a queue from a different region when the
// caller supplies an explicit region. This locks in the parity fix that replaced
// the O(n) URL-string scan fallback with an effectiveRegion-scoped key lookup.
func TestLookupQueueByURL_RegionIsolation(t *testing.T) {
	t.Parallel()

	const (
		east  = "us-east-1"
		west  = "us-west-2"
		name  = "same-name"
		ep    = "localhost:4566"
		accID = "000000000000"
	)

	tests := []struct {
		name          string
		sendRegion    string
		opRegion      string
		wantSendErr   bool
		wantReceiveOK bool
	}{
		{
			name:          "same_region_send_and_receive",
			sendRegion:    east,
			opRegion:      east,
			wantReceiveOK: true,
		},
		{
			name:          "wrong_region_cannot_receive",
			sendRegion:    east,
			opRegion:      west,
			wantReceiveOK: false,
		},
		{
			name:          "empty_region_uses_backend_default",
			sendRegion:    "",
			opRegion:      "",
			wantReceiveOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackendWithConfig(accID, east)
			t.Cleanup(b.Close)

			out, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: name,
				Endpoint:  ep,
				Region:    tt.sendRegion,
			})
			require.NoError(t, err)

			_, err = b.SendMessage(&sqs.SendMessageInput{
				QueueURL:    out.QueueURL,
				Region:      tt.sendRegion,
				MessageBody: "hello",
			})
			require.NoError(t, err)

			recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            out.QueueURL,
				Region:              tt.opRegion,
				MaxNumberOfMessages: 1,
			})

			if tt.wantReceiveOK {
				require.NoError(t, err)
				assert.Len(t, recv.Messages, 1)
			} else {
				require.Error(t, err, "expected error when using wrong region")
			}
		})
	}
}

// TestLookupQueueByURL_CrossRegionURLScanEliminated verifies that the previous
// URL-scan fallback no longer bleeds across region boundaries: creating a queue
// in us-east-1 and then accessing it with a us-west-2 request returns not-found
// rather than silently returning the east queue.
func TestLookupQueueByURL_CrossRegionURLScanEliminated(t *testing.T) {
	t.Parallel()

	const (
		east  = "us-east-1"
		west  = "us-west-2"
		name  = "isolation-queue"
		ep    = "localhost:4566"
		accID = "000000000000"
	)

	b := sqs.NewInMemoryBackendWithConfig(accID, east)
	t.Cleanup(b.Close)

	eastOut, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: name,
		Endpoint:  ep,
		Region:    east,
	})
	require.NoError(t, err)

	_, sendErr := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    eastOut.QueueURL,
		Region:      east,
		MessageBody: "eastbound",
	})
	require.NoError(t, sendErr)

	// A west-region request using the east queue URL must not find the queue.
	_, err = b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            eastOut.QueueURL,
		Region:              west,
		MaxNumberOfMessages: 1,
	})
	require.Error(t, err, "west-region request should not find east queue")

	err = b.DeleteMessage(&sqs.DeleteMessageInput{
		QueueURL:      eastOut.QueueURL,
		Region:        west,
		ReceiptHandle: "any-handle",
	})
	require.Error(t, err, "west-region DeleteMessage should not find east queue")
}
