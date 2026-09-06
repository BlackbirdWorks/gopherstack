package integration_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// azureQueueDevAccountName and azureQueueDevAccountKey are Azurite's
// published well-known development storage account name/key, which
// gopherstack accepts as its default identity (see pkgs/azureauth and
// AZURE.md section 5) so that unmodified Azure SDKs pointed at this server
// work out of the box, the same way real SDKs work against Azurite with no
// configuration beyond the endpoint. Mirrors azureblob_test.go's identical
// constants.
const (
	azureQueueDevAccountName = "devstoreaccount1"
	azureQueueDevAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

// createAzureQueueClient returns an azure-sdk-for-go Queue service client
// pointed at the shared test container's dedicated Azure Queue port (see
// azureQueueEndpoint in main_test.go). Skips the calling test if that port
// could not be determined (mirrors createAzureBlobClient).
func createAzureQueueClient(t *testing.T) *azqueue.ServiceClient {
	t.Helper()

	if azureQueueEndpoint == "" {
		t.Skip("Azure Queue endpoint not available (mapped port could not be determined)")
	}

	cred, err := azqueue.NewSharedKeyCredential(azureQueueDevAccountName, azureQueueDevAccountKey)
	require.NoError(t, err, "unable to build SharedKeyCredential")

	// Path-style addressing (account name as the first path segment), matching
	// Azurite's own convention and gopherstack's single-account routing.
	client, err := azqueue.NewServiceClientWithSharedKeyCredential(
		azureQueueEndpoint+"/"+azureQueueDevAccountName, cred, nil,
	)
	require.NoError(t, err, "unable to construct Azure Queue service client")

	return client
}

func TestIntegration_AzureQueue_QueueAndMessageLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	service := createAzureQueueClient(t)
	ctx := t.Context()

	queueName := "test-queue-" + uuid.NewString()
	queueClient := service.NewQueueClient(queueName)

	// CreateQueue
	_, err := queueClient.Create(ctx, nil)
	require.NoError(t, err)

	// ListQueues: created queue should appear
	found := false

	pager := service.NewListQueuesPager(nil)
	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		for _, q := range page.Queues {
			if q.Name != nil && *q.Name == queueName {
				found = true
			}
		}
	}

	assert.True(t, found, "created queue should appear in ListQueues")

	// EnqueueMessage (Put Message)
	const messageText = "hello from gopherstack azurequeue"

	enqueueResp, err := queueClient.EnqueueMessage(ctx, messageText, nil)
	require.NoError(t, err)
	require.Len(t, enqueueResp.Messages, 1)

	// PeekMessages: message should be visible without being dequeued
	peekResp, err := queueClient.PeekMessages(ctx, nil)
	require.NoError(t, err)
	require.Len(t, peekResp.Messages, 1)
	assert.Equal(t, messageText, *peekResp.Messages[0].MessageText)

	// DequeueMessages (Get Messages): hides the message and assigns a pop receipt
	dequeueResp, err := queueClient.DequeueMessages(ctx, nil)
	require.NoError(t, err)
	require.Len(t, dequeueResp.Messages, 1)
	msg := dequeueResp.Messages[0]
	assert.Equal(t, messageText, *msg.MessageText)
	require.NotNil(t, msg.PopReceipt)

	// UpdateMessage: extend visibility
	newVisibilityTimeout := int32(30)

	updateResp, err := queueClient.UpdateMessage(ctx, *msg.MessageID, *msg.PopReceipt, messageText,
		&azqueue.UpdateMessageOptions{VisibilityTimeout: &newVisibilityTimeout})
	require.NoError(t, err)
	require.NotNil(t, updateResp.PopReceipt)

	// DeleteMessage using the rotated pop receipt from UpdateMessage
	_, err = queueClient.DeleteMessage(ctx, *msg.MessageID, *updateResp.PopReceipt, nil)
	require.NoError(t, err)

	// ClearMessages
	_, err = queueClient.EnqueueMessage(ctx, "another message", nil)
	require.NoError(t, err)
	_, err = queueClient.ClearMessages(ctx, nil)
	require.NoError(t, err)

	peekResp, err = queueClient.PeekMessages(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, peekResp.Messages, "queue should be empty after ClearMessages")

	// DeleteQueue
	_, err = queueClient.Delete(ctx, nil)
	require.NoError(t, err)

	// Verify gone
	found = false

	pager = service.NewListQueuesPager(nil)
	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		for _, q := range page.Queues {
			if q.Name != nil && *q.Name == queueName {
				found = true
			}
		}
	}

	assert.False(t, found, "deleted queue should no longer appear in ListQueues")
}
