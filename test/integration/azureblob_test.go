package integration_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// azureBlobDevAccountName and azureBlobDevAccountKey are Azurite's published
// well-known development storage account name/key, which gopherstack accepts
// as its default identity (see pkgs/azureauth and AZURE.md section 5) so
// that unmodified Azure SDKs pointed at this server work out of the box, the
// same way real SDKs work against Azurite with no configuration beyond the
// endpoint.
const (
	azureBlobDevAccountName = "devstoreaccount1"
	azureBlobDevAccountKey  = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

// createAzureBlobClient returns an azure-sdk-for-go Blob client pointed at
// the shared test container's dedicated Azure Blob port (see
// azureBlobEndpoint in main_test.go). Skips the calling test if that port
// could not be determined (mirrors how MQTT/IoT tests are skipped when
// mqttEndpoint is unavailable).
func createAzureBlobClient(t *testing.T) *azblob.Client {
	t.Helper()

	if azureBlobEndpoint == "" {
		t.Skip("Azure Blob endpoint not available (mapped port could not be determined)")
	}

	cred, err := azblob.NewSharedKeyCredential(azureBlobDevAccountName, azureBlobDevAccountKey)
	require.NoError(t, err, "unable to build SharedKeyCredential")

	// Path-style addressing (account name as the first path segment), matching
	// Azurite's own convention and gopherstack's single-account routing.
	client, err := azblob.NewClientWithSharedKeyCredential(
		azureBlobEndpoint+"/"+azureBlobDevAccountName, cred, nil,
	)
	require.NoError(t, err, "unable to construct Azure Blob client")

	return client
}

func TestIntegration_AzureBlob_ContainerAndBlobLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createAzureBlobClient(t)
	ctx := t.Context()

	containerName := "test-container-" + uuid.NewString()
	const blobName = "test-blob.txt"
	content := []byte("hello from gopherstack azureblob")

	// CreateContainer
	_, err := client.CreateContainer(ctx, containerName, nil)
	require.NoError(t, err)

	// PutBlob
	_, err = client.UploadBuffer(ctx, containerName, blobName, content, nil)
	require.NoError(t, err)

	// ListBlobs: uploaded blob should appear
	found := false

	pager := client.NewListBlobsFlatPager(containerName, nil)
	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		for _, b := range page.Segment.BlobItems {
			if b.Name != nil && *b.Name == blobName {
				found = true
			}
		}
	}

	assert.True(t, found, "uploaded blob should appear in ListBlobs")

	// GetBlob: downloaded bytes should round-trip exactly
	downloadResp, err := client.DownloadStream(ctx, containerName, blobName, nil)
	require.NoError(t, err)

	body, err := io.ReadAll(downloadResp.Body)
	_ = downloadResp.Body.Close()
	require.NoError(t, err)
	assert.True(t, bytes.Equal(content, body), "downloaded blob content should match uploaded content")

	// DeleteBlob
	_, err = client.DeleteBlob(ctx, containerName, blobName, nil)
	require.NoError(t, err)

	// DeleteContainer
	_, err = client.DeleteContainer(ctx, containerName, nil)
	require.NoError(t, err)
}

func TestIntegration_AzureBlob_ListContainers(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createAzureBlobClient(t)
	ctx := t.Context()

	containerName := "test-list-container-" + uuid.NewString()

	_, err := client.CreateContainer(ctx, containerName, nil)
	require.NoError(t, err)

	found := false

	pager := client.NewListContainersPager(nil)
	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr)

		for _, c := range page.ContainerItems {
			if c.Name != nil && *c.Name == containerName {
				found = true
			}
		}
	}

	assert.True(t, found, "created container should appear in ListContainers")

	_, err = client.DeleteContainer(ctx, containerName, nil)
	require.NoError(t, err)
}
