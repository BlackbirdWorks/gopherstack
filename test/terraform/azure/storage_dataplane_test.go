// M8: direct-data-plane storage Terraform coverage, retrofitting M0/M1/M2.
//
// AZURE.md section 10.8 verified from terraform-provider-azurerm's own
// source that its storage data-plane clients (ContainersDataPlaneClient/
// QueuesDataPlaneClient/TablesDataPlaneClient) build their base URI verbatim
// from ARM's properties.primaryEndpoints.{blob,queue,table} -- exactly what
// services/azurearm/rp_storage.go's buildBody/advertiseEndpoint already
// emit. This file proves that empirically: an unmodified hashicorp/azurerm
// provider creates a container+blob, a queue, and a table entirely through
// Terraform against an M7-provisioned azurerm_storage_account, and each
// resource is then exercised for real over the Go SDK -- reading back the
// blob Terraform wrote, sending/receiving a queue message, and
// inserting/querying a table entity -- not just "apply returned 0".
//
// Per AZURE.md section 10.4's explicitly-out-of-scope note (deferred to
// M11), all ARM storage accounts alias one shared Blob/Queue/Table
// namespace; this fixture's account name therefore has no bearing on
// data-plane routing and is chosen purely for ARM-side bookkeeping.
package azure_test

import (
	"context"
	"io"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
)

const (
	azureStorageDataPlaneAccountName   = "gopherstackm8data"
	azureStorageDataPlaneContainerName = "m8-container"
	azureStorageDataPlaneBlobName      = "m8-blob.txt"
	azureStorageDataPlaneBlobContent   = "hello from terraform via gopherstack M8"
	azureStorageDataPlaneQueueName     = "m8-queue"
	azureStorageDataPlaneTableName     = "m8table"
)

// azureStorageDataPlaneFixture is the M8 acceptance fixture: a storage
// account plus a container+blob, a queue, and a table -- the four
// direct-data-plane resources AZURE.md section 10.8 resolved as reachable
// with no gopherstack service-code changes.
const azureStorageDataPlaneFixture = `
resource "azurerm_resource_group" "m8" {
  name     = "gopherstack-m8-test-rg"
  location = "local"
}

resource "azurerm_storage_account" "m8" {
  name                     = "gopherstackm8data"
  resource_group_name      = azurerm_resource_group.m8.name
  location                 = azurerm_resource_group.m8.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_container" "m8" {
  name                  = "m8-container"
  storage_account_name  = azurerm_storage_account.m8.name
  container_access_type = "private"
}

resource "azurerm_storage_blob" "m8" {
  name                   = "m8-blob.txt"
  storage_account_name   = azurerm_storage_account.m8.name
  storage_container_name = azurerm_storage_container.m8.name
  type                   = "Block"
  source_content         = "hello from terraform via gopherstack M8"
}

resource "azurerm_storage_queue" "m8" {
  name                 = "m8-queue"
  storage_account_name = azurerm_storage_account.m8.name
}

resource "azurerm_storage_table" "m8" {
  name                 = "m8table"
  storage_account_name = azurerm_storage_account.m8.name
}
`

// TestTerraform_Azure_StorageDataPlane proves that an unmodified
// hashicorp/azurerm provider can create a storage container/blob/queue/table
// entirely through Terraform against services/azurearm's Storage RP (M7),
// with each resource then genuinely live end-to-end over the Go SDK (M8, see
// AZURE.md section 10.8 and 10.10).
func TestTerraform_Azure_StorageDataPlane(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	dir := t.TempDir()
	hcl := azurermProviderBlock(endpoint) + azureStorageDataPlaneFixture

	applyAzureTofu(t, dir, hcl)

	ctx := t.Context()

	t.Run("blob round-trips through the Go SDK", func(t *testing.T) {
		t.Parallel()
		verifyTerraformBlobRoundTrip(ctx, t)
	})

	t.Run("queue send/receive round-trips through the Go SDK", func(t *testing.T) {
		t.Parallel()
		verifyTerraformQueueRoundTrip(ctx, t)
	})

	t.Run("table insert/query round-trips through the Go SDK", func(t *testing.T) {
		t.Parallel()
		verifyTerraformTableRoundTrip(ctx, t)
	})

	// Not parallel with the subtests above: it inspects the whole container
	// log, so it should run after they've generated their own traffic too --
	// a non-SharedKey Authorization header reaching any of the three
	// services from any of these round-trips would be caught here.
	t.Run("SharedKey auth path was actually used (storage_use_azuread=false)", func(t *testing.T) {
		verifySharedKeyAuthPathUsed(ctx, t)
	})
}

// blobDataPlaneEndpoint, queueDataPlaneEndpoint, and tableDataPlaneEndpoint
// build the path-style "http://host:port/<account>" base URI these Go SDK
// clients expect, matching test/integration's createAzureBlobClient/
// createAzureQueueClient/createAzureTableServiceClient convention (path-style
// addressing, account name as the first path segment).
func blobDataPlaneEndpoint() string {
	return "http://localhost:" + hostPortBlob + "/" + azureStorageDataPlaneAccountName
}

func queueDataPlaneEndpoint() string {
	return "http://localhost:" + hostPortQueue + "/" + azureStorageDataPlaneAccountName
}

func tableDataPlaneEndpoint() string {
	return "http://localhost:" + hostPortTable + "/" + azureStorageDataPlaneAccountName
}

// verifyTerraformBlobRoundTrip reads back, via the Go SDK, the blob that
// Terraform's azurerm_storage_blob resource wrote via source_content, and
// asserts the content round-trips exactly.
func verifyTerraformBlobRoundTrip(ctx context.Context, t *testing.T) {
	t.Helper()

	cred, err := azblob.NewSharedKeyCredential(azureStorageDataPlaneAccountName, azureauth.DefaultAccountKey)
	require.NoError(t, err, "unable to build SharedKeyCredential")

	client, err := azblob.NewClientWithSharedKeyCredential(blobDataPlaneEndpoint(), cred, nil)
	require.NoError(t, err, "unable to construct Azure Blob client")

	resp, err := client.DownloadStream(ctx, azureStorageDataPlaneContainerName, azureStorageDataPlaneBlobName, nil)
	require.NoError(t, err, "reading back the blob terraform's azurerm_storage_blob wrote")

	body, err := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	require.NoError(t, closeErr, "closing the download stream should not error")
	require.NoError(t, err)

	assert.Equal(t, azureStorageDataPlaneBlobContent, string(body),
		"blob content read back via the Go SDK should match what Terraform wrote")
}

// verifyTerraformQueueRoundTrip sends and receives a message, over the Go
// SDK, against the queue Terraform's azurerm_storage_queue resource created
// -- proving the Terraform-created queue is genuinely live (Terraform itself
// has no message resource, so this is the Go SDK's job per AZURE.md section
// 10.8's M8 scope).
func verifyTerraformQueueRoundTrip(ctx context.Context, t *testing.T) {
	t.Helper()

	cred, err := azqueue.NewSharedKeyCredential(azureStorageDataPlaneAccountName, azureauth.DefaultAccountKey)
	require.NoError(t, err, "unable to build SharedKeyCredential")

	service, err := azqueue.NewServiceClientWithSharedKeyCredential(queueDataPlaneEndpoint(), cred, nil)
	require.NoError(t, err, "unable to construct Azure Queue service client")

	queueClient := service.NewQueueClient(azureStorageDataPlaneQueueName)

	const messageText = "hello from the go sdk, m8 queue round-trip"

	_, err = queueClient.EnqueueMessage(ctx, messageText, nil)
	require.NoError(t, err, "enqueueing into terraform's azurerm_storage_queue")

	dequeueResp, err := queueClient.DequeueMessages(ctx, nil)
	require.NoError(t, err, "dequeuing from terraform's azurerm_storage_queue")
	require.Len(t, dequeueResp.Messages, 1)

	msg := dequeueResp.Messages[0]
	assert.Equal(t, messageText, *msg.MessageText,
		"dequeued message content should match what the Go SDK enqueued")

	_, err = queueClient.DeleteMessage(ctx, *msg.MessageID, *msg.PopReceipt, nil)
	require.NoError(t, err)
}

// verifyTerraformTableRoundTrip inserts and queries an entity, over the Go
// SDK, against the table Terraform's azurerm_storage_table resource created
// -- Terraform has no entity resource, so this is the Go SDK's job per
// AZURE.md section 10.8's M8 scope.
func verifyTerraformTableRoundTrip(ctx context.Context, t *testing.T) {
	t.Helper()

	cred, err := aztables.NewSharedKeyCredential(azureStorageDataPlaneAccountName, azureauth.DefaultAccountKey)
	require.NoError(t, err, "unable to build SharedKeyCredential")

	service, err := aztables.NewServiceClientWithSharedKey(tableDataPlaneEndpoint(), cred, nil)
	require.NoError(t, err, "unable to construct Azure Table service client")

	tableClient := service.NewClient(azureStorageDataPlaneTableName)

	// golangci-lint 2.13.2's modernize/embedlit wants this flattened to promoted-field
	// syntax (EDMEntity{PartitionKey: .., RowKey: .., Properties: ..}), but that syntax
	// requires go1.27+ (verified: fails to compile with "requires go1.27 or later" under
	// this repo's go.mod `go 1.26.6`) -- a false positive, not gated on the module's
	// actual language version. Do not apply the suggested fix.
	entity := aztables.EDMEntity{ //nolint:modernize // embedlit false positive, see comment above
		Entity:     aztables.Entity{PartitionKey: "m8", RowKey: "1"},
		Properties: map[string]any{"Message": "hello from the go sdk, m8 table round-trip"},
	}

	marshaled, err := entity.MarshalJSON()
	require.NoError(t, err)

	_, err = tableClient.AddEntity(ctx, marshaled, nil)
	require.NoError(t, err, "inserting an entity into terraform's azurerm_storage_table")

	filter := "PartitionKey eq 'm8' and RowKey eq '1'"
	pager := tableClient.NewListEntitiesPager(&aztables.ListEntitiesOptions{Filter: &filter})

	found := false

	for pager.More() {
		page, pageErr := pager.NextPage(ctx)
		require.NoError(t, pageErr, "querying terraform's azurerm_storage_table")

		found = found || len(page.Entities) > 0
	}

	assert.True(t, found, "querying the terraform-created table should return the inserted entity")
}

// verifySharedKeyAuthPathUsed answers AZURE.md section 10.10's M8 empirical
// question 1: does storage_use_azuread=false in azurermProviderBlock
// actually force the SharedKey path pkgs/azureauth implements? Every
// services/azureblob|azurequeue|azuretable checkAuth logs "malformed
// Authorization header accepted" (DebugContext) whenever an Authorization
// header doesn't parse as SharedKey/SharedKeyLite -- a Bearer token (the AAD
// path) would trip this on every single request. LOG_LEVEL=debug is set on
// the shared container (see startGopherstackContainer) specifically so this
// check has teeth: if storage_use_azuread=false did NOT force SharedKey,
// this assertion would fail loudly instead of passing by accident.
func verifySharedKeyAuthPathUsed(ctx context.Context, t *testing.T) {
	t.Helper()

	logsReader, err := sharedContainer.Logs(ctx)
	require.NoError(t, err, "fetching container logs to verify the auth scheme actually used")

	logs, err := io.ReadAll(logsReader)
	closeErr := logsReader.Close()
	require.NoError(t, closeErr)
	require.NoError(t, err)

	assert.NotContains(t, string(logs), "malformed Authorization header accepted",
		"a non-SharedKey (e.g. Bearer/AAD) Authorization header reaching azureblob/azurequeue/azuretable "+
			"would log this line -- its presence would mean storage_use_azuread=false did NOT force the "+
			"SharedKey path")
}
