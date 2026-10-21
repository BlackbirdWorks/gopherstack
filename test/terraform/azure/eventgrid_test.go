package azure_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/eventgrid/azeventgrid"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
)

const (
	azureEventGridAccountName = "gopherstackegdata"
	azureEventGridQueueName   = "eg-events"
	azureEventGridTopicName   = "gopherstack-eg-topic"
	azureEventGridTopicPath   = "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/" +
		"gopherstack-eg-test-rg/providers/Microsoft.EventGrid/topics/" + azureEventGridTopicName
)

const azureEventGridFixture = `
resource "azurerm_resource_group" "eg" {
  name     = "gopherstack-eg-test-rg"
  location = "local"
}

resource "azurerm_storage_account" "eg" {
  name                     = "gopherstackegdata"
  resource_group_name      = azurerm_resource_group.eg.name
  location                 = azurerm_resource_group.eg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

resource "azurerm_storage_queue" "eg" {
  name                 = "eg-events"
  storage_account_name = azurerm_storage_account.eg.name
}

resource "azurerm_storage_container" "eg" {
  name                  = "eg-dead"
  storage_account_name  = azurerm_storage_account.eg.name
  container_access_type = "private"
}

resource "azurerm_eventgrid_topic" "eg" {
  name                = "gopherstack-eg-topic"
  location            = azurerm_resource_group.eg.location
  resource_group_name = azurerm_resource_group.eg.name
}

resource "azurerm_eventgrid_event_subscription" "eg" {
  name  = "tf-sub"
  scope = azurerm_eventgrid_topic.eg.id

  included_event_types = ["App.Thing"]
  labels               = ["terraform"]

  storage_queue_endpoint {
    storage_account_id = azurerm_storage_account.eg.id
    queue_name         = azurerm_storage_queue.eg.name
  }

  retry_policy {
    max_delivery_attempts = 5
    event_time_to_live    = 60
  }

  storage_blob_dead_letter_destination {
    storage_account_id          = azurerm_storage_account.eg.id
    storage_blob_container_name = azurerm_storage_container.eg.name
  }
}
`

// TestTerraform_Azure_EventGrid applies a topic and an event subscription delivering to
// a storage queue, requires an empty second plan, publishes through the official SDK
// with the topic key ARM hands out, observes the event in the queue, then destroys.
func TestTerraform_Azure_EventGrid(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	dir := t.TempDir()
	hcl := azurermProviderBlock(endpoint) + azureEventGridFixture

	applyAzureTofuNoDrift(t, dir, hcl)

	t.Run("published event reaches the storage queue", func(t *testing.T) {
		t.Parallel()
		verifyEventGridRoundTrip(t)
	})
}

func armHTTPClient(t *testing.T) *http.Client {
	t.Helper()

	pem, err := os.ReadFile(certPEMPath)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pem))

	return &http.Client{
		Timeout:   30 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
	}
}

func armCall(ctx context.Context, t *testing.T, c *http.Client, method, path string) map[string]any {
	t.Helper()

	req, err := http.NewRequestWithContext(ctx, method, "https://"+endpoint+path+"?api-version=2025-02-15",
		bytes.NewReader(nil))
	require.NoError(t, err)

	req.Header.Set("Authorization", "Bearer gopherstack")

	resp, err := c.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))

	var out map[string]any

	require.NoError(t, json.Unmarshal(raw, &out))

	return out
}

func verifyEventGridRoundTrip(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	c := armHTTPClient(t)
	topic := armCall(ctx, t, c, http.MethodGet, azureEventGridTopicPath)
	topicEndpoint, _ := topic["properties"].(map[string]any)["endpoint"].(string)
	require.Equal(t, "https://localhost:"+hostPortEventGrid+"/api/events", topicEndpoint)

	keys := armCall(ctx, t, c, http.MethodPost, azureEventGridTopicPath+"/listKeys")
	key, _ := keys["key1"].(string)
	require.NotEmpty(t, key)

	opts := &azeventgrid.ClientOptions{}
	opts.Transport = c

	pub, err := azeventgrid.NewClientWithSharedKeyCredential(topicEndpoint, azcore.NewKeyCredential(key), opts)
	require.NoError(t, err)

	now := time.Now().UTC()
	_, err = pub.PublishEvents(ctx, []azeventgrid.Event{
		{
			ID: new("skipped"), Subject: new("/things/0"), EventType: new("Other.Thing"),
			DataVersion: new("1"), EventTime: &now, Data: map[string]any{"n": 0},
		},
		{
			ID: new("from-terraform"), Subject: new("/things/1"), EventType: new("App.Thing"),
			DataVersion: new("1"), EventTime: &now, Data: map[string]any{"n": 1},
		},
	}, nil)
	require.NoError(t, err)

	cred, err := azqueue.NewSharedKeyCredential(azureEventGridAccountName, azureauth.DefaultAccountKey)
	require.NoError(t, err)

	svc, err := azqueue.NewServiceClientWithSharedKeyCredential(
		"http://localhost:"+hostPortQueue+"/"+azureEventGridAccountName, cred, nil)
	require.NoError(t, err)

	q := svc.NewQueueClient(azureEventGridQueueName)

	var text string

	require.Eventually(t, func() bool {
		resp, deqErr := q.DequeueMessages(ctx, nil)
		if deqErr != nil || len(resp.Messages) == 0 {
			return false
		}

		text = *resp.Messages[0].MessageText

		return true
	}, 60*time.Second, 500*time.Millisecond)

	var got map[string]any

	require.NoError(t, json.Unmarshal([]byte(text), &got))
	assert.Equal(t, "from-terraform", got["id"])
	assert.Equal(t, "App.Thing", got["eventType"])
	assert.Contains(t, got["topic"], "Microsoft.EventGrid/topics/"+azureEventGridTopicName)

	more, err := q.DequeueMessages(ctx, nil)
	require.NoError(t, err)
	assert.Empty(t, more.Messages, "the filtered-out event must not be delivered")
}
