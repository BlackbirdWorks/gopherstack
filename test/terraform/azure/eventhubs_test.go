package azure_test

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureeventhubs"
)

const azureEventHubsFixture = `
resource "azurerm_resource_group" "eh" {
  name     = "gopherstack-eh-test-rg"
  location = "local"
}

resource "azurerm_eventhub_namespace" "eh" {
  name                = "gopherstackehns"
  location            = azurerm_resource_group.eh.location
  resource_group_name = azurerm_resource_group.eh.name
  sku                 = "Standard"
  capacity            = 1
}

resource "azurerm_eventhub" "eh" {
  name              = "tf-hub"
  namespace_id      = azurerm_eventhub_namespace.eh.id
  partition_count   = 2
  message_retention = 1
}

resource "azurerm_eventhub_consumer_group" "eh" {
  name                = "tf-consumers"
  namespace_name      = azurerm_eventhub_namespace.eh.name
  eventhub_name       = azurerm_eventhub.eh.name
  resource_group_name = azurerm_resource_group.eh.name
  user_metadata       = "terraform"
}

resource "azurerm_eventhub_authorization_rule" "eh" {
  name                = "tf-rule"
  namespace_name      = azurerm_eventhub_namespace.eh.name
  eventhub_name       = azurerm_eventhub.eh.name
  resource_group_name = azurerm_resource_group.eh.name
  listen              = true
  send                = true
  manage              = false
}
`

// TestTerraform_Azure_EventHubs applies a namespace, event hub, consumer group
// and authorization rule through the azurerm provider, requires an empty
// second plan, proves the hub is live over AMQP with the official SDK, then
// destroys everything.
func TestTerraform_Azure_EventHubs(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	dir := t.TempDir()
	hcl := azurermProviderBlock(endpoint) + azureEventHubsFixture

	applyAzureTofuNoDrift(t, dir, hcl)

	t.Run("terraform-created hub round-trips an event over AMQP", func(t *testing.T) {
		t.Parallel()
		verifyEventHubRoundTrip(t)
	})
}

func verifyEventHubRoundTrip(t *testing.T) {
	t.Helper()

	cs := "Endpoint=sb://localhost:" + hostPortEventHubsAMQP + ";SharedAccessKeyName=" + azureeventhubs.DefaultKeyName +
		";SharedAccessKey=" + azureeventhubs.DefaultKeyValue + ";UseDevelopmentEmulator=true"

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	producer, err := azeventhubs.NewProducerClientFromConnectionString(cs, "tf-hub", nil)
	require.NoError(t, err)

	defer producer.Close(context.Background())

	props, err := producer.GetEventHubProperties(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"0", "1"}, props.PartitionIDs)

	batch, err := producer.NewEventDataBatch(ctx, &azeventhubs.EventDataBatchOptions{PartitionID: new("1")})
	require.NoError(t, err)
	require.NoError(t, batch.AddEventData(&azeventhubs.EventData{Body: []byte("from terraform")}, nil))
	require.NoError(t, producer.SendEventDataBatch(ctx, batch, nil))

	consumer, err := azeventhubs.NewConsumerClientFromConnectionString(cs, "tf-hub", "tf-consumers", nil)
	require.NoError(t, err)

	defer consumer.Close(context.Background())

	pc, err := consumer.NewPartitionClient("1", &azeventhubs.PartitionClientOptions{
		StartPosition: azeventhubs.StartPosition{Earliest: new(true)},
	})
	require.NoError(t, err)

	defer pc.Close(context.Background())

	evs, err := pc.ReceiveEvents(ctx, 1, nil)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	assert.Equal(t, "from terraform", string(evs[0].Body))
}
