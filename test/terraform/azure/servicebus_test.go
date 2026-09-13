// M9: Microsoft.ServiceBus Terraform acceptance coverage.
//
// AZURE.md section 10.10's M9 entry requires proving that an unmodified
// hashicorp/azurerm provider can apply azurerm_servicebus_namespace/_queue/
// _topic/_subscription against services/azurearm's dedicated ServiceBus RP
// (rp_servicebus.go), and that the Terraform-created queue is genuinely live
// -- not just an ARM-side bookkeeping stub -- via a real REST
// send->peek-lock->complete round-trip against services/azureservicebus's
// own listener (M5), reached through the cross-service ServiceBusEntities
// adapter (cli.go's wireAzureARMResourceProviders).
package azure_test

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	azureServiceBusNamespaceName    = "gopherstackm9ns"
	azureServiceBusQueueName        = "m9-queue"
	azureServiceBusTopicName        = "m9-topic"
	azureServiceBusSubscriptionName = "m9-subscription"
)

// azureServiceBusFixture is the M9 acceptance fixture: a resource group, a
// Service Bus namespace, a queue, a topic, and a subscription on that topic.
const azureServiceBusFixture = `
resource "azurerm_resource_group" "m9" {
  name     = "gopherstack-m9-test-rg"
  location = "local"
}

resource "azurerm_servicebus_namespace" "m9" {
  name                = "gopherstackm9ns"
  resource_group_name = azurerm_resource_group.m9.name
  location            = azurerm_resource_group.m9.location
  sku                 = "Standard"
}

resource "azurerm_servicebus_queue" "m9" {
  namespace_id = azurerm_servicebus_namespace.m9.id
  name         = "m9-queue"

  lock_duration              = "PT30S"
  default_message_ttl        = "P1D"
  max_delivery_count         = 10
}

resource "azurerm_servicebus_topic" "m9" {
  namespace_id = azurerm_servicebus_namespace.m9.id
  name         = "m9-topic"

  default_message_ttl = "P1D"
}

resource "azurerm_servicebus_subscription" "m9" {
  topic_id = azurerm_servicebus_topic.m9.id
  name     = "m9-subscription"

  max_delivery_count = 5
  lock_duration       = "PT1M"
}
`

// sbBrokerProperties mirrors services/azureservicebus/handler.go's private
// brokerProperties wire shape -- test/integration/azureservicebus_test.go
// redeclares the same subset for the same reason (only HTTP is spoken here).
type sbBrokerProperties struct {
	MessageID string `json:"MessageId,omitempty"`
	LockToken string `json:"LockToken,omitempty"`
}

func serviceBusDataPlaneEndpoint() string {
	return "http://localhost:" + hostPortServiceBus
}

// sbDataPlaneRequest performs one Service Bus Brokered Messaging REST call
// directly against services/azureservicebus's own listener (not through
// ARM), mirroring test/integration/azureservicebus_test.go's sbRequest.
func sbDataPlaneRequest(t *testing.T, method, path string, body []byte) *http.Response {
	t.Helper()

	var bodyReader io.Reader
	if body != nil {
		bodyReader = strings.NewReader(string(body))
	}

	req, err := http.NewRequestWithContext(t.Context(), method, serviceBusDataPlaneEndpoint()+path, bodyReader)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	t.Cleanup(func() { _ = resp.Body.Close() })

	return resp
}

// TestTerraform_Azure_ServiceBus proves that an unmodified hashicorp/azurerm
// provider can create a Service Bus namespace/queue/topic/subscription
// entirely through Terraform against services/azurearm's ServiceBus RP, with
// the Terraform-created queue then proven genuinely live via a real REST
// send->peek-lock->complete round-trip against services/azureservicebus's
// own listener (AZURE.md section 10.10's M9 entry).
func TestTerraform_Azure_ServiceBus(t *testing.T) {
	t.Parallel()

	if sharedContainer == nil {
		t.Skip("azure terraform suite was skipped in TestMain (see its logged reason)")
	}

	dir := t.TempDir()
	hcl := azurermProviderBlock(endpoint) + azureServiceBusFixture

	applyAzureTofu(t, dir, hcl)

	// Both subtests exercise independent, already-Terraform-created
	// resources (the queue vs. the topic/subscription pair) against
	// services/azureservicebus's own listener, so -- unlike storage_dataplane_
	// test.go's SharedKey-auth-path subtest, which must observe traffic its
	// siblings already generated -- there's no ordering dependency between
	// them, and both can run in parallel.
	t.Run(
		"terraform-created queue send/peek-lock/complete round-trips against services/azureservicebus",
		func(t *testing.T) {
			t.Parallel()
			verifyServiceBusQueueRoundTrip(t)
		},
	)

	t.Run("terraform-created topic/subscription round-trips against services/azureservicebus", func(t *testing.T) {
		t.Parallel()
		verifyServiceBusTopicSubscriptionRoundTrip(t)
	})
}

// verifyServiceBusQueueRoundTrip sends a message to the Terraform-created
// queue, peek-locks it, and completes it -- proving the queue is genuinely
// live in services/azureservicebus (M5), not merely recorded as ARM
// bookkeeping.
func verifyServiceBusQueueRoundTrip(t *testing.T) {
	t.Helper()

	const messageBody = "hello from terraform via gopherstack M9"

	resp := sbDataPlaneRequest(t, http.MethodPost, "/"+azureServiceBusQueueName+"/messages", []byte(messageBody))
	require.Equal(t, http.StatusCreated, resp.StatusCode, "sending to the terraform-created queue")

	resp = sbDataPlaneRequest(t, http.MethodPost, "/"+azureServiceBusQueueName+"/messages/head", nil)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "peek-locking the terraform-created queue")

	gotBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, messageBody, string(gotBody))

	var bp sbBrokerProperties
	require.NoError(t, json.Unmarshal([]byte(resp.Header.Get("Brokerproperties")), &bp))
	require.NotEmpty(t, bp.LockToken)

	location := resp.Header.Get("Location")
	require.NotEmpty(t, location)

	resp = sbDataPlaneRequest(t, http.MethodDelete, location, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "completing the peek-locked message")
}

// verifyServiceBusTopicSubscriptionRoundTrip sends to the Terraform-created
// topic and receives it via the Terraform-created subscription -- proving
// both are genuinely live in services/azureservicebus, not just ARM
// bookkeeping.
func verifyServiceBusTopicSubscriptionRoundTrip(t *testing.T) {
	t.Helper()

	const messageBody = "fan-out message from terraform via gopherstack M9"

	resp := sbDataPlaneRequest(t, http.MethodPost, "/"+azureServiceBusTopicName+"/messages", []byte(messageBody))
	require.Equal(t, http.StatusCreated, resp.StatusCode, "sending to the terraform-created topic")

	subPath := "/" + azureServiceBusTopicName + "/subscriptions/" + azureServiceBusSubscriptionName + "/messages/head"

	resp = sbDataPlaneRequest(t, http.MethodPost, subPath, nil)
	require.Equal(t, http.StatusCreated, resp.StatusCode, "peek-locking via the terraform-created subscription")

	gotBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, messageBody, string(gotBody))

	location := resp.Header.Get("Location")
	require.NotEmpty(t, location)

	resp = sbDataPlaneRequest(t, http.MethodDelete, location, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "completing the peek-locked message")
}
