package app

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azureeventgrid"
	"github.com/blackbirdworks/gopherstack/services/azureeventhubs"
	"github.com/blackbirdworks/gopherstack/services/azurequeue"
	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
)

// wireAzureARMAndEventGrid wires ARM's resource providers (Service Bus, Key Vault, Cosmos DB,
// Event Hubs), then Event Grid and App Configuration.
func wireAzureARMAndEventGrid(byName map[string]service.Registerable) {
	wireAzureARMResourceProviders(
		byName["AzureARM"], byName["AzureServiceBus"], byName["AzureKeyVault"], byName["CosmosDB"],
		byName["AzureEventHubs"],
	)
	wireAzureEventGrid(byName)
	wireAzureAppConfig(byName)
}

// wireAzureEventGrid connects Event Grid to ARM's Microsoft.EventGrid provider and to
// the in-emulator delivery sinks (Service Bus, Event Hubs, Storage queues, blobs).
func wireAzureEventGrid(byName map[string]service.Registerable) {
	egH, ok := byName["AzureEventGrid"].(*azureeventgrid.Handler)
	if !ok {
		return
	}

	if armH, armOk := byName["AzureARM"].(*azurearm.Handler); armOk {
		armH.Registry.SetEventGrid(&azureEventGridAdapter{backend: egH.Backend})
	}

	t := &azureEventGridTargets{}

	if h, hOk := byName["AzureServiceBus"].(*azureservicebus.Handler); hOk {
		t.serviceBus = h.Backend
	}

	if h, hOk := byName["AzureQueue"].(*azurequeue.Handler); hOk {
		t.queues = h.Backend
	}

	if h, hOk := byName["AzureBlob"].(*azureblob.Handler); hOk {
		t.blobs = h.Backend
	}

	if h, hOk := byName["AzureEventHubs"].(*azureeventhubs.Handler); hOk {
		t.eventHubs = h.Backend
	}

	egH.Backend.SetTargets(t)
}

// azureEventGridTargets delivers Event Grid events into sibling Azure emulators.
type azureEventGridTargets struct {
	serviceBus azureservicebus.StorageBackend
	queues     azurequeue.StorageBackend
	blobs      azureblob.StorageBackend
	eventHubs  *azureeventhubs.InMemoryBackend
}

func (t *azureEventGridTargets) SendServiceBus(queue, topic string, m azureeventgrid.Message) error {
	if t.serviceBus == nil {
		return azureeventgrid.ErrTargetUnavailable
	}

	ref := azureservicebus.EntityRef{Queue: queue, Topic: topic}
	_, err := t.serviceBus.Send(ref, azureservicebus.NewMessage{
		Body: m.Body, ContentType: m.ContentType, MessageID: m.ID, Label: m.Subject,
	})

	return err
}

func (t *azureEventGridTargets) SendEventHub(namespace, hub string, m azureeventgrid.Message) error {
	if t.eventHubs == nil {
		return azureeventgrid.ErrTargetUnavailable
	}

	raw, err := (&amqp1.Message{
		Data: [][]byte{m.Body}, Properties: &amqp1.Properties{ContentType: m.ContentType},
	}).Encode()
	if err != nil {
		return err
	}

	_, err = t.eventHubs.Append(
		azureeventhubs.HubRef{Namespace: namespace, Hub: hub}, -1, []azureeventhubs.NewEvent{{Raw: raw}},
	)

	return err
}

func (t *azureEventGridTargets) SendStorageQueue(_, queue, text string, ttl time.Duration) error {
	if t.queues == nil {
		return azureeventgrid.ErrTargetUnavailable
	}

	_, err := t.queues.PutMessage(queue, text, 0, ttl)

	return err
}

func (t *azureEventGridTargets) PutBlob(_, container, blob string, data []byte, contentType string) error {
	if t.blobs == nil {
		return azureeventgrid.ErrTargetUnavailable
	}

	_, err := t.blobs.PutBlobWith(container, blob, data, azureblob.BlobAttrs{ContentType: contentType})

	return err
}

// azureEventGridAdapter adapts services/azureeventgrid's backend to services/azurearm.EventGrid.
type azureEventGridAdapter struct {
	backend *azureeventgrid.InMemoryBackend
}

func (a *azureEventGridAdapter) PutTopic(name, resourceID, schema string) (azurearm.EventGridTopic, error) {
	info, err := a.backend.PutTopic(name, resourceID, schema)
	if err != nil {
		return azurearm.EventGridTopic{}, err
	}

	return azurearm.EventGridTopic{CreatedAt: info.CreatedAt, InputSchema: info.InputSchema}, nil
}

func (a *azureEventGridAdapter) TopicInfo(name string) (azurearm.EventGridTopic, bool) {
	info, err := a.backend.Topic(name)
	if err != nil {
		return azurearm.EventGridTopic{}, false
	}

	return azurearm.EventGridTopic{CreatedAt: info.CreatedAt, InputSchema: info.InputSchema}, true
}

func (a *azureEventGridAdapter) DeleteTopic(name string) error {
	return ignoreMissing(a.backend.DeleteTopic(name), azureeventgrid.ErrTopicNotFound)
}

func keyPair(k azureeventgrid.Keys, err error) (string, string, error) {
	return k.Key1, k.Key2, err
}

func (a *azureEventGridAdapter) TopicKeys(name string) (string, string, error) {
	return keyPair(a.backend.TopicKeys(name))
}

func (a *azureEventGridAdapter) RegenerateTopicKey(name, keyName string) (string, string, error) {
	return keyPair(a.backend.RegenerateTopicKey(name, keyName))
}

func (a *azureEventGridAdapter) PutSubscription(
	ctx context.Context, topic, name string, props map[string]any,
) (map[string]any, error) {
	return a.backend.PutSubscription(ctx, topic, name, props)
}

func (a *azureEventGridAdapter) GetSubscription(topic, name string) (map[string]any, bool) {
	props, err := a.backend.Subscription(topic, name)

	return props, err == nil
}

func (a *azureEventGridAdapter) DeleteSubscription(topic, name string) error {
	return ignoreMissing(
		a.backend.DeleteSubscription(topic, name),
		azureeventgrid.ErrSubscriptionNotFound, azureeventgrid.ErrTopicNotFound,
	)
}

func (a *azureEventGridAdapter) SubscriptionFullURL(topic, name string) (string, error) {
	return a.backend.FullURL(topic, name)
}

func (a *azureEventGridAdapter) PutNamespace(name string) error { return a.backend.PutNamespace(name) }

func (a *azureEventGridAdapter) DeleteNamespace(name string) error {
	return ignoreMissing(a.backend.DeleteNamespace(name), azureeventgrid.ErrNamespaceNotFound)
}

func (a *azureEventGridAdapter) NamespaceKeys(name string) (string, string, error) {
	return keyPair(a.backend.NamespaceKeys(name))
}

func (a *azureEventGridAdapter) RegenerateNamespaceKey(name, keyName string) (string, string, error) {
	return keyPair(a.backend.RegenerateNamespaceKey(name, keyName))
}

func (a *azureEventGridAdapter) PutNamespaceTopic(ns, name string) (time.Time, error) {
	return a.backend.PutNamespaceTopic(ns, name)
}

func (a *azureEventGridAdapter) NamespaceTopicCreated(ns, name string) (time.Time, bool) {
	created, err := a.backend.NamespaceTopicCreated(ns, name)

	return created, err == nil
}

func (a *azureEventGridAdapter) DeleteNamespaceTopic(ns, name string) error {
	return ignoreMissing(
		a.backend.DeleteNamespaceTopic(ns, name),
		azureeventgrid.ErrNamespaceTopicNotFound, azureeventgrid.ErrNamespaceNotFound,
	)
}

func (a *azureEventGridAdapter) PutPullSubscription(
	ns, topic, name string, props map[string]any,
) (map[string]any, error) {
	return a.backend.PutPullSubscription(ns, topic, name, props)
}

func (a *azureEventGridAdapter) GetPullSubscription(ns, topic, name string) (map[string]any, bool) {
	props, _, err := a.backend.PullSubscription(ns, topic, name)

	return props, err == nil
}

func (a *azureEventGridAdapter) DeletePullSubscription(ns, topic, name string) error {
	return ignoreMissing(
		a.backend.DeletePullSubscription(ns, topic, name),
		azureeventgrid.ErrPullSubNotFound, azureeventgrid.ErrNamespaceTopicNotFound,
		azureeventgrid.ErrNamespaceNotFound,
	)
}
