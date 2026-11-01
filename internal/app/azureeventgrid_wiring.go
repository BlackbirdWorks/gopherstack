package app

import (
	"context"
	"errors"
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
	wireAzureContainerRegistry(byName)
	wireAzureFunctions(byName)
	wireAzureFile(byName)
	wireAzureMonitor(byName)
	wireAzureGraph(byName)
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
		armH.SetResourceObserver(&azureARMEventSource{backend: egH.Backend})
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

func (a *azureEventGridAdapter) PutDomain(
	name, resourceID, schema string, autoCreate, autoDelete *bool,
) (azurearm.EventGridDomain, error) {
	info, err := a.backend.PutDomain(name, resourceID, schema, autoCreate, autoDelete)
	if err != nil {
		return azurearm.EventGridDomain{}, err
	}

	return domainView(info), nil
}

func domainView(info azureeventgrid.DomainInfo) azurearm.EventGridDomain {
	return azurearm.EventGridDomain{
		CreatedAt:   info.CreatedAt,
		InputSchema: info.InputSchema,
		AutoCreate:  info.AutoCreate,
		AutoDelete:  info.AutoDelete,
	}
}

func (a *azureEventGridAdapter) DomainInfo(name string) (azurearm.EventGridDomain, bool) {
	info, err := a.backend.Domain(name)
	if err != nil {
		return azurearm.EventGridDomain{}, false
	}

	return domainView(info), true
}

func (a *azureEventGridAdapter) DeleteDomain(name string) error {
	return ignoreMissing(a.backend.DeleteDomain(name), azureeventgrid.ErrDomainNotFound)
}

func (a *azureEventGridAdapter) DomainKeys(name string) (string, string, error) {
	return keyPair(a.backend.DomainKeys(name))
}

func (a *azureEventGridAdapter) RegenerateDomainKey(name, keyName string) (string, string, error) {
	return keyPair(a.backend.RegenerateDomainKey(name, keyName))
}

func (a *azureEventGridAdapter) PutDomainTopic(domain, name string) (time.Time, error) {
	return a.backend.PutDomainTopic(domain, name)
}

func (a *azureEventGridAdapter) DomainTopic(domain, name string) (azurearm.EventGridDomainTopic, bool) {
	t, err := a.backend.DomainTopic(domain, name)

	return azurearm.EventGridDomainTopic{CreatedAt: t.CreatedAt, Name: t.Name}, err == nil
}

func (a *azureEventGridAdapter) DomainTopics(domain string) ([]azurearm.EventGridDomainTopic, error) {
	topics, err := a.backend.DomainTopics(domain)
	if err != nil {
		return nil, err
	}

	out := make([]azurearm.EventGridDomainTopic, 0, len(topics))
	for _, t := range topics {
		out = append(out, azurearm.EventGridDomainTopic{CreatedAt: t.CreatedAt, Name: t.Name})
	}

	return out, nil
}

func (a *azureEventGridAdapter) DeleteDomainTopic(domain, name string) error {
	return ignoreMissing(
		a.backend.DeleteDomainTopic(domain, name),
		azureeventgrid.ErrDomainTopicNotFound, azureeventgrid.ErrDomainNotFound,
	)
}

func (a *azureEventGridAdapter) PutScopedSubscription(
	ctx context.Context, domain, topic, name string, props map[string]any,
) (map[string]any, error) {
	out, err := a.backend.PutScopedSubscription(ctx, azureeventgrid.Scope{Domain: domain, Topic: topic}, name, props)

	return out, mapDomainErr(err)
}

func (a *azureEventGridAdapter) PatchScopedSubscription(
	ctx context.Context, domain, topic, name string, patch map[string]any,
) (map[string]any, error) {
	out, err := a.backend.PatchScopedSubscription(ctx, azureeventgrid.Scope{Domain: domain, Topic: topic}, name, patch)

	return out, mapSubscriptionErr(err)
}

func (a *azureEventGridAdapter) PatchResourceSubscription(
	ctx context.Context, scopeID, name string, patch map[string]any,
) (map[string]any, error) {
	out, err := a.backend.PatchScopedSubscription(ctx, azureeventgrid.Scope{Resource: scopeID}, name, patch)

	return out, mapSubscriptionErr(err)
}

func mapSubscriptionErr(err error) error {
	switch {
	case errors.Is(err, azureeventgrid.ErrSubscriptionNotFound):
		return azurearm.ErrEventGridSubscriptionNotFound
	case errors.Is(err, azureeventgrid.ErrTopicNotFound):
		return azurearm.ErrEventGridTopicNotFound
	}

	return mapDomainErr(err)
}

// mapDomainErr turns a missing domain or domain topic into the ARM provider's not-found errors.
func mapDomainErr(err error) error {
	switch {
	case errors.Is(err, azureeventgrid.ErrDomainTopicNotFound):
		return azurearm.ErrEventGridDomainTopicNotFound
	case errors.Is(err, azureeventgrid.ErrDomainNotFound):
		return azurearm.ErrEventGridDomainNotFound
	}

	return err
}

func (a *azureEventGridAdapter) GetScopedSubscription(domain, topic, name string) (map[string]any, bool) {
	props, err := a.backend.ScopedSubscription(azureeventgrid.Scope{Domain: domain, Topic: topic}, name)

	return props, err == nil
}

func (a *azureEventGridAdapter) DeleteScopedSubscription(domain, topic, name string) error {
	return ignoreMissing(
		a.backend.DeleteScopedSubscription(azureeventgrid.Scope{Domain: domain, Topic: topic}, name),
		azureeventgrid.ErrSubscriptionNotFound, azureeventgrid.ErrTopicNotFound,
		azureeventgrid.ErrDomainTopicNotFound, azureeventgrid.ErrDomainNotFound,
	)
}

func (a *azureEventGridAdapter) ScopedSubscriptionFullURL(domain, topic, name string) (string, error) {
	return a.backend.ScopedFullURL(azureeventgrid.Scope{Domain: domain, Topic: topic}, name)
}

func (a *azureEventGridAdapter) DeliveryAttributes(domain, topic, name string) ([]map[string]any, error) {
	return a.backend.DeliveryAttributes(azureeventgrid.Scope{Domain: domain, Topic: topic}, name)
}

func (a *azureEventGridAdapter) PutResourceSubscription(
	ctx context.Context, scopeID, name string, props map[string]any,
) (map[string]any, error) {
	return a.backend.PutScopedSubscription(ctx, azureeventgrid.Scope{Resource: scopeID}, name, props)
}

func (a *azureEventGridAdapter) GetResourceSubscription(scopeID, name string) (map[string]any, bool) {
	props, err := a.backend.ScopedSubscription(azureeventgrid.Scope{Resource: scopeID}, name)

	return props, err == nil
}

func (a *azureEventGridAdapter) DeleteResourceSubscription(scopeID, name string) error {
	return ignoreMissing(
		a.backend.DeleteScopedSubscription(azureeventgrid.Scope{Resource: scopeID}, name),
		azureeventgrid.ErrSubscriptionNotFound,
	)
}

func (a *azureEventGridAdapter) ResourceSubscriptionFullURL(scopeID, name string) (string, error) {
	return a.backend.ScopedFullURL(azureeventgrid.Scope{Resource: scopeID}, name)
}

func (a *azureEventGridAdapter) ResourceDeliveryAttributes(scopeID, name string) ([]map[string]any, error) {
	return a.backend.DeliveryAttributes(azureeventgrid.Scope{Resource: scopeID}, name)
}

// azureARMEventSource turns ARM write, delete and action results into Event Grid resource events.
type azureARMEventSource struct {
	backend *azureeventgrid.InMemoryBackend
}

var eventTypeByChange = map[azurearm.ResourceChangeKind]string{ //nolint:gochecknoglobals // static lookup
	azurearm.ResourceWritten:   azureeventgrid.EventResourceWriteSuccess,
	azurearm.ResourceDeleted:   azureeventgrid.EventResourceDeleteSuccess,
	azurearm.ResourceActionRun: azureeventgrid.EventResourceActionSuccess,
}

func (s *azureARMEventSource) ResourceChanged(ch azurearm.ResourceChange) {
	s.backend.PublishResourceEvent(azureeventgrid.ResourceEvent{
		Time: ch.Time, Claims: ch.Claims, EventType: eventTypeByChange[ch.Kind],
		SubscriptionID: ch.SubscriptionID, ResourceGroup: ch.ResourceGroup, ResourceURI: ch.ResourceURI,
		Provider: ch.Provider, OperationName: ch.OperationName, CorrelationID: ch.CorrelationID,
		TenantID: ch.TenantID, HTTPMethod: ch.HTTPMethod, HTTPURL: ch.HTTPURL,
		ClientRequestID: ch.ClientRequestID, ClientIP: ch.ClientIP,
	})
}
