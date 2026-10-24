package azurearm

import (
	"context"
	"strings"
	"time"
)

const (
	egPropEndpoint  = "endpoint"
	egPropSchema    = "inputSchema"
	egKey1          = "key1"
	egKey2          = "key2"
	egPropLocalAuth = "disableLocalAuth"
	egPropCapacity  = "capacity"

	egPropAutoCreate = "autoCreateTopicWithFirstSubscription"
	egPropAutoDelete = "autoDeleteTopicWithLastSubscription"
)

func (p *EventGridProvider) bodyOf(id ResourceID, rec *storedEG) (map[string]any, error) {
	switch rec.kind {
	case egKindTopic:
		return p.topicBody(id, rec)
	case egKindTopicSub:
		return p.subscriptionBody(id, rec)
	case egKindNamespace:
		return p.namespaceBody(id, rec), nil
	case egKindNsTopic:
		return p.nsTopicBody(id, rec)
	case egKindNsSub:
		return p.nsSubBody(id, rec)
	case egKindDomain:
		return p.domainBody(id, rec)
	case egKindDomainTopic:
		return egEnvelope(id, rec, map[string]any{fieldProvisioningState: provisioningStateSucceeded}), nil
	case egKindDomainSub, egKindDomainTopicSub:
		return p.scopedSubscriptionBody(id, rec)
	case egKindScopeSub:
		return p.scopeSubBody(rec)
	case egKindUnsupported:
	}

	return nil, errUnsupportedEventGridType(id)
}

func egEnvelope(id ResourceID, rec *storedEG, props map[string]any) map[string]any {
	out := map[string]any{
		"id": id.ARMID(), fieldName: rec.name, fieldType: egTypeName(rec.kind), fieldProperties: props,
	}

	if rec.kind == egKindTopic || rec.kind == egKindNamespace || rec.kind == egKindDomain {
		out[fieldLocation], out[fieldTags] = rec.location, tagsOrEmpty(rec.tags)
	}

	return out
}

func (p *EventGridProvider) topicBody(id ResourceID, rec *storedEG) (map[string]any, error) {
	info, ok := p.dataPlane.TopicInfo(rec.name)
	if !ok {
		return nil, ErrEventGridTopicNotFound
	}

	props := ehMerge(map[string]any{
		cosmosFieldPublicNetwork: kvPublicNetworkEnabled, egPropLocalAuth: false,
		"dataResidencyBoundary": "WithinGeopair",
	}, rec.extra, map[string]any{
		fieldProvisioningState: provisioningStateSucceeded,
		egPropEndpoint:         "https://" + p.hostPort(rec) + "/api/events",
		egPropSchema:           info.InputSchema,
		"metricResourceId":     id.SubscriptionID + ":" + strings.ToLower(rec.name),
	})

	return egEnvelope(id, rec, props), nil
}

func (p *EventGridProvider) domainBody(id ResourceID, rec *storedEG) (map[string]any, error) {
	info, ok := p.dataPlane.DomainInfo(rec.name)
	if !ok {
		return nil, ErrEventGridDomainNotFound
	}

	props := ehMerge(map[string]any{
		cosmosFieldPublicNetwork: kvPublicNetworkEnabled, egPropLocalAuth: false,
		"dataResidencyBoundary": "WithinGeopair",
	}, rec.extra, map[string]any{
		fieldProvisioningState: provisioningStateSucceeded,
		egPropEndpoint:         "https://" + p.hostPort(rec) + "/api/events",
		egPropSchema:           info.InputSchema,
		"metricResourceId":     id.SubscriptionID + ":" + strings.ToLower(rec.name),
		egPropAutoCreate:       info.AutoCreate,
		egPropAutoDelete:       info.AutoDelete,
	})

	return egEnvelope(id, rec, props), nil
}

func (p *EventGridProvider) scopedSubscriptionBody(id ResourceID, rec *storedEG) (map[string]any, error) {
	domain, topic := egScopeOf(rec.names, rec.kind)

	props, ok := p.dataPlane.GetScopedSubscription(domain, topic, rec.name)
	if !ok {
		return nil, ErrEventGridSubscriptionNotFound
	}

	return egEnvelope(id, rec, props), nil
}

func (p *EventGridProvider) subscriptionBody(id ResourceID, rec *storedEG) (map[string]any, error) {
	props, ok := p.dataPlane.GetSubscription(rec.names[0], rec.name)
	if !ok {
		return nil, ErrEventGridSubscriptionNotFound
	}

	return egEnvelope(id, rec, props), nil
}

func (p *EventGridProvider) namespaceBody(id ResourceID, rec *storedEG) map[string]any {
	props := ehMerge(map[string]any{
		cosmosFieldPublicNetwork: kvPublicNetworkEnabled, "minimumTlsVersionAllowed": "1.2", "isZoneRedundant": true,
	}, rec.extra, map[string]any{
		fieldProvisioningState: provisioningStateSucceeded,
		"topicsConfiguration":  map[string]any{"hostname": p.hostPort(rec)},
		"topicSpacesConfiguration": map[string]any{
			"state": redisPublicDisabled, "hostname": p.hostPort(rec),
		},
	})

	out := egEnvelope(id, rec, props)
	out[ehFieldSKU] = map[string]any{fieldName: skuTierStandard, egPropCapacity: 1}

	return out
}

func (p *EventGridProvider) nsTopicBody(id ResourceID, rec *storedEG) (map[string]any, error) {
	if _, ok := p.dataPlane.NamespaceTopicCreated(rec.names[0], rec.name); !ok {
		return nil, ErrEventGridNamespaceTopicNotFound
	}

	props := ehMerge(map[string]any{"publisherType": "Custom", "eventRetentionInDays": 1}, rec.extra, map[string]any{
		fieldProvisioningState: provisioningStateSucceeded, egPropSchema: egNamespaceSchema,
	})

	return egEnvelope(id, rec, props), nil
}

func (p *EventGridProvider) nsSubBody(id ResourceID, rec *storedEG) (map[string]any, error) {
	props, ok := p.dataPlane.GetPullSubscription(rec.names[0], rec.names[1], rec.name)
	if !ok {
		return nil, ErrEventGridSubscriptionNotFound
	}

	return egEnvelope(id, rec, props), nil
}

func (p *EventGridProvider) keysFor(id ResourceID, regenerate string) (string, string, error) {
	kind := classifyEventGridPath(id)
	if _, ok := p.get(id, kind); !ok || (kind != egKindTopic && kind != egKindNamespace && kind != egKindDomain) {
		return "", "", egKeyNotFound(kind, id)
	}

	name := id.LeafName()

	switch {
	case kind == egKindDomain && regenerate != "":
		return p.dataPlane.RegenerateDomainKey(name, regenerate)
	case kind == egKindDomain:
		return p.dataPlane.DomainKeys(name)
	case kind == egKindTopic && regenerate != "":
		return p.dataPlane.RegenerateTopicKey(name, regenerate)
	case kind == egKindTopic:
		return p.dataPlane.TopicKeys(name)
	case regenerate != "":
		return p.dataPlane.RegenerateNamespaceKey(name, regenerate)
	}

	return p.dataPlane.NamespaceKeys(name)
}

func egKeyNotFound(kind egKind, id ResourceID) error {
	if kind == egKindTopic || kind == egKindNamespace || kind == egKindDomain {
		return egNotFound(kind)
	}

	return errUnsupportedEventGridType(id)
}

// ListKeys implements ResourceProvider for topics and namespaces.
func (p *EventGridProvider) ListKeys(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("ListKeys")
	defer p.mu.RUnlock()

	k1, k2, err := p.keysFor(id, "")
	if err != nil {
		return nil, err
	}

	return map[string]any{egKey1: k1, egKey2: k2}, nil
}

// RegenerateKey replaces key1 or key2 and returns both keys.
func (p *EventGridProvider) RegenerateKey(_ context.Context, id ResourceID, keyName string) (map[string]any, error) {
	if !strings.EqualFold(keyName, egKey1) && !strings.EqualFold(keyName, egKey2) {
		return nil, badEventGridRequest(ErrInvalidRequestBody)
	}

	p.mu.RLock("RegenerateKey")
	defer p.mu.RUnlock()

	k1, k2, err := p.keysFor(id, keyName)
	if err != nil {
		return nil, err
	}

	return map[string]any{egKey1: k1, egKey2: k2}, nil
}

// FullURL returns a WebHook subscription's complete endpoint URL (POST .../getFullUrl).
func (p *EventGridProvider) FullURL(_ context.Context, id ResourceID) (map[string]any, error) {
	kind := classifyEventGridPath(id)
	if !isEventSubscriptionKind(kind) {
		return nil, errUnsupportedEventGridType(id)
	}

	p.mu.RLock("FullURL")
	defer p.mu.RUnlock()

	rec, ok := p.get(id, kind)
	if !ok {
		return nil, ErrEventGridSubscriptionNotFound
	}

	domain, topic := egScopeOf(rec.names, kind)

	url, err := p.dataPlane.ScopedSubscriptionFullURL(domain, topic, rec.name)
	if err != nil {
		return nil, badEventGridRequest(err)
	}

	return map[string]any{"endpointUrl": url}, nil
}

func isEventSubscriptionKind(kind egKind) bool {
	return kind == egKindTopicSub || kind == egKindDomainSub || kind == egKindDomainTopicSub
}

// DeliveryAttributes returns a subscription's delivery attribute mappings with
// secret values (POST .../getDeliveryAttributes).
func (p *EventGridProvider) DeliveryAttributes(_ context.Context, id ResourceID) (map[string]any, error) {
	kind := classifyEventGridPath(id)
	if !isEventSubscriptionKind(kind) {
		return nil, errUnsupportedEventGridType(id)
	}

	p.mu.RLock("DeliveryAttributes")
	defer p.mu.RUnlock()

	rec, ok := p.get(id, kind)
	if !ok {
		return nil, ErrEventGridSubscriptionNotFound
	}

	domain, topic := egScopeOf(rec.names, kind)

	list, err := p.dataPlane.DeliveryAttributes(domain, topic, rec.name)
	if err != nil {
		return nil, badEventGridRequest(err)
	}

	return map[string]any{fieldValue: list}, nil
}

// ListDomainTopics lists the topics of the domain identified by id.
func (p *EventGridProvider) ListDomainTopics(_ context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("ListDomainTopics")
	defer p.mu.RUnlock()

	if _, ok := p.res[egKey(id.ResourceGroup, egKindDomain, id.Names[0])]; !ok {
		return nil, ErrEventGridDomainNotFound
	}

	topics, err := p.dataPlane.DomainTopics(id.Names[0])
	if err != nil {
		return nil, ErrEventGridDomainNotFound
	}

	out := make([]map[string]any, 0, len(topics))

	for _, t := range topics {
		tid := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Namespace: namespaceMicrosoftEventGrid,
			Types: []string{egDomainsType, "topics"}, Names: []string{id.Names[0], t.Name},
		}
		rec := &storedEG{name: t.Name, resourceGroup: id.ResourceGroup, kind: egKindDomainTopic, names: tid.Names}
		out = append(out, egEnvelope(tid, rec, map[string]any{fieldProvisioningState: provisioningStateSucceeded}))
	}

	return out, nil
}

type eventGridRecord struct {
	CreatedAt      time.Time         `json:"createdAt"`
	Tags           map[string]string `json:"tags,omitempty"`
	Extra          map[string]any    `json:"extra,omitempty"`
	Name           string            `json:"name"`
	ResourceGroup  string            `json:"resourceGroup"`
	SubscriptionID string            `json:"subscriptionId,omitempty"`
	Location       string            `json:"location"`
	Host           string            `json:"host,omitempty"`
	Names          []string          `json:"names"`
	Kind           int               `json:"kind"`
}

// eventGridSnapshot is the Microsoft.EventGrid provider's persisted ARM-side state.
type eventGridSnapshot struct {
	Resources map[string]eventGridRecord `json:"resources,omitempty"`
}

func (p *EventGridProvider) snapshot() *eventGridSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &eventGridSnapshot{Resources: make(map[string]eventGridRecord, len(p.res))}

	for k, r := range p.res {
		snap.Resources[k] = eventGridRecord{
			CreatedAt: r.createdAt, Tags: r.tags, Extra: r.extra, Name: r.name, ResourceGroup: r.resourceGroup,
			Location: r.location, Host: r.host, Names: r.names, Kind: int(r.kind), SubscriptionID: r.subscriptionID,
		}
	}

	return snap
}

// restore replaces the provider's state; nil clears it. The data plane is not touched.
func (p *EventGridProvider) restore(snap *eventGridSnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.res = map[string]*storedEG{}

	if snap == nil {
		return
	}

	for k, r := range snap.Resources {
		p.res[k] = &storedEG{
			createdAt: r.CreatedAt, tags: r.Tags, extra: r.Extra, name: r.Name, resourceGroup: r.ResourceGroup,
			location: r.Location, host: r.Host, names: r.Names, kind: egKind(r.Kind), subscriptionID: r.SubscriptionID,
		}
	}
}

func (h *Handler) eventGridProvider() (*EventGridProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftEventGrid)
	if !ok {
		return nil, false
	}

	eg, ok := p.(*EventGridProvider)

	return eg, ok
}
