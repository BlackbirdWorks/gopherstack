package azurearm

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/iso8601"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// Resource type segments for Microsoft.ServiceBus, and the four shapes this
// RP serves (AZURE.md section 10.10's M9 entry):
//
//	namespaces/{ns}                                   (1 pair)
//	namespaces/{ns}/queues/{q}                        (2 pairs)
//	namespaces/{ns}/topics/{t}                        (2 pairs)
//	namespaces/{ns}/authorizationRules/{rule}         (2 pairs, sibling of queues/topics)
//	namespaces/{ns}/topics/{t}/subscriptions/{s}      (3 pairs)
const (
	sbNamespacesType    = "namespaces"
	sbQueuesType        = "queues"
	sbTopicsType        = "topics"
	sbSubscriptionsType = "subscriptions"
	sbAuthRulesType     = "authorizationRules"
)

const namespaceMicrosoftServiceBus = "Microsoft.ServiceBus"

// sbDefaultKeyName/sbDefaultKeyValue mirror services/azureservicebus/sas.go's
// fixed dev SAS identity (DefaultKeyName/DefaultKeyValue) -- duplicated as
// three string literals here rather than imported, since importing a
// data-plane package for three constants would be a needless coupling (see
// interfaces.go's ServiceBusEntities doc comment for the broader rule this
// follows). Keep in sync with services/azureservicebus/sas.go if that ever
// changes.
const (
	sbDefaultKeyName  = "RootManageSharedAccessKey"
	sbDefaultKeyValue = "2R1W2VORtFi9HrmRQ1Gxp7xbySq7W0FAs2BvTZDdXeo="
)

func serviceBusAPIVersions() []string {
	return []string{"2022-10-01-preview", "2021-11-01"}
}

// ServiceBusEndpointConfig configures how the ServiceBus resource provider
// advertises the namespace's serviceBusEndpoint. Unlike Storage's
// StorageEndpointConfig, this needs no vhost/shared-port scheme:
// services/azureservicebus has no namespace-in-URL routing at all (a flat
// host:port reaches it directly), so a single scalar override suffices.
type ServiceBusEndpointConfig struct {
	Override string
	Port     int
}

type storedSBNamespace struct {
	name          string
	resourceGroup string
	location      string
	tags          map[string]string
	sku           map[string]any
	host          string // request Host's hostname at creation time, mirrors storedStorageAccount.host
}

type storedSBQueue struct {
	name              string
	namespace         string
	resourceGroup     string
	lockDuration      time.Duration
	defaultMessageTTL time.Duration
	maxDeliveryCount  int
}

type storedSBTopic struct {
	name              string
	namespace         string
	resourceGroup     string
	defaultMessageTTL time.Duration
}

type storedSBSubscription struct {
	name             string
	namespace        string
	topic            string
	resourceGroup    string
	lockDuration     time.Duration
	maxDeliveryCount int
}

// ServiceBusProvider implements ResourceProvider for Microsoft.ServiceBus.
// Unlike StorageProvider, its queue/topic/subscription Put calls ARE
// load-bearing against dataPlane (services/azureservicebus, M5) -- M5 has no
// ARM-shaped state of its own, so a Terraform-created entity must genuinely
// exist in the real backend for AZURE.md's M9 test plan (a REST
// send->peek-lock->complete round-trip) to succeed. Namespace Put is
// metadata-only: M5 is one-process-one-namespace, so there's nothing to
// delegate for "creating" one.
type ServiceBusProvider struct {
	dataPlane     ServiceBusEntities
	mu            *lockmetrics.RWMutex
	namespaces    map[string]*storedSBNamespace
	queues        map[string]*storedSBQueue
	topics        map[string]*storedSBTopic
	subscriptions map[string]*storedSBSubscription
	cfg           ServiceBusEndpointConfig
}

// NewServiceBusProvider creates a ServiceBusProvider. dataPlane may be nil,
// in which case a no-op default is used (see interfaces.go) until
// cli.go's wireAzureARMResourceProviders wires a real one post-construction.
func NewServiceBusProvider(cfg ServiceBusEndpointConfig, dataPlane ServiceBusEntities) *ServiceBusProvider {
	if dataPlane == nil {
		dataPlane = noopServiceBusEntities{}
	}

	return &ServiceBusProvider{
		mu:            lockmetrics.New("azurearm.servicebusprovider"),
		namespaces:    make(map[string]*storedSBNamespace),
		queues:        make(map[string]*storedSBQueue),
		topics:        make(map[string]*storedSBTopic),
		subscriptions: make(map[string]*storedSBSubscription),
		cfg:           cfg,
		dataPlane:     dataPlane,
	}
}

var _ ResourceProvider = (*ServiceBusProvider)(nil)

// Namespace implements ResourceProvider.
func (p *ServiceBusProvider) Namespace() string { return namespaceMicrosoftServiceBus }

// ResourceTypes implements ResourceProvider.
func (p *ServiceBusProvider) ResourceTypes() []ResourceTypeDef {
	versions := serviceBusAPIVersions()

	return []ResourceTypeDef{
		{Type: sbNamespacesType, APIVersions: versions, HasChildren: true},
		{Type: sbNamespacesType + "/" + sbQueuesType, APIVersions: versions, HasChildren: false},
		{Type: sbNamespacesType + "/" + sbTopicsType, APIVersions: versions, HasChildren: true},
		{Type: sbNamespacesType + "/" + sbAuthRulesType, APIVersions: versions, HasChildren: false},
		{
			Type:        sbNamespacesType + "/" + sbTopicsType + "/" + sbSubscriptionsType,
			APIVersions: versions,
			HasChildren: false,
		},
	}
}

// sbPathKind identifies which of the four ServiceBus resource shapes id
// addresses.
type sbPathKind int

const (
	sbKindUnsupported sbPathKind = iota
	sbKindNamespace
	sbKindQueue
	sbKindTopic
	sbKindAuthRule
	sbKindSubscription
)

// Type-segment-pair counts classifyServiceBusPath switches on: a bare
// namespace is 1 pair deep, a queue/topic/authorizationRule is 2 pairs deep,
// and a topic's subscription is 3 pairs deep (see this file's top-of-file
// comment for the full shape table).
const (
	sbNamespacePairCount    = 1
	sbQueueOrTopicPairCount = 2
	sbSubscriptionPairCount = 3
)

// classifyServiceBusPath validates id's Types shape and returns which of the
// four resource kinds it addresses -- generalizing checkResourceType's
// single-constant-equality check (rp_storage.go), since unlike Storage,
// ServiceBus serves multiple resource types at different nesting depths.
func classifyServiceBusPath(id ResourceID) sbPathKind {
	switch len(id.Types) {
	case sbNamespacePairCount:
		if strings.EqualFold(id.Types[0], sbNamespacesType) {
			return sbKindNamespace
		}
	case sbQueueOrTopicPairCount:
		if !strings.EqualFold(id.Types[0], sbNamespacesType) {
			return sbKindUnsupported
		}

		switch {
		case strings.EqualFold(id.Types[1], sbQueuesType):
			return sbKindQueue
		case strings.EqualFold(id.Types[1], sbTopicsType):
			return sbKindTopic
		case strings.EqualFold(id.Types[1], sbAuthRulesType):
			return sbKindAuthRule
		}
	case sbSubscriptionPairCount:
		if strings.EqualFold(id.Types[0], sbNamespacesType) &&
			strings.EqualFold(id.Types[1], sbTopicsType) &&
			strings.EqualFold(id.Types[2], sbSubscriptionsType) {
			return sbKindSubscription
		}
	}

	return sbKindUnsupported
}

func errUnsupportedServiceBusType(id ResourceID) error {
	return fmt.Errorf("%w: unsupported Microsoft.ServiceBus resource type %q", ErrResourceNotFound, id.ResourceType())
}

// sbKey builds the case-insensitive lookup key for a resource owned by
// resourceGroup, scoped by its full name path (e.g. "ns" for a namespace,
// "ns/queue" for a queue) -- mirroring StorageProvider's per-resource-group
// ownership check (rp_storage.go's lookupOwnedAccount) generalized to
// ServiceBus's nested names.
func sbKey(resourceGroup string, parts ...string) string {
	return strings.ToLower(resourceGroup) + "|" + strings.ToLower(strings.Join(parts, "/"))
}

// Put implements ResourceProvider.
func (p *ServiceBusProvider) Put(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	switch classifyServiceBusPath(id) {
	case sbKindNamespace:
		return p.putNamespace(ctx, id, body)
	case sbKindQueue:
		return p.putQueue(ctx, id, body)
	case sbKindTopic:
		return p.putTopic(ctx, id, body)
	case sbKindSubscription:
		return p.putSubscription(ctx, id, body)
	case sbKindAuthRule:
		return p.putAuthRule(id, body)
	default:
		return nil, errUnsupportedServiceBusType(id)
	}
}

func (p *ServiceBusProvider) putNamespace(
	ctx context.Context,
	id ResourceID,
	body map[string]any,
) (map[string]any, error) {
	name := id.LeafName()
	key := sbKey(id.ResourceGroup, name)

	p.mu.Lock("Put/namespace")
	defer p.mu.Unlock()

	existing, existed := p.namespaces[key]

	location := DefaultLocation
	if loc, ok := body["location"].(string); ok && loc != "" {
		location = loc
	} else if existed {
		location = existing.location
	}

	host := RequestHostFromContext(ctx)
	if host == "" && existed {
		host = existing.host
	}

	ns := &storedSBNamespace{
		name:          name,
		resourceGroup: id.ResourceGroup,
		location:      location,
		tags:          stringTags(body["tags"]),
		host:          host,
	}

	if sku, ok := body["sku"].(map[string]any); ok {
		ns.sku = sku
	} else if existed {
		ns.sku = existing.sku
	}

	p.namespaces[key] = ns

	return p.buildNamespaceBody(id, ns), nil
}

// requireNamespace returns an error if id's parent namespace hasn't been
// created yet -- real ARM requires the parent to exist before a child
// queue/topic/authorizationRule/subscription can be created.
func (p *ServiceBusProvider) requireNamespace(id ResourceID) error {
	if _, ok := p.namespaces[sbKey(id.ResourceGroup, id.Names[0])]; !ok {
		return ErrServiceBusNamespaceNotFound
	}

	return nil
}

func (p *ServiceBusProvider) putQueue(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.Lock("Put/queue")

	if err := p.requireNamespace(id); err != nil {
		p.mu.Unlock()

		return nil, err
	}

	lockDuration, defaultTTL, maxDeliveryCount, err := parseSBEntityProperties(body)
	if err != nil {
		p.mu.Unlock()

		return nil, err
	}

	nsName, name := id.Names[0], id.LeafName()
	q := &storedSBQueue{
		name:              name,
		namespace:         nsName,
		resourceGroup:     id.ResourceGroup,
		lockDuration:      lockDuration,
		defaultMessageTTL: defaultTTL,
		maxDeliveryCount:  maxDeliveryCount,
	}
	p.queues[sbKey(id.ResourceGroup, nsName, name)] = q
	p.mu.Unlock()

	if createErr := p.dataPlane.CreateQueue(name, lockDuration, defaultTTL, maxDeliveryCount); createErr != nil {
		logServiceBusAdapterError(ctx, "CreateQueue", name, createErr)
	}

	return p.buildQueueBody(id, q), nil
}

func (p *ServiceBusProvider) putTopic(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.Lock("Put/topic")

	if err := p.requireNamespace(id); err != nil {
		p.mu.Unlock()

		return nil, err
	}

	_, defaultTTL, _, err := parseSBEntityProperties(body)
	if err != nil {
		p.mu.Unlock()

		return nil, err
	}

	nsName, name := id.Names[0], id.LeafName()
	t := &storedSBTopic{name: name, namespace: nsName, resourceGroup: id.ResourceGroup, defaultMessageTTL: defaultTTL}
	p.topics[sbKey(id.ResourceGroup, nsName, name)] = t
	p.mu.Unlock()

	if createErr := p.dataPlane.CreateTopic(name, defaultTTL); createErr != nil {
		logServiceBusAdapterError(ctx, "CreateTopic", name, createErr)
	}

	return p.buildTopicBody(id, t), nil
}

func (p *ServiceBusProvider) putSubscription(
	ctx context.Context,
	id ResourceID,
	body map[string]any,
) (map[string]any, error) {
	p.mu.Lock("Put/subscription")

	if err := p.requireNamespace(id); err != nil {
		p.mu.Unlock()

		return nil, err
	}

	nsName, topicName := id.Names[0], id.Names[1]
	if _, ok := p.topics[sbKey(id.ResourceGroup, nsName, topicName)]; !ok {
		p.mu.Unlock()

		return nil, ErrServiceBusTopicNotFound
	}

	lockDuration, _, maxDeliveryCount, err := parseSBEntityProperties(body)
	if err != nil {
		p.mu.Unlock()

		return nil, err
	}

	name := id.LeafName()
	s := &storedSBSubscription{
		name:             name,
		namespace:        nsName,
		topic:            topicName,
		resourceGroup:    id.ResourceGroup,
		lockDuration:     lockDuration,
		maxDeliveryCount: maxDeliveryCount,
	}
	p.subscriptions[sbKey(id.ResourceGroup, nsName, topicName, name)] = s
	p.mu.Unlock()

	if createErr := p.dataPlane.CreateSubscription(topicName, name, lockDuration, maxDeliveryCount); createErr != nil {
		logServiceBusAdapterError(ctx, "CreateSubscription", name, createErr)
	}

	return p.buildSubscriptionBody(id, s), nil
}

// putAuthRule is metadata-only: real Azure lets a caller create additional
// named authorization rules with a configurable rights list, but this
// emulator only ever backs listKeys with the one fixed dev SAS identity
// (sbDefaultKeyName/sbDefaultKeyValue) regardless of which rule name was
// PUT -- so accepting and 200ing any rule name here, without storing
// per-rule rights, is sufficient for every caller that immediately follows
// up with a listKeys call (which is the only real use of an authorization
// rule in this emulator's scope).
func (p *ServiceBusProvider) putAuthRule(id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.RLock("Put/authRule")
	err := p.requireNamespace(id)
	p.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	props, _ := body["properties"].(map[string]any)

	rights, _ := props["rights"].([]any)
	if rights == nil {
		rights = []any{"Listen", "Send", "Manage"}
	}

	return map[string]any{
		"id":            id.ARMID(),
		fieldName:       id.LeafName(),
		fieldType:       namespaceMicrosoftServiceBus + "/" + sbNamespacesType + "/" + sbAuthRulesType,
		fieldProperties: map[string]any{"rights": rights},
	}, nil
}

// parseSBEntityProperties extracts lockDuration/defaultMessageTimeToLive
// (ISO 8601, parsed via pkgs/iso8601) and maxDeliveryCount (a plain integer,
// not a duration -- see pkgs/iso8601's own doc comment) from an ARM request
// body's properties object. A field's absence is treated as zero/unset, not
// an error -- real ARM defaults these when omitted, and this emulator's own
// buildBody functions substitute their own defaults for a zero value.
func parseSBEntityProperties(
	body map[string]any,
) (lockDuration, defaultTTL time.Duration, maxDeliveryCount int, err error) {
	props, _ := body["properties"].(map[string]any)

	if s, ok := props["lockDuration"].(string); ok && s != "" {
		lockDuration, err = iso8601.Parse(s)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("%w: lockDuration: %w", ErrInvalidRequestBody, err)
		}
	}

	if s, ok := props["defaultMessageTimeToLive"].(string); ok && s != "" {
		defaultTTL, err = iso8601.Parse(s)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("%w: defaultMessageTimeToLive: %w", ErrInvalidRequestBody, err)
		}
	}

	if n, ok := props["maxDeliveryCount"].(float64); ok {
		maxDeliveryCount = int(n)
	}

	return lockDuration, defaultTTL, maxDeliveryCount, nil
}

// Get implements ResourceProvider.
func (p *ServiceBusProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	switch classifyServiceBusPath(id) {
	case sbKindNamespace:
		ns, ok := p.namespaces[sbKey(id.ResourceGroup, id.LeafName())]
		if !ok {
			return nil, ErrServiceBusNamespaceNotFound
		}

		return p.buildNamespaceBody(id, ns), nil
	case sbKindQueue:
		q, ok := p.queues[sbKey(id.ResourceGroup, id.Names[0], id.LeafName())]
		if !ok {
			return nil, ErrServiceBusQueueNotFound
		}

		return p.buildQueueBody(id, q), nil
	case sbKindTopic:
		t, ok := p.topics[sbKey(id.ResourceGroup, id.Names[0], id.LeafName())]
		if !ok {
			return nil, ErrServiceBusTopicNotFound
		}

		return p.buildTopicBody(id, t), nil
	case sbKindSubscription:
		s, ok := p.subscriptions[sbKey(id.ResourceGroup, id.Names[0], id.Names[1], id.LeafName())]
		if !ok {
			return nil, ErrServiceBusSubscriptionNotFound
		}

		return p.buildSubscriptionBody(id, s), nil
	default:
		return nil, errUnsupportedServiceBusType(id)
	}
}

// Delete implements ResourceProvider.
func (p *ServiceBusProvider) Delete(ctx context.Context, id ResourceID) error {
	p.mu.Lock("Delete")

	switch classifyServiceBusPath(id) {
	case sbKindNamespace:
		key := sbKey(id.ResourceGroup, id.LeafName())
		if _, ok := p.namespaces[key]; !ok {
			p.mu.Unlock()

			return ErrServiceBusNamespaceNotFound
		}

		delete(p.namespaces, key)
		p.mu.Unlock()

		return nil
	case sbKindQueue:
		name := id.LeafName()
		key := sbKey(id.ResourceGroup, id.Names[0], name)

		if _, ok := p.queues[key]; !ok {
			p.mu.Unlock()

			return ErrServiceBusQueueNotFound
		}

		delete(p.queues, key)
		p.mu.Unlock()

		if err := p.dataPlane.DeleteQueue(name); err != nil {
			logServiceBusAdapterError(ctx, "DeleteQueue", name, err)
		}

		return nil
	case sbKindTopic:
		name := id.LeafName()
		key := sbKey(id.ResourceGroup, id.Names[0], name)

		if _, ok := p.topics[key]; !ok {
			p.mu.Unlock()

			return ErrServiceBusTopicNotFound
		}

		delete(p.topics, key)
		p.mu.Unlock()

		if err := p.dataPlane.DeleteTopic(name); err != nil {
			logServiceBusAdapterError(ctx, "DeleteTopic", name, err)
		}

		return nil
	case sbKindSubscription:
		name, topic := id.LeafName(), id.Names[1]
		key := sbKey(id.ResourceGroup, id.Names[0], topic, name)

		if _, ok := p.subscriptions[key]; !ok {
			p.mu.Unlock()

			return ErrServiceBusSubscriptionNotFound
		}

		delete(p.subscriptions, key)
		p.mu.Unlock()

		if err := p.dataPlane.DeleteSubscription(topic, name); err != nil {
			logServiceBusAdapterError(ctx, "DeleteSubscription", name, err)
		}

		return nil
	default:
		p.mu.Unlock()

		return errUnsupportedServiceBusType(id)
	}
}

// List implements ResourceProvider, scoped to id.ResourceGroup if set, else
// every resource of id's LeafType in the subscription.
func (p *ServiceBusProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("List")
	defer p.mu.RUnlock()

	var out []map[string]any

	switch classifyServiceBusPath(id) {
	case sbKindNamespace:
		out = p.listNamespaces(id)
	case sbKindQueue:
		out = p.listQueues(id)
	case sbKindTopic:
		out = p.listTopics(id)
	case sbKindSubscription:
		out = p.listSubscriptions(id)
	case sbKindUnsupported, sbKindAuthRule:
		// Neither shape is listable: authorizationRules only ever supports
		// per-rule Put/listKeys (AZURE.md section 10.10), and an unsupported
		// path has nothing to enumerate. Both fall through to an empty list
		// rather than an error, matching real ARM's List semantics for a
		// collection URL it doesn't otherwise recognize as erroring.
	}

	sort.Slice(out, func(i, j int) bool {
		return stringField(out[i], fieldName) < stringField(out[j], fieldName)
	})

	return out, nil
}

// listNamespaces builds the List response for every stored namespace scoped
// to id.ResourceGroup (or every namespace in the subscription if unset).
func (p *ServiceBusProvider) listNamespaces(id ResourceID) []map[string]any {
	var out []map[string]any

	for _, ns := range p.namespaces {
		if id.ResourceGroup != "" && !resourceGroupsEqual(ns.resourceGroup, id.ResourceGroup) {
			continue
		}

		nsID := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: ns.resourceGroup,
			Namespace: namespaceMicrosoftServiceBus, Types: []string{sbNamespacesType}, Names: []string{ns.name},
		}
		out = append(out, p.buildNamespaceBody(nsID, ns))
	}

	return out
}

// listQueues builds the List response for every stored queue scoped to
// id.ResourceGroup (or every queue in the subscription if unset).
func (p *ServiceBusProvider) listQueues(id ResourceID) []map[string]any {
	var out []map[string]any

	for _, q := range p.queues {
		if id.ResourceGroup != "" && !resourceGroupsEqual(q.resourceGroup, id.ResourceGroup) {
			continue
		}

		qID := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: q.resourceGroup,
			Namespace: namespaceMicrosoftServiceBus,
			Types:     []string{sbNamespacesType, sbQueuesType}, Names: []string{q.namespace, q.name},
		}
		out = append(out, p.buildQueueBody(qID, q))
	}

	return out
}

// listTopics builds the List response for every stored topic scoped to
// id.ResourceGroup (or every topic in the subscription if unset).
func (p *ServiceBusProvider) listTopics(id ResourceID) []map[string]any {
	var out []map[string]any

	for _, t := range p.topics {
		if id.ResourceGroup != "" && !resourceGroupsEqual(t.resourceGroup, id.ResourceGroup) {
			continue
		}

		tID := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: t.resourceGroup,
			Namespace: namespaceMicrosoftServiceBus,
			Types:     []string{sbNamespacesType, sbTopicsType}, Names: []string{t.namespace, t.name},
		}
		out = append(out, p.buildTopicBody(tID, t))
	}

	return out
}

// listSubscriptions builds the List response for every stored subscription
// scoped to id.ResourceGroup (or every subscription in the subscription if
// unset).
func (p *ServiceBusProvider) listSubscriptions(id ResourceID) []map[string]any {
	var out []map[string]any

	for _, s := range p.subscriptions {
		if id.ResourceGroup != "" && !resourceGroupsEqual(s.resourceGroup, id.ResourceGroup) {
			continue
		}

		sID := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: s.resourceGroup,
			Namespace: namespaceMicrosoftServiceBus,
			Types:     []string{sbNamespacesType, sbTopicsType, sbSubscriptionsType},
			Names:     []string{s.namespace, s.topic, s.name},
		}
		out = append(out, p.buildSubscriptionBody(sID, s))
	}

	return out
}

// Reset implements ResourceProvider (Registry.ResetAll, the
// /_gopherstack/reset endpoint).
func (p *ServiceBusProvider) Reset() {
	p.mu.Lock("Reset")
	defer p.mu.Unlock()

	p.namespaces = make(map[string]*storedSBNamespace)
	p.queues = make(map[string]*storedSBQueue)
	p.topics = make(map[string]*storedSBTopic)
	p.subscriptions = make(map[string]*storedSBSubscription)
}

// DeleteResourcesInGroup implements ResourceProvider (Registry's cascade
// delete when a resource group is deleted).
func (p *ServiceBusProvider) DeleteResourcesInGroup(ctx context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")
	deletedQueues, deletedTopics, deletedSubs := p.deleteGroupResourcesLocked(resourceGroup)
	p.mu.Unlock()

	p.cascadeDeleteFromDataPlane(ctx, deletedQueues, deletedTopics, deletedSubs)
}

// deleteGroupResourcesLocked removes every queue/topic/subscription/
// namespace owned by resourceGroup from p's in-memory maps -- caller must
// hold p.mu for writing. Returns the names of the deleted queues/topics
// (for cascadeDeleteFromDataPlane to also clean up in the data plane) and
// the "topic/name" pairs of the deleted subscriptions.
func (p *ServiceBusProvider) deleteGroupResourcesLocked(resourceGroup string) (queues, topics, subs []string) {
	for key, q := range p.queues {
		if resourceGroupsEqual(q.resourceGroup, resourceGroup) {
			queues = append(queues, q.name)
			delete(p.queues, key)
		}
	}

	for key, t := range p.topics {
		if resourceGroupsEqual(t.resourceGroup, resourceGroup) {
			topics = append(topics, t.name)
			delete(p.topics, key)
		}
	}

	for key, s := range p.subscriptions {
		if resourceGroupsEqual(s.resourceGroup, resourceGroup) {
			subs = append(subs, s.topic+"/"+s.name)
			delete(p.subscriptions, key)
		}
	}

	for key, ns := range p.namespaces {
		if resourceGroupsEqual(ns.resourceGroup, resourceGroup) {
			delete(p.namespaces, key)
		}
	}

	return queues, topics, subs
}

// cascadeDeleteFromDataPlane best-effort deletes the given queues/topics/
// subscriptions (the latter as "topic/name" pairs) from the ServiceBus data
// plane, logging (not failing) any individual error -- mirroring Delete's
// own dataPlane-failure handling. Must be called without p.mu held, since
// the data-plane adapter call may block.
func (p *ServiceBusProvider) cascadeDeleteFromDataPlane(
	ctx context.Context,
	deletedQueues, deletedTopics, deletedSubs []string,
) {
	for _, name := range deletedQueues {
		if err := p.dataPlane.DeleteQueue(name); err != nil {
			logServiceBusAdapterError(ctx, "DeleteQueue", name, err)
		}
	}

	for _, name := range deletedTopics {
		if err := p.dataPlane.DeleteTopic(name); err != nil {
			logServiceBusAdapterError(ctx, "DeleteTopic", name, err)
		}
	}

	for _, topicAndName := range deletedSubs {
		parts := strings.SplitN(topicAndName, "/", 2) //nolint:mnd // topic/name, always exactly 2 parts
		if err := p.dataPlane.DeleteSubscription(parts[0], parts[1]); err != nil {
			logServiceBusAdapterError(ctx, "DeleteSubscription", topicAndName, err)
		}
	}
}

// ListKeys implements ResourceProvider for
// namespaces/{ns}/authorizationRules/{rule}/listKeys. Response shape --
// {"primaryConnectionString","secondaryConnectionString","primaryKey",
// "secondaryKey","keyName"} -- matches real ARM's Service Bus
// AuthorizationRules ListKeys AccessKeys model. This emulator has exactly
// one fixed dev SAS identity (services/azureservicebus/sas.go's
// DefaultKeyName/DefaultKeyValue, mirrored here as sbDefaultKeyName/
// sbDefaultKeyValue), so both primary/secondary key are the same value --
// matching StorageProvider.ListKeys's identical key1/key2 convention.
func (p *ServiceBusProvider) ListKeys(_ context.Context, id ResourceID) (map[string]any, error) {
	if classifyServiceBusPath(id) != sbKindAuthRule {
		return nil, errUnsupportedServiceBusType(id)
	}

	p.mu.RLock("ListKeys")
	ns, ok := p.namespaces[sbKey(id.ResourceGroup, id.Names[0])]
	p.mu.RUnlock()

	if !ok {
		return nil, ErrServiceBusNamespaceNotFound
	}

	endpoint := advertiseServiceBusEndpoint(p.cfg.Override, ns.host, p.cfg.Port)
	connStr := fmt.Sprintf("Endpoint=%s;SharedAccessKeyName=%s;SharedAccessKey=%s",
		endpoint, sbDefaultKeyName, sbDefaultKeyValue)

	return map[string]any{
		"primaryConnectionString":   connStr,
		"secondaryConnectionString": connStr,
		"primaryKey":                sbDefaultKeyValue,
		"secondaryKey":              sbDefaultKeyValue,
		fieldKeyName:                sbDefaultKeyName,
	}, nil
}

func (p *ServiceBusProvider) buildNamespaceBody(id ResourceID, ns *storedSBNamespace) map[string]any {
	sku := ns.sku
	if len(sku) == 0 {
		sku = map[string]any{"name": skuTierStandard, "tier": skuTierStandard}
	}

	return map[string]any{
		"id":          id.ARMID(),
		fieldName:     ns.name,
		fieldType:     namespaceMicrosoftServiceBus + "/" + sbNamespacesType,
		fieldLocation: ns.location,
		fieldTags:     tagsOrEmpty(ns.tags),
		"sku":         sku,
		fieldProperties: map[string]any{
			fieldProvisioningState: provisioningStateSucceeded,
			"status":               "Active",
			"serviceBusEndpoint":   advertiseServiceBusEndpoint(p.cfg.Override, ns.host, p.cfg.Port),
		},
	}
}

func (p *ServiceBusProvider) buildQueueBody(id ResourceID, q *storedSBQueue) map[string]any {
	return map[string]any{
		"id":          id.ARMID(),
		fieldName:     q.name,
		fieldType:     namespaceMicrosoftServiceBus + "/" + sbNamespacesType + "/" + sbQueuesType,
		fieldLocation: "",
		fieldProperties: map[string]any{
			fieldProvisioningState:             provisioningStateSucceeded,
			"lockDuration":                     iso8601.Format(q.lockDuration),
			"defaultMessageTimeToLive":         iso8601.Format(q.defaultMessageTTL),
			"maxDeliveryCount":                 q.maxDeliveryCount,
			"requiresDuplicateDetection":       false,
			"deadLetteringOnMessageExpiration": false,
		},
	}
}

func (p *ServiceBusProvider) buildTopicBody(id ResourceID, t *storedSBTopic) map[string]any {
	return map[string]any{
		"id":      id.ARMID(),
		fieldName: t.name,
		fieldType: namespaceMicrosoftServiceBus + "/" + sbNamespacesType + "/" + sbTopicsType,
		fieldProperties: map[string]any{
			fieldProvisioningState:       provisioningStateSucceeded,
			"defaultMessageTimeToLive":   iso8601.Format(t.defaultMessageTTL),
			"requiresDuplicateDetection": false,
			"supportOrdering":            false,
		},
	}
}

func (p *ServiceBusProvider) buildSubscriptionBody(id ResourceID, s *storedSBSubscription) map[string]any {
	return map[string]any{
		"id":      id.ARMID(),
		fieldName: s.name,
		fieldType: namespaceMicrosoftServiceBus + "/" + sbNamespacesType + "/" + sbTopicsType + "/" + sbSubscriptionsType,
		fieldProperties: map[string]any{
			fieldProvisioningState:             provisioningStateSucceeded,
			"lockDuration":                     iso8601.Format(s.lockDuration),
			"maxDeliveryCount":                 s.maxDeliveryCount,
			"deadLetteringOnMessageExpiration": false,
			"requiresSession":                  false,
		},
	}
}

// advertiseServiceBusEndpoint builds the serviceBusEndpoint/connection-string
// host:port ServiceBus advertises. Unlike Storage's advertiseVHostEndpoint,
// this needs no account-name-in-hostname/shared-port scheme --
// services/azureservicebus has no namespace-in-URL routing at all (a flat
// host:port reaches it directly, confirmed via its handler's path parser),
// so a plain "http://host:port/" suffices. override is the externally
// reachable "host:port" this listener is published on if set (mirrors the
// AZURE_ARM_ADVERTISE_STORAGE_VHOST override mechanism), else host:port is
// built from host (defaulting to "localhost" if unset) and the configured
// port.
func advertiseServiceBusEndpoint(override, host string, port int) string {
	hostAndPort := override
	if hostAndPort == "" {
		if host == "" {
			host = "localhost"
		}

		hostAndPort = net.JoinHostPort(host, strconv.Itoa(port))
	}

	return "http://" + hostAndPort + "/"
}

// logServiceBusAdapterError logs a failure from the ServiceBusEntities
// adapter. The nil-safe default (noopServiceBusEntities) never errors; a
// real adapter failing here is logged but never fails the ARM operation
// itself, matching logStorageAdapterError's philosophy -- though unlike
// Storage, a persistent ServiceBusEntities failure here does mean the
// AZURE.md M9 test plan's liveness round-trip will fail downstream, since
// ARM's own state is not the same source of truth the data plane reads from.
func logServiceBusAdapterError(ctx context.Context, op, name string, err error) {
	logger.Load(ctx).WarnContext(ctx, "azurearm: service bus data-plane adapter call failed",
		"op", op, "name", name, "error", err)
}
