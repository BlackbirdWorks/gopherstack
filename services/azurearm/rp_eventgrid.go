package azurearm

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftEventGrid = "Microsoft.EventGrid"
	egTopicsType                = "topics"
	egNamespacesType            = "namespaces"
	egDomainsType               = "domains"
	egSubscriptionsType         = "eventSubscriptions"
	egDefaultHost               = "localhost"
	egInputSchemaDefault        = "EventGridSchema"
	egInputSchemaCloud          = "CloudEventSchemaV1_0"
	egNamespaceSchema           = "CloudEventSchemaV1_0"
	egProviders                 = "providers"
	egTopicSubTypes             = "topics/providers/eventsubscriptions"
)

func eventGridAPIVersions() []string {
	return []string{"2025-02-15", "2024-06-01-preview", "2022-06-15"}
}

// EventGridEndpointConfig configures the endpoints the provider advertises.
type EventGridEndpointConfig struct {
	Override string
	Port     int
}

type egKind int

const (
	egKindUnsupported egKind = iota
	egKindTopic
	egKindTopicSub
	egKindNamespace
	egKindNsTopic
	egKindNsSub
	egKindDomain
	egKindDomainTopic
	egKindDomainSub
	egKindDomainTopicSub
)

type storedEG struct {
	tags          map[string]string
	extra         map[string]any
	createdAt     time.Time
	name          string
	resourceGroup string
	location      string
	host          string
	names         []string
	kind          egKind
}

// EventGridProvider implements ResourceProvider for Microsoft.EventGrid. ARM keeps
// only ARM-side metadata; topics, keys, subscriptions and queued events live in
// the data plane.
type EventGridProvider struct {
	dataPlane EventGrid
	mu        *lockmetrics.RWMutex
	res       map[string]*storedEG
	cfg       EventGridEndpointConfig
}

// NewEventGridProvider creates an EventGridProvider; dataPlane may be nil.
func NewEventGridProvider(cfg EventGridEndpointConfig, dataPlane EventGrid) *EventGridProvider {
	if dataPlane == nil {
		dataPlane = noopEventGrid{}
	}

	if cfg.Port == 0 {
		cfg.Port = DefaultEventGridPort
	}

	return &EventGridProvider{
		mu: lockmetrics.New("azurearm.eventgridprovider"), res: map[string]*storedEG{}, cfg: cfg, dataPlane: dataPlane,
	}
}

var _ ResourceProvider = (*EventGridProvider)(nil)

// Namespace implements ResourceProvider.
func (p *EventGridProvider) Namespace() string { return namespaceMicrosoftEventGrid }

// ResourceTypes implements ResourceProvider.
func (p *EventGridProvider) ResourceTypes() []ResourceTypeDef {
	v := eventGridAPIVersions()

	return []ResourceTypeDef{
		{Type: egTopicsType, APIVersions: v, HasChildren: true},
		{Type: egSubscriptionsType, APIVersions: v, HasChildren: false},
		{Type: egNamespacesType, APIVersions: v, HasChildren: true},
		{Type: egNamespacesType + "/topics", APIVersions: v, HasChildren: true},
		{Type: egNamespacesType + "/topics/eventSubscriptions", APIVersions: v, HasChildren: false},
		{Type: egDomainsType, APIVersions: v, HasChildren: true},
		{Type: egDomainsType + "/topics", APIVersions: v, HasChildren: true},
		{Type: egDomainsType + "/eventSubscriptions", APIVersions: v, HasChildren: false},
		{Type: egDomainsType + "/topics/eventSubscriptions", APIVersions: v, HasChildren: false},
	}
}

//nolint:gochecknoglobals // static lookup, read-only after init
var egPlainKinds = map[string]egKind{
	egTopicsType:                           egKindTopic,
	egNamespacesType:                       egKindNamespace,
	"namespaces/topics":                    egKindNsTopic,
	"namespaces/topics/eventsubscriptions": egKindNsSub,
	egDomainsType:                          egKindDomain,
	"domains/topics":                       egKindDomainTopic,
	"domains/eventsubscriptions":           egKindDomainSub,
	"domains/topics/eventsubscriptions":    egKindDomainTopicSub,
}

func classifyEventGridPath(id ResourceID) egKind {
	path := strings.ToLower(strings.Join(id.Types, "/"))
	if kind, ok := egPlainKinds[path]; ok {
		return kind
	}

	// scope form: .../{resource}/providers/Microsoft.EventGrid/eventSubscriptions/{name}
	n := len(id.Names)
	if n < 3 || !strings.EqualFold(id.Names[n-2], namespaceMicrosoftEventGrid) {
		return egKindUnsupported
	}

	switch {
	case path == egTopicSubTypes && n == 3:
		return egKindTopicSub
	case path == "domains/providers/eventsubscriptions" && n == 3:
		return egKindDomainSub
	case path == "domains/topics/providers/eventsubscriptions" && n == 4:
		return egKindDomainTopicSub
	}

	return egKindUnsupported
}

// egNames returns the entity names of id without the "providers" pair of a topic-scoped subscription.
func egNames(id ResourceID, kind egKind) []string {
	switch {
	case kind == egKindTopicSub, kind == egKindDomainSub && len(id.Names) == 3:
		return []string{id.Names[0], id.Names[2]}
	case kind == egKindDomainTopicSub && len(id.Names) == 4:
		return []string{id.Names[0], id.Names[1], id.Names[3]}
	}

	return id.Names
}

func egKey(rg string, kind egKind, names ...string) string {
	return ehKey(rg, append([]string{strconv.Itoa(int(kind))}, names...)...)
}

func egTypeName(kind egKind) string {
	switch kind {
	case egKindTopic:
		return namespaceMicrosoftEventGrid + "/topics"
	case egKindTopicSub:
		return namespaceMicrosoftEventGrid + "/eventSubscriptions"
	case egKindNamespace:
		return namespaceMicrosoftEventGrid + "/namespaces"
	case egKindNsTopic:
		return namespaceMicrosoftEventGrid + "/namespaces/topics"
	case egKindNsSub:
		return namespaceMicrosoftEventGrid + "/namespaces/topics/eventSubscriptions"
	case egKindDomain:
		return namespaceMicrosoftEventGrid + "/domains"
	case egKindDomainTopic:
		return namespaceMicrosoftEventGrid + "/domains/topics"
	case egKindDomainSub:
		return namespaceMicrosoftEventGrid + "/domains/eventSubscriptions"
	case egKindDomainTopicSub:
		return namespaceMicrosoftEventGrid + "/domains/topics/eventSubscriptions"
	case egKindUnsupported:
	}

	return ""
}

func errUnsupportedEventGridType(id ResourceID) error {
	return fmt.Errorf("%w: unsupported Microsoft.EventGrid resource type %q", ErrResourceNotFound, id.ResourceType())
}

func (p *EventGridProvider) get(id ResourceID, kind egKind) (*storedEG, bool) {
	if kind == egKindDomainTopic {
		return p.domainTopicRecord(id)
	}

	r, ok := p.res[egKey(id.ResourceGroup, kind, egNames(id, kind)...)]

	return r, ok
}

// domainTopicRecord stands in for a domain topic, which the data plane owns
// (it can appear and vanish with its first and last subscription).
func (p *EventGridProvider) domainTopicRecord(id ResourceID) (*storedEG, bool) {
	if _, ok := p.res[egKey(id.ResourceGroup, egKindDomain, id.Names[0])]; !ok {
		return nil, false
	}

	t, ok := p.dataPlane.DomainTopic(id.Names[0], id.LeafName())
	if !ok {
		return nil, false
	}

	return &storedEG{
		name: t.Name, resourceGroup: id.ResourceGroup, createdAt: t.CreatedAt, names: id.Names, kind: egKindDomainTopic,
	}, true
}

func (p *EventGridProvider) requireParents(id ResourceID, kind egKind) error {
	switch kind {
	case egKindTopicSub:
		return p.requireKind(id, egKindTopic, ErrEventGridTopicNotFound, id.Names[0])
	case egKindNsTopic:
		return p.requireKind(id, egKindNamespace, ErrEventGridNamespaceNotFound, id.Names[0])
	case egKindNsSub:
		return p.requireKind(id, egKindNsTopic, ErrEventGridNamespaceTopicNotFound, id.Names[0], id.Names[1])
	case egKindDomainTopic, egKindDomainSub, egKindDomainTopicSub:
		return p.requireKind(id, egKindDomain, ErrEventGridDomainNotFound, id.Names[0])
	case egKindTopic, egKindNamespace, egKindDomain, egKindUnsupported:
	}

	return nil
}

func (p *EventGridProvider) requireKind(id ResourceID, kind egKind, notFound error, names ...string) error {
	if _, ok := p.res[egKey(id.ResourceGroup, kind, names...)]; !ok {
		return notFound
	}

	return nil
}

// Put implements ResourceProvider.
func (p *EventGridProvider) Put(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	kind := classifyEventGridPath(id)
	if kind == egKindUnsupported {
		return nil, errUnsupportedEventGridType(id)
	}

	p.mu.Lock("Put")
	defer p.mu.Unlock()

	if err := p.requireParents(id, kind); err != nil {
		return nil, err
	}

	rec := p.newRecord(ctx, id, kind, body)
	if err := p.putDataPlane(ctx, id, kind, body, rec); err != nil {
		if isEventGridNotFound(err) {
			return nil, err
		}

		return nil, badEventGridRequest(err)
	}

	if kind != egKindDomainTopic {
		p.res[egKey(id.ResourceGroup, kind, rec.names...)] = rec
	}

	return p.bodyOf(id, rec)
}

func (p *EventGridProvider) newRecord(ctx context.Context, id ResourceID, kind egKind, body map[string]any) *storedEG {
	rec := &storedEG{
		name: id.LeafName(), resourceGroup: id.ResourceGroup, location: DefaultLocation, createdAt: time.Now().UTC(),
		tags: stringTags(body[fieldTags]), extra: egExtra(body, kind), names: egNames(id, kind), kind: kind,
	}

	if existing, ok := p.get(id, kind); ok {
		rec.createdAt, rec.host, rec.location = existing.createdAt, existing.host, existing.location
	}

	if loc, _ := body[fieldLocation].(string); loc != "" {
		rec.location = loc
	}

	if h := RequestHostFromContext(ctx); h != "" {
		rec.host = h
	}

	return rec
}

// egExtra keeps the request properties the emulator does not model, so they round-trip.
func egExtra(body map[string]any, kind egKind) map[string]any {
	switch kind {
	case egKindTopicSub, egKindNsSub:
		return nil
	case egKindTopic:
		return ehExtra(body, "endpoint", "inputSchema", "metricResourceId")
	case egKindDomain:
		return ehExtra(body, "endpoint", "inputSchema", "metricResourceId",
			egPropAutoCreate, egPropAutoDelete)
	case egKindDomainTopic, egKindDomainSub, egKindDomainTopicSub:
		return nil
	case egKindNamespace, egKindNsTopic, egKindUnsupported:
	}

	return ehExtra(body)
}

func badEventGridRequest(err error) error {
	return fmt.Errorf("%w: %w", ErrInvalidRequestBody, err)
}

func (p *EventGridProvider) putDataPlane(
	ctx context.Context, id ResourceID, kind egKind, body map[string]any, rec *storedEG,
) error {
	props, _ := body[fieldProperties].(map[string]any)

	switch kind {
	case egKindTopic:
		schema, _ := props["inputSchema"].(string)

		_, err := p.dataPlane.PutTopic(rec.name, id.ARMID(), schema)

		return err
	case egKindTopicSub:
		_, err := p.dataPlane.PutSubscription(ctx, id.Names[0], rec.name, props)

		return err
	case egKindNamespace:
		return p.dataPlane.PutNamespace(rec.name)
	case egKindNsTopic:
		_, err := p.dataPlane.PutNamespaceTopic(id.Names[0], rec.name)

		return err
	case egKindNsSub:
		_, err := p.dataPlane.PutPullSubscription(id.Names[0], id.Names[1], rec.name, props)

		return err
	case egKindDomain:
		schema, _ := props[egPropSchema].(string)

		_, err := p.dataPlane.PutDomain(rec.name, id.ARMID(), schema,
			optionalBool(props, egPropAutoCreate), optionalBool(props, egPropAutoDelete))

		return err
	case egKindDomainTopic:
		_, err := p.dataPlane.PutDomainTopic(id.Names[0], rec.name)

		return err
	case egKindDomainSub, egKindDomainTopicSub:
		domain, topic := egScopeOf(rec.names, rec.kind)
		_, err := p.dataPlane.PutScopedSubscription(ctx, domain, topic, rec.name, props)

		return err
	case egKindUnsupported:
	}

	return errUnsupportedEventGridType(id)
}

// Get implements ResourceProvider.
func (p *EventGridProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	kind := classifyEventGridPath(id)
	if kind == egKindUnsupported {
		return nil, errUnsupportedEventGridType(id)
	}

	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	rec, ok := p.get(id, kind)
	if !ok {
		return nil, egNotFound(kind)
	}

	return p.bodyOf(id, rec)
}

func egNotFound(kind egKind) error {
	switch kind {
	case egKindTopic:
		return ErrEventGridTopicNotFound
	case egKindTopicSub, egKindNsSub:
		return ErrEventGridSubscriptionNotFound
	case egKindNamespace:
		return ErrEventGridNamespaceNotFound
	case egKindNsTopic:
		return ErrEventGridNamespaceTopicNotFound
	case egKindDomain:
		return ErrEventGridDomainNotFound
	case egKindDomainTopic:
		return ErrEventGridDomainTopicNotFound
	case egKindDomainSub, egKindDomainTopicSub:
		return ErrEventGridSubscriptionNotFound
	case egKindUnsupported:
	}

	return ErrResourceNotFound
}

// Delete implements ResourceProvider.
func (p *EventGridProvider) Delete(_ context.Context, id ResourceID) error {
	kind := classifyEventGridPath(id)
	if kind == egKindUnsupported {
		return errUnsupportedEventGridType(id)
	}

	p.mu.Lock("Delete")
	defer p.mu.Unlock()

	return p.deleteRecord(id.ResourceGroup, kind, egNames(id, kind))
}

func (p *EventGridProvider) deleteRecord(rg string, kind egKind, names []string) error {
	if kind == egKindDomainTopic {
		return p.deleteDomainTopic(rg, names)
	}

	key := egKey(rg, kind, names...)
	if _, ok := p.res[key]; !ok {
		return egNotFound(kind)
	}

	p.deleteChildren(rg, kind, names)
	delete(p.res, key)

	return p.deleteFromDataPlane(kind, names)
}

func (p *EventGridProvider) deleteDomainTopic(rg string, names []string) error {
	if _, ok := p.dataPlane.DomainTopic(names[0], names[1]); !ok {
		return ErrEventGridDomainTopicNotFound
	}

	p.deleteChildren(rg, egKindDomainTopic, names)

	return p.dataPlane.DeleteDomainTopic(names[0], names[1])
}

func (p *EventGridProvider) deleteChildren(rg string, kind egKind, names []string) {
	for k, r := range p.res {
		if !resourceGroupsEqual(r.resourceGroup, rg) || r.kind <= kind || len(r.names) <= len(names) {
			continue
		}

		if egChildOf(kind, r.kind) && namesPrefix(r.names, names) {
			delete(p.res, k)
		}
	}
}

func egChildOf(parent, child egKind) bool {
	switch parent {
	case egKindTopic:
		return child == egKindTopicSub
	case egKindNamespace:
		return child == egKindNsTopic || child == egKindNsSub
	case egKindNsTopic:
		return child == egKindNsSub
	case egKindDomain:
		return child == egKindDomainTopic || child == egKindDomainSub || child == egKindDomainTopicSub
	case egKindDomainTopic:
		return child == egKindDomainTopicSub
	case egKindTopicSub, egKindNsSub, egKindDomainSub, egKindDomainTopicSub, egKindUnsupported:
	}

	return false
}

func namesPrefix(names, prefix []string) bool {
	for i, n := range prefix {
		if !strings.EqualFold(names[i], n) {
			return false
		}
	}

	return true
}

func (p *EventGridProvider) deleteFromDataPlane(kind egKind, names []string) error {
	switch kind {
	case egKindTopic:
		return p.dataPlane.DeleteTopic(names[0])
	case egKindTopicSub:
		return p.dataPlane.DeleteSubscription(names[0], names[1])
	case egKindNamespace:
		return p.dataPlane.DeleteNamespace(names[0])
	case egKindNsTopic:
		return p.dataPlane.DeleteNamespaceTopic(names[0], names[1])
	case egKindNsSub:
		return p.dataPlane.DeletePullSubscription(names[0], names[1], names[2])
	case egKindDomain:
		return p.dataPlane.DeleteDomain(names[0])
	case egKindDomainSub, egKindDomainTopicSub:
		domain, topic := egScopeOf(names, kind)

		return p.dataPlane.DeleteScopedSubscription(domain, topic, names[len(names)-1])
	case egKindDomainTopic, egKindUnsupported:
	}

	return nil
}

// List implements ResourceProvider; only topics and namespaces are listable.
func (p *EventGridProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	kind := classifyEventGridPath(id)

	p.mu.RLock("List")
	defer p.mu.RUnlock()

	var out []map[string]any

	if kind == egKindTopic || kind == egKindNamespace || kind == egKindDomain {
		out = p.listKind(id, kind)
	}

	sort.Slice(out, func(i, j int) bool { return nameField(out[i]) < nameField(out[j]) })

	return out, nil
}

func (p *EventGridProvider) listKind(id ResourceID, kind egKind) []map[string]any {
	var out []map[string]any

	for _, rec := range p.res {
		if rec.kind != kind || (id.ResourceGroup != "" && !resourceGroupsEqual(rec.resourceGroup, id.ResourceGroup)) {
			continue
		}

		rid := ResourceID{
			SubscriptionID: id.SubscriptionID, ResourceGroup: rec.resourceGroup, Namespace: namespaceMicrosoftEventGrid,
			Types: id.Types, Names: rec.names,
		}

		if body, err := p.bodyOf(rid, rec); err == nil {
			out = append(out, body)
		}
	}

	return out
}

// Reset implements ResourceProvider.
func (p *EventGridProvider) Reset() {
	p.mu.Lock("Reset")
	defer p.mu.Unlock()

	p.res = map[string]*storedEG{}
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *EventGridProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")
	defer p.mu.Unlock()

	for _, kind := range []egKind{egKindTopic, egKindNamespace, egKindDomain} {
		for _, rec := range p.res {
			if rec.kind == kind && resourceGroupsEqual(rec.resourceGroup, resourceGroup) {
				_ = p.deleteRecord(rec.resourceGroup, rec.kind, rec.names)
			}
		}
	}
}

func eventGridErrorDetails(err error) (errorEntry, bool) {
	msgs := []struct {
		err error
		msg string
	}{
		{ErrEventGridTopicNotFound, "The Event Grid topic was not found."},
		{ErrEventGridSubscriptionNotFound, "The event subscription was not found."},
		{ErrEventGridNamespaceNotFound, "The Event Grid namespace was not found."},
		{ErrEventGridNamespaceTopicNotFound, "The Event Grid namespace topic was not found."},
		{ErrEventGridDomainNotFound, "The Event Grid domain was not found."},
		{ErrEventGridDomainTopicNotFound, "The Event Grid domain topic was not found."},
	}

	for _, m := range msgs {
		if errors.Is(err, m.err) {
			return errorEntry{armErrorCodeResourceNotFound, m.msg, http.StatusNotFound}, true
		}
	}

	return errorEntry{}, false
}

func (p *EventGridProvider) hostPort(rec *storedEG) string {
	if p.cfg.Override != "" {
		return p.cfg.Override
	}

	host := rec.host
	if host == "" {
		host = egDefaultHost
	}

	return net.JoinHostPort(host, strconv.Itoa(p.cfg.Port))
}

func isEventGridNotFound(err error) bool {
	_, ok := eventGridErrorDetails(err)

	return ok
}

// egScopeOf maps a domain-scoped subscription's names to its data-plane (domain, topic) scope.
func egScopeOf(names []string, kind egKind) (string, string) {
	switch kind {
	case egKindDomainSub:
		return names[0], ""
	case egKindDomainTopicSub:
		return names[0], names[1]
	case egKindTopicSub:
		return "", names[0]
	case egKindTopic, egKindNamespace, egKindNsTopic, egKindNsSub, egKindDomain, egKindDomainTopic, egKindUnsupported:
	}

	return "", ""
}

func optionalBool(props map[string]any, key string) *bool {
	v, ok := props[key].(bool)
	if !ok {
		return nil
	}

	return &v
}
