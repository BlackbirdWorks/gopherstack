package azurearm

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftEventHub = "Microsoft.EventHub"
	ehNamespacesType           = "namespaces"
	ehHubsType                 = "eventhubs"
	ehGroupsType               = "consumergroups"
	ehRulesType                = "authorizationRules"
	ehNetworkRuleSetType       = "networkRuleSets"
	ehDefaultHost              = "localhost"
	ehHoursPerDay              = 24
	ehMaxRights                = 3
)

func eventHubAPIVersions() []string { return []string{"2024-01-01", "2022-10-01-preview"} }

// EventHubEndpointConfig configures the endpoints the provider advertises.
type EventHubEndpointConfig struct {
	Override string
	Port     int
	AMQPPort int
}

type storedEHNamespace struct {
	tags          map[string]string
	sku           map[string]any
	identity      map[string]any
	extra         map[string]any
	createdAt     time.Time
	name          string
	resourceGroup string
	location      string
	host          string
}

type storedEHHub struct {
	extra         map[string]any
	name          string
	namespace     string
	resourceGroup string
}

type storedEHGroup struct {
	name          string
	hub           string
	namespace     string
	resourceGroup string
	userMetadata  string
}

// EventHubProvider implements ResourceProvider for Microsoft.EventHub. ARM keeps
// only ARM-side metadata; hubs, consumer groups, rules and their keys live in
// the data plane.
type EventHubProvider struct {
	dataPlane       EventHubs
	mu              *lockmetrics.RWMutex
	namespaces      map[string]*storedEHNamespace
	hubs            map[string]*storedEHHub
	groups          map[string]*storedEHGroup
	networkRuleSets map[string]map[string]any
	drConfigs       map[string]*storedEHDR
	cfg             EventHubEndpointConfig
}

// NewEventHubProvider creates an EventHubProvider; dataPlane may be nil.
func NewEventHubProvider(cfg EventHubEndpointConfig, dataPlane EventHubs) *EventHubProvider {
	if dataPlane == nil {
		dataPlane = noopEventHubs{}
	}

	if cfg.Port == 0 {
		cfg.Port = DefaultEventHubPort
	}

	if cfg.AMQPPort == 0 {
		cfg.AMQPPort = DefaultEventHubAMQPPort
	}

	return &EventHubProvider{
		mu:              lockmetrics.New("azurearm.eventhubprovider"),
		namespaces:      make(map[string]*storedEHNamespace),
		hubs:            make(map[string]*storedEHHub),
		groups:          make(map[string]*storedEHGroup),
		networkRuleSets: make(map[string]map[string]any),
		drConfigs:       make(map[string]*storedEHDR),
		cfg:             cfg,
		dataPlane:       dataPlane,
	}
}

var _ ResourceProvider = (*EventHubProvider)(nil)

// Namespace implements ResourceProvider.
func (p *EventHubProvider) Namespace() string { return namespaceMicrosoftEventHub }

// ResourceTypes implements ResourceProvider.
func (p *EventHubProvider) ResourceTypes() []ResourceTypeDef {
	v := eventHubAPIVersions()

	return []ResourceTypeDef{
		{Type: ehNamespacesType, APIVersions: v, HasChildren: true},
		{Type: ehNamespacesType + "/" + ehHubsType, APIVersions: v, HasChildren: true},
		{Type: ehNamespacesType + "/" + ehHubsType + "/" + ehGroupsType, APIVersions: v, HasChildren: false},
		{Type: ehNamespacesType + "/" + ehHubsType + "/" + ehRulesType, APIVersions: v, HasChildren: false},
		{Type: ehNamespacesType + "/" + ehRulesType, APIVersions: v, HasChildren: false},
		{Type: ehNamespacesType + "/" + ehNetworkRuleSetType, APIVersions: v, HasChildren: false},
		{Type: ehNamespacesType + "/" + ehDRType, APIVersions: v, HasChildren: true},
		{Type: ehNamespacesType + "/" + ehDRType + "/" + ehRulesType, APIVersions: v, HasChildren: false},
	}
}

type ehKind int

const (
	ehKindUnsupported ehKind = iota
	ehKindNamespace
	ehKindHub
	ehKindGroup
	ehKindNSRule
	ehKindHubRule
	ehKindNetworkRuleSet
	ehKindDR
	ehKindDRRule
)

func classifyEventHubPath(id ResourceID) ehKind {
	switch strings.ToLower(strings.Join(id.Types, "/")) {
	case "namespaces":
		return ehKindNamespace
	case "namespaces/eventhubs":
		return ehKindHub
	case "namespaces/eventhubs/consumergroups":
		return ehKindGroup
	case "namespaces/authorizationrules":
		return ehKindNSRule
	case "namespaces/eventhubs/authorizationrules":
		return ehKindHubRule
	case "namespaces/networkrulesets":
		return ehKindNetworkRuleSet
	case "namespaces/disasterrecoveryconfigs":
		return ehKindDR
	case "namespaces/disasterrecoveryconfigs/authorizationrules":
		return ehKindDRRule
	}

	return ehKindUnsupported
}

func errUnsupportedEventHubType(id ResourceID) error {
	return fmt.Errorf("%w: unsupported Microsoft.EventHub resource type %q", ErrResourceNotFound, id.ResourceType())
}

func ehKey(rg string, parts ...string) string {
	return strings.ToLower(rg) + "|" + strings.ToLower(strings.Join(parts, "/"))
}

func (p *EventHubProvider) requireNamespace(id ResourceID) error {
	if _, ok := p.namespaces[ehKey(id.ResourceGroup, id.Names[0])]; !ok {
		return ErrEventHubNamespaceNotFound
	}

	return nil
}

func (p *EventHubProvider) requireHub(id ResourceID) (*storedEHHub, error) {
	if err := p.requireNamespace(id); err != nil {
		return nil, err
	}

	h, ok := p.hubs[ehKey(id.ResourceGroup, id.Names[0], id.Names[1])]
	if !ok {
		return nil, ErrEventHubNotFound
	}

	return h, nil
}

// Put implements ResourceProvider.
func (p *EventHubProvider) Put(ctx context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	p.mu.Lock("Put")
	defer p.mu.Unlock()

	if err := p.rejectSecondaryWrite(id); err != nil {
		return nil, err
	}

	switch classifyEventHubPath(id) {
	case ehKindNamespace:
		return p.putNamespace(ctx, id, body)
	case ehKindHub:
		return p.replicated(id, func() (map[string]any, error) { return p.putHub(id, body) })
	case ehKindGroup:
		return p.replicated(id, func() (map[string]any, error) { return p.putGroup(id, body) })
	case ehKindDR:
		return p.putDR(id, body)
	case ehKindNSRule, ehKindHubRule:
		return p.putRule(id, body)
	case ehKindNetworkRuleSet:
		return p.putNetworkRuleSet(id, body)
	case ehKindDRRule, ehKindUnsupported:
	}

	return nil, errUnsupportedEventHubType(id)
}

// Get implements ResourceProvider.
func (p *EventHubProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	switch classifyEventHubPath(id) {
	case ehKindNamespace:
		ns, ok := p.namespaces[ehKey(id.ResourceGroup, id.LeafName())]
		if !ok {
			return nil, ErrEventHubNamespaceNotFound
		}

		return p.namespaceBody(id, ns), nil
	case ehKindHub:
		return p.hubBodyFor(id)
	case ehKindGroup:
		return p.groupBodyFor(id)
	case ehKindNSRule, ehKindHubRule:
		return p.ruleBodyFor(id)
	case ehKindNetworkRuleSet:
		if err := p.requireNamespace(id); err != nil {
			return nil, err
		}

		return networkRuleSetBody(id, p.networkRuleSets[ehKey(id.ResourceGroup, id.Names[0])]), nil
	case ehKindDR:
		c, err := p.drOf(id)
		if err != nil {
			return nil, err
		}

		return p.drBody(id, c), nil
	case ehKindDRRule:
		return p.drRuleBodyFor(id)
	case ehKindUnsupported:
	}

	return nil, errUnsupportedEventHubType(id)
}

// Delete implements ResourceProvider.
func (p *EventHubProvider) Delete(_ context.Context, id ResourceID) error {
	p.mu.Lock("Delete")
	defer p.mu.Unlock()

	if err := p.rejectSecondaryWrite(id); err != nil {
		return err
	}

	switch classifyEventHubPath(id) {
	case ehKindNamespace:
		return p.deleteNamespace(id.ResourceGroup, id.LeafName())
	case ehKindHub:
		return p.replicatedErr(id, func() error { return p.deleteHub(id.ResourceGroup, id.Names[0], id.LeafName()) })
	case ehKindGroup:
		return p.replicatedErr(id, func() error { return p.deleteGroup(id) })
	case ehKindDR:
		return p.deleteDR(id)
	case ehKindNSRule, ehKindHubRule:
		return p.deleteRule(id)
	case ehKindNetworkRuleSet:
		delete(p.networkRuleSets, ehKey(id.ResourceGroup, id.Names[0]))

		return nil
	case ehKindDRRule, ehKindUnsupported:
	}

	return errUnsupportedEventHubType(id)
}

// List implements ResourceProvider.
func (p *EventHubProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("List")
	defer p.mu.RUnlock()

	var out []map[string]any

	if classifyEventHubPath(id) != ehKindNamespace && classifyEventHubPath(id) != ehKindUnsupported {
		if err := p.requireListParent(id); err != nil {
			return nil, err
		}
	}

	switch classifyEventHubPath(id) {
	case ehKindNamespace:
		out = p.listNamespaces(id)
	case ehKindHub:
		out = p.listHubs(id)
	case ehKindGroup:
		out = p.listGroups(id)
	case ehKindDR:
		out = p.listDR(id)
	case ehKindNSRule, ehKindHubRule, ehKindDRRule:
		out = p.listRules(id)
	case ehKindNetworkRuleSet, ehKindUnsupported:
	}

	sort.Slice(out, func(i, j int) bool { return nameField(out[i]) < nameField(out[j]) })

	return out, nil
}

// Reset implements ResourceProvider.
func (p *EventHubProvider) Reset() {
	p.mu.Lock("Reset")
	defer p.mu.Unlock()

	p.namespaces = make(map[string]*storedEHNamespace)
	p.hubs = make(map[string]*storedEHHub)
	p.groups = make(map[string]*storedEHGroup)
	p.networkRuleSets = make(map[string]map[string]any)
	p.drConfigs = make(map[string]*storedEHDR)
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *EventHubProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")
	defer p.mu.Unlock()

	for _, ns := range p.namespaces {
		if resourceGroupsEqual(ns.resourceGroup, resourceGroup) {
			_ = p.deleteNamespace(ns.resourceGroup, ns.name)
		}
	}
}

func (p *EventHubProvider) deleteNamespace(rg, name string) error {
	key := ehKey(rg, name)
	if _, ok := p.namespaces[key]; !ok {
		return ErrEventHubNamespaceNotFound
	}

	for k, h := range p.hubs {
		if resourceGroupsEqual(h.resourceGroup, rg) && strings.EqualFold(h.namespace, name) {
			delete(p.hubs, k)
		}
	}

	for k, g := range p.groups {
		if resourceGroupsEqual(g.resourceGroup, rg) && strings.EqualFold(g.namespace, name) {
			delete(p.groups, k)
		}
	}

	p.dropNamespaceConfigs(rg, name)
	delete(p.namespaces, key)
	delete(p.networkRuleSets, key)

	return p.dataPlane.DeleteNamespace(name)
}

func (p *EventHubProvider) endpointHost(ns *storedEHNamespace) string {
	host := ns.host
	if host == "" {
		host = ehDefaultHost
	}

	return host
}

func (p *EventHubProvider) amqpAddr(ns *storedEHNamespace) string {
	if p.cfg.Override != "" {
		return p.cfg.Override
	}

	return net.JoinHostPort(p.endpointHost(ns), strconv.Itoa(p.cfg.AMQPPort))
}

func (p *EventHubProvider) restEndpoint(ns *storedEHNamespace) string {
	return "http://" + net.JoinHostPort(p.endpointHost(ns), strconv.Itoa(p.cfg.Port)) + "/"
}

func ehRights(body map[string]any) []string {
	props, _ := body[fieldProperties].(map[string]any)
	raw, _ := props["rights"].([]any)

	out := make([]string, 0, ehMaxRights)

	for _, r := range raw {
		if s, ok := r.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

func ehTypeName(id ResourceID) string {
	return namespaceMicrosoftEventHub + "/" + strings.Join(id.Types, "/")
}

func ehMerge(base, extra, typed map[string]any) map[string]any {
	maps.Copy(base, extra)
	maps.Copy(base, typed)

	return base
}

func ehExtra(body map[string]any, owned ...string) map[string]any {
	in, _ := body[fieldProperties].(map[string]any)
	out := make(map[string]any, len(in))
	maps.Copy(out, in)

	for _, k := range append(owned, fieldProvisioningState) {
		delete(out, k)
	}

	return out
}

func eventHubErrorDetails(err error) (errorEntry, bool) {
	msgs := []struct {
		err error
		msg string
	}{
		{ErrEventHubNamespaceNotFound, "The Event Hubs namespace was not found."},
		{ErrEventHubNotFound, "The event hub was not found."},
		{ErrEventHubConsumerGroupNotFound, "The consumer group was not found."},
		{ErrEventHubRuleNotFound, "The authorization rule was not found."},
	}

	if e, ok := eventHubDRErrorDetails(err); ok {
		return e, true
	}

	for _, m := range msgs {
		if errors.Is(err, m.err) {
			return errorEntry{armErrorCodeResourceNotFound, m.msg, http.StatusNotFound}, true
		}
	}

	return errorEntry{}, false
}
