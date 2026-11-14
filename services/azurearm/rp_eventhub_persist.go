package azurearm

import (
	"maps"
	"time"
)

type eventHubNamespaceRecord struct {
	CreatedAt     time.Time         `json:"createdAt"`
	Tags          map[string]string `json:"tags,omitempty"`
	SKU           map[string]any    `json:"sku,omitempty"`
	Identity      map[string]any    `json:"identity,omitempty"`
	Extra         map[string]any    `json:"extra,omitempty"`
	Name          string            `json:"name"`
	ResourceGroup string            `json:"resourceGroup"`
	Location      string            `json:"location"`
	Host          string            `json:"host,omitempty"`
}

type eventHubRecord struct {
	Extra         map[string]any `json:"extra,omitempty"`
	Name          string         `json:"name"`
	Namespace     string         `json:"namespace"`
	ResourceGroup string         `json:"resourceGroup"`
}

type eventHubGroupRecord struct {
	Name          string `json:"name"`
	Hub           string `json:"hub"`
	Namespace     string `json:"namespace"`
	ResourceGroup string `json:"resourceGroup"`
	UserMetadata  string `json:"userMetadata,omitempty"`
}

type eventHubDRRecord struct {
	Alias         string `json:"alias"`
	Namespace     string `json:"namespace"`
	ResourceGroup string `json:"resourceGroup"`
	Partner       string `json:"partner"`
	PartnerRG     string `json:"partnerResourceGroup"`
	AlternateName string `json:"alternateName,omitempty"`
	Role          string `json:"role"`
}

// eventHubSnapshot is the Microsoft.EventHub provider's persisted ARM-side state.
type eventHubSnapshot struct {
	Namespaces       map[string]eventHubNamespaceRecord `json:"namespaces,omitempty"`
	Hubs             map[string]eventHubRecord          `json:"hubs,omitempty"`
	Groups           map[string]eventHubGroupRecord     `json:"groups,omitempty"`
	NetworkRuleSets  map[string]map[string]any          `json:"networkRuleSets,omitempty"`
	DisasterRecovery map[string]eventHubDRRecord        `json:"disasterRecoveryConfigs,omitempty"`
}

func (p *EventHubProvider) snapshot() *eventHubSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &eventHubSnapshot{
		Namespaces:       make(map[string]eventHubNamespaceRecord, len(p.namespaces)),
		Hubs:             make(map[string]eventHubRecord, len(p.hubs)),
		Groups:           make(map[string]eventHubGroupRecord, len(p.groups)),
		NetworkRuleSets:  p.networkRuleSets,
		DisasterRecovery: make(map[string]eventHubDRRecord, len(p.drConfigs)),
	}

	for k, c := range p.drConfigs {
		snap.DisasterRecovery[k] = eventHubDRRecord{
			Alias: c.alias, Namespace: c.namespace, ResourceGroup: c.resourceGroup, Partner: c.partner,
			PartnerRG: c.partnerRG, AlternateName: c.alternateName, Role: c.role,
		}
	}

	for k, n := range p.namespaces {
		snap.Namespaces[k] = eventHubNamespaceRecord{
			CreatedAt: n.createdAt, Tags: n.tags, SKU: n.sku, Identity: n.identity, Extra: n.extra, Name: n.name,
			ResourceGroup: n.resourceGroup, Location: n.location, Host: n.host,
		}
	}

	for k, h := range p.hubs {
		snap.Hubs[k] = eventHubRecord{
			Extra:         h.extra,
			Name:          h.name,
			Namespace:     h.namespace,
			ResourceGroup: h.resourceGroup,
		}
	}

	for k, g := range p.groups {
		snap.Groups[k] = eventHubGroupRecord{
			Name:          g.name,
			Hub:           g.hub,
			Namespace:     g.namespace,
			ResourceGroup: g.resourceGroup,
			UserMetadata:  g.userMetadata,
		}
	}

	return snap
}

// restore replaces the provider's state; nil clears it. The data plane is not touched.
func (p *EventHubProvider) restore(snap *eventHubSnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.namespaces = make(map[string]*storedEHNamespace)
	p.hubs = make(map[string]*storedEHHub)
	p.groups = make(map[string]*storedEHGroup)
	p.networkRuleSets = make(map[string]map[string]any)
	p.drConfigs = make(map[string]*storedEHDR)

	if snap == nil {
		return
	}

	for k, n := range snap.Namespaces {
		p.namespaces[k] = &storedEHNamespace{
			createdAt: n.CreatedAt, tags: n.Tags, sku: n.SKU, identity: n.Identity, extra: n.Extra, name: n.Name,
			resourceGroup: n.ResourceGroup, location: n.Location, host: n.Host,
		}
	}

	for k, h := range snap.Hubs {
		p.hubs[k] = &storedEHHub{extra: h.Extra, name: h.Name, namespace: h.Namespace, resourceGroup: h.ResourceGroup}
	}

	for k, g := range snap.Groups {
		p.groups[k] = &storedEHGroup{
			name:          g.Name,
			hub:           g.Hub,
			namespace:     g.Namespace,
			resourceGroup: g.ResourceGroup,
			userMetadata:  g.UserMetadata,
		}
	}

	maps.Copy(p.networkRuleSets, snap.NetworkRuleSets)

	for k, c := range snap.DisasterRecovery {
		p.drConfigs[k] = &storedEHDR{
			alias: c.Alias, namespace: c.Namespace, resourceGroup: c.ResourceGroup, partner: c.Partner,
			partnerRG: c.PartnerRG, alternateName: c.AlternateName, role: c.Role,
		}
	}
}

func (h *Handler) eventHubProvider() (*EventHubProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftEventHub)
	if !ok {
		return nil, false
	}

	eh, ok := p.(*EventHubProvider)

	return eh, ok
}
