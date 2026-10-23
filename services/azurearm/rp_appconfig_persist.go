package azurearm

import "time"

type appConfigRecord struct {
	CreatedAt     time.Time         `json:"createdAt"`
	Tags          map[string]string `json:"tags,omitempty"`
	Extra         map[string]any    `json:"extra,omitempty"`
	Identity      map[string]any    `json:"identity,omitempty"`
	Name          string            `json:"name"`
	ResourceGroup string            `json:"resourceGroup"`
	Location      string            `json:"location"`
	Host          string            `json:"host,omitempty"`
	SKU           string            `json:"sku,omitempty"`
	Store         string            `json:"store,omitempty"`
	Kind          int               `json:"kind"`
}

type appConfigDeletedRecord struct {
	DeletedAt      time.Time         `json:"deletedAt"`
	Tags           map[string]string `json:"tags,omitempty"`
	Name           string            `json:"name"`
	Location       string            `json:"location"`
	SubscriptionID string            `json:"subscriptionId"`
	StoreID        string            `json:"storeId"`
	PurgeProtected bool              `json:"purgeProtected,omitempty"`
}

// appConfigSnapshot is the Microsoft.AppConfiguration provider's persisted ARM-side state.
type appConfigSnapshot struct {
	Resources map[string]appConfigRecord        `json:"resources,omitempty"`
	Deleted   map[string]appConfigDeletedRecord `json:"deleted,omitempty"`
}

func (p *AppConfigProvider) snapshot() *appConfigSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &appConfigSnapshot{
		Resources: make(map[string]appConfigRecord, len(p.res)),
		Deleted:   make(map[string]appConfigDeletedRecord, len(p.deleted)),
	}

	for k, r := range p.res {
		snap.Resources[k] = appConfigRecord{
			CreatedAt: r.createdAt, Tags: r.tags, Extra: r.extra, Identity: r.identity, Name: r.name,
			ResourceGroup: r.resourceGroup, Location: r.location, Host: r.host, SKU: r.sku, Store: r.store,
			Kind: int(r.kind),
		}
	}

	for k, d := range p.deleted {
		snap.Deleted[k] = appConfigDeletedRecord{
			DeletedAt: d.deletedAt, Tags: d.tags, Name: d.name, Location: d.location,
			SubscriptionID: d.subscriptionID, StoreID: d.storeID, PurgeProtected: d.purgeProtected,
		}
	}

	return snap
}

// restore replaces the provider's state; nil clears it. The data plane is not touched.
func (p *AppConfigProvider) restore(snap *appConfigSnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.res, p.deleted = map[string]*storedAC{}, map[string]*deletedAC{}

	if snap == nil {
		return
	}

	for k, r := range snap.Resources {
		p.res[k] = &storedAC{
			createdAt: r.CreatedAt, tags: r.Tags, extra: r.Extra, identity: r.Identity, name: r.Name,
			resourceGroup: r.ResourceGroup, location: r.Location, host: r.Host, sku: r.SKU, store: r.Store,
			kind: acKind(r.Kind),
		}
	}

	for k, d := range snap.Deleted {
		p.deleted[k] = &deletedAC{
			deletedAt: d.DeletedAt, tags: d.Tags, name: d.Name, location: d.Location,
			subscriptionID: d.SubscriptionID, storeID: d.StoreID, purgeProtected: d.PurgeProtected,
		}
	}
}

func (h *Handler) appConfigProvider() (*AppConfigProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftAppConfig)
	if !ok {
		return nil, false
	}

	ac, ok := p.(*AppConfigProvider)

	return ac, ok
}
