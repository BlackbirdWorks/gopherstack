package azurearm

import "time"

type searchRecord struct {
	CreatedAt     time.Time         `json:"createdAt"`
	Tags          map[string]string `json:"tags,omitempty"`
	Extra         map[string]any    `json:"extra,omitempty"`
	AuthOptions   map[string]any    `json:"authOptions,omitempty"`
	Identity      map[string]any    `json:"identity,omitempty"`
	Name          string            `json:"name"`
	ResourceGroup string            `json:"resourceGroup"`
	Location      string            `json:"location"`
	Host          string            `json:"host,omitempty"`
	SKU           string            `json:"sku"`
	HostingMode   string            `json:"hostingMode"`
	PublicAccess  string            `json:"publicAccess"`
	Replicas      int               `json:"replicas"`
	Partitions    int               `json:"partitions"`
	DisableLocal  bool              `json:"disableLocalAuth,omitempty"`
}

// searchSnapshot is the Microsoft.Search provider's persisted ARM-side state.
type searchSnapshot struct {
	Services map[string]searchRecord `json:"services,omitempty"`
}

func (p *SearchProvider) snapshot() *searchSnapshot {
	p.mu.RLock("snapshot")
	defer p.mu.RUnlock()

	snap := &searchSnapshot{Services: make(map[string]searchRecord, len(p.res))}

	for k, r := range p.res {
		snap.Services[k] = searchRecord{
			CreatedAt: r.createdAt, Tags: r.tags, Extra: r.extra, AuthOptions: r.authOptions, Identity: r.identity,
			Name: r.name, ResourceGroup: r.resourceGroup, Location: r.location, Host: r.host, SKU: r.sku,
			HostingMode: r.hostingMode, PublicAccess: r.publicAccess, Replicas: r.replicas, Partitions: r.partitions,
			DisableLocal: r.disableLocal,
		}
	}

	return snap
}

// restore replaces the provider's state; nil clears it. The data plane is not touched.
func (p *SearchProvider) restore(snap *searchSnapshot) {
	p.mu.Lock("restore")
	defer p.mu.Unlock()

	p.res = map[string]*storedSearch{}

	if snap == nil {
		return
	}

	for k, r := range snap.Services {
		p.res[k] = &storedSearch{
			createdAt: r.CreatedAt, tags: r.Tags, extra: r.Extra, authOptions: r.AuthOptions, identity: r.Identity,
			name: r.Name, resourceGroup: r.ResourceGroup, location: r.Location, host: r.Host, sku: r.SKU,
			hostingMode: r.HostingMode, publicAccess: r.PublicAccess, replicas: r.Replicas,
			partitions: r.Partitions, disableLocal: r.DisableLocal,
		}
	}
}
