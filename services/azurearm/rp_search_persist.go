package azurearm

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

type searchLinkRecord struct {
	Name       string `json:"name"`
	GroupID    string `json:"groupId"`
	ResourceID string `json:"privateLinkResourceId"`
	Message    string `json:"requestMessage,omitempty"`
	Region     string `json:"resourceRegion,omitempty"`
	Status     string `json:"status"`
}

type searchRecord struct {
	CreatedAt     time.Time          `json:"createdAt"`
	Extra         map[string]any     `json:"extra,omitempty"`
	Tags          map[string]string  `json:"tags,omitempty"`
	AuthOptions   map[string]any     `json:"authOptions,omitempty"`
	Identity      map[string]any     `json:"identity,omitempty"`
	Name          string             `json:"name"`
	PublicAccess  string             `json:"publicAccess"`
	Location      string             `json:"location"`
	HostingMode   string             `json:"hostingMode"`
	SKU           string             `json:"sku"`
	Host          string             `json:"host,omitempty"`
	ResourceGroup string             `json:"resourceGroup"`
	SharedLinks   []searchLinkRecord `json:"sharedLinks,omitempty"`
	Replicas      int                `json:"replicas"`
	Partitions    int                `json:"partitions"`
	DisableLocal  bool               `json:"disableLocalAuth,omitempty"`
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
			DisableLocal: r.disableLocal, SharedLinks: linkRecords(r.links),
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
			partitions: r.Partitions, disableLocal: r.DisableLocal, links: linksFromRecords(r.SharedLinks),
		}
	}
}

func linkRecords(links map[string]*storedSearchLink) []searchLinkRecord {
	out := make([]searchLinkRecord, 0, len(links))

	for _, l := range links {
		out = append(out, searchLinkRecord{
			Name: l.name, GroupID: l.groupID, ResourceID: l.resourceID, Message: l.requestMsg,
			Region: l.resourceRegion, Status: l.status,
		})
	}

	return out
}

func linksFromRecords(recs []searchLinkRecord) map[string]*storedSearchLink {
	if len(recs) == 0 {
		return nil
	}

	out := make(map[string]*storedSearchLink, len(recs))

	for _, r := range recs {
		out[strs.Fold(r.Name)] = &storedSearchLink{
			name: r.Name, groupID: r.GroupID, resourceID: r.ResourceID, requestMsg: r.Message,
			resourceRegion: r.Region, status: r.Status,
		}
	}

	return out
}
