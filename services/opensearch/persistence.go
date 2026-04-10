package opensearch

import (
	"encoding/json"
	"log/slog"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type backendSnapshot struct {
	Domains                map[string]*Domain                `json:"domains"`
	InboundConnections     map[string]*InboundConnection     `json:"inboundConnections"`
	DomainDataSources      map[string]map[string]*DataSource `json:"domainDataSources"`
	DirectQueryDataSources map[string]*DirectQueryDataSource `json:"directQueryDataSources"`
	PackageAssociations    map[string]map[string]bool        `json:"packageAssociations"`
	VpcAuthorizations      map[string][]AuthorizedPrincipal  `json:"vpcAuthorizations"`
	Applications           map[string]*Application           `json:"applications"`
	AccountID              string                            `json:"accountID"`
	Region                 string                            `json:"region"`
	AppIDCounter           int                               `json:"appIDCounter"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Domains:                b.domains,
		InboundConnections:     b.inboundConnections,
		DomainDataSources:      b.domainDataSources,
		DirectQueryDataSources: b.directQueryDataSources,
		PackageAssociations:    b.packageAssociations,
		VpcAuthorizations:      b.vpcAuthorizations,
		Applications:           b.applications,
		AppIDCounter:           b.appIDCounter,
		AccountID:              b.accountID,
		Region:                 b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("opensearch: failed to snapshot backend", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)
	fixNilDomainTags(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Release tags resources from the domains being replaced.
	for _, d := range b.domains {
		d.Tags.Close()
	}

	b.domains = snap.Domains
	b.inboundConnections = snap.InboundConnections
	b.domainDataSources = snap.DomainDataSources
	b.directQueryDataSources = snap.DirectQueryDataSources
	b.packageAssociations = snap.PackageAssociations
	b.vpcAuthorizations = snap.VpcAuthorizations
	b.applications = snap.Applications
	b.appIDCounter = snap.AppIDCounter
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Rebuild the ARN index from restored domains.
	b.arnIndex = rebuildARNIndex(snap.Domains, snap.AccountID, snap.Region)

	return nil
}

// ensureNonNilMaps initialises nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Domains == nil {
		snap.Domains = make(map[string]*Domain)
	}

	if snap.InboundConnections == nil {
		snap.InboundConnections = make(map[string]*InboundConnection)
	}

	if snap.DomainDataSources == nil {
		snap.DomainDataSources = make(map[string]map[string]*DataSource)
	}

	if snap.DirectQueryDataSources == nil {
		snap.DirectQueryDataSources = make(map[string]*DirectQueryDataSource)
	}

	if snap.PackageAssociations == nil {
		snap.PackageAssociations = make(map[string]map[string]bool)
	}

	if snap.VpcAuthorizations == nil {
		snap.VpcAuthorizations = make(map[string][]AuthorizedPrincipal)
	}

	if snap.Applications == nil {
		snap.Applications = make(map[string]*Application)
	}
}

// fixNilDomainTags ensures that every restored domain has a valid Tags instance.
// When a domain is round-tripped through JSON the tags.Tags.UnmarshalJSON creates
// a new underlying safemap, but if the JSON value was null we still need a usable Tags.
func fixNilDomainTags(snap *backendSnapshot) {
	for name, d := range snap.Domains {
		if d != nil && d.Tags == nil {
			d.Tags = tags.New("opensearch." + name + ".tags")
		}
	}
}

// rebuildARNIndex reconstructs the arnIndex map from a set of restored domains.
func rebuildARNIndex(domains map[string]*Domain, accountID, region string) map[string]string {
	idx := make(map[string]string, len(domains))

	for name := range domains {
		domainARN := arn.Build("es", region, accountID, "domain/"+name)
		idx[domainARN] = name
	}

	return idx
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
