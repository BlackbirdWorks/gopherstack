package opensearch

import (
	"encoding/json"
	"log/slog"
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

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

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

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
