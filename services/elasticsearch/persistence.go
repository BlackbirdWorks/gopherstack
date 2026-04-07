package elasticsearch

import (
	"encoding/json"
	"log/slog"
	"maps"
)

type backendSnapshot struct {
	Domains             map[string]*Domain             `json:"domains"`
	Packages            map[string]*Package            `json:"packages"`
	PackagesByName      map[string]string              `json:"packagesByName"`
	PackageAssociations map[string][]string            `json:"packageAssociations"`
	InboundConnections  map[string]*InboundConnection  `json:"inboundConnections"`
	OutboundConnections map[string]*OutboundConnection `json:"outboundConnections"`
	VpcEndpoints        map[string]*VpcEndpoint        `json:"vpcEndpoints"`
	AccountID           string                         `json:"accountID"`
	Region              string                         `json:"region"`
	NextID              int                            `json:"nextID"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Deep-copy all maps so the snapshot is independent of live state.
	// Domains are serialized with their Tags intact (Tags implements json.Marshaler).
	domains := make(map[string]*Domain, len(b.domains))
	for k, v := range b.domains {
		cp := *v
		domains[k] = &cp
	}

	packages := make(map[string]*Package, len(b.packages))
	for k, v := range b.packages {
		cp := *v
		packages[k] = &cp
	}

	packagesByName := make(map[string]string, len(b.packagesByName))
	maps.Copy(packagesByName, b.packagesByName)

	packageAssociations := make(map[string][]string, len(b.packageAssociations))
	for k, v := range b.packageAssociations {
		cp := make([]string, len(v))
		copy(cp, v)
		packageAssociations[k] = cp
	}

	inbound := make(map[string]*InboundConnection, len(b.inboundConnections))
	for k, v := range b.inboundConnections {
		cp := *v
		inbound[k] = &cp
	}

	outbound := make(map[string]*OutboundConnection, len(b.outboundConnections))
	for k, v := range b.outboundConnections {
		cp := *v
		outbound[k] = &cp
	}

	vpcEndpoints := make(map[string]*VpcEndpoint, len(b.vpcEndpoints))
	for k, v := range b.vpcEndpoints {
		cp := *v
		if v.VpcOptions != nil {
			opts := make(map[string]string, len(v.VpcOptions))
			maps.Copy(opts, v.VpcOptions)
			cp.VpcOptions = opts
		}
		vpcEndpoints[k] = &cp
	}

	snap := backendSnapshot{
		Domains:             domains,
		Packages:            packages,
		PackagesByName:      packagesByName,
		PackageAssociations: packageAssociations,
		InboundConnections:  inbound,
		OutboundConnections: outbound,
		VpcEndpoints:        vpcEndpoints,
		AccountID:           b.accountID,
		Region:              b.region,
		NextID:              b.nextID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("elasticsearch: snapshot marshal failed", "err", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Close existing Tags to release Prometheus metrics before replacing state.
	for _, d := range b.domains {
		d.Tags.Close()
	}

	if snap.Domains == nil {
		snap.Domains = make(map[string]*Domain)
	}

	if snap.Packages == nil {
		snap.Packages = make(map[string]*Package)
	}

	if snap.PackagesByName == nil {
		snap.PackagesByName = make(map[string]string)
	}

	if snap.PackageAssociations == nil {
		snap.PackageAssociations = make(map[string][]string)
	}

	if snap.InboundConnections == nil {
		snap.InboundConnections = make(map[string]*InboundConnection)
	}

	if snap.OutboundConnections == nil {
		snap.OutboundConnections = make(map[string]*OutboundConnection)
	}

	if snap.VpcEndpoints == nil {
		snap.VpcEndpoints = make(map[string]*VpcEndpoint)
	}

	b.domains = snap.Domains
	b.packages = snap.Packages
	b.packagesByName = snap.PackagesByName
	b.packageAssociations = snap.PackageAssociations
	b.inboundConnections = snap.InboundConnections
	b.outboundConnections = snap.OutboundConnections
	b.vpcEndpoints = snap.VpcEndpoints
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.nextID = snap.NextID

	// Rebuild ARN index from restored state.
	b.arnIndex = make(map[string]string, len(b.domains))
	for name, d := range b.domains {
		b.arnIndex[d.ARN] = name
	}

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

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
