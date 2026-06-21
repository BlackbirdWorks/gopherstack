package elasticsearch

import (
	"context"
	"encoding/json"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// backendSnapshot persists the backend state. All resource maps are nested by
// region (outer key = region) so same-named resources in different regions stay
// isolated across snapshot/restore.
type backendSnapshot struct {
	Domains             map[string]map[string]*Domain             `json:"domains"`
	Packages            map[string]map[string]*Package            `json:"packages"`
	PackagesByName      map[string]map[string]string              `json:"packagesByName"`
	PackageAssociations map[string]map[string][]string            `json:"packageAssociations"`
	InboundConnections  map[string]map[string]*InboundConnection  `json:"inboundConnections"`
	OutboundConnections map[string]map[string]*OutboundConnection `json:"outboundConnections"`
	VpcEndpoints        map[string]map[string]*VpcEndpoint        `json:"vpcEndpoints"`
	VpcAccess           map[string]map[string][]string            `json:"vpcAccess"`
	ReservedInstances   map[string]map[string]*ReservedInstance   `json:"reservedInstances"`
	AccountID           string                                    `json:"accountID"`
	Region              string                                    `json:"region"`
	NextID              int                                       `json:"nextID"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Domains:             snapshotDomains(b.domains),
		Packages:            snapshotPackages(b.packages),
		PackagesByName:      snapshotStringMaps(b.packagesByName),
		PackageAssociations: snapshotStringSliceMaps(b.packageAssociations),
		InboundConnections:  snapshotInbound(b.inboundConnections),
		OutboundConnections: snapshotOutbound(b.outboundConnections),
		VpcEndpoints:        snapshotVpcEndpoints(b.vpcEndpoints),
		VpcAccess:           snapshotStringSliceMaps(b.vpcAccess),
		ReservedInstances:   snapshotReserved(b.reservedInstances),
		AccountID:           b.accountID,
		Region:              b.region,
		NextID:              b.nextID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "elasticsearch: snapshot marshal failed", "err", err)

		return nil
	}

	return data
}

// snapshotDomains deep-copies the region-nested domain map.
// Domains are serialized with their Tags intact (Tags implements json.Marshaler).
func snapshotDomains(src map[string]map[string]*Domain) map[string]map[string]*Domain {
	out := make(map[string]map[string]*Domain, len(src))
	for region, domains := range src {
		regionCopy := make(map[string]*Domain, len(domains))
		for name, d := range domains {
			cp := *d
			regionCopy[name] = &cp
		}
		out[region] = regionCopy
	}

	return out
}

// snapshotPackages deep-copies the region-nested package map.
func snapshotPackages(src map[string]map[string]*Package) map[string]map[string]*Package {
	out := make(map[string]map[string]*Package, len(src))
	for region, packages := range src {
		regionCopy := make(map[string]*Package, len(packages))
		for id, p := range packages {
			cp := *p
			regionCopy[id] = &cp
		}
		out[region] = regionCopy
	}

	return out
}

// snapshotStringMaps deep-copies a region-nested map[string]string.
func snapshotStringMaps(src map[string]map[string]string) map[string]map[string]string {
	out := make(map[string]map[string]string, len(src))
	for region, inner := range src {
		regionCopy := make(map[string]string, len(inner))
		maps.Copy(regionCopy, inner)
		out[region] = regionCopy
	}

	return out
}

// snapshotStringSliceMaps deep-copies a region-nested map[string][]string.
func snapshotStringSliceMaps(src map[string]map[string][]string) map[string]map[string][]string {
	out := make(map[string]map[string][]string, len(src))
	for region, inner := range src {
		regionCopy := make(map[string][]string, len(inner))
		for k, v := range inner {
			regionCopy[k] = append([]string(nil), v...)
		}
		out[region] = regionCopy
	}

	return out
}

// snapshotInbound deep-copies the region-nested inbound connection map.
func snapshotInbound(src map[string]map[string]*InboundConnection) map[string]map[string]*InboundConnection {
	out := make(map[string]map[string]*InboundConnection, len(src))
	for region, conns := range src {
		regionCopy := make(map[string]*InboundConnection, len(conns))
		for id, c := range conns {
			cp := *c
			regionCopy[id] = &cp
		}
		out[region] = regionCopy
	}

	return out
}

// snapshotOutbound deep-copies the region-nested outbound connection map.
func snapshotOutbound(src map[string]map[string]*OutboundConnection) map[string]map[string]*OutboundConnection {
	out := make(map[string]map[string]*OutboundConnection, len(src))
	for region, conns := range src {
		regionCopy := make(map[string]*OutboundConnection, len(conns))
		for id, c := range conns {
			cp := *c
			regionCopy[id] = &cp
		}
		out[region] = regionCopy
	}

	return out
}

// snapshotVpcEndpoints deep-copies the region-nested VPC endpoint map, cloning VpcOptions.
func snapshotVpcEndpoints(src map[string]map[string]*VpcEndpoint) map[string]map[string]*VpcEndpoint {
	out := make(map[string]map[string]*VpcEndpoint, len(src))
	for region, endpoints := range src {
		regionCopy := make(map[string]*VpcEndpoint, len(endpoints))
		for id, ep := range endpoints {
			cp := *ep
			if ep.VpcOptions != nil {
				opts := make(map[string]string, len(ep.VpcOptions))
				maps.Copy(opts, ep.VpcOptions)
				cp.VpcOptions = opts
			}
			regionCopy[id] = &cp
		}
		out[region] = regionCopy
	}

	return out
}

// snapshotReserved deep-copies the region-nested reserved instance map.
func snapshotReserved(src map[string]map[string]*ReservedInstance) map[string]map[string]*ReservedInstance {
	out := make(map[string]map[string]*ReservedInstance, len(src))
	for region, reserved := range src {
		regionCopy := make(map[string]*ReservedInstance, len(reserved))
		for id, ri := range reserved {
			cp := *ri
			regionCopy[id] = &cp
		}
		out[region] = regionCopy
	}

	return out
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	// Close existing Tags to release Prometheus metrics before replacing state.
	for _, regionDomains := range b.domains {
		for _, d := range regionDomains {
			d.Tags.Close()
		}
	}

	ensureNonNilMaps(&snap)

	b.domains = snap.Domains
	b.packages = snap.Packages
	b.packagesByName = snap.PackagesByName
	b.packageAssociations = snap.PackageAssociations
	b.inboundConnections = snap.InboundConnections
	b.outboundConnections = snap.OutboundConnections
	b.vpcEndpoints = snap.VpcEndpoints
	b.vpcAccess = snap.VpcAccess
	b.reservedInstances = snap.ReservedInstances
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.nextID = snap.NextID

	// Rebuild the region-nested ARN index from restored state.
	b.arnIndex = make(map[string]map[string]string, len(b.domains))
	for region, domains := range b.domains {
		index := make(map[string]string, len(domains))
		for name, d := range domains {
			index[d.ARN] = name
		}
		b.arnIndex[region] = index
	}

	return nil
}

// ensureNonNilMaps initialises nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Domains == nil {
		snap.Domains = make(map[string]map[string]*Domain)
	}

	if snap.Packages == nil {
		snap.Packages = make(map[string]map[string]*Package)
	}

	if snap.PackagesByName == nil {
		snap.PackagesByName = make(map[string]map[string]string)
	}

	if snap.PackageAssociations == nil {
		snap.PackageAssociations = make(map[string]map[string][]string)
	}

	if snap.InboundConnections == nil {
		snap.InboundConnections = make(map[string]map[string]*InboundConnection)
	}

	if snap.OutboundConnections == nil {
		snap.OutboundConnections = make(map[string]map[string]*OutboundConnection)
	}

	if snap.VpcEndpoints == nil {
		snap.VpcEndpoints = make(map[string]map[string]*VpcEndpoint)
	}

	if snap.VpcAccess == nil {
		snap.VpcAccess = make(map[string]map[string][]string)
	}

	if snap.ReservedInstances == nil {
		snap.ReservedInstances = make(map[string]map[string]*ReservedInstance)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// Reset implements service.Resettable by delegating to the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
}
