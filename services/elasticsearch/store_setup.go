package elasticsearch

// Code in this file supports Phase 3.3 of the datalayer refactor: the six
// resource collections that were previously nested by region (outer key =
// region, e.g. map[string]map[string]*Domain) are each replaced by a single
// flat *store.Table[T], keyed by the composite "region|id" string (see
// regionKey in backend.go), with a companion *store.Index grouping entries by
// region for the per-region scans the nested maps used to answer directly --
// the region-qualified-table pattern services/emr and services/codeartifact
// use.
//
// None of Domain, Package, InboundConnection, OutboundConnection,
// VpcEndpoint, or ReservedInstance carried a region field of their own before
// this conversion, so each gained an unexported region field purely for this
// composite key (see the field comments in backend.go). Every table below is
// therefore built with store.New only and also registered on b.registry via
// store.Register -- registering them keeps InMemoryBackend.Reset a single
// b.registry.ResetAll() call (mirrors services/emr), while persistence.go
// deliberately never calls b.registry.SnapshotAll()/RestoreAll() directly: a
// plain json.Marshal/Unmarshal of these value types cannot see the
// unexported region field, so Snapshot/Restore instead route every table
// through an ephemeral regionalDTO-wrapped registry that carries region
// alongside Value (see persistence.go).
//
// arnIndex, packagesByName, packageAssociations, and vpcAccess are
// deliberately NOT converted: store.Table requires a *V value with its own
// identity, but each entry in these four maps is a bare string or string
// slice with no identifier of its own. They remain plain region-nested maps
// (see the accessor helpers in backend.go), persisted directly by
// persistence.go exactly as before this refactor.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func domainKeyFn(v *Domain) string            { return regionKey(v.region, v.Name) }
func domainRegionIndexKeyFn(v *Domain) string { return v.region }

func packageKeyFn(v *Package) string            { return regionKey(v.region, v.ID) }
func packageRegionIndexKeyFn(v *Package) string { return v.region }

func inboundConnectionKeyFn(v *InboundConnection) string {
	return regionKey(v.region, v.ConnectionID)
}
func inboundConnectionRegionIndexKeyFn(v *InboundConnection) string { return v.region }

func outboundConnectionKeyFn(v *OutboundConnection) string {
	return regionKey(v.region, v.ConnectionID)
}
func outboundConnectionRegionIndexKeyFn(v *OutboundConnection) string { return v.region }

func vpcEndpointKeyFn(v *VpcEndpoint) string            { return regionKey(v.region, v.ID) }
func vpcEndpointRegionIndexKeyFn(v *VpcEndpoint) string { return v.region }

func reservedInstanceKeyFn(v *ReservedInstance) string {
	return regionKey(v.region, v.ReservationID)
}
func reservedInstanceRegionIndexKeyFn(v *ReservedInstance) string { return v.region }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created -- see NewInMemoryBackend), never
// on every Reset() -- store.Register panics on a duplicate name, so runtime
// resets go through b.registry.ResetAll() instead (see InMemoryBackend.Reset
// in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.domains = store.Register(b.registry, "domains", store.New(domainKeyFn))
	b.domainsByRegion = b.domains.AddIndex("byRegion", domainRegionIndexKeyFn)

	b.packages = store.Register(b.registry, "packages", store.New(packageKeyFn))
	b.packagesByRegion = b.packages.AddIndex("byRegion", packageRegionIndexKeyFn)

	b.inboundConnections = store.Register(b.registry, "inboundConnections", store.New(inboundConnectionKeyFn))
	b.inboundConnectionsByRegion = b.inboundConnections.AddIndex("byRegion", inboundConnectionRegionIndexKeyFn)

	b.outboundConnections = store.Register(b.registry, "outboundConnections", store.New(outboundConnectionKeyFn))
	b.outboundConnectionsByRegion = b.outboundConnections.AddIndex("byRegion", outboundConnectionRegionIndexKeyFn)

	b.vpcEndpoints = store.Register(b.registry, "vpcEndpoints", store.New(vpcEndpointKeyFn))
	b.vpcEndpointsByRegion = b.vpcEndpoints.AddIndex("byRegion", vpcEndpointRegionIndexKeyFn)

	b.reservedInstances = store.Register(b.registry, "reservedInstances", store.New(reservedInstanceKeyFn))
	b.reservedInstancesByRegion = b.reservedInstances.AddIndex("byRegion", reservedInstanceRegionIndexKeyFn)
}
