package mediapackage

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 and
// services/dax (data-driven, direct registration -- see pkgs/store's package
// doc for the underlying primitive).
//
// channels, originEndpoints, and harvestJobs each key off a real,
// non-json:"-" identity field the value type already carries
// (storedChannel.ID, storedOriginEndpoint.ID, storedHarvestJob.ID), and
// every one of those IDs is globally unique (MediaPackage v1 origin
// endpoint and harvest job IDs are top-level resource identifiers, not
// scoped to their parent channel -- ChannelID is just a foreign-key
// field). So all three register directly on b.registry, no DTO indirection
// needed.
//
// originEndpoints additionally gets a secondary store.Index grouping by
// ChannelID, replacing the linear scans DeleteChannel and
// ListOriginEndpoints previously did over the raw map to find a channel's
// endpoints.
//
// Left as a plain map (not store.Table-backed): tags
// (map[string]map[string]string, keyed by ARN) -- values are plain strings,
// not *T, so it does not fit store.Table's keyed-by-identity-value shape.
// See persistence.go for how it round-trips alongside the registered tables.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func storedChannelKeyFn(v *storedChannel) string { return v.ID }

func storedOriginEndpointKeyFn(v *storedOriginEndpoint) string { return v.ID }

func storedHarvestJobKeyFn(v *storedHarvestJob) string { return v.ID }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.channels = store.Register(b.registry, "channels", store.New(storedChannelKeyFn))

	b.originEndpoints = store.Register(b.registry, "originEndpoints", store.New(storedOriginEndpointKeyFn))
	b.originEndpointsByChannel = b.originEndpoints.AddIndex(
		"byChannel",
		func(v *storedOriginEndpoint) string { return v.ChannelID },
	)

	b.harvestJobs = store.Register(b.registry, "harvestJobs", store.New(storedHarvestJobKeyFn))
}
