package iotanalytics

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 /
// services/codebuild / services/dax (data-driven, direct registration --
// see pkgs/store's package doc for the underlying primitive).
//
// channels, datastores, datasets, and pipelines each key off a real,
// non-json:"-" identity field the value type already carries (Channel.Name,
// Datastore.Name, Dataset.Name, Pipeline.Name), so all four register
// directly on b.registry -- no DTO indirection is needed. No secondary
// store.Index is needed either: IoT Analytics has no ARN-keyed
// reverse-lookup map (resolveARNResource parses the resource name back out
// of the ARN string and looks it up directly by name) and no cross-resource
// grouping is ever derived from these four tables.
//
// tags (map[string]map[string]string, keyed by resource ARN),
// channelMessages (map[string][]ChannelMessage, keyed by channel name), and
// datasetContents (map[string][]*DatasetContent, keyed by dataset name) are
// left as plain fields: none of their value types is a *T -- tags' and
// channelMessages' values are non-pointer maps/slices, and datasetContents'
// value is a slice of *DatasetContent rather than a single *DatasetContent
// -- so none fits store.Table's keyed-by-single-identity-value shape. See
// persistence.go for how they round-trip alongside the registered tables.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func channelKeyFn(v *Channel) string { return v.Name }

func datastoreKeyFn(v *Datastore) string { return v.Name }

func datasetKeyFn(v *Dataset) string { return v.Name }

func pipelineKeyFn(v *Pipeline) string { return v.Name }

// registerAllTables registers every backend resource table exactly once. It
// must be called during construction only, immediately after b.registry is
// created -- store.Register panics on a duplicate name, so runtime resets go
// through b.registry.ResetAll() instead (see InMemoryBackend.Reset in store.go).
func registerAllTables(b *InMemoryBackend) {
	b.channels = store.Register(b.registry, "channels", store.New(channelKeyFn))
	b.datastores = store.Register(b.registry, "datastores", store.New(datastoreKeyFn))
	b.datasets = store.Register(b.registry, "datasets", store.New(datasetKeyFn))
	b.pipelines = store.Register(b.registry, "pipelines", store.New(pipelineKeyFn))
}
