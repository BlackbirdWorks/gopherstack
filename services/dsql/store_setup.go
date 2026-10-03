package dsql

import "github.com/blackbirdworks/gopherstack/pkgs/store"

func clusterKeyFn(v *Cluster) string { return v.Identifier }

func streamKeyFn(v *Stream) string { return streamKey(v.ClusterIdentifier, v.StreamIdentifier) }

// registerAllTables registers every backend resource table exactly once.
// Must be called during construction only -- store.Register panics on a
// duplicate name.
func registerAllTables(b *InMemoryBackend) {
	b.clusters = store.Register(b.registry, "clusters", store.New(clusterKeyFn))
	b.streams = store.Register(b.registry, "streams", store.New(streamKeyFn))
}
