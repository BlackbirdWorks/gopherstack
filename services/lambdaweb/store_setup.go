package lambdaweb

import "github.com/blackbirdworks/gopherstack/pkgs/store"

func functionKeyFn(v *Function) string { return functionKey(v.Region, v.Name) }

func revisionKeyFn(v *Revision) string { return revisionKey(v.Region, v.FunctionName, v.ID) }

func endpointKeyFn(v *Endpoint) string { return endpointKey(v.Region, v.FunctionName, v.Name) }

// registerAllTables registers every backend resource table exactly once.
func registerAllTables(b *InMemoryBackend) {
	b.functions = store.Register(b.registry, "functions", store.New(functionKeyFn))
	b.revisions = store.Register(b.registry, "revisions", store.New(revisionKeyFn))
	b.endpoints = store.Register(b.registry, "endpoints", store.New(endpointKeyFn))
}
