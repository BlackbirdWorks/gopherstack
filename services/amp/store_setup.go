package amp

import "github.com/blackbirdworks/gopherstack/pkgs/store"

func registerAllTables(b *InMemoryBackend) {
	b.workspaces = store.Register(b.registry, "workspaces", store.New(workspaceKey))
	b.namespaces = store.Register(b.registry, "ruleGroupsNamespaces", store.New(namespaceKey))
	b.scrapers = store.Register(b.registry, "scrapers", store.New(scraperKey))
	b.detectors = store.Register(b.registry, "anomalyDetectors", store.New(detectorKeyOf))
	b.series = store.Register(b.registry, "series", store.New(seriesKeyOf))
	b.seriesByWS = b.series.AddIndex("byWorkspace", seriesWorkspace)
	b.metadata = store.Register(b.registry, "metadata", store.New(metadataKeyOf))
	b.metadataByWS = b.metadata.AddIndex("byWorkspace", metadataWorkspace)
}
