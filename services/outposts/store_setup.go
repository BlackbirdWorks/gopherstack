package outposts

import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func outpostKeyFn(v *Outpost) string { return v.ID }

func outpostSiteIndexKeyFn(v *Outpost) string { return v.SiteID }

func siteKeyFn(v *Site) string { return v.ID }

func orderKeyFn(v *Order) string { return v.ID }

func orderOutpostIndexKeyFn(v *Order) string { return v.OutpostID }

func quoteKeyFn(v *Quote) string { return v.ID }

func capacityTaskKeyFn(v *CapacityTask) string { return v.ID }

func capacityTaskOutpostIndexKeyFn(v *CapacityTask) string { return v.OutpostID }

func assetKeyFn(v *Asset) string { return v.ID }

func assetOutpostIndexKeyFn(v *Asset) string { return v.OutpostID }

func connectionKeyFn(v *Connection) string { return v.ID }

// registerAllTables registers every resource collection exactly once. Must
// be called during construction only -- see services/s3tables/store_setup.go's
// doc comment for why (store.Register panics on a duplicate name).
func registerAllTables(b *InMemoryBackend) {
	b.outposts = store.Register(b.registry, "outposts", store.New(outpostKeyFn))
	b.outpostsBySite = b.outposts.AddIndex("bySite", outpostSiteIndexKeyFn)

	b.sites = store.Register(b.registry, "sites", store.New(siteKeyFn))

	b.orders = store.Register(b.registry, "orders", store.New(orderKeyFn))
	b.ordersByOutpost = b.orders.AddIndex("byOutpost", orderOutpostIndexKeyFn)

	b.quotes = store.Register(b.registry, "quotes", store.New(quoteKeyFn))

	b.capacityTasks = store.Register(b.registry, "capacityTasks", store.New(capacityTaskKeyFn))
	b.capacityTasksByOutpost = b.capacityTasks.AddIndex("byOutpost", capacityTaskOutpostIndexKeyFn)

	b.assets = store.Register(b.registry, "assets", store.New(assetKeyFn))
	b.assetsByOutpost = b.assets.AddIndex("byOutpost", assetOutpostIndexKeyFn)

	b.connections = store.Register(b.registry, "connections", store.New(connectionKeyFn))
}
