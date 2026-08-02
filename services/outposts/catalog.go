package outposts

import "github.com/blackbirdworks/gopherstack/pkgs/page"

// GetCatalogItem returns the seeded catalog item with the given ID (a
// static reference catalog -- see seed_data.go).
func (b *InMemoryBackend) GetCatalogItem(id string) (CatalogItem, error) {
	item, ok := findCatalogItem(id)
	if !ok {
		return CatalogItem{}, notFoundError(resourceCatalogItem, id)
	}

	return item, nil
}

// catalogItemFilter holds ListCatalogItems' optional filters.
type catalogItemFilter struct {
	ec2Families      []string
	itemClasses      []string
	supportedStorage []string
}

func matchesCatalogItemFilter(c CatalogItem, f catalogItemFilter) bool {
	if len(f.itemClasses) > 0 && !containsStr(f.itemClasses, c.ItemClass) {
		return false
	}

	if len(f.supportedStorage) > 0 {
		found := false

		for _, s := range f.supportedStorage {
			if containsStr(c.SupportedStorage, s) {
				found = true

				break
			}
		}

		if !found {
			return false
		}
	}

	if len(f.ec2Families) > 0 {
		found := false

		for _, ec := range c.EC2Capacities {
			if containsStr(f.ec2Families, ec.Family) {
				found = true

				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

// ListCatalogItems returns a page of the static seeded catalog matching f.
func (b *InMemoryBackend) ListCatalogItems(f catalogItemFilter, token string, limit int) page.Page[CatalogItem] {
	filtered := make([]CatalogItem, 0, len(catalogItemSeed))

	for _, c := range catalogItemSeed {
		if matchesCatalogItemFilter(c, f) {
			filtered = append(filtered, c)
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit)
}

// ListOrderableInstanceTypes returns a page of the static seeded orderable
// instance types, optionally filtered by OutpostGeneration.
func (b *InMemoryBackend) ListOrderableInstanceTypes(
	generationFilter, token string, limit int,
) page.Page[OrderableInstanceType] {
	all := orderableInstanceTypeSeed
	if generationFilter == "" {
		return page.New(all, token, limit, defaultPageLimit)
	}

	filtered := make([]OrderableInstanceType, 0, len(all))

	for _, it := range all {
		if it.OutpostGeneration == generationFilter {
			filtered = append(filtered, it)
		}
	}

	return page.New(filtered, token, limit, defaultPageLimit)
}
