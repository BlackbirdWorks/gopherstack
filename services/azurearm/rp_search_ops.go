package azurearm

import (
	"context"
	"net/url"
	"slices"
	"strings"
)

// AdminKeys returns the service's admin keys, regenerating the named one first when kind is set.
func (p *SearchProvider) AdminKeys(id ResourceID, kind string) (SearchAdminKeys, error) {
	if kind != "" && !strings.EqualFold(kind, "primary") && !strings.EqualFold(kind, "secondary") {
		return SearchAdminKeys{}, badSearchRequest("keyKind must be primary or secondary")
	}

	p.mu.RLock("AdminKeys")
	defer p.mu.RUnlock()

	rec, err := p.service(id)
	if err != nil {
		return SearchAdminKeys{}, err
	}

	if kind == "" {
		return p.dataPlane.AdminKeys(rec.name)
	}

	return p.dataPlane.RegenerateAdminKey(rec.name, strings.ToLower(kind))
}

// CreateQueryKey creates a named query key.
func (p *SearchProvider) CreateQueryKey(id ResourceID, name string) (SearchQueryKey, error) {
	p.mu.RLock("CreateQueryKey")
	defer p.mu.RUnlock()

	rec, err := p.service(id)
	if err != nil {
		return SearchQueryKey{}, err
	}

	decoded, uErr := url.PathUnescape(name)
	if uErr != nil {
		decoded = name
	}

	return p.dataPlane.CreateQueryKey(rec.name, decoded)
}

// ListQueryKeys lists the service's query keys.
func (p *SearchProvider) ListQueryKeys(id ResourceID) ([]SearchQueryKey, error) {
	p.mu.RLock("ListQueryKeys")
	defer p.mu.RUnlock()

	rec, err := p.service(id)
	if err != nil {
		return nil, err
	}

	return p.dataPlane.QueryKeys(rec.name)
}

// DeleteQueryKey deletes a query key by its value.
func (p *SearchProvider) DeleteQueryKey(id ResourceID, key string) error {
	p.mu.RLock("DeleteQueryKey")
	defer p.mu.RUnlock()

	rec, err := p.service(id)
	if err != nil {
		return err
	}

	return p.dataPlane.DeleteQueryKey(rec.name, key)
}

// srchQuota is the documented default services-per-subscription limit by SKU (unverified for
// tenants with raised quotas).
var srchQuota = map[string]int{ //nolint:gochecknoglobals // read-only quota table
	"free": 1, srchSKUBasic: srchDefaultQuota, srchSKUStandard: srchDefaultQuota, "standard2": 16, "standard3": 16,
	"storage_optimized_l1": srchDefaultQuota, "storage_optimized_l2": srchDefaultQuota,
}

// Usages reports per-SKU service counts in the subscription against the SKU quota; sku narrows the result.
func (p *SearchProvider) Usages(subscription, location, sku string) ([]map[string]any, error) {
	p.mu.RLock("Usages")
	defer p.mu.RUnlock()

	skus := make([]string, 0, len(srchQuota))
	for name := range srchQuota {
		skus = append(skus, name)
	}

	slices.Sort(skus)

	if sku != "" {
		if _, ok := srchQuota[strings.ToLower(sku)]; !ok {
			return nil, ErrSearchServiceNotFound
		}

		skus = []string{strings.ToLower(sku)}
	}

	out := make([]map[string]any, 0, len(skus))

	for _, name := range skus {
		used := 0

		for _, r := range p.res {
			if r.sku == name && strings.EqualFold(r.location, location) {
				used++
			}
		}

		out = append(out, map[string]any{
			"id": "/subscriptions/" + subscription + "/providers/Microsoft.Search/locations/" + location +
				"/usages/" + name,
			"unit": "Count", "currentValue": used, "limit": srchQuota[name],
			fieldName: map[string]any{fieldValue: name, "localizedValue": name},
		})
	}

	return out, nil
}

// ListKeys implements ResourceProvider; search keys use listAdminKeys/listQueryKeys instead.
func (p *SearchProvider) ListKeys(_ context.Context, id ResourceID) (map[string]any, error) {
	return nil, errUnsupportedSearchType(id)
}

const srchDefaultQuota = 16
