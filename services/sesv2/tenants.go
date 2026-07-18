package sesv2

import (
	"fmt"
	"maps"
)

const keyTenantName = "TenantName"

// ---- tenants ----

func (b *InMemoryBackend) CreateTenant(tenantName string) (map[string]any, error) {
	b.mu.Lock("CreateTenant")
	defer b.mu.Unlock()

	b.tenants[tenantName] = map[string]any{keyTenantName: tenantName, keyStatus: "ACTIVE"}

	return map[string]any{keyTenantName: tenantName}, nil
}

func (b *InMemoryBackend) GetTenant(tenantName string) (map[string]any, error) {
	b.mu.RLock("GetTenant")
	defer b.mu.RUnlock()

	t, ok := b.tenants[tenantName]
	if !ok {
		return nil, fmt.Errorf("%w: Tenant %s not found", ErrNotFound, tenantName)
	}

	out := make(map[string]any, len(t))
	maps.Copy(out, t)

	return out, nil
}

func (b *InMemoryBackend) DeleteTenant(tenantName string) error {
	b.mu.Lock("DeleteTenant")
	defer b.mu.Unlock()

	delete(b.tenants, tenantName)

	return nil
}

func (b *InMemoryBackend) ListTenants(nextToken string, pageSize int) ([]map[string]any, string, error) {
	b.mu.RLock("ListTenants")

	all := make([]map[string]any, 0, len(b.tenants))
	for _, t := range b.tenants {
		cp := make(map[string]any, len(t))
		maps.Copy(cp, t)
		all = append(all, cp)
	}

	b.mu.RUnlock()

	return paginateMaps(all, nextToken, pageSize, keyTenantName)
}

func (b *InMemoryBackend) CreateTenantResourceAssociation(tenantName, resourceArn string) error {
	b.mu.Lock("CreateTenantResourceAssociation")
	defer b.mu.Unlock()

	b.tenantResources[tenantName] = append(b.tenantResources[tenantName], resourceArn)
	b.resourceTenants[resourceArn] = append(b.resourceTenants[resourceArn], tenantName)

	return nil
}

func (b *InMemoryBackend) DeleteTenantResourceAssociation(tenantName, resourceArn string) error {
	b.mu.Lock("DeleteTenantResourceAssociation")
	defer b.mu.Unlock()

	b.tenantResources[tenantName] = removeString(b.tenantResources[tenantName], resourceArn)
	b.resourceTenants[resourceArn] = removeString(b.resourceTenants[resourceArn], tenantName)

	return nil
}

func (b *InMemoryBackend) ListResourceTenants(
	resourceArn string,
	_ int,
) ([]map[string]any, string, error) {
	b.mu.RLock("ListResourceTenants")
	names := b.resourceTenants[resourceArn]
	b.mu.RUnlock()

	out := make([]map[string]any, 0, len(names))
	for _, n := range names {
		out = append(out, map[string]any{keyTenantName: n})
	}

	return out, "", nil
}

func (b *InMemoryBackend) ListTenantResources(
	tenantName string,
	_ int,
) ([]map[string]any, string, error) {
	b.mu.RLock("ListTenantResources")
	arns := b.tenantResources[tenantName]
	b.mu.RUnlock()

	out := make([]map[string]any, 0, len(arns))
	for _, a := range arns {
		out = append(out, map[string]any{"ResourceArn": a})
	}

	return out, "", nil
}

func removeString(s []string, v string) []string {
	out := s[:0]
	for _, x := range s {
		if x != v {
			out = append(out, x)
		}
	}

	return out
}
