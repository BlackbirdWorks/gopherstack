package quicksight

import (
	"fmt"
	"maps"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// ---- Namespaces ----

func (b *InMemoryBackend) CreateNamespace(
	accountID, namespace, capacityRegion string,
	tags map[string]string,
) (*Namespace, error) {
	if namespace == "" {
		return nil, ErrValidation
	}

	if capacityRegion == "" {
		capacityRegion = b.region
	}

	b.mu.Lock("CreateNamespace")
	defer b.mu.Unlock()

	key := nsKey(accountID, namespace)
	if b.namespaces.Has(key) {
		return nil, ErrNamespaceAlreadyExists
	}

	ns := &storedNamespace{
		Name:           namespace,
		Arn:            arn.Build("quicksight", b.region, accountID, fmt.Sprintf("namespace/%s", namespace)),
		CapacityRegion: capacityRegion,
		Status:         statusCreationSuccessful,
		IdentityStore:  identityStoreQuickSight,
	}
	b.namespaces.Put(ns)

	if len(tags) > 0 {
		b.tags[ns.Arn] = maps.Clone(tags)
	}

	return ns.toNamespace(), nil
}

func (b *InMemoryBackend) DescribeNamespace(accountID, namespace string) (*Namespace, error) {
	b.mu.RLock("DescribeNamespace")
	defer b.mu.RUnlock()

	ns, ok := b.namespaces.Get(nsKey(accountID, namespace))
	if !ok {
		return nil, ErrNamespaceNotFound
	}

	return ns.toNamespace(), nil
}

func (b *InMemoryBackend) DeleteNamespace(accountID, namespace string) error {
	if namespace == defaultNamespace {
		return ErrValidation
	}

	b.mu.Lock("DeleteNamespace")
	defer b.mu.Unlock()

	key := nsKey(accountID, namespace)
	if !b.namespaces.Delete(key) {
		return ErrNamespaceNotFound
	}

	return nil
}

func (b *InMemoryBackend) ListNamespaces(
	_ string,
	maxResults int32,
	nextToken string,
) ([]*Namespace, string, error) {
	b.mu.RLock("ListNamespaces")
	defer b.mu.RUnlock()

	all := b.namespaces.All()

	result, next := paginateNamespaces(all, maxResults, nextToken)

	return result, next, nil
}

func paginateNamespaces(all []*storedNamespace, maxResults int32, nextToken string) ([]*Namespace, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	start := 0
	if nextToken != "" {
		if off, err := decodePageToken(nextToken); err == nil {
			start = off
		}
	}

	end := start + int(maxResults)

	var next string
	if end < len(all) {
		next = encodePageToken(end)
	} else {
		end = len(all)
	}

	result := make([]*Namespace, 0, end-start)
	for _, ns := range all[start:end] {
		result = append(result, ns.toNamespace())
	}

	return result, next
}
