package s3tables

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateNamespace creates a new namespace within a table bucket.
func (b *InMemoryBackend) CreateNamespace(
	tableBucketARN string,
	namespace []string,
) (*Namespace, error) {
	if err := validateNamespaceParts(namespace); err != nil {
		return nil, err
	}

	b.muBuckets.RLock("CreateNamespace")
	defer b.muBuckets.RUnlock()

	b.muNamespaces.Lock("CreateNamespace")
	defer b.muNamespaces.Unlock()

	if !b.tableBuckets.Has(tableBucketARN) {
		return nil, fmt.Errorf(
			"%w: table bucket %q not found",
			ErrTableBucketNotFound,
			tableBucketARN,
		)
	}

	nsStr := joinNamespace(namespace)
	key := namespaceKey(tableBucketARN, nsStr)

	if b.namespaces.Has(key) {
		return nil, fmt.Errorf(
			"%w: namespace %q already exists in bucket %s",
			ErrNamespaceAlreadyExists,
			nsStr,
			tableBucketARN,
		)
	}

	ns := &Namespace{
		Namespace:      cloneStringSlice(namespace),
		TableBucketARN: tableBucketARN,
		OwnerAccountID: b.accountID,
		CreatedBy:      b.accountID,
		CreatedAt:      time.Now().UTC(),
		NamespaceID:    uuid.NewString(),
	}
	b.namespaces.Put(ns)

	return cloneNamespace(ns), nil
}

// GetNamespace returns a namespace by bucket ARN and namespace name.
func (b *InMemoryBackend) GetNamespace(
	tableBucketARN string,
	namespace []string,
) (*Namespace, error) {
	b.muNamespaces.RLock("GetNamespace")
	defer b.muNamespaces.RUnlock()

	nsStr := joinNamespace(namespace)
	key := namespaceKey(tableBucketARN, nsStr)

	ns, ok := b.namespaces.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: namespace %q not found in bucket %s",
			ErrNamespaceNotFound,
			nsStr,
			tableBucketARN,
		)
	}

	return cloneNamespace(ns), nil
}

// DeleteNamespace deletes a namespace from a table bucket.
func (b *InMemoryBackend) DeleteNamespace(tableBucketARN string, namespace []string) error {
	b.muNamespaces.Lock("DeleteNamespace")
	defer b.muNamespaces.Unlock()

	nsStr := joinNamespace(namespace)
	key := namespaceKey(tableBucketARN, nsStr)

	if !b.namespaces.Has(key) {
		return fmt.Errorf(
			"%w: namespace %q not found in bucket %s",
			ErrNamespaceNotFound,
			nsStr,
			tableBucketARN,
		)
	}

	b.namespaces.Delete(key)

	return nil
}

// ListNamespaces returns all namespaces in a table bucket sorted by name.
func (b *InMemoryBackend) ListNamespaces(
	tableBucketARN string,
	p ListNamespacesParams,
) (page.Page[*Namespace], error) {
	if err := page.ValidateToken(p.ContinuationToken); err != nil {
		return page.Page[*Namespace]{}, fmt.Errorf(
			"%w: invalid continuationToken",
			ErrInvalidContinuationToken,
		)
	}

	b.muBuckets.RLock("ListNamespaces")
	defer b.muBuckets.RUnlock()

	b.muNamespaces.RLock("ListNamespaces")
	defer b.muNamespaces.RUnlock()

	if !b.tableBuckets.Has(tableBucketARN) {
		return page.Page[*Namespace]{}, fmt.Errorf(
			"%w: table bucket %q not found",
			ErrTableBucketNotFound,
			tableBucketARN,
		)
	}

	items := b.namespacesByBucket.Get(tableBucketARN)
	list := make([]*Namespace, 0, len(items))

	for _, ns := range items {
		if p.Prefix != "" && !strings.HasPrefix(joinNamespace(ns.Namespace), p.Prefix) {
			continue
		}

		list = append(list, cloneNamespace(ns))
	}

	sort.Slice(list, func(i, j int) bool {
		return joinNamespace(list[i].Namespace) < joinNamespace(list[j].Namespace)
	})

	return page.New(list, p.ContinuationToken, p.MaxNamespaces, s3tablesDefaultMaxNamespaces), nil
}

func cloneNamespace(ns *Namespace) *Namespace {
	cp := *ns
	cp.Namespace = cloneStringSlice(ns.Namespace)

	return &cp
}
