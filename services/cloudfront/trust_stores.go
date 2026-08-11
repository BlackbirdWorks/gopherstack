package cloudfront

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// isEmpty reports whether none of the bundle's fields have been set.
func (bundle TrustStoreCertificateBundle) isEmpty() bool {
	return bundle.S3Bucket == "" && bundle.S3Key == "" && bundle.InlineCertificateBundle == ""
}

func (b *InMemoryBackend) trustStoreARN(id string) string {
	return arn.Build("cloudfront", "", b.accountID, fmt.Sprintf("trust-store/%s", id))
}

// copyTrustStore returns a deep copy of a TrustStore. Must be called with the lock held.
func (b *InMemoryBackend) copyTrustStore(ts *TrustStore) *TrustStore {
	cp := *ts
	if ts.Tags != nil {
		cp.Tags = make(map[string]string, len(ts.Tags))
		maps.Copy(cp.Tags, ts.Tags)
	}

	return &cp
}

// CreateTrustStore creates a new trust store. Name must be unique among existing trust stores.
func (b *InMemoryBackend) CreateTrustStore(
	name, comment string, bundle TrustStoreCertificateBundle, tags map[string]string,
) (*TrustStore, error) {
	b.mu.Lock("CreateTrustStore")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name must not be empty", ErrValidation)
	}

	if _, exists := b.trustStoreByName[name]; exists {
		return nil, fmt.Errorf("%w: trust store with name %q already exists", ErrAlreadyExists, name)
	}

	id := generateID()
	ts := &TrustStore{
		ID:                                     id,
		ARN:                                    b.trustStoreARN(id),
		Name:                                   name,
		Comment:                                comment,
		Status:                                 statusDeployed,
		ETag:                                   uuid.NewString(),
		LastModifiedTime:                       time.Now().UTC().Format(time.RFC3339),
		CertificateAuthorityCertificatesBundle: bundle,
		Tags:                                   make(map[string]string, len(tags)),
	}
	maps.Copy(ts.Tags, tags)
	b.trustStores.Put(ts)
	b.trustStoreARNs[ts.ARN] = id
	b.trustStoreByName[name] = id

	return b.copyTrustStore(ts), nil
}

// GetTrustStore returns a trust store by ID.
func (b *InMemoryBackend) GetTrustStore(id string) (*TrustStore, error) {
	b.mu.RLock("GetTrustStore")
	defer b.mu.RUnlock()

	ts, ok := b.trustStores.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: trust store %s not found", ErrTrustStoreNotFound, id)
	}

	return b.copyTrustStore(ts), nil
}

// ListTrustStores returns all trust stores sorted by ID.
func (b *InMemoryBackend) ListTrustStores() []*TrustStore {
	b.mu.RLock("ListTrustStores")
	defer b.mu.RUnlock()

	out := make([]*TrustStore, 0, b.trustStores.Len())
	for _, ts := range b.trustStores.All() {
		out = append(out, b.copyTrustStore(ts))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// UpdateTrustStore updates an existing trust store. Empty fields (name, comment, and an empty
// certificate bundle) leave the corresponding current value unchanged. If name changes, it must
// remain unique among existing trust stores.
func (b *InMemoryBackend) UpdateTrustStore(
	id, name, comment string, bundle TrustStoreCertificateBundle,
) (*TrustStore, error) {
	b.mu.Lock("UpdateTrustStore")
	defer b.mu.Unlock()

	ts, ok := b.trustStores.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: trust store %s not found", ErrTrustStoreNotFound, id)
	}

	if name != "" && name != ts.Name {
		if _, exists := b.trustStoreByName[name]; exists {
			return nil, fmt.Errorf("%w: trust store with name %q already exists", ErrAlreadyExists, name)
		}
		delete(b.trustStoreByName, ts.Name)
		b.trustStoreByName[name] = id
		ts.Name = name
	}

	if comment != "" {
		ts.Comment = comment
	}

	if !bundle.isEmpty() {
		ts.CertificateAuthorityCertificatesBundle = bundle
	}

	ts.ETag = uuid.NewString()
	ts.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)

	return b.copyTrustStore(ts), nil
}

// DeleteTrustStore deletes a trust store by ID.
func (b *InMemoryBackend) DeleteTrustStore(id string) error {
	b.mu.Lock("DeleteTrustStore")
	defer b.mu.Unlock()

	ts, ok := b.trustStores.Get(id)
	if !ok {
		return fmt.Errorf("%w: trust store %s not found", ErrTrustStoreNotFound, id)
	}

	delete(b.trustStoreByName, ts.Name)
	delete(b.trustStoreARNs, ts.ARN)
	b.trustStores.Delete(id)

	return nil
}

// ---------------------------------------------------------------------------
// StreamingDistribution
// ---------------------------------------------------------------------------
