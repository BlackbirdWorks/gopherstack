package elbv2

import (
	"fmt"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (b *InMemoryBackend) trustStoreARN(id string) string {
	return arn.Build("elasticloadbalancing", b.region, b.accountID, "truststore/"+id)
}

// CreateTrustStore creates a new trust store. s3Bucket/s3Key/s3ObjectVersion
// are CreateTrustStoreInput's CaCertificatesBundleS3* fields (Bucket/Key
// required on the real wire) -- stored inertly, see TrustStore's doc comment.
func (b *InMemoryBackend) CreateTrustStore(
	name string,
	kvs []tags.KV,
	s3Bucket, s3Key, s3ObjectVersion string,
) (*TrustStore, error) {
	b.mu.Lock("CreateTrustStore")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	for _, ts := range b.trustStores.All() {
		if ts.Name == name {
			return nil, ErrTrustStoreAlreadyExists
		}
	}

	id := name + "/" + uuid.New().String()
	tsArn := b.trustStoreARN(id)

	t := tags.New("elbv2.ts." + name + ".tags")
	for _, kv := range kvs {
		t.Set(kv.Key, kv.Value)
	}

	ts := &TrustStore{
		TrustStoreArn:                       tsArn,
		Name:                                name,
		Status:                              "ACTIVE",
		CaCertificatesBundleS3Bucket:        s3Bucket,
		CaCertificatesBundleS3Key:           s3Key,
		CaCertificatesBundleS3ObjectVersion: s3ObjectVersion,
		Revocations:                         []TrustStoreRevocation{},
		Tags:                                t,
	}

	b.trustStores.Put(ts)

	cp := *ts

	return &cp, nil
}

// DescribeTrustStores returns trust stores filtered by ARNs and/or names.
func (b *InMemoryBackend) DescribeTrustStores(arns []string, names []string) ([]TrustStore, error) {
	b.mu.RLock("DescribeTrustStores")
	defer b.mu.RUnlock()

	filterArns := len(arns) > 0
	filterNames := len(names) > 0

	var wantArn map[string]struct{}
	if filterArns {
		wantArn = make(map[string]struct{}, len(arns))
		for _, a := range arns {
			wantArn[a] = struct{}{}
		}
	}

	var wantName map[string]struct{}
	if filterNames {
		wantName = make(map[string]struct{}, len(names))
		for _, n := range names {
			wantName[n] = struct{}{}
		}
	}

	result := make([]TrustStore, 0, b.trustStores.Len())

	for _, ts := range b.trustStores.All() {
		if filterArns {
			if _, ok := wantArn[ts.TrustStoreArn]; !ok {
				continue
			}
		}

		if filterNames {
			if _, ok := wantName[ts.Name]; !ok {
				continue
			}
		}

		result = append(result, *ts)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteTrustStore deletes a trust store by ARN.
func (b *InMemoryBackend) DeleteTrustStore(trustStoreArn string) error {
	b.mu.Lock("DeleteTrustStore")
	defer b.mu.Unlock()

	ts, ok := b.trustStores.Get(trustStoreArn)
	if !ok {
		return ErrTrustStoreNotFound
	}

	ts.Tags.Close()
	b.trustStores.Delete(trustStoreArn)

	return nil
}

// AddTrustStoreRevocations appends revocation entries to a trust store, assigning
// each a monotonically increasing RevocationId (real AWS assigns this server-side;
// see RevocationContentInput doc comment). Returns the newly created revocations so
// the caller can echo them in the AddTrustStoreRevocationsResult response.
func (b *InMemoryBackend) AddTrustStoreRevocations(
	trustStoreArn string,
	contents []RevocationContentInput,
) ([]TrustStoreRevocation, error) {
	b.mu.Lock("AddTrustStoreRevocations")
	defer b.mu.Unlock()

	ts, ok := b.trustStores.Get(trustStoreArn)
	if !ok {
		return nil, ErrTrustStoreNotFound
	}

	added := make([]TrustStoreRevocation, 0, len(contents))

	for _, c := range contents {
		b.revocationIDCounter++
		added = append(added, TrustStoreRevocation{
			RevocationID:   b.revocationIDCounter,
			RevocationType: c.RevocationType,
		})
	}

	ts.Revocations = append(ts.Revocations, added...)

	return added, nil
}

// DescribeTrustStoreAssociations returns listener ARNs whose trust store is set to this ARN.
func (b *InMemoryBackend) DescribeTrustStoreAssociations(trustStoreArn string) ([]string, error) {
	b.mu.RLock("DescribeTrustStoreAssociations")
	defer b.mu.RUnlock()

	if _, ok := b.trustStores.Get(trustStoreArn); !ok {
		return nil, ErrTrustStoreNotFound
	}

	var result []string

	for _, l := range b.listeners.All() {
		if l.MutualAuthentication != nil && l.MutualAuthentication.TrustStoreArn == trustStoreArn {
			result = append(result, l.ListenerArn)
		}
	}

	if result == nil {
		result = []string{}
	}

	return result, nil
}

// DeleteSharedTrustStoreAssociation removes the association between a trust store and a
// resource (listener). The association exists when the listener's MutualAuthentication
// references the trust store; deleting it clears that reference.
func (b *InMemoryBackend) DeleteSharedTrustStoreAssociation(trustStoreArn, resourceArn string) error {
	b.mu.Lock("DeleteSharedTrustStoreAssociation")
	defer b.mu.Unlock()

	if _, ok := b.trustStores.Get(trustStoreArn); !ok {
		return ErrTrustStoreNotFound
	}

	listener, ok := b.listeners.Get(resourceArn)
	if !ok || listener.MutualAuthentication == nil ||
		listener.MutualAuthentication.TrustStoreArn != trustStoreArn {
		return ErrTrustStoreAssociationNotFound
	}

	listener.MutualAuthentication.TrustStoreArn = ""

	return nil
}

// ModifyTrustStore looks up a trust store for ModifyTrustStoreInput, whose only
// real fields are TrustStoreArn and the CA certificates bundle location
// (CaCertificatesBundleS3Bucket/Key/ObjectVersion, both Bucket/Key required on
// the real wire). Bundle content itself stays inert -- see TrustStore's doc
// comment -- but the location is now recorded (gopherstack-hl3h), matching
// CreateTrustStore so the two ops model the same shape consistently.
func (b *InMemoryBackend) ModifyTrustStore(
	trustStoreArn string,
	s3Bucket, s3Key, s3ObjectVersion string,
) (*TrustStore, error) {
	b.mu.Lock("ModifyTrustStore")
	defer b.mu.Unlock()

	ts, ok := b.trustStores.Get(trustStoreArn)
	if !ok {
		return nil, ErrTrustStoreNotFound
	}

	if s3Bucket != "" {
		ts.CaCertificatesBundleS3Bucket = s3Bucket
	}
	if s3Key != "" {
		ts.CaCertificatesBundleS3Key = s3Key
	}
	ts.CaCertificatesBundleS3ObjectVersion = s3ObjectVersion

	cp := *ts

	return &cp, nil
}

// RemoveTrustStoreRevocations removes revocation entries from a trust store by RevocationID.
func (b *InMemoryBackend) RemoveTrustStoreRevocations(
	trustStoreArn string,
	revocationIDs []int64,
) error {
	b.mu.Lock("RemoveTrustStoreRevocations")
	defer b.mu.Unlock()

	ts, ok := b.trustStores.Get(trustStoreArn)
	if !ok {
		return ErrTrustStoreNotFound
	}

	remove := make(map[int64]bool, len(revocationIDs))
	for _, id := range revocationIDs {
		remove[id] = true
	}

	remaining := make([]TrustStoreRevocation, 0, len(ts.Revocations))
	for _, r := range ts.Revocations {
		if !remove[r.RevocationID] {
			remaining = append(remaining, r)
		}
	}

	ts.Revocations = remaining

	return nil
}

// DescribeTrustStoreRevocations returns revocation entries for a trust store.
func (b *InMemoryBackend) DescribeTrustStoreRevocations(
	trustStoreArn string,
) ([]TrustStoreRevocation, error) {
	b.mu.RLock("DescribeTrustStoreRevocations")
	defer b.mu.RUnlock()

	ts, ok := b.trustStores.Get(trustStoreArn)
	if !ok {
		return nil, ErrTrustStoreNotFound
	}

	result := make([]TrustStoreRevocation, len(ts.Revocations))
	copy(result, ts.Revocations)

	return result, nil
}
