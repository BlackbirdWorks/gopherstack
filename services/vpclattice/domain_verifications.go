package vpclattice

import (
	"context"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// resolveDomainVerificationID resolves a domain verification identifier (ID
// or ARN) to an ID.
func (b *InMemoryBackend) resolveDomainVerificationID(identifier string) (string, bool) {
	if b.domainVerifications.Has(identifier) {
		return identifier, true
	}

	for _, d := range b.domainVerifications.All() {
		if d.ARN == identifier {
			return d.ID, true
		}
	}

	return "", false
}

// ------- DomainVerification operations -------

// StartDomainVerification begins ownership verification for a custom
// domain name. Real AWS returns a TXT record for the caller to publish and
// polls public DNS for it; this backend has no DNS to observe, so the
// verification is created and stays PENDING forever -- see
// storedDomainVerification's doc comment.
func (b *InMemoryBackend) StartDomainVerification(
	ctx context.Context,
	domainName string,
	tags map[string]string,
) (*DomainVerification, error) {
	if domainName == "" {
		return nil, ErrInvalidParameter
	}

	b.mu.Lock("StartDomainVerification")
	defer b.mu.Unlock()

	if len(b.domainVerificationsByDomain.Get(domainName)) > 0 {
		return nil, ErrAlreadyExists
	}

	now := time.Now().UTC()
	id := newID(idPrefixDomainVerification)
	region := b.regionFor(ctx)
	dvARN := arn.Build(arnService, region, b.accountID, resourceDomainVerification+"/"+id)

	dv := &storedDomainVerification{
		ARN:        dvARN,
		ID:         id,
		DomainName: domainName,
		Status:     verificationStatusPending,
		Tags:       copyTags(tags),
		CreatedAt:  now,
		Region:     region,
	}

	b.domainVerifications.Put(dv)
	b.tags[dvARN] = copyTags(tags)

	return dv.toDomainVerification(), nil
}

// GetDomainVerification returns a domain verification.
func (b *InMemoryBackend) GetDomainVerification(id string) (*DomainVerification, error) {
	b.mu.RLock("GetDomainVerification")
	defer b.mu.RUnlock()

	dvID, ok := b.resolveDomainVerificationID(id)
	if !ok {
		return nil, ErrNotFound
	}

	dv, _ := b.domainVerifications.Get(dvID)

	return dv.toDomainVerification(), nil
}

// DeleteDomainVerification deletes a domain verification.
func (b *InMemoryBackend) DeleteDomainVerification(id string) error {
	b.mu.Lock("DeleteDomainVerification")
	defer b.mu.Unlock()

	dvID, ok := b.resolveDomainVerificationID(id)
	if !ok {
		return ErrNotFound
	}

	dv, _ := b.domainVerifications.Get(dvID)
	b.domainVerifications.Delete(dvID)
	delete(b.tags, dv.ARN)

	return nil
}

// ListDomainVerifications returns a paginated list of domain verifications.
func (b *InMemoryBackend) ListDomainVerifications(
	ctx context.Context,
	maxResults int32,
	nextToken string,
) ([]*DomainVerificationSummary, string, error) {
	b.mu.RLock("ListDomainVerifications")
	defer b.mu.RUnlock()

	region := b.regionFor(ctx)
	all := make([]*DomainVerificationSummary, 0, b.domainVerifications.Len())

	for _, dv := range b.domainVerifications.All() {
		if dv.Region != region {
			continue
		}

		all = append(all, dv.toSummary())
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	p := page.New(all, nextToken, int(maxResults), defaultMaxResults)

	return p.Data, p.Next, nil
}
