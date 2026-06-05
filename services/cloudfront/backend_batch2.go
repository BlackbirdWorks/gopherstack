package cloudfront

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ---------------------------------------------------------------------------
// Error sentinels
// ---------------------------------------------------------------------------

// ErrDistributionTenantNotFound is returned when a distribution tenant does not exist.
var ErrDistributionTenantNotFound = awserr.New("NoSuchDistributionTenant", awserr.ErrNotFound)

// ErrInvalidTagging is returned when tag key/value constraints are violated.
var ErrInvalidTagging = awserr.New("InvalidTagging", awserr.ErrInvalidParameter)

const (
	maxTagKeyLen   = 128
	maxTagValueLen = 256
	maxTagCount    = 50
)

// validateCFTags enforces CloudFront tag constraints: key 1-128 chars, value 0-256 chars,
// no "aws:" prefix on keys, max 50 tags total.
func validateCFTags(tags map[string]string) error {
	if len(tags) > maxTagCount {
		return fmt.Errorf("%w: cannot have more than %d tags per resource", ErrInvalidTagging, maxTagCount)
	}

	for k, v := range tags {
		if k == "" || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be between 1 and %d characters", ErrInvalidTagging, maxTagKeyLen)
		}

		if strings.HasPrefix(k, "aws:") {
			return fmt.Errorf("%w: tag key must not start with \"aws:\"", ErrInvalidTagging)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value must be at most %d characters", ErrInvalidTagging, maxTagValueLen)
		}
	}

	return nil
}

// ---------------------------------------------------------------------------
// DistributionTenant type
// ---------------------------------------------------------------------------

// DistributionTenant represents a CloudFront distribution tenant.
type DistributionTenant struct {
	Customizations   map[string]any    `json:"Customizations,omitempty"`
	Tags             map[string]string `json:"Tags,omitempty"`
	ID               string            `json:"Id"`
	DistributionID   string            `json:"DistributionId"`
	Domain           string            `json:"Domain"`
	Status           string            `json:"Status"`
	CreationTime     string            `json:"CreationTime,omitempty"`
	LastModifiedTime string            `json:"LastModifiedTime,omitempty"`
	ETag             string            `json:"-"`
}

// ---------------------------------------------------------------------------
// DistributionTenant backend methods
// ---------------------------------------------------------------------------

// CreateDistributionTenant creates a new distribution tenant.
func (b *InMemoryBackend) CreateDistributionTenant(
	distributionID, domain string,
	tags map[string]string,
) (*DistributionTenant, error) {
	b.mu.Lock("CreateDistributionTenant")
	defer b.mu.Unlock()

	if distributionID == "" {
		return nil, fmt.Errorf("%w: DistributionId must not be empty", ErrValidation)
	}

	if domain == "" {
		return nil, fmt.Errorf("%w: Domain must not be empty", ErrValidation)
	}

	if _, exists := b.distributionTenantsByDomain[domain]; exists {
		return nil, fmt.Errorf("%w: distribution tenant with domain %q already exists", ErrAlreadyExists, domain)
	}

	id := uuid.NewString()[:12]
	now := time.Now().UTC().Format(time.RFC3339)
	t := &DistributionTenant{
		ID:               id,
		DistributionID:   distributionID,
		Domain:           domain,
		Status:           "Enabled",
		CreationTime:     now,
		LastModifiedTime: now,
		ETag:             uuid.NewString(),
		Tags:             tags,
	}
	if t.Tags == nil {
		t.Tags = make(map[string]string)
	}
	b.distributionTenants[id] = t
	b.distributionTenantsByDomain[domain] = id
	cp := b.copyTenant(t)

	return cp, nil
}

// GetDistributionTenant returns a distribution tenant by ID.
func (b *InMemoryBackend) GetDistributionTenant(id string) (*DistributionTenant, error) {
	b.mu.RLock("GetDistributionTenant")
	defer b.mu.RUnlock()

	t, ok := b.distributionTenants[id]
	if !ok {
		return nil, fmt.Errorf("%w: tenant %s not found", ErrDistributionTenantNotFound, id)
	}

	return b.copyTenant(t), nil
}

// GetDistributionTenantByDomain returns a distribution tenant by domain.
func (b *InMemoryBackend) GetDistributionTenantByDomain(domain string) (*DistributionTenant, error) {
	b.mu.RLock("GetDistributionTenantByDomain")
	defer b.mu.RUnlock()

	id, ok := b.distributionTenantsByDomain[domain]
	if !ok {
		return nil, fmt.Errorf("%w: tenant with domain %s not found", ErrDistributionTenantNotFound, domain)
	}
	t, ok := b.distributionTenants[id]
	if !ok {
		return nil, fmt.Errorf("%w: tenant %s not found", ErrDistributionTenantNotFound, id)
	}

	return b.copyTenant(t), nil
}

// UpdateDistributionTenant updates a distribution tenant's customizations.
func (b *InMemoryBackend) UpdateDistributionTenant(
	id string,
	customizations map[string]any,
) (*DistributionTenant, error) {
	b.mu.Lock("UpdateDistributionTenant")
	defer b.mu.Unlock()

	t, ok := b.distributionTenants[id]
	if !ok {
		return nil, fmt.Errorf("%w: tenant %s not found", ErrDistributionTenantNotFound, id)
	}

	if customizations != nil {
		t.Customizations = customizations
	}
	t.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)
	t.ETag = uuid.NewString()

	return b.copyTenant(t), nil
}

// DeleteDistributionTenant deletes a distribution tenant by ID.
func (b *InMemoryBackend) DeleteDistributionTenant(id string) error {
	b.mu.Lock("DeleteDistributionTenant")
	defer b.mu.Unlock()

	t, ok := b.distributionTenants[id]
	if !ok {
		return fmt.Errorf("%w: tenant %s not found", ErrDistributionTenantNotFound, id)
	}

	delete(b.distributionTenantsByDomain, t.Domain)
	delete(b.distributionTenants, id)
	delete(b.tenantInvalidations, id)

	return nil
}

// ListDistributionTenants returns all distribution tenants sorted by ID.
func (b *InMemoryBackend) ListDistributionTenants() []*DistributionTenant {
	b.mu.RLock("ListDistributionTenants")
	defer b.mu.RUnlock()

	out := make([]*DistributionTenant, 0, len(b.distributionTenants))
	for _, t := range b.distributionTenants {
		out = append(out, b.copyTenant(t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// DisassociateDistributionTenantWebACL clears the web ACL association for a distribution tenant.
func (b *InMemoryBackend) DisassociateDistributionTenantWebACL(tenantID string) error {
	b.mu.Lock("DisassociateDistributionTenantWebACL")
	defer b.mu.Unlock()

	delete(b.distributionTenantWebACLs, tenantID)

	return nil
}

// DisassociateDistributionWebACL clears the web ACL association for a distribution.
func (b *InMemoryBackend) DisassociateDistributionWebACL(distID string) error {
	b.mu.Lock("DisassociateDistributionWebACL")
	defer b.mu.Unlock()

	if _, ok := b.distributions[distID]; !ok {
		return fmt.Errorf("%w: distribution %s not found", ErrNotFound, distID)
	}
	delete(b.distributionWebACLs, distID)

	return nil
}

// CreateInvalidationForTenant creates an invalidation for a distribution tenant.
func (b *InMemoryBackend) CreateInvalidationForTenant(tenantID string, paths []string) (*Invalidation, error) {
	b.mu.Lock("CreateInvalidationForTenant")
	defer b.mu.Unlock()

	if _, ok := b.distributionTenants[tenantID]; !ok {
		return nil, fmt.Errorf("%w: tenant %s not found", ErrDistributionTenantNotFound, tenantID)
	}

	const tenantInvalidationDelay = 100 * time.Millisecond

	now := time.Now().UTC()
	inv := &Invalidation{
		ID:         uuid.NewString()[:12],
		Status:     statusInProgress,
		CreateTime: now,
		Paths:      paths,
	}
	b.tenantInvalidations[tenantID] = append(b.tenantInvalidations[tenantID], inv)

	if b.tenantInvalidationReadyAt[tenantID] == nil {
		b.tenantInvalidationReadyAt[tenantID] = make(map[string]time.Time)
	}

	b.tenantInvalidationReadyAt[tenantID][inv.ID] = now.Add(tenantInvalidationDelay)

	cp := *inv

	return &cp, nil
}

// GetInvalidationForTenant returns a specific invalidation for a distribution tenant.
func (b *InMemoryBackend) GetInvalidationForTenant(tenantID, invalidationID string) (*Invalidation, error) {
	b.mu.RLock("GetInvalidationForTenant")
	defer b.mu.RUnlock()

	invs, ok := b.tenantInvalidations[tenantID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: invalidation %s not found for tenant %s",
			ErrInvalidationNotFound,
			invalidationID,
			tenantID,
		)
	}
	for _, inv := range invs {
		if inv.ID == invalidationID {
			cp := *inv

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: invalidation %s not found", ErrInvalidationNotFound, invalidationID)
}

// ListInvalidationsForTenant returns all invalidations for a distribution tenant.
func (b *InMemoryBackend) ListInvalidationsForTenant(tenantID string) []*Invalidation {
	b.mu.RLock("ListInvalidationsForTenant")
	defer b.mu.RUnlock()

	invs := b.tenantInvalidations[tenantID]
	out := make([]*Invalidation, 0, len(invs))
	for _, inv := range invs {
		cp := *inv
		out = append(out, &cp)
	}

	return out
}

// UpdateDistributionWithStagingConfig copies the staging distribution's config to the primary.
func (b *InMemoryBackend) UpdateDistributionWithStagingConfig(primaryID, stagingID string) (*Distribution, error) {
	b.mu.Lock("UpdateDistributionWithStagingConfig")
	defer b.mu.Unlock()

	primary, ok := b.distributions[primaryID]
	if !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, primaryID)
	}

	staging, ok := b.distributions[stagingID]
	if !ok {
		return nil, fmt.Errorf("%w: staging distribution %s not found", ErrNotFound, stagingID)
	}

	rawCopy := make([]byte, len(staging.RawConfig))
	copy(rawCopy, staging.RawConfig)
	primary.RawConfig = rawCopy
	primary.ETag = uuid.NewString()
	primary.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)

	return b.copyDistribution(primary), nil
}

// distributionsByConfigSearch scans all distributions and returns those whose raw config contains searchStr.
// Must be called with the read lock held.
func (b *InMemoryBackend) distributionsByConfigSearch(searchStr string) []*Distribution {
	var out []*Distribution
	for _, d := range b.distributions {
		if strings.Contains(string(d.RawConfig), searchStr) {
			cp := *d
			out = append(out, &cp)
		}
	}

	return out
}

// ListDistributionsByKeyGroup returns distributions that reference a key group.
func (b *InMemoryBackend) ListDistributionsByKeyGroup(keyGroupID string) []*Distribution {
	b.mu.RLock("ListDistributionsByKeyGroup")
	defer b.mu.RUnlock()

	return b.distributionsByConfigSearch(keyGroupID)
}

// ListDistributionsByVpcOriginID returns distributions that reference a VPC origin.
func (b *InMemoryBackend) ListDistributionsByVpcOriginID(vpcOriginID string) []*Distribution {
	b.mu.RLock("ListDistributionsByVpcOriginID")
	defer b.mu.RUnlock()

	return b.distributionsByConfigSearch(vpcOriginID)
}

// ListDistributionsByAnycastIPListID returns distributions that reference an anycast IP list.
func (b *InMemoryBackend) ListDistributionsByAnycastIPListID(anycastID string) []*Distribution {
	b.mu.RLock("ListDistributionsByAnycastIPListID")
	defer b.mu.RUnlock()

	return b.distributionsByConfigSearch(anycastID)
}

// ListDistributionsByConnectionFunction returns distributions that reference a connection function.
func (b *InMemoryBackend) ListDistributionsByConnectionFunction(funcID string) []*Distribution {
	b.mu.RLock("ListDistributionsByConnectionFunction")
	defer b.mu.RUnlock()

	return b.distributionsByConfigSearch(funcID)
}

// ListDistributionsByConnectionMode returns distributions that match a connection mode.
func (b *InMemoryBackend) ListDistributionsByConnectionMode(mode string) []*Distribution {
	b.mu.RLock("ListDistributionsByConnectionMode")
	defer b.mu.RUnlock()

	return b.distributionsByConfigSearch(mode)
}

// ListDistributionsByTrustStore returns distributions that reference a trust store.
func (b *InMemoryBackend) ListDistributionsByTrustStore(trustStoreID string) []*Distribution {
	b.mu.RLock("ListDistributionsByTrustStore")
	defer b.mu.RUnlock()

	return b.distributionsByConfigSearch(trustStoreID)
}

// ListDistributionsByOwnedResource returns distributions that reference an owned resource ARN.
func (b *InMemoryBackend) ListDistributionsByOwnedResource(resourceARN string) []*Distribution {
	b.mu.RLock("ListDistributionsByOwnedResource")
	defer b.mu.RUnlock()

	return b.distributionsByConfigSearch(resourceARN)
}

// ListConflictingAliasesByDomain returns distributions that have a conflicting CNAME alias.
func (b *InMemoryBackend) ListConflictingAliasesByDomain(domain string) []*Distribution {
	b.mu.RLock("ListConflictingAliasesByDomain")
	defer b.mu.RUnlock()

	var out []*Distribution
	for distID, aliases := range b.distributionAliases {
		if slices.Contains(aliases, domain) {
			if d, ok := b.distributions[distID]; ok {
				cp := *d
				out = append(out, &cp)
			}
		}
	}

	return out
}

// UpdateKeyValueStore updates a Key Value Store's comment.
func (b *InMemoryBackend) UpdateKeyValueStore(id, comment string) (*KeyValueStore, error) {
	b.mu.Lock("UpdateKeyValueStore")
	defer b.mu.Unlock()

	kvs, ok := b.keyValueStores[id]
	if !ok {
		return nil, fmt.Errorf("%w: key value store %s not found", ErrKeyValueStoreNotFound, id)
	}
	if comment != "" {
		kvs.Comment = comment
	}
	kvs.ETag = uuid.NewString()
	cp := *kvs

	return &cp, nil
}

// copyTenant returns a deep copy of a DistributionTenant.
// Must be called with the lock held.
func (b *InMemoryBackend) copyTenant(t *DistributionTenant) *DistributionTenant {
	cp := *t
	if t.Tags != nil {
		cp.Tags = make(map[string]string, len(t.Tags))
		maps.Copy(cp.Tags, t.Tags)
	}

	return &cp
}
