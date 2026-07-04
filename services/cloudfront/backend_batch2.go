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

// ErrDomainConflict is returned when a domain is already associated with another
// distribution tenant or distribution.
var ErrDomainConflict = awserr.New("DomainConflictException", awserr.ErrConflict)

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
	Customizations    map[string]any    `json:"Customizations,omitempty"`
	Parameters        map[string]string `json:"Parameters,omitempty"`
	Tags              map[string]string `json:"Tags,omitempty"`
	Name              string            `json:"Name,omitempty"`
	ARN               string            `json:"Arn"`
	DistributionID    string            `json:"DistributionId"`
	ID                string            `json:"Id"`
	Domain            string            `json:"Domain"`
	ConnectionGroupID string            `json:"ConnectionGroupId,omitempty"`
	Status            string            `json:"Status"`
	CreationTime      string            `json:"CreationTime,omitempty"`
	LastModifiedTime  string            `json:"LastModifiedTime,omitempty"`
	ETag              string            `json:"-"`
	Domains           []string          `json:"Domains,omitempty"`
	Enabled           bool              `json:"Enabled"`
}

// DomainConflict describes an existing resource that already claims a domain.
type DomainConflict struct {
	Domain       string
	ResourceType string // "DISTRIBUTION" | "DISTRIBUTION_TENANT"
	ResourceID   string
	AccountID    string
}

// DNSConfiguration reports the DNS verification status for a single domain.
type DNSConfiguration struct {
	Domain string
	Status string // "PASSED" | "FAILED"
	Reason string
}

// DomainAssociationResult is returned by UpdateDomainAssociation.
type DomainAssociationResult struct {
	Domain               string
	DistributionID       string
	DistributionTenantID string
}

// distributionTenantARN returns the ARN for a distribution tenant ID.
func (b *InMemoryBackend) distributionTenantARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:distribution-tenant/%s", b.accountID, id)
}

// normalizeDomains dedupes and drops empty domain strings while preserving order.
func normalizeDomains(domain string, extra []string) []string {
	seen := make(map[string]bool, len(extra)+1)
	out := make([]string, 0, len(extra)+1)

	add := func(d string) {
		if d == "" || seen[d] {
			return
		}
		seen[d] = true
		out = append(out, d)
	}

	add(domain)
	for _, d := range extra {
		add(d)
	}

	return out
}

// ---------------------------------------------------------------------------
// DistributionTenant backend methods
// ---------------------------------------------------------------------------

// findDomainConflicts returns every existing distribution tenant or distribution that already
// claims domain, excluding the tenant identified by excludeTenantID (used when re-checking a
// tenant's own domains during an update). Must be called with the lock held.
func (b *InMemoryBackend) findDomainConflicts(domain, excludeTenantID string) []DomainConflict {
	var conflicts []DomainConflict

	if tid, ok := b.distributionTenantsByDomain[domain]; ok && tid != excludeTenantID {
		conflicts = append(conflicts, DomainConflict{
			Domain:       domain,
			ResourceType: "DISTRIBUTION_TENANT",
			ResourceID:   tid,
			AccountID:    b.accountID,
		})
	}

	distIDs := make([]string, 0, len(b.distributionAliases))
	for distID := range b.distributionAliases {
		distIDs = append(distIDs, distID)
	}
	sort.Strings(distIDs)

	for _, distID := range distIDs {
		if slices.Contains(b.distributionAliases[distID], domain) {
			conflicts = append(conflicts, DomainConflict{
				Domain:       domain,
				ResourceType: "DISTRIBUTION",
				ResourceID:   distID,
				AccountID:    b.accountID,
			})
		}
	}

	return conflicts
}

// CreateDistributionTenant creates a new distribution tenant.
func (b *InMemoryBackend) CreateDistributionTenant(
	distributionID, name string,
	domains []string,
	tags map[string]string,
) (*DistributionTenant, error) {
	b.mu.Lock("CreateDistributionTenant")
	defer b.mu.Unlock()

	if distributionID == "" {
		return nil, fmt.Errorf("%w: DistributionId must not be empty", ErrValidation)
	}

	domains = normalizeDomains("", domains)
	if len(domains) == 0 {
		return nil, fmt.Errorf("%w: Domain must not be empty", ErrValidation)
	}

	for _, d := range domains {
		if conflicts := b.findDomainConflicts(d, ""); len(conflicts) > 0 {
			return nil, fmt.Errorf(
				"%w: domain %q is already associated with %s %s",
				ErrDomainConflict, d, strings.ToLower(conflicts[0].ResourceType), conflicts[0].ResourceID,
			)
		}
	}

	id := uuid.NewString()[:12]
	now := time.Now().UTC().Format(time.RFC3339)
	t := &DistributionTenant{
		ID:               id,
		ARN:              b.distributionTenantARN(id),
		DistributionID:   distributionID,
		Name:             name,
		Domain:           domains[0],
		Domains:          domains,
		Enabled:          true,
		Status:           statusDeployed,
		CreationTime:     now,
		LastModifiedTime: now,
		ETag:             uuid.NewString(),
		Tags:             tags,
	}
	if t.Tags == nil {
		t.Tags = make(map[string]string)
	}
	b.distributionTenants[id] = t
	b.distributionTenantARNs[t.ARN] = id
	for _, d := range domains {
		b.distributionTenantsByDomain[d] = id
	}
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

// DistributionTenantUpdate carries the mutable fields accepted by UpdateDistributionTenant.
// A zero-value field is left unchanged; Domains and Enabled require an explicit non-empty /
// non-nil value to take effect.
type DistributionTenantUpdate struct {
	Customizations    map[string]any
	Enabled           *bool
	ConnectionGroupID string
	Domains           []string
}

// UpdateDistributionTenant updates a distribution tenant's domains, connection group,
// enabled state, and/or customizations. Domain changes are validated against
// findDomainConflicts before being applied.
func (b *InMemoryBackend) UpdateDistributionTenant(
	id string,
	upd DistributionTenantUpdate,
) (*DistributionTenant, error) {
	b.mu.Lock("UpdateDistributionTenant")
	defer b.mu.Unlock()

	t, ok := b.distributionTenants[id]
	if !ok {
		return nil, fmt.Errorf("%w: tenant %s not found", ErrDistributionTenantNotFound, id)
	}

	if len(upd.Domains) > 0 {
		domains := normalizeDomains("", upd.Domains)
		for _, d := range domains {
			if conflicts := b.findDomainConflicts(d, id); len(conflicts) > 0 {
				return nil, fmt.Errorf(
					"%w: domain %q is already associated with %s %s",
					ErrDomainConflict, d, strings.ToLower(conflicts[0].ResourceType), conflicts[0].ResourceID,
				)
			}
		}

		for _, d := range t.Domains {
			delete(b.distributionTenantsByDomain, d)
		}
		for _, d := range domains {
			b.distributionTenantsByDomain[d] = id
		}
		t.Domains = domains
		t.Domain = domains[0]
	}

	if upd.ConnectionGroupID != "" {
		t.ConnectionGroupID = upd.ConnectionGroupID
	}

	if upd.Enabled != nil {
		t.Enabled = *upd.Enabled
	}

	if upd.Customizations != nil {
		t.Customizations = upd.Customizations
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

	for _, d := range t.Domains {
		delete(b.distributionTenantsByDomain, d)
	}
	delete(b.distributionTenantsByDomain, t.Domain)
	delete(b.distributionTenantARNs, t.ARN)
	delete(b.distributionTenantWebACLs, id)
	delete(b.distributionTenants, id)
	delete(b.tenantInvalidations, id)
	delete(b.tenantInvalidationReadyAt, id)

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

// TenantWebACLArn returns the WAF web ACL ARN currently associated with a distribution tenant,
// or "" if none is associated.
func (b *InMemoryBackend) TenantWebACLArn(tenantID string) string {
	b.mu.RLock("TenantWebACLArn")
	defer b.mu.RUnlock()

	return b.distributionTenantWebACLs[tenantID]
}

// ListDistributionTenantsByCustomization returns distribution tenants filtered by an associated
// WAF web ACL ARN. When webACLArn is empty, all tenants are returned (same as
// ListDistributionTenants), since no customization filter was supplied.
func (b *InMemoryBackend) ListDistributionTenantsByCustomization(webACLArn string) []*DistributionTenant {
	b.mu.RLock("ListDistributionTenantsByCustomization")
	defer b.mu.RUnlock()

	out := make([]*DistributionTenant, 0, len(b.distributionTenants))
	for _, t := range b.distributionTenants {
		if webACLArn != "" && b.distributionTenantWebACLs[t.ID] != webACLArn {
			continue
		}
		out = append(out, b.copyTenant(t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// ListDomainConflicts returns every existing distribution tenant or distribution that already
// claims domain.
func (b *InMemoryBackend) ListDomainConflicts(domain string) []DomainConflict {
	b.mu.RLock("ListDomainConflicts")
	defer b.mu.RUnlock()

	return b.findDomainConflicts(domain, "")
}

// UpdateDomainAssociation moves a domain's association to the given target distribution tenant
// or distribution. Exactly one of targetTenantID / targetDistID must be set. The domain is
// removed from its previous owner (if any) and attached to the target; a conflict with a
// *different* existing owner returns ErrDomainConflict.
func (b *InMemoryBackend) UpdateDomainAssociation(
	domain, targetTenantID, targetDistID string,
) (*DomainAssociationResult, error) {
	b.mu.Lock("UpdateDomainAssociation")
	defer b.mu.Unlock()

	if domain == "" {
		return nil, fmt.Errorf("%w: Domain must not be empty", ErrValidation)
	}

	if (targetTenantID == "") == (targetDistID == "") {
		return nil, fmt.Errorf("%w: exactly one of DistributionTenantId or DistributionId must be set", ErrValidation)
	}

	if targetTenantID != "" {
		return b.updateDomainAssociationToTenant(domain, targetTenantID)
	}

	return b.updateDomainAssociationToDistribution(domain, targetDistID)
}

// updateDomainAssociationToTenant reassigns domain to targetTenantID. Must be called with the
// lock held.
func (b *InMemoryBackend) updateDomainAssociationToTenant(
	domain, targetTenantID string,
) (*DomainAssociationResult, error) {
	target, ok := b.distributionTenants[targetTenantID]
	if !ok {
		return nil, fmt.Errorf("%w: tenant %s not found", ErrDistributionTenantNotFound, targetTenantID)
	}

	if conflicts := b.findDomainConflicts(domain, targetTenantID); len(conflicts) > 0 {
		return nil, fmt.Errorf(
			"%w: domain %q is already associated with %s %s",
			ErrDomainConflict, domain, strings.ToLower(conflicts[0].ResourceType), conflicts[0].ResourceID,
		)
	}

	target.Domains = normalizeDomains(domain, target.Domains)
	if target.Domain == "" {
		target.Domain = domain
	}
	target.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)
	target.ETag = uuid.NewString()
	b.distributionTenantsByDomain[domain] = targetTenantID

	return &DomainAssociationResult{Domain: domain, DistributionTenantID: targetTenantID}, nil
}

// updateDomainAssociationToDistribution reassigns domain to targetDistID's alias list. Must be
// called with the lock held.
func (b *InMemoryBackend) updateDomainAssociationToDistribution(
	domain, targetDistID string,
) (*DomainAssociationResult, error) {
	d, ok := b.distributions[targetDistID]
	if !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, targetDistID)
	}

	if conflicts := b.findDomainConflicts(domain, ""); len(conflicts) > 0 {
		for _, c := range conflicts {
			if c.ResourceType != "DISTRIBUTION" || c.ResourceID != targetDistID {
				return nil, fmt.Errorf(
					"%w: domain %q is already associated with %s %s",
					ErrDomainConflict, domain, strings.ToLower(c.ResourceType), c.ResourceID,
				)
			}
		}
	}

	if !slices.Contains(b.distributionAliases[targetDistID], domain) {
		b.distributionAliases[targetDistID] = append(b.distributionAliases[targetDistID], domain)
	}
	d.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)
	d.ETag = uuid.NewString()

	return &DomainAssociationResult{Domain: domain, DistributionID: targetDistID}, nil
}

// VerifyDNSConfiguration checks the DNS status of every domain associated with identifier, which
// may be a distribution tenant ID or a distribution ID. A domain is reported PASSED when it is
// syntactically well-formed (contains a dot and no whitespace) and FAILED otherwise. When
// identifier is empty, a single generic PASSED entry is returned.
func (b *InMemoryBackend) VerifyDNSConfiguration(identifier string) ([]DNSConfiguration, error) {
	b.mu.RLock("VerifyDNSConfiguration")
	defer b.mu.RUnlock()

	if identifier == "" {
		return []DNSConfiguration{{Domain: "", Status: "PASSED"}}, nil
	}

	var domains []string
	switch {
	case b.distributionTenants[identifier] != nil:
		domains = b.distributionTenants[identifier].Domains
	case b.distributions[identifier] != nil:
		domains = b.distributionAliases[identifier]
	default:
		return nil, fmt.Errorf("%w: identifier %s not found", ErrDistributionTenantNotFound, identifier)
	}

	out := make([]DNSConfiguration, 0, len(domains))
	for _, d := range domains {
		out = append(out, DNSConfiguration{Domain: d, Status: dnsCheckStatus(d)})
	}

	return out, nil
}

// dnsCheckStatus performs a syntactic sanity check on a domain, standing in for real DNS
// resolution in this emulator.
func dnsCheckStatus(domain string) string {
	if domain == "" || strings.ContainsAny(domain, " \t") || !strings.Contains(domain, ".") {
		return "FAILED"
	}

	return "PASSED"
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

	if t.Domains != nil {
		cp.Domains = make([]string, len(t.Domains))
		copy(cp.Domains, t.Domains)
	}

	if t.Parameters != nil {
		cp.Parameters = make(map[string]string, len(t.Parameters))
		maps.Copy(cp.Parameters, t.Parameters)
	}

	return &cp
}
