package cloudfront

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// AssociateDistributionTenantWebACL associates a WAF web ACL with a distribution tenant.
func (b *InMemoryBackend) AssociateDistributionTenantWebACL(tenantID, webACLID string) error {
	b.mu.Lock("AssociateDistributionTenantWebACL")
	defer b.mu.Unlock()

	if tenantID == "" {
		return fmt.Errorf("%w: tenantId must not be empty", ErrValidation)
	}

	b.distributionTenantWebACLs[tenantID] = webACLID

	return nil
}

// dnsStatusPassed and dnsStatusFailed are the two outcomes of the syntactic domain-validation
// check dnsCheckStatus performs, standing in for real DNS/ACM validation in this emulator.
const (
	dnsStatusPassed = "PASSED"
	dnsStatusFailed = "FAILED"
)

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
// claims domain, excluding the tenant identified by excludeTenantID and the distribution
// identified by excludeDistID (used when re-checking a resource's own domains during an update,
// and by ListDomainConflicts to scope out the resource whose certificate validates the domain).
// Must be called with the lock held.
func (b *InMemoryBackend) findDomainConflicts(domain, excludeTenantID, excludeDistID string) []DomainConflict {
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
		if distID == excludeDistID {
			continue
		}
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
		if conflicts := b.findDomainConflicts(d, "", ""); len(conflicts) > 0 {
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
	b.distributionTenants.Put(t)
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

	t, ok := b.distributionTenants.Get(id)
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
	t, ok := b.distributionTenants.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: tenant %s not found", ErrDistributionTenantNotFound, id)
	}

	return b.copyTenant(t), nil
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

	t, ok := b.distributionTenants.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: tenant %s not found", ErrDistributionTenantNotFound, id)
	}

	if len(upd.Domains) > 0 {
		domains := normalizeDomains("", upd.Domains)
		for _, d := range domains {
			if conflicts := b.findDomainConflicts(d, id, ""); len(conflicts) > 0 {
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

	t, ok := b.distributionTenants.Get(id)
	if !ok {
		return fmt.Errorf("%w: tenant %s not found", ErrDistributionTenantNotFound, id)
	}

	for _, d := range t.Domains {
		delete(b.distributionTenantsByDomain, d)
	}
	delete(b.distributionTenantsByDomain, t.Domain)
	delete(b.distributionTenantARNs, t.ARN)
	delete(b.distributionTenantWebACLs, id)
	b.distributionTenants.Delete(id)
	b.deleteInvalidationsForTenant(id)
	delete(b.tenantInvalidationReadyAt, id)

	return nil
}

// ListDistributionTenants returns all distribution tenants sorted by ID.
func (b *InMemoryBackend) ListDistributionTenants() []*DistributionTenant {
	b.mu.RLock("ListDistributionTenants")
	defer b.mu.RUnlock()

	out := make([]*DistributionTenant, 0, b.distributionTenants.Len())
	for _, t := range b.distributionTenants.All() {
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

// TenantCertificateArn returns the ARN of the certificate a distribution tenant uses, or "" if
// the tenant does not exist. Real AWS lets a tenant use either a customer-supplied ACM
// certificate (Customizations.Certificate.Arn) or CloudFront's own managed certificate;
// CreateDistributionTenant/UpdateDistributionTenant don't model the former here, so this always
// reports the deterministic managed-certificate ARN every tenant has.
func (b *InMemoryBackend) TenantCertificateArn(tenantID string) string {
	b.mu.RLock("TenantCertificateArn")
	defer b.mu.RUnlock()

	if _, ok := b.distributionTenants.Get(tenantID); !ok {
		return ""
	}

	return b.managedCertificateARN(tenantID)
}

// ListDistributionTenantsByCustomization returns distribution tenants filtered by an associated
// WAF web ACL ARN. When webACLArn is empty, all tenants are returned (same as
// ListDistributionTenants), since no customization filter was supplied.
func (b *InMemoryBackend) ListDistributionTenantsByCustomization(webACLArn string) []*DistributionTenant {
	b.mu.RLock("ListDistributionTenantsByCustomization")
	defer b.mu.RUnlock()

	out := make([]*DistributionTenant, 0, b.distributionTenants.Len())
	for _, t := range b.distributionTenants.All() {
		if webACLArn != "" && b.distributionTenantWebACLs[t.ID] != webACLArn {
			continue
		}
		out = append(out, b.copyTenant(t))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// ListDomainConflicts returns every existing distribution tenant or distribution that already
// claims domain, excluding the resource identified by distributionID/distributionTenantID -- the
// resource whose certificate is being used to validate control of the domain. Real AWS scopes
// the check to that resource (excludes it from its own conflict list); the caller must ensure
// exactly one of distributionID/distributionTenantID is set.
func (b *InMemoryBackend) ListDomainConflicts(
	domain, distributionID, distributionTenantID string,
) ([]DomainConflict, error) {
	b.mu.RLock("ListDomainConflicts")
	defer b.mu.RUnlock()

	if distributionTenantID != "" {
		if _, ok := b.distributionTenants.Get(distributionTenantID); !ok {
			return nil, fmt.Errorf(
				"%w: distribution tenant %s not found",
				ErrDomainControlValidationResourceNotFound, distributionTenantID,
			)
		}

		return b.findDomainConflicts(domain, distributionTenantID, ""), nil
	}

	if _, ok := b.distributions.Get(distributionID); !ok {
		return nil, fmt.Errorf(
			"%w: distribution %s not found", ErrDomainControlValidationResourceNotFound, distributionID,
		)
	}

	return b.findDomainConflicts(domain, "", distributionID), nil
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
	target, ok := b.distributionTenants.Get(targetTenantID)
	if !ok {
		return nil, fmt.Errorf("%w: tenant %s not found", ErrDistributionTenantNotFound, targetTenantID)
	}

	if conflicts := b.findDomainConflicts(domain, targetTenantID, ""); len(conflicts) > 0 {
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
	d, ok := b.distributions.Get(targetDistID)
	if !ok {
		return nil, fmt.Errorf("%w: distribution %s not found", ErrNotFound, targetDistID)
	}

	if conflicts := b.findDomainConflicts(domain, "", ""); len(conflicts) > 0 {
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
		return []DNSConfiguration{{Domain: "", Status: dnsStatusPassed}}, nil
	}

	var domains []string

	if t, ok := b.distributionTenants.Get(identifier); ok {
		domains = t.Domains
	} else if _, distOK := b.distributions.Get(identifier); distOK {
		domains = b.distributionAliases[identifier]
	} else {
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
		return dnsStatusFailed
	}

	return dnsStatusPassed
}

// DisassociateDistributionTenantWebACL clears the web ACL association for a distribution tenant.
func (b *InMemoryBackend) DisassociateDistributionTenantWebACL(tenantID string) error {
	b.mu.Lock("DisassociateDistributionTenantWebACL")
	defer b.mu.Unlock()

	delete(b.distributionTenantWebACLs, tenantID)

	return nil
}

// CreateInvalidationForTenant creates an invalidation for a distribution tenant.
func (b *InMemoryBackend) CreateInvalidationForTenant(tenantID string, paths []string) (*Invalidation, error) {
	b.mu.Lock("CreateInvalidationForTenant")
	defer b.mu.Unlock()

	if _, ok := b.distributionTenants.Get(tenantID); !ok {
		return nil, fmt.Errorf("%w: tenant %s not found", ErrDistributionTenantNotFound, tenantID)
	}

	const tenantInvalidationDelay = 100 * time.Millisecond

	now := time.Now().UTC()
	inv := &Invalidation{
		ID:         uuid.NewString()[:12],
		Status:     statusInProgress,
		CreateTime: now,
		Paths:      paths,
		tenantID:   tenantID,
	}
	b.tenantInvalidations.Put(inv)

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

	invs := b.tenantInvalidationsByTenant.Get(tenantID)
	if len(invs) == 0 {
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

	invs := b.tenantInvalidationsByTenant.Get(tenantID)
	out := make([]*Invalidation, 0, len(invs))
	for _, inv := range invs {
		cp := *inv
		out = append(out, &cp)
	}

	return out
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

// managedCertificateARN builds a deterministic ACM-style ARN for a distribution tenant's
// CloudFront-managed certificate, stable across repeated Get calls for the same tenant.
func (b *InMemoryBackend) managedCertificateARN(tenantID string) string {
	sum := sha256.Sum256([]byte("managed-cert-" + tenantID))

	return fmt.Sprintf("arn:aws:acm:us-east-1:%s:certificate/%s", b.accountID, hex.EncodeToString(sum[:16]))
}

// GetManagedCertificateDetails returns the CloudFront-managed certificate details for a
// distribution tenant, deriving validation state from the tenant's real domains. The result is
// cached per tenant so repeated calls return a stable certificate ARN and validation tokens,
// mirroring a certificate that, once issued, does not change on every read.
func (b *InMemoryBackend) GetManagedCertificateDetails(tenantID string) (*ManagedCertificateDetails, error) {
	b.mu.Lock("GetManagedCertificateDetails")
	defer b.mu.Unlock()

	tenant, ok := b.distributionTenants.Get(tenantID)
	if !ok {
		return nil, fmt.Errorf("%w: distribution tenant %s not found", ErrDistributionTenantNotFound, tenantID)
	}

	if cached, cachedOK := b.managedCertificates[tenantID]; cachedOK {
		cp := *cached
		cp.ValidationTokenDetails = append([]ValidationTokenDetail(nil), cached.ValidationTokenDetails...)

		return &cp, nil
	}

	domains := tenant.Domains
	if len(domains) == 0 && tenant.Domain != "" {
		domains = []string{tenant.Domain}
	}

	// CertificateStatus mirrors ACM's own vocabulary: "SUCCESS" once every domain has passed DNS
	// validation, "PENDING_VALIDATION" if any domain has not (there is always at least one
	// domain, since GetManagedCertificateDetails 404s for tenants with none).
	status := "SUCCESS"
	tokens := make([]ValidationTokenDetail, 0, len(domains))
	for _, domain := range domains {
		if dnsCheckStatus(domain) != dnsStatusPassed {
			status = "PENDING_VALIDATION"
		}
		sum := sha256.Sum256([]byte("validation-" + domain))
		tokens = append(tokens, ValidationTokenDetail{
			Domain:       domain,
			RedirectFrom: fmt.Sprintf("_%s.%s.", hex.EncodeToString(sum[:8]), domain),
			RedirectTo:   fmt.Sprintf("_%s.acm-validations.aws.", hex.EncodeToString(sum[8:16])),
		})
	}

	details := &ManagedCertificateDetails{
		CertificateARN:         b.managedCertificateARN(tenantID),
		CertificateStatus:      status,
		ValidationTokenHost:    "cloudfront",
		ValidationTokenDetails: tokens,
	}
	b.managedCertificates[tenantID] = details

	cp := *details
	cp.ValidationTokenDetails = append([]ValidationTokenDetail(nil), details.ValidationTokenDetails...)

	return &cp, nil
}

// ---------------------------------------------------------------------------
// ListDistributionsBy* helpers
// ---------------------------------------------------------------------------
