package workmail

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"
)

// --- Organizations ---

// CreateOrganization creates a new WorkMail organization.
func (b *InMemoryBackend) CreateOrganization(
	ctx context.Context,
	alias string,
	domains []string,
) (*Organization, error) {
	region := b.regionFor(ctx)

	b.mu.Lock("CreateOrganization")
	defer b.mu.Unlock()

	if alias == "" {
		return nil, fmt.Errorf("%w: Alias is required", ErrValidation)
	}
	if _, exists := b.orgsByAlias[alias]; exists {
		return nil, fmt.Errorf("%w: organization with alias %q already exists", ErrConflict, alias)
	}

	orgID := "m-" + strings.ReplaceAll(newID(), "-", "")[:20]
	defaultDomain := alias + ".awsapps.com"
	now := time.Now().UTC()

	org := &Organization{
		CreatedAt:         now,
		CompletedDate:     now,
		OrgID:             orgID,
		Alias:             alias,
		ARN:               b.orgARN(orgID, region),
		State:             stateActive,
		DirectoryID:       "d-" + strings.ReplaceAll(newID(), "-", "")[:10],
		DirectoryType:     "SimpleAD",
		DefaultMailDomain: defaultDomain,
		Region:            region,
	}

	b.organizations.Put(org)
	b.orgsByAlias[alias] = orgID
	b.initOrgMaps(orgID)

	// Register default domain.
	b.mailDomains.Put(&MailDomain{
		DomainName:                  defaultDomain,
		IsDefault:                   true,
		IsTestDomain:                true,
		OwnershipVerificationStatus: dnsVerificationVerified,
		DkimVerificationStatus:      dnsVerificationVerified,
		Records:                     dnsRecordsForDomain(defaultDomain, region),
		orgID:                       orgID,
	})

	for _, d := range domains {
		if d == "" {
			continue
		}
		b.mailDomains.Put(&MailDomain{
			DomainName:                  d,
			IsDefault:                   false,
			IsTestDomain:                false,
			OwnershipVerificationStatus: dnsVerificationVerified,
			DkimVerificationStatus:      dnsVerificationVerified,
			Records:                     dnsRecordsForDomain(d, region),
			orgID:                       orgID,
		})
	}

	return org, nil
}

// DescribeOrganization returns details about an organization.
func (b *InMemoryBackend) DescribeOrganization(orgID string) (*Organization, error) {
	b.mu.RLock("DescribeOrganization")
	defer b.mu.RUnlock()

	org, ok := b.organizations.Get(orgID)
	if !ok {
		return nil, fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	return org, nil
}

// DeleteOrganization removes a WorkMail organization and every collection
// nested under it, including the two that were previously left behind as
// ghost rows (see deleteTagsForOrg/deleteGlobalAliasesForOrg): tags on the
// organization itself and on every user/group/resource it contained, and
// globalAliases rows (primary emails and CreateAlias-created aliases alike)
// for every entity that belonged to it. availabilityConfigs,
// mobileDeviceRules, mobileDeviceOverrides, emailMonitoring, inboundDmarc,
// retentionPolicies, exportJobs, identityCenterApps, idpConfig,
// personalTokens, and issuedTokens are deliberately left untouched, matching
// prior behavior.
func (b *InMemoryBackend) DeleteOrganization(orgID string, _ bool) error {
	b.mu.Lock("DeleteOrganization")
	defer b.mu.Unlock()

	org, ok := b.organizations.Get(orgID)
	if !ok {
		return fmt.Errorf("%w: organization %q not found", ErrNotFound, orgID)
	}

	delete(b.orgsByAlias, org.Alias)
	b.organizations.Delete(orgID)
	deleteAllForOrg(b.users, b.usersByOrg, userKeyFn, orgID)
	delete(b.usersByEmail, orgID)
	deleteAllForOrg(b.groups, b.groupsByOrg, groupKeyFn, orgID)
	delete(b.groupsByEmail, orgID)
	delete(b.groupMembers, orgID)
	deleteAllForOrg(b.resources, b.resourcesByOrg, resourceKeyFn, orgID)
	delete(b.resourcesByEmail, orgID)
	delete(b.delegates, orgID)
	delete(b.aliases, orgID)
	b.deletePermissionsForOrg(orgID)
	deleteAllForOrg(b.mailDomains, b.mailDomainsByOrg, mailDomainKeyFn, orgID)
	deleteAllForOrg(b.accessRules, b.accessRulesByOrg, accessRuleKeyFn, orgID)
	deleteAllForOrg(b.impersonation, b.impersonationByOrg, impersonationKeyFn, orgID)
	delete(b.mailboxQuotas, orgID)
	b.deleteTagsForOrg(org.ARN)
	b.deleteGlobalAliasesForOrg(orgID)

	return nil
}

// deletePermissionsForOrg removes every permission entry belonging to orgID.
// permissions has no byOrg index (only byOrgEntity, used by the hot-path
// per-entity lookups); org-wide deletion only happens here, on
// DeleteOrganization, so a full Range scan is acceptable.
func (b *InMemoryBackend) deletePermissionsForOrg(orgID string) {
	var toDelete []string

	b.permissions.Range(func(p *Permission) bool {
		if p.orgID == orgID {
			toDelete = append(toDelete, permissionKey(p.orgID, p.entityID, p.GranteeID))
		}

		return true
	})

	for _, k := range toDelete {
		b.permissions.Delete(k)
	}
}

// ListOrganizations returns a paginated list of organizations.
func (b *InMemoryBackend) ListOrganizations(
	ctx context.Context,
	maxResults int32,
	nextToken string,
) ([]*OrgSummary, string, error) {
	requestRegion := b.regionFor(ctx)

	b.mu.RLock("ListOrganizations")
	defer b.mu.RUnlock()

	orgs := make([]*OrgSummary, 0, b.organizations.Len())
	for _, o := range b.organizations.All() {
		if o.Region != "" && requestRegion != "" && o.Region != requestRegion {
			continue
		}
		orgs = append(orgs, &OrgSummary{
			OrgID:             o.OrgID,
			Alias:             o.Alias,
			DefaultMailDomain: o.DefaultMailDomain,
			State:             o.State,
			ErrorMessage:      o.ErrorMessage,
		})
	}
	sort.Slice(orgs, func(i, j int) bool { return orgs[i].Alias < orgs[j].Alias })

	items, next := paginate(orgs, maxResults, nextToken)

	return items, next, nil
}
