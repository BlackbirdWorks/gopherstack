package lakeformation

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"
)

// AddPermissionInternal seeds a permission entry directly for testing.
func (b *InMemoryBackend) AddPermissionInternal(entry *PermissionEntry) {
	b.mu.Lock("AddPermissionInternal")
	defer b.mu.Unlock()

	_ = b.grantPermissionsLocked(entry)
}

// permissionKey returns a unique string for a principal and resource.
func permissionKey(entry *PermissionEntry) string {
	if entry == nil {
		return ""
	}

	return principalID(entry.Principal) + "|" + resourceToKey(entry.Resource)
}

func (b *InMemoryBackend) grantPermissionsLocked(entry *PermissionEntry) error {
	if entry == nil || entry.Principal == nil || entry.Resource == nil {
		return fmt.Errorf("invalid entry: %w", ErrValidation)
	}
	if err := validatePermissions(entry.Permissions); err != nil {
		return err
	}
	if err := validateGrantOptionSubset(entry.Permissions, entry.PermissionsWithGrantOption); err != nil {
		return err
	}
	if entry.Resource.TableWithColumns != nil && entry.Resource.Table == nil {
		twc := entry.Resource.TableWithColumns
		entry.Resource.Table = &TableResource{
			DatabaseName: twc.DatabaseName,
			Name:         twc.Name,
			CatalogID:    twc.CatalogID,
		}
	}

	now := time.Now()
	entry.LastUpdated = &now

	key := permissionKey(entry)
	if existing, ok := b.permissionsMap.Get(key); ok {
		mergeStringSlice(&existing.Permissions, entry.Permissions)
		mergeStringSlice(&existing.PermissionsWithGrantOption, entry.PermissionsWithGrantOption)
		existing.LastUpdated = &now

		if entry.Condition != nil {
			existing.Condition = entry.Condition
		}

		return nil
	}
	b.permissionsMap.Put(entry)
	b.permissionsList = append(b.permissionsList, entry)
	sort.Slice(b.permissionsList, func(i, j int) bool {
		pi := principalID(b.permissionsList[i].Principal)
		pj := principalID(b.permissionsList[j].Principal)
		if pi != pj {
			return pi < pj
		}

		return resourceToKey(b.permissionsList[i].Resource) < resourceToKey(b.permissionsList[j].Resource)
	})

	return nil
}

// GrantPermissions adds a permission entry.
func (b *InMemoryBackend) GrantPermissions(entry *PermissionEntry) error {
	b.mu.Lock("GrantPermissions")
	defer b.mu.Unlock()

	return b.grantPermissionsLocked(entry)
}

// RevokePermissions removes specific permissions from a matching entry.
// If all permissions are revoked, the entry is deleted.
func (b *InMemoryBackend) RevokePermissions(entry *PermissionEntry) error {
	b.mu.Lock("RevokePermissions")
	defer b.mu.Unlock()

	return b.revokePermissionsLocked(entry)
}

func (b *InMemoryBackend) revokePermissionsLocked(entry *PermissionEntry) error {
	if entry == nil || entry.Principal == nil || entry.Resource == nil {
		return fmt.Errorf("invalid entry: %w", ErrValidation)
	}
	key := permissionKey(entry)
	p, ok := b.permissionsMap.Get(key)
	if !ok {
		return nil
	}
	remaining := make([]string, 0, len(p.Permissions))
	for _, perm := range p.Permissions {
		if !slices.Contains(entry.Permissions, perm) {
			remaining = append(remaining, perm)
		}
	}
	if len(remaining) > 0 {
		p.Permissions = remaining
		now := time.Now()
		p.LastUpdated = &now

		return nil
	}
	b.permissionsMap.Delete(key)
	newList := make([]*PermissionEntry, 0, len(b.permissionsList)-1)
	for _, lp := range b.permissionsList {
		if permissionKey(lp) != key {
			newList = append(newList, lp)
		}
	}
	b.permissionsList = newList

	return nil
}

// ListPermissions returns a paginated list of permission entries filtered by resource,
// principal, and/or resource type. resource mirrors the real ListPermissionsInput.Resource
// shape (a nested Resource union), not a flat ARN -- see permissionMatchesResource.
func (b *InMemoryBackend) ListPermissions(
	resource *Resource,
	maxResults int,
	nextToken string,
	principal *DataLakePrincipal,
	resourceType string,
) ([]*PermissionEntry, string) {
	b.mu.RLock("ListPermissions")
	defer b.mu.RUnlock()

	filtered := make([]*PermissionEntry, 0, len(b.permissionsList))

	for _, p := range b.permissionsList {
		if !permissionMatchesResource(p, resource) {
			continue
		}

		if principal != nil && principal.DataLakePrincipalIdentifier != "" {
			if p.Principal == nil || p.Principal.DataLakePrincipalIdentifier != principal.DataLakePrincipalIdentifier {
				continue
			}
		}

		if resourceType != "" && !permissionMatchesResourceType(p, resourceType) {
			continue
		}

		filtered = append(filtered, deepCopyPermissionEntry(p))
	}

	return paginate(filtered, maxResults, nextToken, defaultMaxResults)
}

// toPermissionEntry converts a batch request entry to the internal PermissionEntry
// shape used for grant/revoke. The caller-supplied Id is not part of PermissionEntry
// (it exists only to correlate BatchFailureEntry.RequestEntry back to the request).
func toPermissionEntry(e *BatchPermissionsRequestEntry) *PermissionEntry {
	if e == nil {
		return nil
	}

	return &PermissionEntry{
		Principal:                  e.Principal,
		Resource:                   e.Resource,
		Permissions:                e.Permissions,
		PermissionsWithGrantOption: e.PermissionsWithGrantOption,
		Condition:                  e.Condition,
	}
}

// BatchGrantPermissions grants permissions for multiple entries.
func (b *InMemoryBackend) BatchGrantPermissions(entries []*BatchPermissionsRequestEntry) []*BatchFailureEntry {
	var failures []*BatchFailureEntry

	b.mu.Lock("BatchGrantPermissions")
	defer b.mu.Unlock()

	for _, e := range entries {
		if err := b.grantPermissionsLocked(toPermissionEntry(e)); err != nil {
			errCode := "InternalServiceException"
			if errors.Is(err, ErrValidation) {
				errCode = errCodeInvalidInput
			}

			failures = append(failures, &BatchFailureEntry{
				RequestEntry: e,
				Error: &errorDetail{
					ErrorCode:    errCode,
					ErrorMessage: err.Error(),
				},
			})
		}
	}

	return failures
}

// BatchRevokePermissions revokes permissions for multiple entries.
func (b *InMemoryBackend) BatchRevokePermissions(entries []*BatchPermissionsRequestEntry) []*BatchFailureEntry {
	var failures []*BatchFailureEntry

	b.mu.Lock("BatchRevokePermissions")
	defer b.mu.Unlock()

	for _, e := range entries {
		if err := b.revokePermissionsLocked(toPermissionEntry(e)); err != nil {
			errCode := "InternalServiceException"
			if errors.Is(err, ErrValidation) {
				errCode = errCodeInvalidInput
			}

			failures = append(failures, &BatchFailureEntry{
				RequestEntry: e,
				Error: &errorDetail{
					ErrorCode:    errCode,
					ErrorMessage: err.Error(),
				},
			})
		}
	}

	return failures
}

// mergeStringSlice appends values from src to dst if not already present.
func mergeStringSlice(dst *[]string, src []string) {
	for _, v := range src {
		if !slices.Contains(*dst, v) {
			*dst = append(*dst, v)
		}
	}
}

func principalEqual(a, b *DataLakePrincipal) bool {
	if a == nil || b == nil {
		return a == b
	}

	return a.DataLakePrincipalIdentifier == b.DataLakePrincipalIdentifier
}

// resourceEqual reports whether a and b identify the same resource, comparing
// by the same per-kind key resourceToKey uses for indexing/sorting.
func resourceEqual(a, b *Resource) bool {
	if a == nil || b == nil {
		return a == b
	}

	ka, kb := resourceToKey(a), resourceToKey(b)
	if ka == "" || kb == "" {
		return false
	}

	return ka == kb
}

// permissionMatchesARN returns true if the permission entry's resource matches the given ARN.
// For DataLocation resources the ARN is compared directly; for database/table resources an
// ARN suffix match is used (arn:…:database/name or arn:…:table/db/name).
func permissionMatchesARN(p *PermissionEntry, arn string) bool {
	if p.Resource == nil {
		return false
	}

	if p.Resource.DataLocation != nil {
		return p.Resource.DataLocation.ResourceArn == arn
	}

	if p.Resource.Database != nil {
		return strings.HasSuffix(arn, "/"+p.Resource.Database.Name) ||
			strings.HasSuffix(arn, ":database/"+p.Resource.Database.Name)
	}

	if p.Resource.Table != nil {
		return strings.HasSuffix(arn, "/"+p.Resource.Table.DatabaseName+"/"+p.Resource.Table.Name) ||
			strings.HasSuffix(arn, ":table/"+p.Resource.Table.DatabaseName+"/"+p.Resource.Table.Name)
	}

	return false
}

// permissionMatchesResource reports whether a permission entry's resource
// matches the given ListPermissions filter Resource. A nil filter (no
// Resource specified in the request) matches everything. TableWithColumns in
// the filter is treated as its containing Table, matching AWS's documented
// behavior that ListPermissions "does not support getting privileges on a
// table with columns" as a filter -- callers pass the table instead.
func permissionMatchesResource(p *PermissionEntry, filter *Resource) bool {
	if filter == nil {
		return true
	}

	if p.Resource == nil {
		return false
	}

	switch {
	case filter.Catalog != nil:
		return p.Resource.Catalog != nil
	case filter.Database != nil:
		return resourceMatchesDatabase(p.Resource, filter.Database)
	case filter.Table != nil:
		return resourceMatchesTable(p.Resource, filter.Table.DatabaseName, filter.Table.Name)
	case filter.TableWithColumns != nil:
		return resourceMatchesTable(p.Resource, filter.TableWithColumns.DatabaseName, filter.TableWithColumns.Name)
	case filter.DataLocation != nil:
		return resourceMatchesDataLocation(p.Resource, filter.DataLocation)
	case filter.DataCellsFilter != nil:
		return resourceMatchesDataCellsFilter(p.Resource, filter.DataCellsFilter)
	case filter.LFTag != nil:
		return resourceMatchesLFTag(p.Resource, filter.LFTag)
	case filter.LFTagExpression != nil:
		return resourceMatchesLFTagExpression(p.Resource, filter.LFTagExpression)
	case filter.LFTagPolicy != nil:
		return resourceMatchesLFTagPolicy(p.Resource, filter.LFTagPolicy)
	default:
		return true
	}
}

func resourceMatchesDatabase(r *Resource, want *DatabaseResource) bool {
	return r.Database != nil && r.Database.Name == want.Name
}

// resourceMatchesTable matches r's Table against a (databaseName, name) pair.
// A ListPermissions filter's TableWithColumns is treated as its containing
// Table (AWS documents that ListPermissions "does not support getting
// privileges on a table with columns" as a filter -- callers pass the table).
func resourceMatchesTable(r *Resource, databaseName, name string) bool {
	return r.Table != nil && r.Table.DatabaseName == databaseName && r.Table.Name == name
}

func resourceMatchesDataLocation(r *Resource, want *DataLocationResource) bool {
	return r.DataLocation != nil && r.DataLocation.ResourceArn == want.ResourceArn
}

func resourceMatchesDataCellsFilter(r *Resource, want *DataCellsFilterResource) bool {
	return r.DataCellsFilter != nil && *r.DataCellsFilter == *want
}

func resourceMatchesLFTag(r *Resource, want *LFTagKeyResource) bool {
	return r.LFTag != nil && r.LFTag.CatalogID == want.CatalogID && r.LFTag.TagKey == want.TagKey
}

func resourceMatchesLFTagExpression(r *Resource, want *LFTagExpressionResource) bool {
	return r.LFTagExpression != nil &&
		r.LFTagExpression.CatalogID == want.CatalogID &&
		r.LFTagExpression.Name == want.Name
}

func resourceMatchesLFTagPolicy(r *Resource, want *LFTagPolicyResource) bool {
	return r.LFTagPolicy != nil &&
		r.LFTagPolicy.CatalogID == want.CatalogID &&
		r.LFTagPolicy.ResourceType == want.ResourceType
}

// validateBatchPermissionsEntries checks that every entry carries the
// caller-supplied Id the real API requires (types.BatchPermissionsRequestEntry.Id
// is a required member) so BatchFailureEntry.RequestEntry can be correlated
// back to the request that produced it.
func validateBatchPermissionsEntries(entries []*BatchPermissionsRequestEntry) error {
	for i, e := range entries {
		if e == nil || strings.TrimSpace(e.ID) == "" {
			return fmt.Errorf("Entries[%d].Id is required: %w", i, ErrValidation)
		}
	}

	return nil
}

// isValidPermission returns true if the given permission string is a known
// Lake Formation permission. This must match types.Permission's Values()
// exactly (aws-sdk-go-v2/service/lakeformation/types/enums.go) -- gopherstack
// previously accepted three permission strings that do not exist in the real
// enum at all ("CREATE_TAG", "CREATE_LAKE_FORMATION_OPT_IN", and "SUPER",
// the last a typo'd stand-in for the real "SUPER_USER") and was missing the
// real "CREATE_LF_TAG_EXPRESSION" value.
func isValidPermission(perm string) bool {
	switch perm {
	case "ALL", "SELECT", "ALTER", "DROP", "DELETE", "INSERT", "DESCRIBE",
		"CREATE_DATABASE", "CREATE_TABLE", "DATA_LOCATION_ACCESS",
		"CREATE_LF_TAG", "ASSOCIATE", "GRANT_WITH_LF_TAG_EXPRESSION",
		"CREATE_LF_TAG_EXPRESSION", "CREATE_CATALOG", "SUPER_USER":
		return true
	default:
		return false
	}
}

// validateGrantOptionSubset checks that every permission in
// permissionsWithGrantOption also appears in permissions. This is enforced
// here (not only in handleGrantPermissions) so BatchGrantPermissions gets the
// same validation per-entry, surfaced as a Failures[] InvalidInputException
// rather than skipped entirely -- the real API validates
// BatchPermissionsRequestEntry the same way it validates GrantPermissionsInput.
func validateGrantOptionSubset(permissions, permissionsWithGrantOption []string) error {
	if len(permissionsWithGrantOption) == 0 {
		return nil
	}

	permSet := make(map[string]bool, len(permissions))
	for _, p := range permissions {
		permSet[p] = true
	}

	for _, g := range permissionsWithGrantOption {
		if !permSet[g] {
			return fmt.Errorf("PermissionsWithGrantOption must be a subset of Permissions: %w", ErrValidation)
		}
	}

	return nil
}

// validatePermissions checks that all permission strings are valid.
func validatePermissions(perms []string) error {
	for _, p := range perms {
		if !isValidPermission(p) {
			return fmt.Errorf("invalid permission: %s: %w", p, ErrValidation)
		}
	}

	return nil
}

// permissionMatchesResourceType checks whether a permission entry's resource
// matches the given resource type string (e.g. "DATABASE", "TABLE", "DATA_LOCATION").
func permissionMatchesResourceType(p *PermissionEntry, resourceType string) bool {
	if p.Resource == nil {
		return false
	}

	switch strings.ToUpper(resourceType) {
	case "DATABASE":
		return p.Resource.Database != nil
	case "TABLE":
		return p.Resource.Table != nil || p.Resource.TableWithColumns != nil
	case "DATA_LOCATION":
		return p.Resource.DataLocation != nil
	case "CATALOG":
		return p.Resource.Catalog != nil
	case "LF_TAG":
		return p.Resource.LFTag != nil
	case "LF_TAG_POLICY":
		return p.Resource.LFTagPolicy != nil
	case "LF_TAG_POLICY_DATABASE":
		return p.Resource.LFTagPolicy != nil && p.Resource.LFTagPolicy.ResourceType == "DATABASE"
	case "LF_TAG_POLICY_TABLE":
		return p.Resource.LFTagPolicy != nil && p.Resource.LFTagPolicy.ResourceType == "TABLE"
	case "LF_NAMED_TAG_EXPRESSION":
		return p.Resource.LFTagExpression != nil
	default:
		return false
	}
}

// principalID returns the DataLakePrincipalIdentifier for a principal, or "" if nil.
func principalID(p *DataLakePrincipal) string {
	if p == nil {
		return ""
	}

	return p.DataLakePrincipalIdentifier
}

// deepCopyPermissionEntry returns a deep copy of a PermissionEntry including pointer fields.
func deepCopyPermissionEntry(e *PermissionEntry) *PermissionEntry {
	if e == nil {
		return nil
	}

	cp := &PermissionEntry{}

	if e.Principal != nil {
		p := *e.Principal
		cp.Principal = &p
	}

	if e.Resource != nil {
		cp.Resource = copyResource(e.Resource)
	}

	if e.Permissions != nil {
		cp.Permissions = make([]string, len(e.Permissions))
		copy(cp.Permissions, e.Permissions)
	}

	if e.PermissionsWithGrantOption != nil {
		cp.PermissionsWithGrantOption = make([]string, len(e.PermissionsWithGrantOption))
		copy(cp.PermissionsWithGrantOption, e.PermissionsWithGrantOption)
	}

	if e.Condition != nil {
		cond := *e.Condition
		cp.Condition = &cond
	}

	if e.LastUpdated != nil {
		t := *e.LastUpdated
		cp.LastUpdated = &t
	}

	cp.LastUpdatedBy = e.LastUpdatedBy

	return cp
}

// GetEffectivePermissionsForPath returns effective permissions for a resource
// path. Unlike ListPermissions, the real GetEffectivePermissionsForPathInput
// filters by a flat ResourceArn string, so this uses permissionMatchesARN
// directly rather than ListPermissions' Resource-shaped filter.
func (b *InMemoryBackend) GetEffectivePermissionsForPath(
	resourceArn string, maxResults int, nextToken string,
) ([]*PermissionEntry, string) {
	b.mu.RLock("GetEffectivePermissionsForPath")
	defer b.mu.RUnlock()

	filtered := make([]*PermissionEntry, 0, len(b.permissionsList))

	for _, p := range b.permissionsList {
		if resourceArn != "" && !permissionMatchesARN(p, resourceArn) {
			continue
		}

		filtered = append(filtered, deepCopyPermissionEntry(p))
	}

	return paginate(filtered, maxResults, nextToken, defaultMaxResults)
}
