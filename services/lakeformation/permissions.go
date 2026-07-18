package lakeformation

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
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
	if entry.Resource.TableWithColumns != nil && entry.Resource.Table == nil {
		twc := entry.Resource.TableWithColumns
		entry.Resource.Table = &TableResource{
			DatabaseName: twc.DatabaseName,
			Name:         twc.Name,
			CatalogID:    twc.CatalogID,
		}
	}
	key := permissionKey(entry)
	if existing, ok := b.permissionsMap.Get(key); ok {
		mergeStringSlice(&existing.Permissions, entry.Permissions)
		mergeStringSlice(&existing.PermissionsWithGrantOption, entry.PermissionsWithGrantOption)

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

// ListPermissions returns a paginated list of permission entries filtered by resource ARN,
// principal, and/or resource type.
func (b *InMemoryBackend) ListPermissions(
	resourceArn string,
	maxResults int,
	nextToken string,
	principal *DataLakePrincipal,
	resourceType string,
) ([]*PermissionEntry, string) {
	b.mu.RLock("ListPermissions")
	defer b.mu.RUnlock()

	filtered := make([]*PermissionEntry, 0, len(b.permissionsList))

	for _, p := range b.permissionsList {
		if resourceArn != "" && !permissionMatchesARN(p, resourceArn) {
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

// BatchGrantPermissions grants permissions for multiple entries.
func (b *InMemoryBackend) BatchGrantPermissions(entries []*PermissionEntry) []*BatchFailureEntry {
	var failures []*BatchFailureEntry

	b.mu.Lock("BatchGrantPermissions")
	defer b.mu.Unlock()

	for _, e := range entries {
		if err := b.grantPermissionsLocked(e); err != nil {
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
func (b *InMemoryBackend) BatchRevokePermissions(entries []*PermissionEntry) []*BatchFailureEntry {
	var failures []*BatchFailureEntry

	b.mu.Lock("BatchRevokePermissions")
	defer b.mu.Unlock()

	for _, e := range entries {
		if err := b.revokePermissionsLocked(e); err != nil {
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

func resourceEqual(a, b *Resource) bool {
	if a == nil || b == nil {
		return a == b
	}

	if a.DataLocation != nil && b.DataLocation != nil {
		return a.DataLocation.ResourceArn == b.DataLocation.ResourceArn
	}

	if a.Database != nil && b.Database != nil {
		return a.Database.Name == b.Database.Name
	}

	if a.Table != nil && b.Table != nil {
		return a.Table.DatabaseName == b.Table.DatabaseName && a.Table.Name == b.Table.Name
	}

	return false
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

// isValidPermission returns true if the given permission string is a known Lake Formation permission.
func isValidPermission(perm string) bool {
	switch perm {
	case "ALL", "SELECT", "ALTER", "DROP", "DELETE", "INSERT", "DESCRIBE",
		"CREATE_DATABASE", "CREATE_TABLE", "DATA_LOCATION_ACCESS",
		"CREATE_TAG", "ASSOCIATE", "CREATE_LAKE_FORMATION_OPT_IN",
		"GRANT_WITH_LF_TAG_EXPRESSION", "CREATE_LF_TAG", "CREATE_CATALOG", "SUPER":
		return true
	default:
		return false
	}
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

	return cp
}

// GetEffectivePermissionsForPath returns effective permissions for a resource path.
func (b *InMemoryBackend) GetEffectivePermissionsForPath(
	resourceArn string, maxResults int, nextToken string,
) ([]*PermissionEntry, string) {
	return b.ListPermissions(resourceArn, maxResults, nextToken, nil, "")
}
