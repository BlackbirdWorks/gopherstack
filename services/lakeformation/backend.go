package lakeformation

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// StorageBackend is the interface for Lake Formation backend operations.
type StorageBackend interface {
	GetDataLakeSettings() *DataLakeSettings
	PutDataLakeSettings(settings *DataLakeSettings)

	RegisterResource(resourceArn, roleArn string) error
	DeregisterResource(resourceArn string) error
	DescribeResource(resourceArn string) (*ResourceInfo, error)
	ListResources(maxResults int, nextToken string) ([]*ResourceInfo, string)

	GrantPermissions(entry *PermissionEntry) error
	RevokePermissions(entry *PermissionEntry) error
	ListPermissions(resourceArn string, maxResults int, nextToken string) ([]*PermissionEntry, string)

	CreateLFTag(catalogID, tagKey string, tagValues []string) error
	DeleteLFTag(catalogID, tagKey string) error
	GetLFTag(catalogID, tagKey string) (*LFTag, error)
	UpdateLFTag(catalogID, tagKey string, tagValuesToAdd, tagValuesToDelete []string) error
	ListLFTags(catalogID string, maxResults int, nextToken string) ([]*LFTag, string)

	BatchGrantPermissions(entries []*PermissionEntry) []*BatchFailureEntry
	BatchRevokePermissions(entries []*PermissionEntry) []*BatchFailureEntry
}

// lfTagKey uniquely identifies a LF tag by catalog and key.
type lfTagKey struct {
	CatalogID string
	TagKey    string
}

// InMemoryBackend is the in-memory backend for Lake Formation.
type InMemoryBackend struct {
	dataLakeSettings *DataLakeSettings
	resources        map[string]*ResourceInfo
	lfTags           map[lfTagKey]*LFTag
	permissions      []*PermissionEntry
	mu               sync.RWMutex
}

// NewInMemoryBackend creates a new in-memory Lake Formation backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		dataLakeSettings: &DataLakeSettings{},
		resources:        make(map[string]*ResourceInfo),
		permissions:      make([]*PermissionEntry, 0),
		lfTags:           make(map[lfTagKey]*LFTag),
	}
}

// GetDataLakeSettings returns the current data lake settings.
func (b *InMemoryBackend) GetDataLakeSettings() *DataLakeSettings {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.dataLakeSettings == nil {
		return &DataLakeSettings{}
	}

	return b.dataLakeSettings
}

// PutDataLakeSettings replaces the data lake settings.
func (b *InMemoryBackend) PutDataLakeSettings(settings *DataLakeSettings) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.dataLakeSettings = settings
}

// RegisterResource registers an S3 location as a data lake resource.
func (b *InMemoryBackend) RegisterResource(resourceArn, roleArn string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.resources[resourceArn]; ok {
		return awserr.New(
			fmt.Sprintf("resource already registered: %s", resourceArn),
			awserr.ErrAlreadyExists,
		)
	}

	now := time.Now()
	b.resources[resourceArn] = &ResourceInfo{
		ResourceArn:  resourceArn,
		RoleArn:      roleArn,
		LastModified: &now,
	}

	return nil
}

// DeregisterResource removes a registered data lake resource.
func (b *InMemoryBackend) DeregisterResource(resourceArn string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.resources[resourceArn]; !ok {
		return awserr.New(
			fmt.Sprintf("resource not found: %s", resourceArn),
			awserr.ErrNotFound,
		)
	}

	delete(b.resources, resourceArn)

	return nil
}

// DescribeResource returns information about a registered resource.
func (b *InMemoryBackend) DescribeResource(resourceArn string) (*ResourceInfo, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	info, ok := b.resources[resourceArn]
	if !ok {
		return nil, awserr.New(
			fmt.Sprintf("resource not found: %s", resourceArn),
			awserr.ErrNotFound,
		)
	}

	return info, nil
}

const defaultMaxResults = 100

// ListResources returns a paginated list of registered resources.
func (b *InMemoryBackend) ListResources(maxResults int, nextToken string) ([]*ResourceInfo, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := make([]*ResourceInfo, 0, len(b.resources))
	for _, v := range b.resources {
		all = append(all, v)
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].ResourceArn < all[j].ResourceArn
	})

	return paginate(all, maxResults, nextToken, defaultMaxResults)
}

// GrantPermissions adds a permission entry.
func (b *InMemoryBackend) GrantPermissions(entry *PermissionEntry) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.permissions = append(b.permissions, entry)

	return nil
}

// RevokePermissions removes a matching permission entry.
func (b *InMemoryBackend) RevokePermissions(entry *PermissionEntry) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	updated := b.permissions[:0]

	for _, p := range b.permissions {
		if !permissionMatches(p, entry) {
			updated = append(updated, p)
		}
	}

	b.permissions = updated

	return nil
}

// ListPermissions returns a paginated list of permission entries filtered by resource ARN.
func (b *InMemoryBackend) ListPermissions(
	resourceArn string,
	maxResults int,
	nextToken string,
) ([]*PermissionEntry, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var filtered []*PermissionEntry

	for _, p := range b.permissions {
		if resourceArn == "" || permissionMatchesARN(p, resourceArn) {
			filtered = append(filtered, p)
		}
	}

	return paginate(filtered, maxResults, nextToken, defaultMaxResults)
}

// CreateLFTag creates a new LF tag with the given values.
func (b *InMemoryBackend) CreateLFTag(catalogID, tagKey string, tagValues []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	k := lfTagKey{CatalogID: catalogID, TagKey: tagKey}

	if _, ok := b.lfTags[k]; ok {
		return awserr.New(
			fmt.Sprintf("LF tag already exists: %s", tagKey),
			awserr.ErrAlreadyExists,
		)
	}

	vals := make([]string, len(tagValues))
	copy(vals, tagValues)

	b.lfTags[k] = &LFTag{
		CatalogID: catalogID,
		TagKey:    tagKey,
		TagValues: vals,
	}

	return nil
}

// DeleteLFTag removes a LF tag.
func (b *InMemoryBackend) DeleteLFTag(catalogID, tagKey string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	k := lfTagKey{CatalogID: catalogID, TagKey: tagKey}

	if _, ok := b.lfTags[k]; !ok {
		return awserr.New(
			fmt.Sprintf("LF tag not found: %s", tagKey),
			awserr.ErrNotFound,
		)
	}

	delete(b.lfTags, k)

	return nil
}

// GetLFTag returns the LF tag for the given catalog and key.
func (b *InMemoryBackend) GetLFTag(catalogID, tagKey string) (*LFTag, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	k := lfTagKey{CatalogID: catalogID, TagKey: tagKey}

	tag, ok := b.lfTags[k]
	if !ok {
		return nil, awserr.New(
			fmt.Sprintf("LF tag not found: %s", tagKey),
			awserr.ErrNotFound,
		)
	}

	return tag, nil
}

// UpdateLFTag adds and removes values from an existing LF tag.
func (b *InMemoryBackend) UpdateLFTag(catalogID, tagKey string, tagValuesToAdd, tagValuesToDelete []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	k := lfTagKey{CatalogID: catalogID, TagKey: tagKey}

	tag, ok := b.lfTags[k]
	if !ok {
		return awserr.New(
			fmt.Sprintf("LF tag not found: %s", tagKey),
			awserr.ErrNotFound,
		)
	}

	vals := tag.TagValues

	for _, v := range tagValuesToAdd {
		if !slices.Contains(vals, v) {
			vals = append(vals, v)
		}
	}

	for _, v := range tagValuesToDelete {
		vals = slices.DeleteFunc(vals, func(s string) bool { return s == v })
	}

	tag.TagValues = vals

	return nil
}

// ListLFTags returns a paginated list of LF tags for the given catalog.
func (b *InMemoryBackend) ListLFTags(catalogID string, maxResults int, nextToken string) ([]*LFTag, string) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var all []*LFTag

	for k, t := range b.lfTags {
		if catalogID == "" || k.CatalogID == catalogID {
			all = append(all, t)
		}
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].CatalogID != all[j].CatalogID {
			return all[i].CatalogID < all[j].CatalogID
		}

		return all[i].TagKey < all[j].TagKey
	})

	return paginate(all, maxResults, nextToken, defaultMaxResults)
}

// BatchGrantPermissions grants permissions for multiple entries.
func (b *InMemoryBackend) BatchGrantPermissions(entries []*PermissionEntry) []*BatchFailureEntry {
	var failures []*BatchFailureEntry

	for _, e := range entries {
		if err := b.GrantPermissions(e); err != nil {
			failures = append(failures, &BatchFailureEntry{
				RequestEntry: e,
				Error: &errorDetail{
					ErrorCode:    "InternalServiceException",
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

	for _, e := range entries {
		if err := b.RevokePermissions(e); err != nil {
			failures = append(failures, &BatchFailureEntry{
				RequestEntry: e,
				Error: &errorDetail{
					ErrorCode:    "InternalServiceException",
					ErrorMessage: err.Error(),
				},
			})
		}
	}

	return failures
}

// permissionMatches returns true if two permission entries have the same principal, resource,
// and overlapping permissions (i.e., all revoke permissions are present in the stored entry).
func permissionMatches(a, b *PermissionEntry) bool {
	if a == nil || b == nil {
		return a == b
	}

	if !principalEqual(a.Principal, b.Principal) {
		return false
	}

	if !resourceEqual(a.Resource, b.Resource) {
		return false
	}

	// If the revoke request specifies permissions, only match entries that contain them all.
	if len(b.Permissions) > 0 {
		for _, p := range b.Permissions {
			if !slices.Contains(a.Permissions, p) {
				return false
			}
		}
	}

	return true
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
func permissionMatchesARN(p *PermissionEntry, arn string) bool {
	if p.Resource == nil {
		return false
	}

	if p.Resource.DataLocation != nil {
		return p.Resource.DataLocation.ResourceArn == arn
	}

	return false
}

// paginate is a simple index-based paginator for slices.
// nextToken is used as a decimal start index.
func paginate[T any](items []T, maxResults int, nextToken string, defaultMax int) ([]T, string) {
	start := 0

	if nextToken != "" {
		if _, err := fmt.Sscanf(nextToken, "%d", &start); err != nil {
			start = 0
		}

		if start < 0 {
			start = 0
		}
	}

	if start >= len(items) {
		return items[:0], ""
	}

	limit := defaultMax
	if maxResults > 0 {
		limit = maxResults
	}

	end := min(start+limit, len(items))

	page := items[start:end]

	var outToken string

	if end < len(items) {
		outToken = strconv.Itoa(end)
	}

	return page, outToken
}
