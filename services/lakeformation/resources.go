package lakeformation

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// AddResourceInternal seeds a registered resource directly for testing.
func (b *InMemoryBackend) AddResourceInternal(resourceArn, roleArn string) {
	b.mu.Lock("AddResourceInternal")
	defer b.mu.Unlock()

	now := time.Now()
	b.resources.Put(&ResourceInfo{
		ResourceArn:  resourceArn,
		RoleArn:      roleArn,
		LastModified: &now,
	})
}

// RegisterResource registers an S3 location as a data lake resource.
func (b *InMemoryBackend) RegisterResource(resourceArn, roleArn string) error {
	if resourceArn == "" {
		return fmt.Errorf("ResourceArn is required: %w", ErrValidation)
	}

	b.mu.Lock("RegisterResource")
	defer b.mu.Unlock()

	if b.resources.Has(resourceArn) {
		return awserr.New(
			"resource already registered: "+resourceArn,
			awserr.ErrAlreadyExists,
		)
	}

	now := time.Now()
	b.resources.Put(&ResourceInfo{
		ResourceArn:  resourceArn,
		RoleArn:      roleArn,
		LastModified: &now,
	})

	return nil
}

// DeregisterResource removes a registered data lake resource and its associated permissions.
func (b *InMemoryBackend) DeregisterResource(resourceArn string) error {
	if resourceArn == "" {
		return fmt.Errorf("ResourceArn is required: %w", ErrValidation)
	}

	b.mu.Lock("DeregisterResource")
	defer b.mu.Unlock()

	if !b.resources.Has(resourceArn) {
		return awserr.New(
			"resource not found: "+resourceArn,
			awserr.ErrNotFound,
		)
	}

	b.resources.Delete(resourceArn)

	// Clean up all permissions associated with this resource.
	newList := make([]*PermissionEntry, 0, len(b.permissionsList))
	for _, p := range b.permissionsList {
		if !permissionMatchesARN(p, resourceArn) {
			newList = append(newList, p)
		} else {
			b.permissionsMap.Delete(permissionKey(p))
		}
	}
	b.permissionsList = newList

	return nil
}

// DescribeResource returns information about a registered resource.
func (b *InMemoryBackend) DescribeResource(resourceArn string) (*ResourceInfo, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("ResourceArn is required: %w", ErrValidation)
	}

	b.mu.RLock("DescribeResource")
	defer b.mu.RUnlock()

	info, ok := b.resources.Get(resourceArn)
	if !ok {
		return nil, awserr.New(
			"resource not found: "+resourceArn,
			awserr.ErrNotFound,
		)
	}

	return copyResourceInfo(info), nil
}

// ListResources returns a paginated list of registered resources.
func (b *InMemoryBackend) ListResources(maxResults int, nextToken string) ([]*ResourceInfo, string) {
	b.mu.RLock("ListResources")
	defer b.mu.RUnlock()

	all := make([]*ResourceInfo, 0, b.resources.Len())
	for _, v := range b.resources.All() {
		all = append(all, copyResourceInfo(v))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].ResourceArn < all[j].ResourceArn
	})

	return paginate(all, maxResults, nextToken, defaultMaxResults)
}

// resourceToKey returns a stable string key for a Resource pointer (used to index resourceLFTags).
func resourceToKey(r *Resource) string {
	if r == nil {
		return ""
	}

	if r.DataLocation != nil {
		return "datalocation:" + r.DataLocation.ResourceArn
	}

	if r.Database != nil {
		return "database:" + r.Database.Name
	}

	if r.Table != nil {
		return "table:" + r.Table.DatabaseName + "." + r.Table.Name
	}

	if r.Catalog != nil {
		return "catalog"
	}

	return ""
}

// copyResource returns a shallow copy of a Resource, preserving nested pointers.
func copyResource(r *Resource) *Resource {
	if r == nil {
		return nil
	}

	cp := &Resource{}

	if r.Catalog != nil {
		cat := *r.Catalog
		cp.Catalog = &cat
	}

	if r.Database != nil {
		db := *r.Database
		cp.Database = &db
	}

	if r.Table != nil {
		tbl := *r.Table
		cp.Table = &tbl
	}

	if r.TableWithColumns != nil {
		twc := *r.TableWithColumns
		if r.TableWithColumns.ColumnNames != nil {
			twc.ColumnNames = make([]string, len(r.TableWithColumns.ColumnNames))
			copy(twc.ColumnNames, r.TableWithColumns.ColumnNames)
		}
		cp.TableWithColumns = &twc
	}

	if r.DataLocation != nil {
		dl := *r.DataLocation
		cp.DataLocation = &dl
	}

	return cp
}

// copyResourceInfo returns a deep copy of a ResourceInfo, including the LastModified pointer.
func copyResourceInfo(ri *ResourceInfo) *ResourceInfo {
	if ri == nil {
		return nil
	}

	cp := &ResourceInfo{
		ResourceArn: ri.ResourceArn,
		RoleArn:     ri.RoleArn,
	}

	if ri.LastModified != nil {
		t := *ri.LastModified
		cp.LastModified = &t
	}

	return cp
}

// UpdateResource updates the role ARN of an already registered resource.
func (b *InMemoryBackend) UpdateResource(resourceArn, roleArn string) error {
	if resourceArn == "" {
		return fmt.Errorf("ResourceArn is required: %w", ErrValidation)
	}

	if roleArn == "" {
		return fmt.Errorf("RoleArn is required: %w", ErrValidation)
	}

	b.mu.Lock("UpdateResource")
	defer b.mu.Unlock()

	info, ok := b.resources.Get(resourceArn)
	if !ok {
		return awserr.New(
			"resource not found: "+resourceArn,
			awserr.ErrNotFound,
		)
	}

	info.RoleArn = roleArn
	now := time.Now()
	info.LastModified = &now

	return nil
}
