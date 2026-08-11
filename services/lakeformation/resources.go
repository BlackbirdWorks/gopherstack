package lakeformation

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// verificationStatusVerified is the ResourceInfo.VerificationStatus value the
// emulator always reports: it never performs real IAM verification of the
// registered role's access to the Amazon S3 location, so registration always
// "succeeds" (matches VerificationStatusVerified in aws-sdk-go-v2's
// types.VerificationStatus enum).
const verificationStatusVerified = "VERIFIED"

// RegisterResourceOptions carries the RegisterResource/UpdateResource fields
// beyond ResourceArn/RoleArn that the real AWS API supports (see
// types.RegisterResourceInput / types.UpdateResourceInput). A zero value
// matches the pre-existing 2-arg registration behavior.
type RegisterResourceOptions struct {
	ExpectedResourceOwnerAccount string
	WithFederation               bool
	WithPrivilegedAccess         bool
	HybridAccessEnabled          bool
}

// AddResourceInternal seeds a registered resource directly for testing.
func (b *InMemoryBackend) AddResourceInternal(resourceArn, roleArn string) {
	b.mu.Lock("AddResourceInternal")
	defer b.mu.Unlock()

	now := time.Now()
	b.resources.Put(&ResourceInfo{
		ResourceArn:        resourceArn,
		RoleArn:            roleArn,
		LastModified:       &now,
		VerificationStatus: verificationStatusVerified,
	})
}

// RegisterResource registers an S3 location as a data lake resource.
func (b *InMemoryBackend) RegisterResource(resourceArn, roleArn string, opts RegisterResourceOptions) error {
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
		ResourceArn:                  resourceArn,
		RoleArn:                      roleArn,
		LastModified:                 &now,
		ExpectedResourceOwnerAccount: opts.ExpectedResourceOwnerAccount,
		WithFederation:               opts.WithFederation,
		WithPrivilegedAccess:         opts.WithPrivilegedAccess,
		HybridAccessEnabled:          opts.HybridAccessEnabled,
		// The emulator never performs real IAM verification, so a registered
		// role is always reported as able to access the location.
		VerificationStatus: verificationStatusVerified,
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

// resourceToKey returns a stable string key for a Resource pointer (used to
// index resourceLFTags and as the permission-map key component). Exactly one
// of Resource's fields is expected to be set (AWS models Resource as a
// union), so the checks below are mutually exclusive in practice.
func resourceToKey(r *Resource) string {
	if r == nil {
		return ""
	}

	switch {
	case r.DataLocation != nil:
		return "datalocation:" + r.DataLocation.ResourceArn
	case r.Database != nil:
		return "database:" + r.Database.Name
	case r.Table != nil:
		return "table:" + r.Table.DatabaseName + "." + r.Table.Name
	case r.TableWithColumns != nil:
		// Column-level tags are not tracked separately (no per-column storage
		// exists); a TableWithColumns resource shares its containing table's key.
		return "table:" + r.TableWithColumns.DatabaseName + "." + r.TableWithColumns.Name
	case r.DataCellsFilter != nil:
		return "datacellsfilter:" + r.DataCellsFilter.TableCatalogID + "|" + r.DataCellsFilter.DatabaseName +
			"|" + r.DataCellsFilter.TableName + "|" + r.DataCellsFilter.Name
	case r.LFTag != nil:
		return "lftag:" + r.LFTag.CatalogID + "|" + r.LFTag.TagKey
	case r.LFTagExpression != nil:
		return "lftagexpression:" + r.LFTagExpression.CatalogID + "|" + r.LFTagExpression.Name
	case r.LFTagPolicy != nil:
		return "lftagpolicy:" + r.LFTagPolicy.CatalogID + "|" + r.LFTagPolicy.ResourceType
	case r.Catalog != nil:
		return "catalog:" + r.Catalog.ID
	default:
		return ""
	}
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

		if r.TableWithColumns.ColumnWildcard != nil {
			cw := *r.TableWithColumns.ColumnWildcard
			twc.ColumnWildcard = &cw
		}

		cp.TableWithColumns = &twc
	}

	if r.DataLocation != nil {
		dl := *r.DataLocation
		cp.DataLocation = &dl
	}

	if r.DataCellsFilter != nil {
		dcf := *r.DataCellsFilter
		cp.DataCellsFilter = &dcf
	}

	if r.LFTag != nil {
		lt := *r.LFTag
		if r.LFTag.TagValues != nil {
			lt.TagValues = make([]string, len(r.LFTag.TagValues))
			copy(lt.TagValues, r.LFTag.TagValues)
		}

		cp.LFTag = &lt
	}

	if r.LFTagExpression != nil {
		le := *r.LFTagExpression
		cp.LFTagExpression = &le
	}

	if r.LFTagPolicy != nil {
		lp := *r.LFTagPolicy
		if r.LFTagPolicy.Expression != nil {
			lp.Expression = make([]LFTag, len(r.LFTagPolicy.Expression))
			copy(lp.Expression, r.LFTagPolicy.Expression)
		}

		cp.LFTagPolicy = &lp
	}

	return cp
}

// copyResourceInfo returns a deep copy of a ResourceInfo, including the LastModified pointer.
func copyResourceInfo(ri *ResourceInfo) *ResourceInfo {
	if ri == nil {
		return nil
	}

	cp := &ResourceInfo{
		ResourceArn:                  ri.ResourceArn,
		RoleArn:                      ri.RoleArn,
		ExpectedResourceOwnerAccount: ri.ExpectedResourceOwnerAccount,
		VerificationStatus:           ri.VerificationStatus,
		HybridAccessEnabled:          ri.HybridAccessEnabled,
		WithFederation:               ri.WithFederation,
		WithPrivilegedAccess:         ri.WithPrivilegedAccess,
	}

	if ri.LastModified != nil {
		t := *ri.LastModified
		cp.LastModified = &t
	}

	return cp
}

// UpdateResource updates the role ARN of an already registered resource.
func (b *InMemoryBackend) UpdateResource(resourceArn, roleArn string, opts RegisterResourceOptions) error {
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
	info.WithFederation = opts.WithFederation
	info.HybridAccessEnabled = opts.HybridAccessEnabled

	if opts.ExpectedResourceOwnerAccount != "" {
		info.ExpectedResourceOwnerAccount = opts.ExpectedResourceOwnerAccount
	}

	now := time.Now()
	info.LastModified = &now

	return nil
}
