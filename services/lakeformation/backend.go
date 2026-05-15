package lakeformation

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	transactionStatusActive    = "ACTIVE"
	transactionStatusCommitted = "COMMITTED"
	transactionStatusAborted   = "ABORTED"
)

// ErrValidation is returned when input validation fails.
var ErrValidation = errors.New("validation error")

// StorageBackend is the interface for Lake Formation backend operations.
type StorageBackend interface {
	Reset()

	GetDataLakeSettings() *DataLakeSettings
	PutDataLakeSettings(settings *DataLakeSettings)

	RegisterResource(resourceArn, roleArn string) error
	UpdateResource(resourceArn, roleArn string) error
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

	AddLFTagsToResource(catalogID string, resource *Resource, lfTags []LFTagPair) []LFTagError
	RemoveLFTagsFromResource(catalogID string, resource *Resource, lfTags []LFTagPair) []LFTagError
	GetResourceLFTags(catalogID string, resource *Resource) ([]LFTagPair, error)

	AssumeDecoratedRoleWithSAML(
		principalArn, roleArn, samlAssertion string,
		durationSeconds *int32,
	) *SAMLCredentials

	StartTransaction() string
	CancelTransaction(transactionID string) error
	CommitTransaction(transactionID string) (string, error)
	DescribeTransaction(transactionID string) (*Transaction, error)
	ListTransactions(statusFilter string, maxResults int, nextToken string) ([]*Transaction, string)

	CreateDataCellsFilter(filter *DataCellsFilter) error
	DeleteDataCellsFilter(tableCatalogID, databaseName, tableName, name string) error
	ListDataCellsFilter(
		tableCatalogID, databaseName, tableName string,
		maxResults int,
		nextToken string,
	) ([]*DataCellsFilter, string)

	CreateLFTagExpression(name, description, catalogID string, expression []LFTag) error
	DeleteLFTagExpression(name, catalogID string) error
	ListLFTagExpressions(catalogID string, maxResults int, nextToken string) ([]*LFTagExpression, string)

	CreateLakeFormationIdentityCenterConfiguration(catalogID, instanceArn string) string
	DeleteLakeFormationIdentityCenterConfiguration(catalogID string) error
	DescribeLakeFormationIdentityCenterConfiguration(catalogID string) (*IdentityCenterConfiguration, error)
	UpdateLakeFormationIdentityCenterConfiguration(
		catalogID string,
		externalFiltering *ExternalFilteringConfiguration,
		appStatus string,
	) error

	CreateLakeFormationOptIn(principal *DataLakePrincipal, resource *Resource) error
	DeleteLakeFormationOptIn(principal *DataLakePrincipal, resource *Resource) error
	ListLakeFormationOptIns(principalIdentifier string, maxResults int, nextToken string) ([]*LFOptIn, string)

	GetDataLakePrincipal() *DataLakePrincipal

	ExtendTransaction(transactionID string) error
	DeleteObjectsOnCancel(transactionID string) error

	GetDataCellsFilter(tableCatalogID, databaseName, tableName, name string) (*DataCellsFilter, error)
	UpdateDataCellsFilter(filter *DataCellsFilter) error

	GetLFTagExpression(name, catalogID string) (*LFTagExpression, error)
	UpdateLFTagExpression(name, catalogID, description string, expression []LFTag) error

	GetEffectivePermissionsForPath(resourceArn string, maxResults int, nextToken string) ([]*PermissionEntry, string)

	GetTemporaryCredentials(durationSeconds *int32) *TemporaryCredentials

	GetTableObjects(maxResults int, nextToken string) ([]PartitionedTableObjectsList, string)
	UpdateTableObjects(transactionID string) error

	StartQueryPlanning(queryString string) string
	GetQueryState(queryID string) (string, error)
	GetQueryStatistics(queryID string) (*ExecutionStatistics, *PlanningStatistics, error)
	GetWorkUnits(queryID string) ([]WorkUnitRange, string, error)
	GetWorkUnitResults(queryID, workUnitToken string) error

	ListTableStorageOptimizers(catalogID, databaseName, tableName, storageOptimizerType string) []StorageOptimizer
	UpdateTableStorageOptimizer(catalogID, databaseName, tableName string, config map[string]map[string]string) string

	SearchDatabasesByLFTags(
		expression []LFTag, catalogID string, maxResults int, nextToken string,
	) ([]TaggedDatabase, string)
	SearchTablesByLFTags(
		expression []LFTag, catalogID string, maxResults int, nextToken string,
	) ([]TaggedTable, string)
}

// lfTagKey uniquely identifies a LF tag by catalog and key.
type lfTagKey struct {
	CatalogID string
	TagKey    string
}

// dataCellsFilterKey uniquely identifies a DataCellsFilter.
type dataCellsFilterKey struct {
	TableCatalogID string
	DatabaseName   string
	TableName      string
	Name           string
}

// lfTagExpressionKey uniquely identifies an LF-tag expression.
type lfTagExpressionKey struct {
	CatalogID string
	Name      string
}

// InMemoryBackend is the in-memory backend for Lake Formation.
type InMemoryBackend struct {
	identityCenterConfigs  map[string]*IdentityCenterConfiguration
	resources              map[string]*ResourceInfo
	lfTags                 map[lfTagKey]*LFTag
	transactions           map[string]string
	dataCellsFilters       map[dataCellsFilterKey]*DataCellsFilter
	lfTagExpressions       map[lfTagExpressionKey]*LFTagExpression
	dataLakeSettings       *DataLakeSettings
	resourceLFTags         map[string][]LFTagPair
	mu                     *lockmetrics.RWMutex
	queries                map[string]string
	tableStorageOptimizers map[string][]StorageOptimizer
	lakeFormationOptIns    []*LFOptIn
	permissions            []*PermissionEntry
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates a new in-memory Lake Formation backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		dataLakeSettings:       &DataLakeSettings{},
		resources:              make(map[string]*ResourceInfo),
		permissions:            make([]*PermissionEntry, 0),
		lfTags:                 make(map[lfTagKey]*LFTag),
		transactions:           make(map[string]string),
		dataCellsFilters:       make(map[dataCellsFilterKey]*DataCellsFilter),
		lfTagExpressions:       make(map[lfTagExpressionKey]*LFTagExpression),
		identityCenterConfigs:  make(map[string]*IdentityCenterConfiguration),
		lakeFormationOptIns:    make([]*LFOptIn, 0),
		resourceLFTags:         make(map[string][]LFTagPair),
		queries:                make(map[string]string),
		tableStorageOptimizers: make(map[string][]StorageOptimizer),
		mu:                     lockmetrics.New("lakeformation"),
	}
}

// Reset restores the backend to a clean initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.dataLakeSettings = &DataLakeSettings{}
	b.resources = make(map[string]*ResourceInfo)
	b.permissions = make([]*PermissionEntry, 0)
	b.lfTags = make(map[lfTagKey]*LFTag)
	b.transactions = make(map[string]string)
	b.dataCellsFilters = make(map[dataCellsFilterKey]*DataCellsFilter)
	b.lfTagExpressions = make(map[lfTagExpressionKey]*LFTagExpression)
	b.identityCenterConfigs = make(map[string]*IdentityCenterConfiguration)
	b.lakeFormationOptIns = make([]*LFOptIn, 0)
	b.resourceLFTags = make(map[string][]LFTagPair)
	b.queries = make(map[string]string)
	b.tableStorageOptimizers = make(map[string][]StorageOptimizer)
}

// AddLFTagInternal seeds an LF-tag directly for testing.
func (b *InMemoryBackend) AddLFTagInternal(catalogID, tagKey string, tagValues []string) {
	b.mu.Lock("AddLFTagInternal")
	defer b.mu.Unlock()

	vals := make([]string, len(tagValues))
	copy(vals, tagValues)

	b.lfTags[lfTagKey{CatalogID: catalogID, TagKey: tagKey}] = &LFTag{
		CatalogID: catalogID,
		TagKey:    tagKey,
		TagValues: vals,
	}
}

// AddResourceInternal seeds a registered resource directly for testing.
func (b *InMemoryBackend) AddResourceInternal(resourceArn, roleArn string) {
	b.mu.Lock("AddResourceInternal")
	defer b.mu.Unlock()

	now := time.Now()
	b.resources[resourceArn] = &ResourceInfo{
		ResourceArn:  resourceArn,
		RoleArn:      roleArn,
		LastModified: &now,
	}
}

// AddPermissionInternal seeds a permission entry directly for testing.
func (b *InMemoryBackend) AddPermissionInternal(entry *PermissionEntry) {
	b.mu.Lock("AddPermissionInternal")
	defer b.mu.Unlock()

	b.permissions = append(b.permissions, entry)
}

// AddDataCellsFilterInternal seeds a DataCellsFilter directly for testing.
func (b *InMemoryBackend) AddDataCellsFilterInternal(filter *DataCellsFilter) {
	b.mu.Lock("AddDataCellsFilterInternal")
	defer b.mu.Unlock()

	k := dataCellsFilterKey{
		TableCatalogID: filter.TableCatalogID,
		DatabaseName:   filter.DatabaseName,
		TableName:      filter.TableName,
		Name:           filter.Name,
	}

	cp := *filter
	b.dataCellsFilters[k] = &cp
}

// AddLFTagExpressionInternal seeds an LFTagExpression directly for testing.
func (b *InMemoryBackend) AddLFTagExpressionInternal(expr *LFTagExpression) {
	b.mu.Lock("AddLFTagExpressionInternal")
	defer b.mu.Unlock()

	k := lfTagExpressionKey{CatalogID: expr.CatalogID, Name: expr.Name}

	cp := *expr

	if expr.Expression != nil {
		cp.Expression = make([]LFTag, len(expr.Expression))
		copy(cp.Expression, expr.Expression)
	}

	b.lfTagExpressions[k] = &cp
}

// GetDataLakeSettings returns the current data lake settings.
func (b *InMemoryBackend) GetDataLakeSettings() *DataLakeSettings {
	b.mu.RLock("GetDataLakeSettings")
	defer b.mu.RUnlock()

	if b.dataLakeSettings == nil {
		return &DataLakeSettings{}
	}

	return copyDataLakeSettings(b.dataLakeSettings)
}

// PutDataLakeSettings replaces the data lake settings.
func (b *InMemoryBackend) PutDataLakeSettings(settings *DataLakeSettings) {
	b.mu.Lock("PutDataLakeSettings")
	defer b.mu.Unlock()

	b.dataLakeSettings = copyDataLakeSettings(settings)
}

// RegisterResource registers an S3 location as a data lake resource.
func (b *InMemoryBackend) RegisterResource(resourceArn, roleArn string) error {
	if resourceArn == "" {
		return fmt.Errorf("ResourceArn is required: %w", ErrValidation)
	}

	b.mu.Lock("RegisterResource")
	defer b.mu.Unlock()

	if _, ok := b.resources[resourceArn]; ok {
		return awserr.New(
			"resource already registered: "+resourceArn,
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

// DeregisterResource removes a registered data lake resource and its associated permissions.
func (b *InMemoryBackend) DeregisterResource(resourceArn string) error {
	if resourceArn == "" {
		return fmt.Errorf("ResourceArn is required: %w", ErrValidation)
	}

	b.mu.Lock("DeregisterResource")
	defer b.mu.Unlock()

	if _, ok := b.resources[resourceArn]; !ok {
		return awserr.New(
			"resource not found: "+resourceArn,
			awserr.ErrNotFound,
		)
	}

	delete(b.resources, resourceArn)

	// Clean up all permissions associated with this resource.
	updated := make([]*PermissionEntry, 0, len(b.permissions))
	for _, p := range b.permissions {
		if !permissionMatchesARN(p, resourceArn) {
			updated = append(updated, p)
		}
	}
	b.permissions = updated

	return nil
}

// DescribeResource returns information about a registered resource.
func (b *InMemoryBackend) DescribeResource(resourceArn string) (*ResourceInfo, error) {
	if resourceArn == "" {
		return nil, fmt.Errorf("ResourceArn is required: %w", ErrValidation)
	}

	b.mu.RLock("DescribeResource")
	defer b.mu.RUnlock()

	info, ok := b.resources[resourceArn]
	if !ok {
		return nil, awserr.New(
			"resource not found: "+resourceArn,
			awserr.ErrNotFound,
		)
	}

	return copyResourceInfo(info), nil
}

const defaultMaxResults = 100

// ListResources returns a paginated list of registered resources.
func (b *InMemoryBackend) ListResources(maxResults int, nextToken string) ([]*ResourceInfo, string) {
	b.mu.RLock("ListResources")
	defer b.mu.RUnlock()

	all := make([]*ResourceInfo, 0, len(b.resources))
	for _, v := range b.resources {
		all = append(all, copyResourceInfo(v))
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].ResourceArn < all[j].ResourceArn
	})

	return paginate(all, maxResults, nextToken, defaultMaxResults)
}

// GrantPermissions adds a permission entry.
func (b *InMemoryBackend) GrantPermissions(entry *PermissionEntry) error {
	if entry == nil {
		return fmt.Errorf("entry is required: %w", ErrValidation)
	}

	if entry.Principal == nil {
		return fmt.Errorf("principal is required: %w", ErrValidation)
	}

	if entry.Resource == nil {
		return fmt.Errorf("resource is required: %w", ErrValidation)
	}

	b.mu.Lock("GrantPermissions")
	defer b.mu.Unlock()

	b.permissions = append(b.permissions, entry)

	return nil
}

// RevokePermissions removes a matching permission entry.
func (b *InMemoryBackend) RevokePermissions(entry *PermissionEntry) error {
	if entry == nil {
		return fmt.Errorf("entry is required: %w", ErrValidation)
	}

	b.mu.Lock("RevokePermissions")
	defer b.mu.Unlock()

	updated := make([]*PermissionEntry, 0, len(b.permissions))

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
	b.mu.RLock("ListPermissions")
	defer b.mu.RUnlock()

	filtered := make([]*PermissionEntry, 0, len(b.permissions))

	for _, p := range b.permissions {
		if resourceArn == "" || permissionMatchesARN(p, resourceArn) {
			cp := deepCopyPermissionEntry(p)
			filtered = append(filtered, cp)
		}
	}

	// Sort deterministically by principal identifier then by resource key.
	sort.Slice(filtered, func(i, j int) bool {
		pi := principalID(filtered[i].Principal)
		pj := principalID(filtered[j].Principal)
		if pi != pj {
			return pi < pj
		}

		return resourceToKey(filtered[i].Resource) < resourceToKey(filtered[j].Resource)
	})

	return paginate(filtered, maxResults, nextToken, defaultMaxResults)
}

// CreateLFTag creates a new LF tag with the given values.
func (b *InMemoryBackend) CreateLFTag(catalogID, tagKey string, tagValues []string) error {
	if tagKey == "" {
		return fmt.Errorf("TagKey is required: %w", ErrValidation)
	}

	if len(tagValues) == 0 {
		return fmt.Errorf("TagValues must not be empty: %w", ErrValidation)
	}

	b.mu.Lock("CreateLFTag")
	defer b.mu.Unlock()

	k := lfTagKey{CatalogID: catalogID, TagKey: tagKey}

	if _, ok := b.lfTags[k]; ok {
		return awserr.New(
			"LF tag already exists: "+tagKey,
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
	if tagKey == "" {
		return fmt.Errorf("TagKey is required: %w", ErrValidation)
	}

	b.mu.Lock("DeleteLFTag")
	defer b.mu.Unlock()

	k := lfTagKey{CatalogID: catalogID, TagKey: tagKey}

	if _, ok := b.lfTags[k]; !ok {
		return awserr.New(
			"LF tag not found: "+tagKey,
			awserr.ErrNotFound,
		)
	}

	delete(b.lfTags, k)

	return nil
}

// GetLFTag returns the LF tag for the given catalog and key.
func (b *InMemoryBackend) GetLFTag(catalogID, tagKey string) (*LFTag, error) {
	if tagKey == "" {
		return nil, fmt.Errorf("TagKey is required: %w", ErrValidation)
	}

	b.mu.RLock("GetLFTag")
	defer b.mu.RUnlock()

	k := lfTagKey{CatalogID: catalogID, TagKey: tagKey}

	tag, ok := b.lfTags[k]
	if !ok {
		return nil, awserr.New(
			"LF tag not found: "+tagKey,
			awserr.ErrNotFound,
		)
	}

	return copyLFTag(tag), nil
}

// UpdateLFTag adds and removes values from an existing LF tag.
// TagValues are sorted after modification for deterministic output.
func (b *InMemoryBackend) UpdateLFTag(catalogID, tagKey string, tagValuesToAdd, tagValuesToDelete []string) error {
	if tagKey == "" {
		return fmt.Errorf("TagKey is required: %w", ErrValidation)
	}

	b.mu.Lock("UpdateLFTag")
	defer b.mu.Unlock()

	k := lfTagKey{CatalogID: catalogID, TagKey: tagKey}

	tag, ok := b.lfTags[k]
	if !ok {
		return awserr.New(
			"LF tag not found: "+tagKey,
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

	sort.Strings(vals)
	tag.TagValues = vals

	return nil
}

// ListLFTags returns a paginated list of LF tags for the given catalog.
func (b *InMemoryBackend) ListLFTags(catalogID string, maxResults int, nextToken string) ([]*LFTag, string) {
	b.mu.RLock("ListLFTags")
	defer b.mu.RUnlock()

	all := make([]*LFTag, 0, len(b.lfTags))

	for k, t := range b.lfTags {
		if catalogID == "" || k.CatalogID == catalogID {
			all = append(all, copyLFTag(t))
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
			errCode := "InternalServiceException"
			if errors.Is(err, ErrValidation) {
				errCode = "InvalidInputException"
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

	for _, e := range entries {
		if err := b.RevokePermissions(e); err != nil {
			errCode := "InternalServiceException"
			if errors.Is(err, ErrValidation) {
				errCode = "InvalidInputException"
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

// copyDataLakeSettings returns a deep copy of the DataLakeSettings.
func copyDataLakeSettings(s *DataLakeSettings) *DataLakeSettings {
	if s == nil {
		return nil
	}

	cp := &DataLakeSettings{}

	if s.DataLakeAdmins != nil {
		cp.DataLakeAdmins = make([]DataLakePrincipal, len(s.DataLakeAdmins))
		copy(cp.DataLakeAdmins, s.DataLakeAdmins)
	}

	if s.CreateDatabaseDefaultPermissions != nil {
		cp.CreateDatabaseDefaultPermissions = copyPrincipalPermissions(s.CreateDatabaseDefaultPermissions)
	}

	if s.CreateTableDefaultPermissions != nil {
		cp.CreateTableDefaultPermissions = copyPrincipalPermissions(s.CreateTableDefaultPermissions)
	}

	if s.TrustedResourceOwners != nil {
		cp.TrustedResourceOwners = make([]string, len(s.TrustedResourceOwners))
		copy(cp.TrustedResourceOwners, s.TrustedResourceOwners)
	}

	return cp
}

// copyPrincipalPermissions returns a deep copy of a []PrincipalPermissions slice,
// copying the Permissions []string slice and cloning the Principal pointer for each element.
func copyPrincipalPermissions(src []PrincipalPermissions) []PrincipalPermissions {
	dst := make([]PrincipalPermissions, len(src))

	for i, pp := range src {
		elem := PrincipalPermissions{}

		if pp.Principal != nil {
			p := *pp.Principal
			elem.Principal = &p
		}

		if pp.Permissions != nil {
			elem.Permissions = make([]string, len(pp.Permissions))
			copy(elem.Permissions, pp.Permissions)
		}

		dst[i] = elem
	}

	return dst
}

// copyLFTag returns a deep copy of the LFTag, including a copy of TagValues.
func copyLFTag(t *LFTag) *LFTag {
	if t == nil {
		return nil
	}

	cp := *t

	if t.TagValues != nil {
		cp.TagValues = make([]string, len(t.TagValues))
		copy(cp.TagValues, t.TagValues)
	}

	return &cp
}

// AddLFTagsToResource attaches LF-tags to the specified resource.
// Valid tags are always stored; failures are returned for any tag not found.
// This mirrors AWS behavior where valid tags are applied even if some fail.
func (b *InMemoryBackend) AddLFTagsToResource(catalogID string, resource *Resource, lfTags []LFTagPair) []LFTagError {
	b.mu.Lock("AddLFTagsToResource")
	defer b.mu.Unlock()

	failures := make([]LFTagError, 0, len(lfTags))
	resourceKey := resourceToKey(resource)
	existing := b.resourceLFTags[resourceKey]

	for _, pair := range lfTags {
		k := lfTagKey{CatalogID: catalogID, TagKey: pair.TagKey}
		if _, ok := b.lfTags[k]; !ok {
			failures = append(failures, LFTagError{
				LFTag: &pair,
				Error: &errorDetail{
					ErrorCode:    "EntityNotFoundException",
					ErrorMessage: "LF tag not found: " + pair.TagKey,
				},
			})

			continue
		}

		// Store the valid tag association.
		found := false

		for i, ex := range existing {
			if ex.TagKey == pair.TagKey {
				existing[i] = pair
				found = true

				break
			}
		}

		if !found {
			existing = append(existing, pair)
		}
	}

	b.resourceLFTags[resourceKey] = existing

	return failures
}

// AssumeDecoratedRoleWithSAML returns synthetic temporary credentials.
// The actual SAML assertion and role are not validated in the in-memory backend.
func (b *InMemoryBackend) AssumeDecoratedRoleWithSAML(
	_, _, _ string,
	_ *int32,
) *SAMLCredentials {
	return &SAMLCredentials{
		AccessKeyID:     "ASIALAKEFORMATION0001",
		SecretAccessKey: "syntheticSecretKey00000000000000000000000",
		SessionToken:    "syntheticSessionToken",
		Expiration:      time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
	}
}

// CancelTransaction cancels an in-flight transaction.
// Returns an error if the transaction is already committed.
func (b *InMemoryBackend) CancelTransaction(transactionID string) error {
	b.mu.Lock("CancelTransaction")
	defer b.mu.Unlock()

	if status, ok := b.transactions[transactionID]; ok && status == transactionStatusCommitted {
		return awserr.New(
			fmt.Sprintf("transaction %s is already committed", transactionID),
			awserr.ErrConflict,
		)
	}

	b.transactions[transactionID] = transactionStatusAborted

	return nil
}

// CommitTransaction commits an in-flight transaction.
// Returns an error if the transaction is already aborted.
func (b *InMemoryBackend) CommitTransaction(transactionID string) (string, error) {
	b.mu.Lock("CommitTransaction")
	defer b.mu.Unlock()

	if status, ok := b.transactions[transactionID]; ok && status == transactionStatusAborted {
		return "", awserr.New(
			fmt.Sprintf("transaction %s has been cancelled", transactionID),
			awserr.ErrConflict,
		)
	}

	b.transactions[transactionID] = transactionStatusCommitted

	return transactionStatusCommitted, nil
}

// CreateDataCellsFilter stores a new data cells filter.
func (b *InMemoryBackend) CreateDataCellsFilter(filter *DataCellsFilter) error {
	if filter == nil {
		return fmt.Errorf("filter is required: %w", ErrValidation)
	}

	if strings.TrimSpace(filter.TableCatalogID) == "" {
		return fmt.Errorf("TableCatalogId is required: %w", ErrValidation)
	}

	if strings.TrimSpace(filter.DatabaseName) == "" {
		return fmt.Errorf("DatabaseName is required: %w", ErrValidation)
	}

	if strings.TrimSpace(filter.TableName) == "" {
		return fmt.Errorf("TableName is required: %w", ErrValidation)
	}

	if strings.TrimSpace(filter.Name) == "" {
		return fmt.Errorf("Name is required: %w", ErrValidation)
	}

	b.mu.Lock("CreateDataCellsFilter")
	defer b.mu.Unlock()

	k := dataCellsFilterKey{
		TableCatalogID: filter.TableCatalogID,
		DatabaseName:   filter.DatabaseName,
		TableName:      filter.TableName,
		Name:           filter.Name,
	}

	if _, ok := b.dataCellsFilters[k]; ok {
		return awserr.New(
			"data cells filter already exists: "+filter.Name,
			awserr.ErrAlreadyExists,
		)
	}

	cp := *filter
	b.dataCellsFilters[k] = &cp

	return nil
}

// CreateLFTagExpression stores a new named LF-tag expression.
func (b *InMemoryBackend) CreateLFTagExpression(name, description, catalogID string, expression []LFTag) error {
	b.mu.Lock("CreateLFTagExpression")
	defer b.mu.Unlock()

	k := lfTagExpressionKey{CatalogID: catalogID, Name: name}

	// Validate each tag in the expression has a TagKey.
	for i, tag := range expression {
		if strings.TrimSpace(tag.TagKey) == "" {
			return fmt.Errorf("Expression[%d].TagKey is required: %w", i, ErrValidation)
		}
	}

	if _, ok := b.lfTagExpressions[k]; ok {
		return awserr.New(
			"LF-tag expression already exists: "+name,
			awserr.ErrAlreadyExists,
		)
	}

	expr := make([]LFTag, len(expression))
	copy(expr, expression)

	b.lfTagExpressions[k] = &LFTagExpression{
		Name:        name,
		Description: description,
		CatalogID:   catalogID,
		Expression:  expr,
	}

	return nil
}

// CreateLakeFormationIdentityCenterConfiguration creates or replaces the IAM Identity Center
// integration for the given catalog and returns a synthetic application ARN.
func (b *InMemoryBackend) CreateLakeFormationIdentityCenterConfiguration(catalogID, instanceArn string) string {
	b.mu.Lock("CreateLakeFormationIdentityCenterConfiguration")
	defer b.mu.Unlock()

	appArn := fmt.Sprintf(
		"arn:aws:sso::%s:application/ssoins-0000000000000000/apl-%s",
		catalogID,
		catalogID,
	)

	b.identityCenterConfigs[catalogID] = &IdentityCenterConfiguration{
		CatalogID:      catalogID,
		InstanceArn:    instanceArn,
		ApplicationArn: appArn,
	}

	return appArn
}

// CreateLakeFormationOptIn adds an opt-in enforcement entry for a principal and resource.
func (b *InMemoryBackend) CreateLakeFormationOptIn(principal *DataLakePrincipal, resource *Resource) error {
	b.mu.Lock("CreateLakeFormationOptIn")
	defer b.mu.Unlock()

	for _, o := range b.lakeFormationOptIns {
		if principalEqual(o.Principal, principal) && resourceEqual(o.Resource, resource) {
			return awserr.New("opt-in already exists for this principal and resource", awserr.ErrAlreadyExists)
		}
	}

	b.lakeFormationOptIns = append(b.lakeFormationOptIns, &LFOptIn{
		Principal: principal,
		Resource:  resource,
	})

	return nil
}

// DeleteDataCellsFilter removes the named data cells filter.
func (b *InMemoryBackend) DeleteDataCellsFilter(tableCatalogID, databaseName, tableName, name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("Name is required: %w", ErrValidation)
	}

	b.mu.Lock("DeleteDataCellsFilter")
	defer b.mu.Unlock()

	k := dataCellsFilterKey{
		TableCatalogID: tableCatalogID,
		DatabaseName:   databaseName,
		TableName:      tableName,
		Name:           name,
	}

	if _, ok := b.dataCellsFilters[k]; !ok {
		return awserr.New(
			"data cells filter not found: "+name,
			awserr.ErrNotFound,
		)
	}

	delete(b.dataCellsFilters, k)

	return nil
}

// DeleteLFTagExpression removes the named LF-tag expression.
func (b *InMemoryBackend) DeleteLFTagExpression(name, catalogID string) error {
	b.mu.Lock("DeleteLFTagExpression")
	defer b.mu.Unlock()

	k := lfTagExpressionKey{CatalogID: catalogID, Name: name}

	if _, ok := b.lfTagExpressions[k]; !ok {
		return awserr.New(
			"LF-tag expression not found: "+name,
			awserr.ErrNotFound,
		)
	}

	delete(b.lfTagExpressions, k)

	return nil
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

// transactionIDBytesLen is the number of random bytes used for transaction IDs.
const transactionIDBytesLen = 16

// newTransactionID generates a random hex transaction ID.
func newTransactionID() string {
	b := make([]byte, transactionIDBytesLen)

	if _, err := rand.Read(b); err != nil {
		// Fallback: use time-based ID (practically unreachable).
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	return hex.EncodeToString(b)
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

	info, ok := b.resources[resourceArn]
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

// StartTransaction begins a new in-flight transaction and returns its ID.
func (b *InMemoryBackend) StartTransaction() string {
	id := newTransactionID()

	b.mu.Lock("StartTransaction")
	defer b.mu.Unlock()

	b.transactions[id] = transactionStatusActive

	return id
}

// DescribeTransaction returns the status of a specific transaction.
func (b *InMemoryBackend) DescribeTransaction(transactionID string) (*Transaction, error) {
	if strings.TrimSpace(transactionID) == "" {
		return nil, fmt.Errorf("TransactionId is required: %w", ErrValidation)
	}

	b.mu.RLock("DescribeTransaction")
	defer b.mu.RUnlock()

	status, ok := b.transactions[transactionID]
	if !ok {
		return nil, awserr.New(
			"transaction not found: "+transactionID,
			awserr.ErrNotFound,
		)
	}

	return &Transaction{TransactionID: transactionID, TransactionStatus: status}, nil
}

// ListTransactions returns a paginated list of transactions, optionally filtered by status.
func (b *InMemoryBackend) ListTransactions(
	statusFilter string, maxResults int, nextToken string,
) ([]*Transaction, string) {
	b.mu.RLock("ListTransactions")
	defer b.mu.RUnlock()

	all := make([]*Transaction, 0, len(b.transactions))

	for id, status := range b.transactions {
		if statusFilter != "" && status != statusFilter {
			continue
		}

		all = append(all, &Transaction{TransactionID: id, TransactionStatus: status})
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].TransactionID < all[j].TransactionID
	})

	return paginate(all, maxResults, nextToken, defaultMaxResults)
}

// RemoveLFTagsFromResource detaches LF-tags from the specified resource.
// Failures are returned for any tag not currently attached to the resource.
func (b *InMemoryBackend) RemoveLFTagsFromResource(
	_ string,
	resource *Resource,
	lfTags []LFTagPair,
) []LFTagError {
	b.mu.Lock("RemoveLFTagsFromResource")
	defer b.mu.Unlock()

	failures := make([]LFTagError, 0, len(lfTags))
	resourceKey := resourceToKey(resource)
	existing := b.resourceLFTags[resourceKey]

	for _, pair := range lfTags {
		found := false

		for _, ex := range existing {
			if ex.TagKey == pair.TagKey {
				found = true

				break
			}
		}

		if !found {
			pair := pair
			failures = append(failures, LFTagError{
				LFTag: &pair,
				Error: &errorDetail{
					ErrorCode:    "EntityNotFoundException",
					ErrorMessage: "LF tag not attached to resource: " + pair.TagKey,
				},
			})

			continue
		}

		existing = slices.DeleteFunc(existing, func(e LFTagPair) bool {
			return e.TagKey == pair.TagKey
		})
	}

	if len(existing) == 0 {
		delete(b.resourceLFTags, resourceKey)
	} else {
		b.resourceLFTags[resourceKey] = existing
	}

	return failures
}

// GetResourceLFTags returns the LF-tags currently attached to a resource.
func (b *InMemoryBackend) GetResourceLFTags(_ string, resource *Resource) ([]LFTagPair, error) {
	if resource == nil {
		return nil, fmt.Errorf("resource is required: %w", ErrValidation)
	}

	b.mu.RLock("GetResourceLFTags")
	defer b.mu.RUnlock()

	resourceKey := resourceToKey(resource)
	pairs := b.resourceLFTags[resourceKey]

	result := make([]LFTagPair, len(pairs))
	copy(result, pairs)

	return result, nil
}

// GetDataLakePrincipal returns a synthetic caller-identity principal.
// In a real deployment, this returns the ARN of the calling IAM entity.
func (b *InMemoryBackend) GetDataLakePrincipal() *DataLakePrincipal {
	return &DataLakePrincipal{
		DataLakePrincipalIdentifier: "arn:aws:iam::000000000000:user/gopherstack-user",
	}
}

// ListDataCellsFilter returns a paginated list of data cells filters.
// Optional tableCatalogID, databaseName, and tableName act as filters.
func (b *InMemoryBackend) ListDataCellsFilter(
	tableCatalogID, databaseName, tableName string,
	maxResults int,
	nextToken string,
) ([]*DataCellsFilter, string) {
	b.mu.RLock("ListDataCellsFilter")
	defer b.mu.RUnlock()

	all := make([]*DataCellsFilter, 0, len(b.dataCellsFilters))

	for _, v := range b.dataCellsFilters {
		if tableCatalogID != "" && v.TableCatalogID != tableCatalogID {
			continue
		}

		if databaseName != "" && v.DatabaseName != databaseName {
			continue
		}

		if tableName != "" && v.TableName != tableName {
			continue
		}

		cp := *v
		all = append(all, &cp)
	}

	sort.Slice(all, func(i, j int) bool {
		ki := all[i].TableCatalogID + "|" + all[i].DatabaseName + "|" + all[i].TableName + "|" + all[i].Name
		kj := all[j].TableCatalogID + "|" + all[j].DatabaseName + "|" + all[j].TableName + "|" + all[j].Name

		return ki < kj
	})

	return paginate(all, maxResults, nextToken, defaultMaxResults)
}

// ListLFTagExpressions returns a paginated list of LF-tag expressions for the given catalog.
func (b *InMemoryBackend) ListLFTagExpressions(
	catalogID string,
	maxResults int,
	nextToken string,
) ([]*LFTagExpression, string) {
	b.mu.RLock("ListLFTagExpressions")
	defer b.mu.RUnlock()

	all := make([]*LFTagExpression, 0, len(b.lfTagExpressions))

	for k, v := range b.lfTagExpressions {
		if catalogID != "" && k.CatalogID != catalogID {
			continue
		}

		expr := *v

		if v.Expression != nil {
			expr.Expression = make([]LFTag, len(v.Expression))
			copy(expr.Expression, v.Expression)
		}

		all = append(all, &expr)
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].CatalogID != all[j].CatalogID {
			return all[i].CatalogID < all[j].CatalogID
		}

		return all[i].Name < all[j].Name
	})

	return paginate(all, maxResults, nextToken, defaultMaxResults)
}

// DeleteLakeFormationOptIn removes an opt-in enforcement entry for a principal and resource.
func (b *InMemoryBackend) DeleteLakeFormationOptIn(principal *DataLakePrincipal, resource *Resource) error {
	if principal == nil {
		return fmt.Errorf("principal is required: %w", ErrValidation)
	}

	if resource == nil {
		return fmt.Errorf("resource is required: %w", ErrValidation)
	}

	b.mu.Lock("DeleteLakeFormationOptIn")
	defer b.mu.Unlock()

	updated := make([]*LFOptIn, 0, len(b.lakeFormationOptIns))
	found := false

	for _, o := range b.lakeFormationOptIns {
		if principalEqual(o.Principal, principal) && resourceEqual(o.Resource, resource) {
			found = true

			continue
		}

		updated = append(updated, o)
	}

	if !found {
		return awserr.New("opt-in not found for this principal and resource", awserr.ErrNotFound)
	}

	b.lakeFormationOptIns = updated

	return nil
}

// ListLakeFormationOptIns returns a paginated list of opt-in entries.
// Optional principalIdentifier acts as a filter.
func (b *InMemoryBackend) ListLakeFormationOptIns(
	principalIdentifier string,
	maxResults int,
	nextToken string,
) ([]*LFOptIn, string) {
	b.mu.RLock("ListLakeFormationOptIns")
	defer b.mu.RUnlock()

	all := make([]*LFOptIn, 0, len(b.lakeFormationOptIns))

	for _, o := range b.lakeFormationOptIns {
		if principalIdentifier != "" && principalID(o.Principal) != principalIdentifier {
			continue
		}

		cp := &LFOptIn{}

		if o.Principal != nil {
			p := *o.Principal
			cp.Principal = &p
		}

		if o.Resource != nil {
			cp.Resource = copyResource(o.Resource)
		}

		all = append(all, cp)
	}

	sort.Slice(all, func(i, j int) bool {
		pi := principalID(all[i].Principal)
		pj := principalID(all[j].Principal)
		if pi != pj {
			return pi < pj
		}

		return resourceToKey(all[i].Resource) < resourceToKey(all[j].Resource)
	})

	return paginate(all, maxResults, nextToken, defaultMaxResults)
}

// DeleteLakeFormationIdentityCenterConfiguration removes the identity center config for a catalog.
func (b *InMemoryBackend) DeleteLakeFormationIdentityCenterConfiguration(catalogID string) error {
	b.mu.Lock("DeleteLakeFormationIdentityCenterConfiguration")
	defer b.mu.Unlock()
	if _, ok := b.identityCenterConfigs[catalogID]; !ok {
		return awserr.New("identity center configuration not found for catalog: "+catalogID, awserr.ErrNotFound)
	}
	delete(b.identityCenterConfigs, catalogID)

	return nil
}

// DescribeLakeFormationIdentityCenterConfiguration returns the identity center config for a catalog.
func (b *InMemoryBackend) DescribeLakeFormationIdentityCenterConfiguration(
	catalogID string,
) (*IdentityCenterConfiguration, error) {
	b.mu.RLock("DescribeLakeFormationIdentityCenterConfiguration")
	defer b.mu.RUnlock()
	cfg, ok := b.identityCenterConfigs[catalogID]
	if !ok {
		return nil, awserr.New("identity center configuration not found for catalog: "+catalogID, awserr.ErrNotFound)
	}
	cp := *cfg

	return &cp, nil
}

// UpdateLakeFormationIdentityCenterConfiguration updates or creates the identity center config.
func (b *InMemoryBackend) UpdateLakeFormationIdentityCenterConfiguration(
	catalogID string, externalFiltering *ExternalFilteringConfiguration, _ string,
) error {
	b.mu.Lock("UpdateLakeFormationIdentityCenterConfiguration")
	defer b.mu.Unlock()
	cfg, ok := b.identityCenterConfigs[catalogID]
	if !ok {
		b.identityCenterConfigs[catalogID] = &IdentityCenterConfiguration{
			CatalogID:         catalogID,
			ExternalFiltering: externalFiltering,
		}

		return nil
	}
	if externalFiltering != nil {
		cfg.ExternalFiltering = externalFiltering
	}

	return nil
}

// ExtendTransaction validates that a transaction is active (no-op extension in-memory).
func (b *InMemoryBackend) ExtendTransaction(transactionID string) error {
	if strings.TrimSpace(transactionID) == "" {
		return fmt.Errorf("TransactionId is required: %w", ErrValidation)
	}
	b.mu.RLock("ExtendTransaction")
	defer b.mu.RUnlock()
	status, ok := b.transactions[transactionID]
	if !ok {
		return awserr.New("transaction not found: "+transactionID, awserr.ErrNotFound)
	}
	if status != transactionStatusActive {
		return awserr.New(fmt.Sprintf("transaction %s is not active", transactionID), awserr.ErrConflict)
	}

	return nil
}

// DeleteObjectsOnCancel removes governed table objects written during a cancelled transaction.
// AWS requires the transaction to be in ABORTED state before objects can be deleted.
func (b *InMemoryBackend) DeleteObjectsOnCancel(transactionID string) error {
	if strings.TrimSpace(transactionID) == "" {
		return fmt.Errorf("TransactionId is required: %w", ErrValidation)
	}
	b.mu.RLock("DeleteObjectsOnCancel")
	defer b.mu.RUnlock()
	status, ok := b.transactions[transactionID]
	if !ok {
		return awserr.New("transaction not found: "+transactionID, awserr.ErrNotFound)
	}
	if status != transactionStatusAborted {
		return awserr.New(
			fmt.Sprintf("transaction %s must be in ABORTED state (current: %s)", transactionID, status),
			awserr.ErrConflict,
		)
	}

	return nil
}

// GetDataCellsFilter returns the named data cells filter.
func (b *InMemoryBackend) GetDataCellsFilter(
	tableCatalogID, databaseName, tableName, name string,
) (*DataCellsFilter, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("Name is required: %w", ErrValidation)
	}
	b.mu.RLock("GetDataCellsFilter")
	defer b.mu.RUnlock()
	k := dataCellsFilterKey{
		TableCatalogID: tableCatalogID, DatabaseName: databaseName, TableName: tableName, Name: name,
	}
	f, ok := b.dataCellsFilters[k]
	if !ok {
		return nil, awserr.New("data cells filter not found: "+name, awserr.ErrNotFound)
	}
	cp := *f

	return &cp, nil
}

// UpdateDataCellsFilter replaces an existing data cells filter.
func (b *InMemoryBackend) UpdateDataCellsFilter(filter *DataCellsFilter) error {
	if filter == nil {
		return fmt.Errorf("filter is required: %w", ErrValidation)
	}
	if strings.TrimSpace(filter.TableCatalogID) == "" {
		return fmt.Errorf("TableCatalogId is required: %w", ErrValidation)
	}
	if strings.TrimSpace(filter.DatabaseName) == "" {
		return fmt.Errorf("DatabaseName is required: %w", ErrValidation)
	}
	if strings.TrimSpace(filter.TableName) == "" {
		return fmt.Errorf("TableName is required: %w", ErrValidation)
	}
	if strings.TrimSpace(filter.Name) == "" {
		return fmt.Errorf("Name is required: %w", ErrValidation)
	}
	b.mu.Lock("UpdateDataCellsFilter")
	defer b.mu.Unlock()
	k := dataCellsFilterKey{
		TableCatalogID: filter.TableCatalogID,
		DatabaseName:   filter.DatabaseName,
		TableName:      filter.TableName,
		Name:           filter.Name,
	}
	if _, ok := b.dataCellsFilters[k]; !ok {
		return awserr.New("data cells filter not found: "+filter.Name, awserr.ErrNotFound)
	}
	cp := *filter
	b.dataCellsFilters[k] = &cp

	return nil
}

// GetLFTagExpression returns the named LF-tag expression.
func (b *InMemoryBackend) GetLFTagExpression(name, catalogID string) (*LFTagExpression, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("Name is required: %w", ErrValidation)
	}
	b.mu.RLock("GetLFTagExpression")
	defer b.mu.RUnlock()
	k := lfTagExpressionKey{CatalogID: catalogID, Name: name}
	expr, ok := b.lfTagExpressions[k]
	if !ok {
		return nil, awserr.New("LF-tag expression not found: "+name, awserr.ErrNotFound)
	}
	cp := *expr
	if expr.Expression != nil {
		cp.Expression = make([]LFTag, len(expr.Expression))
		copy(cp.Expression, expr.Expression)
	}

	return &cp, nil
}

// UpdateLFTagExpression updates the description and expression of an existing LF-tag expression.
func (b *InMemoryBackend) UpdateLFTagExpression(name, catalogID, description string, expression []LFTag) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("Name is required: %w", ErrValidation)
	}
	b.mu.Lock("UpdateLFTagExpression")
	defer b.mu.Unlock()
	k := lfTagExpressionKey{CatalogID: catalogID, Name: name}
	expr, ok := b.lfTagExpressions[k]
	if !ok {
		return awserr.New("LF-tag expression not found: "+name, awserr.ErrNotFound)
	}
	expr.Description = description
	if expression != nil {
		cp := make([]LFTag, len(expression))
		copy(cp, expression)
		expr.Expression = cp
	}

	return nil
}

// GetEffectivePermissionsForPath returns effective permissions for a resource path.
func (b *InMemoryBackend) GetEffectivePermissionsForPath(
	resourceArn string, maxResults int, nextToken string,
) ([]*PermissionEntry, string) {
	return b.ListPermissions(resourceArn, maxResults, nextToken)
}

// GetTemporaryCredentials returns synthetic temporary AWS credentials.
func (b *InMemoryBackend) GetTemporaryCredentials(_ *int32) *TemporaryCredentials {
	return &TemporaryCredentials{
		AccessKeyID:     "ASIALAKEFORMATION0002",
		SecretAccessKey: "syntheticSecretKey00000000000000000000001",
		SessionToken:    "syntheticSessionToken002",
	}
}

// GetTableObjects returns an empty list of governed table objects.
func (b *InMemoryBackend) GetTableObjects(_ int, _ string) ([]PartitionedTableObjectsList, string) {
	return []PartitionedTableObjectsList{}, ""
}

// UpdateTableObjects validates the transaction and records the write operations.
func (b *InMemoryBackend) UpdateTableObjects(transactionID string) error {
	if strings.TrimSpace(transactionID) == "" {
		return nil
	}
	b.mu.RLock("UpdateTableObjects")
	defer b.mu.RUnlock()
	if _, ok := b.transactions[transactionID]; !ok {
		return awserr.New("transaction not found: "+transactionID, awserr.ErrNotFound)
	}

	return nil
}

// StartQueryPlanning registers a new query and returns its ID.
func (b *InMemoryBackend) StartQueryPlanning(queryString string) string {
	id := newTransactionID()
	b.mu.Lock("StartQueryPlanning")
	defer b.mu.Unlock()
	b.queries[id] = "WORKUNITS_AVAILABLE"
	_ = queryString

	return id
}

// GetQueryState returns the current state of a query.
func (b *InMemoryBackend) GetQueryState(queryID string) (string, error) {
	if strings.TrimSpace(queryID) == "" {
		return "", fmt.Errorf("QueryId is required: %w", ErrValidation)
	}
	b.mu.RLock("GetQueryState")
	defer b.mu.RUnlock()
	state, ok := b.queries[queryID]
	if !ok {
		return "", awserr.New("query not found: "+queryID, awserr.ErrNotFound)
	}

	return state, nil
}

// GetQueryStatistics returns synthetic statistics for a query.
func (b *InMemoryBackend) GetQueryStatistics(queryID string) (*ExecutionStatistics, *PlanningStatistics, error) {
	if strings.TrimSpace(queryID) == "" {
		return nil, nil, fmt.Errorf("QueryId is required: %w", ErrValidation)
	}
	b.mu.RLock("GetQueryStatistics")
	defer b.mu.RUnlock()
	if _, ok := b.queries[queryID]; !ok {
		return nil, nil, awserr.New("query not found: "+queryID, awserr.ErrNotFound)
	}
	zero := int64(0)
	exec := &ExecutionStatistics{WorkUnitsExecutedCount: &zero}
	plan := &PlanningStatistics{WorkUnitsGeneratedCount: &zero}

	return exec, plan, nil
}

// GetWorkUnits returns the work unit ranges for a completed query plan.
func (b *InMemoryBackend) GetWorkUnits(queryID string) ([]WorkUnitRange, string, error) {
	if strings.TrimSpace(queryID) == "" {
		return nil, "", fmt.Errorf("QueryId is required: %w", ErrValidation)
	}
	b.mu.RLock("GetWorkUnits")
	defer b.mu.RUnlock()
	if _, ok := b.queries[queryID]; !ok {
		return nil, "", awserr.New("query not found: "+queryID, awserr.ErrNotFound)
	}

	return []WorkUnitRange{}, "", nil
}

// GetWorkUnitResults validates that the query exists and returns successfully.
func (b *InMemoryBackend) GetWorkUnitResults(queryID, _ string) error {
	if strings.TrimSpace(queryID) == "" {
		return fmt.Errorf("QueryId is required: %w", ErrValidation)
	}
	b.mu.RLock("GetWorkUnitResults")
	defer b.mu.RUnlock()
	if _, ok := b.queries[queryID]; !ok {
		return awserr.New("query not found: "+queryID, awserr.ErrNotFound)
	}

	return nil
}

// tableStorageKey returns a composite key for table storage optimizer lookups.
func tableStorageKey(catalogID, databaseName, tableName string) string {
	return catalogID + "|" + databaseName + "|" + tableName
}

// ListTableStorageOptimizers returns the storage optimizers for a table, filtered by type if specified.
func (b *InMemoryBackend) ListTableStorageOptimizers(
	catalogID, databaseName, tableName, storageOptimizerType string,
) []StorageOptimizer {
	b.mu.RLock("ListTableStorageOptimizers")
	defer b.mu.RUnlock()
	key := tableStorageKey(catalogID, databaseName, tableName)
	opts := b.tableStorageOptimizers[key]
	result := make([]StorageOptimizer, 0, len(opts))

	for _, o := range opts {
		if storageOptimizerType == "" || o.StorageOptimizerType == storageOptimizerType {
			result = append(result, o)
		}
	}

	return result
}

// UpdateTableStorageOptimizer replaces the storage optimizer config for a table.
func (b *InMemoryBackend) UpdateTableStorageOptimizer(
	catalogID, databaseName, tableName string, config map[string]map[string]string,
) string {
	b.mu.Lock("UpdateTableStorageOptimizer")
	defer b.mu.Unlock()
	key := tableStorageKey(catalogID, databaseName, tableName)
	opts := make([]StorageOptimizer, 0, len(config))
	for optimizerType, cfg := range config {
		opts = append(opts, StorageOptimizer{StorageOptimizerType: optimizerType, Config: cfg})
	}
	b.tableStorageOptimizers[key] = opts

	return "Optimizer updated successfully"
}

// SearchDatabasesByLFTags is implemented in backend_comprehensive.go.

// SearchTablesByLFTags is implemented in backend_comprehensive.go.
