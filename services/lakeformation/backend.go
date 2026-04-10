package lakeformation

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
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
	AssumeDecoratedRoleWithSAML(
		principalArn, roleArn, samlAssertion string,
		durationSeconds *int32,
	) *SAMLCredentials
	CancelTransaction(transactionID string) error
	CommitTransaction(transactionID string) (string, error)
	CreateDataCellsFilter(filter *DataCellsFilter) error
	CreateLFTagExpression(name, description, catalogID string, expression []LFTag) error
	CreateLakeFormationIdentityCenterConfiguration(catalogID, instanceArn string) string
	CreateLakeFormationOptIn(principal *DataLakePrincipal, resource *Resource) error
	DeleteDataCellsFilter(tableCatalogID, databaseName, tableName, name string) error
	DeleteLFTagExpression(name, catalogID string) error
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
	dataLakeSettings      *DataLakeSettings
	resources             map[string]*ResourceInfo
	lfTags                map[lfTagKey]*LFTag
	transactions          map[string]string
	dataCellsFilters      map[dataCellsFilterKey]*DataCellsFilter
	lfTagExpressions      map[lfTagExpressionKey]*LFTagExpression
	identityCenterConfigs map[string]*IdentityCenterConfiguration
	lakeFormationOptIns   []*LFOptIn
	resourceLFTags        map[string][]LFTagPair
	mu                    *lockmetrics.RWMutex
	permissions           []*PermissionEntry
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates a new in-memory Lake Formation backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		dataLakeSettings:      &DataLakeSettings{},
		resources:             make(map[string]*ResourceInfo),
		permissions:           make([]*PermissionEntry, 0),
		lfTags:                make(map[lfTagKey]*LFTag),
		transactions:          make(map[string]string),
		dataCellsFilters:      make(map[dataCellsFilterKey]*DataCellsFilter),
		lfTagExpressions:      make(map[lfTagExpressionKey]*LFTagExpression),
		identityCenterConfigs: make(map[string]*IdentityCenterConfiguration),
		lakeFormationOptIns:   make([]*LFOptIn, 0),
		resourceLFTags:        make(map[string][]LFTagPair),
		mu:                    lockmetrics.New("lakeformation"),
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

// DeregisterResource removes a registered data lake resource and its associated permissions.
func (b *InMemoryBackend) DeregisterResource(resourceArn string) error {
	if resourceArn == "" {
		return fmt.Errorf("ResourceArn is required: %w", ErrValidation)
	}

	b.mu.Lock("DeregisterResource")
	defer b.mu.Unlock()

	if _, ok := b.resources[resourceArn]; !ok {
		return awserr.New(
			fmt.Sprintf("resource not found: %s", resourceArn),
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
			fmt.Sprintf("resource not found: %s", resourceArn),
			awserr.ErrNotFound,
		)
	}

	cp := *info

	return &cp, nil
}

const defaultMaxResults = 100

// ListResources returns a paginated list of registered resources.
func (b *InMemoryBackend) ListResources(maxResults int, nextToken string) ([]*ResourceInfo, string) {
	b.mu.RLock("ListResources")
	defer b.mu.RUnlock()

	all := make([]*ResourceInfo, 0, len(b.resources))
	for _, v := range b.resources {
		cp := *v
		all = append(all, &cp)
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

	filtered := make([]*PermissionEntry, 0)

	for _, p := range b.permissions {
		if resourceArn == "" || permissionMatchesARN(p, resourceArn) {
			cp := *p
			filtered = append(filtered, &cp)
		}
	}

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
	if tagKey == "" {
		return fmt.Errorf("TagKey is required: %w", ErrValidation)
	}

	b.mu.Lock("DeleteLFTag")
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
	if tagKey == "" {
		return nil, fmt.Errorf("TagKey is required: %w", ErrValidation)
	}

	b.mu.RLock("GetLFTag")
	defer b.mu.RUnlock()

	k := lfTagKey{CatalogID: catalogID, TagKey: tagKey}

	tag, ok := b.lfTags[k]
	if !ok {
		return nil, awserr.New(
			fmt.Sprintf("LF tag not found: %s", tagKey),
			awserr.ErrNotFound,
		)
	}

	return copyLFTag(tag), nil
}

// UpdateLFTag adds and removes values from an existing LF tag.
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
	b.mu.RLock("ListLFTags")
	defer b.mu.RUnlock()

	all := make([]*LFTag, 0)

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

	failures := make([]LFTagError, 0)
	resourceKey := resourceToKey(resource)
	existing := b.resourceLFTags[resourceKey]

	for _, pair := range lfTags {
		k := lfTagKey{CatalogID: catalogID, TagKey: pair.TagKey}
		if _, ok := b.lfTags[k]; !ok {
			failures = append(failures, LFTagError{
				LFTag: &pair,
				Error: &errorDetail{
					ErrorCode:    "EntityNotFoundException",
					ErrorMessage: fmt.Sprintf("LF tag not found: %s", pair.TagKey),
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
			fmt.Sprintf("data cells filter already exists: %s", filter.Name),
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

	if _, ok := b.lfTagExpressions[k]; ok {
		return awserr.New(
			fmt.Sprintf("LF-tag expression already exists: %s", name),
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
			fmt.Sprintf("data cells filter not found: %s", name),
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
			fmt.Sprintf("LF-tag expression not found: %s", name),
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
