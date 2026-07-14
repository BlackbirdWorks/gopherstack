package lakeformation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	transactionStatusActive    = "ACTIVE"
	transactionStatusCommitted = "COMMITTED"
	transactionStatusAborted   = "ABORTED"

	transactionTypeReadOnly  = "READ_ONLY"
	transactionTypeReadWrite = "READ_AND_WRITE"

	randAccessKeyBytes  = 16
	randSecretKeyBytes  = 20
	randSessionKeyBytes = 32

	errCodeInvalidInput = "InvalidInputException"
)

// transactionInfo holds transaction metadata.
type transactionInfo struct {
	// ID is the transaction ID this value is keyed by in the transactions
	// Table (see store_setup.go). It is tagged json:"-" because the
	// transactions Table is a "dirty" table -- persistence.go instead
	// round-trips it through a dedicated transactionSnapshot DTO that carries
	// the ID as a real JSON field, so it survives the round trip despite
	// being excluded here. It must never change after the transaction is
	// created (store.Table's keyFn purity requirement).
	ID           string `json:"-"`
	Status       string `json:"Status"`
	Type         string `json:"Type,omitempty"`
	StartTime    string `json:"StartTime,omitempty"`
	EndTime      string `json:"EndTime,omitempty"`
	LastExtended string `json:"LastExtended,omitempty"`
}

// randomHex returns a random hex string of n bytes (2n hex chars).
func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

// ErrValidation is returned when input validation fails.
var ErrValidation = errors.New("validation error")

// errTransactionCommitted indicates an operation that requires a transaction
// to not be committed yet (CancelTransaction, ExtendTransaction,
// DeleteObjectsOnCancel) was attempted on a transaction that already
// committed. It wraps awserr.ErrConflict so existing errors.Is(err,
// awserr.ErrConflict) checks keep matching; handler.go additionally checks
// errors.Is(err, errTransactionCommitted) first so this maps to the AWS
// TransactionCommittedException wire error instead of the
// TransactionCanceledException used for the "already aborted" conflict case.
// Per the real aws-sdk-go-v2 deserializers, CancelTransaction's valid error
// set includes TransactionCommittedException but NOT TransactionCanceledException
// -- the reverse of CommitTransaction's valid set -- so these must not share
// one error code.
var errTransactionCommitted = fmt.Errorf("transaction already committed: %w", awserr.ErrConflict)

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
	ListPermissions(
		resourceArn string,
		maxResults int,
		nextToken string,
		principal *DataLakePrincipal,
		resourceType string,
	) ([]*PermissionEntry, string)

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

	StartTransaction(transactionType string) string
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

	CreateLakeFormationIdentityCenterConfiguration(
		catalogID, instanceArn string,
		externalFiltering *ExternalFilteringConfiguration,
		shareRecipients []DataLakePrincipal,
	) (string, error)
	DeleteLakeFormationIdentityCenterConfiguration(catalogID string) error
	DescribeLakeFormationIdentityCenterConfiguration(catalogID string) (*IdentityCenterConfiguration, error)
	UpdateLakeFormationIdentityCenterConfiguration(
		catalogID string,
		externalFiltering *ExternalFilteringConfiguration,
		appStatus string,
	) error

	CreateLakeFormationOptIn(principal *DataLakePrincipal, resource *Resource) error
	DeleteLakeFormationOptIn(principal *DataLakePrincipal, resource *Resource) error
	ListLakeFormationOptIns(
		principalIdentifier string,
		resource *Resource,
		maxResults int,
		nextToken string,
	) ([]*LFOptIn, string)

	GetDataLakePrincipal(ctx context.Context) *DataLakePrincipal

	ExtendTransaction(transactionID string) error
	DeleteObjectsOnCancel(transactionID string) error

	GetDataCellsFilter(tableCatalogID, databaseName, tableName, name string) (*DataCellsFilter, error)
	UpdateDataCellsFilter(filter *DataCellsFilter) error

	GetLFTagExpression(name, catalogID string) (*LFTagExpression, error)
	UpdateLFTagExpression(name, catalogID, description string, expression []LFTag) error

	GetEffectivePermissionsForPath(resourceArn string, maxResults int, nextToken string) ([]*PermissionEntry, string)

	GetTemporaryCredentials(durationSeconds *int32) *TemporaryCredentials

	GetTableObjects(
		catalogID, databaseName, tableName, transactionID string,
		maxResults int,
		nextToken string,
	) ([]PartitionedTableObjectsList, string)
	UpdateTableObjects(catalogID, databaseName, tableName, transactionID string, writes []WriteOperation) error

	StartQueryPlanning(queryString string) string
	GetQueryState(queryID string) (string, error)
	GetQueryStatistics(queryID string) (*ExecutionStatistics, *PlanningStatistics, error)
	GetWorkUnits(queryID string) ([]WorkUnitRange, string, error)
	GetWorkUnitResults(queryID, workUnitToken string) (string, error)

	ListTableStorageOptimizers(catalogID, databaseName, tableName, storageOptimizerType string) []StorageOptimizer
	UpdateTableStorageOptimizer(catalogID, databaseName, tableName string, config map[string]map[string]string) string

	SearchDatabasesByLFTags(
		expression []LFTag, catalogID string, maxResults int, nextToken string,
	) ([]TaggedDatabase, string)
	SearchTablesByLFTags(
		expression []LFTag, catalogID string, maxResults int, nextToken string,
	) ([]TaggedTable, string)
}

// InMemoryBackend is the in-memory backend for Lake Formation.
type InMemoryBackend struct {
	dataLakeSettings       *DataLakeSettings
	resourceLFTags         map[string][]LFTagPair
	lfTags                 *store.Table[LFTag]
	transactions           *store.Table[transactionInfo]
	dataCellsFilters       *store.Table[DataCellsFilter]
	lfTagExpressions       *store.Table[LFTagExpression]
	resources              *store.Table[ResourceInfo]
	queries                map[string]string
	identityCenterConfigs  *store.Table[IdentityCenterConfiguration]
	permissionsMap         *store.Table[PermissionEntry]
	mu                     *lockmetrics.RWMutex
	registry               *store.Registry
	tableStorageOptimizers map[string][]StorageOptimizer
	tableObjects           map[string][]PartitionedTableObjectsList
	permissionsList        []*PermissionEntry
	lakeFormationOptIns    []*LFOptIn
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates a new in-memory Lake Formation backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		dataLakeSettings:       &DataLakeSettings{},
		permissionsList:        make([]*PermissionEntry, 0),
		lakeFormationOptIns:    make([]*LFOptIn, 0),
		resourceLFTags:         make(map[string][]LFTagPair),
		queries:                make(map[string]string),
		tableStorageOptimizers: make(map[string][]StorageOptimizer),
		tableObjects:           make(map[string][]PartitionedTableObjectsList),
		mu:                     lockmetrics.New("lakeformation"),
		registry:               store.NewRegistry(),
	}

	registerAllTables(b)

	return b
}

// Reset restores the backend to a clean initial state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetTablesLocked()
	b.dataLakeSettings = &DataLakeSettings{}
	b.permissionsList = make([]*PermissionEntry, 0)
	b.lakeFormationOptIns = make([]*LFOptIn, 0)
	b.resourceLFTags = make(map[string][]LFTagPair)
	b.queries = make(map[string]string)
	b.tableStorageOptimizers = make(map[string][]StorageOptimizer)
}

// resetTablesLocked resets every store.Table-backed resource field to empty:
// the "clean" tables via one b.registry.ResetAll() call, plus the "dirty"
// transactions table and the permissionsMap derived cache individually since
// neither is registered on b.registry (see store_setup.go). The caller MUST
// hold b.mu for writing.
func (b *InMemoryBackend) resetTablesLocked() {
	b.registry.ResetAll()
	b.transactions.Reset()
	b.permissionsMap.Reset()
}

// AddLFTagInternal seeds an LF-tag directly for testing.
func (b *InMemoryBackend) AddLFTagInternal(catalogID, tagKey string, tagValues []string) {
	b.mu.Lock("AddLFTagInternal")
	defer b.mu.Unlock()

	vals := make([]string, len(tagValues))
	copy(vals, tagValues)

	b.lfTags.Put(&LFTag{
		CatalogID: catalogID,
		TagKey:    tagKey,
		TagValues: vals,
	})
}

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

// AddPermissionInternal seeds a permission entry directly for testing.
func (b *InMemoryBackend) AddPermissionInternal(entry *PermissionEntry) {
	b.mu.Lock("AddPermissionInternal")
	defer b.mu.Unlock()

	_ = b.grantPermissionsLocked(entry)
}

// AddDataCellsFilterInternal seeds a DataCellsFilter directly for testing.
func (b *InMemoryBackend) AddDataCellsFilterInternal(filter *DataCellsFilter) {
	b.mu.Lock("AddDataCellsFilterInternal")
	defer b.mu.Unlock()

	cp := *filter
	b.dataCellsFilters.Put(&cp)
}

// AddLFTagExpressionInternal seeds an LFTagExpression directly for testing.
func (b *InMemoryBackend) AddLFTagExpressionInternal(expr *LFTagExpression) {
	b.mu.Lock("AddLFTagExpressionInternal")
	defer b.mu.Unlock()

	cp := *expr

	if expr.Expression != nil {
		cp.Expression = make([]LFTag, len(expr.Expression))
		copy(cp.Expression, expr.Expression)
	}

	b.lfTagExpressions.Put(&cp)
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

const defaultMaxResults = 100

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

	k := lfTagKeyStr(catalogID, tagKey)

	if b.lfTags.Has(k) {
		return awserr.New(
			"LF tag already exists: "+tagKey,
			awserr.ErrAlreadyExists,
		)
	}

	vals := make([]string, len(tagValues))
	copy(vals, tagValues)

	b.lfTags.Put(&LFTag{
		CatalogID: catalogID,
		TagKey:    tagKey,
		TagValues: vals,
	})

	return nil
}

// DeleteLFTag removes a LF tag.
func (b *InMemoryBackend) DeleteLFTag(catalogID, tagKey string) error {
	if tagKey == "" {
		return fmt.Errorf("TagKey is required: %w", ErrValidation)
	}

	b.mu.Lock("DeleteLFTag")
	defer b.mu.Unlock()

	k := lfTagKeyStr(catalogID, tagKey)

	if !b.lfTags.Has(k) {
		return awserr.New(
			"LF tag not found: "+tagKey,
			awserr.ErrNotFound,
		)
	}

	b.lfTags.Delete(k)

	return nil
}

// GetLFTag returns the LF tag for the given catalog and key.
func (b *InMemoryBackend) GetLFTag(catalogID, tagKey string) (*LFTag, error) {
	if tagKey == "" {
		return nil, fmt.Errorf("TagKey is required: %w", ErrValidation)
	}

	b.mu.RLock("GetLFTag")
	defer b.mu.RUnlock()

	k := lfTagKeyStr(catalogID, tagKey)

	tag, ok := b.lfTags.Get(k)
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

	k := lfTagKeyStr(catalogID, tagKey)

	tag, ok := b.lfTags.Get(k)
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

	all := make([]*LFTag, 0, b.lfTags.Len())

	for _, t := range b.lfTags.All() {
		if catalogID == "" || t.CatalogID == catalogID {
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

// paginate is a simple opaque paginator for slices.
func paginate[T any](items []T, maxResults int, nextToken string, defaultMax int) ([]T, string) {
	pg := page.New(items, nextToken, maxResults, defaultMax)

	return pg.Data, pg.Next
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

	if s.ReadOnlyAdmins != nil {
		cp.ReadOnlyAdmins = make([]DataLakePrincipal, len(s.ReadOnlyAdmins))
		copy(cp.ReadOnlyAdmins, s.ReadOnlyAdmins)
	}

	if s.Parameters != nil {
		cp.Parameters = make(map[string]string, len(s.Parameters))
		maps.Copy(cp.Parameters, s.Parameters)
	}

	cp.AllowExternalDataFiltering = s.AllowExternalDataFiltering
	cp.AllowFullTableExternalDataAccess = s.AllowFullTableExternalDataAccess

	if s.AuthorizedSessionTagValueList != nil {
		cp.AuthorizedSessionTagValueList = make([]string, len(s.AuthorizedSessionTagValueList))
		copy(cp.AuthorizedSessionTagValueList, s.AuthorizedSessionTagValueList)
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
		k := lfTagKeyStr(catalogID, pair.TagKey)
		tag, ok := b.lfTags.Get(k)
		if !ok {
			failures = append(failures, LFTagError{
				LFTag: &pair,
				Error: &errorDetail{
					ErrorCode:    "EntityNotFoundException",
					ErrorMessage: "LF tag not found: " + pair.TagKey,
				},
			})

			continue
		}

		// Validate tag values against allowed values.
		invalid := false
		for _, v := range pair.TagValues {
			if !slices.Contains(tag.TagValues, v) {
				pair := pair
				failures = append(failures, LFTagError{
					LFTag: &pair,
					Error: &errorDetail{
						ErrorCode:    errCodeInvalidInput,
						ErrorMessage: "invalid tag value: " + v,
					},
				})
				invalid = true

				break
			}
		}
		if invalid {
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

// defaultCredentialsDuration is the AWS default lifetime for temporary
// credentials when the caller does not specify DurationSeconds.
const defaultCredentialsDuration = time.Hour

// credentialsDuration returns the requested credential lifetime, falling
// back to defaultCredentialsDuration when durationSeconds is unset or
// non-positive.
func credentialsDuration(durationSeconds *int32) time.Duration {
	if durationSeconds != nil && *durationSeconds > 0 {
		return time.Duration(*durationSeconds) * time.Second
	}

	return defaultCredentialsDuration
}

// AssumeDecoratedRoleWithSAML returns synthetic temporary credentials.
// The actual SAML assertion and role are not validated in the in-memory backend.
func (b *InMemoryBackend) AssumeDecoratedRoleWithSAML(
	_, _, _ string,
	durationSeconds *int32,
) *SAMLCredentials {
	return &SAMLCredentials{
		AccessKeyID:     "ASIA" + randomHex(randAccessKeyBytes),
		SecretAccessKey: randomHex(randSecretKeyBytes),
		SessionToken:    randomHex(randSessionKeyBytes),
		Expiration:      awstime.Epoch(time.Now().Add(credentialsDuration(durationSeconds))),
	}
}

// CancelTransaction cancels an in-flight transaction.
// Returns an error if the transaction is already committed.
func (b *InMemoryBackend) CancelTransaction(transactionID string) error {
	b.mu.Lock("CancelTransaction")
	defer b.mu.Unlock()

	if info, ok := b.transactions.Get(transactionID); ok && info.Status == transactionStatusCommitted {
		return awserr.New(
			fmt.Sprintf("transaction %s is already committed", transactionID),
			errTransactionCommitted,
		)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if info, ok := b.transactions.Get(transactionID); ok {
		info.Status = transactionStatusAborted
		info.EndTime = now
	} else {
		b.transactions.Put(&transactionInfo{
			ID:        transactionID,
			Status:    transactionStatusAborted,
			StartTime: now,
			EndTime:   now,
		})
	}

	return nil
}

// CommitTransaction commits an in-flight transaction.
// Returns an error if the transaction is already aborted.
func (b *InMemoryBackend) CommitTransaction(transactionID string) (string, error) {
	b.mu.Lock("CommitTransaction")
	defer b.mu.Unlock()

	if info, ok := b.transactions.Get(transactionID); ok && info.Status == transactionStatusAborted {
		return "", awserr.New(
			fmt.Sprintf("transaction %s has been cancelled", transactionID),
			awserr.ErrConflict,
		)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if info, ok := b.transactions.Get(transactionID); ok {
		info.Status = transactionStatusCommitted
		info.EndTime = now
	} else {
		b.transactions.Put(&transactionInfo{
			ID:        transactionID,
			Status:    transactionStatusCommitted,
			StartTime: now,
			EndTime:   now,
		})
	}

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

	k := dataCellsFilterKeyStr(filter.TableCatalogID, filter.DatabaseName, filter.TableName, filter.Name)

	if b.dataCellsFilters.Has(k) {
		return awserr.New(
			"data cells filter already exists: "+filter.Name,
			awserr.ErrAlreadyExists,
		)
	}

	cp := *filter
	b.dataCellsFilters.Put(&cp)

	return nil
}

// CreateLFTagExpression stores a new named LF-tag expression.
func (b *InMemoryBackend) CreateLFTagExpression(name, description, catalogID string, expression []LFTag) error {
	b.mu.Lock("CreateLFTagExpression")
	defer b.mu.Unlock()

	k := lfTagExpressionKeyStr(catalogID, name)

	// Validate each tag in the expression has a TagKey.
	for i, tag := range expression {
		if strings.TrimSpace(tag.TagKey) == "" {
			return fmt.Errorf("Expression[%d].TagKey is required: %w", i, ErrValidation)
		}
	}

	if b.lfTagExpressions.Has(k) {
		return awserr.New(
			"LF-tag expression already exists: "+name,
			awserr.ErrAlreadyExists,
		)
	}

	expr := make([]LFTag, len(expression))
	copy(expr, expression)

	b.lfTagExpressions.Put(&LFTagExpression{
		Name:        name,
		Description: description,
		CatalogID:   catalogID,
		Expression:  expr,
	})

	return nil
}

// CreateLakeFormationIdentityCenterConfiguration creates or replaces the IAM Identity Center
// integration for the given catalog and returns a synthetic application ARN.
func (b *InMemoryBackend) CreateLakeFormationIdentityCenterConfiguration(
	catalogID, instanceArn string,
	externalFiltering *ExternalFilteringConfiguration,
	shareRecipients []DataLakePrincipal,
) (string, error) {
	b.mu.Lock("CreateLakeFormationIdentityCenterConfiguration")
	defer b.mu.Unlock()

	if b.identityCenterConfigs.Has(catalogID) {
		return "", awserr.New(
			"identity center configuration already exists for catalog: "+catalogID,
			awserr.ErrAlreadyExists,
		)
	}

	appArn := fmt.Sprintf(
		"arn:aws:sso::%s:application/ssoins-0000000000000000/apl-%s",
		catalogID,
		catalogID,
	)

	b.identityCenterConfigs.Put(&IdentityCenterConfiguration{
		CatalogID:         catalogID,
		InstanceArn:       instanceArn,
		ApplicationArn:    appArn,
		ExternalFiltering: externalFiltering,
		ShareRecipients:   shareRecipients,
	})

	return appArn, nil
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

	now := time.Now().UTC().Format(time.RFC3339)
	b.lakeFormationOptIns = append(b.lakeFormationOptIns, &LFOptIn{
		Principal:     principal,
		Resource:      resource,
		LastModified:  now,
		LastUpdatedBy: "lakeformation.amazonaws.com",
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

	k := dataCellsFilterKeyStr(tableCatalogID, databaseName, tableName, name)

	if !b.dataCellsFilters.Has(k) {
		return awserr.New(
			"data cells filter not found: "+name,
			awserr.ErrNotFound,
		)
	}

	b.dataCellsFilters.Delete(k)

	return nil
}

// DeleteLFTagExpression removes the named LF-tag expression.
func (b *InMemoryBackend) DeleteLFTagExpression(name, catalogID string) error {
	b.mu.Lock("DeleteLFTagExpression")
	defer b.mu.Unlock()

	k := lfTagExpressionKeyStr(catalogID, name)

	if !b.lfTagExpressions.Has(k) {
		return awserr.New(
			"LF-tag expression not found: "+name,
			awserr.ErrNotFound,
		)
	}

	b.lfTagExpressions.Delete(k)

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

// StartTransaction begins a new in-flight transaction and returns its ID.
func (b *InMemoryBackend) StartTransaction(transactionType string) string {
	id := newTransactionID()

	if transactionType == "" {
		transactionType = transactionTypeReadWrite
	}

	b.mu.Lock("StartTransaction")
	defer b.mu.Unlock()

	b.transactions.Put(&transactionInfo{
		ID:        id,
		Status:    transactionStatusActive,
		Type:      transactionType,
		StartTime: time.Now().UTC().Format(time.RFC3339),
	})

	return id
}

// StartJanitor starts a background goroutine to clean up stale transactions.
const janitorInterval = 5 * time.Minute
const janitorTimeout = time.Hour

// StartJanitor starts a background goroutine to clean up stale transactions.
func (b *InMemoryBackend) StartJanitor(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	go func() {
		l := logger.Load(ctx).With("worker", "lakeformation-janitor")
		ticker := time.NewTicker(janitorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.cleanupStaleTransactions(ctx, l)
			}
		}
	}()
}

func (b *InMemoryBackend) cleanupStaleTransactions(ctx context.Context, l *slog.Logger) {
	b.mu.Lock("JanitorCleanup")
	now := time.Now()
	staleCount := 0
	for _, info := range b.transactions.All() {
		refTimeStr := info.LastExtended
		if refTimeStr == "" {
			refTimeStr = info.StartTime
		}
		t, err := time.Parse(time.RFC3339, refTimeStr)
		if err == nil && now.Sub(t) > janitorTimeout {
			b.transactions.Delete(info.ID)
			staleCount++
		}
	}
	b.mu.Unlock()
	if staleCount > 0 {
		l.InfoContext(ctx, "cleaned up stale lakeformation transactions", "count", staleCount)
	}
}

// DescribeTransaction returns the status of a specific transaction.
func (b *InMemoryBackend) DescribeTransaction(transactionID string) (*Transaction, error) {
	if strings.TrimSpace(transactionID) == "" {
		return nil, fmt.Errorf("TransactionId is required: %w", ErrValidation)
	}

	b.mu.RLock("DescribeTransaction")
	defer b.mu.RUnlock()

	info, ok := b.transactions.Get(transactionID)
	if !ok {
		return nil, awserr.New(
			"transaction not found: "+transactionID,
			awserr.ErrNotFound,
		)
	}

	return &Transaction{
		TransactionID:        transactionID,
		TransactionStatus:    info.Status,
		TransactionStartTime: info.StartTime,
		TransactionEndTime:   info.EndTime,
	}, nil
}

// ListTransactions returns a paginated list of transactions, optionally filtered by status.
func (b *InMemoryBackend) ListTransactions(
	statusFilter string, maxResults int, nextToken string,
) ([]*Transaction, string) {
	b.mu.RLock("ListTransactions")
	defer b.mu.RUnlock()

	all := make([]*Transaction, 0, b.transactions.Len())

	for _, info := range b.transactions.All() {
		if statusFilter != "" && statusFilter != "ALL" {
			if statusFilter == "COMPLETED" {
				// COMPLETED means both COMMITTED and ABORTED.
				if info.Status != transactionStatusCommitted && info.Status != transactionStatusAborted {
					continue
				}
			} else if info.Status != statusFilter {
				continue
			}
		}

		all = append(all, &Transaction{
			TransactionID:        info.ID,
			TransactionStatus:    info.Status,
			TransactionStartTime: info.StartTime,
			TransactionEndTime:   info.EndTime,
		})
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
func (b *InMemoryBackend) GetDataLakePrincipal(ctx context.Context) *DataLakePrincipal {
	account := awsmeta.Account(ctx)
	if account == "" {
		account = awsmeta.DefaultAccount
	}

	return &DataLakePrincipal{
		DataLakePrincipalIdentifier: "arn:aws:iam::" + account + ":user/gopherstack-user",
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

	all := make([]*DataCellsFilter, 0, b.dataCellsFilters.Len())

	for _, v := range b.dataCellsFilters.All() {
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

	all := make([]*LFTagExpression, 0, b.lfTagExpressions.Len())

	for _, v := range b.lfTagExpressions.All() {
		if catalogID != "" && v.CatalogID != catalogID {
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
	resource *Resource,
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
		if resource != nil && !resourceEqual(o.Resource, resource) {
			continue
		}

		cp := &LFOptIn{
			LastModified:  o.LastModified,
			LastUpdatedBy: o.LastUpdatedBy,
		}

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
	if !b.identityCenterConfigs.Has(catalogID) {
		return awserr.New("identity center configuration not found for catalog: "+catalogID, awserr.ErrNotFound)
	}
	b.identityCenterConfigs.Delete(catalogID)

	return nil
}

// DescribeLakeFormationIdentityCenterConfiguration returns the identity center config for a catalog.
func (b *InMemoryBackend) DescribeLakeFormationIdentityCenterConfiguration(
	catalogID string,
) (*IdentityCenterConfiguration, error) {
	b.mu.RLock("DescribeLakeFormationIdentityCenterConfiguration")
	defer b.mu.RUnlock()
	cfg, ok := b.identityCenterConfigs.Get(catalogID)
	if !ok {
		return nil, awserr.New("identity center configuration not found for catalog: "+catalogID, awserr.ErrNotFound)
	}
	cp := *cfg

	return &cp, nil
}

// UpdateLakeFormationIdentityCenterConfiguration updates or creates the identity center config.
func (b *InMemoryBackend) UpdateLakeFormationIdentityCenterConfiguration(
	catalogID string, externalFiltering *ExternalFilteringConfiguration, appStatus string,
) error {
	// Validate ApplicationStatus if provided.
	if appStatus != "" && appStatus != "ENABLED" && appStatus != "DISABLED" {
		return fmt.Errorf("invalid ApplicationStatus: %s: %w", appStatus, ErrValidation)
	}

	b.mu.Lock("UpdateLakeFormationIdentityCenterConfiguration")
	defer b.mu.Unlock()
	cfg, ok := b.identityCenterConfigs.Get(catalogID)
	if !ok {
		b.identityCenterConfigs.Put(&IdentityCenterConfiguration{
			CatalogID:         catalogID,
			ExternalFiltering: externalFiltering,
			ApplicationStatus: appStatus,
		})

		return nil
	}
	if externalFiltering != nil {
		cfg.ExternalFiltering = externalFiltering
	}
	if appStatus != "" {
		cfg.ApplicationStatus = appStatus
	}

	return nil
}

// ExtendTransaction validates that a transaction is active and records the extension.
func (b *InMemoryBackend) ExtendTransaction(transactionID string) error {
	if strings.TrimSpace(transactionID) == "" {
		return fmt.Errorf("TransactionId is required: %w", ErrValidation)
	}
	b.mu.Lock("ExtendTransaction")
	defer b.mu.Unlock()
	info, ok := b.transactions.Get(transactionID)
	if !ok {
		return awserr.New("transaction not found: "+transactionID, awserr.ErrNotFound)
	}
	if info.Status == transactionStatusCommitted {
		return awserr.New(fmt.Sprintf("transaction %s is already committed", transactionID), errTransactionCommitted)
	}
	if info.Status != transactionStatusActive {
		return awserr.New(fmt.Sprintf("transaction %s is not active", transactionID), awserr.ErrConflict)
	}
	info.LastExtended = time.Now().UTC().Format(time.RFC3339)

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
	info, ok := b.transactions.Get(transactionID)
	if !ok {
		return awserr.New("transaction not found: "+transactionID, awserr.ErrNotFound)
	}
	if info.Status == transactionStatusCommitted {
		return awserr.New(fmt.Sprintf("transaction %s is already committed", transactionID), errTransactionCommitted)
	}
	if info.Status != transactionStatusAborted {
		return awserr.New(
			fmt.Sprintf("transaction %s must be in ABORTED state (current: %s)", transactionID, info.Status),
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
	k := dataCellsFilterKeyStr(tableCatalogID, databaseName, tableName, name)
	f, ok := b.dataCellsFilters.Get(k)
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
	k := dataCellsFilterKeyStr(filter.TableCatalogID, filter.DatabaseName, filter.TableName, filter.Name)
	if !b.dataCellsFilters.Has(k) {
		return awserr.New("data cells filter not found: "+filter.Name, awserr.ErrNotFound)
	}
	cp := *filter
	b.dataCellsFilters.Put(&cp)

	return nil
}

// GetLFTagExpression returns the named LF-tag expression.
func (b *InMemoryBackend) GetLFTagExpression(name, catalogID string) (*LFTagExpression, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("Name is required: %w", ErrValidation)
	}
	b.mu.RLock("GetLFTagExpression")
	defer b.mu.RUnlock()
	k := lfTagExpressionKeyStr(catalogID, name)
	expr, ok := b.lfTagExpressions.Get(k)
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
	k := lfTagExpressionKeyStr(catalogID, name)
	expr, ok := b.lfTagExpressions.Get(k)
	if !ok {
		return awserr.New("LF-tag expression not found: "+name, awserr.ErrNotFound)
	}
	expr.Description = description
	if expression != nil {
		if len(expression) == 0 {
			return fmt.Errorf("expression must not be empty: %w", ErrValidation)
		}
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
	return b.ListPermissions(resourceArn, maxResults, nextToken, nil, "")
}

// GetTemporaryCredentials returns synthetic temporary AWS credentials.
func (b *InMemoryBackend) GetTemporaryCredentials(durationSeconds *int32) *TemporaryCredentials {
	return &TemporaryCredentials{
		AccessKeyID:     "ASIA" + randomHex(randAccessKeyBytes),
		SecretAccessKey: randomHex(randSecretKeyBytes),
		SessionToken:    randomHex(randSessionKeyBytes),
		Expiration:      awstime.Epoch(time.Now().Add(credentialsDuration(durationSeconds))),
	}
}

// tableKey generates a unique key for a table.
func tableKey(catalogID, db, table string) string {
	return catalogID + "|" + db + "|" + table
}

// GetTableObjects returns a paginated list of governed table objects.
func (b *InMemoryBackend) GetTableObjects(
	catalogID, databaseName, tableName, _ string,
	maxResults int, nextToken string,
) ([]PartitionedTableObjectsList, string) {
	b.mu.RLock("GetTableObjects")
	defer b.mu.RUnlock()
	key := tableKey(catalogID, databaseName, tableName)
	objects := b.tableObjects[key]

	return paginate(objects, maxResults, nextToken, defaultMaxResults)
}

// UpdateTableObjects validates the transaction and records the write operations.
func (b *InMemoryBackend) UpdateTableObjects(
	catalogID, databaseName, tableName, transactionID string,
	writes []WriteOperation,
) error {
	if strings.TrimSpace(transactionID) == "" {
		return nil
	}
	b.mu.Lock("UpdateTableObjects")
	defer b.mu.Unlock()
	info, ok := b.transactions.Get(transactionID)
	if !ok {
		return awserr.New("transaction not found: "+transactionID, awserr.ErrNotFound)
	}
	if info.Type == transactionTypeReadOnly {
		return fmt.Errorf("cannot write to READ_ONLY transaction: %w", ErrValidation)
	}

	key := tableKey(catalogID, databaseName, tableName)

	// Create a new partitioned list to hold the added objects
	list := PartitionedTableObjectsList{
		Objects: make([]TableObject, 0),
	}

	for _, w := range writes {
		if w.AddObject != nil {
			list.Objects = append(list.Objects, *w.AddObject)
		}
	}

	if len(list.Objects) > 0 {
		b.tableObjects[key] = append(b.tableObjects[key], list)
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
	one := int64(1)
	exec := &ExecutionStatistics{WorkUnitsExecutedCount: &one}
	plan := &PlanningStatistics{WorkUnitsGeneratedCount: &one}

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

	return []WorkUnitRange{{WorkUnitIDMax: 0, WorkUnitIDMin: 0, WorkUnitToken: queryID}}, "", nil
}

// GetWorkUnitResults validates that the query exists and returns its content.
func (b *InMemoryBackend) GetWorkUnitResults(queryID, _ string) (string, error) {
	if strings.TrimSpace(queryID) == "" {
		return "", fmt.Errorf("QueryId is required: %w", ErrValidation)
	}
	b.mu.RLock("GetWorkUnitResults")
	defer b.mu.RUnlock()
	query, ok := b.queries[queryID]
	if !ok {
		return "", awserr.New("query not found: "+queryID, awserr.ErrNotFound)
	}

	return query, nil
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
