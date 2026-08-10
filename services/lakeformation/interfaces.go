package lakeformation

import "context"

// StorageBackend is the interface for Lake Formation backend operations.
type StorageBackend interface {
	Reset()

	GetDataLakeSettings() *DataLakeSettings
	PutDataLakeSettings(settings *DataLakeSettings)

	RegisterResource(resourceArn, roleArn string, opts RegisterResourceOptions) error
	UpdateResource(resourceArn, roleArn string, opts RegisterResourceOptions) error
	DeregisterResource(resourceArn string) error
	DescribeResource(resourceArn string) (*ResourceInfo, error)
	ListResources(maxResults int, nextToken string) ([]*ResourceInfo, string)

	GrantPermissions(ctx context.Context, entry *PermissionEntry) error
	RevokePermissions(ctx context.Context, entry *PermissionEntry) error
	ListPermissions(
		resource *Resource,
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

	BatchGrantPermissions(ctx context.Context, entries []*BatchPermissionsRequestEntry) []*BatchFailureEntry
	BatchRevokePermissions(ctx context.Context, entries []*BatchPermissionsRequestEntry) []*BatchFailureEntry

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

	CreateLakeFormationOptIn(principal *DataLakePrincipal, resource *Resource, condition *Condition) error
	DeleteLakeFormationOptIn(principal *DataLakePrincipal, resource *Resource, condition *Condition) error
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
