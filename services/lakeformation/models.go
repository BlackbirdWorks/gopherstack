package lakeformation

import "time"

// DataLakeSettings contains the data lake settings for an account.
type DataLakeSettings struct {
	DataLakeAdmins                   []DataLakePrincipal    `json:"DataLakeAdmins,omitempty"`
	CreateDatabaseDefaultPermissions []PrincipalPermissions `json:"CreateDatabaseDefaultPermissions,omitempty"`
	CreateTableDefaultPermissions    []PrincipalPermissions `json:"CreateTableDefaultPermissions,omitempty"`
	TrustedResourceOwners            []string               `json:"TrustedResourceOwners,omitempty"`
}

// DataLakePrincipal represents an IAM principal in the data lake.
type DataLakePrincipal struct {
	DataLakePrincipalIdentifier string `json:"DataLakePrincipalIdentifier"`
}

// PrincipalPermissions pairs a principal with a set of permissions.
type PrincipalPermissions struct {
	Principal   *DataLakePrincipal `json:"Principal,omitempty"`
	Permissions []string           `json:"Permissions,omitempty"`
}

// ResourceInfo holds registration info for a data lake resource.
type ResourceInfo struct {
	LastModified *time.Time `json:"LastModified,omitempty"`
	ResourceArn  string     `json:"ResourceArn"`
	RoleArn      string     `json:"RoleArn"`
}

// LFTag represents a Lake Formation tag with its allowed values.
type LFTag struct {
	CatalogID string   `json:"CatalogId,omitempty"`
	TagKey    string   `json:"TagKey"`
	TagValues []string `json:"TagValues"`
}

// Resource describes the resource to which permissions are granted.
type Resource struct {
	Catalog      *CatalogResource      `json:"Catalog,omitempty"`
	Database     *DatabaseResource     `json:"Database,omitempty"`
	Table        *TableResource        `json:"Table,omitempty"`
	DataLocation *DataLocationResource `json:"DataLocation,omitempty"`
}

// CatalogResource represents the data catalog resource.
type CatalogResource struct{}

// DatabaseResource represents a database resource.
type DatabaseResource struct {
	Name string `json:"Name"`
}

// TableResource represents a table resource.
type TableResource struct {
	CatalogID    string `json:"CatalogId,omitempty"`
	DatabaseName string `json:"DatabaseName"`
	Name         string `json:"Name"`
}

// DataLocationResource represents an Amazon S3 data location resource.
type DataLocationResource struct {
	ResourceArn string `json:"ResourceArn"`
}

// PermissionEntry associates a principal and resource with a set of permissions.
type PermissionEntry struct {
	Principal                  *DataLakePrincipal `json:"Principal,omitempty"`
	Resource                   *Resource          `json:"Resource,omitempty"`
	Permissions                []string           `json:"Permissions,omitempty"`
	PermissionsWithGrantOption []string           `json:"PermissionsWithGrantOption,omitempty"`
}

// BatchFailureEntry reports a failure for a single entry in a batch operation.
type BatchFailureEntry struct {
	RequestEntry *PermissionEntry `json:"RequestEntry,omitempty"`
	Error        *errorDetail     `json:"Error,omitempty"`
}

// errorDetail is the nested error object in a BatchFailureEntry.
type errorDetail struct {
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}

// --- Request / Response types ---

// getDataLakeSettingsInput is the request body for GetDataLakeSettings.
type getDataLakeSettingsInput struct {
	CatalogID string `json:"CatalogId,omitempty"`
}

// getDataLakeSettingsOutput is the response body for GetDataLakeSettings.
type getDataLakeSettingsOutput struct {
	DataLakeSettings *DataLakeSettings `json:"DataLakeSettings"`
}

// putDataLakeSettingsInput is the request body for PutDataLakeSettings.
type putDataLakeSettingsInput struct {
	DataLakeSettings *DataLakeSettings `json:"DataLakeSettings"`
	CatalogID        string            `json:"CatalogId,omitempty"`
}

// registerResourceInput is the request body for RegisterResource.
type registerResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	RoleArn     string `json:"RoleArn"`
}

// registerResourceOutput is the response body for RegisterResource (empty).
type registerResourceOutput struct{}

// deregisterResourceInput is the request body for DeregisterResource.
type deregisterResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

// deregisterResourceOutput is the response body for DeregisterResource (empty).
type deregisterResourceOutput struct{}

// describeResourceInput is the request body for DescribeResource.
type describeResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

// describeResourceOutput is the response body for DescribeResource.
type describeResourceOutput struct {
	ResourceInfo *ResourceInfo `json:"ResourceInfo"`
}

// listResourcesInput is the request body for ListResources.
type listResourcesInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

// listResourcesOutput is the response body for ListResources.
type listResourcesOutput struct {
	NextToken        string          `json:"NextToken,omitempty"`
	ResourceInfoList []*ResourceInfo `json:"ResourceInfoList"`
}

// grantPermissionsInput is the request body for GrantPermissions.
type grantPermissionsInput struct {
	CatalogID                  string             `json:"CatalogId,omitempty"`
	Principal                  *DataLakePrincipal `json:"Principal"`
	Resource                   *Resource          `json:"Resource"`
	Permissions                []string           `json:"Permissions"`
	PermissionsWithGrantOption []string           `json:"PermissionsWithGrantOption,omitempty"`
}

// grantPermissionsOutput is the response body for GrantPermissions (empty).
type grantPermissionsOutput struct{}

// revokePermissionsInput is the request body for RevokePermissions.
type revokePermissionsInput struct {
	CatalogID                  string             `json:"CatalogId,omitempty"`
	Principal                  *DataLakePrincipal `json:"Principal"`
	Resource                   *Resource          `json:"Resource"`
	Permissions                []string           `json:"Permissions"`
	PermissionsWithGrantOption []string           `json:"PermissionsWithGrantOption,omitempty"`
}

// revokePermissionsOutput is the response body for RevokePermissions (empty).
type revokePermissionsOutput struct{}

// listPermissionsInput is the request body for ListPermissions.
type listPermissionsInput struct {
	ResourceArn string `json:"ResourceArn,omitempty"`
	NextToken   string `json:"NextToken,omitempty"`
	MaxResults  int    `json:"MaxResults,omitempty"`
}

// listPermissionsOutput is the response body for ListPermissions.
type listPermissionsOutput struct {
	NextToken                    string             `json:"NextToken,omitempty"`
	PrincipalResourcePermissions []*PermissionEntry `json:"PrincipalResourcePermissions"`
}

// createLFTagInput is the request body for CreateLFTag.
type createLFTagInput struct {
	CatalogID string   `json:"CatalogId,omitempty"`
	TagKey    string   `json:"TagKey"`
	TagValues []string `json:"TagValues"`
}

// createLFTagOutput is the response body for CreateLFTag (empty).
type createLFTagOutput struct{}

// deleteLFTagInput is the request body for DeleteLFTag.
type deleteLFTagInput struct {
	CatalogID string `json:"CatalogId,omitempty"`
	TagKey    string `json:"TagKey"`
}

// deleteLFTagOutput is the response body for DeleteLFTag (empty).
type deleteLFTagOutput struct{}

// getLFTagInput is the request body for GetLFTag.
type getLFTagInput struct {
	CatalogID string `json:"CatalogId,omitempty"`
	TagKey    string `json:"TagKey"`
}

// getLFTagOutput is the response body for GetLFTag.
type getLFTagOutput struct {
	CatalogID string   `json:"CatalogId,omitempty"`
	TagKey    string   `json:"TagKey"`
	TagValues []string `json:"TagValues"`
}

// updateLFTagInput is the request body for UpdateLFTag.
type updateLFTagInput struct {
	CatalogID         string   `json:"CatalogId,omitempty"`
	TagKey            string   `json:"TagKey"`
	TagValuesToAdd    []string `json:"TagValuesToAdd,omitempty"`
	TagValuesToDelete []string `json:"TagValuesToDelete,omitempty"`
}

// updateLFTagOutput is the response body for UpdateLFTag (empty).
type updateLFTagOutput struct{}

// listLFTagsInput is the request body for ListLFTags.
type listLFTagsInput struct {
	CatalogID  string `json:"CatalogId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

// listLFTagsOutput is the response body for ListLFTags.
type listLFTagsOutput struct {
	NextToken string   `json:"NextToken,omitempty"`
	LFTags    []*LFTag `json:"LFTags"`
}

// batchGrantPermissionsInput is the request body for BatchGrantPermissions.
type batchGrantPermissionsInput struct {
	CatalogID string             `json:"CatalogId,omitempty"`
	Entries   []*PermissionEntry `json:"Entries"`
}

// batchGrantPermissionsOutput is the response body for BatchGrantPermissions.
type batchGrantPermissionsOutput struct {
	Failures []BatchFailureEntry `json:"Failures"`
}

// batchRevokePermissionsInput is the request body for BatchRevokePermissions.
type batchRevokePermissionsInput struct {
	CatalogID string             `json:"CatalogId,omitempty"`
	Entries   []*PermissionEntry `json:"Entries"`
}

// batchRevokePermissionsOutput is the response body for BatchRevokePermissions.
type batchRevokePermissionsOutput struct {
	Failures []BatchFailureEntry `json:"Failures"`
}

// errorResponse is the standard Lake Formation error response envelope.
type errorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// LFTagPair represents an LF-tag key with its associated values attached to a resource.
type LFTagPair struct {
	CatalogID string   `json:"CatalogId,omitempty"`
	TagKey    string   `json:"TagKey"`
	TagValues []string `json:"TagValues"`
}

// LFTagError represents a failure to attach or detach a single LF-tag.
type LFTagError struct {
	LFTag *LFTagPair   `json:"LFTag,omitempty"`
	Error *errorDetail `json:"Error,omitempty"`
}

// DataCellsFilter holds the definition of a cell-level access filter.
type DataCellsFilter struct {
	TableCatalogID string `json:"TableCatalogId"`
	DatabaseName   string `json:"DatabaseName"`
	TableName      string `json:"TableName"`
	Name           string `json:"Name"`
}

// LFTagExpression holds a saved, named LF-tag expression.
type LFTagExpression struct {
	Name        string  `json:"Name"`
	Description string  `json:"Description,omitempty"`
	CatalogID   string  `json:"CatalogId,omitempty"`
	Expression  []LFTag `json:"Expression,omitempty"`
}

// Transaction represents an in-flight Lake Formation governed table transaction.
type Transaction struct {
	TransactionID     string `json:"TransactionId"`
	TransactionStatus string `json:"TransactionStatus"`
}

// IdentityCenterConfiguration holds the IAM Identity Center integration configuration.
type IdentityCenterConfiguration struct {
	ExternalFiltering *ExternalFilteringConfiguration `json:"ExternalFiltering,omitempty"`
	CatalogID         string                          `json:"CatalogId,omitempty"`
	InstanceArn       string                          `json:"InstanceArn,omitempty"`
	ApplicationArn    string                          `json:"ApplicationArn,omitempty"`
	ShareRecipients   []DataLakePrincipal             `json:"ShareRecipients,omitempty"`
}

// LFOptIn associates a principal and resource for opt-in enforcement.
type LFOptIn struct {
	Principal *DataLakePrincipal `json:"Principal,omitempty"`
	Resource  *Resource          `json:"Resource,omitempty"`
}

// --- Request / Response types for new operations ---

// addLFTagsToResourceInput is the request body for AddLFTagsToResource.
type addLFTagsToResourceInput struct {
	CatalogID string      `json:"CatalogId,omitempty"`
	Resource  *Resource   `json:"Resource"`
	LFTags    []LFTagPair `json:"LFTags"`
}

// addLFTagsToResourceOutput is the response body for AddLFTagsToResource.
type addLFTagsToResourceOutput struct {
	Failures []LFTagError `json:"Failures"`
}

// assumeDecoratedRoleWithSAMLInput is the request body for AssumeDecoratedRoleWithSAML.
type assumeDecoratedRoleWithSAMLInput struct {
	DurationSeconds *int32 `json:"DurationSeconds,omitempty"`
	PrincipalArn    string `json:"PrincipalArn"`
	RoleArn         string `json:"RoleArn"`
	SAMLAssertion   string `json:"SAMLAssertion"`
}

// SAMLCredentials is the response body for AssumeDecoratedRoleWithSAML.
type SAMLCredentials struct {
	AccessKeyID     string `json:"AccessKeyId,omitempty"`
	SecretAccessKey string `json:"SecretAccessKey,omitempty"`
	SessionToken    string `json:"SessionToken,omitempty"`
	Expiration      string `json:"Expiration,omitempty"`
}

// cancelTransactionInput is the request body for CancelTransaction.
type cancelTransactionInput struct {
	TransactionID string `json:"TransactionId"`
}

// cancelTransactionOutput is the response body for CancelTransaction (empty).
type cancelTransactionOutput struct{}

// commitTransactionInput is the request body for CommitTransaction.
type commitTransactionInput struct {
	TransactionID string `json:"TransactionId"`
}

// commitTransactionOutput is the response body for CommitTransaction.
type commitTransactionOutput struct {
	TransactionStatus string `json:"TransactionStatus,omitempty"`
}

// createDataCellsFilterInput is the request body for CreateDataCellsFilter.
type createDataCellsFilterInput struct {
	TableData *DataCellsFilter `json:"TableData"`
}

// createDataCellsFilterOutput is the response body for CreateDataCellsFilter (empty).
type createDataCellsFilterOutput struct{}

// createLFTagExpressionInput is the request body for CreateLFTagExpression.
type createLFTagExpressionInput struct {
	CatalogID   string  `json:"CatalogId,omitempty"`
	Name        string  `json:"Name"`
	Description string  `json:"Description,omitempty"`
	Expression  []LFTag `json:"Expression"`
}

// createLFTagExpressionOutput is the response body for CreateLFTagExpression (empty).
type createLFTagExpressionOutput struct{}

// createLakeFormationIdentityCenterConfigurationInput is the request body for
// CreateLakeFormationIdentityCenterConfiguration.
type createLakeFormationIdentityCenterConfigurationInput struct {
	CatalogID         string                          `json:"CatalogId,omitempty"`
	InstanceArn       string                          `json:"InstanceArn,omitempty"`
	ExternalFiltering *ExternalFilteringConfiguration `json:"ExternalFiltering,omitempty"`
	ShareRecipients   []DataLakePrincipal             `json:"ShareRecipients,omitempty"`
}

// createLakeFormationIdentityCenterConfigurationOutput is the response body for
// CreateLakeFormationIdentityCenterConfiguration.
type createLakeFormationIdentityCenterConfigurationOutput struct {
	ApplicationArn string `json:"ApplicationArn,omitempty"`
}

// createLakeFormationOptInInput is the request body for CreateLakeFormationOptIn.
type createLakeFormationOptInInput struct {
	Principal *DataLakePrincipal `json:"Principal"`
	Resource  *Resource          `json:"Resource"`
}

// createLakeFormationOptInOutput is the response body for CreateLakeFormationOptIn (empty).
type createLakeFormationOptInOutput struct{}

// deleteDataCellsFilterInput is the request body for DeleteDataCellsFilter.
type deleteDataCellsFilterInput struct {
	TableCatalogID string `json:"TableCatalogId,omitempty"`
	DatabaseName   string `json:"DatabaseName,omitempty"`
	TableName      string `json:"TableName,omitempty"`
	Name           string `json:"Name,omitempty"`
}

// deleteDataCellsFilterOutput is the response body for DeleteDataCellsFilter (empty).
type deleteDataCellsFilterOutput struct{}

// deleteLFTagExpressionInput is the request body for DeleteLFTagExpression.
type deleteLFTagExpressionInput struct {
	CatalogID string `json:"CatalogId,omitempty"`
	Name      string `json:"Name"`
}

// deleteLFTagExpressionOutput is the response body for DeleteLFTagExpression (empty).
type deleteLFTagExpressionOutput struct{}

// --- New operation request/response types ---

// updateResourceInput is the request body for UpdateResource.
type updateResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	RoleArn     string `json:"RoleArn"`
}

// updateResourceOutput is the response body for UpdateResource (empty).
type updateResourceOutput struct{}

// startTransactionOutput is the response body for StartTransaction.
type startTransactionOutput struct {
	TransactionID string `json:"TransactionId,omitempty"`
}

// describeTransactionInput is the request body for DescribeTransaction.
type describeTransactionInput struct {
	TransactionID string `json:"TransactionId"`
}

// describeTransactionOutput is the response body for DescribeTransaction.
type describeTransactionOutput struct {
	TransactionDescription *Transaction `json:"TransactionDescription,omitempty"`
}

// listTransactionsInput is the request body for ListTransactions.
type listTransactionsInput struct {
	StatusFilter string `json:"StatusFilter,omitempty"`
	NextToken    string `json:"NextToken,omitempty"`
	MaxResults   int    `json:"MaxResults,omitempty"`
}

// listTransactionsOutput is the response body for ListTransactions.
type listTransactionsOutput struct {
	NextToken    string         `json:"NextToken,omitempty"`
	Transactions []*Transaction `json:"Transactions"`
}

// removeLFTagsFromResourceInput is the request body for RemoveLFTagsFromResource.
type removeLFTagsFromResourceInput struct {
	CatalogID string      `json:"CatalogId,omitempty"`
	Resource  *Resource   `json:"Resource"`
	LFTags    []LFTagPair `json:"LFTags"`
}

// removeLFTagsFromResourceOutput is the response body for RemoveLFTagsFromResource.
type removeLFTagsFromResourceOutput struct {
	Failures []LFTagError `json:"Failures"`
}

// getResourceLFTagsInput is the request body for GetResourceLFTags.
type getResourceLFTagsInput struct {
	Resource           *Resource `json:"Resource"`
	ShowAssignedLFTags *bool     `json:"ShowAssignedLFTags,omitempty"`
	CatalogID          string    `json:"CatalogId,omitempty"`
}

// getResourceLFTagsOutput is the response body for GetResourceLFTags.
type getResourceLFTagsOutput struct {
	LFTagOnDatabase []LFTagPair `json:"LFTagOnDatabase,omitempty"`
	LFTagsOnColumns []LFTagPair `json:"LFTagsOnColumns,omitempty"`
	LFTagsOnTable   []LFTagPair `json:"LFTagsOnTable,omitempty"`
}

// listDataCellsFilterInput is the request body for ListDataCellsFilter.
type listDataCellsFilterInput struct {
	Table      *TableResource `json:"Table,omitempty"`
	NextToken  string         `json:"NextToken,omitempty"`
	MaxResults int            `json:"MaxResults,omitempty"`
}

// listDataCellsFilterOutput is the response body for ListDataCellsFilter.
type listDataCellsFilterOutput struct {
	NextToken        string             `json:"NextToken,omitempty"`
	DataCellsFilters []*DataCellsFilter `json:"DataCellsFilters"`
}

// listLFTagExpressionsInput is the request body for ListLFTagExpressions.
type listLFTagExpressionsInput struct {
	CatalogID  string `json:"CatalogId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

// listLFTagExpressionsOutput is the response body for ListLFTagExpressions.
type listLFTagExpressionsOutput struct {
	NextToken        string             `json:"NextToken,omitempty"`
	LFTagExpressions []*LFTagExpression `json:"LFTagExpressions"`
}

// deleteLakeFormationOptInInput is the request body for DeleteLakeFormationOptIn.
type deleteLakeFormationOptInInput struct {
	Principal *DataLakePrincipal `json:"Principal"`
	Resource  *Resource          `json:"Resource"`
}

// deleteLakeFormationOptInOutput is the response body for DeleteLakeFormationOptIn (empty).
type deleteLakeFormationOptInOutput struct{}

// listLakeFormationOptInsInput is the request body for ListLakeFormationOptIns.
type listLakeFormationOptInsInput struct {
	Principal  *DataLakePrincipal `json:"Principal,omitempty"`
	Resource   *Resource          `json:"Resource,omitempty"`
	NextToken  string             `json:"NextToken,omitempty"`
	MaxResults int                `json:"MaxResults,omitempty"`
}

// listLakeFormationOptInsOutput is the response body for ListLakeFormationOptIns.
type listLakeFormationOptInsOutput struct {
	NextToken                   string     `json:"NextToken,omitempty"`
	LakeFormationOptInsInfoList []*LFOptIn `json:"LakeFormationOptInsInfoList"`
}

// getDataLakePrincipalOutput is the response body for GetDataLakePrincipal.
type getDataLakePrincipalOutput struct {
	Identity string `json:"Identity,omitempty"`
}

// --- New types for 24 additional operations ---

// ExternalFilteringConfiguration holds external filtering config.
type ExternalFilteringConfiguration struct {
	Status            string   `json:"Status,omitempty"`
	AuthorizedTargets []string `json:"AuthorizedTargets,omitempty"`
}

// VirtualObject is a reference to an S3 object.
type VirtualObject struct {
	URI  string `json:"Uri"`
	ETag string `json:"ETag,omitempty"`
}

// TableObject represents an object in a governed table.
type TableObject struct {
	Size *int64 `json:"Size,omitempty"`
	URI  string `json:"Uri"`
	ETag string `json:"ETag,omitempty"`
}

// PartitionedTableObjectsList holds objects for a partition.
type PartitionedTableObjectsList struct {
	PartitionValues []string      `json:"PartitionValues,omitempty"`
	Objects         []TableObject `json:"Objects,omitempty"`
}

// WriteOperation represents a single governed table write.
type WriteOperation struct {
	AddObject    *TableObject   `json:"AddObject,omitempty"`
	DeleteObject *VirtualObject `json:"DeleteObject,omitempty"`
}

// TemporaryCredentials holds temporary AWS credentials.
type TemporaryCredentials struct {
	AccessKeyID     string `json:"AccessKeyId,omitempty"`
	SecretAccessKey string `json:"SecretAccessKey,omitempty"`
	SessionToken    string `json:"SessionToken,omitempty"`
}

// AuditContext carries audit information.
type AuditContext struct {
	AdditionalAuditContext string `json:"AdditionalAuditContext,omitempty"`
}

// Partition represents a Glue partition.
type Partition struct {
	Values []string `json:"Values,omitempty"`
}

// ExecutionStatistics holds statistics for a query execution.
type ExecutionStatistics struct {
	AverageExecutionTimeMillis *int64 `json:"AverageExecutionTimeMillis,omitempty"`
	DataScannedBytes           *int64 `json:"DataScannedBytes,omitempty"`
	WorkUnitsExecutedCount     *int64 `json:"WorkUnitsExecutedCount,omitempty"`
}

// PlanningStatistics holds query planning statistics.
type PlanningStatistics struct {
	EstimatedDataToScanBytes *int64 `json:"EstimatedDataToScanBytes,omitempty"`
	PlanningTimeMillis       *int64 `json:"PlanningTimeMillis,omitempty"`
	QueueTimeMillis          *int64 `json:"QueueTimeMillis,omitempty"`
	WorkUnitsGeneratedCount  *int64 `json:"WorkUnitsGeneratedCount,omitempty"`
}

// WorkUnitRange represents a range of work units.
type WorkUnitRange struct {
	WorkUnitToken string `json:"WorkUnitToken"`
	WorkUnitIDMax int64  `json:"WorkUnitIdMax"`
	WorkUnitIDMin int64  `json:"WorkUnitIdMin"`
}

// QueryPlanningContext provides context for query planning.
type QueryPlanningContext struct {
	CatalogID       string            `json:"CatalogId,omitempty"`
	DatabaseName    string            `json:"DatabaseName"`
	QueryAsOfTime   *string           `json:"QueryAsOfTime,omitempty"`
	QueryParameters map[string]string `json:"QueryParameters,omitempty"`
	TransactionID   string            `json:"TransactionId,omitempty"`
}

// StorageOptimizer holds storage optimizer info.
type StorageOptimizer struct {
	StorageOptimizerType string            `json:"StorageOptimizerType,omitempty"`
	Config               map[string]string `json:"Config,omitempty"`
	ErrorMessage         string            `json:"ErrorMessage,omitempty"`
}

// TaggedDatabase holds a database with its LF-tags.
type TaggedDatabase struct {
	Database *DatabaseResource `json:"Database,omitempty"`
	LFTags   []LFTagPair       `json:"LFTags,omitempty"`
}

// ColumnLFTag holds LF-tags for a column.
type ColumnLFTag struct {
	Name   string      `json:"Name,omitempty"`
	LFTags []LFTagPair `json:"LFTags,omitempty"`
}

// TaggedTable holds a table with its LF-tags.
type TaggedTable struct {
	Table           *TableResource `json:"Table,omitempty"`
	LFTagOnDatabase []LFTagPair    `json:"LFTagOnDatabase,omitempty"`
	LFTagsOnTable   []LFTagPair    `json:"LFTagsOnTable,omitempty"`
	LFTagsOnColumns []ColumnLFTag  `json:"LFTagsOnColumns,omitempty"`
}

// --- New request/response types ---

type deleteLakeFormationIdentityCenterConfigurationInput struct {
	CatalogID string `json:"CatalogId,omitempty"`
}
type deleteLakeFormationIdentityCenterConfigurationOutput struct{}

type deleteObjectsOnCancelInput struct {
	CatalogID     string          `json:"CatalogId,omitempty"`
	DatabaseName  string          `json:"DatabaseName,omitempty"`
	TableName     string          `json:"TableName,omitempty"`
	TransactionID string          `json:"TransactionId"`
	Objects       []VirtualObject `json:"Objects,omitempty"`
}
type deleteObjectsOnCancelOutput struct{}

type describeLakeFormationIdentityCenterConfigurationInput struct {
	CatalogID string `json:"CatalogId,omitempty"`
}
type describeLakeFormationIdentityCenterConfigurationOutput struct {
	ExternalFiltering *ExternalFilteringConfiguration `json:"ExternalFiltering,omitempty"`
	CatalogID         string                          `json:"CatalogId,omitempty"`
	InstanceArn       string                          `json:"InstanceArn,omitempty"`
	ApplicationArn    string                          `json:"ApplicationArn,omitempty"`
	ShareRecipients   []DataLakePrincipal             `json:"ShareRecipients,omitempty"`
}

type extendTransactionInput struct {
	TransactionID string `json:"TransactionId,omitempty"`
}
type extendTransactionOutput struct{}

type getDataCellsFilterInput struct {
	TableCatalogID string `json:"TableCatalogId,omitempty"`
	DatabaseName   string `json:"DatabaseName"`
	TableName      string `json:"TableName"`
	Name           string `json:"Name"`
}
type getDataCellsFilterOutput struct {
	DataCellsFilter *DataCellsFilter `json:"DataCellsFilter,omitempty"`
}

type getEffectivePermissionsForPathInput struct {
	ResourceArn string `json:"ResourceArn,omitempty"`
	NextToken   string `json:"NextToken,omitempty"`
	MaxResults  int    `json:"MaxResults,omitempty"`
}
type getEffectivePermissionsForPathOutput struct {
	NextToken                    string             `json:"NextToken,omitempty"`
	PrincipalResourcePermissions []*PermissionEntry `json:"PrincipalResourcePermissions"`
}

type getLFTagExpressionInput struct {
	Name      string `json:"Name"`
	CatalogID string `json:"CatalogId,omitempty"`
}
type getLFTagExpressionOutput struct {
	Name        string  `json:"Name,omitempty"`
	Description string  `json:"Description,omitempty"`
	CatalogID   string  `json:"CatalogId,omitempty"`
	Expression  []LFTag `json:"Expression,omitempty"`
}

type getQueryStateInput struct {
	QueryID string `json:"QueryId"`
}
type getQueryStateOutput struct {
	Error string `json:"Error,omitempty"`
	State string `json:"State"`
}

type getQueryStatisticsInput struct {
	QueryID string `json:"QueryId"`
}
type getQueryStatisticsOutput struct {
	ExecutionStatistics *ExecutionStatistics `json:"ExecutionStatistics,omitempty"`
	PlanningStatistics  *PlanningStatistics  `json:"PlanningStatistics,omitempty"`
	QuerySubmissionTime *string              `json:"QuerySubmissionTime,omitempty"`
}

type getTableObjectsInput struct {
	CatalogID     string `json:"CatalogId,omitempty"`
	DatabaseName  string `json:"DatabaseName,omitempty"`
	TableName     string `json:"TableName,omitempty"`
	TransactionID string `json:"TransactionId,omitempty"`
	NextToken     string `json:"NextToken,omitempty"`
	MaxResults    int    `json:"MaxResults,omitempty"`
}
type getTableObjectsOutput struct {
	NextToken string                        `json:"NextToken,omitempty"`
	Objects   []PartitionedTableObjectsList `json:"Objects,omitempty"`
}

type getTemporaryDataLocationCredentialsInput struct {
	ResourceArn              string        `json:"ResourceArn"`
	Permissions              []string      `json:"Permissions,omitempty"`
	DurationSeconds          *int32        `json:"DurationSeconds,omitempty"`
	AuditContext             *AuditContext `json:"AuditContext,omitempty"`
	SupportedPermissionTypes []string      `json:"SupportedPermissionTypes,omitempty"`
}
type getTemporaryDataLocationCredentialsOutput struct {
	Credentials *TemporaryCredentials `json:"Credentials,omitempty"`
	Expiration  *string               `json:"Expiration,omitempty"`
}

type getTemporaryGluePartitionCredentialsInput struct {
	TableArn                 string        `json:"TableArn"`
	Partition                *Partition    `json:"Partition,omitempty"`
	Permissions              []string      `json:"Permissions,omitempty"`
	DurationSeconds          *int32        `json:"DurationSeconds,omitempty"`
	AuditContext             *AuditContext `json:"AuditContext,omitempty"`
	SupportedPermissionTypes []string      `json:"SupportedPermissionTypes,omitempty"`
}
type getTemporaryGluePartitionCredentialsOutput struct {
	Credentials *TemporaryCredentials `json:"Credentials,omitempty"`
	Expiration  *string               `json:"Expiration,omitempty"`
}

type getTemporaryGlueTableCredentialsInput struct {
	TableArn                 string        `json:"TableArn"`
	Permissions              []string      `json:"Permissions,omitempty"`
	DurationSeconds          *int32        `json:"DurationSeconds,omitempty"`
	AuditContext             *AuditContext `json:"AuditContext,omitempty"`
	SupportedPermissionTypes []string      `json:"SupportedPermissionTypes,omitempty"`
}
type getTemporaryGlueTableCredentialsOutput struct {
	Credentials *TemporaryCredentials `json:"Credentials,omitempty"`
	Expiration  *string               `json:"Expiration,omitempty"`
}

type getWorkUnitResultsInput struct {
	QueryID       string `json:"QueryId"`
	WorkUnitToken string `json:"WorkUnitToken"`
}
type getWorkUnitResultsOutput struct{}

type getWorkUnitsInput struct {
	NextToken string `json:"NextToken,omitempty"`
	PageSize  *int32 `json:"PageSize,omitempty"`
	QueryID   string `json:"QueryId"`
}
type getWorkUnitsOutput struct {
	NextToken      string          `json:"NextToken,omitempty"`
	QueryID        string          `json:"QueryId,omitempty"`
	WorkUnitRanges []WorkUnitRange `json:"WorkUnitRanges"`
}

type listTableStorageOptimizersInput struct {
	CatalogID            string `json:"CatalogId,omitempty"`
	DatabaseName         string `json:"DatabaseName"`
	TableName            string `json:"TableName"`
	StorageOptimizerType string `json:"StorageOptimizerType,omitempty"`
	NextToken            string `json:"NextToken,omitempty"`
}
type listTableStorageOptimizersOutput struct {
	NextToken            string             `json:"NextToken,omitempty"`
	StorageOptimizerList []StorageOptimizer `json:"StorageOptimizerList"`
}

type searchDatabasesByLFTagsInput struct {
	CatalogID  string  `json:"CatalogId,omitempty"`
	NextToken  string  `json:"NextToken,omitempty"`
	Expression []LFTag `json:"Expression"`
}
type searchDatabasesByLFTagsOutput struct {
	NextToken    string           `json:"NextToken,omitempty"`
	DatabaseList []TaggedDatabase `json:"DatabaseList"`
}

type searchTablesByLFTagsInput struct {
	CatalogID  string  `json:"CatalogId,omitempty"`
	NextToken  string  `json:"NextToken,omitempty"`
	Expression []LFTag `json:"Expression"`
}
type searchTablesByLFTagsOutput struct {
	NextToken string        `json:"NextToken,omitempty"`
	TableList []TaggedTable `json:"TableList"`
}

type startQueryPlanningInput struct {
	QueryPlanningContext QueryPlanningContext `json:"QueryPlanningContext"`
	QueryString          string               `json:"QueryString"`
}
type startQueryPlanningOutput struct {
	QueryID string `json:"QueryId"`
}

type updateDataCellsFilterInput struct {
	TableData *DataCellsFilter `json:"TableData"`
}
type updateDataCellsFilterOutput struct{}

type updateLFTagExpressionInput struct {
	Name        string  `json:"Name"`
	CatalogID   string  `json:"CatalogId,omitempty"`
	Description string  `json:"Description,omitempty"`
	Expression  []LFTag `json:"Expression,omitempty"`
}
type updateLFTagExpressionOutput struct{}

type updateLakeFormationIdentityCenterConfigurationInput struct {
	CatalogID         string                          `json:"CatalogId,omitempty"`
	ExternalFiltering *ExternalFilteringConfiguration `json:"ExternalFiltering,omitempty"`
	ApplicationStatus string                          `json:"ApplicationStatus,omitempty"`
}
type updateLakeFormationIdentityCenterConfigurationOutput struct{}

type updateTableObjectsInput struct {
	CatalogID       string           `json:"CatalogId,omitempty"`
	DatabaseName    string           `json:"DatabaseName,omitempty"`
	TableName       string           `json:"TableName,omitempty"`
	TransactionID   string           `json:"TransactionId,omitempty"`
	WriteOperations []WriteOperation `json:"WriteOperations,omitempty"`
}
type updateTableObjectsOutput struct{}

type updateTableStorageOptimizerInput struct {
	StorageOptimizerConfig map[string]map[string]string `json:"StorageOptimizerConfig,omitempty"`
	CatalogID              string                       `json:"CatalogId,omitempty"`
	DatabaseName           string                       `json:"DatabaseName"`
	TableName              string                       `json:"TableName"`
}
type updateTableStorageOptimizerOutput struct {
	Result string `json:"Result,omitempty"`
}
