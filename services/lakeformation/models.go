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
	CatalogID      string `json:"CatalogId,omitempty"`
	InstanceArn    string `json:"InstanceArn,omitempty"`
	ApplicationArn string `json:"ApplicationArn,omitempty"`
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
	CatalogID   string `json:"CatalogId,omitempty"`
	InstanceArn string `json:"InstanceArn,omitempty"`
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
