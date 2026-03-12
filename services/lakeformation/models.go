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
