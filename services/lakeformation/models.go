package lakeformation

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// DataLakeSettings contains the data lake settings for an account.
type DataLakeSettings struct {
	DataLakeAdmins                   []DataLakePrincipal    `json:"DataLakeAdmins,omitempty"`
	ReadOnlyAdmins                   []DataLakePrincipal    `json:"ReadOnlyAdmins,omitempty"`
	CreateDatabaseDefaultPermissions []PrincipalPermissions `json:"CreateDatabaseDefaultPermissions,omitempty"`
	CreateTableDefaultPermissions    []PrincipalPermissions `json:"CreateTableDefaultPermissions,omitempty"`
	TrustedResourceOwners            []string               `json:"TrustedResourceOwners,omitempty"`
	Parameters                       map[string]string      `json:"Parameters,omitempty"`
	ExternalDataFilteringAllowList   []DataLakePrincipal    `json:"ExternalDataFilteringAllowList,omitempty"`
	AllowExternalDataFiltering       *bool                  `json:"AllowExternalDataFiltering,omitempty"`
	AllowFullTableExternalDataAccess *bool                  `json:"AllowFullTableExternalDataAccess,omitempty"`
	AuthorizedSessionTagValueList    []string               `json:"AuthorizedSessionTagValueList,omitempty"`
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

// ResourceInfo holds registration info for a data lake resource. This is the
// internal/persisted representation -- LastModified is a *time.Time for
// business-logic convenience. HTTP responses must go through
// toResourceInfoWire, which re-encodes LastModified as epoch seconds to
// match the real wire format (see resourceInfoWire).
type ResourceInfo struct {
	LastModified                 *time.Time `json:"LastModified,omitempty"`
	ResourceArn                  string     `json:"ResourceArn"`
	RoleArn                      string     `json:"RoleArn"`
	ExpectedResourceOwnerAccount string     `json:"ExpectedResourceOwnerAccount,omitempty"`
	VerificationStatus           string     `json:"VerificationStatus,omitempty"`
	HybridAccessEnabled          bool       `json:"HybridAccessEnabled,omitempty"`
	WithFederation               bool       `json:"WithFederation,omitempty"`
	WithPrivilegedAccess         bool       `json:"WithPrivilegedAccess,omitempty"`
}

// resourceInfoWire is the wire representation of ResourceInfo returned by
// DescribeResource/ListResources. LastModified is emitted as epoch seconds
// (a JSON number) via awstime.Epoch, matching the real
// types.ResourceInfo.LastModified wire format -- the aws-sdk-go-v2
// deserializer rejects Go's default RFC3339-string time.Time encoding here.
type resourceInfoWire struct {
	LastModified                 *float64 `json:"LastModified,omitempty"`
	ResourceArn                  string   `json:"ResourceArn"`
	RoleArn                      string   `json:"RoleArn"`
	ExpectedResourceOwnerAccount string   `json:"ExpectedResourceOwnerAccount,omitempty"`
	VerificationStatus           string   `json:"VerificationStatus,omitempty"`
	HybridAccessEnabled          bool     `json:"HybridAccessEnabled,omitempty"`
	WithFederation               bool     `json:"WithFederation,omitempty"`
	WithPrivilegedAccess         bool     `json:"WithPrivilegedAccess,omitempty"`
}

// toResourceInfoWire converts a ResourceInfo to its wire representation.
func toResourceInfoWire(ri *ResourceInfo) *resourceInfoWire {
	if ri == nil {
		return nil
	}

	w := &resourceInfoWire{
		ResourceArn:                  ri.ResourceArn,
		RoleArn:                      ri.RoleArn,
		ExpectedResourceOwnerAccount: ri.ExpectedResourceOwnerAccount,
		VerificationStatus:           ri.VerificationStatus,
		HybridAccessEnabled:          ri.HybridAccessEnabled,
		WithFederation:               ri.WithFederation,
		WithPrivilegedAccess:         ri.WithPrivilegedAccess,
	}

	if ri.LastModified != nil {
		e := awstime.Epoch(*ri.LastModified)
		w.LastModified = &e
	}

	return w
}

// toResourceInfoWireList converts a slice of ResourceInfo to their wire representation.
func toResourceInfoWireList(list []*ResourceInfo) []*resourceInfoWire {
	out := make([]*resourceInfoWire, len(list))
	for i, ri := range list {
		out[i] = toResourceInfoWire(ri)
	}

	return out
}

// rfc3339ToEpoch parses an RFC3339-formatted timestamp string (the internal
// storage format used by several LakeFormation domain fields, e.g.
// LFOptIn.LastModified and Transaction.TransactionStartTime/EndTime) and
// returns its value as an epoch-seconds float, matching the wire format the
// real SDK expects. Returns nil for an empty or unparseable string, matching
// AWS's behavior of omitting a timestamp field that has no value yet (e.g. a
// still-active transaction has no TransactionEndTime).
func rfc3339ToEpoch(s string) *float64 {
	if s == "" {
		return nil
	}

	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}

	e := awstime.Epoch(t)

	return &e
}

// LFTag represents a Lake Formation tag with its allowed values.
type LFTag struct {
	CatalogID string   `json:"CatalogId,omitempty"`
	TagKey    string   `json:"TagKey"`
	TagValues []string `json:"TagValues"`
}

// Resource describes the resource to which permissions are granted.
type Resource struct {
	Catalog          *CatalogResource          `json:"Catalog,omitempty"`
	Database         *DatabaseResource         `json:"Database,omitempty"`
	Table            *TableResource            `json:"Table,omitempty"`
	TableWithColumns *TableWithColumnsResource `json:"TableWithColumns,omitempty"`
	DataLocation     *DataLocationResource     `json:"DataLocation,omitempty"`
	DataCellsFilter  *DataCellsFilterResource  `json:"DataCellsFilter,omitempty"`
	LFTag            *LFTagKeyResource         `json:"LFTag,omitempty"`
	LFTagExpression  *LFTagExpressionResource  `json:"LFTagExpression,omitempty"`
	LFTagPolicy      *LFTagPolicyResource      `json:"LFTagPolicy,omitempty"`
}

// TableWithColumnsResource represents a table resource with column-level access.
type TableWithColumnsResource struct {
	ColumnWildcard *ColumnWildcard `json:"ColumnWildcard,omitempty"`
	CatalogID      string          `json:"CatalogId,omitempty"`
	DatabaseName   string          `json:"DatabaseName"`
	Name           string          `json:"Name"`
	ColumnNames    []string        `json:"ColumnNames,omitempty"`
}

// CatalogResource represents the data catalog resource.
type CatalogResource struct {
	ID string `json:"Id,omitempty"`
}

// DatabaseResource represents a database resource.
type DatabaseResource struct {
	Name      string `json:"Name"`
	CatalogID string `json:"CatalogId,omitempty"`
}

// TableResource represents a table resource.
type TableResource struct {
	TableWildcard *TableWildcard `json:"TableWildcard,omitempty"`
	CatalogID     string         `json:"CatalogId,omitempty"`
	DatabaseName  string         `json:"DatabaseName"`
	Name          string         `json:"Name,omitempty"`
}

// TableWildcard is a structure that indicates all tables in a database.
type TableWildcard struct{}

// ColumnWildcard is a wildcard object, consisting of an optional list of
// excluded column names.
type ColumnWildcard struct {
	ExcludedColumnNames []string `json:"ExcludedColumnNames,omitempty"`
}

// Condition is a Lake Formation condition (Cedar expression) which applies to
// permissions and opt-ins.
type Condition struct {
	Expression string `json:"Expression,omitempty"`
}

// DataCellsFilterResource identifies a data cells filter as a permission resource.
type DataCellsFilterResource struct {
	TableCatalogID string `json:"TableCatalogId,omitempty"`
	DatabaseName   string `json:"DatabaseName,omitempty"`
	TableName      string `json:"TableName,omitempty"`
	Name           string `json:"Name,omitempty"`
}

// LFTagKeyResource identifies an LF-tag key/values pair as a permission resource.
type LFTagKeyResource struct {
	CatalogID string   `json:"CatalogId,omitempty"`
	TagKey    string   `json:"TagKey"`
	TagValues []string `json:"TagValues"`
}

// LFTagExpressionResource identifies a saved LF-tag expression as a permission resource.
type LFTagExpressionResource struct {
	Name      string `json:"Name"`
	CatalogID string `json:"CatalogId,omitempty"`
}

// LFTagPolicyResource identifies an LF-tag policy (a set of LF-tag conditions,
// or a reference to a saved expression) applying to DATABASE or TABLE resources.
type LFTagPolicyResource struct {
	CatalogID      string  `json:"CatalogId,omitempty"`
	ResourceType   string  `json:"ResourceType"`
	ExpressionName string  `json:"ExpressionName,omitempty"`
	Expression     []LFTag `json:"Expression,omitempty"`
}

// DataLocationResource represents an Amazon S3 data location resource.
type DataLocationResource struct {
	ResourceArn string `json:"ResourceArn"`
	CatalogID   string `json:"CatalogId,omitempty"`
}

// PermissionEntry associates a principal and resource with a set of permissions.
type PermissionEntry struct {
	Principal                  *DataLakePrincipal `json:"Principal,omitempty"`
	Resource                   *Resource          `json:"Resource,omitempty"`
	Condition                  *Condition         `json:"Condition,omitempty"`
	LastUpdated                *time.Time         `json:"LastUpdated,omitempty"`
	LastUpdatedBy              string             `json:"LastUpdatedBy,omitempty"`
	Permissions                []string           `json:"Permissions,omitempty"`
	PermissionsWithGrantOption []string           `json:"PermissionsWithGrantOption,omitempty"`
}

// BatchPermissionsRequestEntry is a single entry of a BatchGrantPermissions or
// BatchRevokePermissions request. Unlike PermissionEntry (used directly by
// GrantPermissions/RevokePermissions), the real AWS API requires a caller-supplied
// Id per entry so BatchGrantPermissionsOutput/BatchRevokePermissionsOutput's
// Failures can be correlated back to the request that produced them.
type BatchPermissionsRequestEntry struct {
	Principal                  *DataLakePrincipal `json:"Principal,omitempty"`
	Resource                   *Resource          `json:"Resource,omitempty"`
	Condition                  *Condition         `json:"Condition,omitempty"`
	ID                         string             `json:"Id"`
	Permissions                []string           `json:"Permissions,omitempty"`
	PermissionsWithGrantOption []string           `json:"PermissionsWithGrantOption,omitempty"`
}

// BatchFailureEntry reports a failure for a single entry in a batch operation.
type BatchFailureEntry struct {
	RequestEntry *BatchPermissionsRequestEntry `json:"RequestEntry,omitempty"`
	Error        *errorDetail                  `json:"Error,omitempty"`
}

// permissionEntryWire is the wire representation of PermissionEntry returned
// by ListPermissions/GetEffectivePermissionsForPath. LastUpdated is emitted
// as epoch seconds (a JSON number) via awstime.Epoch, matching the real
// types.PrincipalResourcePermissions.LastUpdated wire format -- the
// aws-sdk-go-v2 deserializer rejects Go's default RFC3339-string time.Time
// encoding here.
type permissionEntryWire struct {
	Principal                  *DataLakePrincipal `json:"Principal,omitempty"`
	Resource                   *Resource          `json:"Resource,omitempty"`
	Condition                  *Condition         `json:"Condition,omitempty"`
	LastUpdated                *float64           `json:"LastUpdated,omitempty"`
	LastUpdatedBy              string             `json:"LastUpdatedBy,omitempty"`
	Permissions                []string           `json:"Permissions,omitempty"`
	PermissionsWithGrantOption []string           `json:"PermissionsWithGrantOption,omitempty"`
}

// toPermissionEntryWire converts a PermissionEntry to its wire representation.
func toPermissionEntryWire(p *PermissionEntry) *permissionEntryWire {
	if p == nil {
		return nil
	}

	w := &permissionEntryWire{
		Principal:                  p.Principal,
		Resource:                   p.Resource,
		Permissions:                p.Permissions,
		PermissionsWithGrantOption: p.PermissionsWithGrantOption,
		Condition:                  p.Condition,
		LastUpdatedBy:              p.LastUpdatedBy,
	}

	if p.LastUpdated != nil {
		e := awstime.Epoch(*p.LastUpdated)
		w.LastUpdated = &e
	}

	return w
}

// toPermissionEntryWireList converts a slice of PermissionEntry to their wire representation.
func toPermissionEntryWireList(list []*PermissionEntry) []*permissionEntryWire {
	out := make([]*permissionEntryWire, len(list))
	for i, p := range list {
		out[i] = toPermissionEntryWire(p)
	}

	return out
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
	ResourceArn                  string `json:"ResourceArn"`
	RoleArn                      string `json:"RoleArn"`
	ExpectedResourceOwnerAccount string `json:"ExpectedResourceOwnerAccount,omitempty"`
	UseServiceLinkedRole         bool   `json:"UseServiceLinkedRole,omitempty"`
	WithFederation               bool   `json:"WithFederation,omitempty"`
	WithPrivilegedAccess         bool   `json:"WithPrivilegedAccess,omitempty"`
	HybridAccessEnabled          bool   `json:"HybridAccessEnabled,omitempty"`
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
	ResourceInfo *resourceInfoWire `json:"ResourceInfo"`
}

// listResourcesInput is the request body for ListResources.
type listResourcesInput struct {
	NextToken  string `json:"NextToken,omitempty"`
	MaxResults int    `json:"MaxResults,omitempty"`
}

// listResourcesOutput is the response body for ListResources.
type listResourcesOutput struct {
	NextToken        string              `json:"NextToken,omitempty"`
	ResourceInfoList []*resourceInfoWire `json:"ResourceInfoList"`
}

// grantPermissionsInput is the request body for GrantPermissions.
type grantPermissionsInput struct {
	CatalogID                  string             `json:"CatalogId,omitempty"`
	Principal                  *DataLakePrincipal `json:"Principal"`
	Resource                   *Resource          `json:"Resource"`
	Condition                  *Condition         `json:"Condition,omitempty"`
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
	Condition                  *Condition         `json:"Condition,omitempty"`
	Permissions                []string           `json:"Permissions"`
	PermissionsWithGrantOption []string           `json:"PermissionsWithGrantOption,omitempty"`
}

// revokePermissionsOutput is the response body for RevokePermissions (empty).
type revokePermissionsOutput struct{}

// listPermissionsInput is the request body for ListPermissions. Note: the
// real API filters by a nested Resource object (matching Grant/RevokePermissions'
// shape), NOT a flat ResourceArn string -- GetEffectivePermissionsForPath is the
// only Lake Formation op that takes a flat ResourceArn.
type listPermissionsInput struct {
	Principal      *DataLakePrincipal `json:"Principal,omitempty"`
	Resource       *Resource          `json:"Resource,omitempty"`
	NextToken      string             `json:"NextToken,omitempty"`
	ResourceType   string             `json:"ResourceType,omitempty"`
	IncludeRelated string             `json:"IncludeRelated,omitempty"`
	MaxResults     int                `json:"MaxResults,omitempty"`
}

// listPermissionsOutput is the response body for ListPermissions.
type listPermissionsOutput struct {
	NextToken                    string                 `json:"NextToken,omitempty"`
	PrincipalResourcePermissions []*permissionEntryWire `json:"PrincipalResourcePermissions"`
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
	CatalogID string                          `json:"CatalogId,omitempty"`
	Entries   []*BatchPermissionsRequestEntry `json:"Entries"`
}

// batchGrantPermissionsOutput is the response body for BatchGrantPermissions.
type batchGrantPermissionsOutput struct {
	Failures []BatchFailureEntry `json:"Failures"`
}

// batchRevokePermissionsInput is the request body for BatchRevokePermissions.
type batchRevokePermissionsInput struct {
	CatalogID string                          `json:"CatalogId,omitempty"`
	Entries   []*BatchPermissionsRequestEntry `json:"Entries"`
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

// RowFilter holds a filter expression for a data cells filter.
type RowFilter struct {
	AllRowsWildcard  *AllRowsWildcard `json:"AllRowsWildcard,omitempty"`
	FilterExpression string           `json:"FilterExpression,omitempty"`
}

// AllRowsWildcard indicates that all rows in a data cells filter are included.
type AllRowsWildcard struct{}

// DataCellsFilter holds the definition of a cell-level access filter.
type DataCellsFilter struct {
	RowFilter      *RowFilter      `json:"RowFilter,omitempty"`
	ColumnWildcard *ColumnWildcard `json:"ColumnWildcard,omitempty"`
	TableCatalogID string          `json:"TableCatalogId"`
	DatabaseName   string          `json:"DatabaseName"`
	TableName      string          `json:"TableName"`
	Name           string          `json:"Name"`
	VersionID      string          `json:"VersionId,omitempty"`
	ColumnNames    []string        `json:"ColumnNames,omitempty"`
}

// LFTagExpression holds a saved, named LF-tag expression.
type LFTagExpression struct {
	Name        string  `json:"Name"`
	Description string  `json:"Description,omitempty"`
	CatalogID   string  `json:"CatalogId,omitempty"`
	Expression  []LFTag `json:"Expression,omitempty"`
}

// Transaction represents an in-flight Lake Formation governed table
// transaction. This is the internal representation -- TransactionStartTime
// and TransactionEndTime are RFC3339 strings for business-logic convenience.
// HTTP responses must go through toTransactionWire, which re-encodes them as
// epoch seconds to match the real wire format (see transactionWire).
type Transaction struct {
	TransactionID        string `json:"TransactionId"`
	TransactionStatus    string `json:"TransactionStatus"`
	TransactionStartTime string `json:"TransactionStartTime,omitempty"`
	TransactionEndTime   string `json:"TransactionEndTime,omitempty"`
}

// transactionWire is the wire representation of Transaction returned by
// DescribeTransaction/ListTransactions. TransactionStartTime/EndTime are
// emitted as epoch seconds (JSON numbers) via rfc3339ToEpoch, matching the
// real types.TransactionDescription wire format.
type transactionWire struct {
	TransactionStartTime *float64 `json:"TransactionStartTime,omitempty"`
	TransactionEndTime   *float64 `json:"TransactionEndTime,omitempty"`
	TransactionID        string   `json:"TransactionId"`
	TransactionStatus    string   `json:"TransactionStatus"`
}

// toTransactionWire converts a Transaction to its wire representation.
func toTransactionWire(t *Transaction) *transactionWire {
	if t == nil {
		return nil
	}

	return &transactionWire{
		TransactionID:        t.TransactionID,
		TransactionStatus:    t.TransactionStatus,
		TransactionStartTime: rfc3339ToEpoch(t.TransactionStartTime),
		TransactionEndTime:   rfc3339ToEpoch(t.TransactionEndTime),
	}
}

// toTransactionWireList converts a slice of Transaction to their wire representation.
func toTransactionWireList(list []*Transaction) []*transactionWire {
	out := make([]*transactionWire, len(list))
	for i, t := range list {
		out[i] = toTransactionWire(t)
	}

	return out
}

// IdentityCenterConfiguration holds the IAM Identity Center integration
// configuration for internal storage/persistence. Its JSON tags are for the
// snapshot/restore DTO shape only, NOT the AWS wire shape -- the HTTP
// response is built field-by-field in
// describeLakeFormationIdentityCenterConfigurationOutput. ApplicationStatus
// is tracked here (set only via
// UpdateLakeFormationIdentityCenterConfigurationInput.ApplicationStatus) but
// deliberately excluded from that wire output: the real
// DescribeLakeFormationIdentityCenterConfigurationOutput has no
// ApplicationStatus member at all (confirmed against
// deserializers.go's awsRestjson1_deserializeOpDocumentDescribeLakeFormationIdentityCenterConfigurationOutput
// case list) -- ApplicationStatus is real only as an Update *request* field.
type IdentityCenterConfiguration struct {
	ExternalFiltering   *ExternalFilteringConfiguration `json:"ExternalFiltering,omitempty"`
	CatalogID           string                          `json:"CatalogId,omitempty"`
	InstanceArn         string                          `json:"InstanceArn,omitempty"`
	ApplicationArn      string                          `json:"ApplicationArn,omitempty"`
	ApplicationStatus   string                          `json:"ApplicationStatus,omitempty"`
	ShareRecipients     []DataLakePrincipal             `json:"ShareRecipients,omitempty"`
	ServiceIntegrations []ServiceIntegration            `json:"ServiceIntegrations,omitempty"`
}

// LFOptIn associates a principal and resource for opt-in enforcement. This is
// the internal representation -- LastModified is an RFC3339 string for
// business-logic convenience. HTTP responses must go through
// toLFOptInWire, which re-encodes it as epoch seconds to match the real wire
// format (see lfOptInWire).
type LFOptIn struct {
	Principal     *DataLakePrincipal `json:"Principal,omitempty"`
	Resource      *Resource          `json:"Resource,omitempty"`
	Condition     *Condition         `json:"Condition,omitempty"`
	LastModified  string             `json:"LastModified,omitempty"`
	LastUpdatedBy string             `json:"LastUpdatedBy,omitempty"`
}

// lfOptInWire is the wire representation of LFOptIn returned by
// ListLakeFormationOptIns. LastModified is emitted as epoch seconds (a JSON
// number) via rfc3339ToEpoch, matching the real
// types.LakeFormationOptInsInfo.LastModified wire format.
type lfOptInWire struct {
	Principal     *DataLakePrincipal `json:"Principal,omitempty"`
	Resource      *Resource          `json:"Resource,omitempty"`
	Condition     *Condition         `json:"Condition,omitempty"`
	LastModified  *float64           `json:"LastModified,omitempty"`
	LastUpdatedBy string             `json:"LastUpdatedBy,omitempty"`
}

// toLFOptInWire converts an LFOptIn to its wire representation.
func toLFOptInWire(o *LFOptIn) *lfOptInWire {
	if o == nil {
		return nil
	}

	return &lfOptInWire{
		Principal:     o.Principal,
		Resource:      o.Resource,
		Condition:     o.Condition,
		LastModified:  rfc3339ToEpoch(o.LastModified),
		LastUpdatedBy: o.LastUpdatedBy,
	}
}

// toLFOptInWireList converts a slice of LFOptIn to their wire representation.
func toLFOptInWireList(list []*LFOptIn) []*lfOptInWire {
	out := make([]*lfOptInWire, len(list))
	for i, o := range list {
		out[i] = toLFOptInWire(o)
	}

	return out
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
// Expiration is emitted as epoch seconds (a JSON number) via awstime.Epoch,
// matching the real AssumeDecoratedRoleWithSAMLOutput.Expiration wire format
// -- the aws-sdk-go-v2 deserializer rejects an RFC3339 string here.
type SAMLCredentials struct {
	AccessKeyID     string  `json:"AccessKeyId,omitempty"`
	SecretAccessKey string  `json:"SecretAccessKey,omitempty"`
	SessionToken    string  `json:"SessionToken,omitempty"`
	Expiration      float64 `json:"Expiration,omitempty"`
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
	CatalogID           string                          `json:"CatalogId,omitempty"`
	InstanceArn         string                          `json:"InstanceArn,omitempty"`
	ExternalFiltering   *ExternalFilteringConfiguration `json:"ExternalFiltering,omitempty"`
	ShareRecipients     []DataLakePrincipal             `json:"ShareRecipients,omitempty"`
	ServiceIntegrations []ServiceIntegration            `json:"ServiceIntegrations,omitempty"`
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
	Condition *Condition         `json:"Condition,omitempty"`
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
	ResourceArn                  string `json:"ResourceArn"`
	RoleArn                      string `json:"RoleArn"`
	ExpectedResourceOwnerAccount string `json:"ExpectedResourceOwnerAccount,omitempty"`
	WithFederation               bool   `json:"WithFederation,omitempty"`
	HybridAccessEnabled          bool   `json:"HybridAccessEnabled,omitempty"`
}

// updateResourceOutput is the response body for UpdateResource (empty).
type updateResourceOutput struct{}

// startTransactionInput is the request body for StartTransaction.
type startTransactionInput struct {
	TransactionType string `json:"TransactionType,omitempty"`
}

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
	TransactionDescription *transactionWire `json:"TransactionDescription,omitempty"`
}

// listTransactionsInput is the request body for ListTransactions.
type listTransactionsInput struct {
	StatusFilter string `json:"StatusFilter,omitempty"`
	NextToken    string `json:"NextToken,omitempty"`
	MaxResults   int    `json:"MaxResults,omitempty"`
}

// listTransactionsOutput is the response body for ListTransactions.
type listTransactionsOutput struct {
	NextToken    string             `json:"NextToken,omitempty"`
	Transactions []*transactionWire `json:"Transactions"`
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
// LFTagsOnColumns is []ColumnLFTag (Name + LFTags), not a flat []LFTagPair --
// matches types.GetResourceLFTagsOutput.LFTagsOnColumns
// (api_op_GetResourceLFTags.go:53, aws-sdk-go-v2/service/lakeformation
// @v1.50.4).
type getResourceLFTagsOutput struct {
	LFTagOnDatabase []LFTagPair   `json:"LFTagOnDatabase,omitempty"`
	LFTagsOnColumns []ColumnLFTag `json:"LFTagsOnColumns,omitempty"`
	LFTagsOnTable   []LFTagPair   `json:"LFTagsOnTable,omitempty"`
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
	Condition *Condition         `json:"Condition,omitempty"`
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
	NextToken                   string         `json:"NextToken,omitempty"`
	LakeFormationOptInsInfoList []*lfOptInWire `json:"LakeFormationOptInsInfoList"`
}

// getDataLakePrincipalOutput is the response body for GetDataLakePrincipal.
type getDataLakePrincipalOutput struct {
	Identity string `json:"Identity,omitempty"`
}

// --- New types for 24 additional operations ---

// RedshiftConnect describes the Redshift Connect service-integration
// authorization state, matching the real types.RedshiftConnect.
type RedshiftConnect struct {
	Authorization string `json:"Authorization,omitempty"`
}

// RedshiftScopeUnion wraps a single Redshift-scoped service integration
// entry, matching the real types.RedshiftScopeUnion (currently a
// single-member union: RedshiftConnect).
type RedshiftScopeUnion struct {
	RedshiftConnect *RedshiftConnect `json:"RedshiftConnect,omitempty"`
}

// ServiceIntegration is one entry of
// CreateLakeFormationIdentityCenterConfigurationInput.ServiceIntegrations /
// UpdateLakeFormationIdentityCenterConfigurationInput.ServiceIntegrations /
// DescribeLakeFormationIdentityCenterConfigurationOutput.ServiceIntegrations,
// matching the real types.ServiceIntegrationUnion (currently a single-member
// union: Redshift).
type ServiceIntegration struct {
	Redshift []RedshiftScopeUnion `json:"Redshift,omitempty"`
}

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

// TemporaryCredentials holds temporary AWS credentials. Expiration is
// emitted as epoch seconds (a JSON number) via awstime.Epoch, matching the
// real types.TemporaryCredentials.Expiration wire format.
type TemporaryCredentials struct {
	AccessKeyID     string  `json:"AccessKeyId,omitempty"`
	SecretAccessKey string  `json:"SecretAccessKey,omitempty"`
	SessionToken    string  `json:"SessionToken,omitempty"`
	Expiration      float64 `json:"Expiration,omitempty"`
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

// describeLakeFormationIdentityCenterConfigurationOutput deliberately has no
// ApplicationStatus field: it is not a member of the real
// DescribeLakeFormationIdentityCenterConfigurationOutput at all (that name
// is real only as UpdateLakeFormationIdentityCenterConfigurationInput's
// request field -- a real key surfacing on the wrong op/direction). Also
// missing ResourceShare (*string, the RAM resource-share ARN AWS creates
// when ShareRecipients is set): disclosed in PARITY.md, not fabricated here
// -- this backend has no region available where the ARN would be
// synthesized and no real RAM integration (same class as the
// already-documented AdditionalDetails/RAM gap).
type describeLakeFormationIdentityCenterConfigurationOutput struct {
	ExternalFiltering   *ExternalFilteringConfiguration `json:"ExternalFiltering,omitempty"`
	CatalogID           string                          `json:"CatalogId,omitempty"`
	InstanceArn         string                          `json:"InstanceArn,omitempty"`
	ApplicationArn      string                          `json:"ApplicationArn,omitempty"`
	ShareRecipients     []DataLakePrincipal             `json:"ShareRecipients,omitempty"`
	ServiceIntegrations []ServiceIntegration            `json:"ServiceIntegrations,omitempty"`
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
	NextToken                    string                 `json:"NextToken,omitempty"`
	PrincipalResourcePermissions []*permissionEntryWire `json:"PrincipalResourcePermissions"`
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

// getTemporaryDataLocationCredentialsInput is the request body for
// GetTemporaryDataLocationCredentials. WIRE-BREAKING BUG FIXED: this used to
// be shaped like the GetTemporaryGlue*Credentials sibling family
// (ResourceArn/Permissions/SupportedPermissionTypes) -- a sibling-copy
// mistake. The real GetTemporaryDataLocationCredentialsInput has no
// ResourceArn/Permissions/SupportedPermissionTypes members at all; it takes
// DataLocations ([]string, plural) and CredentialsScope instead (confirmed
// against api_op_GetTemporaryDataLocationCredentials.go and
// serializers.go's awsRestjson1_serializeOpDocumentGetTemporaryDataLocationCredentialsInput).
// A real aws-sdk-go-v2 client's request always sent {"DataLocations":
// [...]}, which gopherstack's old ResourceArn field could never read --
// every real-client call failed the "ResourceArn is required" check.
type getTemporaryDataLocationCredentialsInput struct {
	DurationSeconds  *int32        `json:"DurationSeconds,omitempty"`
	AuditContext     *AuditContext `json:"AuditContext,omitempty"`
	CredentialsScope string        `json:"CredentialsScope,omitempty"`
	DataLocations    []string      `json:"DataLocations,omitempty"`
}

// getTemporaryDataLocationCredentialsOutput is the response body for
// GetTemporaryDataLocationCredentials. Real AWS nests Expiration inside
// Credentials (see types.TemporaryCredentials) rather than at the top level
// -- unlike GetTemporaryGluePartitionCredentials/GetTemporaryGlueTableCredentials,
// which return the credential fields flat. AccessibleDataLocations and
// CredentialsScope are real response members that were entirely missing
// (deserializers.go's GetTemporaryDataLocationCredentialsOutput case list:
// AccessibleDataLocations, Credentials, CredentialsScope).
type getTemporaryDataLocationCredentialsOutput struct {
	Credentials             *TemporaryCredentials `json:"Credentials,omitempty"`
	CredentialsScope        string                `json:"CredentialsScope,omitempty"`
	AccessibleDataLocations []string              `json:"AccessibleDataLocations,omitempty"`
}

type getTemporaryGluePartitionCredentialsInput struct {
	TableArn                 string        `json:"TableArn"`
	Partition                *Partition    `json:"Partition,omitempty"`
	Permissions              []string      `json:"Permissions,omitempty"`
	DurationSeconds          *int32        `json:"DurationSeconds,omitempty"`
	AuditContext             *AuditContext `json:"AuditContext,omitempty"`
	SupportedPermissionTypes []string      `json:"SupportedPermissionTypes,omitempty"`
}

// getTemporaryGluePartitionCredentialsOutput is the response body for
// GetTemporaryGluePartitionCredentials. Real AWS returns these fields flat
// (no nested "Credentials" object) with Expiration as epoch seconds -- see
// GetTemporaryGluePartitionCredentialsOutput in the aws-sdk-go-v2 model.
type getTemporaryGluePartitionCredentialsOutput struct {
	AccessKeyID     string  `json:"AccessKeyId,omitempty"`
	SecretAccessKey string  `json:"SecretAccessKey,omitempty"`
	SessionToken    string  `json:"SessionToken,omitempty"`
	Expiration      float64 `json:"Expiration,omitempty"`
}

// getTemporaryGlueTableCredentialsInput is the request body for
// GetTemporaryGlueTableCredentials. S3Path (the Amazon S3 path for the
// table) is a real request member that was entirely unparsed -- confirmed
// in api_op_GetTemporaryGlueTableCredentials.go's
// GetTemporaryGlueTableCredentialsInput.
type getTemporaryGlueTableCredentialsInput struct {
	TableArn                 string        `json:"TableArn"`
	S3Path                   string        `json:"S3Path,omitempty"`
	Permissions              []string      `json:"Permissions,omitempty"`
	DurationSeconds          *int32        `json:"DurationSeconds,omitempty"`
	AuditContext             *AuditContext `json:"AuditContext,omitempty"`
	SupportedPermissionTypes []string      `json:"SupportedPermissionTypes,omitempty"`
}

// getTemporaryGlueTableCredentialsOutput is the response body for
// GetTemporaryGlueTableCredentials. Real AWS returns these fields flat (no
// nested "Credentials" object) with Expiration as epoch seconds -- see
// GetTemporaryGlueTableCredentialsOutput in the aws-sdk-go-v2 model.
// VendedS3Path is a real response member that was entirely missing
// (deserializers.go's case list includes it alongside AccessKeyId/
// Expiration/SecretAccessKey/SessionToken).
type getTemporaryGlueTableCredentialsOutput struct {
	AccessKeyID     string   `json:"AccessKeyId,omitempty"`
	SecretAccessKey string   `json:"SecretAccessKey,omitempty"`
	SessionToken    string   `json:"SessionToken,omitempty"`
	VendedS3Path    []string `json:"VendedS3Path,omitempty"`
	Expiration      float64  `json:"Expiration,omitempty"`
}

type getWorkUnitResultsInput struct {
	QueryID       string `json:"QueryId"`
	WorkUnitToken string `json:"WorkUnitToken"`
	WorkUnitID    int64  `json:"WorkUnitId"`
}

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
	MaxResults *int    `json:"MaxResults,omitempty"`
	Expression []LFTag `json:"Expression"`
}
type searchDatabasesByLFTagsOutput struct {
	NextToken    string           `json:"NextToken,omitempty"`
	DatabaseList []TaggedDatabase `json:"DatabaseList"`
}

type searchTablesByLFTagsInput struct {
	CatalogID  string  `json:"CatalogId,omitempty"`
	NextToken  string  `json:"NextToken,omitempty"`
	MaxResults *int    `json:"MaxResults,omitempty"`
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
	CatalogID           string                          `json:"CatalogId,omitempty"`
	ExternalFiltering   *ExternalFilteringConfiguration `json:"ExternalFiltering,omitempty"`
	ApplicationStatus   string                          `json:"ApplicationStatus,omitempty"`
	ShareRecipients     []DataLakePrincipal             `json:"ShareRecipients,omitempty"`
	ServiceIntegrations []ServiceIntegration            `json:"ServiceIntegrations,omitempty"`
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
