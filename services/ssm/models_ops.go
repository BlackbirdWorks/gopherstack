package ssm

// models_ops.go contains model types for the 50 backend operations implemented
// in backend_ops.go.  Types that were already defined in backend_stubs.go are
// left there; only new or expanded types live here.

// ---------------------------------------------------------------------------
// Inventory
// ---------------------------------------------------------------------------

// InventoryItem is a single inventory data item for an instance.
// Fields are ordered for optimal struct alignment.
type InventoryItem struct {
	Context       map[string]string   `json:"Context,omitempty"`
	TypeName      string              `json:"TypeName"`
	SchemaVersion string              `json:"SchemaVersion,omitempty"`
	CaptureTime   string              `json:"CaptureTime,omitempty"`
	ContentHash   string              `json:"ContentHash,omitempty"`
	Content       []map[string]string `json:"Content,omitempty"`
}

// InventoryResultEntity represents inventory results for a single instance.
type InventoryResultEntity struct {
	Data map[string]InventoryTypeData `json:"Data,omitempty"`
	ID   string                       `json:"Id"`
}

// InventoryTypeData holds the data for a single inventory type.
type InventoryTypeData struct {
	TypeName      string              `json:"TypeName"`
	SchemaVersion string              `json:"SchemaVersion,omitempty"`
	CaptureTime   string              `json:"CaptureTime,omitempty"`
	ContentHash   string              `json:"ContentHash,omitempty"`
	Content       []map[string]string `json:"Content,omitempty"`
}

// ---------------------------------------------------------------------------
// Compliance
// ---------------------------------------------------------------------------

// ComplianceItem is a single compliance data item for a resource.
// Fields are ordered for optimal struct alignment.
type ComplianceItem struct {
	Details        map[string]string `json:"Details,omitempty"`
	ResourceID     string            `json:"ResourceId"`
	ResourceType   string            `json:"ResourceType"`
	ComplianceType string            `json:"ComplianceType,omitempty"`
	Status         string            `json:"Status,omitempty"`
	Severity       string            `json:"Severity,omitempty"`
	Title          string            `json:"Title,omitempty"`
}

// ---------------------------------------------------------------------------
// Inventory input/output types (replacing empty stubs)
// ---------------------------------------------------------------------------

// PutInventoryInput is the request payload for PutInventory.
type PutInventoryInput struct {
	InstanceID string          `json:"InstanceId"`
	Items      []InventoryItem `json:"Items"`
}

// GetInventoryInput is the request payload for GetInventory.
type GetInventoryInput struct {
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// GetInventoryOutput is the response payload for GetInventory.
type GetInventoryOutput struct {
	NextToken string                  `json:"NextToken,omitempty"`
	Entities  []InventoryResultEntity `json:"Entities"`
}

// GetInventorySchemaInput is the request payload for GetInventorySchema.
type GetInventorySchemaInput struct {
	TypeName  string `json:"TypeName,omitempty"`
	NextToken string `json:"NextToken,omitempty"`
}

// GetInventorySchemaOutput is the response payload for GetInventorySchema.
type GetInventorySchemaOutput struct {
	NextToken string `json:"NextToken,omitempty"`
	Schemas   []any  `json:"Schemas"`
}

// ListInventoryEntriesInput is the request payload for ListInventoryEntries.
type ListInventoryEntriesInput struct {
	MaxResults *int64 `json:"MaxResults,omitempty"`
	InstanceID string `json:"InstanceId"`
	TypeName   string `json:"TypeName"`
	NextToken  string `json:"NextToken,omitempty"`
}

// ListInventoryEntriesOutput is the response payload for ListInventoryEntries.
type ListInventoryEntriesOutput struct {
	InstanceID string              `json:"InstanceId,omitempty"`
	TypeName   string              `json:"TypeName,omitempty"`
	NextToken  string              `json:"NextToken,omitempty"`
	Entries    []map[string]string `json:"Entries"`
}

// DeleteInventoryInput is the request payload for DeleteInventory.
type DeleteInventoryInput struct {
	TypeName string `json:"TypeName"`
}

// ---------------------------------------------------------------------------
// Compliance input/output types (replacing empty stubs)
// ---------------------------------------------------------------------------

// PutComplianceItemsInput is the request payload for PutComplianceItems.
type PutComplianceItemsInput struct {
	ResourceID     string           `json:"ResourceId"`
	ResourceType   string           `json:"ResourceType"`
	ComplianceType string           `json:"ComplianceType,omitempty"`
	Items          []ComplianceItem `json:"Items"`
}

// ListComplianceItemsInput is the request payload for ListComplianceItems.
type ListComplianceItemsInput struct {
	MaxResults   *int64 `json:"MaxResults,omitempty"`
	ResourceID   string `json:"ResourceId,omitempty"`
	ResourceType string `json:"ResourceType,omitempty"`
	NextToken    string `json:"NextToken,omitempty"`
}

// ListComplianceItemsOutput is the response payload for ListComplianceItems.
type ListComplianceItemsOutput struct {
	NextToken       string           `json:"NextToken,omitempty"`
	ComplianceItems []ComplianceItem `json:"ComplianceItems"`
}

// ---------------------------------------------------------------------------
// Document metadata types (replacing empty stubs)
// ---------------------------------------------------------------------------

// UpdateDocumentDefaultVersionInput is the request payload for UpdateDocumentDefaultVersion.
type UpdateDocumentDefaultVersionInput struct {
	Name            string `json:"Name"`
	DocumentVersion string `json:"DocumentVersion"`
}

// UpdateDocumentDefaultVersionOutput is the response payload for UpdateDocumentDefaultVersion.
type UpdateDocumentDefaultVersionOutput struct {
	Description *DocumentDefaultVersionDescription `json:"Description,omitempty"`
}

// DocumentDefaultVersionDescription describes a document's default version.
type DocumentDefaultVersionDescription struct {
	Name               string `json:"Name"`
	DefaultVersion     string `json:"DefaultVersion"`
	DefaultVersionName string `json:"DefaultVersionName,omitempty"`
}

// UpdateDocumentMetadataInput is the request payload for UpdateDocumentMetadata.
// Fields ordered for alignment.
type UpdateDocumentMetadataInput struct {
	DocumentReviews *DocumentReviews `json:"DocumentReviews,omitempty"`
	Name            string           `json:"Name"`
	DocumentVersion string           `json:"DocumentVersion,omitempty"`
}

// DocumentReviews holds review metadata for a document version.
type DocumentReviews struct {
	Action  string                        `json:"Action"`
	Comment []DocumentReviewCommentSource `json:"Comment,omitempty"`
}

// DocumentReviewCommentSource is a single review comment.
type DocumentReviewCommentSource struct {
	Type    string `json:"Type,omitempty"`
	Content string `json:"Content,omitempty"`
}

// ListDocumentMetadataHistoryInput is the request payload.
// Fields ordered for alignment.
type ListDocumentMetadataHistoryInput struct {
	MaxResults      *int64 `json:"MaxResults,omitempty"`
	Name            string `json:"Name"`
	DocumentVersion string `json:"DocumentVersion,omitempty"`
	Metadata        string `json:"Metadata,omitempty"`
	NextToken       string `json:"NextToken,omitempty"`
}

// ListDocumentMetadataHistoryOutput is the response payload.
type ListDocumentMetadataHistoryOutput struct {
	Metadata        *DocumentMetadataResponseInfo `json:"Metadata,omitempty"`
	Name            string                        `json:"Name,omitempty"`
	DocumentVersion string                        `json:"DocumentVersion,omitempty"`
	Author          string                        `json:"Author,omitempty"`
	NextToken       string                        `json:"NextToken,omitempty"`
}

// DocumentMetadataResponseInfo holds review history.
type DocumentMetadataResponseInfo struct {
	ReviewerResponse []DocumentReviewerResponseSource `json:"ReviewerResponse,omitempty"`
}

// DocumentReviewerResponseSource is a single reviewer response.
// Fields ordered for alignment.
type DocumentReviewerResponseSource struct {
	ReviewStatus string                        `json:"ReviewStatus,omitempty"`
	Reviewer     string                        `json:"Reviewer,omitempty"`
	Comment      []DocumentReviewCommentSource `json:"Comment,omitempty"`
	CreatedTime  float64                       `json:"CreatedTime,omitempty"`
	UpdatedTime  float64                       `json:"UpdatedTime,omitempty"`
}

// ---------------------------------------------------------------------------
// Patch types (replacing empty stubs)
// ---------------------------------------------------------------------------

// GetDefaultPatchBaselineInput is the request payload for GetDefaultPatchBaseline.
type GetDefaultPatchBaselineInput struct {
	OperatingSystem string `json:"OperatingSystem,omitempty"`
}

// GetDefaultPatchBaselineOutput is the response payload for GetDefaultPatchBaseline.
type GetDefaultPatchBaselineOutput struct {
	BaselineID      string `json:"BaselineId"`
	OperatingSystem string `json:"OperatingSystem,omitempty"`
}

// GetPatchBaselineForPatchGroupInput is the request payload for GetPatchBaselineForPatchGroup.
type GetPatchBaselineForPatchGroupInput struct {
	PatchGroup      string `json:"PatchGroup"`
	OperatingSystem string `json:"OperatingSystem,omitempty"`
}

// GetPatchBaselineForPatchBaselineOutput is the response for GetPatchBaselineForPatchGroup.
type GetPatchBaselineForPatchBaselineOutput struct {
	BaselineID      string `json:"BaselineId"`
	PatchGroup      string `json:"PatchGroup,omitempty"`
	OperatingSystem string `json:"OperatingSystem,omitempty"`
}

// RegisterDefaultPatchBaselineInput is the request payload for RegisterDefaultPatchBaseline.
type RegisterDefaultPatchBaselineInput struct {
	BaselineID string `json:"BaselineId"`
}

// RegisterDefaultPatchBaselineOutput is the response payload for RegisterDefaultPatchBaseline.
type RegisterDefaultPatchBaselineOutput struct {
	BaselineID string `json:"BaselineId"`
}

// DeletePatchBaselineInput is the request payload for DeletePatchBaseline.
type DeletePatchBaselineInput struct {
	BaselineID string `json:"BaselineId"`
}

// DeletePatchBaselineOutput is the response payload for DeletePatchBaseline.
type DeletePatchBaselineOutput struct {
	BaselineID string `json:"BaselineId"`
}

// DescribePatchGroupStateInput is the request payload for DescribePatchGroupState.
type DescribePatchGroupStateInput struct {
	PatchGroup string `json:"PatchGroup"`
}

// DescribePatchGroupStateOutput is the response payload for DescribePatchGroupState.
type DescribePatchGroupStateOutput struct {
	Instances                                int32 `json:"Instances"`
	InstancesWithCriticalNonCompliantPatches int32 `json:"InstancesWithCriticalNonCompliantPatches"`
	InstancesWithFailedPatches               int32 `json:"InstancesWithFailedPatches"`
	InstancesWithInstalledPatches            int32 `json:"InstancesWithInstalledPatches"`
	InstancesWithInstalledOtherPatches       int32 `json:"InstancesWithInstalledOtherPatches"`
	InstancesWithMissingPatches              int32 `json:"InstancesWithMissingPatches"`
	InstancesWithNotApplicablePatches        int32 `json:"InstancesWithNotApplicablePatches"`
}

// DescribePatchGroupsInput is the request payload for DescribePatchGroups.
type DescribePatchGroupsInput struct {
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// PatchGroupPatchBaselineMapping maps a patch group to a baseline identity.
type PatchGroupPatchBaselineMapping struct {
	BaselineIdentity PatchBaselineIdentity `json:"BaselineIdentity"`
	PatchGroup       string                `json:"PatchGroup"`
}

// DescribePatchGroupsOutput is the response payload for DescribePatchGroups.
type DescribePatchGroupsOutput struct {
	NextToken string                           `json:"NextToken,omitempty"`
	Mappings  []PatchGroupPatchBaselineMapping `json:"Mappings"`
}

// DescribePatchPropertiesInput is the request payload for DescribePatchProperties.
type DescribePatchPropertiesInput struct {
	OperatingSystem string `json:"OperatingSystem,omitempty"`
	Property        string `json:"Property,omitempty"`
	PatchSet        string `json:"PatchSet,omitempty"`
}

// DescribePatchPropertiesOutput is the response payload for DescribePatchProperties.
type DescribePatchPropertiesOutput struct {
	NextToken  string              `json:"NextToken,omitempty"`
	Properties []map[string]string `json:"Properties"`
}

// DescribeEffectivePatchesForPatchBaselineInput is the request payload.
type DescribeEffectivePatchesForPatchBaselineInput struct {
	MaxResults *int64 `json:"MaxResults,omitempty"`
	BaselineID string `json:"BaselineId"`
	NextToken  string `json:"NextToken,omitempty"`
}

// EffectivePatch represents a patch matched by a baseline rule.
type EffectivePatch struct {
	PatchStatus *PatchStatus `json:"PatchStatus,omitempty"`
}

// PatchStatus holds the deployment/compliance status of a patch.
type PatchStatus struct {
	ApprovalDate     string `json:"ApprovalDate,omitempty"`
	ComplianceLevel  string `json:"ComplianceLevel,omitempty"`
	DeploymentStatus string `json:"DeploymentStatus,omitempty"`
}

// DescribeEffectivePatchesForPatchBaselineOutput is the response payload.
type DescribeEffectivePatchesForPatchBaselineOutput struct {
	NextToken        string           `json:"NextToken,omitempty"`
	EffectivePatches []EffectivePatch `json:"EffectivePatches"`
}

// GetDeployablePatchSnapshotForInstanceInput is the request payload.
type GetDeployablePatchSnapshotForInstanceInput struct {
	InstanceID string `json:"InstanceId"`
	SnapshotID string `json:"SnapshotId"`
}

// GetDeployablePatchSnapshotForInstanceOutput is the response payload.
type GetDeployablePatchSnapshotForInstanceOutput struct {
	InstanceID          string `json:"InstanceId,omitempty"`
	SnapshotID          string `json:"SnapshotId,omitempty"`
	SnapshotDownloadURL string `json:"SnapshotDownloadUrl,omitempty"`
	Product             string `json:"Product,omitempty"`
}

// ---------------------------------------------------------------------------
// Maintenance window types (replacing empty stubs)
// ---------------------------------------------------------------------------

// DeleteMaintenanceWindowOutput is the response payload for DeleteMaintenanceWindow.
type DeleteMaintenanceWindowOutput struct {
	WindowID string `json:"WindowId"`
}

// GetMaintenanceWindowTaskInput is the request payload for GetMaintenanceWindowTask.
type GetMaintenanceWindowTaskInput struct {
	WindowID     string `json:"WindowId"`
	WindowTaskID string `json:"WindowTaskId"`
}

// GetMaintenanceWindowTaskOutput is the response payload for GetMaintenanceWindowTask.
type GetMaintenanceWindowTaskOutput struct {
	MaintenanceWindowTask
}

// UpdateMaintenanceWindowTargetInput is the request payload for UpdateMaintenanceWindowTarget.
// Fields ordered for alignment.
type UpdateMaintenanceWindowTargetInput struct {
	WindowID       string         `json:"WindowId"`
	WindowTargetID string         `json:"WindowTargetId"`
	OwnerInfo      string         `json:"OwnerInfo,omitempty"`
	Name           string         `json:"Name,omitempty"`
	Description    string         `json:"Description,omitempty"`
	Targets        []WindowTarget `json:"Targets,omitempty"`
}

// UpdateMaintenanceWindowTargetOutput is the response payload for UpdateMaintenanceWindowTarget.
type UpdateMaintenanceWindowTargetOutput struct {
	WindowID       string         `json:"WindowId,omitempty"`
	WindowTargetID string         `json:"WindowTargetId,omitempty"`
	OwnerInfo      string         `json:"OwnerInfo,omitempty"`
	Name           string         `json:"Name,omitempty"`
	Description    string         `json:"Description,omitempty"`
	Targets        []WindowTarget `json:"Targets,omitempty"`
}

// UpdateMaintenanceWindowTaskInput is the request payload for UpdateMaintenanceWindowTask.
// Fields ordered for alignment.
type UpdateMaintenanceWindowTaskInput struct {
	Priority     *int32 `json:"Priority,omitempty"`
	WindowID     string `json:"WindowId"`
	WindowTaskID string `json:"WindowTaskId"`
	TaskArn      string `json:"TaskArn,omitempty"`
	Name         string `json:"Name,omitempty"`
	Description  string `json:"Description,omitempty"`
}

// UpdateMaintenanceWindowTaskOutput is the response payload for UpdateMaintenanceWindowTask.
type UpdateMaintenanceWindowTaskOutput struct {
	WindowID     string `json:"WindowId,omitempty"`
	WindowTaskID string `json:"WindowTaskId,omitempty"`
	TaskArn      string `json:"TaskArn,omitempty"`
	Name         string `json:"Name,omitempty"`
	Description  string `json:"Description,omitempty"`
	Priority     int32  `json:"Priority,omitempty"`
}

// DescribeMaintenanceWindowsForTargetInput is the request payload.
// Fields ordered for alignment.
type DescribeMaintenanceWindowsForTargetInput struct {
	MaxResults   *int64         `json:"MaxResults,omitempty"`
	ResourceType string         `json:"ResourceType,omitempty"`
	NextToken    string         `json:"NextToken,omitempty"`
	Targets      []WindowTarget `json:"Targets,omitempty"`
}

// DescribeMaintenanceWindowsForTargetOutput is the response payload.
type DescribeMaintenanceWindowsForTargetOutput struct {
	NextToken        string                      `json:"NextToken,omitempty"`
	WindowIdentities []MaintenanceWindowIdentity `json:"WindowIdentities"`
}

// ---------------------------------------------------------------------------
// OpsItem types (replacing empty stubs)
// ---------------------------------------------------------------------------

// DisassociateOpsItemRelatedItemInput is the request payload.
type DisassociateOpsItemRelatedItemInput struct {
	OpsItemID     string `json:"OpsItemId"`
	AssociationID string `json:"AssociationId"`
}

// ListOpsItemRelatedItemsInput is the request payload.
type ListOpsItemRelatedItemsInput struct {
	MaxResults *int64 `json:"MaxResults,omitempty"`
	OpsItemID  string `json:"OpsItemId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// ListOpsItemRelatedItemsOutput is the response payload.
type ListOpsItemRelatedItemsOutput struct {
	NextToken string               `json:"NextToken,omitempty"`
	Summaries []OpsItemRelatedItem `json:"Summaries"`
}

// ListOpsItemEventsInput is the request payload.
type ListOpsItemEventsInput struct {
	MaxResults *int64 `json:"MaxResults,omitempty"`
	OpsItemID  string `json:"OpsItemId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// OpsItemEventSummary is a summary of an OpsItem event.
type OpsItemEventSummary struct {
	OpsItemID   string  `json:"OpsItemId,omitempty"`
	EventID     string  `json:"EventId,omitempty"`
	Source      string  `json:"Source,omitempty"`
	Detail      string  `json:"Detail,omitempty"`
	CreatedTime float64 `json:"CreatedTime,omitempty"`
}

// ListOpsItemEventsOutput is the response payload.
type ListOpsItemEventsOutput struct {
	NextToken string                `json:"NextToken,omitempty"`
	Summaries []OpsItemEventSummary `json:"Summaries"`
}

// ---------------------------------------------------------------------------
// Compliance summary types
// ---------------------------------------------------------------------------

// ListComplianceSummariesInput is the request payload.
type ListComplianceSummariesInput struct {
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// ListComplianceSummariesOutput is the response payload.
type ListComplianceSummariesOutput struct {
	NextToken              string `json:"NextToken,omitempty"`
	ComplianceSummaryItems []any  `json:"ComplianceSummaryItems"`
}

// ListResourceComplianceSummariesInput is the request payload.
type ListResourceComplianceSummariesInput struct {
	MaxResults *int64 `json:"MaxResults,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// ListResourceComplianceSummariesOutput is the response payload.
type ListResourceComplianceSummariesOutput struct {
	NextToken                      string `json:"NextToken,omitempty"`
	ResourceComplianceSummaryItems []any  `json:"ResourceComplianceSummaryItems"`
}

// DescribeInventoryDeletionsInput is the request payload for DescribeInventoryDeletions.
type DescribeInventoryDeletionsInput struct {
	MaxResults *int64 `json:"MaxResults,omitempty"`
	DeletionID string `json:"DeletionId,omitempty"`
	NextToken  string `json:"NextToken,omitempty"`
}

// DescribeInventoryDeletionsOutput is the response payload.
type DescribeInventoryDeletionsOutput struct {
	NextToken          string `json:"NextToken,omitempty"`
	InventoryDeletions []any  `json:"InventoryDeletions"`
}
