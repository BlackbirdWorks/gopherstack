package ssm

// DeleteInventoryOutput is the response for DeleteInventory.
type DeleteInventoryOutput struct {
	DeletionSummary *InventoryDeletionSummary `json:"DeletionSummary,omitempty"`
	DeletionID      string                    `json:"DeletionId,omitempty"`
	TypeName        string                    `json:"TypeName,omitempty"`
}

// PutComplianceItemsOutput is the response for PutComplianceItems.
type PutComplianceItemsOutput struct{}

// PutInventoryOutput is the response for PutInventory.
type PutInventoryOutput struct{}
type complianceTally struct {
	compliantCount    int
	nonCompliantCount int
}
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

// InventorySchemaItem represents a single inventory schema type entry.
type InventorySchemaItem struct {
	TypeName string `json:"TypeName"`
	Version  string `json:"Version"`
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

// ComplianceCountSummary holds compliant or non-compliant item counts.
type ComplianceCountSummary struct {
	CompliantCount    int `json:"CompliantCount,omitempty"`
	NonCompliantCount int `json:"NonCompliantCount,omitempty"`
}

// ComplianceSummaryItem represents a rolled-up compliance summary by type.
type ComplianceSummaryItem struct {
	ComplianceType      string                 `json:"ComplianceType"`
	NonCompliantSummary ComplianceCountSummary `json:"NonCompliantSummary"`
	CompliantSummary    ComplianceCountSummary `json:"CompliantSummary"`
}

// ResourceComplianceSummaryItem represents per-resource compliance status.
type ResourceComplianceSummaryItem struct {
	ResourceID          string                 `json:"ResourceId"`
	ResourceType        string                 `json:"ResourceType"`
	ComplianceType      string                 `json:"ComplianceType"`
	OverallSeverity     string                 `json:"OverallSeverity"`
	Status              string                 `json:"Status"`
	NonCompliantSummary ComplianceCountSummary `json:"NonCompliantSummary"`
	CompliantSummary    ComplianceCountSummary `json:"CompliantSummary"`
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
