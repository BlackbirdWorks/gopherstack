package ssm

// DeleteOpsItemOutput is the response for DeleteOpsItem.
type DeleteOpsItemOutput struct{}

// DeleteOpsMetadataOutput is the response for DeleteOpsMetadata.
type DeleteOpsMetadataOutput struct{}

// DisassociateOpsItemRelatedItemOutput is the response for DisassociateOpsItemRelatedItem.
type DisassociateOpsItemRelatedItemOutput struct{}

// UpdateOpsItemOutput is the response for UpdateOpsItem.
type UpdateOpsItemOutput struct{}

// DeleteOpsItemInput is the request for DeleteOpsItem.
type DeleteOpsItemInput struct {
	OpsItemID string `json:"OpsItemId"`
}

// DeleteOpsMetadataInput is the request for DeleteOpsMetadata.
type DeleteOpsMetadataInput struct {
	OpsMetadataArn string `json:"OpsMetadataArn"`
}

// OpsItemFilter is a filter for DescribeOpsItems.
type OpsItemFilter struct {
	Key      string   `json:"Key"`
	Operator string   `json:"Operator,omitempty"`
	Values   []string `json:"Values"`
}

// DescribeOpsItemsInput is the request payload for DescribeOpsItems.
type DescribeOpsItemsInput struct {
	MaxResults     *int64          `json:"MaxResults,omitempty"`
	NextToken      string          `json:"NextToken,omitempty"`
	OpsItemFilters []OpsItemFilter `json:"OpsItemFilters,omitempty"`
}

// DescribeOpsItemsOutput is the response payload for DescribeOpsItems.
type DescribeOpsItemsOutput struct {
	NextToken        string           `json:"NextToken,omitempty"`
	OpsItemSummaries []OpsItemSummary `json:"OpsItemSummaries"`
}

// OpsItemSummary is a lightweight OpsItem listing entry.
type OpsItemSummary struct {
	OpsItemID   string  `json:"OpsItemId"`
	Title       string  `json:"Title"`
	Status      string  `json:"Status"`
	Source      string  `json:"Source"`
	CreatedTime float64 `json:"CreatedTime"`
}

// GetOpsItemInput is the request payload for GetOpsItem.
type GetOpsItemInput struct {
	OpsItemID string `json:"OpsItemId"`
}

// GetOpsItemOutput is the response payload for GetOpsItem.
type GetOpsItemOutput struct {
	OpsItem OpsItem `json:"OpsItem"`
}

// GetOpsMetadataInput is the request payload for GetOpsMetadata.
type GetOpsMetadataInput struct {
	OpsMetadataArn string `json:"OpsMetadataArn"`
}

// GetOpsMetadataOutput is the response payload for GetOpsMetadata.
type GetOpsMetadataOutput struct {
	OpsMetadata
}

// GetOpsSummaryInput is the request payload.
type GetOpsSummaryInput struct{}

// GetOpsSummaryOutput is the response payload.
type GetOpsSummaryOutput struct{}

// ListOpsMetadataInput is the request payload.
type ListOpsMetadataInput struct{}

// ListOpsMetadataOutput is the response payload.
type ListOpsMetadataOutput struct{}

// UpdateOpsItemInput is the request payload for UpdateOpsItem.
type UpdateOpsItemInput struct {
	OperationalData map[string]OpsItemDataValue `json:"OperationalData,omitempty"`
	OpsItemID       string                      `json:"OpsItemId"`
	Title           string                      `json:"Title,omitempty"`
	Description     string                      `json:"Description,omitempty"`
	Status          string                      `json:"Status,omitempty"`
	Severity        string                      `json:"Severity,omitempty"`
	Category        string                      `json:"Category,omitempty"`
}

// UpdateOpsMetadataInput is the request payload for UpdateOpsMetadata.
type UpdateOpsMetadataInput struct {
	Metadata       map[string]MetadataValue `json:"Metadata,omitempty"`
	OpsMetadataArn string                   `json:"OpsMetadataArn"`
}

// UpdateOpsMetadataOutput is the response payload for UpdateOpsMetadata.
type UpdateOpsMetadataOutput struct {
	OpsMetadataArn string `json:"OpsMetadataArn"`
}

// OpsItemDataValue represents a value in OpsItem OperationalData.
type OpsItemDataValue struct {
	Type  string `json:"Type,omitempty"`
	Value string `json:"Value,omitempty"`
}

// OpsItem represents an SSM OpsItem.
type OpsItem struct {
	OperationalData  map[string]OpsItemDataValue `json:"OperationalData,omitempty"`
	OpsItemID        string                      `json:"OpsItemId"`
	OpsItemArn       string                      `json:"OpsItemArn,omitempty"`
	OpsItemType      string                      `json:"OpsItemType,omitempty"`
	Title            string                      `json:"Title"`
	Source           string                      `json:"Source"`
	Description      string                      `json:"Description,omitempty"`
	Status           string                      `json:"Status"`
	Severity         string                      `json:"Severity,omitempty"`
	Category         string                      `json:"Category,omitempty"`
	CreatedTime      float64                     `json:"CreatedTime"`
	LastModifiedTime float64                     `json:"LastModifiedTime"`
}

// OpsItemRelatedItem represents an item related to an OpsItem.
type OpsItemRelatedItem struct {
	AssociationID   string `json:"AssociationId"`
	AssociationType string `json:"AssociationType"`
	ResourceType    string `json:"ResourceType"`
	ResourceURI     string `json:"ResourceUri"`
}

// CreateOpsItemInput is the request payload for CreateOpsItem.
type CreateOpsItemInput struct {
	Title           string                      `json:"Title"`
	Source          string                      `json:"Source"`
	Description     string                      `json:"Description,omitempty"`
	OpsItemType     string                      `json:"OpsItemType,omitempty"`
	Severity        string                      `json:"Severity,omitempty"`
	Category        string                      `json:"Category,omitempty"`
	OperationalData map[string]OpsItemDataValue `json:"OperationalData,omitempty"`
	Tags            []Tag                       `json:"Tags,omitempty"`
}

// CreateOpsItemOutput is the response payload for CreateOpsItem.
type CreateOpsItemOutput struct {
	OpsItemArn string `json:"OpsItemArn,omitempty"`
	OpsItemID  string `json:"OpsItemId"`
}

// AssociateOpsItemRelatedItemInput is the request payload for AssociateOpsItemRelatedItem.
type AssociateOpsItemRelatedItemInput struct {
	OpsItemID       string `json:"OpsItemId"`
	AssociationType string `json:"AssociationType"`
	ResourceType    string `json:"ResourceType"`
	ResourceURI     string `json:"ResourceUri"`
}

// AssociateOpsItemRelatedItemOutput is the response payload for AssociateOpsItemRelatedItem.
type AssociateOpsItemRelatedItemOutput struct {
	AssociationID string `json:"AssociationId"`
}

// MetadataValue represents a single metadata entry value.
type MetadataValue struct {
	Value string `json:"Value,omitempty"`
}

// OpsMetadata represents SSM OpsMetadata for a resource.
type OpsMetadata struct {
	Metadata         map[string]MetadataValue `json:"Metadata,omitempty"`
	OpsMetadataArn   string                   `json:"OpsMetadataArn"`
	ResourceID       string                   `json:"ResourceId"`
	CreationDate     float64                  `json:"CreationDate"`
	LastModifiedDate float64                  `json:"LastModifiedDate"`
}

// CreateOpsMetadataInput is the request payload for CreateOpsMetadata.
type CreateOpsMetadataInput struct {
	ResourceID string                   `json:"ResourceId"`
	Metadata   map[string]MetadataValue `json:"Metadata,omitempty"`
	Tags       []Tag                    `json:"Tags,omitempty"`
}

// CreateOpsMetadataOutput is the response payload for CreateOpsMetadata.
type CreateOpsMetadataOutput struct {
	OpsMetadataArn string `json:"OpsMetadataArn"`
}

// GetOpsSummaryOutputFull has summary counts.
type GetOpsSummaryOutputFull struct {
	Entities []OpsSummaryEntity `json:"Entities,omitempty"`
}

// OpsSummaryEntity represents a summary entry for ops items.
type OpsSummaryEntity struct {
	Data map[string]OpsSummaryValue `json:"Data,omitempty"`
	ID   string                     `json:"Id"`
}

// OpsSummaryValue holds aggregated value for an ops summary.
type OpsSummaryValue struct {
	Unit  string `json:"Unit"`
	Count int    `json:"Count"`
}

// ListOpsMetadataOutputFull extends the empty output.
type ListOpsMetadataOutputFull struct {
	NextToken       string        `json:"NextToken,omitempty"`
	OpsMetadataList []OpsMetadata `json:"OpsMetadataList"`
}

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
