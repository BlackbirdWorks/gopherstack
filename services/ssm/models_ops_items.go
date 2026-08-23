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

// OpsItemSummary is a lightweight OpsItem listing entry (real
// types.OpsItemSummary, types.go:4600). CreatedBy/LastModifiedBy are real
// members not modeled -- no caller-identity/SigV4-principal infra to derive
// a real IAM ARN from, same disclosed-gap class as ServiceSetting.LastModifiedUser.
// There is no Version or OpsItemArn member on the real type at all, unlike
// the full OpsItem (OpsItemOutput.Version/.OpsItemArn).
type OpsItemSummary struct {
	OperationalData  map[string]OpsItemDataValue `json:"OperationalData,omitempty"`
	PlannedEndTime   *float64                    `json:"PlannedEndTime,omitempty"`
	PlannedStartTime *float64                    `json:"PlannedStartTime,omitempty"`
	ActualEndTime    *float64                    `json:"ActualEndTime,omitempty"`
	ActualStartTime  *float64                    `json:"ActualStartTime,omitempty"`
	OpsItemID        string                      `json:"OpsItemId"`
	Title            string                      `json:"Title"`
	Status           string                      `json:"Status"`
	Source           string                      `json:"Source"`
	OpsItemType      string                      `json:"OpsItemType,omitempty"`
	Category         string                      `json:"Category,omitempty"`
	Severity         string                      `json:"Severity,omitempty"`
	CreatedTime      float64                     `json:"CreatedTime"`
	LastModifiedTime float64                     `json:"LastModifiedTime,omitempty"`
	Priority         int32                       `json:"Priority,omitempty"`
}

// GetOpsItemInput is the request payload for GetOpsItem.
type GetOpsItemInput struct {
	OpsItemID string `json:"OpsItemId"`
}

// GetOpsItemOutput is the response payload for GetOpsItem.
type GetOpsItemOutput struct {
	OpsItem OpsItemOutput `json:"OpsItem"`
}

// OpsItemOutput projects OpsItem onto the wire shape the real
// GetOpsItemOutput/DescribeOpsItems.OpsItemSummary declare (types.OpsItem,
// types.OpsItemSummary) -- neither has an AccountId member at all (it exists
// only on CreateOpsItemInput, a create-time-only attribution field this
// backend tracks internally for DescribeOpsItems' AccountId filter key but
// never returns on the wire). CreatedBy/LastModifiedBy are real members not
// modeled -- no caller-identity/SigV4-principal infra to derive a real IAM
// ARN from, same disclosed-gap class as ServiceSetting.LastModifiedUser.
func opsItemToOutput(item OpsItem) OpsItemOutput {
	return OpsItemOutput{
		OperationalData:  item.OperationalData,
		PlannedEndTime:   item.PlannedEndTime,
		PlannedStartTime: item.PlannedStartTime,
		ActualEndTime:    item.ActualEndTime,
		ActualStartTime:  item.ActualStartTime,
		Source:           item.Source,
		OpsItemType:      item.OpsItemType,
		Status:           item.Status,
		Severity:         item.Severity,
		Category:         item.Category,
		OpsItemID:        item.OpsItemID,
		OpsItemArn:       item.OpsItemArn,
		Description:      item.Description,
		Title:            item.Title,
		Notifications:    item.Notifications,
		RelatedOpsItems:  item.RelatedOpsItems,
		LastModifiedTime: item.LastModifiedTime,
		CreatedTime:      item.CreatedTime,
		Priority:         item.Priority,
		Version:          item.Version,
	}
}

// OpsItemOutput is the wire-safe projection of OpsItem -- see opsItemToOutput.
type OpsItemOutput struct {
	OperationalData  map[string]OpsItemDataValue `json:"OperationalData,omitempty"`
	PlannedEndTime   *float64                    `json:"PlannedEndTime,omitempty"`
	PlannedStartTime *float64                    `json:"PlannedStartTime,omitempty"`
	ActualEndTime    *float64                    `json:"ActualEndTime,omitempty"`
	ActualStartTime  *float64                    `json:"ActualStartTime,omitempty"`
	OpsItemID        string                      `json:"OpsItemId"`
	Description      string                      `json:"Description,omitempty"`
	Status           string                      `json:"Status"`
	Severity         string                      `json:"Severity,omitempty"`
	Category         string                      `json:"Category,omitempty"`
	Source           string                      `json:"Source"`
	OpsItemArn       string                      `json:"OpsItemArn,omitempty"`
	OpsItemType      string                      `json:"OpsItemType,omitempty"`
	Title            string                      `json:"Title"`
	Version          string                      `json:"Version,omitempty"`
	RelatedOpsItems  []RelatedOpsItemRef         `json:"RelatedOpsItems,omitempty"`
	Notifications    []OpsItemNotification       `json:"Notifications,omitempty"`
	LastModifiedTime float64                     `json:"LastModifiedTime"`
	CreatedTime      float64                     `json:"CreatedTime"`
	Priority         int32                       `json:"Priority,omitempty"`
}

// GetOpsMetadataInput is the request payload for GetOpsMetadata.
type GetOpsMetadataInput struct {
	OpsMetadataArn string `json:"OpsMetadataArn"`
}

// GetOpsMetadataOutput is the response payload for GetOpsMetadata (real
// api_op_GetOpsMetadata.go). It is deliberately NOT the full OpsMetadata
// shape -- the real op has no OpsMetadataArn/CreationDate/LastModifiedDate
// members at all, only Metadata/NextToken/ResourceId; those other three are
// members of the OpsMetadata type itself (used by ListOpsMetadata), a
// different real response shape. NextToken/MaxResults pagination over
// Metadata's keys is accepted-and-ignored -- disclosed, not implemented.
type GetOpsMetadataOutput struct {
	Metadata   map[string]MetadataValue `json:"Metadata,omitempty"`
	NextToken  string                   `json:"NextToken,omitempty"`
	ResourceID string                   `json:"ResourceId,omitempty"`
}

// GetOpsSummaryInput is the request payload. The real GetOpsSummaryInput
// (api_op_GetOpsSummary.go) also declares Aggregators, Filters,
// MaxResults, NextToken, ResultAttributes and SyncName, but this backend's
// GetOpsSummary (ops_items.go) always returns a single fixed
// "AWS:OpsItem"/Count entity -- it has no queryable multi-type OpsData
// dataset for Aggregators/ResultAttributes to project across or for Filters
// to subset, unlike DescribeActivations/ListAssociations/etc, which filter
// a real typed collection this backend already tracks. Left struct{}
// deliberately (gopherstack-a250 triage): wiring these members would mean
// inventing query semantics this backend has no honest state to back.
type GetOpsSummaryInput struct{}

// GetOpsSummaryOutput is the response payload.
type GetOpsSummaryOutput struct{}

// OpsMetadataFilterEntry filters ListOpsMetadata results by ResourceId
// (api_op_ListOpsMetadata.go Filters member, types.OpsMetadataFilter).
type OpsMetadataFilterEntry struct {
	Key    string   `json:"Key,omitempty"`
	Values []string `json:"Values,omitempty"`
}

// ListOpsMetadataInput is the request payload.
type ListOpsMetadataInput struct {
	MaxResults *int32                   `json:"MaxResults,omitempty"`
	NextToken  string                   `json:"NextToken,omitempty"`
	Filters    []OpsMetadataFilterEntry `json:"Filters,omitempty"`
}

// ListOpsMetadataOutput is the response payload.
type ListOpsMetadataOutput struct{}

// UpdateOpsItemInput is the request payload for UpdateOpsItem. There is no
// AccountId member on the real op at all (api_op_UpdateOpsItem.go) -- that
// member exists only on CreateOpsItemInput; a fabricated AccountId here
// previously let a caller silently rewrite an OpsItem's AccountId through
// UpdateOpsItem, an operation the real SDK cannot even express.
type UpdateOpsItemInput struct {
	OperationalData  map[string]OpsItemDataValue `json:"OperationalData,omitempty"`
	Priority         *int32                      `json:"Priority,omitempty"`
	OpsItemID        string                      `json:"OpsItemId"`
	OpsItemArn       string                      `json:"OpsItemArn,omitempty"`
	Title            string                      `json:"Title,omitempty"`
	Description      string                      `json:"Description,omitempty"`
	Status           string                      `json:"Status,omitempty"`
	Severity         string                      `json:"Severity,omitempty"`
	Category         string                      `json:"Category,omitempty"`
	ActualStartTime  *float64                    `json:"ActualStartTime,omitempty"`
	ActualEndTime    *float64                    `json:"ActualEndTime,omitempty"`
	Notifications    []OpsItemNotification       `json:"Notifications,omitempty"`
	PlannedStartTime *float64                    `json:"PlannedStartTime,omitempty"`
	PlannedEndTime   *float64                    `json:"PlannedEndTime,omitempty"`
	RelatedOpsItems  []RelatedOpsItemRef         `json:"RelatedOpsItems,omitempty"`
	// OperationalDataToDelete removes keys from OperationalData. Confirmed
	// present in aws-sdk-go-v2 v1.73.4's api_op_UpdateOpsItem.go but out of
	// scope for this pass (tracked separately, not part of bd gopherstack-iq4m's
	// field list).
}

// UpdateOpsMetadataInput is the request payload for UpdateOpsMetadata.
type UpdateOpsMetadataInput struct {
	Metadata       map[string]MetadataValue `json:"MetadataToUpdate,omitempty"`
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

// OpsItemNotification is an SNS topic ARN notified when an OpsItem is edited
// or changed.
type OpsItemNotification struct {
	Arn string `json:"Arn,omitempty"`
}

// RelatedOpsItemRef references another OpsItem that shares something in
// common with the current one (e.g. similar error messages, impacted
// resources, or statuses).
type RelatedOpsItemRef struct {
	OpsItemID string `json:"OpsItemId"`
}

// OpsItem represents an SSM OpsItem.
type OpsItem struct {
	OperationalData  map[string]OpsItemDataValue `json:"OperationalData,omitempty"`
	PlannedEndTime   *float64                    `json:"PlannedEndTime,omitempty"`
	PlannedStartTime *float64                    `json:"PlannedStartTime,omitempty"`
	ActualEndTime    *float64                    `json:"ActualEndTime,omitempty"`
	ActualStartTime  *float64                    `json:"ActualStartTime,omitempty"`
	OpsItemID        string                      `json:"OpsItemId"`
	Description      string                      `json:"Description,omitempty"`
	Status           string                      `json:"Status"`
	Severity         string                      `json:"Severity,omitempty"`
	Category         string                      `json:"Category,omitempty"`
	Source           string                      `json:"Source"`
	OpsItemArn       string                      `json:"OpsItemArn,omitempty"`
	OpsItemType      string                      `json:"OpsItemType,omitempty"`
	AccountID        string                      `json:"AccountId,omitempty"`
	Title            string                      `json:"Title"`
	Version          string                      `json:"Version,omitempty"`
	RelatedOpsItems  []RelatedOpsItemRef         `json:"RelatedOpsItems,omitempty"`
	Notifications    []OpsItemNotification       `json:"Notifications,omitempty"`
	LastModifiedTime float64                     `json:"LastModifiedTime"`
	CreatedTime      float64                     `json:"CreatedTime"`
	Priority         int32                       `json:"Priority,omitempty"`
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
	OperationalData  map[string]OpsItemDataValue `json:"OperationalData,omitempty"`
	PlannedEndTime   *float64                    `json:"PlannedEndTime,omitempty"`
	PlannedStartTime *float64                    `json:"PlannedStartTime,omitempty"`
	ActualStartTime  *float64                    `json:"ActualStartTime,omitempty"`
	ActualEndTime    *float64                    `json:"ActualEndTime,omitempty"`
	Title            string                      `json:"Title"`
	Source           string                      `json:"Source"`
	Description      string                      `json:"Description,omitempty"`
	OpsItemType      string                      `json:"OpsItemType,omitempty"`
	Severity         string                      `json:"Severity,omitempty"`
	Category         string                      `json:"Category,omitempty"`
	AccountID        string                      `json:"AccountId,omitempty"`
	Notifications    []OpsItemNotification       `json:"Notifications,omitempty"`
	Tags             []Tag                       `json:"Tags,omitempty"`
	RelatedOpsItems  []RelatedOpsItemRef         `json:"RelatedOpsItems,omitempty"`
	Priority         int32                       `json:"Priority,omitempty"`
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
