package resourcegroups

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ResourceQuery represents a tag-based resource query for a group.
type ResourceQuery struct {
	Type  string `json:"Type"`
	Query string `json:"Query"`
}

// Group represents a Resource Group.
//
// This is the internal backend representation, not an AWS wire shape: the
// real types.Group has no Tags or ResourceQuery members (those travel as
// separate top-level response fields -- see createGroupOutput/getGroupBody in
// handler.go), so handlers must build a dedicated wire struct rather than
// marshaling *Group directly.
type Group struct {
	Tags           *tags.Tags        `json:"-"`
	ResourceQuery  *ResourceQuery    `json:"-"`
	ApplicationTag map[string]string `json:"ApplicationTag,omitempty"`
	Name           string            `json:"Name"`
	ARN            string            `json:"GroupArn"`
	Description    string            `json:"Description,omitempty"`
	// Owner is a free-form name/email/identifier for the person or team that
	// owns the group. The real AWS API field is called "Owner" on the wire
	// (not "OwnerId"); it is optional and, unlike an AWS account ID, is never
	// auto-populated by the service.
	Owner       string `json:"Owner,omitempty"`
	DisplayName string `json:"DisplayName,omitempty"`
	Criticality int    `json:"Criticality,omitempty"`
}

// ListGroupsFilter holds a single filter for the ListGroups operation.
type ListGroupsFilter struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// GroupConfigurationParameter is a key-value parameter for a group configuration item.
type GroupConfigurationParameter struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// GroupConfigurationItem is a single configuration item for a group.
type GroupConfigurationItem struct {
	Type       string                        `json:"Type"`
	Parameters []GroupConfigurationParameter `json:"Parameters,omitempty"`
}

// AccountSettings holds account-level settings for Resource Groups.
type AccountSettings struct {
	GroupLifecycleEventsDesiredStatus string `json:"GroupLifecycleEventsDesiredStatus,omitempty"`
	GroupLifecycleEventsStatus        string `json:"GroupLifecycleEventsStatus,omitempty"`
	GroupLifecycleEventsStatusMessage string `json:"GroupLifecycleEventsStatusMessage,omitempty"`
}

// TagSyncTask represents a tag-sync task for an application group.
type TagSyncTask struct {
	CreatedAt     time.Time      `json:"CreatedAt"`
	ResourceQuery *ResourceQuery `json:"ResourceQuery,omitempty"`
	GroupArn      string         `json:"GroupArn"`
	GroupName     string         `json:"GroupName"`
	RoleArn       string         `json:"RoleArn"`
	TagKey        string         `json:"TagKey,omitempty"`
	TagValue      string         `json:"TagValue,omitempty"`
	TaskArn       string         `json:"TaskArn"`
	Status        string         `json:"Status"`
	ErrorMessage  string         `json:"ErrorMessage,omitempty"`
}

// ResourceIdentifier holds an ARN and resource type.
type ResourceIdentifier struct {
	ResourceArn  string `json:"ResourceArn,omitempty"`
	ResourceType string `json:"ResourceType,omitempty"`
}

// GroupingStatusItem holds the grouping/ungrouping status for a resource.
type GroupingStatusItem struct {
	UpdatedAt    time.Time `json:"UpdatedAt"`
	ResourceArn  string    `json:"ResourceArn,omitempty"`
	Action       string    `json:"Action,omitempty"`
	Status       string    `json:"Status,omitempty"`
	ErrorCode    string    `json:"ErrorCode,omitempty"`
	ErrorMessage string    `json:"ErrorMessage,omitempty"`
}

// ListTagSyncTasksFilter holds filter criteria for listing tag-sync tasks.
type ListTagSyncTasksFilter struct {
	GroupArn  string `json:"GroupArn,omitempty"`
	GroupName string `json:"GroupName,omitempty"`
}

// ListGroupResourcesFilter holds a single filter criterion for ListGroupResources.
// Supported Name: "resource-type" (filter by AWS CloudFormation resource type string).
type ListGroupResourcesFilter struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

// tagFilterQuery is the parsed form of a TAG_FILTERS_1_0 ResourceQuery string.
type tagFilterQuery struct {
	ResourceTypeFilters []string    `json:"ResourceTypeFilters"`
	TagFilters          []tagFilter `json:"TagFilters"`
}

// tagFilter holds a tag key and allowed values for SearchResources filtering.
type tagFilter struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// UngroupResourcesResult holds the result of an UngroupResources call.
type UngroupResourcesResult struct {
	Succeeded []string
	Failed    []GroupingFailedItem
}

// GroupingFailedItem describes a resource that could not be grouped or ungrouped.
type GroupingFailedItem struct {
	ResourceArn  string `json:"ResourceArn"`
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}

// queryErrorWire mirrors the real types.QueryError shape (ErrorCode, Message)
// returned by SearchResources/ListGroupResources. Its documented ErrorCode
// values (CLOUDFORMATION_STACK_INACTIVE, CLOUDFORMATION_STACK_NOT_EXISTING,
// CLOUDFORMATION_STACK_UNASSUMABLE_ROLE, RESOURCE_TYPE_NOT_SUPPORTED) only
// ever arise for CLOUDFORMATION_STACK_1_0-based groups, which this emulator
// does not model -- so this always serializes as an empty/omitted list here.
// See PARITY.md gaps.
type queryErrorWire struct {
	ErrorCode string `json:"ErrorCode,omitempty"`
	Message   string `json:"Message,omitempty"`
}
