package resourcegroupstaggingapi

import "context"

// defaultComplianceSummaryMaxResults is the default MaxResults for GetComplianceSummary.
const defaultComplianceSummaryMaxResults = 50

// maxComplianceSummaryMaxResults is the maximum MaxResults for GetComplianceSummary.
const maxComplianceSummaryMaxResults = 1000

// isValidGroupByValue returns true when v is a recognised GetComplianceSummary GroupBy value.
func isValidGroupByValue(v string) bool {
	switch v {
	case "TARGET_ID", "REGION", "RESOURCE_TYPE":
		return true
	default:
		return false
	}
}

// GetComplianceSummaryInput is the request payload for GetComplianceSummary.
type GetComplianceSummaryInput struct {
	// GroupBy specifies attributes to group noncompliant resource counts by.
	GroupBy []string `json:"GroupBy,omitempty"`
	// MaxResults is the maximum number of results per page.
	MaxResults *int32 `json:"MaxResults,omitempty"`
	// PaginationToken is the cursor from a previous call.
	PaginationToken *string `json:"PaginationToken,omitempty"`
	// RegionFilters restricts output to specified regions.
	RegionFilters []string `json:"RegionFilters,omitempty"`
	// ResourceTypeFilters restricts output to specified resource types.
	ResourceTypeFilters []string `json:"ResourceTypeFilters,omitempty"`
	// TagKeyFilters restricts output to resources with specified tag keys.
	TagKeyFilters []string `json:"TagKeyFilters,omitempty"`
	// TargetIDFilters restricts output to specified target IDs.
	TargetIDFilters []string `json:"TargetIdFilters,omitempty"`
}

// ComplianceSummary is a count of noncompliant resources.
type ComplianceSummary struct {
	LastUpdated           *string `json:"LastUpdated,omitempty"`
	Region                *string `json:"Region,omitempty"`
	ResourceType          *string `json:"ResourceType,omitempty"`
	TargetID              *string `json:"TargetId,omitempty"`
	TargetIDType          *string `json:"TargetIdType,omitempty"`
	NonCompliantResources int64   `json:"NonCompliantResources"`
}

// GetComplianceSummaryOutput is the response payload for GetComplianceSummary.
type GetComplianceSummaryOutput struct {
	// PaginationToken is the cursor for the next page.
	PaginationToken *string `json:"PaginationToken,omitempty"`
	// SummaryList contains the noncompliant resource counts.
	SummaryList []ComplianceSummary `json:"SummaryList"`
}

// GetComplianceSummary returns compliance summary data filtered by the supplied parameters.
// The in-memory backend has no tag policy, so all resources are always compliant and
// NonCompliantResources is always 0.  Filters and pagination are honoured so callers
// get accurate (empty) results rather than a stub.
func (b *InMemoryBackend) GetComplianceSummary(
	ctx context.Context,
	input *GetComplianceSummaryInput,
) *GetComplianceSummaryOutput {
	b.mu.Lock("GetComplianceSummary")
	defer b.mu.Unlock()

	// GroupBy validation is handled by the HTTP handler before reaching the backend.
	// The handler enforces valid values (REGION, RESOURCE_TYPE, TARGET_ID).

	// Resolve MaxResults.
	maxResults := int32(defaultComplianceSummaryMaxResults)
	if input.MaxResults != nil {
		mr := *input.MaxResults
		if mr >= 1 && mr <= int32(maxComplianceSummaryMaxResults) {
			maxResults = mr
		}
	}

	all := b.getResources(ctx, nil, nil)

	// Apply filters.
	if len(input.ResourceTypeFilters) > 0 {
		all = applyResourceTypeFilter(all, input.ResourceTypeFilters)
	}

	if len(input.RegionFilters) > 0 {
		all = applyRegionFilter(all, input.RegionFilters)
	}

	if len(input.TagKeyFilters) > 0 {
		all = applyTagKeyFilter(all, input.TagKeyFilters)
	}

	// No tag policy means zero non-compliant resources regardless of filters.
	_ = all
	_ = maxResults

	return &GetComplianceSummaryOutput{SummaryList: []ComplianceSummary{}}
}

// ListRequiredTagsInput is the request payload for ListRequiredTags.
type ListRequiredTagsInput struct {
	// MaxResults is the maximum number of results per page.
	MaxResults *int32 `json:"MaxResults,omitempty"`
	// NextToken is the cursor from a previous call.
	NextToken *string `json:"NextToken,omitempty"`
}

// RequiredTag describes required tags for a resource type.
type RequiredTag struct {
	ResourceType                *string  `json:"ResourceType,omitempty"`
	CloudFormationResourceTypes []string `json:"CloudFormationResourceTypes,omitempty"`
	ReportingTagKeys            []string `json:"ReportingTagKeys,omitempty"`
}

// ListRequiredTagsOutput is the response payload for ListRequiredTags.
type ListRequiredTagsOutput struct {
	// NextToken is the cursor for the next page.
	NextToken *string `json:"NextToken,omitempty"`
	// RequiredTags lists the required tags for supported resource types.
	RequiredTags []RequiredTag `json:"RequiredTags"`
}

// ListRequiredTags returns required tags for supported resource types.
// The in-memory backend always returns an empty list.
func (b *InMemoryBackend) ListRequiredTags(_ context.Context, _ *ListRequiredTagsInput) *ListRequiredTagsOutput {
	return &ListRequiredTagsOutput{RequiredTags: []RequiredTag{}}
}
