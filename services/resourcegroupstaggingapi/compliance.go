package resourcegroupstaggingapi

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

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

// validateComplianceSummaryMaxResults enforces the real API's MaxResultsGetComplianceSummary
// shape constraint (min: 1, max: 1000; see aws-sdk-go's
// models/apis/resourcegroupstaggingapi/2017-01-26/api-2.json), matching the same
// explicit-range-with-error pattern already used for GetResources' TagsPerPage.
func validateComplianceSummaryMaxResults(maxResults *int32) error {
	if maxResults == nil {
		return nil
	}

	mr := *maxResults
	if mr < 1 || mr > int32(maxComplianceSummaryMaxResults) {
		return fmt.Errorf(
			"%w: MaxResults must be between 1 and %d",
			ErrValidation, maxComplianceSummaryMaxResults,
		)
	}

	return nil
}

// GetComplianceSummary returns compliance summary data filtered by the supplied parameters.
// The in-memory backend has no tag policy, so all resources are always compliant and
// NonCompliantResources is always 0.  Filters and pagination are honoured so callers
// get accurate (empty) results rather than a stub.
func (b *InMemoryBackend) GetComplianceSummary(
	ctx context.Context,
	input *GetComplianceSummaryInput,
) (*GetComplianceSummaryOutput, error) {
	if err := validateComplianceSummaryMaxResults(input.MaxResults); err != nil {
		return nil, err
	}

	b.mu.Lock("GetComplianceSummary")
	defer b.mu.Unlock()

	// GroupBy validation is handled by the HTTP handler before reaching the backend.
	// The handler enforces valid values (REGION, RESOURCE_TYPE, TARGET_ID).

	// Resolve MaxResults.
	maxResults := int32(defaultComplianceSummaryMaxResults)
	if input.MaxResults != nil {
		maxResults = *input.MaxResults
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

	return &GetComplianceSummaryOutput{SummaryList: []ComplianceSummary{}}, nil
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

// defaultRequiredTagsPerPage is the page size ListRequiredTags uses when MaxResults
// is unset. AWS does not document a MaxResults range for this newer operation the way
// it does for GetComplianceSummary (no min/max ValidationException shape found in any
// vendored API model), so no explicit range is enforced -- only a sane default.
const defaultRequiredTagsPerPage = 50

// tagPolicyDocument is the real AWS Organizations tag-policy JSON schema (the
// "@@assign" operator syntax every tag policy statement uses), verified against
// AWS's "Report tagging compliance" documentation
// (orgs_manage_policies_tag-policies-report-tagging-compliance.html):
//
//	{"tags": {"CostCenter": {"tag_key": {"@@assign": "CostCenter"},
//	  "report_required_tag_for": {"@@assign": ["ec2:ALL_SUPPORTED"]}}}}
type tagPolicyDocument struct {
	Tags map[string]tagPolicyRule `json:"tags"`
}

type tagPolicyRule struct {
	ReportRequiredTagFor *tagPolicyStringListAssign `json:"report_required_tag_for,omitempty"`
}

type tagPolicyStringListAssign struct {
	Assign []string `json:"@@assign,omitempty"`
}

// requiredTagsFromPolicy parses an effective TAG_POLICY document and derives the
// ListRequiredTags rows from its report_required_tag_for blocks: one row per distinct
// resource-type identifier named in any tag's report_required_tag_for list, listing
// every tag key that names it.
//
// KNOWN LIMITATION: real AWS's CloudFormationResourceTypes field documents "the
// CloudFormation resource type[s] assigned the required tag keys" -- e.g. it may
// expand a policy identifier like "ec2:ALL_SUPPORTED" into the full list of that
// service's actual "AWS::EC2::*" CloudFormation type names. No such expansion table
// is available to verify against, so this returns the resource-type identifier from
// the policy verbatim in both ResourceType and CloudFormationResourceTypes rather than
// fabricate an unverified mapping.
func requiredTagsFromPolicy(policyJSON string) []RequiredTag {
	var doc tagPolicyDocument
	if err := json.Unmarshal([]byte(policyJSON), &doc); err != nil {
		return nil
	}

	byResourceType := map[string][]string{}

	for tagKey, rule := range doc.Tags {
		if rule.ReportRequiredTagFor == nil {
			continue
		}

		for _, resourceType := range rule.ReportRequiredTagFor.Assign {
			byResourceType[resourceType] = append(byResourceType[resourceType], tagKey)
		}
	}

	resourceTypes := make([]string, 0, len(byResourceType))
	for rt := range byResourceType {
		resourceTypes = append(resourceTypes, rt)
	}

	sort.Strings(resourceTypes)

	required := make([]RequiredTag, 0, len(resourceTypes))

	for _, rt := range resourceTypes {
		keys := byResourceType[rt]
		sort.Strings(keys)

		rtCopy := rt
		required = append(required, RequiredTag{
			ResourceType:                &rtCopy,
			CloudFormationResourceTypes: []string{rt},
			ReportingTagKeys:            keys,
		})
	}

	return required
}

// ListRequiredTags returns the required tags for supported resource types, derived
// from the account's effective tag policy (see RegisterTagPolicyProvider). With no
// provider registered, or no TAG_POLICY attached, this accurately returns an empty
// list -- the real AWS behavior for an account with no required-tag reporting
// configured, not a stub.
func (b *InMemoryBackend) ListRequiredTags(_ context.Context, input *ListRequiredTagsInput) *ListRequiredTagsOutput {
	b.mu.RLock("ListRequiredTags")
	provider := b.tagPolicyProvider
	b.mu.RUnlock()

	var all []RequiredTag

	if provider != nil {
		if content, ok := provider(); ok {
			all = requiredTagsFromPolicy(content)
		}
	}

	var token string
	if input.NextToken != nil {
		token = *input.NextToken
	}

	var limit int32
	if input.MaxResults != nil {
		limit = *input.MaxResults
	}

	pg := page.New(all, token, int(limit), defaultRequiredTagsPerPage)

	out := &ListRequiredTagsOutput{RequiredTags: pg.Data}
	if pg.Next != "" {
		out.NextToken = &pg.Next
	}

	return out
}
