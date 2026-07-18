package resourcegroupstaggingapi

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// maxTagFilters is the maximum number of tag filters in a GetResources request.
const maxTagFilters = 50

// maxTagFilterKeyLength is the maximum length of a TagFilter Key field.
const maxTagFilterKeyLength = 128

// maxTagFilterValueLength is the maximum length of a single TagFilter Value entry.
const maxTagFilterValueLength = 256

// maxTagFilterValues is the maximum number of Values in a single TagFilter.
const maxTagFilterValues = 256

// minTagsPerPage is the minimum allowed TagsPerPage value for GetResources.
const minTagsPerPage = 100

// maxTagsPerPage is the maximum allowed TagsPerPage value for GetResources.
const maxTagsPerPage = 500

// resourceTypeFilterRE validates a ResourceTypeFilters entry.
// Service prefix must be lowercase; resource suffix may contain mixed-case characters.
var resourceTypeFilterRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*(:[a-zA-Z0-9][a-zA-Z0-9-_/.]*)?$`)

// GetResourcesInput is the request payload for GetResources.
type GetResourcesInput struct {
	ResourcesPerPage          *int32      `json:"ResourcesPerPage,omitempty"`
	TagsPerPage               *int32      `json:"TagsPerPage,omitempty"`
	PaginationToken           string      `json:"PaginationToken,omitempty"`
	TagFilters                []TagFilter `json:"TagFilters,omitempty"`
	ResourceTypeFilters       []string    `json:"ResourceTypeFilters,omitempty"`
	ResourceARNList           []string    `json:"ResourceARNList,omitempty"`
	IncludeComplianceDetails  bool        `json:"IncludeComplianceDetails,omitempty"`
	ExcludeCompliantResources bool        `json:"ExcludeCompliantResources,omitempty"`
}

// GetResourcesOutput is the response payload for GetResources.
type GetResourcesOutput struct {
	PaginationToken        *string              `json:"PaginationToken,omitempty"`
	ResourceTagMappingList []ResourceTagMapping `json:"ResourceTagMappingList"`
}

// ComplianceDetails records tag-policy compliance information for a resource.
type ComplianceDetails struct {
	KeysWithNoncompliantValues []string `json:"KeysWithNoncompliantValues,omitempty"`
	NoncompliantKeys           []string `json:"NoncompliantKeys,omitempty"`
	ComplianceStatus           bool     `json:"ComplianceStatus"`
}

// ResourceTagMapping associates a resource ARN with its tags.
type ResourceTagMapping struct {
	// ComplianceDetails is populated when IncludeComplianceDetails is true.
	ComplianceDetails *ComplianceDetails `json:"ComplianceDetails,omitempty"`
	// ResourceARN is the full ARN of the resource.
	ResourceARN string `json:"ResourceARN"`
	// Tags is the list of {Key, Value} pairs.
	Tags []Tag `json:"Tags"`
}

// Tag is a single key-value pair.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// validateTagFilter validates a single TagFilter entry at position i.
func validateTagFilter(i int, f TagFilter, seenKeys map[string]struct{}) error {
	if f.Key == "" {
		return fmt.Errorf("%w: TagFilters[%d].Key must not be empty", ErrValidation, i)
	}

	if len(f.Key) > maxTagFilterKeyLength {
		return fmt.Errorf(
			"%w: TagFilters[%d].Key exceeds maximum length of %d",
			ErrValidation, i, maxTagFilterKeyLength,
		)
	}

	if len(f.Values) > maxTagFilterValues {
		return fmt.Errorf(
			"%w: TagFilters[%d].Values exceeds maximum of %d",
			ErrValidation, i, maxTagFilterValues,
		)
	}

	for j, v := range f.Values {
		if len(v) > maxTagFilterValueLength {
			return fmt.Errorf(
				"%w: TagFilters[%d].Values[%d] exceeds maximum length of %d",
				ErrValidation, i, j, maxTagFilterValueLength,
			)
		}
	}

	if _, exists := seenKeys[f.Key]; exists {
		return fmt.Errorf("%w: duplicate TagFilter key %q", ErrValidation, f.Key)
	}

	seenKeys[f.Key] = struct{}{}

	return nil
}

// validateResourceARNListExclusivity enforces the real API's constraint that
// ResourceARNList cannot be combined with ResourceTypeFilters, TagFilters, or any of
// the pagination parameters (ResourcesPerPage, TagsPerPage, PaginationToken) -- real AWS
// returns InvalidParameterException ("You can't specify both this parameter and the ...
// parameter in the same request") when they are combined.
func validateResourceARNListExclusivity(input *GetResourcesInput) error {
	if len(input.ResourceARNList) == 0 {
		return nil
	}

	if len(input.ResourceTypeFilters) > 0 {
		return fmt.Errorf("%w: ResourceARNList can't be specified together with ResourceTypeFilters", ErrValidation)
	}

	if len(input.TagFilters) > 0 {
		return fmt.Errorf("%w: ResourceARNList can't be specified together with TagFilters", ErrValidation)
	}

	if input.ResourcesPerPage != nil || input.TagsPerPage != nil || input.PaginationToken != "" {
		return fmt.Errorf(
			"%w: ResourceARNList can't be specified together with ResourcesPerPage, TagsPerPage, or PaginationToken",
			ErrValidation,
		)
	}

	return nil
}

// validateGetResourcesInput validates the GetResources request parameters.
func validateGetResourcesInput(input *GetResourcesInput) error {
	if err := validateResourceARNListExclusivity(input); err != nil {
		return err
	}

	if input.ExcludeCompliantResources && !input.IncludeComplianceDetails {
		return fmt.Errorf(
			"%w: ExcludeCompliantResources requires IncludeComplianceDetails to be true",
			ErrValidation,
		)
	}

	if len(input.TagFilters) > maxTagFilters {
		return fmt.Errorf("%w: TagFilters exceeds maximum of %d", ErrValidation, maxTagFilters)
	}

	seenKeys := make(map[string]struct{}, len(input.TagFilters))

	for i, f := range input.TagFilters {
		if err := validateTagFilter(i, f, seenKeys); err != nil {
			return err
		}
	}

	for _, rt := range input.ResourceTypeFilters {
		if !resourceTypeFilterRE.MatchString(rt) {
			return fmt.Errorf("%w: invalid ResourceTypeFilter format %q", ErrValidation, rt)
		}
	}

	if input.TagsPerPage != nil {
		tpp := *input.TagsPerPage
		if tpp < int32(minTagsPerPage) || tpp > int32(maxTagsPerPage) {
			return fmt.Errorf(
				"%w: TagsPerPage must be between %d and %d",
				ErrValidation, minTagsPerPage, maxTagsPerPage,
			)
		}
	}

	return nil
}

// matchesResourceTypeFilter returns true when resourceType matches filter.
// Matching is case-sensitive. A service-only filter (no colon) matches any
// resource whose type begins with "service:".
func matchesResourceTypeFilter(resourceType, filter string) bool {
	if resourceType == filter {
		return true
	}

	if !strings.Contains(filter, ":") && strings.HasPrefix(resourceType, filter+":") {
		return true
	}

	return false
}

// arnSplitParts is the maximum number of colon-separated components in an ARN.
const arnSplitParts = 6

// arnRegionIndex is the 0-based index of the region component after splitting.
const arnRegionIndex = 3

// arnMinComponents is the minimum number of components required for a region to be present.
const arnMinComponents = 5

// extractRegionFromARN returns the region segment from an ARN string,
// or "" when the ARN is too short to contain a region.
func extractRegionFromARN(arn string) string {
	parts := strings.SplitN(arn, ":", arnSplitParts)
	if len(parts) < arnMinComponents {
		return ""
	}

	return parts[arnRegionIndex]
}

// applyRegionFilter returns only those resources whose ARN region is in regionFilters.
func applyRegionFilter(all []TaggedResource, regionFilters []string) []TaggedResource {
	if len(regionFilters) == 0 {
		return all
	}

	regionSet := make(map[string]struct{}, len(regionFilters))
	for _, r := range regionFilters {
		regionSet[r] = struct{}{}
	}

	filtered := make([]TaggedResource, 0, len(all))

	for _, r := range all {
		region := extractRegionFromARN(r.ResourceARN)
		if _, ok := regionSet[region]; ok {
			filtered = append(filtered, r)
		}
	}

	return filtered
}

// applyTagKeyFilter returns only those resources that possess ALL keys in tagKeyFilters.
func applyTagKeyFilter(all []TaggedResource, tagKeyFilters []string) []TaggedResource {
	if len(tagKeyFilters) == 0 {
		return all
	}

	filtered := make([]TaggedResource, 0, len(all))

	for _, r := range all {
		hasAll := true

		for _, key := range tagKeyFilters {
			if _, ok := r.Tags[key]; !ok {
				hasAll = false

				break
			}
		}

		if hasAll {
			filtered = append(filtered, r)
		}
	}

	return filtered
}

// GetResources queries resources across all registered providers. It applies
// tag filters, resource-type filters, compliance filters, and cursor-based pagination.
// When filtered providers are registered the filters are pushed down to them;
// the returned results are still post-filtered to ensure correctness from
// plain providers.
func (b *InMemoryBackend) GetResources(ctx context.Context, input *GetResourcesInput) (*GetResourcesOutput, error) {
	if err := validateGetResourcesInput(input); err != nil {
		return nil, err
	}

	// Use a write lock because getResources may update the cache.
	b.mu.Lock("GetResources")
	defer b.mu.Unlock()

	all := b.getResources(ctx, input.TagFilters, input.ResourceTypeFilters)
	all = applyARNListFilter(all, input.ResourceARNList)
	all = applyResourceTypeFilter(all, input.ResourceTypeFilters)
	all = applyTagFilters(all, input.TagFilters)

	// ExcludeCompliantResources: since the mock has no tag policy, every resource is
	// considered compliant (ComplianceStatus=true).  Excluding them yields an empty set.
	if input.ExcludeCompliantResources {
		all = all[:0]
	}

	sort.Slice(all, func(i, j int) bool { return all[i].ResourceARN < all[j].ResourceARN })

	page, nextToken := paginateResources(
		all, input.PaginationToken, resolvePageSize(input.ResourcesPerPage), resolveTagsPerPage(input.TagsPerPage),
	)

	return &GetResourcesOutput{
		ResourceTagMappingList: buildTagMappings(page, input.IncludeComplianceDetails),
		PaginationToken:        nextToken,
	}, nil
}

// applyARNListFilter returns only those resources whose ARN is in the provided set.
// Returns all resources when arnList is empty.
func applyARNListFilter(all []TaggedResource, arnList []string) []TaggedResource {
	if len(arnList) == 0 {
		return all
	}

	arnSet := make(map[string]struct{}, len(arnList))
	for _, a := range arnList {
		arnSet[a] = struct{}{}
	}

	filtered := make([]TaggedResource, 0, len(arnList))

	for _, r := range all {
		if _, ok := arnSet[r.ResourceARN]; ok {
			filtered = append(filtered, r)
		}
	}

	return filtered
}

// applyResourceTypeFilter filters resources by resource type.
// Matching is case-sensitive. A service-only filter (no colon) matches any
// resource whose ResourceType starts with "service:".
// Returns all resources if typeFilters is empty.
func applyResourceTypeFilter(all []TaggedResource, typeFilters []string) []TaggedResource {
	if len(typeFilters) == 0 {
		return all
	}

	filtered := make([]TaggedResource, 0, len(all))

	for _, r := range all {
		for _, f := range typeFilters {
			if matchesResourceTypeFilter(r.ResourceType, f) {
				filtered = append(filtered, r)

				break
			}
		}
	}

	return filtered
}

// applyTagFilters filters resources by tag filters (AND between filters, OR within a filter).
func applyTagFilters(all []TaggedResource, filters []TagFilter) []TaggedResource {
	for _, f := range filters {
		filtered := make([]TaggedResource, 0, len(all))
		for _, r := range all {
			if matchesTagFilter(r.Tags, f) {
				filtered = append(filtered, r)
			}
		}

		all = filtered
	}

	return all
}

// buildTagMappings converts a slice of TaggedResource into ResourceTagMapping output,
// sorting tag keys alphabetically within each mapping.
func buildTagMappings(page []TaggedResource, includeCompliance bool) []ResourceTagMapping {
	mappings := make([]ResourceTagMapping, 0, len(page))

	for _, r := range page {
		m := ResourceTagMapping{
			ResourceARN: r.ResourceARN,
			Tags:        make([]Tag, 0, len(r.Tags)),
		}
		keys := collections.SortedKeys(r.Tags)

		for _, k := range keys {
			m.Tags = append(m.Tags, Tag{Key: k, Value: r.Tags[k]})
		}

		if includeCompliance {
			m.ComplianceDetails = &ComplianceDetails{ComplianceStatus: true}
		}

		mappings = append(mappings, m)
	}

	return mappings
}

// matchesTagFilter returns true when the given tag map satisfies the filter.
// An empty Values slice means any value is accepted.
func matchesTagFilter(tagMap map[string]string, f TagFilter) bool {
	val, ok := tagMap[f.Key]
	if !ok {
		return false
	}

	if len(f.Values) == 0 {
		return true
	}

	return slices.Contains(f.Values, val)
}
