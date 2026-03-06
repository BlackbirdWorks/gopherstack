// Package resourcegroupstaggingapi provides a mock implementation of the AWS Resource Groups
// Tagging API service. It provides a cross-service tag-based resource lookup layer on top
// of the existing per-service backends.
package resourcegroupstaggingapi

import (
	"slices"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// TaggedResource represents a resource with its ARN, type, and tag set.
type TaggedResource struct {
	Tags         map[string]string
	ResourceARN  string
	ResourceType string
}

// TagFilter represents a single tag filter: resources must have the given key
// and (if Values is non-empty) one of the given values.
type TagFilter struct {
	// Key is the tag key to filter by.
	Key string `json:"Key"`
	// Values are the acceptable tag values; empty means any value.
	Values []string `json:"Values,omitempty"`
}

// ResourceProvider is a function that enumerates tagged resources for a service.
// Registered providers are called on every GetResources request.
type ResourceProvider func() []TaggedResource

// ARNTagger applies a set of tags to the resource identified by the given ARN.
// It returns true when it handled the ARN (even on error) and false when the ARN
// belongs to a different service and should be tried by the next registered tagger.
type ARNTagger func(arn string, tags map[string]string) (bool, error)

// ARNUntagger removes the specified tag keys from the resource identified by the
// given ARN. Same handled/not-handled semantics as ARNTagger.
type ARNUntagger func(arn string, keys []string) (bool, error)

// InMemoryBackend is the in-memory store for the Resource Groups Tagging API.
// It maintains a registry of service-specific resource providers and tagging adapters.
type InMemoryBackend struct {
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
	providers []ResourceProvider
	taggers   []ARNTagger
	untaggers []ARNUntagger
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("resourcegroupstaggingapi"),
	}
}

// RegisterProvider adds a tagged-resource provider to the registry.
// Providers are called in registration order on every GetResources request.
func (b *InMemoryBackend) RegisterProvider(p ResourceProvider) {
	b.mu.Lock("RegisterProvider")
	defer b.mu.Unlock()

	b.providers = append(b.providers, p)
}

// RegisterARNTagger adds an ARN-based tagger to the registry.
// Taggers are tried in registration order; the first one that returns
// handled=true is used and the rest are skipped.
func (b *InMemoryBackend) RegisterARNTagger(t ARNTagger) {
	b.mu.Lock("RegisterARNTagger")
	defer b.mu.Unlock()

	b.taggers = append(b.taggers, t)
}

// RegisterARNUntagger adds an ARN-based untagger to the registry.
// Same semantics as RegisterARNTagger.
func (b *InMemoryBackend) RegisterARNUntagger(u ARNUntagger) {
	b.mu.Lock("RegisterARNUntagger")
	defer b.mu.Unlock()

	b.untaggers = append(b.untaggers, u)
}

// getResources collects all resources from all registered providers.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) getResources() []TaggedResource {
	perProvider := make([][]TaggedResource, 0, len(b.providers))
	for _, p := range b.providers {
		perProvider = append(perProvider, p())
	}

	return slices.Concat(perProvider...)
}

// GetResourcesInput is the request payload for GetResources.
type GetResourcesInput struct {
	ResourcesPerPage    *int32      `json:"ResourcesPerPage,omitempty"`
	PaginationToken     string      `json:"PaginationToken,omitempty"`
	TagFilters          []TagFilter `json:"TagFilters,omitempty"`
	ResourceTypeFilters []string    `json:"ResourceTypeFilters,omitempty"`
}

// GetResourcesOutput is the response payload for GetResources.
type GetResourcesOutput struct {
	PaginationToken        *string              `json:"PaginationToken,omitempty"`
	ResourceTagMappingList []ResourceTagMapping `json:"ResourceTagMappingList"`
}

// ResourceTagMapping associates a resource ARN with its tags.
type ResourceTagMapping struct {
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

const (
	defaultResourcesPerPage = 100
	maxResourcesPerPage     = 100
)

// GetResources queries resources across all registered providers. It applies
// tag filters, resource-type filters, and cursor-based pagination.
func (b *InMemoryBackend) GetResources(input *GetResourcesInput) *GetResourcesOutput {
	b.mu.RLock("GetResources")
	defer b.mu.RUnlock()

	all := b.getResources()
	all = applyResourceTypeFilter(all, input.ResourceTypeFilters)
	all = applyTagFilters(all, input.TagFilters)

	sort.Slice(all, func(i, j int) bool { return all[i].ResourceARN < all[j].ResourceARN })

	page, nextToken := paginateResources(all, input.PaginationToken, resolvePageSize(input.ResourcesPerPage))

	return &GetResourcesOutput{
		ResourceTagMappingList: buildTagMappings(page),
		PaginationToken:        nextToken,
	}
}

// applyResourceTypeFilter filters resources by resource type.
// Returns all resources if typeFilters is empty.
func applyResourceTypeFilter(all []TaggedResource, typeFilters []string) []TaggedResource {
	if len(typeFilters) == 0 {
		return all
	}

	typeSet := make(map[string]struct{}, len(typeFilters))
	for _, rt := range typeFilters {
		typeSet[strings.ToLower(rt)] = struct{}{}
	}

	filtered := make([]TaggedResource, 0, len(all))
	for _, r := range all {
		if _, ok := typeSet[strings.ToLower(r.ResourceType)]; ok {
			filtered = append(filtered, r)
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

// resolvePageSize returns the effective page size, capped at maxResourcesPerPage.
func resolvePageSize(perPage *int32) int {
	if perPage == nil || *perPage <= 0 {
		return defaultResourcesPerPage
	}

	return min(int(*perPage), maxResourcesPerPage)
}

// paginateResources applies cursor-based pagination and returns the current page and the
// next pagination token (nil when there are no more results).
func paginateResources(all []TaggedResource, token string, pageSize int) ([]TaggedResource, *string) {
	start := findTokenStart(all, token)
	page := all[start:]

	if len(page) <= pageSize {
		return page, nil
	}

	page = page[:pageSize]
	tok := page[len(page)-1].ResourceARN

	return page, &tok
}

// findTokenStart returns the index after the resource whose ARN equals token,
// or 0 if the token is empty or not found.
func findTokenStart(all []TaggedResource, token string) int {
	if token == "" {
		return 0
	}

	for i, r := range all {
		if r.ResourceARN == token {
			return i + 1
		}
	}

	return 0
}

// buildTagMappings converts a slice of TaggedResource into ResourceTagMapping output,
// sorting tag keys alphabetically within each mapping.
func buildTagMappings(page []TaggedResource) []ResourceTagMapping {
	mappings := make([]ResourceTagMapping, 0, len(page))

	for _, r := range page {
		m := ResourceTagMapping{ResourceARN: r.ResourceARN}
		keys := make([]string, 0, len(r.Tags))

		for k := range r.Tags {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		for _, k := range keys {
			m.Tags = append(m.Tags, Tag{Key: k, Value: r.Tags[k]})
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

// GetTagKeysOutput is the response payload for GetTagKeys.
type GetTagKeysOutput struct {
	PaginationToken *string  `json:"PaginationToken,omitempty"`
	TagKeys         []string `json:"TagKeys"`
}

// GetTagKeys returns all unique tag keys across all registered resource providers.
func (b *InMemoryBackend) GetTagKeys() *GetTagKeysOutput {
	b.mu.RLock("GetTagKeys")
	defer b.mu.RUnlock()

	all := b.getResources()
	keySet := make(map[string]struct{})

	for _, r := range all {
		for k := range r.Tags {
			keySet[k] = struct{}{}
		}
	}

	keys := make([]string, 0, len(keySet))
	for k := range keySet {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return &GetTagKeysOutput{TagKeys: keys}
}

// GetTagValuesInput is the request payload for GetTagValues.
type GetTagValuesInput struct {
	// Key is the tag key whose values to enumerate.
	Key string `json:"Key"`
	// PaginationToken is the cursor from a previous call.
	PaginationToken string `json:"PaginationToken,omitempty"`
}

// GetTagValuesOutput is the response payload for GetTagValues.
type GetTagValuesOutput struct {
	PaginationToken *string  `json:"PaginationToken,omitempty"`
	TagValues       []string `json:"TagValues"`
}

// GetTagValues returns all unique values for the given tag key.
func (b *InMemoryBackend) GetTagValues(input *GetTagValuesInput) *GetTagValuesOutput {
	b.mu.RLock("GetTagValues")
	defer b.mu.RUnlock()

	all := b.getResources()
	valSet := make(map[string]struct{})

	for _, r := range all {
		if v, ok := r.Tags[input.Key]; ok {
			valSet[v] = struct{}{}
		}
	}

	values := make([]string, 0, len(valSet))
	for v := range valSet {
		values = append(values, v)
	}

	sort.Strings(values)

	return &GetTagValuesOutput{TagValues: values}
}

// TagResourcesInput is the request payload for TagResources.
type TagResourcesInput struct {
	Tags            map[string]string `json:"Tags"`
	ResourceARNList []string          `json:"ResourceARNList"`
}

// TagResourcesOutput is the response payload for TagResources.
type TagResourcesOutput struct {
	// FailedResourcesMap maps ARN to failure reason for resources that could not be tagged.
	FailedResourcesMap map[string]FailureInfo `json:"FailedResourcesMap,omitempty"`
}

// FailureInfo describes why a particular resource could not be tagged.
type FailureInfo struct {
	// ErrorCode is the error code.
	ErrorCode string `json:"ErrorCode"`
	// ErrorMessage is the human-readable error message.
	ErrorMessage string `json:"ErrorMessage"`
	// StatusCode is the HTTP status code.
	StatusCode int `json:"StatusCode"`
}

// TagResources applies tags to the specified resources by routing to registered ARN taggers.
// Resources whose ARN does not match any registered tagger are reported in FailedResourcesMap
// with an InvalidParameterException, matching the AWS API behavior.
func (b *InMemoryBackend) TagResources(input *TagResourcesInput) *TagResourcesOutput {
	b.mu.RLock("TagResources")
	taggers := slices.Clone(b.taggers)
	b.mu.RUnlock()

	failed := make(map[string]FailureInfo)

	for _, arn := range input.ResourceARNList {
		var handled bool

		for _, t := range taggers {
			ok, err := t(arn, input.Tags)
			if ok {
				handled = true
				if err != nil {
					failed[arn] = FailureInfo{
						ErrorCode:    "InternalServiceException",
						ErrorMessage: err.Error(),
						StatusCode:   500, //nolint:mnd // HTTP 500
					}
				}

				break
			}
		}

		if !handled {
			failed[arn] = FailureInfo{
				ErrorCode:    "InvalidParameterException",
				ErrorMessage: "no registered tagger handles ARN: " + arn,
				StatusCode:   400, //nolint:mnd // HTTP 400
			}
		}
	}

	out := &TagResourcesOutput{}
	if len(failed) > 0 {
		out.FailedResourcesMap = failed
	}

	return out
}

// UntagResourcesInput is the request payload for UntagResources.
type UntagResourcesInput struct {
	// ResourceARNList is the list of ARNs to untag.
	ResourceARNList []string `json:"ResourceARNList"`
	// TagKeys is the list of tag keys to remove.
	TagKeys []string `json:"TagKeys"`
}

// UntagResourcesOutput is the response payload for UntagResources.
type UntagResourcesOutput struct {
	// FailedResourcesMap maps ARN to failure reason.
	FailedResourcesMap map[string]FailureInfo `json:"FailedResourcesMap,omitempty"`
}

// UntagResources removes the specified tag keys from the given resources.
func (b *InMemoryBackend) UntagResources(input *UntagResourcesInput) *UntagResourcesOutput {
	b.mu.RLock("UntagResources")
	untaggers := slices.Clone(b.untaggers)
	b.mu.RUnlock()

	failed := make(map[string]FailureInfo)

	for _, arn := range input.ResourceARNList {
		var handled bool

		for _, u := range untaggers {
			ok, err := u(arn, input.TagKeys)
			if ok {
				handled = true
				if err != nil {
					failed[arn] = FailureInfo{
						ErrorCode:    "InternalServiceException",
						ErrorMessage: err.Error(),
						StatusCode:   500, //nolint:mnd // HTTP 500
					}
				}

				break
			}
		}

		if !handled {
			failed[arn] = FailureInfo{
				ErrorCode:    "InvalidParameterException",
				ErrorMessage: "no registered untagger handles ARN: " + arn,
				StatusCode:   400, //nolint:mnd // HTTP 400
			}
		}
	}

	out := &UntagResourcesOutput{}
	if len(failed) > 0 {
		out.FailedResourcesMap = failed
	}

	return out
}
