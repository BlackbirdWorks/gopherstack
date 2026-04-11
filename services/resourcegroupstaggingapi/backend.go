// Package resourcegroupstaggingapi provides a mock implementation of the AWS Resource Groups
// Tagging API service. It provides a cross-service tag-based resource lookup layer on top
// of the existing per-service backends.
package resourcegroupstaggingapi

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// ErrMissingS3Bucket is returned when StartReportCreation is called without an S3 bucket.
var ErrMissingS3Bucket = errors.New("S3Bucket is required")

// ErrValidation is returned when a request fails parameter validation.
var ErrValidation = errors.New("ValidationException")

const (
	// maxARNsPerTagRequest is the maximum number of ARNs in a single TagResources or
	// UntagResources request, matching the AWS API limit.
	maxARNsPerTagRequest = 20

	// maxTagFilters is the maximum number of tag filters in a GetResources request.
	maxTagFilters = 50

	// maxTagsPerRequest is the maximum number of tag key-value pairs in a TagResources request.
	maxTagsPerRequest = 50

	// maxTagKeyLength is the maximum length of a tag key.
	maxTagKeyLength = 128

	// maxTagValueLength is the maximum length of a tag value.
	maxTagValueLength = 256
)

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

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
	mu          *lockmetrics.RWMutex
	reportState *reportCreationState
	nowFunc     func() string
	accountID   string
	region      string
	providers   []ResourceProvider
	taggers     []ARNTagger
	untaggers   []ARNUntagger
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("resourcegroupstaggingapi"),
	}

	b.nowFunc = b.defaultNow

	return b
}

// defaultNow returns the current UTC time in RFC3339 format.
func (b *InMemoryBackend) defaultNow() string {
	return time.Now().UTC().Format(time.RFC3339)
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears dynamic per-test state (reportState) but intentionally preserves
// the registered providers, taggers, and untaggers. These are wired at server
// startup by wireResourceGroupsTagging and must persist across service resets,
// otherwise the cross-service tagging integration breaks.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.reportState = nil
}

// now returns the current time string using nowFunc.
func (b *InMemoryBackend) now() string {
	return b.nowFunc()
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
	ResourcesPerPage          *int32      `json:"ResourcesPerPage,omitempty"`
	PaginationToken           string      `json:"PaginationToken,omitempty"`
	TagFilters                []TagFilter `json:"TagFilters,omitempty"`
	ResourceTypeFilters       []string    `json:"ResourceTypeFilters,omitempty"`
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
	KeysWithNonCompliantValues []string `json:"KeysWithNonCompliantValues,omitempty"`
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

const (
	defaultResourcesPerPage = 100
	maxResourcesPerPage     = 100
)

// GetResources queries resources across all registered providers. It applies
// tag filters, resource-type filters, and cursor-based pagination.
func (b *InMemoryBackend) GetResources(input *GetResourcesInput) (*GetResourcesOutput, error) {
	if len(input.TagFilters) > maxTagFilters {
		return nil, fmt.Errorf("%w: TagFilters exceeds maximum of %d", ErrValidation, maxTagFilters)
	}

	b.mu.RLock("GetResources")
	defer b.mu.RUnlock()

	all := b.getResources()
	all = applyResourceTypeFilter(all, input.ResourceTypeFilters)
	all = applyTagFilters(all, input.TagFilters)

	sort.Slice(all, func(i, j int) bool { return all[i].ResourceARN < all[j].ResourceARN })

	page, nextToken := paginateResources(all, input.PaginationToken, resolvePageSize(input.ResourcesPerPage))

	return &GetResourcesOutput{
		ResourceTagMappingList: buildTagMappings(page, input.IncludeComplianceDetails),
		PaginationToken:        nextToken,
	}, nil
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

// paginateStrings applies cursor-based pagination over a sorted string slice and returns
// the current page and the next pagination token (nil when there are no more results).
func paginateStrings(all []string, token string, pageSize int) ([]string, *string) {
	start := 0

	if token != "" {
		for i, s := range all {
			if s == token {
				start = i + 1

				break
			}
		}
	}

	page := all[start:]

	if len(page) <= pageSize {
		return page, nil
	}

	page = page[:pageSize]
	tok := page[len(page)-1]

	return page, &tok
}

// ptrStringValue dereferences a *string and returns "" for nil.
func ptrStringValue(s *string) string {
	if s == nil {
		return ""
	}

	return *s
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
func buildTagMappings(page []TaggedResource, includeCompliance bool) []ResourceTagMapping {
	mappings := make([]ResourceTagMapping, 0, len(page))

	for _, r := range page {
		m := ResourceTagMapping{
			ResourceARN: r.ResourceARN,
			Tags:        make([]Tag, 0, len(r.Tags)),
		}
		keys := make([]string, 0, len(r.Tags))

		for k := range r.Tags {
			keys = append(keys, k)
		}

		sort.Strings(keys)

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

// GetTagKeysInput is the request payload for GetTagKeys.
type GetTagKeysInput struct {
	// PaginationToken is the cursor from a previous call.
	PaginationToken *string `json:"PaginationToken,omitempty"`
}

// GetTagKeysOutput is the response payload for GetTagKeys.
type GetTagKeysOutput struct {
	PaginationToken *string  `json:"PaginationToken,omitempty"`
	TagKeys         []string `json:"TagKeys"`
}

// GetTagKeys returns all unique tag keys across all registered resource providers.
// Keys are returned in sorted order, with optional cursor-based pagination.
func (b *InMemoryBackend) GetTagKeys(input *GetTagKeysInput) *GetTagKeysOutput {
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

	page, nextToken := paginateStrings(keys, ptrStringValue(input.PaginationToken), defaultResourcesPerPage)

	return &GetTagKeysOutput{TagKeys: page, PaginationToken: nextToken}
}

// GetTagValuesInput is the request payload for GetTagValues.
type GetTagValuesInput struct {
	// Key is the tag key whose values to enumerate.
	Key *string `json:"Key,omitempty"`
	// PaginationToken is the cursor from a previous call.
	PaginationToken *string `json:"PaginationToken,omitempty"`
}

// GetTagValuesOutput is the response payload for GetTagValues.
type GetTagValuesOutput struct {
	PaginationToken *string  `json:"PaginationToken,omitempty"`
	TagValues       []string `json:"TagValues"`
}

// GetTagValues returns all unique values for the given tag key.
// Values are returned in sorted order, with optional cursor-based pagination.
func (b *InMemoryBackend) GetTagValues(input *GetTagValuesInput) *GetTagValuesOutput {
	b.mu.RLock("GetTagValues")
	defer b.mu.RUnlock()

	if input.Key == nil {
		return &GetTagValuesOutput{TagValues: []string{}}
	}

	all := b.getResources()
	valSet := make(map[string]struct{})
	key := *input.Key

	for _, r := range all {
		if v, ok := r.Tags[key]; ok {
			valSet[v] = struct{}{}
		}
	}

	values := make([]string, 0, len(valSet))
	for v := range valSet {
		values = append(values, v)
	}

	sort.Strings(values)

	page, nextToken := paginateStrings(values, ptrStringValue(input.PaginationToken), defaultResourcesPerPage)

	return &GetTagValuesOutput{TagValues: page, PaginationToken: nextToken}
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
// validateTagResourcesInput validates the TagResources request parameters.
func validateTagResourcesInput(input *TagResourcesInput) error {
	if len(input.ResourceARNList) == 0 {
		return fmt.Errorf("%w: ResourceARNList must not be empty", ErrValidation)
	}

	if len(input.ResourceARNList) > maxARNsPerTagRequest {
		return fmt.Errorf("%w: ResourceARNList exceeds maximum of %d", ErrValidation, maxARNsPerTagRequest)
	}

	if len(input.Tags) == 0 {
		return fmt.Errorf("%w: Tags must not be empty", ErrValidation)
	}

	if len(input.Tags) > maxTagsPerRequest {
		return fmt.Errorf("%w: Tags exceeds maximum of %d", ErrValidation, maxTagsPerRequest)
	}

	return validateTagEntries(input.Tags)
}

// validateTagEntries validates all tag key-value pairs against AWS limits.
func validateTagEntries(tags map[string]string) error {
	for k, v := range tags {
		if k == "" {
			return fmt.Errorf("%w: tag key must not be empty", ErrValidation)
		}

		if len(k) > maxTagKeyLength {
			return fmt.Errorf("%w: tag key exceeds maximum length of %d", ErrValidation, maxTagKeyLength)
		}

		if len(v) > maxTagValueLength {
			return fmt.Errorf(
				"%w: tag value for key %q exceeds maximum length of %d",
				ErrValidation,
				k,
				maxTagValueLength,
			)
		}
	}

	return nil
}

func (b *InMemoryBackend) TagResources(input *TagResourcesInput) (*TagResourcesOutput, error) {
	if err := validateTagResourcesInput(input); err != nil {
		return nil, err
	}

	b.mu.RLock("TagResources")
	taggers := slices.Clone(b.taggers)
	b.mu.RUnlock()

	// Deep-copy the tag map so that tagger callbacks cannot mutate the caller's map.
	tagsCopy := maps.Clone(input.Tags)

	failed := make(map[string]FailureInfo)

	for _, arn := range input.ResourceARNList {
		var handled bool

		for _, t := range taggers {
			ok, err := t(arn, tagsCopy)
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

	return out, nil
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
func (b *InMemoryBackend) UntagResources(input *UntagResourcesInput) (*UntagResourcesOutput, error) {
	if len(input.ResourceARNList) == 0 {
		return nil, fmt.Errorf("%w: ResourceARNList must not be empty", ErrValidation)
	}

	if len(input.ResourceARNList) > maxARNsPerTagRequest {
		return nil, fmt.Errorf("%w: ResourceARNList exceeds maximum of %d", ErrValidation, maxARNsPerTagRequest)
	}

	if len(input.TagKeys) == 0 {
		return nil, fmt.Errorf("%w: TagKeys must not be empty", ErrValidation)
	}

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

	return out, nil
}

// reportStatusSucceeded is the status for a successfully created report.
const reportStatusSucceeded = "SUCCEEDED"

// reportStatusNoReport is the status when no report has been generated in the last 90 days.
const reportStatusNoReport = "NO REPORT"

// reportS3PathTemplate is the S3 path template for generated reports.
const reportS3PathTemplate = "AwsTagPolicies/report.csv"

// reportCreationState holds the state of a StartReportCreation job.
type reportCreationState struct {
	S3Location string `json:"s3Location"`
	StartDate  string `json:"startDate"`
	Status     string `json:"status"`
}

// StartReportCreationInput is the request payload for StartReportCreation.
type StartReportCreationInput struct {
	// S3Bucket is the Amazon S3 bucket to store the report in.
	S3Bucket string `json:"S3Bucket"`
}

// StartReportCreationOutput is the response payload for StartReportCreation.
type StartReportCreationOutput struct{}

// StartReportCreation records a new report creation request.
// In the in-memory backend, the report is immediately set to SUCCEEDED.
func (b *InMemoryBackend) StartReportCreation(input *StartReportCreationInput) (*StartReportCreationOutput, error) {
	if input.S3Bucket == "" {
		return nil, ErrMissingS3Bucket
	}

	b.mu.Lock("StartReportCreation")
	defer b.mu.Unlock()

	b.reportState = &reportCreationState{
		S3Location: "s3://" + input.S3Bucket + "/" + reportS3PathTemplate,
		StartDate:  b.now(),
		Status:     reportStatusSucceeded,
	}

	return &StartReportCreationOutput{}, nil
}

// DescribeReportCreationInput is the request payload for DescribeReportCreation.
type DescribeReportCreationInput struct{}

// DescribeReportCreationOutput is the response payload for DescribeReportCreation.
type DescribeReportCreationOutput struct {
	// ErrorMessage is set when Status is FAILED.
	ErrorMessage *string `json:"ErrorMessage,omitempty"`
	// S3Location is the path to the report in the S3 bucket.
	S3Location *string `json:"S3Location,omitempty"`
	// StartDate is the date and time that the report was started.
	StartDate *string `json:"StartDate,omitempty"`
	// Status is the current status of the report (RUNNING, SUCCEEDED, FAILED, NO REPORT).
	Status *string `json:"Status"`
}

// DescribeReportCreation returns the status of the most recent StartReportCreation operation.
func (b *InMemoryBackend) DescribeReportCreation() *DescribeReportCreationOutput {
	b.mu.RLock("DescribeReportCreation")
	defer b.mu.RUnlock()

	if b.reportState == nil {
		s := reportStatusNoReport

		return &DescribeReportCreationOutput{Status: &s}
	}

	s3Loc := b.reportState.S3Location
	startDate := b.reportState.StartDate
	status := b.reportState.Status

	return &DescribeReportCreationOutput{
		S3Location: &s3Loc,
		StartDate:  &startDate,
		Status:     &status,
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

// GetComplianceSummary returns compliance summary data.
// The in-memory backend always returns an empty SummaryList.
func (b *InMemoryBackend) GetComplianceSummary(_ *GetComplianceSummaryInput) *GetComplianceSummaryOutput {
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
func (b *InMemoryBackend) ListRequiredTags(_ *ListRequiredTagsInput) *ListRequiredTagsOutput {
	return &ListRequiredTagsOutput{RequiredTags: []RequiredTag{}}
}
