package resourcegroupstaggingapi

import "context"

// StorageBackend is the interface for the Resource Groups Tagging API backend.
type StorageBackend interface {
	// Tag/resource operations
	GetResources(ctx context.Context, input *GetResourcesInput) (*GetResourcesOutput, error)
	GetTagKeys(ctx context.Context, input *GetTagKeysInput) *GetTagKeysOutput
	GetTagValues(ctx context.Context, input *GetTagValuesInput) *GetTagValuesOutput
	TagResources(ctx context.Context, input *TagResourcesInput) (*TagResourcesOutput, error)
	UntagResources(ctx context.Context, input *UntagResourcesInput) (*UntagResourcesOutput, error)

	// Report creation operations
	StartReportCreation(ctx context.Context, input *StartReportCreationInput) (*StartReportCreationOutput, error)
	DescribeReportCreation(ctx context.Context) *DescribeReportCreationOutput

	// Compliance and policy operations
	GetComplianceSummary(ctx context.Context, input *GetComplianceSummaryInput) *GetComplianceSummaryOutput
	ListRequiredTags(ctx context.Context, input *ListRequiredTagsInput) *ListRequiredTagsOutput

	// Provider registration
	RegisterProvider(p ResourceProvider)
	RegisterFilteredProvider(p FilteredResourceProvider)
	RegisterARNTagger(t ARNTagger)
	RegisterARNUntagger(u ARNUntagger)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// Resettable is implemented by any type that supports being reset.
type Resettable interface {
	Reset()
}
