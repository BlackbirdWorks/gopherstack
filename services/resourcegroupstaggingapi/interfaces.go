package resourcegroupstaggingapi

// StorageBackend is the interface for the Resource Groups Tagging API backend.
type StorageBackend interface {
	// Tag/resource operations
	GetResources(input *GetResourcesInput) *GetResourcesOutput
	GetTagKeys(input *GetTagKeysInput) *GetTagKeysOutput
	GetTagValues(input *GetTagValuesInput) *GetTagValuesOutput
	TagResources(input *TagResourcesInput) *TagResourcesOutput
	UntagResources(input *UntagResourcesInput) *UntagResourcesOutput

	// Report creation operations
	StartReportCreation(input *StartReportCreationInput) (*StartReportCreationOutput, error)
	DescribeReportCreation() *DescribeReportCreationOutput

	// Compliance and policy operations
	GetComplianceSummary(input *GetComplianceSummaryInput) *GetComplianceSummaryOutput
	ListRequiredTags(input *ListRequiredTagsInput) *ListRequiredTagsOutput

	// Provider registration
	RegisterProvider(p ResourceProvider)
	RegisterARNTagger(t ARNTagger)
	RegisterARNUntagger(u ARNUntagger)

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// Resettable is implemented by any type that supports being reset.
type Resettable interface {
	Reset()
}
