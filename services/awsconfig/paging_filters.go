package awsconfig

// aggregateScopeFilters carries the AccountId/AwsRegion members shared by the aggregate
// Filters types (types.ConfigRuleComplianceSummaryFilters and the conformance-pack equivalent).
type aggregateScopeFilters struct {
	AccountID string `json:"AccountId,omitempty"`
	AwsRegion string `json:"AwsRegion,omitempty"`
}

// aggregateScopeMatches reports whether the single emulated account/region satisfies f.
func (b *InMemoryBackend) aggregateScopeMatches(f *aggregateScopeFilters) bool {
	if f == nil {
		return true
	}

	return (f.AccountID == "" || f.AccountID == b.accountID) && (f.AwsRegion == "" || f.AwsRegion == b.region)
}
