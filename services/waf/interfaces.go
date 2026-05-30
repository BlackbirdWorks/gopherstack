package waf

// StorageBackend is the interface for WAF Classic storage operations.
type StorageBackend interface {
	// Change tokens
	GetChangeToken() string
	GetChangeTokenStatus(token string) string

	// WebACL
	CreateWebACL(
		name, metricName string,
		defaultAction WafAction,
		tags map[string]string,
	) (*WebACL, error)
	GetWebACL(id string) (*WebACL, error)
	UpdateWebACL(id, changeToken string, defaultAction *WafAction, updates []WebACLUpdate) error
	DeleteWebACL(id, changeToken string) error
	ListWebACLs() []WebACLSummary

	// Rule
	CreateRule(name, metricName, changeToken string, tags map[string]string) (*Rule, error)
	GetRule(id string) (*Rule, error)
	UpdateRule(id, changeToken string, updates []RuleUpdate) error
	DeleteRule(id, changeToken string) error
	ListRules() []RuleSummary

	// IPSet
	CreateIPSet(name, changeToken string, tags map[string]string) (*IPSet, error)
	GetIPSet(id string) (*IPSet, error)
	UpdateIPSet(id, changeToken string, updates []IPSetUpdate) error
	DeleteIPSet(id, changeToken string) error
	ListIPSets() []IPSetSummary

	// ByteMatchSet
	CreateByteMatchSet(name, changeToken string) (*ByteMatchSet, error)
	GetByteMatchSet(id string) (*ByteMatchSet, error)
	UpdateByteMatchSet(id, changeToken string, updates []ByteMatchSetUpdate) error
	DeleteByteMatchSet(id, changeToken string) error
	ListByteMatchSets() []ByteMatchSetSummary

	// SizeConstraintSet
	CreateSizeConstraintSet(name, changeToken string) (*SizeConstraintSet, error)
	GetSizeConstraintSet(id string) (*SizeConstraintSet, error)
	UpdateSizeConstraintSet(id, changeToken string, updates []SizeConstraintSetUpdate) error
	DeleteSizeConstraintSet(id, changeToken string) error
	ListSizeConstraintSets() []SizeConstraintSetSummary

	// SqlInjectionMatchSet
	CreateSqlInjectionMatchSet(name, changeToken string) (*SqlInjectionMatchSet, error)
	GetSqlInjectionMatchSet(id string) (*SqlInjectionMatchSet, error)
	UpdateSqlInjectionMatchSet(id, changeToken string, updates []SqlInjectionMatchSetUpdate) error
	DeleteSqlInjectionMatchSet(id, changeToken string) error
	ListSqlInjectionMatchSets() []SqlInjectionMatchSetSummary

	// XssMatchSet
	CreateXssMatchSet(name, changeToken string) (*XssMatchSet, error)
	GetXssMatchSet(id string) (*XssMatchSet, error)
	UpdateXssMatchSet(id, changeToken string, updates []XssMatchSetUpdate) error
	DeleteXssMatchSet(id, changeToken string) error
	ListXssMatchSets() []XssMatchSetSummary

	// GeoMatchSet
	CreateGeoMatchSet(name, changeToken string) (*GeoMatchSet, error)
	GetGeoMatchSet(id string) (*GeoMatchSet, error)
	UpdateGeoMatchSet(id, changeToken string, updates []GeoMatchSetUpdate) error
	DeleteGeoMatchSet(id, changeToken string) error
	ListGeoMatchSets() []GeoMatchSetSummary

	// Tags
	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, keys []string) error
	ListTagsForResource(arn string) ([]Tag, error)

	// Sampled requests (stub)
	GetSampledRequests(webACLID, ruleID string, maxItems int64) []SampledHTTPRequest

	AccountID() string
	Region() string
	Reset()
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
