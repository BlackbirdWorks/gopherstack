package waf

import "context"

// StorageBackend is the interface for WAF Classic storage operations.
type StorageBackend interface {
	// Change tokens
	GetChangeToken() string
	GetChangeTokenStatus(token string) string
	MarkChangeTokenUsed(token string)

	// WebACL
	CreateWebACL(
		name, metricName string,
		defaultAction WafAction,
		changeToken string,
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

	// RateBasedRule
	CreateRateBasedRule(
		name, metricName, rateKey string, rateLimit int64, changeToken string, tags map[string]string,
	) (*RateBasedRule, error)
	GetRateBasedRule(id string) (*RateBasedRule, error)
	UpdateRateBasedRule(id, changeToken string, rateLimit int64, updates []RuleUpdate) error
	DeleteRateBasedRule(id, changeToken string) error
	ListRateBasedRules() []RateBasedRuleSummary
	GetRateBasedRuleManagedKeys(id string) ([]string, error)

	// RegexPatternSet
	CreateRegexPatternSet(name, changeToken string) (*RegexPatternSet, error)
	GetRegexPatternSet(id string) (*RegexPatternSet, error)
	UpdateRegexPatternSet(id, changeToken string, updates []RegexPatternSetUpdate) error
	DeleteRegexPatternSet(id, changeToken string) error
	ListRegexPatternSets() []RegexPatternSetSummary

	// RegexMatchSet
	CreateRegexMatchSet(name, changeToken string) (*RegexMatchSet, error)
	GetRegexMatchSet(id string) (*RegexMatchSet, error)
	UpdateRegexMatchSet(id, changeToken string, updates []RegexMatchSetUpdate) error
	DeleteRegexMatchSet(id, changeToken string) error
	ListRegexMatchSets() []RegexMatchSetSummary

	// RuleGroup
	CreateRuleGroup(name, metricName, changeToken string, tags map[string]string) (*RuleGroup, error)
	GetRuleGroup(id string) (*RuleGroup, error)
	UpdateRuleGroup(id, changeToken string, updates []ActivatedRuleUpdate) error
	DeleteRuleGroup(id, changeToken string) error
	ListRuleGroups() []RuleGroupSummary
	ListActivatedRulesInRuleGroup(id string) ([]ActivatedRule, error)
	ListSubscribedRuleGroups() []SubscribedRuleGroupSummary

	// Logging
	PutLoggingConfiguration(config LoggingConfiguration) (*LoggingConfiguration, error)
	GetLoggingConfiguration(resourceArn string) (*LoggingConfiguration, error)
	DeleteLoggingConfiguration(resourceArn string) error
	ListLoggingConfigurations() []LoggingConfiguration

	// Permission policy
	PutPermissionPolicy(resourceArn, policy string) error
	GetPermissionPolicy(resourceArn string) (string, error)
	DeletePermissionPolicy(resourceArn string) error

	// Tags
	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, keys []string) error
	ListTagsForResource(arn string) ([]Tag, error)

	// Sampled requests (stub)
	GetSampledRequests(webACLID, ruleID string, maxItems int64) []SampledHTTPRequest

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
