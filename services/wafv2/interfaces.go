package wafv2

// StorageBackend is the interface for WAFv2 storage operations.
type StorageBackend interface {
	AccountID() string
	Region() string
	WebACLARN(name, id, scope string) string
	IPSetARN(name, id, scope string) string
	RegexPatternSetARN(name, id, scope string) string
	RuleGroupARN(name, id, scope string) string
	CreateWebACL(
		name, scope, description, defaultAction, visibilityConfig string,
		tags map[string]string,
	) (*WebACL, error)
	GetWebACL(id string) (*WebACL, error)
	UpdateWebACL(id, description, defaultAction, visibilityConfig string) (*WebACL, error)
	DeleteWebACL(id string) error
	ListWebACLs() []*WebACL
	CreateIPSet(
		name, scope, description, ipAddressVersion string,
		addresses []string,
		tags map[string]string,
	) (*IPSet, error)
	GetIPSet(id string) (*IPSet, error)
	UpdateIPSet(id, description string, addresses []string) (*IPSet, error)
	DeleteIPSet(id string) error
	ListIPSets() []*IPSet
	TagResource(resourceARN string, tags map[string]string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
	UntagResource(resourceARN string, tagKeys []string) error
	Reset()
	AssociateWebACL(webACLARN, resourceARN string) error
	DisassociateWebACL(resourceARN string) error
	GetWebACLForResource(resourceARN string) (*WebACL, error)
	CheckCapacity(scope string, rules []map[string]any) (int64, error)
	CreateAPIKey(scope string, tokenDomains []string) (*APIKey, error)
	CreateRegexPatternSet(
		name, scope, description string,
		regularExpressionList []string,
		tags map[string]string,
	) (*RegexPatternSet, error)
	GetRegexPatternSet(id string) (*RegexPatternSet, error)
	ListRegexPatternSets() []*RegexPatternSet
	UpdateRegexPatternSet(id, description string, regularExpressionList []string) (*RegexPatternSet, error)
	CreateRuleGroup(
		name, scope, description, visibilityConfig string,
		capacity int64,
		rules []map[string]any,
		tags map[string]string,
	) (*RuleGroup, error)
	GetRuleGroup(id string) (*RuleGroup, error)
	ListRuleGroups() []*RuleGroup
	UpdateRuleGroup(id, description, visibilityConfig string, rules []map[string]any) (*RuleGroup, error)
	DeleteAPIKey(scope, apiKey string) error
	DeleteFirewallManagerRuleGroups(webACLARN string) (*WebACL, error)
	DeleteLoggingConfiguration(resourceARN string) error
	DeletePermissionPolicy(resourceARN string) error
	DeleteRegexPatternSet(id string) error
	ListAPIKeys(scope string) []*APIKey
	GetDecryptedAPIKey(scope, apiKey string) (*APIKey, error)
	GetLoggingConfiguration(resourceARN string) (bool, error)
	GetPermissionPolicy(resourceARN string) (string, error)
	ListResourcesForWebACL(webACLARN string) ([]string, error)
	PutLoggingConfiguration(resourceARN string) error
	PutPermissionPolicy(resourceARN, policy string) error
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
