package wafv2

import "encoding/json"

// StorageBackend is the interface for WAFv2 storage operations.
type StorageBackend interface {
	AccountID() string
	Region() string
	WebACLARN(name, id, scope string) string
	IPSetARN(name, id, scope string) string
	RegexPatternSetARN(name, id, scope string) string
	RuleGroupARN(name, id, scope string) string
	CreateWebACL(
		name, scope, description string,
		defaultAction, visibilityConfig json.RawMessage,
		rules []map[string]any,
		tokenDomains []string,
		customResponseBodies, associationConfig, captchaConfig, challengeConfig json.RawMessage,
		tags map[string]string,
	) (*WebACL, error)
	GetWebACL(id string) (*WebACL, error)
	UpdateWebACL(
		id, description, lockToken string,
		defaultAction, visibilityConfig json.RawMessage,
		rules []map[string]any,
		tokenDomains []string,
		customResponseBodies, associationConfig, captchaConfig, challengeConfig json.RawMessage,
	) (*WebACL, error)
	DeleteWebACL(id, lockToken string) error
	ListWebACLs() []*WebACL
	CreateIPSet(
		name, scope, description, ipAddressVersion string,
		addresses []string,
		tags map[string]string,
	) (*IPSet, error)
	GetIPSet(id string) (*IPSet, error)
	UpdateIPSet(id, description, lockToken string, addresses []string) (*IPSet, error)
	DeleteIPSet(id, lockToken string) error
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
		regularExpressionList []RegexEntry,
		tags map[string]string,
	) (*RegexPatternSet, error)
	GetRegexPatternSet(id string) (*RegexPatternSet, error)
	ListRegexPatternSets() []*RegexPatternSet
	UpdateRegexPatternSet(
		id, description, lockToken string,
		regularExpressionList []RegexEntry,
	) (*RegexPatternSet, error)
	CreateRuleGroup(
		name, scope, description, visibilityConfig string,
		capacity int64,
		rules []map[string]any,
		tags map[string]string,
	) (*RuleGroup, error)
	GetRuleGroup(id string) (*RuleGroup, error)
	ListRuleGroups() []*RuleGroup
	UpdateRuleGroup(id, description, visibilityConfig, lockToken string, rules []map[string]any) (*RuleGroup, error)
	DeleteRuleGroup(id, lockToken string) error
	DeleteAPIKey(scope, apiKey string) error
	DeleteFirewallManagerRuleGroups(webACLARN string) (*WebACL, error)
	PutLoggingConfiguration(resourceARN string, configJSON json.RawMessage) error
	DeleteLoggingConfiguration(resourceARN string) error
	GetLoggingConfiguration(resourceARN string) (json.RawMessage, error)
	ListLoggingConfigurations() []json.RawMessage
	DeletePermissionPolicy(resourceARN string) error
	DeleteRegexPatternSet(id, lockToken string) error
	ListAPIKeys(scope string) []*APIKey
	GetDecryptedAPIKey(scope, apiKey string) (*APIKey, error)
	GetPermissionPolicy(resourceARN string) (string, error)
	ListResourcesForWebACL(webACLARN string) ([]string, error)
	PutPermissionPolicy(resourceARN, policy string) error
	ManagedRuleSetARN(name, id, scope string) string
	GetManagedRuleSet(id string) (*ManagedRuleSet, error)
	ListManagedRuleSets(scope string) []*ManagedRuleSet
	PutManagedRuleSetVersions(
		id, name, scope, lockToken, recommendedVersion string,
		versionsToPublish map[string]any,
	) (*ManagedRuleSet, error)
	UpdateManagedRuleSetVersionExpiryDate(
		id, lockToken, versionToExpire string,
		expiryTimestamp *int64,
	) (*ManagedRuleSet, error)
	Snapshot() []byte
	Restore(data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
