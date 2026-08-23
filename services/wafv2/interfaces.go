package wafv2

import (
	"context"
	"encoding/json"
)

// StorageBackend is the interface for WAFv2 storage operations.
type StorageBackend interface {
	AccountID() string
	Region() string
	WebACLARN(name, id, scope string) string
	IPSetARN(name, id, scope string) string
	RegexPatternSetARN(name, id, scope string) string
	RuleGroupARN(name, id, scope string) string
	CreateWebACL(
		ctx context.Context,
		name, scope, description string,
		defaultAction, visibilityConfig json.RawMessage,
		rules []map[string]any,
		tokenDomains []string,
		customResponseBodies, associationConfig, captchaConfig, challengeConfig json.RawMessage,
		tags map[string]string,
	) (*WebACL, error)
	GetWebACL(ctx context.Context, id string) (*WebACL, error)
	GetWebACLByARN(ctx context.Context, webACLARN string) (*WebACL, error)
	UpdateWebACL(
		ctx context.Context,
		id, description, lockToken string,
		defaultAction, visibilityConfig json.RawMessage,
		rules []map[string]any,
		tokenDomains []string,
		customResponseBodies, associationConfig, captchaConfig, challengeConfig json.RawMessage,
	) (*WebACL, error)
	DeleteWebACL(ctx context.Context, id, lockToken string) error
	ListWebACLs(ctx context.Context) []*WebACL
	CreateIPSet(
		ctx context.Context,
		name, scope, description, ipAddressVersion string,
		addresses []string,
		tags map[string]string,
	) (*IPSet, error)
	GetIPSet(ctx context.Context, id string) (*IPSet, error)
	UpdateIPSet(ctx context.Context, id, description, lockToken string, addresses []string) (*IPSet, error)
	DeleteIPSet(ctx context.Context, id, lockToken string) error
	ListIPSets(ctx context.Context) []*IPSet
	TagResource(ctx context.Context, resourceARN string, tags map[string]string) error
	ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error)
	UntagResource(ctx context.Context, resourceARN string, tagKeys []string) error
	Reset()
	AssociateWebACL(ctx context.Context, webACLARN, resourceARN string) error
	DisassociateWebACL(ctx context.Context, resourceARN string) error
	GetWebACLForResource(ctx context.Context, resourceARN string) (*WebACL, error)
	CheckCapacity(ctx context.Context, scope string, rules []map[string]any) (int64, error)
	CreateAPIKey(ctx context.Context, scope string, tokenDomains []string) (*APIKey, error)
	CreateRegexPatternSet(
		ctx context.Context,
		name, scope, description string,
		regularExpressionList []RegexEntry,
		tags map[string]string,
	) (*RegexPatternSet, error)
	GetRegexPatternSet(ctx context.Context, id string) (*RegexPatternSet, error)
	ListRegexPatternSets(ctx context.Context) []*RegexPatternSet
	UpdateRegexPatternSet(
		ctx context.Context,
		id, description, lockToken string,
		regularExpressionList []RegexEntry,
	) (*RegexPatternSet, error)
	CreateRuleGroup(
		ctx context.Context,
		name, scope, description, visibilityConfig string,
		capacity int64,
		rules []map[string]any,
		customResponseBodies json.RawMessage,
		tags map[string]string,
	) (*RuleGroup, error)
	GetRuleGroup(ctx context.Context, id string) (*RuleGroup, error)
	GetRuleGroupByARN(ctx context.Context, ruleGroupARN string) (*RuleGroup, error)
	ListRuleGroups(ctx context.Context) []*RuleGroup
	UpdateRuleGroup(
		ctx context.Context,
		id, description, visibilityConfig, lockToken string,
		rules []map[string]any,
		customResponseBodies json.RawMessage,
	) (*RuleGroup, error)
	DeleteRuleGroup(ctx context.Context, id, lockToken string) error
	DeleteAPIKey(ctx context.Context, scope, apiKey string) error
	DeleteFirewallManagerRuleGroups(ctx context.Context, webACLARN string) (*WebACL, error)
	PutLoggingConfiguration(ctx context.Context, resourceARN string, configJSON json.RawMessage) error
	DeleteLoggingConfiguration(ctx context.Context, resourceARN string) error
	GetLoggingConfiguration(ctx context.Context, resourceARN string) (json.RawMessage, error)
	ListLoggingConfigurations(ctx context.Context, scope string) []json.RawMessage
	DeletePermissionPolicy(ctx context.Context, resourceARN string) error
	DeleteRegexPatternSet(ctx context.Context, id, lockToken string) error
	ListAPIKeys(ctx context.Context, scope string) []*APIKey
	GetDecryptedAPIKey(ctx context.Context, scope, apiKey string) (*APIKey, error)
	GetPermissionPolicy(ctx context.Context, resourceARN string) (string, error)
	ListResourcesForWebACL(ctx context.Context, webACLARN string) ([]string, error)
	PutPermissionPolicy(ctx context.Context, resourceARN, policy string) error
	ManagedRuleSetARN(name, id, scope string) string
	GetManagedRuleSet(ctx context.Context, id string) (*ManagedRuleSet, error)
	ListManagedRuleSets(ctx context.Context, scope string) []*ManagedRuleSet
	PutManagedRuleSetVersions(
		ctx context.Context,
		id, name, scope, lockToken, recommendedVersion string,
		versionsToPublish map[string]any,
	) (*ManagedRuleSet, error)
	UpdateManagedRuleSetVersionExpiryDate(
		ctx context.Context,
		id, lockToken, versionToExpire string,
		expiryTimestamp *int64,
	) (*ManagedRuleSet, error)
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

var _ StorageBackend = (*InMemoryBackend)(nil)
