package wafv2

import "encoding/json"

const (
	// ScopeRegional is the REGIONAL WAFv2 scope.
	ScopeRegional = "REGIONAL"
	// ScopeCloudFront is the CLOUDFRONT WAFv2 scope.
	ScopeCloudFront = "CLOUDFRONT"
	// IPVersionIPv4 is the IPV4 address version.
	IPVersionIPv4 = "IPV4"
	// IPVersionIPv6 is the IPV6 address version.
	IPVersionIPv6 = "IPV6"
	wcuPerRule    = int64(1)

	// maxIPSetEntries is the AWS-imposed cap on IP set entries.
	maxIPSetEntries = 10_000
	// maxRegexPatternSetEntries is the AWS-imposed cap on regex pattern set entries.
	maxRegexPatternSetEntries = 10
	// maxTagsPerResource is the AWS-imposed max number of tags per resource.
	maxTagsPerResource = 50
	// minRuleGroupCapacity is the minimum capacity for a rule group.
	minRuleGroupCapacity = int64(1)
	// maxRuleGroupCapacity is the maximum capacity for a rule group.
	maxRuleGroupCapacity = int64(1500)
	// minRateLimit is the minimum RateBasedStatement Limit.
	minRateLimit = int64(100)
	// maxRateLimit is the maximum RateBasedStatement Limit.
	maxRateLimit = int64(2_000_000_000)
	// maxTagKeyLen is the maximum length for a tag key.
	maxTagKeyLen = 128
	// maxTagValueLen is the maximum length for a tag value.
	maxTagValueLen = 256
	// maxDescriptionLen is the AWS-imposed max length for resource descriptions.
	maxDescriptionLen = 256
	// maxRulePriority is the maximum priority value for a rule (0–1000).
	maxRulePriority = int64(1000)
)

// RegexEntry represents a single regex pattern in AWS API shape.
type RegexEntry struct {
	RegexString string `json:"RegexString"`
}

// VisibilityConfig holds the parsed VisibilityConfig structure.
type VisibilityConfig struct {
	MetricName               string `json:"MetricName"`
	SampledRequestsEnabled   bool   `json:"SampledRequestsEnabled"`
	CloudWatchMetricsEnabled bool   `json:"CloudWatchMetricsEnabled"`
}

// WebACL represents an AWS WAFv2 Web ACL.
type WebACL struct {
	Tags                 map[string]string `json:"tags,omitempty"`
	ARN                  string            `json:"arn,omitempty"`
	DefaultAction        json.RawMessage   `json:"defaultAction,omitempty"`
	VisibilityConfig     json.RawMessage   `json:"visibilityConfig,omitempty"`
	CustomResponseBodies json.RawMessage   `json:"customResponseBodies,omitempty"`
	AssociationConfig    json.RawMessage   `json:"associationConfig,omitempty"`
	CaptchaConfig        json.RawMessage   `json:"captchaConfig,omitempty"`
	ChallengeConfig      json.RawMessage   `json:"challengeConfig,omitempty"`
	ID                   string            `json:"id"`
	Name                 string            `json:"name"`
	Scope                string            `json:"scope"`
	Description          string            `json:"description"`
	LockToken            string            `json:"lockToken"`
	TokenDomains         []string          `json:"tokenDomains,omitempty"`
	Rules                []map[string]any  `json:"rules,omitempty"`
}

// IPSet represents an AWS WAFv2 IP Set.
type IPSet struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ARN              string            `json:"arn,omitempty"`
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Scope            string            `json:"scope"`
	Description      string            `json:"description"`
	IPAddressVersion string            `json:"ipAddressVersion"`
	LockToken        string            `json:"lockToken"`
	Addresses        []string          `json:"addresses,omitempty"`
}

// RegexPatternSet represents an AWS WAFv2 Regex Pattern Set.
type RegexPatternSet struct {
	Tags                  map[string]string `json:"tags,omitempty"`
	ARN                   string            `json:"arn,omitempty"`
	ID                    string            `json:"id"`
	Name                  string            `json:"name"`
	Scope                 string            `json:"scope"`
	Description           string            `json:"description"`
	LockToken             string            `json:"lockToken"`
	RegularExpressionList []RegexEntry      `json:"regularExpressionList,omitempty"`
}

// RuleGroup represents an AWS WAFv2 Rule Group.
type RuleGroup struct {
	Tags                 map[string]string `json:"tags,omitempty"`
	ARN                  string            `json:"arn,omitempty"`
	ID                   string            `json:"id"`
	Name                 string            `json:"name"`
	Scope                string            `json:"scope"`
	Description          string            `json:"description"`
	VisibilityConfig     string            `json:"visibilityConfig"`
	CustomResponseBodies json.RawMessage   `json:"customResponseBodies,omitempty"`
	LockToken            string            `json:"lockToken"`
	Rules                []map[string]any  `json:"rules,omitempty"`
	Capacity             int64             `json:"capacity"`
}

// ManagedRuleSetVersion holds metadata for a single published version of a managed rule set.
type ManagedRuleSetVersion struct {
	ExpiryTimestamp        *int64 `json:"ExpiryTimestamp,omitempty"`
	ForecastedLifetime     *int64 `json:"ForecastedLifetime,omitempty"`
	LastUpdateTimestamp    *int64 `json:"LastUpdateTimestamp,omitempty"`
	PublishTimestamp       *int64 `json:"PublishTimestamp,omitempty"`
	AssociatedRuleGroupArn string `json:"AssociatedRuleGroupArn,omitempty"`
	Capacity               int64  `json:"Capacity,omitempty"`
}

// ManagedRuleSet represents an AWS WAFv2 managed rule set.
type ManagedRuleSet struct {
	PublishedVersions map[string]ManagedRuleSetVersion `json:"publishedVersions,omitempty"`
	ID                string                           `json:"id"`
	Name              string                           `json:"name"`
	Scope             string                           `json:"scope"`
	ARN               string                           `json:"arn,omitempty"`
	LockToken         string                           `json:"lockToken"`
	// Region is the request region this managed rule set was created/looked
	// up under (see getRegion). Unlike WebACL/IPSet/RegexPatternSet/RuleGroup,
	// ManagedRuleSet storage keys off the RAW request region rather than the
	// scope-normalized region baked into ARN, so it cannot be reliably
	// recovered from ARN alone -- this field is the store.Table[ManagedRuleSet]
	// key material that replaces the old map[region]map[id] nesting. Tagged
	// json:"-" because it is not part of the AWS-facing shape; it is
	// round-tripped through persistence.go's managedRuleSetSnapshot DTO
	// instead (see that file's doc comment).
	Region             string `json:"-"`
	RecommendedVersion string `json:"recommendedVersion,omitempty"`
}

// APIKey represents an AWS WAFv2 API key.
type APIKey struct {
	APIKeyValue string `json:"apiKey"`
	Scope       string `json:"scope"`
	// Region is the store.Table[APIKey] key material replacing the old
	// map[region]map[key]*APIKey nesting: the region bucket the key was
	// created under (storeRegion-normalized -- see CreateAPIKey). Tagged
	// json:"-" and round-tripped via persistence.go's apiKeySnapshot DTO,
	// mirroring ManagedRuleSet.Region.
	Region       string   `json:"-"`
	TokenDomains []string `json:"tokenDomains,omitempty"`
	// CreatedAt is the key's creation time (Unix epoch seconds), echoed as
	// APIKeySummary.CreationTimestamp / GetDecryptedAPIKeyOutput.CreationTimestamp
	// -- both real, always-populated members (deserializers.go's
	// smithytime.ParseEpochSeconds case for each).
	CreatedAt int64 `json:"createdAt"`
}
