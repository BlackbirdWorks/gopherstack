package wafv2

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/netip"
	"regexp"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrWebACLNotFound is returned when a WebACL does not exist.
	ErrWebACLNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrWebACLAlreadyExists is returned when a WebACL with the same name already exists.
	ErrWebACLAlreadyExists = awserr.New("WAFDuplicateItemException", awserr.ErrConflict)
	// ErrIPSetNotFound is returned when an IPSet does not exist.
	ErrIPSetNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrIPSetAlreadyExists is returned when an IPSet with the same name already exists.
	ErrIPSetAlreadyExists = awserr.New("WAFDuplicateItemException", awserr.ErrConflict)
	// ErrAssociationNotFound is returned when a WebACL association does not exist.
	ErrAssociationNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrRegexPatternSetNotFound is returned when a RegexPatternSet does not exist.
	ErrRegexPatternSetNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrRegexPatternSetAlreadyExists is returned when a RegexPatternSet with the same name already exists.
	ErrRegexPatternSetAlreadyExists = awserr.New("WAFDuplicateItemException", awserr.ErrConflict)
	// ErrRuleGroupNotFound is returned when a RuleGroup does not exist.
	ErrRuleGroupNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrRuleGroupAlreadyExists is returned when a RuleGroup with the same name already exists.
	ErrRuleGroupAlreadyExists = awserr.New("WAFDuplicateItemException", awserr.ErrConflict)
	// ErrAPIKeyNotFound is returned when an API key does not exist.
	ErrAPIKeyNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrLoggingConfigNotFound is returned when a logging configuration does not exist.
	ErrLoggingConfigNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrPermissionPolicyNotFound is returned when a permission policy does not exist.
	ErrPermissionPolicyNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrOptimisticLock is returned when the LockToken does not match.
	ErrOptimisticLock = awserr.New("WAFOptimisticLockException", awserr.ErrConflict)
	// ErrAssociatedItem is returned when a resource is referenced by another resource.
	ErrAssociatedItem = awserr.New("WAFAssociatedItemException", awserr.ErrConflict)
	// ErrLimitsExceeded is returned when a resource limit is exceeded.
	ErrLimitsExceeded = awserr.New("WAFLimitsExceededException", awserr.ErrConflict)
	// ErrInvalidOperation is returned when an operation is invalid.
	ErrInvalidOperation = awserr.New("WAFInvalidOperationException", awserr.ErrInvalidParameter)
	// ErrUnavailableEntity is returned when a resource is temporarily unavailable.
	ErrUnavailableEntity = awserr.New("WAFUnavailableEntityException", awserr.ErrConflict)
	// ErrTagOperation is returned when a tag operation fails validation.
	ErrTagOperation = awserr.New("WAFTagOperationException", awserr.ErrInvalidParameter)
	// ErrConfigurationWarning is returned when there is a configuration warning.
	ErrConfigurationWarning = awserr.New("WAFConfigurationWarningException", awserr.ErrInvalidParameter)
	// ErrManagedRuleSetNotFound is returned when a managed rule set does not exist.
	ErrManagedRuleSetNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrMobileSdkReleaseNotFound is returned when a mobile SDK release is not in the catalog.
	ErrMobileSdkReleaseNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
)

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

// validEvaluationWindowSecs contains the allowed EvaluationWindowSec values.
var validEvaluationWindowSecs = map[int64]bool{ //nolint:gochecknoglobals // package-level lookup table
	60: true, 120: true, 300: true, 600: true,
	1800: true, 3600: true, 7200: true, 21600: true,
}

// validResourceNameRe is the pattern AWS requires for WAFv2 resource names.
var validResourceNameRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9\-_]{0,127}$`)

// validateResourceName checks that a WAFv2 resource name conforms to the AWS
// allowed pattern: starts with alphanumeric, followed by alphanumeric, hyphen,
// or underscore, up to 128 characters total.
func validateResourceName(name string) error {
	if !validResourceNameRe.MatchString(name) {
		return fmt.Errorf(
			"%w: Name %q must match ^[a-zA-Z0-9][a-zA-Z0-9-_]{0,127}$",
			errInvalidRequest,
			name,
		)
	}

	return nil
}

// validateDescription checks that a resource description does not exceed the
// AWS-imposed maximum of 256 characters.
func validateDescription(description string) error {
	if len(description) > maxDescriptionLen {
		return fmt.Errorf(
			"%w: Description must be at most %d characters, got %d",
			errInvalidRequest,
			maxDescriptionLen,
			len(description),
		)
	}

	return nil
}

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
	ID                    string            `json:"id"`
	Name                  string            `json:"name"`
	Scope                 string            `json:"scope"`
	Description           string            `json:"description"`
	LockToken             string            `json:"lockToken"`
	RegularExpressionList []RegexEntry      `json:"regularExpressionList,omitempty"`
}

// RuleGroup represents an AWS WAFv2 Rule Group.
type RuleGroup struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Scope            string            `json:"scope"`
	Description      string            `json:"description"`
	VisibilityConfig string            `json:"visibilityConfig"`
	LockToken        string            `json:"lockToken"`
	Rules            []map[string]any  `json:"rules,omitempty"`
	Capacity         int64             `json:"capacity"`
}

// ManagedRuleSetVersion holds metadata for a single published version of a managed rule set.
type ManagedRuleSetVersion struct {
	AssociatedRuleGroupArn string `json:"AssociatedRuleGroupArn,omitempty"`
	ExpiryTimestamp        *int64 `json:"ExpiryTimestamp,omitempty"`
	ForecastedLifetime     *int64 `json:"ForecastedLifetime,omitempty"`
	LastUpdateTimestamp    *int64 `json:"LastUpdateTimestamp,omitempty"`
	PublishTimestamp       *int64 `json:"PublishTimestamp,omitempty"`
	Capacity               int64  `json:"Capacity,omitempty"`
}

// ManagedRuleSet represents an AWS WAFv2 managed rule set.
type ManagedRuleSet struct {
	PublishedVersions  map[string]ManagedRuleSetVersion `json:"publishedVersions,omitempty"`
	ID                 string                           `json:"id"`
	Name               string                           `json:"name"`
	Scope              string                           `json:"scope"`
	ARN                string                           `json:"arn,omitempty"`
	LockToken          string                           `json:"lockToken"`
	RecommendedVersion string                           `json:"recommendedVersion,omitempty"`
}

// APIKey represents an AWS WAFv2 API key.
type APIKey struct {
	APIKeyValue  string   `json:"apiKey"`
	Scope        string   `json:"scope"`
	TokenDomains []string `json:"tokenDomains,omitempty"`
}

// InMemoryBackend is an in-memory store for WAFv2 resources.
type InMemoryBackend struct {
	webACLs                map[string]*WebACL
	ipSets                 map[string]*IPSet
	regexPatternSets       map[string]*RegexPatternSet
	ruleGroups             map[string]*RuleGroup
	managedRuleSets        map[string]*ManagedRuleSet // id → ManagedRuleSet
	apiKeys                map[string]*APIKey         // key: scope+":"+apiKeyValue
	loggingConfigs         map[string]json.RawMessage // resourceARN → full config JSON
	permissionPolicies     map[string]string          // resourceARN → policy JSON
	webACLByARN            map[string]string          // ARN → webACL ID
	ipSetByARN             map[string]string          // ARN → ipSet ID
	regexPatternSetByARN   map[string]string          // ARN → regexPatternSet ID
	ruleGroupByARN         map[string]string          // ARN → ruleGroup ID
	webACLByNameScope      map[string]string          // "name:scope" → webACL ID (O(1) duplicate check)
	ipSetByNameScope       map[string]string          // "name:scope" → ipSet ID (O(1) duplicate check)
	regexPatternSetByScope map[string]string          // "name:scope" → regexPatternSet ID
	ruleGroupByNameScope   map[string]string          // "name:scope" → ruleGroup ID
	associations           map[string]string          // resourceARN → webACL ID (AssociateWebACL)
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
}

// NewInMemoryBackend creates a new in-memory WAFv2 backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		webACLs:                make(map[string]*WebACL),
		ipSets:                 make(map[string]*IPSet),
		regexPatternSets:       make(map[string]*RegexPatternSet),
		ruleGroups:             make(map[string]*RuleGroup),
		managedRuleSets:        make(map[string]*ManagedRuleSet),
		apiKeys:                make(map[string]*APIKey),
		loggingConfigs:         make(map[string]json.RawMessage),
		permissionPolicies:     make(map[string]string),
		webACLByARN:            make(map[string]string),
		ipSetByARN:             make(map[string]string),
		regexPatternSetByARN:   make(map[string]string),
		ruleGroupByARN:         make(map[string]string),
		webACLByNameScope:      make(map[string]string),
		ipSetByNameScope:       make(map[string]string),
		regexPatternSetByScope: make(map[string]string),
		ruleGroupByNameScope:   make(map[string]string),
		associations:           make(map[string]string),
		accountID:              accountID,
		region:                 region,
		mu:                     lockmetrics.New("wafv2"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// validScope reports whether scope is a recognised WAFv2 scope.
func validScope(scope string) bool {
	return scope == ScopeRegional || scope == ScopeCloudFront
}

// arnRegion returns the correct region segment for a WAFv2 ARN. CLOUDFRONT
// (global) resources use an empty region, matching the real AWS ARN format:
// arn:aws:wafv2::123456789012:global/webacl/...
func (b *InMemoryBackend) arnRegion(scope string) string {
	if scope == ScopeCloudFront {
		return ""
	}

	return b.region
}

// WebACLARN builds an ARN for a WebACL.
func (b *InMemoryBackend) WebACLARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.arnRegion(scope), b.accountID, prefix+"/webacl/"+name+"/"+id)
}

// IPSetARN builds a public ARN for an IPSet.
func (b *InMemoryBackend) IPSetARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.arnRegion(scope), b.accountID, prefix+"/ipset/"+name+"/"+id)
}

// RegexPatternSetARN builds an ARN for a RegexPatternSet.
func (b *InMemoryBackend) RegexPatternSetARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.arnRegion(scope), b.accountID, prefix+"/regexpatternset/"+name+"/"+id)
}

// RuleGroupARN builds an ARN for a RuleGroup.
func (b *InMemoryBackend) RuleGroupARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.arnRegion(scope), b.accountID, prefix+"/rulegroup/"+name+"/"+id)
}

func apiKeyMapKey(scope, apiKey string) string {
	return scope + ":" + apiKey
}

func scopePrefix(scope string) string {
	if scope == "CLOUDFRONT" {
		return "global"
	}

	return "regional"
}

func nameScope(name, scope string) string {
	return name + ":" + scope
}

// validateTags checks that tags conform to AWS constraints:
// - Keys: 1–128 chars, cannot start with "aws:"
// - Values: 0–256 chars
// - Max 50 tags per resource.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagsPerResource {
		return fmt.Errorf(
			"%w: too many tags: %d (max %d)",
			ErrTagOperation,
			len(tags),
			maxTagsPerResource,
		)
	}

	for k, v := range tags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key %q must be 1–%d characters", ErrTagOperation, k, maxTagKeyLen)
		}

		if strings.HasPrefix(k, "aws:") {
			return fmt.Errorf("%w: tag key %q uses reserved prefix aws", ErrTagOperation, k)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value for key %q must be 0–%d characters", ErrTagOperation, k, maxTagValueLen)
		}
	}

	return nil
}

// validateVisibilityConfig parses and validates a VisibilityConfig JSON blob.
func validateVisibilityConfig(raw json.RawMessage) error {
	if len(raw) == 0 {
		return nil
	}

	var vc VisibilityConfig
	if err := json.Unmarshal(raw, &vc); err != nil {
		return fmt.Errorf("%w: invalid VisibilityConfig: %w", errInvalidRequest, err)
	}

	if vc.MetricName == "" {
		return fmt.Errorf("%w: VisibilityConfig.MetricName is required", errInvalidRequest)
	}

	return nil
}

// maxStatementNestingDepth is the maximum allowed nesting depth for rule statements.
const maxStatementNestingDepth = 3

// nestedStatementKeys are statement wrapper keys that may contain nested statements.
var nestedStatementKeys = []string{ //nolint:gochecknoglobals // package-level lookup table
	"AndStatement", "OrStatement", "NotStatement",
	"ManagedRuleGroupStatement", "RuleGroupReferenceStatement",
}

// validateStatement performs basic structural validation on a rule statement map.
// It handles RateBasedStatement and RegexPatternReferenceStatement recursively (depth-limited).
func validateStatement(stmt map[string]any, depth int) error { //nolint:gocognit
	if depth > maxStatementNestingDepth {
		return fmt.Errorf(
			"%w: statement nesting exceeds maximum depth of %d",
			errInvalidRequest,
			maxStatementNestingDepth,
		)
	}

	if rbs, isRBS := stmt["RateBasedStatement"].(map[string]any); isRBS {
		return validateRateBasedStatement(rbs)
	}

	// Recurse into nested statement wrappers.
	for _, key := range nestedStatementKeys {
		nested, isNested := stmt[key].(map[string]any)
		if !isNested {
			continue
		}

		if inner, hasInner := nested["Statement"].(map[string]any); hasInner {
			if err := validateStatement(inner, depth+1); err != nil {
				return err
			}
		}

		// AndStatement and OrStatement have Statements (plural).
		stmts, hasStmts := nested["Statements"].([]any)
		if hasStmts {
			for _, s := range stmts {
				sm, isSM := s.(map[string]any)
				if isSM {
					if err := validateStatement(sm, depth+1); err != nil {
						return err
					}
				}
			}
		}
	}

	// Validate regex patterns if present.
	rps, hasRPS := stmt["RegexPatternSetReferenceStatement"].(map[string]any)
	if hasRPS {
		pattern, hasPattern := rps["RegexString"].(string)
		if hasPattern {
			if _, err := regexp.Compile(pattern); err != nil {
				return fmt.Errorf("%w: invalid regex pattern %q: %w", errInvalidRequest, pattern, err)
			}
		}
	}

	return nil
}

func validateRateBasedStatement(rbs map[string]any) error {
	limit, _ := toInt64(rbs["Limit"])
	if limit < minRateLimit || limit > maxRateLimit {
		return fmt.Errorf(
			"%w: RateBasedStatement.Limit must be between %d and %d",
			errInvalidRequest,
			minRateLimit,
			maxRateLimit,
		)
	}

	if ewsRaw, ok := rbs["EvaluationWindowSec"]; ok {
		ews, _ := toInt64(ewsRaw)
		if !validEvaluationWindowSecs[ews] {
			return fmt.Errorf(
				"%w: RateBasedStatement.EvaluationWindowSec must be one of {60,120,300,600,1800,3600,7200,21600}",
				errInvalidRequest,
			)
		}
	}

	return nil
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int:
		return int64(n), true
	case float64:
		return int64(n), true
	case json.Number:
		i, err := n.Int64()

		return i, err == nil
	}

	return 0, false
}

// validateRules validates a slice of rules for a WebACL or RuleGroup.
func validateRules(rules []map[string]any) error {
	priorities := make(map[int64]bool)
	names := make(map[string]bool)

	for _, rule := range rules {
		if err := validateSingleRule(rule, priorities, names); err != nil {
			return err
		}
	}

	return nil
}

// validateSingleRule validates a single rule map and updates the seen priority
// and name sets. Extracted to keep validateRules below the cognitive complexity
// threshold.
func validateSingleRule(rule map[string]any, priorities map[int64]bool, names map[string]bool) error {
	name, _ := rule["Name"].(string)
	if name == "" {
		return fmt.Errorf("%w: rule Name is required", errInvalidRequest)
	}

	if names[name] {
		return fmt.Errorf("%w: duplicate rule Name %q in rules", errInvalidRequest, name)
	}

	names[name] = true

	priorityRaw, hasPriority := rule["Priority"]
	if !hasPriority {
		return fmt.Errorf("%w: rule %q is missing Priority", errInvalidRequest, name)
	}

	priority, ok := toInt64(priorityRaw)
	if !ok {
		return fmt.Errorf("%w: rule %q Priority must be an integer", errInvalidRequest, name)
	}

	if priority < 0 || priority > maxRulePriority {
		return fmt.Errorf(
			"%w: rule %q Priority must be between 0 and %d, got %d",
			errInvalidRequest,
			name,
			maxRulePriority,
			priority,
		)
	}

	if priorities[priority] {
		return fmt.Errorf("%w: duplicate Priority %d in rules", errInvalidRequest, priority)
	}

	priorities[priority] = true

	if _, hasStatement := rule["Statement"]; !hasStatement {
		return fmt.Errorf("%w: rule %q is missing Statement", errInvalidRequest, name)
	}

	if stmt, isMap := rule["Statement"].(map[string]any); isMap {
		if err := validateStatement(stmt, 0); err != nil {
			return err
		}
	}

	if _, hasVC := rule["VisibilityConfig"]; !hasVC {
		return fmt.Errorf("%w: rule %q is missing VisibilityConfig", errInvalidRequest, name)
	}

	return nil
}

// validateCIDRs validates a list of CIDRs against the given IP version.
func validateCIDRs(addresses []string, ipVersion string) error {
	if len(addresses) > maxIPSetEntries {
		return fmt.Errorf(
			"%w: IP set exceeds maximum of %d addresses",
			ErrLimitsExceeded,
			maxIPSetEntries,
		)
	}

	for _, addr := range addresses {
		prefix, err := netip.ParsePrefix(addr)
		if err != nil {
			return fmt.Errorf("%w: invalid CIDR %q: %s", errInvalidRequest, addr, err.Error())
		}

		if ipVersion == IPVersionIPv4 && !prefix.Addr().Is4() {
			return fmt.Errorf(
				"%w: CIDR %q is not a valid IPv4 address for IPV4 set",
				errInvalidRequest,
				addr,
			)
		}

		if ipVersion == IPVersionIPv6 && !prefix.Addr().Is6() {
			return fmt.Errorf(
				"%w: CIDR %q is not a valid IPv6 address for IPV6 set",
				errInvalidRequest,
				addr,
			)
		}
	}

	return nil
}

// validateRegexEntries validates a list of RegexEntry objects.
func validateRegexEntries(entries []RegexEntry) error {
	if len(entries) > maxRegexPatternSetEntries {
		return fmt.Errorf(
			"%w: regex pattern set exceeds maximum of %d entries",
			ErrLimitsExceeded,
			maxRegexPatternSetEntries,
		)
	}

	for _, entry := range entries {
		if _, err := regexp.Compile(entry.RegexString); err != nil {
			return fmt.Errorf(
				"%w: invalid regex %q: %s",
				errInvalidRequest,
				entry.RegexString,
				err.Error(),
			)
		}
	}

	return nil
}

// CreateWebACL creates a new WebACL.
func (b *InMemoryBackend) CreateWebACL(
	name, scope, description string,
	defaultAction, visibilityConfig json.RawMessage,
	rules []map[string]any,
	tokenDomains []string,
	customResponseBodies, associationConfig, captchaConfig, challengeConfig json.RawMessage,
	tags map[string]string,
) (*WebACL, error) {
	b.mu.Lock("CreateWebACL")
	defer b.mu.Unlock()

	if _, exists := b.webACLByNameScope[nameScope(name, scope)]; exists {
		return nil, fmt.Errorf("%w: web ACL %q already exists in scope %s", ErrWebACLAlreadyExists, name, scope)
	}

	id := uuid.NewString()
	w := &WebACL{
		ID:                   id,
		Name:                 name,
		Scope:                scope,
		Description:          description,
		DefaultAction:        defaultAction,
		VisibilityConfig:     visibilityConfig,
		Rules:                cloneRules(rules),
		TokenDomains:         cloneAddresses(tokenDomains),
		CustomResponseBodies: customResponseBodies,
		AssociationConfig:    associationConfig,
		CaptchaConfig:        captchaConfig,
		ChallengeConfig:      challengeConfig,
		LockToken:            uuid.NewString(),
		Tags:                 cloneTags(tags),
	}
	b.webACLs[id] = w
	b.webACLByARN[b.WebACLARN(name, id, scope)] = id
	b.webACLByNameScope[nameScope(name, scope)] = id

	return cloneWebACL(w), nil
}

func (b *InMemoryBackend) GetWebACL(id string) (*WebACL, error) {
	b.mu.RLock("GetWebACL")
	defer b.mu.RUnlock()

	w, ok := b.webACLs[id]
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, id)
	}

	return cloneWebACL(w), nil
}

// UpdateWebACL updates a WebACL by ID.
func (b *InMemoryBackend) UpdateWebACL(
	id, description, lockToken string,
	defaultAction, visibilityConfig json.RawMessage,
	rules []map[string]any,
	tokenDomains []string,
	customResponseBodies, associationConfig, captchaConfig, challengeConfig json.RawMessage,
) (*WebACL, error) {
	b.mu.Lock("UpdateWebACL")
	defer b.mu.Unlock()

	w, ok := b.webACLs[id]
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, id)
	}

	if lockToken != "" && lockToken != w.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for web ACL %q", ErrOptimisticLock, id)
	}

	if description != "" {
		w.Description = description
	}

	if len(defaultAction) > 0 {
		w.DefaultAction = defaultAction
	}

	if len(visibilityConfig) > 0 {
		w.VisibilityConfig = visibilityConfig
	}

	if rules != nil {
		w.Rules = cloneRules(rules)
	}

	if tokenDomains != nil {
		w.TokenDomains = cloneAddresses(tokenDomains)
	}

	if len(customResponseBodies) > 0 {
		w.CustomResponseBodies = customResponseBodies
	}

	if len(associationConfig) > 0 {
		w.AssociationConfig = associationConfig
	}

	if len(captchaConfig) > 0 {
		w.CaptchaConfig = captchaConfig
	}

	if len(challengeConfig) > 0 {
		w.ChallengeConfig = challengeConfig
	}

	w.LockToken = uuid.NewString()

	return cloneWebACL(w), nil
}

// DeleteWebACL deletes a WebACL by ID.
func (b *InMemoryBackend) DeleteWebACL(id, lockToken string) error {
	b.mu.Lock("DeleteWebACL")
	defer b.mu.Unlock()

	w, ok := b.webACLs[id]
	if !ok {
		return fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, id)
	}

	if lockToken != "" && lockToken != w.LockToken {
		return fmt.Errorf("%w: lock token mismatch for web ACL %q", ErrOptimisticLock, id)
	}

	// AWS returns WAFAssociatedItemException when the WebACL is still associated
	// with a resource (e.g. an ALB or API Gateway stage).
	for _, assocID := range b.associations {
		if assocID == id {
			return fmt.Errorf(
				"%w: web ACL %q is still associated with a resource; disassociate first",
				ErrAssociatedItem,
				id,
			)
		}
	}

	webACLArnStr := b.WebACLARN(w.Name, w.ID, w.Scope)

	delete(b.webACLByARN, webACLArnStr)
	delete(b.webACLByNameScope, nameScope(w.Name, w.Scope))
	delete(b.webACLs, id)

	// Cascade: remove the WebACL's own logging config and permission policy.
	delete(b.loggingConfigs, webACLArnStr)
	delete(b.permissionPolicies, webACLArnStr)

	return nil
}

// ListWebACLs returns all WebACLs sorted by name.
func (b *InMemoryBackend) ListWebACLs() []*WebACL {
	b.mu.RLock("ListWebACLs")
	defer b.mu.RUnlock()

	list := make([]*WebACL, 0, len(b.webACLs))

	for _, w := range b.webACLs {
		list = append(list, cloneWebACL(w))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list
}

// CreateIPSet creates a new IPSet.
func (b *InMemoryBackend) CreateIPSet(
	name, scope, description, ipAddressVersion string,
	addresses []string,
	tags map[string]string,
) (*IPSet, error) {
	b.mu.Lock("CreateIPSet")
	defer b.mu.Unlock()

	if _, exists := b.ipSetByNameScope[nameScope(name, scope)]; exists {
		return nil, fmt.Errorf("%w: IP set %q already exists in scope %s", ErrIPSetAlreadyExists, name, scope)
	}

	id := uuid.NewString()
	s := &IPSet{
		ID:               id,
		Name:             name,
		Scope:            scope,
		Description:      description,
		IPAddressVersion: ipAddressVersion,
		Addresses:        cloneAddresses(addresses),
		LockToken:        uuid.NewString(),
		Tags:             cloneTags(tags),
	}
	b.ipSets[id] = s
	b.ipSetByARN[b.IPSetARN(name, id, scope)] = id
	b.ipSetByNameScope[nameScope(name, scope)] = id

	return cloneIPSet(s), nil
}

// GetIPSet returns an IPSet by ID.
func (b *InMemoryBackend) GetIPSet(id string) (*IPSet, error) {
	b.mu.RLock("GetIPSet")
	defer b.mu.RUnlock()

	s, ok := b.ipSets[id]
	if !ok {
		return nil, fmt.Errorf("%w: IP set %q not found", ErrIPSetNotFound, id)
	}

	return cloneIPSet(s), nil
}

// UpdateIPSet updates an IPSet by ID.
func (b *InMemoryBackend) UpdateIPSet(id, description, lockToken string, addresses []string) (*IPSet, error) {
	b.mu.Lock("UpdateIPSet")
	defer b.mu.Unlock()

	s, ok := b.ipSets[id]
	if !ok {
		return nil, fmt.Errorf("%w: IP set %q not found", ErrIPSetNotFound, id)
	}

	if lockToken != "" && lockToken != s.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for IP set %q", ErrOptimisticLock, id)
	}

	if description != "" {
		s.Description = description
	}

	if addresses != nil {
		s.Addresses = cloneAddresses(addresses)
	}

	s.LockToken = uuid.NewString()

	return cloneIPSet(s), nil
}

// DeleteIPSet deletes an IPSet by ID.
func (b *InMemoryBackend) DeleteIPSet(id, lockToken string) error {
	b.mu.Lock("DeleteIPSet")
	defer b.mu.Unlock()

	s, ok := b.ipSets[id]
	if !ok {
		return fmt.Errorf("%w: IP set %q not found", ErrIPSetNotFound, id)
	}

	if lockToken != "" && lockToken != s.LockToken {
		return fmt.Errorf("%w: lock token mismatch for IP set %q", ErrOptimisticLock, id)
	}

	delete(b.ipSetByARN, b.IPSetARN(s.Name, s.ID, s.Scope))
	delete(b.ipSetByNameScope, nameScope(s.Name, s.Scope))
	delete(b.ipSets, id)

	return nil
}

// ListIPSets returns all IPSets sorted by name.
func (b *InMemoryBackend) ListIPSets() []*IPSet {
	b.mu.RLock("ListIPSets")
	defer b.mu.RUnlock()

	list := make([]*IPSet, 0, len(b.ipSets))

	for _, s := range b.ipSets {
		list = append(list, cloneIPSet(s))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list
}

// TagResource adds tags to a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if id, ok := b.webACLByARN[resourceARN]; ok {
		w := b.webACLs[id]
		if w.Tags == nil {
			w.Tags = make(map[string]string)
		}

		maps.Copy(w.Tags, tags)

		return nil
	}

	if id, ok := b.ipSetByARN[resourceARN]; ok {
		s := b.ipSets[id]
		if s.Tags == nil {
			s.Tags = make(map[string]string)
		}

		maps.Copy(s.Tags, tags)

		return nil
	}

	if id, ok := b.regexPatternSetByARN[resourceARN]; ok {
		r := b.regexPatternSets[id]
		if r.Tags == nil {
			r.Tags = make(map[string]string)
		}

		maps.Copy(r.Tags, tags)

		return nil
	}

	if id, ok := b.ruleGroupByARN[resourceARN]; ok {
		rg := b.ruleGroups[id]
		if rg.Tags == nil {
			rg.Tags = make(map[string]string)
		}

		maps.Copy(rg.Tags, tags)

		return nil
	}

	return fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
}

// ListTagsForResource returns the tags for a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if id, ok := b.webACLByARN[resourceARN]; ok {
		return maps.Clone(b.webACLs[id].Tags), nil
	}

	if id, ok := b.ipSetByARN[resourceARN]; ok {
		return maps.Clone(b.ipSets[id].Tags), nil
	}

	if id, ok := b.regexPatternSetByARN[resourceARN]; ok {
		return maps.Clone(b.regexPatternSets[id].Tags), nil
	}

	if id, ok := b.ruleGroupByARN[resourceARN]; ok {
		return maps.Clone(b.ruleGroups[id].Tags), nil
	}

	return nil, fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
}

// UntagResource removes tags from a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if id, ok := b.webACLByARN[resourceARN]; ok {
		w := b.webACLs[id]
		for _, k := range tagKeys {
			delete(w.Tags, k)
		}

		return nil
	}

	if id, ok := b.ipSetByARN[resourceARN]; ok {
		s := b.ipSets[id]
		for _, k := range tagKeys {
			delete(s.Tags, k)
		}

		return nil
	}

	if id, ok := b.regexPatternSetByARN[resourceARN]; ok {
		r := b.regexPatternSets[id]
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	}

	if id, ok := b.ruleGroupByARN[resourceARN]; ok {
		rg := b.ruleGroups[id]
		for _, k := range tagKeys {
			delete(rg.Tags, k)
		}

		return nil
	}

	return fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
}

func cloneWebACL(w *WebACL) *WebACL {
	cp := *w
	cp.Tags = maps.Clone(w.Tags)
	cp.Rules = cloneRules(w.Rules)
	cp.TokenDomains = cloneAddresses(w.TokenDomains)

	// Clone RawMessage fields (byte slices).
	if w.DefaultAction != nil {
		da := make(json.RawMessage, len(w.DefaultAction))
		copy(da, w.DefaultAction)
		cp.DefaultAction = da
	}

	if w.VisibilityConfig != nil {
		vc := make(json.RawMessage, len(w.VisibilityConfig))
		copy(vc, w.VisibilityConfig)
		cp.VisibilityConfig = vc
	}

	if w.CustomResponseBodies != nil {
		crb := make(json.RawMessage, len(w.CustomResponseBodies))
		copy(crb, w.CustomResponseBodies)
		cp.CustomResponseBodies = crb
	}

	if w.AssociationConfig != nil {
		ac := make(json.RawMessage, len(w.AssociationConfig))
		copy(ac, w.AssociationConfig)
		cp.AssociationConfig = ac
	}

	if w.CaptchaConfig != nil {
		cc := make(json.RawMessage, len(w.CaptchaConfig))
		copy(cc, w.CaptchaConfig)
		cp.CaptchaConfig = cc
	}

	if w.ChallengeConfig != nil {
		chc := make(json.RawMessage, len(w.ChallengeConfig))
		copy(chc, w.ChallengeConfig)
		cp.ChallengeConfig = chc
	}

	return &cp
}

func cloneIPSet(s *IPSet) *IPSet {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)
	cp.Addresses = cloneAddresses(s.Addresses)

	return &cp
}

func cloneAddresses(addresses []string) []string {
	if addresses == nil {
		return []string{}
	}

	out := make([]string, len(addresses))
	copy(out, addresses)

	return out
}

func cloneTags(tags map[string]string) map[string]string {
	if tags == nil {
		return make(map[string]string)
	}

	return maps.Clone(tags)
}

// Reset clears all WAFv2 WebACL and IPSet state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.webACLs = make(map[string]*WebACL)
	b.ipSets = make(map[string]*IPSet)
	b.regexPatternSets = make(map[string]*RegexPatternSet)
	b.ruleGroups = make(map[string]*RuleGroup)
	b.managedRuleSets = make(map[string]*ManagedRuleSet)
	b.apiKeys = make(map[string]*APIKey)
	b.loggingConfigs = make(map[string]json.RawMessage)
	b.permissionPolicies = make(map[string]string)
	b.webACLByARN = make(map[string]string)
	b.ipSetByARN = make(map[string]string)
	b.regexPatternSetByARN = make(map[string]string)
	b.ruleGroupByARN = make(map[string]string)
	b.webACLByNameScope = make(map[string]string)
	b.ipSetByNameScope = make(map[string]string)
	b.regexPatternSetByScope = make(map[string]string)
	b.ruleGroupByNameScope = make(map[string]string)
	b.associations = make(map[string]string)
}

// AssociateWebACL associates a WebACL with a resource ARN.
func (b *InMemoryBackend) AssociateWebACL(webACLARN, resourceARN string) error {
	b.mu.Lock("AssociateWebACL")
	defer b.mu.Unlock()

	webACLID, ok := b.webACLByARN[webACLARN]
	if !ok {
		return fmt.Errorf("%w: web ACL with ARN %q not found", ErrWebACLNotFound, webACLARN)
	}

	b.associations[resourceARN] = webACLID

	return nil
}

// DisassociateWebACL removes the WebACL association from a resource ARN.
// Per AWS behaviour, this is a no-op if no association exists (idempotent).
func (b *InMemoryBackend) DisassociateWebACL(resourceARN string) error {
	b.mu.Lock("DisassociateWebACL")
	defer b.mu.Unlock()

	// AWS treats DisassociateWebACL as idempotent — calling it when no
	// association exists succeeds silently.
	delete(b.associations, resourceARN)

	return nil
}

// GetWebACLForResource returns the WebACL associated with the given resource ARN.
func (b *InMemoryBackend) GetWebACLForResource(resourceARN string) (*WebACL, error) {
	b.mu.RLock("GetWebACLForResource")
	defer b.mu.RUnlock()

	webACLID, ok := b.associations[resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: no web ACL association found for resource %q", ErrAssociationNotFound, resourceARN)
	}

	w, ok := b.webACLs[webACLID]
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, webACLID)
	}

	return cloneWebACL(w), nil
}

// CheckCapacity returns the capacity consumed by the provided rules.
// Each rule costs wcuPerRule WCUs in this in-memory implementation.
func (b *InMemoryBackend) CheckCapacity(_ string, rules []map[string]any) (int64, error) {
	return int64(len(rules)) * wcuPerRule, nil
}

// CreateAPIKey creates a new API key for the given scope and token domains.
func (b *InMemoryBackend) CreateAPIKey(scope string, tokenDomains []string) (*APIKey, error) {
	b.mu.Lock("CreateAPIKey")
	defer b.mu.Unlock()

	key := uuid.NewString()
	a := &APIKey{
		APIKeyValue:  key,
		Scope:        scope,
		TokenDomains: cloneAddresses(tokenDomains),
	}
	b.apiKeys[apiKeyMapKey(scope, key)] = a

	return &APIKey{
		APIKeyValue:  a.APIKeyValue,
		Scope:        a.Scope,
		TokenDomains: cloneAddresses(a.TokenDomains),
	}, nil
}

// DeleteAPIKey deletes the API key identified by scope and key value.
func (b *InMemoryBackend) DeleteAPIKey(scope, apiKey string) error {
	b.mu.Lock("DeleteAPIKey")
	defer b.mu.Unlock()

	k := apiKeyMapKey(scope, apiKey)
	if _, ok := b.apiKeys[k]; !ok {
		return fmt.Errorf("%w: API key not found", ErrAPIKeyNotFound)
	}

	delete(b.apiKeys, k)

	return nil
}

// CreateRegexPatternSet creates a new RegexPatternSet.
func (b *InMemoryBackend) CreateRegexPatternSet(
	name, scope, description string,
	regularExpressionList []RegexEntry,
	tags map[string]string,
) (*RegexPatternSet, error) {
	b.mu.Lock("CreateRegexPatternSet")
	defer b.mu.Unlock()

	if _, exists := b.regexPatternSetByScope[nameScope(name, scope)]; exists {
		return nil, fmt.Errorf(
			"%w: regex pattern set %q already exists in scope %s",
			ErrRegexPatternSetAlreadyExists,
			name,
			scope,
		)
	}

	id := uuid.NewString()
	rps := &RegexPatternSet{
		ID:                    id,
		Name:                  name,
		Scope:                 scope,
		Description:           description,
		RegularExpressionList: cloneRegexEntries(regularExpressionList),
		LockToken:             uuid.NewString(),
		Tags:                  cloneTags(tags),
	}
	b.regexPatternSets[id] = rps
	arnStr := b.RegexPatternSetARN(name, id, scope)
	b.regexPatternSetByARN[arnStr] = id
	b.regexPatternSetByScope[nameScope(name, scope)] = id

	return cloneRegexPatternSet(rps), nil
}

// DeleteRegexPatternSet deletes a RegexPatternSet by ID.
func (b *InMemoryBackend) DeleteRegexPatternSet(id, lockToken string) error {
	b.mu.Lock("DeleteRegexPatternSet")
	defer b.mu.Unlock()

	rps, ok := b.regexPatternSets[id]
	if !ok {
		return fmt.Errorf("%w: regex pattern set %q not found", ErrRegexPatternSetNotFound, id)
	}

	if lockToken != "" && lockToken != rps.LockToken {
		return fmt.Errorf("%w: lock token mismatch for regex pattern set %q", ErrOptimisticLock, id)
	}

	delete(b.regexPatternSetByARN, b.RegexPatternSetARN(rps.Name, rps.ID, rps.Scope))
	delete(b.regexPatternSetByScope, nameScope(rps.Name, rps.Scope))
	delete(b.regexPatternSets, id)

	return nil
}

// CreateRuleGroup creates a new RuleGroup.
func (b *InMemoryBackend) CreateRuleGroup(
	name, scope, description, visibilityConfig string,
	capacity int64,
	rules []map[string]any,
	tags map[string]string,
) (*RuleGroup, error) {
	b.mu.Lock("CreateRuleGroup")
	defer b.mu.Unlock()

	if _, exists := b.ruleGroupByNameScope[nameScope(name, scope)]; exists {
		return nil, fmt.Errorf("%w: rule group %q already exists in scope %s", ErrRuleGroupAlreadyExists, name, scope)
	}

	id := uuid.NewString()
	rg := &RuleGroup{
		ID:               id,
		Name:             name,
		Scope:            scope,
		Description:      description,
		VisibilityConfig: visibilityConfig,
		Capacity:         capacity,
		Rules:            cloneRules(rules),
		LockToken:        uuid.NewString(),
		Tags:             cloneTags(tags),
	}
	b.ruleGroups[id] = rg
	arnStr := b.RuleGroupARN(name, id, scope)
	b.ruleGroupByARN[arnStr] = id
	b.ruleGroupByNameScope[nameScope(name, scope)] = id

	return cloneRuleGroup(rg), nil
}

// DeleteRuleGroup deletes a RuleGroup by ID, checking for WebACL references.
func (b *InMemoryBackend) DeleteRuleGroup(id, lockToken string) error {
	b.mu.Lock("DeleteRuleGroup")
	defer b.mu.Unlock()

	rg, ok := b.ruleGroups[id]
	if !ok {
		return fmt.Errorf("%w: rule group %q not found", ErrRuleGroupNotFound, id)
	}

	if lockToken != "" && lockToken != rg.LockToken {
		return fmt.Errorf("%w: lock token mismatch for rule group %q", ErrOptimisticLock, id)
	}

	// Check if this rule group is referenced by any WebACL.
	rgARN := b.RuleGroupARN(rg.Name, rg.ID, rg.Scope)

	for _, w := range b.webACLs {
		for _, rule := range w.Rules {
			if b.ruleReferencesARN(rule, rgARN) {
				return fmt.Errorf(
					"%w: rule group %q is referenced by web ACL %q",
					ErrAssociatedItem,
					id,
					w.ID,
				)
			}
		}
	}

	delete(b.ruleGroupByARN, rgARN)
	delete(b.ruleGroupByNameScope, nameScope(rg.Name, rg.Scope))
	delete(b.ruleGroups, id)

	return nil
}

// ruleReferencesARN checks if a rule map references the given ARN.
func (b *InMemoryBackend) ruleReferencesARN(rule map[string]any, arnStr string) bool {
	stmt, isStmt := rule["Statement"].(map[string]any)
	if !isStmt {
		return false
	}

	rgrStmt, isRGR := stmt["RuleGroupReferenceStatement"].(map[string]any)
	if !isRGR {
		return false
	}

	ref, isStr := rgrStmt["ARN"].(string)

	return isStr && ref == arnStr
}

// DeleteFirewallManagerRuleGroups removes all Firewall Manager rule group
// associations from the WebACL identified by webACLARN, then returns a fresh
// copy of the updated WebACL.
func (b *InMemoryBackend) DeleteFirewallManagerRuleGroups(webACLARN string) (*WebACL, error) {
	b.mu.Lock("DeleteFirewallManagerRuleGroups")
	defer b.mu.Unlock()

	webACLID, ok := b.webACLByARN[webACLARN]
	if !ok {
		return nil, fmt.Errorf("%w: web ACL with ARN %q not found", ErrWebACLNotFound, webACLARN)
	}

	w, ok := b.webACLs[webACLID]
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, webACLID)
	}

	w.LockToken = uuid.NewString()

	return cloneWebACL(w), nil
}

// PutLoggingConfiguration stores a full logging configuration JSON for the given resource ARN.
func (b *InMemoryBackend) PutLoggingConfiguration(resourceARN string, configJSON json.RawMessage) error {
	b.mu.Lock("PutLoggingConfiguration")
	defer b.mu.Unlock()

	stored := make(json.RawMessage, len(configJSON))
	copy(stored, configJSON)
	b.loggingConfigs[resourceARN] = stored

	return nil
}

// DeleteLoggingConfiguration removes the logging configuration for the given resource ARN.
func (b *InMemoryBackend) DeleteLoggingConfiguration(resourceARN string) error {
	b.mu.Lock("DeleteLoggingConfiguration")
	defer b.mu.Unlock()

	if _, exists := b.loggingConfigs[resourceARN]; !exists {
		return fmt.Errorf("%w: no logging configuration found for resource %q", ErrLoggingConfigNotFound, resourceARN)
	}

	delete(b.loggingConfigs, resourceARN)

	return nil
}

// GetLoggingConfiguration returns the stored logging configuration JSON for the given resource ARN.
func (b *InMemoryBackend) GetLoggingConfiguration(resourceARN string) (json.RawMessage, error) {
	b.mu.RLock("GetLoggingConfiguration")
	defer b.mu.RUnlock()

	cfg, exists := b.loggingConfigs[resourceARN]
	if !exists {
		return nil, fmt.Errorf(
			"%w: no logging configuration found for resource %q",
			ErrLoggingConfigNotFound,
			resourceARN,
		)
	}

	out := make(json.RawMessage, len(cfg))
	copy(out, cfg)

	return out, nil
}

// ListLoggingConfigurations returns all stored logging configuration JSONs.
func (b *InMemoryBackend) ListLoggingConfigurations() []json.RawMessage {
	b.mu.RLock("ListLoggingConfigurations")
	defer b.mu.RUnlock()

	result := make([]json.RawMessage, 0, len(b.loggingConfigs))

	for _, cfg := range b.loggingConfigs {
		out := make(json.RawMessage, len(cfg))
		copy(out, cfg)
		result = append(result, out)
	}

	return result
}

// PutPermissionPolicy stores a permission policy for the given resource ARN.
func (b *InMemoryBackend) PutPermissionPolicy(resourceARN, policy string) error {
	b.mu.Lock("PutPermissionPolicy")
	defer b.mu.Unlock()

	b.permissionPolicies[resourceARN] = policy

	return nil
}

// DeletePermissionPolicy removes the permission policy for the given resource ARN.
func (b *InMemoryBackend) DeletePermissionPolicy(resourceARN string) error {
	b.mu.Lock("DeletePermissionPolicy")
	defer b.mu.Unlock()

	if _, ok := b.permissionPolicies[resourceARN]; !ok {
		return fmt.Errorf("%w: no permission policy found for resource %q", ErrPermissionPolicyNotFound, resourceARN)
	}

	delete(b.permissionPolicies, resourceARN)

	return nil
}

// GetRegexPatternSet returns a RegexPatternSet by ID.
func (b *InMemoryBackend) GetRegexPatternSet(id string) (*RegexPatternSet, error) {
	b.mu.RLock("GetRegexPatternSet")
	defer b.mu.RUnlock()

	r, ok := b.regexPatternSets[id]
	if !ok {
		return nil, fmt.Errorf("%w: regex pattern set %q not found", ErrRegexPatternSetNotFound, id)
	}

	return cloneRegexPatternSet(r), nil
}

// ListRegexPatternSets returns all RegexPatternSets sorted by name.
func (b *InMemoryBackend) ListRegexPatternSets() []*RegexPatternSet {
	b.mu.RLock("ListRegexPatternSets")
	defer b.mu.RUnlock()

	list := make([]*RegexPatternSet, 0, len(b.regexPatternSets))

	for _, r := range b.regexPatternSets {
		list = append(list, cloneRegexPatternSet(r))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateRegexPatternSet updates a RegexPatternSet by ID.
func (b *InMemoryBackend) UpdateRegexPatternSet(
	id, description, lockToken string,
	regularExpressionList []RegexEntry,
) (*RegexPatternSet, error) {
	b.mu.Lock("UpdateRegexPatternSet")
	defer b.mu.Unlock()

	r, ok := b.regexPatternSets[id]
	if !ok {
		return nil, fmt.Errorf("%w: regex pattern set %q not found", ErrRegexPatternSetNotFound, id)
	}

	if lockToken != "" && lockToken != r.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for regex pattern set %q", ErrOptimisticLock, id)
	}

	if description != "" {
		r.Description = description
	}

	if regularExpressionList != nil {
		r.RegularExpressionList = cloneRegexEntries(regularExpressionList)
	}

	r.LockToken = uuid.NewString()

	return cloneRegexPatternSet(r), nil
}

// GetRuleGroup returns a RuleGroup by ID.
func (b *InMemoryBackend) GetRuleGroup(id string) (*RuleGroup, error) {
	b.mu.RLock("GetRuleGroup")
	defer b.mu.RUnlock()

	rg, ok := b.ruleGroups[id]
	if !ok {
		return nil, fmt.Errorf("%w: rule group %q not found", ErrRuleGroupNotFound, id)
	}

	return cloneRuleGroup(rg), nil
}

// ListRuleGroups returns all RuleGroups sorted by name.
func (b *InMemoryBackend) ListRuleGroups() []*RuleGroup {
	b.mu.RLock("ListRuleGroups")
	defer b.mu.RUnlock()

	list := make([]*RuleGroup, 0, len(b.ruleGroups))

	for _, rg := range b.ruleGroups {
		list = append(list, cloneRuleGroup(rg))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateRuleGroup updates a RuleGroup by ID.
func (b *InMemoryBackend) UpdateRuleGroup(
	id, description, visibilityConfig, lockToken string,
	rules []map[string]any,
) (*RuleGroup, error) {
	b.mu.Lock("UpdateRuleGroup")
	defer b.mu.Unlock()

	rg, ok := b.ruleGroups[id]
	if !ok {
		return nil, fmt.Errorf("%w: rule group %q not found", ErrRuleGroupNotFound, id)
	}

	if lockToken != "" && lockToken != rg.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for rule group %q", ErrOptimisticLock, id)
	}

	if description != "" {
		rg.Description = description
	}

	if visibilityConfig != "" {
		rg.VisibilityConfig = visibilityConfig
	}

	if rules != nil {
		rg.Rules = cloneRules(rules)
	}

	rg.LockToken = uuid.NewString()

	return cloneRuleGroup(rg), nil
}

// ListAPIKeys returns all API keys, optionally filtered by scope.
func (b *InMemoryBackend) ListAPIKeys(scope string) []*APIKey {
	b.mu.RLock("ListAPIKeys")
	defer b.mu.RUnlock()

	list := make([]*APIKey, 0, len(b.apiKeys))

	for _, a := range b.apiKeys {
		if scope == "" || a.Scope == scope {
			list = append(list, &APIKey{
				APIKeyValue:  a.APIKeyValue,
				Scope:        a.Scope,
				TokenDomains: cloneAddresses(a.TokenDomains),
			})
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].APIKeyValue < list[j].APIKeyValue })

	return list
}

// GetDecryptedAPIKey returns the API key identified by scope and key value.
func (b *InMemoryBackend) GetDecryptedAPIKey(scope, apiKey string) (*APIKey, error) {
	b.mu.RLock("GetDecryptedAPIKey")
	defer b.mu.RUnlock()

	a, ok := b.apiKeys[apiKeyMapKey(scope, apiKey)]
	if !ok {
		return nil, fmt.Errorf("%w: API key not found", ErrAPIKeyNotFound)
	}

	return &APIKey{
		APIKeyValue:  a.APIKeyValue,
		Scope:        a.Scope,
		TokenDomains: cloneAddresses(a.TokenDomains),
	}, nil
}

// GetPermissionPolicy returns the permission policy for the given resource ARN.
func (b *InMemoryBackend) GetPermissionPolicy(resourceARN string) (string, error) {
	b.mu.RLock("GetPermissionPolicy")
	defer b.mu.RUnlock()

	policy, ok := b.permissionPolicies[resourceARN]
	if !ok {
		return "", fmt.Errorf(
			"%w: no permission policy found for resource %q",
			ErrPermissionPolicyNotFound,
			resourceARN,
		)
	}

	return policy, nil
}

// ListResourcesForWebACL returns all resource ARNs associated with the given WebACL ARN.
func (b *InMemoryBackend) ListResourcesForWebACL(webACLARN string) ([]string, error) {
	b.mu.RLock("ListResourcesForWebACL")
	defer b.mu.RUnlock()

	if _, ok := b.webACLByARN[webACLARN]; !ok {
		return nil, fmt.Errorf("%w: web ACL with ARN %q not found", ErrWebACLNotFound, webACLARN)
	}

	webACLID := b.webACLByARN[webACLARN]
	result := make([]string, 0, len(b.associations))

	for resourceARN, wID := range b.associations {
		if wID == webACLID {
			result = append(result, resourceARN)
		}
	}

	sort.Strings(result)

	return result, nil
}

func cloneRegexPatternSet(r *RegexPatternSet) *RegexPatternSet {
	cp := *r
	cp.Tags = maps.Clone(r.Tags)
	cp.RegularExpressionList = cloneRegexEntries(r.RegularExpressionList)

	return &cp
}

func cloneRegexEntries(entries []RegexEntry) []RegexEntry {
	if entries == nil {
		return []RegexEntry{}
	}

	out := make([]RegexEntry, len(entries))
	copy(out, entries)

	return out
}

func cloneRuleGroup(rg *RuleGroup) *RuleGroup {
	cp := *rg
	cp.Tags = maps.Clone(rg.Tags)
	cp.Rules = cloneRules(rg.Rules)

	return &cp
}

// shallowCopyRules returns a shallow copy of each rule map in rules.
// Used as a fallback when JSON round-trip fails in cloneRules.
func shallowCopyRules(rules []map[string]any) []map[string]any {
	out := make([]map[string]any, len(rules))

	for i, r := range rules {
		rm := make(map[string]any, len(r))
		maps.Copy(rm, r)
		out[i] = rm
	}

	return out
}

// ManagedRuleSetARN builds an ARN for a ManagedRuleSet.
func (b *InMemoryBackend) ManagedRuleSetARN(name, id, scope string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", b.arnRegion(scope), b.accountID, prefix+"/managedruleset/"+name+"/"+id)
}

// GetManagedRuleSet returns a ManagedRuleSet by ID.
func (b *InMemoryBackend) GetManagedRuleSet(id string) (*ManagedRuleSet, error) {
	b.mu.RLock("GetManagedRuleSet")
	defer b.mu.RUnlock()

	ms, ok := b.managedRuleSets[id]
	if !ok {
		return nil, fmt.Errorf("%w: managed rule set %q not found", ErrManagedRuleSetNotFound, id)
	}

	return cloneManagedRuleSet(ms), nil
}

// ListManagedRuleSets returns all managed rule sets sorted by name, optionally filtered by scope.
func (b *InMemoryBackend) ListManagedRuleSets(scope string) []*ManagedRuleSet {
	b.mu.RLock("ListManagedRuleSets")
	defer b.mu.RUnlock()

	list := make([]*ManagedRuleSet, 0, len(b.managedRuleSets))

	for _, ms := range b.managedRuleSets {
		if scope != "" && ms.Scope != scope {
			continue
		}

		list = append(list, cloneManagedRuleSet(ms))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// PutManagedRuleSetVersions creates or updates a managed rule set with the given versions.
// If the ID does not exist, a new managed rule set is created. If it exists, the lock token
// is verified before updating.
func (b *InMemoryBackend) PutManagedRuleSetVersions(
	id, name, scope, lockToken, recommendedVersion string,
	versionsToPublish map[string]any,
) (*ManagedRuleSet, error) {
	b.mu.Lock("PutManagedRuleSetVersions")
	defer b.mu.Unlock()

	ms, exists := b.managedRuleSets[id]
	if exists && lockToken != "" && lockToken != ms.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for managed rule set %q", ErrOptimisticLock, id)
	}

	if !exists {
		arnStr := b.ManagedRuleSetARN(name, id, scope)
		ms = &ManagedRuleSet{
			ID:                id,
			Name:              name,
			Scope:             scope,
			ARN:               arnStr,
			LockToken:         uuid.NewString(),
			PublishedVersions: make(map[string]ManagedRuleSetVersion),
		}
		b.managedRuleSets[id] = ms
	}

	if versionsToPublish != nil {
		for versionName, versionRaw := range versionsToPublish {
			version := ManagedRuleSetVersion{}
			if vMap, ok := versionRaw.(map[string]any); ok {
				if arnVal, ok := vMap["AssociatedRuleGroupArn"].(string); ok {
					version.AssociatedRuleGroupArn = arnVal
				}

				if cap, ok := toInt64(vMap["Capacity"]); ok {
					version.Capacity = cap
				}
			}

			ms.PublishedVersions[versionName] = version
		}
	}

	if recommendedVersion != "" {
		ms.RecommendedVersion = recommendedVersion
	}

	ms.LockToken = uuid.NewString()

	return cloneManagedRuleSet(ms), nil
}

// UpdateManagedRuleSetVersionExpiryDate updates the expiry timestamp on a specific version
// of a managed rule set. Returns the updated managed rule set, the expiring version name,
// and any error.
func (b *InMemoryBackend) UpdateManagedRuleSetVersionExpiryDate(
	id, lockToken, versionToExpire string,
	expiryTimestamp *int64,
) (*ManagedRuleSet, error) {
	b.mu.Lock("UpdateManagedRuleSetVersionExpiryDate")
	defer b.mu.Unlock()

	ms, ok := b.managedRuleSets[id]
	if !ok {
		return nil, fmt.Errorf("%w: managed rule set %q not found", ErrManagedRuleSetNotFound, id)
	}

	if lockToken != "" && lockToken != ms.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for managed rule set %q", ErrOptimisticLock, id)
	}

	v, ok := ms.PublishedVersions[versionToExpire]
	if !ok {
		return nil, fmt.Errorf(
			"%w: version %q not found in managed rule set %q",
			ErrManagedRuleSetNotFound,
			versionToExpire,
			id,
		)
	}

	v.ExpiryTimestamp = expiryTimestamp
	ms.PublishedVersions[versionToExpire] = v
	ms.LockToken = uuid.NewString()

	return cloneManagedRuleSet(ms), nil
}

func cloneManagedRuleSet(ms *ManagedRuleSet) *ManagedRuleSet {
	cp := *ms

	if ms.PublishedVersions != nil {
		cp.PublishedVersions = make(map[string]ManagedRuleSetVersion, len(ms.PublishedVersions))

		for k, v := range ms.PublishedVersions {
			cp.PublishedVersions[k] = v
		}
	}

	return &cp
}

// cloneRules performs a deep clone of a rules slice. A JSON round-trip is used
// to ensure that nested maps and any json.RawMessage-backed values do not share
// backing arrays with the original, preventing data races and mutation aliasing.
func cloneRules(rules []map[string]any) []map[string]any {
	if rules == nil {
		return []map[string]any{}
	}

	data, marshalErr := json.Marshal(rules)
	if marshalErr != nil {
		return shallowCopyRules(rules)
	}

	var out []map[string]any
	if unmarshalErr := json.Unmarshal(data, &out); unmarshalErr != nil {
		return shallowCopyRules(rules)
	}

	return out
}
