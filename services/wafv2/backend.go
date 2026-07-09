package wafv2

import (
	"context"
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
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// storeRegion returns the map key used for storing a WAFv2 resource.
// CLOUDFRONT resources are global so they always use "" as the key,
// matching the empty region field in their ARNs.
func storeRegion(scope, requestRegion string) string {
	return arnRegionForScope(scope, requestRegion)
}

// regionFromARN extracts the region component (index 3) from a WAFv2 ARN.
// Returns "" for CLOUDFRONT ARNs (arn:aws:wafv2::account:global/...).
func regionFromARN(resourceARN string) string {
	const regionIdx = 3

	parts := strings.Split(resourceARN, ":")
	if len(parts) > regionIdx {
		return parts[regionIdx]
	}

	return ""
}

// regionKey builds the composite store.Table/Index key used throughout this
// backend to flatten what used to be a map[region]map[key]*T nested map into
// a single map[key]*T (see pkgs/store's package doc). It is the direct
// mechanical replacement for indexing into the region bucket first: any call
// site that used to write `someMap[region][key]` now uses
// `someTable.Get(regionKey(region, key))`.
func regionKey(region, key string) string {
	return region + "|" + key
}

func fillVersionFromRaw(v *ManagedRuleSetVersion, raw any) {
	vMap, ok := raw.(map[string]any)
	if !ok {
		return
	}

	if arnVal, arnOK := vMap["AssociatedRuleGroupArn"].(string); arnOK {
		v.AssociatedRuleGroupArn = arnVal
	}

	if capVal, capOK := toInt64(vMap["Capacity"]); capOK {
		v.Capacity = capVal
	}
}

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
	// ErrManagedRuleGroupNotFound is returned when a managed rule group is not in the catalog.
	ErrManagedRuleGroupNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
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
	Tags             map[string]string `json:"tags,omitempty"`
	ARN              string            `json:"arn,omitempty"`
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
}

// InMemoryBackend is an in-memory store for WAFv2 resources.
//
// webACLs/ipSets/regexPatternSets/ruleGroups are "clean" store.Table
// registrations (Phase 3.3 -- see store_setup.go's file doc comment): each
// replaces what used to be three separate region-nested maps (the primary
// map[region]map[id]*T, plus map[region]map[arn]string and
// map[region]map[nameScope]string secondary indexes) with one
// *store.Table[T] keyed by a region+id composite (see regionKey) and two
// *store.Index[T] (by ARN, by name+scope) plus one *store.Index[T] grouping
// by region for List operations. managedRuleSets/apiKeys are "dirty" tables
// (identity-less: their region-bucket key isn't reliably recoverable from
// their other fields -- see ManagedRuleSet.Region/APIKey.Region) so they are
// NOT registered on b.registry; persistence.go round-trips them through a
// DTO registry instead, mirroring services/ses's pattern.
type InMemoryBackend struct {
	registry                    *store.Registry
	webACLs                     *store.Table[WebACL]
	webACLsByARN                *store.Index[WebACL]
	webACLsByNameScope          *store.Index[WebACL]
	webACLsByRegion             *store.Index[WebACL]
	ipSets                      *store.Table[IPSet]
	ipSetsByARN                 *store.Index[IPSet]
	ipSetsByNameScope           *store.Index[IPSet]
	ipSetsByRegion              *store.Index[IPSet]
	regexPatternSets            *store.Table[RegexPatternSet]
	regexPatternSetsByARN       *store.Index[RegexPatternSet]
	regexPatternSetsByNameScope *store.Index[RegexPatternSet]
	regexPatternSetsByRegion    *store.Index[RegexPatternSet]
	ruleGroups                  *store.Table[RuleGroup]
	ruleGroupsByARN             *store.Index[RuleGroup]
	ruleGroupsByNameScope       *store.Index[RuleGroup]
	ruleGroupsByRegion          *store.Index[RuleGroup]
	managedRuleSets             *store.Table[ManagedRuleSet]
	managedRuleSetsByRegion     *store.Index[ManagedRuleSet]
	apiKeys                     *store.Table[APIKey]
	apiKeysByRegion             *store.Index[APIKey]
	loggingConfigs              map[string]map[string]json.RawMessage
	permissionPolicies          map[string]map[string]string
	associations                map[string]map[string]string
	mu                          *lockmetrics.RWMutex
	accountID                   string
	region                      string
}

// maxRegions caps the number of distinct region keys stored in each per-region map.
// In practice only "" (CLOUDFRONT) and the backend's region are ever used,
// so this guards against pathological multi-region request storms.
const maxRegions = 100

// initRegion returns (or lazily creates) the inner map for region in m.
// If m already holds maxRegions distinct region keys and region is new,
// a fresh unsaved map is returned so the process doesn't crash but
// new data beyond the cap is not persisted.
func initRegion[V any](m map[string]map[string]V, region string) map[string]V {
	if m[region] == nil {
		if len(m) >= maxRegions {
			return make(map[string]V)
		}

		m[region] = make(map[string]V)
	}

	return m[region]
}

// NewInMemoryBackend creates a new in-memory WAFv2 backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:           store.NewRegistry(),
		loggingConfigs:     make(map[string]map[string]json.RawMessage),
		permissionPolicies: make(map[string]map[string]string),
		associations:       make(map[string]map[string]string),
		accountID:          accountID,
		region:             region,
		mu:                 lockmetrics.New("wafv2"),
	}

	registerAllTables(b)

	return b
}

func (b *InMemoryBackend) loggingConfigsStore(region string) map[string]json.RawMessage {
	return initRegion(b.loggingConfigs, region)
}

func (b *InMemoryBackend) permissionPoliciesStore(region string) map[string]string {
	return initRegion(b.permissionPolicies, region)
}

func (b *InMemoryBackend) associationsStore(region string) map[string]string {
	return initRegion(b.associations, region)
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// validScope reports whether scope is a recognised WAFv2 scope.
func validScope(scope string) bool {
	return scope == ScopeRegional || scope == ScopeCloudFront
}

func arnRegionForScope(scope, region string) string {
	if scope == ScopeCloudFront {
		return ""
	}

	return region
}

// arnRegion returns the correct region segment for a WAFv2 ARN. CLOUDFRONT
// (global) resources use an empty region, matching the real AWS ARN format:
// arn:aws:wafv2::123456789012:global/webacl/...
func (b *InMemoryBackend) arnRegion(scope string) string {
	return arnRegionForScope(scope, b.region)
}

func (b *InMemoryBackend) buildWebACLARN(name, id, scope, region string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", arnRegionForScope(scope, region), b.accountID, prefix+"/webacl/"+name+"/"+id)
}

func (b *InMemoryBackend) buildIPSetARN(name, id, scope, region string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", arnRegionForScope(scope, region), b.accountID, prefix+"/ipset/"+name+"/"+id)
}

func (b *InMemoryBackend) buildRegexPatternSetARN(name, id, scope, region string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", arnRegionForScope(scope, region), b.accountID, prefix+"/regexpatternset/"+name+"/"+id)
}

func (b *InMemoryBackend) buildRuleGroupARN(name, id, scope, region string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", arnRegionForScope(scope, region), b.accountID, prefix+"/rulegroup/"+name+"/"+id)
}

func (b *InMemoryBackend) buildManagedRuleSetARN(name, id, scope, region string) string {
	prefix := scopePrefix(scope)

	return arn.Build("wafv2", arnRegionForScope(scope, region), b.accountID, prefix+"/managedruleset/"+name+"/"+id)
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
	ctx context.Context,
	name, scope, description string,
	defaultAction, visibilityConfig json.RawMessage,
	rules []map[string]any,
	tokenDomains []string,
	customResponseBodies, associationConfig, captchaConfig, challengeConfig json.RawMessage,
	tags map[string]string,
) (*WebACL, error) {
	b.mu.Lock("CreateWebACL")
	defer b.mu.Unlock()

	region := storeRegion(scope, getRegion(ctx, b.region))

	if len(b.webACLsByNameScope.Get(regionKey(region, nameScope(name, scope)))) > 0 {
		return nil, fmt.Errorf("%w: web ACL %q already exists in scope %s", ErrWebACLAlreadyExists, name, scope)
	}

	id := uuid.NewString()
	arnStr := b.buildWebACLARN(name, id, scope, region)
	w := &WebACL{
		ARN:                  arnStr,
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
	b.webACLs.Put(w)

	return cloneWebACL(w), nil
}

// lookupWebACLByID finds a WebACL in requestRegion first, then the global CLOUDFRONT
// store ("") so that CLOUDFRONT resources are always accessible.
func (b *InMemoryBackend) lookupWebACLByID(requestRegion, id string) (*WebACL, bool) {
	if w, ok := b.webACLs.Get(regionKey(requestRegion, id)); ok {
		return w, true
	}

	if requestRegion != "" {
		if w, ok := b.webACLs.Get(regionKey("", id)); ok {
			return w, true
		}
	}

	return nil, false
}

// lookupIPSetByID finds an IPSet with the same CLOUDFRONT fallback logic.
func (b *InMemoryBackend) lookupIPSetByID(requestRegion, id string) (*IPSet, bool) {
	if s, ok := b.ipSets.Get(regionKey(requestRegion, id)); ok {
		return s, true
	}

	if requestRegion != "" {
		if s, ok := b.ipSets.Get(regionKey("", id)); ok {
			return s, true
		}
	}

	return nil, false
}

// lookupRegexPatternSetByID finds a RegexPatternSet with the same CLOUDFRONT fallback logic.
func (b *InMemoryBackend) lookupRegexPatternSetByID(requestRegion, id string) (*RegexPatternSet, bool) {
	if r, ok := b.regexPatternSets.Get(regionKey(requestRegion, id)); ok {
		return r, true
	}

	if requestRegion != "" {
		if r, ok := b.regexPatternSets.Get(regionKey("", id)); ok {
			return r, true
		}
	}

	return nil, false
}

// lookupRuleGroupByID finds a RuleGroup with the same CLOUDFRONT fallback logic.
func (b *InMemoryBackend) lookupRuleGroupByID(requestRegion, id string) (*RuleGroup, bool) {
	if rg, ok := b.ruleGroups.Get(regionKey(requestRegion, id)); ok {
		return rg, true
	}

	if requestRegion != "" {
		if rg, ok := b.ruleGroups.Get(regionKey("", id)); ok {
			return rg, true
		}
	}

	return nil, false
}

// GetWebACL returns a WebACL by ID.
func (b *InMemoryBackend) GetWebACL(ctx context.Context, id string) (*WebACL, error) {
	b.mu.RLock("GetWebACL")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	w, ok := b.lookupWebACLByID(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, id)
	}

	return cloneWebACL(w), nil
}

// UpdateWebACL updates a WebACL by ID.
func (b *InMemoryBackend) UpdateWebACL(
	ctx context.Context,
	id, description, lockToken string,
	defaultAction, visibilityConfig json.RawMessage,
	rules []map[string]any,
	tokenDomains []string,
	customResponseBodies, associationConfig, captchaConfig, challengeConfig json.RawMessage,
) (*WebACL, error) {
	b.mu.Lock("UpdateWebACL")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	w, ok := b.lookupWebACLByID(region, id)
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
func (b *InMemoryBackend) DeleteWebACL(ctx context.Context, id, lockToken string) error {
	b.mu.Lock("DeleteWebACL")
	defer b.mu.Unlock()

	requestRegion := getRegion(ctx, b.region)
	w, ok := b.lookupWebACLByID(requestRegion, id)
	if !ok {
		return fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, id)
	}

	// Use the resource's own store region (derived from its ARN).
	region := regionFromARN(w.ARN)

	if lockToken != "" && lockToken != w.LockToken {
		return fmt.Errorf("%w: lock token mismatch for web ACL %q", ErrOptimisticLock, id)
	}

	for _, assocID := range b.associations[region] {
		if assocID == id {
			return fmt.Errorf(
				"%w: web ACL %q is still associated with a resource; disassociate first",
				ErrAssociatedItem,
				id,
			)
		}
	}

	webACLArnStr := w.ARN

	b.webACLs.Delete(regionKey(region, id))
	delete(b.loggingConfigs[region], webACLArnStr)
	delete(b.permissionPolicies[region], webACLArnStr)

	return nil
}

// ListWebACLs returns all WebACLs sorted by name.
// For a REGIONAL request, returns REGIONAL resources from the ctx region PLUS
// any CLOUDFRONT (global) resources.
func (b *InMemoryBackend) ListWebACLs(ctx context.Context) []*WebACL {
	b.mu.RLock("ListWebACLs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	list := make([]*WebACL, 0)

	for _, w := range b.webACLsByRegion.Get(region) {
		list = append(list, cloneWebACL(w))
	}

	if region != "" {
		for _, w := range b.webACLsByRegion.Get("") {
			list = append(list, cloneWebACL(w))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list
}

// CreateIPSet creates a new IPSet.
func (b *InMemoryBackend) CreateIPSet(
	ctx context.Context,
	name, scope, description, ipAddressVersion string,
	addresses []string,
	tags map[string]string,
) (*IPSet, error) {
	b.mu.Lock("CreateIPSet")
	defer b.mu.Unlock()

	region := storeRegion(scope, getRegion(ctx, b.region))

	if len(b.ipSetsByNameScope.Get(regionKey(region, nameScope(name, scope)))) > 0 {
		return nil, fmt.Errorf("%w: IP set %q already exists in scope %s", ErrIPSetAlreadyExists, name, scope)
	}

	id := uuid.NewString()
	arnStr := b.buildIPSetARN(name, id, scope, region)
	s := &IPSet{
		ARN:              arnStr,
		ID:               id,
		Name:             name,
		Scope:            scope,
		Description:      description,
		IPAddressVersion: ipAddressVersion,
		Addresses:        cloneAddresses(addresses),
		LockToken:        uuid.NewString(),
		Tags:             cloneTags(tags),
	}
	b.ipSets.Put(s)

	return cloneIPSet(s), nil
}

// GetIPSet returns an IPSet by ID.
func (b *InMemoryBackend) GetIPSet(ctx context.Context, id string) (*IPSet, error) {
	b.mu.RLock("GetIPSet")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	s, ok := b.lookupIPSetByID(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: IP set %q not found", ErrIPSetNotFound, id)
	}

	return cloneIPSet(s), nil
}

// UpdateIPSet updates an IPSet by ID.
func (b *InMemoryBackend) UpdateIPSet(
	ctx context.Context,
	id, description, lockToken string,
	addresses []string,
) (*IPSet, error) {
	b.mu.Lock("UpdateIPSet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	s, ok := b.lookupIPSetByID(region, id)
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
func (b *InMemoryBackend) DeleteIPSet(ctx context.Context, id, lockToken string) error {
	b.mu.Lock("DeleteIPSet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	s, ok := b.lookupIPSetByID(region, id)
	if !ok {
		return fmt.Errorf("%w: IP set %q not found", ErrIPSetNotFound, id)
	}

	storeReg := regionFromARN(s.ARN)

	if lockToken != "" && lockToken != s.LockToken {
		return fmt.Errorf("%w: lock token mismatch for IP set %q", ErrOptimisticLock, id)
	}

	b.ipSets.Delete(regionKey(storeReg, id))

	return nil
}

// ListIPSets returns all IPSets sorted by name.
func (b *InMemoryBackend) ListIPSets(ctx context.Context) []*IPSet {
	b.mu.RLock("ListIPSets")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	list := make([]*IPSet, 0)

	for _, s := range b.ipSetsByRegion.Get(region) {
		list = append(list, cloneIPSet(s))
	}

	if region != "" {
		for _, s := range b.ipSetsByRegion.Get("") {
			list = append(list, cloneIPSet(s))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})

	return list
}

// lookupTaggedResource resolves the tags pointer for a resource ARN via the
// by-ARN store.Index on each resource table (ARN is already globally unique,
// so no region partitioning is needed for this lookup). Returns nil if not found.
func (b *InMemoryBackend) lookupTaggedResource(resourceARN string) *map[string]string {
	if ws := b.webACLsByARN.Get(resourceARN); len(ws) > 0 {
		return &ws[0].Tags
	}

	if ss := b.ipSetsByARN.Get(resourceARN); len(ss) > 0 {
		return &ss[0].Tags
	}

	if rs := b.regexPatternSetsByARN.Get(resourceARN); len(rs) > 0 {
		return &rs[0].Tags
	}

	if rgs := b.ruleGroupsByARN.Get(resourceARN); len(rgs) > 0 {
		return &rgs[0].Tags
	}

	return nil
}

// TagResource adds tags to a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) TagResource(_ context.Context, resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	tagsPtr := b.lookupTaggedResource(resourceARN)
	if tagsPtr == nil {
		return fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
	}

	if *tagsPtr == nil {
		*tagsPtr = make(map[string]string)
	}

	maps.Copy(*tagsPtr, tags)

	return nil
}

// ListTagsForResource returns the tags for a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(_ context.Context, resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tagsPtr := b.lookupTaggedResource(resourceARN)
	if tagsPtr == nil {
		return nil, fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
	}

	return maps.Clone(*tagsPtr), nil
}

// UntagResource removes tags from a WAFv2 resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(_ context.Context, resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	tagsPtr := b.lookupTaggedResource(resourceARN)
	if tagsPtr == nil {
		return fmt.Errorf("%w: resource %q not found", ErrWebACLNotFound, resourceARN)
	}

	for _, k := range tagKeys {
		delete(*tagsPtr, k)
	}

	return nil
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

// Reset clears all WAFv2 state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resetTablesLocked()
	b.loggingConfigs = make(map[string]map[string]json.RawMessage)
	b.permissionPolicies = make(map[string]map[string]string)
	b.associations = make(map[string]map[string]string)
}

// AssociateWebACL associates a WebACL with a resource ARN.
func (b *InMemoryBackend) AssociateWebACL(ctx context.Context, webACLARN, resourceARN string) error {
	b.mu.Lock("AssociateWebACL")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	webACLID, ok := b.webACLIDByARNInRegion(webACLARN, region)
	if !ok {
		return fmt.Errorf("%w: web ACL with ARN %q not found", ErrWebACLNotFound, webACLARN)
	}

	b.associationsStore(region)[resourceARN] = webACLID

	return nil
}

// webACLIDByARNInRegion resolves webACLARN to its WebACL ID, but only if the
// ARN's own embedded region (see regionFromARN) matches region. This
// reproduces the old map[region]map[arn]string index's behavior exactly: a
// WebACL is only found under the region bucket it was created into, so
// looking it up from a mismatched ctx region must still miss (see
// AssociateWebACL/DeleteFirewallManagerRuleGroups/ListResourcesForWebACL,
// none of which merge in the "" CLOUDFRONT bucket the way lookupWebACLByID does).
func (b *InMemoryBackend) webACLIDByARNInRegion(webACLARN, region string) (string, bool) {
	if regionFromARN(webACLARN) != region {
		return "", false
	}

	ws := b.webACLsByARN.Get(webACLARN)
	if len(ws) == 0 {
		return "", false
	}

	return ws[0].ID, true
}

// DisassociateWebACL removes the WebACL association from a resource ARN.
// Per AWS behaviour, this is a no-op if no association exists (idempotent).
func (b *InMemoryBackend) DisassociateWebACL(ctx context.Context, resourceARN string) error {
	b.mu.Lock("DisassociateWebACL")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	delete(b.associations[region], resourceARN)

	return nil
}

// GetWebACLForResource returns the WebACL associated with the given resource ARN.
func (b *InMemoryBackend) GetWebACLForResource(ctx context.Context, resourceARN string) (*WebACL, error) {
	b.mu.RLock("GetWebACLForResource")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	webACLID, ok := b.associations[region][resourceARN]
	if !ok {
		return nil, fmt.Errorf("%w: no web ACL association found for resource %q", ErrAssociationNotFound, resourceARN)
	}

	w, ok := b.webACLs.Get(regionKey(region, webACLID))
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, webACLID)
	}

	return cloneWebACL(w), nil
}

// CheckCapacity returns the capacity consumed by the provided rules.
// Each rule costs wcuPerRule WCUs in this in-memory implementation.
func (b *InMemoryBackend) CheckCapacity(_ context.Context, _ string, rules []map[string]any) (int64, error) {
	return int64(len(rules)) * wcuPerRule, nil
}

// CreateAPIKey creates a new API key for the given scope and token domains.
func (b *InMemoryBackend) CreateAPIKey(ctx context.Context, scope string, tokenDomains []string) (*APIKey, error) {
	b.mu.Lock("CreateAPIKey")
	defer b.mu.Unlock()

	region := storeRegion(scope, getRegion(ctx, b.region))
	key := uuid.NewString()
	a := &APIKey{
		APIKeyValue:  key,
		Scope:        scope,
		TokenDomains: cloneAddresses(tokenDomains),
		Region:       region,
	}
	b.apiKeys.Put(a)

	return &APIKey{
		APIKeyValue:  a.APIKeyValue,
		Scope:        a.Scope,
		TokenDomains: cloneAddresses(a.TokenDomains),
	}, nil
}

// DeleteAPIKey deletes the API key identified by scope and key value.
func (b *InMemoryBackend) DeleteAPIKey(ctx context.Context, scope, apiKey string) error {
	b.mu.Lock("DeleteAPIKey")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	compositeKey := regionKey(region, apiKeyMapKey(scope, apiKey))

	if !b.apiKeys.Delete(compositeKey) {
		return fmt.Errorf("%w: API key not found", ErrAPIKeyNotFound)
	}

	return nil
}

// CreateRegexPatternSet creates a new RegexPatternSet.
func (b *InMemoryBackend) CreateRegexPatternSet(
	ctx context.Context,
	name, scope, description string,
	regularExpressionList []RegexEntry,
	tags map[string]string,
) (*RegexPatternSet, error) {
	b.mu.Lock("CreateRegexPatternSet")
	defer b.mu.Unlock()

	region := storeRegion(scope, getRegion(ctx, b.region))

	if len(b.regexPatternSetsByNameScope.Get(regionKey(region, nameScope(name, scope)))) > 0 {
		return nil, fmt.Errorf(
			"%w: regex pattern set %q already exists in scope %s",
			ErrRegexPatternSetAlreadyExists,
			name,
			scope,
		)
	}

	id := uuid.NewString()
	arnStr := b.buildRegexPatternSetARN(name, id, scope, region)
	rps := &RegexPatternSet{
		ARN:                   arnStr,
		ID:                    id,
		Name:                  name,
		Scope:                 scope,
		Description:           description,
		RegularExpressionList: cloneRegexEntries(regularExpressionList),
		LockToken:             uuid.NewString(),
		Tags:                  cloneTags(tags),
	}
	b.regexPatternSets.Put(rps)

	return cloneRegexPatternSet(rps), nil
}

// DeleteRegexPatternSet deletes a RegexPatternSet by ID.
func (b *InMemoryBackend) DeleteRegexPatternSet(ctx context.Context, id, lockToken string) error {
	b.mu.Lock("DeleteRegexPatternSet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rps, ok := b.lookupRegexPatternSetByID(region, id)
	if !ok {
		return fmt.Errorf("%w: regex pattern set %q not found", ErrRegexPatternSetNotFound, id)
	}

	storeReg := regionFromARN(rps.ARN)

	if lockToken != "" && lockToken != rps.LockToken {
		return fmt.Errorf("%w: lock token mismatch for regex pattern set %q", ErrOptimisticLock, id)
	}

	b.regexPatternSets.Delete(regionKey(storeReg, id))

	return nil
}

// CreateRuleGroup creates a new RuleGroup.
func (b *InMemoryBackend) CreateRuleGroup(
	ctx context.Context,
	name, scope, description, visibilityConfig string,
	capacity int64,
	rules []map[string]any,
	tags map[string]string,
) (*RuleGroup, error) {
	b.mu.Lock("CreateRuleGroup")
	defer b.mu.Unlock()

	region := storeRegion(scope, getRegion(ctx, b.region))

	if len(b.ruleGroupsByNameScope.Get(regionKey(region, nameScope(name, scope)))) > 0 {
		return nil, fmt.Errorf("%w: rule group %q already exists in scope %s", ErrRuleGroupAlreadyExists, name, scope)
	}

	id := uuid.NewString()
	arnStr := b.buildRuleGroupARN(name, id, scope, region)
	rg := &RuleGroup{
		ARN:              arnStr,
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
	b.ruleGroups.Put(rg)

	return cloneRuleGroup(rg), nil
}

// DeleteRuleGroup deletes a RuleGroup by ID, checking for WebACL references.
func (b *InMemoryBackend) DeleteRuleGroup(ctx context.Context, id, lockToken string) error {
	b.mu.Lock("DeleteRuleGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.lookupRuleGroupByID(region, id)
	if !ok {
		return fmt.Errorf("%w: rule group %q not found", ErrRuleGroupNotFound, id)
	}

	storeReg := regionFromARN(rg.ARN)

	if lockToken != "" && lockToken != rg.LockToken {
		return fmt.Errorf("%w: lock token mismatch for rule group %q", ErrOptimisticLock, id)
	}

	rgARN := rg.ARN

	for _, w := range b.webACLs.All() {
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

	b.ruleGroups.Delete(regionKey(storeReg, id))

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
func (b *InMemoryBackend) DeleteFirewallManagerRuleGroups(ctx context.Context, webACLARN string) (*WebACL, error) {
	b.mu.Lock("DeleteFirewallManagerRuleGroups")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	webACLID, ok := b.webACLIDByARNInRegion(webACLARN, region)
	if !ok {
		return nil, fmt.Errorf("%w: web ACL with ARN %q not found", ErrWebACLNotFound, webACLARN)
	}

	w, ok := b.webACLs.Get(regionKey(region, webACLID))
	if !ok {
		return nil, fmt.Errorf("%w: web ACL %q not found", ErrWebACLNotFound, webACLID)
	}

	w.LockToken = uuid.NewString()

	return cloneWebACL(w), nil
}

// PutLoggingConfiguration stores a full logging configuration JSON for the given resource ARN.
func (b *InMemoryBackend) PutLoggingConfiguration(
	ctx context.Context,
	resourceARN string,
	configJSON json.RawMessage,
) error {
	b.mu.Lock("PutLoggingConfiguration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	stored := make(json.RawMessage, len(configJSON))
	copy(stored, configJSON)
	b.loggingConfigsStore(region)[resourceARN] = stored

	return nil
}

// DeleteLoggingConfiguration removes the logging configuration for the given resource ARN.
func (b *InMemoryBackend) DeleteLoggingConfiguration(ctx context.Context, resourceARN string) error {
	b.mu.Lock("DeleteLoggingConfiguration")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	if _, exists := b.loggingConfigs[region][resourceARN]; !exists {
		return fmt.Errorf("%w: no logging configuration found for resource %q", ErrLoggingConfigNotFound, resourceARN)
	}

	delete(b.loggingConfigs[region], resourceARN)

	return nil
}

// GetLoggingConfiguration returns the stored logging configuration JSON for the given resource ARN.
func (b *InMemoryBackend) GetLoggingConfiguration(ctx context.Context, resourceARN string) (json.RawMessage, error) {
	b.mu.RLock("GetLoggingConfiguration")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	cfg, exists := b.loggingConfigs[region][resourceARN]
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
func (b *InMemoryBackend) ListLoggingConfigurations(ctx context.Context) []json.RawMessage {
	b.mu.RLock("ListLoggingConfigurations")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionMap := b.loggingConfigs[region]
	result := make([]json.RawMessage, 0, len(regionMap))

	for _, cfg := range regionMap {
		out := make(json.RawMessage, len(cfg))
		copy(out, cfg)
		result = append(result, out)
	}

	return result
}

// PutPermissionPolicy stores a permission policy for the given resource ARN.
func (b *InMemoryBackend) PutPermissionPolicy(ctx context.Context, resourceARN, policy string) error {
	b.mu.Lock("PutPermissionPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	b.permissionPoliciesStore(region)[resourceARN] = policy

	return nil
}

// DeletePermissionPolicy removes the permission policy for the given resource ARN.
func (b *InMemoryBackend) DeletePermissionPolicy(ctx context.Context, resourceARN string) error {
	b.mu.Lock("DeletePermissionPolicy")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	if _, ok := b.permissionPolicies[region][resourceARN]; !ok {
		return fmt.Errorf("%w: no permission policy found for resource %q", ErrPermissionPolicyNotFound, resourceARN)
	}

	delete(b.permissionPolicies[region], resourceARN)

	return nil
}

// GetRegexPatternSet returns a RegexPatternSet by ID.
func (b *InMemoryBackend) GetRegexPatternSet(ctx context.Context, id string) (*RegexPatternSet, error) {
	b.mu.RLock("GetRegexPatternSet")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	r, ok := b.lookupRegexPatternSetByID(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: regex pattern set %q not found", ErrRegexPatternSetNotFound, id)
	}

	return cloneRegexPatternSet(r), nil
}

// ListRegexPatternSets returns all RegexPatternSets sorted by name.
func (b *InMemoryBackend) ListRegexPatternSets(ctx context.Context) []*RegexPatternSet {
	b.mu.RLock("ListRegexPatternSets")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionSets := b.regexPatternSetsByRegion.Get(region)
	list := make([]*RegexPatternSet, 0, len(regionSets))

	for _, r := range regionSets {
		list = append(list, cloneRegexPatternSet(r))
	}

	if region != "" {
		for _, r := range b.regexPatternSetsByRegion.Get("") {
			list = append(list, cloneRegexPatternSet(r))
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateRegexPatternSet updates a RegexPatternSet by ID.
func (b *InMemoryBackend) UpdateRegexPatternSet(
	ctx context.Context,
	id, description, lockToken string,
	regularExpressionList []RegexEntry,
) (*RegexPatternSet, error) {
	b.mu.Lock("UpdateRegexPatternSet")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	r, ok := b.lookupRegexPatternSetByID(region, id)
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
func (b *InMemoryBackend) GetRuleGroup(ctx context.Context, id string) (*RuleGroup, error) {
	b.mu.RLock("GetRuleGroup")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.lookupRuleGroupByID(region, id)
	if !ok {
		return nil, fmt.Errorf("%w: rule group %q not found", ErrRuleGroupNotFound, id)
	}

	return cloneRuleGroup(rg), nil
}

// ListRuleGroups returns all RuleGroups sorted by name.
func (b *InMemoryBackend) ListRuleGroups(ctx context.Context) []*RuleGroup {
	b.mu.RLock("ListRuleGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionGroups := b.ruleGroupsByRegion.Get(region)
	list := make([]*RuleGroup, 0, len(regionGroups))

	for _, rg := range regionGroups {
		list = append(list, cloneRuleGroup(rg))
	}

	if region != "" {
		for _, rg := range b.ruleGroupsByRegion.Get("") {
			list = append(list, cloneRuleGroup(rg))
		}
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// UpdateRuleGroup updates a RuleGroup by ID.
func (b *InMemoryBackend) UpdateRuleGroup(
	ctx context.Context,
	id, description, visibilityConfig, lockToken string,
	rules []map[string]any,
) (*RuleGroup, error) {
	b.mu.Lock("UpdateRuleGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	rg, ok := b.lookupRuleGroupByID(region, id)
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
func (b *InMemoryBackend) ListAPIKeys(ctx context.Context, scope string) []*APIKey {
	b.mu.RLock("ListAPIKeys")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionKeys := b.apiKeysByRegion.Get(region)
	list := make([]*APIKey, 0, len(regionKeys))

	for _, a := range regionKeys {
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
func (b *InMemoryBackend) GetDecryptedAPIKey(ctx context.Context, scope, apiKey string) (*APIKey, error) {
	b.mu.RLock("GetDecryptedAPIKey")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	a, ok := b.apiKeys.Get(regionKey(region, apiKeyMapKey(scope, apiKey)))
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
func (b *InMemoryBackend) GetPermissionPolicy(ctx context.Context, resourceARN string) (string, error) {
	b.mu.RLock("GetPermissionPolicy")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	policy, ok := b.permissionPolicies[region][resourceARN]
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
func (b *InMemoryBackend) ListResourcesForWebACL(ctx context.Context, webACLARN string) ([]string, error) {
	b.mu.RLock("ListResourcesForWebACL")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	webACLID, ok := b.webACLIDByARNInRegion(webACLARN, region)
	if !ok {
		return nil, fmt.Errorf("%w: web ACL with ARN %q not found", ErrWebACLNotFound, webACLARN)
	}

	regionAssoc := b.associations[region]
	result := make([]string, 0, len(regionAssoc))

	for resourceARN, wID := range regionAssoc {
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
func (b *InMemoryBackend) GetManagedRuleSet(ctx context.Context, id string) (*ManagedRuleSet, error) {
	b.mu.RLock("GetManagedRuleSet")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	ms, ok := b.managedRuleSets.Get(regionKey(region, id))
	if !ok {
		return nil, fmt.Errorf("%w: managed rule set %q not found", ErrManagedRuleSetNotFound, id)
	}

	return cloneManagedRuleSet(ms), nil
}

// ListManagedRuleSets returns all managed rule sets sorted by name, optionally filtered by scope.
func (b *InMemoryBackend) ListManagedRuleSets(ctx context.Context, scope string) []*ManagedRuleSet {
	b.mu.RLock("ListManagedRuleSets")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	regionSets := b.managedRuleSetsByRegion.Get(region)
	list := make([]*ManagedRuleSet, 0, len(regionSets))

	for _, ms := range regionSets {
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
	ctx context.Context,
	id, name, scope, lockToken, recommendedVersion string,
	versionsToPublish map[string]any,
) (*ManagedRuleSet, error) {
	b.mu.Lock("PutManagedRuleSetVersions")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ms, exists := b.managedRuleSets.Get(regionKey(region, id))
	if exists && lockToken != "" && lockToken != ms.LockToken {
		return nil, fmt.Errorf("%w: lock token mismatch for managed rule set %q", ErrOptimisticLock, id)
	}

	if !exists {
		arnStr := b.buildManagedRuleSetARN(name, id, scope, region)
		ms = &ManagedRuleSet{
			ID:                id,
			Name:              name,
			Scope:             scope,
			ARN:               arnStr,
			LockToken:         uuid.NewString(),
			PublishedVersions: make(map[string]ManagedRuleSetVersion),
			Region:            region,
		}
		b.managedRuleSets.Put(ms)
	}

	for versionName, versionRaw := range versionsToPublish {
		version := ManagedRuleSetVersion{}
		fillVersionFromRaw(&version, versionRaw)

		ms.PublishedVersions[versionName] = version
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
	ctx context.Context,
	id, lockToken, versionToExpire string,
	expiryTimestamp *int64,
) (*ManagedRuleSet, error) {
	b.mu.Lock("UpdateManagedRuleSetVersionExpiryDate")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	ms, ok := b.managedRuleSets.Get(regionKey(region, id))
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

		maps.Copy(cp.PublishedVersions, ms.PublishedVersions)
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
