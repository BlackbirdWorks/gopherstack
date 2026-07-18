package wafv2

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"strings"

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
func scopePrefix(scope string) string {
	if scope == "CLOUDFRONT" {
		return "global"
	}

	return "regional"
}
func nameScope(name, scope string) string {
	return name + ":" + scope
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
