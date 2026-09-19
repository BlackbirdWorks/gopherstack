package opensearch

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// slLifecyclePolicyResourceTypeIndex is the only real ResourceType value
// (types/enums.go:187-193) -- reported on EffectiveLifecyclePolicyDetail,
// never accepted as request input.
const slLifecyclePolicyResourceTypeIndex = "index"

// ServerlessLifecyclePolicy is an OpenSearch Serverless lifecycle (retention)
// policy: same field set as ServerlessAccessPolicy/ServerlessEncryptionPolicy/
// ServerlessNetworkPolicy (policyLikeJR in handler_serverless_jsonrpc.go).
type ServerlessLifecyclePolicy struct {
	Description      string  `json:"description,omitempty"`
	Name             string  `json:"name"`
	Policy           string  `json:"policy"`
	PolicyVersion    string  `json:"policyVersion"`
	Type             string  `json:"type"`
	CreatedDate      float64 `json:"createdDate"`
	LastModifiedDate float64 `json:"lastModifiedDate"`
}

func serverlessLifecyclePolicyKey(policyType, name string) string {
	return "sl-lp:" + policyType + ":" + name
}

// lifecycleRule is one entry of a lifecycle policy document's "Rules" array
// (AOSS developer guide, "Creating data lifecycle policies": each rule names
// a ResourceType, one or more Resource patterns, and an optional
// MinIndexRetention duration string like "30d"/"24h"). Policy documents are
// otherwise opaque strings in this backend, same as every other serverless
// policy family -- this is parsed only for effective-policy resolution and
// the Resources list filter, never validated on Create/Update.
type lifecycleRule struct {
	ResourceType      string   `json:"ResourceType"`
	MinIndexRetention string   `json:"MinIndexRetention"`
	Resource          []string `json:"Resource"`
}

type lifecyclePolicyDocument struct {
	Rules []lifecycleRule `json:"Rules"`
}

// parseLifecycleRules best-effort decodes a lifecycle policy's JSON body. An
// unparseable or empty document simply matches nothing -- it is never a
// backend error, since Create/Update never validate the policy body's shape
// (matching every other serverless policy family in this package).
func parseLifecycleRules(policy string) []lifecycleRule {
	var doc lifecyclePolicyDocument
	if err := json.Unmarshal([]byte(policy), &doc); err != nil {
		return nil
	}

	return doc.Rules
}

// matchLifecycleResourcePattern reports whether pattern matches resource,
// returning the literal (non-wildcard) prefix matched -- its length is the
// pattern's specificity for "most specific wins" resolution. Only a single
// trailing "*" wildcard is documented for lifecycle policy resource
// patterns.
func matchLifecycleResourcePattern(pattern, resource string) (string, bool) {
	if prefix, hasStar := strings.CutSuffix(pattern, "*"); hasStar {
		if strings.HasPrefix(resource, prefix) {
			return prefix, true
		}

		return "", false
	}

	if pattern == resource {
		return pattern, true
	}

	return "", false
}

// CreateServerlessLifecyclePolicy creates a new lifecycle (retention) policy.
func (b *InMemoryBackend) CreateServerlessLifecyclePolicy(
	policyType, name, description, policy string,
) (*ServerlessLifecyclePolicy, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	if policy == "" {
		return nil, fmt.Errorf("%w: Policy is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateServerlessLifecyclePolicy")
	defer b.mu.Unlock()

	key := serverlessLifecyclePolicyKey(policyType, name)
	if b.slLifecyclePolicies.Has(key) {
		return nil, fmt.Errorf(
			"%w: lifecycle policy %s already exists",
			ErrApplicationAlreadyExists,
			name,
		)
	}

	now := float64(time.Now().Unix())
	lp := &ServerlessLifecyclePolicy{
		Name:             name,
		Type:             policyType,
		Policy:           policy,
		Description:      description,
		PolicyVersion:    defaultSamlSessionTimeoutB64,
		CreatedDate:      now,
		LastModifiedDate: now,
	}

	b.slLifecyclePolicies.Put(lp)

	cp := *lp

	return &cp, nil
}

// UpdateServerlessLifecyclePolicy updates an existing lifecycle policy,
// enforcing optimistic concurrency on PolicyVersion (real AOSS
// UpdateLifecyclePolicy declares ConflictException for exactly this case,
// deserializers.go awsAwsjson10_deserializeOpErrorUpdateLifecyclePolicy).
func (b *InMemoryBackend) UpdateServerlessLifecyclePolicy(
	policyType, name, description, policy, policyVersion string,
) (*ServerlessLifecyclePolicy, error) {
	if policyVersion == "" {
		return nil, fmt.Errorf("%w: PolicyVersion is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateServerlessLifecyclePolicy")
	defer b.mu.Unlock()

	lp, ok := b.slLifecyclePolicies.Get(serverlessLifecyclePolicyKey(policyType, name))
	if !ok {
		return nil, fmt.Errorf("%w: lifecycle policy %s not found", ErrApplicationNotFound, name)
	}

	if lp.PolicyVersion != policyVersion {
		return nil, fmt.Errorf(
			"%w: policyVersion %s is stale for lifecycle policy %s",
			ErrServerlessPolicyVersionConflict,
			policyVersion,
			name,
		)
	}

	if description != "" {
		lp.Description = description
	}

	if policy != "" {
		lp.Policy = policy
	}

	lp.LastModifiedDate = float64(time.Now().Unix())
	lp.PolicyVersion = fmt.Sprintf("v%d", time.Now().UnixMilli())

	cp := *lp

	return &cp, nil
}

// DeleteServerlessLifecyclePolicy removes a lifecycle policy by type and name.
func (b *InMemoryBackend) DeleteServerlessLifecyclePolicy(policyType, name string) error {
	b.mu.Lock("DeleteServerlessLifecyclePolicy")
	defer b.mu.Unlock()

	key := serverlessLifecyclePolicyKey(policyType, name)
	if !b.slLifecyclePolicies.Has(key) {
		return fmt.Errorf("%w: lifecycle policy %s not found", ErrApplicationNotFound, name)
	}

	b.slLifecyclePolicies.Delete(key)

	return nil
}

// ListServerlessLifecyclePolicies lists lifecycle policies of a type,
// optionally filtered to those whose policy document names at least one of
// the given resource patterns verbatim. The real ListLifecyclePolicies
// "Resources" filter is documented only as "Resource filters that policies
// can apply to" (types.go:1087-1094) with no further schema published; exact
// pattern-string membership is the most literal reading available, not
// import behaviour that has ever been observed against real AWS.
func (b *InMemoryBackend) ListServerlessLifecyclePolicies(
	policyType string,
	resources []string,
) []*ServerlessLifecyclePolicy {
	b.mu.RLock("ListServerlessLifecyclePolicies")
	defer b.mu.RUnlock()

	resourceFilter := make(map[string]bool, len(resources))
	for _, r := range resources {
		resourceFilter[r] = true
	}

	var out []*ServerlessLifecyclePolicy

	for _, lp := range b.slLifecyclePolicies.All() {
		if policyType != "" && lp.Type != policyType {
			continue
		}

		if len(resourceFilter) > 0 && !lifecyclePolicyMatchesFilter(lp, resourceFilter) {
			continue
		}

		cp := *lp
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

func lifecyclePolicyMatchesFilter(
	lp *ServerlessLifecyclePolicy,
	resourceFilter map[string]bool,
) bool {
	for _, rule := range parseLifecycleRules(lp.Policy) {
		for _, res := range rule.Resource {
			if resourceFilter[res] {
				return true
			}
		}
	}

	return false
}

// serverlessLifecyclePolicyIdentifier is BatchGetLifecyclePolicy's per-item
// request identifier (Name/Type -- LifecyclePolicyIdentifier, types.go:684-698).
type serverlessLifecyclePolicyIdentifier struct {
	Name string
	Type string
}

// ServerlessLifecyclePolicyError is a not-found entry for BatchGetLifecyclePolicy.
type ServerlessLifecyclePolicyError struct {
	Name, Type, ErrorCode, ErrorMessage string
}

// BatchGetServerlessLifecyclePolicies resolves a list of Name/Type
// identifiers, splitting hits from misses like BatchGetVpcEndpoint's
// DescribeVpcEndpoints/BatchGetServerlessVpcEndpoints do.
func (b *InMemoryBackend) BatchGetServerlessLifecyclePolicies(
	identifiers []serverlessLifecyclePolicyIdentifier,
) ([]*ServerlessLifecyclePolicy, []ServerlessLifecyclePolicyError) {
	b.mu.RLock("BatchGetServerlessLifecyclePolicies")
	defer b.mu.RUnlock()

	var found []*ServerlessLifecyclePolicy

	var errs []ServerlessLifecyclePolicyError

	for _, id := range identifiers {
		lp, ok := b.slLifecyclePolicies.Get(serverlessLifecyclePolicyKey(id.Type, id.Name))
		if !ok {
			errs = append(errs, ServerlessLifecyclePolicyError{
				Name:      id.Name,
				Type:      id.Type,
				ErrorCode: slErrorCodeNotFound,
				ErrorMessage: fmt.Sprintf(
					"The specified Lifecycle Policy %s is not found",
					id.Name,
				),
			})

			continue
		}

		cp := *lp
		found = append(found, &cp)
	}

	return found, errs
}

// ServerlessEffectiveLifecyclePolicyResult is one resolved entry for
// BatchGetEffectiveLifecyclePolicy.
type ServerlessEffectiveLifecyclePolicyResult struct {
	PolicyName           string
	Resource             string
	ResourceType         string
	Type                 string
	RetentionPeriod      string
	NoMinRetentionPeriod bool
}

// ServerlessEffectiveLifecyclePolicyErr is a resolution-failure entry for
// BatchGetEffectiveLifecyclePolicy.
type ServerlessEffectiveLifecyclePolicyErr struct {
	Resource, Type, ErrorCode, ErrorMessage string
}

// BatchGetServerlessEffectiveLifecyclePolicies resolves, for each
// (resource, type) identifier, the retention policy whose Rules best match
// resource: patterns ending in "*" match by prefix, everything else matches
// exactly, and the match with the longest literal prefix wins (most
// specific); ties break on the lexically smallest policy name for
// determinism, since store iteration order is not stable.
func (b *InMemoryBackend) BatchGetServerlessEffectiveLifecyclePolicies(
	identifiers []serverlessLifecyclePolicyIdentifier,
) ([]ServerlessEffectiveLifecyclePolicyResult, []ServerlessEffectiveLifecyclePolicyErr) {
	b.mu.RLock("BatchGetServerlessEffectiveLifecyclePolicies")
	defer b.mu.RUnlock()

	policies := make([]*ServerlessLifecyclePolicy, 0, b.slLifecyclePolicies.Len())
	policies = append(policies, b.slLifecyclePolicies.All()...)

	var found []ServerlessEffectiveLifecyclePolicyResult

	var errs []ServerlessEffectiveLifecyclePolicyErr

	for _, id := range identifiers {
		result, ok := resolveEffectiveLifecyclePolicy(policies, id.Type, id.Name)
		if !ok {
			errs = append(errs, ServerlessEffectiveLifecyclePolicyErr{
				Resource:     id.Name,
				Type:         id.Type,
				ErrorCode:    slErrorCodeNotFound,
				ErrorMessage: fmt.Sprintf("No lifecycle policy applies to resource %s", id.Name),
			})

			continue
		}

		found = append(found, result)
	}

	return found, errs
}

// effectiveLifecycleCandidate pairs a resolved match with the literal prefix
// length it matched on, so callers can compare specificity across policies.
type effectiveLifecycleCandidate struct {
	result    ServerlessEffectiveLifecyclePolicyResult
	prefixLen int
}

// bestRuleMatch returns lp's single most specific match against resource, if
// any of its Rules match at all.
func bestRuleMatch(
	lp *ServerlessLifecyclePolicy,
	policyType, resource string,
) (effectiveLifecycleCandidate, bool) {
	best := -1

	var out ServerlessEffectiveLifecyclePolicyResult

	for _, rule := range parseLifecycleRules(lp.Policy) {
		for _, pattern := range rule.Resource {
			prefix, ok := matchLifecycleResourcePattern(pattern, resource)
			if !ok || len(prefix) <= best {
				continue
			}

			best = len(prefix)
			out = ServerlessEffectiveLifecyclePolicyResult{
				PolicyName:           lp.Name,
				Resource:             resource,
				ResourceType:         slLifecyclePolicyResourceTypeIndex,
				Type:                 policyType,
				RetentionPeriod:      rule.MinIndexRetention,
				NoMinRetentionPeriod: rule.MinIndexRetention == "",
			}
		}
	}

	if best < 0 {
		return effectiveLifecycleCandidate{}, false
	}

	return effectiveLifecycleCandidate{result: out, prefixLen: best}, true
}

func resolveEffectiveLifecyclePolicy(
	policies []*ServerlessLifecyclePolicy, policyType, resource string,
) (ServerlessEffectiveLifecyclePolicyResult, bool) {
	var best effectiveLifecycleCandidate

	found := false

	for _, lp := range policies {
		if lp.Type != policyType {
			continue
		}

		cand, ok := bestRuleMatch(lp, policyType, resource)
		if !ok {
			continue
		}

		if found && (cand.prefixLen < best.prefixLen ||
			(cand.prefixLen == best.prefixLen && cand.result.PolicyName >= best.result.PolicyName)) {
			continue
		}

		best = cand
		found = true
	}

	return best.result, found
}
