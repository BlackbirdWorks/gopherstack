package gcpdns

import (
	"fmt"
	"slices"
	"strings"

	dnsapi "google.golang.org/api/dns/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	respPath   = "entity.responsePolicy"
	rulePath   = "entity.responsePolicyRule"
	maxRules   = 10000
	bypassRule = "bypassResponsePolicy"
)

func validateResponsePolicy(p *dnsapi.ResponsePolicy) error {
	if p.ResponsePolicyName == "" {
		return errRequired(respPath + ".responsePolicyName")
	}

	if !policyNameRE.MatchString(p.ResponsePolicyName) {
		return errInvalidValue(respPath+".responsePolicyName", p.ResponsePolicyName)
	}

	if err := validateLabels(p.Labels, respPath); err != nil {
		return err
	}

	p.Kind = "dns#responsePolicy"

	for i, n := range p.Networks {
		n.Kind = "dns#responsePolicyNetwork"

		if strings.TrimSpace(n.NetworkUrl) == "" {
			return errRequired(fmt.Sprintf("%s.networks[%d].networkUrl", respPath, i))
		}
	}

	for _, g := range p.GkeClusters {
		g.Kind = "dns#responsePolicyGKECluster"
	}

	return nil
}

func validateRuleNames(r *dnsapi.ResponsePolicyRule) error {
	if r.RuleName == "" {
		return errRequired(rulePath + ".ruleName")
	}

	if !policyNameRE.MatchString(r.RuleName) {
		return errInvalidValue(rulePath+".ruleName", r.RuleName)
	}

	if r.DnsName == "" {
		return errRequired(rulePath + ".dnsName")
	}

	if !strings.HasSuffix(r.DnsName, ".") {
		return errInvalidValue(rulePath+".dnsName", r.DnsName)
	}

	return nil
}

func validateRule(r *dnsapi.ResponsePolicyRule) error {
	if err := validateRuleNames(r); err != nil {
		return err
	}

	r.DnsName = strings.ToLower(r.DnsName)
	r.Kind = "dns#responsePolicyRule"

	hasData := r.LocalData != nil && len(r.LocalData.LocalDatas) > 0

	switch {
	case hasData == (r.Behavior != "" && r.Behavior != "behaviorUnspecified"):
		return errInvalid("Invalid value for '%s': exactly one of localData or behavior is required", rulePath)
	case r.Behavior != "" && r.Behavior != bypassRule && r.Behavior != "behaviorUnspecified":
		return errInvalidValue(rulePath+".behavior", r.Behavior)
	}

	if !hasData {
		return nil
	}

	return validateLocalData(r.LocalData.LocalDatas)
}

func validateLocalData(sets []*dnsapi.ResourceRecordSet) error {
	for i, rr := range sets {
		path := fmt.Sprintf("%s.localData.localDatas[%d]", rulePath, i)
		rr.Kind, rr.Name, rr.Type = rrsetKind, canon(rr.Name), strings.ToUpper(rr.Type)

		if rr.Ttl == 0 {
			rr.Ttl = defaultTTL
		}

		if len(rr.Rrdatas) == 0 {
			return errRequired(path + ".rrdata")
		}

		if err := validateRData(rr.Name, rr.Type, rr.Ttl, rr.Rrdatas, path); err != nil {
			return err
		}
	}

	return nil
}

func (b *InMemoryBackend) respLocked(project, name string) (*responseRec, error) {
	rec, ok := b.respPols[key(project, name)]
	if !ok {
		return nil, errNotFound("parameters.responsePolicy", name)
	}

	return rec, nil
}

// CreateResponsePolicy stores a response policy.
func (b *InMemoryBackend) CreateResponsePolicy(
	project string,
	in *dnsapi.ResponsePolicy,
) (*dnsapi.ResponsePolicy, error) {
	p := clone(in)
	if err := validateResponsePolicy(p); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateResponsePolicy")
	defer b.mu.Unlock()

	if _, ok := b.respPols[key(project, p.ResponsePolicyName)]; ok {
		return nil, errExists(respPath, p.ResponsePolicyName)
	}

	p.Id = int64(newID()) //nolint:gosec // newID is bounded below MaxInt64
	b.respPols[key(project, p.ResponsePolicyName)] = &responseRec{
		Policy: p, Rules: map[string]*dnsapi.ResponsePolicyRule{},
	}

	return clone(p), nil
}

// GetResponsePolicy returns one response policy.
func (b *InMemoryBackend) GetResponsePolicy(project, name string) (*dnsapi.ResponsePolicy, error) {
	b.mu.RLock("GetResponsePolicy")
	defer b.mu.RUnlock()

	rec, err := b.respLocked(project, name)
	if err != nil {
		return nil, err
	}

	return clone(rec.Policy), nil
}

// ListResponsePolicies returns a page of the project's response policies.
func (b *InMemoryBackend) ListResponsePolicies(
	project, token string, limit int,
) (page.Page[*dnsapi.ResponsePolicy], error) {
	if err := page.ValidateToken(token); err != nil {
		return page.Page[*dnsapi.ResponsePolicy]{}, errInvalidValue("pageToken", token)
	}

	b.mu.RLock("ListResponsePolicies")
	defer b.mu.RUnlock()

	var all []*dnsapi.ResponsePolicy

	for k, rec := range b.respPols {
		if strings.HasPrefix(k, project+"/") {
			all = append(all, clone(rec.Policy))
		}
	}

	slices.SortFunc(all, func(a, c *dnsapi.ResponsePolicy) int {
		return strings.Compare(a.ResponsePolicyName, c.ResponsePolicyName)
	})

	return page.New(all, token, limit, pageSize), nil
}

// DeleteResponsePolicy removes a response policy with no rules and no networks.
func (b *InMemoryBackend) DeleteResponsePolicy(project, name string) error {
	b.mu.Lock("DeleteResponsePolicy")
	defer b.mu.Unlock()

	rec, err := b.respLocked(project, name)
	if err != nil {
		return err
	}

	if len(rec.Rules) > 0 || len(rec.Policy.Networks) > 0 || len(rec.Policy.GkeClusters) > 0 {
		return errNotEmpty(
			"The response policy '%s' cannot be deleted while it has rules or is attached to networks.",
			name,
		)
	}

	delete(b.respPols, key(project, name))

	return nil
}

// UpdateResponsePolicy replaces (in) or merges (patch) a response policy.
func (b *InMemoryBackend) UpdateResponsePolicy(
	project, name string, in *dnsapi.ResponsePolicy, patch map[string]any,
) (*dnsapi.ResponsePolicy, error) {
	b.mu.Lock("UpdateResponsePolicy")
	defer b.mu.Unlock()

	rec, err := b.respLocked(project, name)
	if err != nil {
		return nil, err
	}

	np := clone(in)

	if patch != nil {
		if np, err = patched(rec.Policy, patch); err != nil {
			return nil, err
		}
	}

	if np.ResponsePolicyName != "" && np.ResponsePolicyName != name {
		return nil, errInvalidValue(respPath+".responsePolicyName", np.ResponsePolicyName)
	}

	np.ResponsePolicyName, np.Id = name, rec.Policy.Id

	if err = validateResponsePolicy(np); err != nil {
		return nil, err
	}

	rec.Policy = np

	return clone(np), nil
}

// CreateRule stores a response policy rule.
func (b *InMemoryBackend) CreateRule(
	project, policy string,
	in *dnsapi.ResponsePolicyRule,
) (*dnsapi.ResponsePolicyRule, error) {
	r := clone(in)
	if err := validateRule(r); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateRule")
	defer b.mu.Unlock()

	rec, err := b.respLocked(project, policy)
	if err != nil {
		return nil, err
	}

	if _, ok := rec.Rules[r.RuleName]; ok {
		return nil, errExists(rulePath, r.RuleName)
	}

	if len(rec.Rules) >= maxRules {
		return nil, errQuota("Quota 'RESPONSE_POLICY_RULES_PER_RESPONSE_POLICY' exceeded.")
	}

	rec.Rules[r.RuleName] = r

	return clone(r), nil
}

// GetRule returns one rule.
func (b *InMemoryBackend) GetRule(project, policy, name string) (*dnsapi.ResponsePolicyRule, error) {
	b.mu.RLock("GetRule")
	defer b.mu.RUnlock()

	rec, err := b.respLocked(project, policy)
	if err != nil {
		return nil, err
	}

	r, ok := rec.Rules[name]
	if !ok {
		return nil, errNotFound("parameters.responsePolicyRule", name)
	}

	return clone(r), nil
}

// ListRules returns a page of a response policy's rules ordered by name.
func (b *InMemoryBackend) ListRules(
	project, policy, token string, limit int,
) (page.Page[*dnsapi.ResponsePolicyRule], error) {
	var none page.Page[*dnsapi.ResponsePolicyRule]

	if err := page.ValidateToken(token); err != nil {
		return none, errInvalidValue("pageToken", token)
	}

	b.mu.RLock("ListRules")
	defer b.mu.RUnlock()

	rec, err := b.respLocked(project, policy)
	if err != nil {
		return none, err
	}

	all := make([]*dnsapi.ResponsePolicyRule, 0, len(rec.Rules))
	for _, r := range rec.Rules {
		all = append(all, clone(r))
	}

	slices.SortFunc(all, func(a, c *dnsapi.ResponsePolicyRule) int { return strings.Compare(a.RuleName, c.RuleName) })

	return page.New(all, token, limit, pageSize), nil
}

// DeleteRule removes a rule.
func (b *InMemoryBackend) DeleteRule(project, policy, name string) error {
	b.mu.Lock("DeleteRule")
	defer b.mu.Unlock()

	rec, err := b.respLocked(project, policy)
	if err != nil {
		return err
	}

	if _, ok := rec.Rules[name]; !ok {
		return errNotFound("parameters.responsePolicyRule", name)
	}

	delete(rec.Rules, name)

	return nil
}

// UpdateRule replaces (in) or merges (patch) a rule.
func (b *InMemoryBackend) UpdateRule(
	project, policy, name string, in *dnsapi.ResponsePolicyRule, patch map[string]any,
) (*dnsapi.ResponsePolicyRule, error) {
	b.mu.Lock("UpdateRule")
	defer b.mu.Unlock()

	rec, err := b.respLocked(project, policy)
	if err != nil {
		return nil, err
	}

	cur, ok := rec.Rules[name]
	if !ok {
		return nil, errNotFound("parameters.responsePolicyRule", name)
	}

	nr := clone(in)

	if patch != nil {
		if nr, err = patched(cur, patch); err != nil {
			return nil, err
		}
	}

	if nr.RuleName != "" && nr.RuleName != name {
		return nil, errInvalidValue(rulePath+".ruleName", nr.RuleName)
	}

	nr.RuleName = name

	if err = validateRule(nr); err != nil {
		return nil, err
	}

	rec.Rules[name] = nr

	return clone(nr), nil
}
