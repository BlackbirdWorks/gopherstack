package azurearm

import (
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"
)

const (
	policyEffectDeny   = "deny"
	policyEffectAudit  = "audit"
	policyEffectAppend = "append"
	policyEffectModify = "modify"
	policyMaxStates    = 5000
	policyFieldTags    = "tags"
)

var (
	errPolicyUnsupported = errors.New("azurearm: policy expression not evaluable")
	errPolicyMissing     = errors.New("azurearm: policy field has no value")
)

// policyResource is the resource a write request creates or updates, as the policy rule fields see it.
type policyResource struct {
	body     map[string]any
	id       string
	typ      string
	name     string
	location string
	kind     string
}

// storedPolicyState records the latest evaluation of one resource against one assigned definition.
type storedPolicyState struct {
	Time         time.Time `json:"time"`
	ResourceID   string    `json:"resourceId"`
	ResourceType string    `json:"resourceType"`
	AssignmentID string    `json:"policyAssignmentId"`
	DefinitionID string    `json:"policyDefinitionId"`
	Effect       string    `json:"effect"`
	Compliant    bool      `json:"compliant"`
}

// PolicyHit names the assignment and definition behind a policy decision.
type PolicyHit struct {
	AssignmentID string
	DefinitionID string
	Effect       string
}

// PolicyOutcome is the result of evaluating the assignments in force for a write.
type PolicyOutcome struct {
	Deny *PolicyHit
	Body map[string]any
}

type policyEnv struct {
	params map[string]any
	res    policyResource
}

// EvaluatePolicies evaluates every enforced policy assignment covering the resource against the write. Rules that read
// anything other than type, name, id, location, kind and tags (aliases, request context, resource group functions) are
// not evaluable and are skipped. Deny stops the evaluation; modify and append change the returned body; every
// evaluated definition leaves a compliance state.
func (p *AuthorizationProvider) EvaluatePolicies(res policyResource) PolicyOutcome {
	p.mu.Lock("EvaluatePolicies")
	defer p.mu.Unlock()

	out := PolicyOutcome{Body: res.body}

	for _, a := range p.policyAssigns {
		if !scopeWithin(res.id, a.Scope) || strOf(a.Props, "enforcementMode") == policyEnforceNone ||
			p.exempted(a, res.id) {
			continue
		}

		for _, def := range p.assignedRules(a) {
			hit, applied := p.evaluateRule(a, def, &res)
			if applied == nil {
				continue
			}

			p.recordState(res, a, def.id, hit.Effect, !applied.matched)

			if !applied.matched {
				continue
			}

			if hit.Effect == policyEffectDeny {
				out.Deny = &hit

				return out
			}

			out.Body = res.body
		}
	}

	return out
}

type policyRuleRef struct {
	rule   map[string]any
	params map[string]any
	id     string
}

type policyApplied struct{ matched bool }

func (p *AuthorizationProvider) exempted(a *storedPolicyAssign, resourceID string) bool {
	id := a.Scope + "/providers/Microsoft.Authorization/policyAssignments/" + a.Name

	for _, e := range p.policyExempts {
		if !strings.EqualFold(strOf(e.Props, authFieldPolicyAssignID), id) || !scopeWithin(resourceID, e.Scope) {
			continue
		}

		if exp, err := time.Parse(time.RFC3339, strOf(e.Props, authFieldExpiresOn)); err == nil && exp.Before(p.now()) {
			continue
		}

		return true
	}

	return false
}

// assignedRules resolves an assignment to the definition rules it applies, expanding initiatives.
func (p *AuthorizationProvider) assignedRules(a *storedPolicyAssign) []policyRuleRef {
	defID := strOf(a.Props, authFieldPolicyDefinitionID)

	kind, props, ok := p.lookupPolicy(defID, a.Scope)
	if !ok {
		return nil
	}

	given := mapOf(a.Props[authFieldParameters])

	if kind == authPolKindDef {
		return []policyRuleRef{
			{
				id:     defID,
				rule:   mapOf(props["policyRule"]),
				params: resolveParams(mapOf(props[authFieldParameters]), given),
			},
		}
	}

	var out []policyRuleRef

	for _, r := range anyList(props["policyDefinitions"]) {
		ref := mapOf(r)
		id := strOf(ref, authFieldPolicyDefinitionID)

		if _, dp, found := p.lookupPolicy(id, a.Scope); found {
			bound := maps.Clone(mapOf(ref[authFieldParameters]))

			out = append(out, policyRuleRef{
				id: id, rule: mapOf(dp["policyRule"]), params: resolveParams(mapOf(dp[authFieldParameters]), bound),
			})
		}
	}

	return out
}

// resolveParams merges definition defaults with the given values; a value of the form
// [parameters('x')] in an initiative reference refers to the set's own parameter and is left unresolved.
func resolveParams(defs, given map[string]any) map[string]any {
	out := map[string]any{}

	for name, d := range defs {
		if v, has := mapOf(d)["defaultValue"]; has {
			out[name] = v
		}
	}

	for name, v := range given {
		if val, has := mapOf(v)["value"]; has {
			out[name] = val
		}
	}

	return out
}

func (p *AuthorizationProvider) evaluateRule(
	a *storedPolicyAssign,
	def policyRuleRef,
	res *policyResource,
) (PolicyHit, *policyApplied) {
	hit := PolicyHit{
		AssignmentID: a.Scope + "/providers/Microsoft.Authorization/policyAssignments/" + a.Name,
		DefinitionID: def.id,
	}
	env := &policyEnv{params: def.params, res: *res}

	matched, err := evalPolicyCond(mapOf(def.rule["if"]), env)
	if err != nil {
		return hit, nil
	}

	then := mapOf(def.rule["then"])

	eff, err := evalPolicyValue(then["effect"], env)
	if err != nil {
		return hit, nil
	}

	hit.Effect = strings.ToLower(fmt.Sprint(eff))

	if matched {
		switch hit.Effect {
		case policyEffectAppend:
			applyAppend(res, then)
		case policyEffectModify:
			applyModify(res, then, env)
		}
	}

	return hit, &policyApplied{matched: matched}
}

func (p *AuthorizationProvider) recordState(
	res policyResource,
	a *storedPolicyAssign,
	defID, effect string,
	compliant bool,
) {
	switch effect {
	case policyEffectDeny, policyEffectAudit, policyEffectAppend, policyEffectModify:
	default:
		return
	}

	if p.policyStates == nil {
		p.policyStates = map[string]*storedPolicyState{}
	}

	aid := a.Scope + "/providers/Microsoft.Authorization/policyAssignments/" + a.Name
	key := strings.ToLower(res.id + "|" + aid + "|" + defID)

	if _, exists := p.policyStates[key]; !exists && len(p.policyStates) >= policyMaxStates {
		return
	}

	p.policyStates[key] = &storedPolicyState{
		Time: p.now().UTC(), ResourceID: res.id, ResourceType: res.typ, AssignmentID: aid, DefinitionID: defID,
		Effect: effect, Compliant: compliant,
	}
}

func evalPolicyCond(c map[string]any, env *policyEnv) (bool, error) {
	switch {
	case c["allOf"] != nil:
		return evalPolicyGroup(anyList(c["allOf"]), env, true)
	case c["anyOf"] != nil:
		return evalPolicyGroup(anyList(c["anyOf"]), env, false)
	case c["not"] != nil:
		v, err := evalPolicyCond(mapOf(c["not"]), env)

		return !v, err
	}

	return evalPolicyLeaf(c, env)
}

func evalPolicyGroup(items []any, env *policyEnv, all bool) (bool, error) {
	for _, it := range items {
		v, err := evalPolicyCond(mapOf(it), env)
		if err != nil {
			return false, err
		}

		if v != all {
			return v, nil
		}
	}

	return all, nil
}

var policyOperators = []string{ //nolint:gochecknoglobals // static operator list
	"equals", "notEquals", opLike, "notLike", opMatch, "notMatch", "matchInsensitively", "notMatchInsensitively",
	opContains, "notContains", "in", "notIn", opContainsKey, "notContainsKey", "less", "lessOrEquals", "greater",
	"greaterOrEquals", "exists",
}

func evalPolicyLeaf(c map[string]any, env *policyEnv) (bool, error) {
	var (
		left any
		err  error
	)

	switch {
	case c["field"] != nil:
		left, err = policyField(strOfAny(c["field"]), env)
		if errors.Is(err, errPolicyMissing) {
			left, err = nil, nil
		}
	case c["value"] != nil:
		left, err = evalPolicyValue(c["value"], env)
	default:
		return false, errPolicyUnsupported
	}

	if err != nil {
		return false, err
	}

	for _, op := range policyOperators {
		raw, has := c[op]
		if !has {
			continue
		}

		right, rerr := evalPolicyValue(raw, env)
		if rerr != nil {
			return false, rerr
		}

		return comparePolicy(op, left, right)
	}

	return false, errPolicyUnsupported
}

func strOfAny(v any) string {
	s, _ := v.(string)

	return s
}

func policyField(name string, env *policyEnv) (any, error) {
	if strings.HasPrefix(name, "[") {
		v, err := evalPolicyValue(name, env)
		if err != nil {
			return nil, err
		}

		name = fmt.Sprint(v)
	}

	res := env.res
	lower := strings.ToLower(name)

	switch lower {
	case fieldType:
		return res.typ, nil
	case fieldName, "fullname":
		return res.name, nil
	case "id":
		return res.id, nil
	case "location":
		return res.location, nil
	case "kind":
		return nilIfEmpty(res.kind), nil
	case policyFieldTags:
		return tagMap(res.body), nil
	}

	if tag, ok := tagFieldName(name); ok {
		for k, v := range tagMap(res.body) {
			if strings.EqualFold(k, tag) {
				return v, nil
			}
		}

		return nil, errPolicyMissing
	}

	return nil, errPolicyUnsupported
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}

	return s
}

func tagMap(body map[string]any) map[string]any {
	return mapOf(body[policyFieldTags])
}

func tagFieldName(field string) (string, bool) {
	lower := strings.ToLower(field)

	switch {
	case strings.HasPrefix(lower, "tags[") && strings.HasSuffix(field, "]"):
		return strings.Trim(field[len("tags["):len(field)-1], "'\""), true
	case strings.HasPrefix(lower, "tags."):
		return field[len("tags."):], true
	}

	return "", false
}

// evalPolicyValue evaluates a literal or a bracketed template expression with parameters(), concat() and field().
func evalPolicyValue(v any, env *policyEnv) (any, error) {
	s, ok := v.(string)
	if !ok || !strings.HasPrefix(s, "[") || !strings.HasSuffix(s, "]") || strings.HasPrefix(s, "[[") {
		return v, nil
	}

	val, rest, err := parsePolicyExpr(strings.TrimSpace(s[1:len(s)-1]), env)
	if err != nil || strings.TrimSpace(rest) != "" {
		return nil, errPolicyUnsupported
	}

	return val, nil
}

func parsePolicyExpr(s string, env *policyEnv) (any, string, error) {
	s = strings.TrimSpace(s)

	if strings.HasPrefix(s, "'") {
		end := strings.Index(s[1:], "'")
		if end < 0 {
			return nil, "", errPolicyUnsupported
		}

		return s[1 : end+1], s[end+2:], nil
	}

	if n, err := strconv.ParseFloat(leadingNumber(s), 64); err == nil && leadingNumber(s) != "" {
		return n, s[len(leadingNumber(s)):], nil
	}

	open := strings.Index(s, "(")
	if open < 0 {
		return nil, "", errPolicyUnsupported
	}

	fn := strings.ToLower(strings.TrimSpace(s[:open]))
	args, rest, err := parsePolicyArgs(s[open+1:], env)

	if err != nil {
		return nil, "", err
	}

	val, err := callPolicyFunc(fn, args, env)

	return val, rest, err
}

func leadingNumber(s string) string {
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}

	return s[:i]
}

func parsePolicyArgs(s string, env *policyEnv) ([]any, string, error) {
	var args []any

	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, ")") {
		return args, s[1:], nil
	}

	for {
		v, rest, err := parsePolicyExpr(s, env)
		if err != nil {
			return nil, "", err
		}

		args = append(args, v)
		s = strings.TrimSpace(rest)

		switch {
		case strings.HasPrefix(s, ","):
			s = s[1:]
		case strings.HasPrefix(s, ")"):
			return args, s[1:], nil
		default:
			return nil, "", errPolicyUnsupported
		}
	}
}

func callPolicyFunc(fn string, args []any, env *policyEnv) (any, error) {
	switch fn {
	case "parameters":
		if len(args) == 1 {
			for k, v := range env.params {
				if strings.EqualFold(k, fmt.Sprint(args[0])) {
					return v, nil
				}
			}
		}
	case "concat":
		var sb strings.Builder

		for _, a := range args {
			fmt.Fprint(&sb, a)
		}

		return sb.String(), nil
	case "field":
		if len(args) == 1 {
			return policyField(fmt.Sprint(args[0]), env)
		}
	}

	return nil, errPolicyUnsupported
}
