package cloudformation

import (
	"maps"
	"regexp"
	"slices"
	"strings"
)

const (
	samAPIGWPolicy      = "x-amazon-apigateway-policy"
	samInvokeAction     = "execute-api:Invoke"
	samKeyCustomStmts   = "CustomStatements"
	samCondIPAddress    = "IpAddress"
	samCondNotIPAddress = "NotIpAddress"
	samKeyAwsPrincipal  = "AWS"
	samKeySourceVpc     = "aws:SourceVpc"
	samKeySourceVpce    = "aws:SourceVpce"
)

var samPathParamRe = regexp.MustCompile(`\{([a-zA-Z0-9._-]+|[a-zA-Z0-9._-]+\+|proxy\+)\}`)

func samResourcePolicyKeys() map[string]bool {
	return keySet("AwsAccountWhitelist", "AwsAccountBlacklist", "IpRangeWhitelist", "IpRangeBlacklist",
		"SourceVpcWhitelist", "SourceVpcBlacklist", "IntrinsicVpcWhitelist", "IntrinsicVpceWhitelist",
		"IntrinsicVpcBlacklist", "IntrinsicVpceBlacklist", samKeyCustomStmts)
}

// applyResourcePolicy renders Auth.ResourcePolicy into x-amazon-apigateway-policy like SAM's SwaggerEditor.
func (a *samAPI) applyResourcePolicy(doc map[string]any) error {
	raw, has := asMap(a.props[samAuthKey])["ResourcePolicy"]
	if !has || raw == nil {
		return nil
	}
	rp, ok := raw.(map[string]any)
	if !ok {
		return samErr(a.id, "ResourcePolicy must be a map (ResourcePolicyStatement).")
	}
	if err := rejectUnknown(a.id, rp, samResourcePolicyKeys()); err != nil {
		return err
	}
	policy := asMap(doc[samAPIGWPolicy])
	if policy == nil {
		policy = map[string]any{}
	}
	paths := asMap(doc["paths"])
	for _, path := range slices.Sorted(maps.Keys(paths)) {
		if err := a.addPolicyForPath(policy, rp, path, paths[path], a.stageRaw); err != nil {
			return err
		}
	}
	if cs := rp[samKeyCustomStmts]; cs != nil {
		addCustomStatements(policy, cs)
	}
	if len(policy) > 0 {
		doc[samAPIGWPolicy] = policy
	}

	return nil
}

// applyEventResourcePolicy renders one Api event's Auth.ResourcePolicy for its own path, with the path's
// methods as they stand when the event is added.
func (a *samAPI) applyEventResourcePolicy(doc map[string]any, rt samRoute, item any) error {
	raw, has := rt.auth["ResourcePolicy"]
	if !has || raw == nil {
		return nil
	}
	rp, ok := raw.(map[string]any)
	if !ok {
		return samErr(rt.fnID, "Auth.ResourcePolicy must be a map.")
	}
	if err := rejectUnknown(rt.fnID, rp, samResourcePolicyKeys()); err != nil {
		return err
	}
	policy := asMap(doc[samAPIGWPolicy])
	if policy == nil {
		policy = map[string]any{}
	}
	var stage any = a.permSuffix()
	if rt.roleID != "" {
		stage = nil
	}
	if err := a.addPolicyForPath(policy, rp, rt.path, item, stage); err != nil {
		return err
	}
	doc[samAPIGWPolicy] = policy
	if cs := rp[samKeyCustomStmts]; cs != nil {
		addCustomStatements(policy, cs)
	}

	return nil
}

func (a *samAPI) addPolicyForPath(policy, rp map[string]any, path string, item, stage any) error {
	resources, err := a.policyResources(path, item, stage)
	if err != nil {
		return err
	}
	for _, step := range []struct {
		key, effect string
	}{
		{"AwsAccountWhitelist", "Allow"}, {"AwsAccountBlacklist", "Deny"},
	} {
		if v, ok := rp[step.key]; ok {
			addIAMPolicyStatement(policy, v, step.effect, resources)
		}
	}
	for _, step := range []struct {
		key, cond string
	}{
		{"IpRangeWhitelist", samCondNotIPAddress}, {"IpRangeBlacklist", samCondIPAddress},
	} {
		if v, ok := rp[step.key]; ok {
			addIPPolicyStatement(policy, v, step.cond, resources)
		}
	}
	for _, step := range []struct {
		kind, cond string
	}{
		{"Blacklist", "StringEquals"}, {"Whitelist", "StringNotEquals"},
	} {
		list, ok := resolvedStringList(rp["SourceVpc"+step.kind])
		if !ok {
			return samErr(a.id, "SourceVpc%s must be a list of strings. Use IntrinsicVpc%s instead for values that "+
				"use Intrinsic Functions", step.kind, step.kind)
		}
		addVPCPolicyStatement(policy, list, asListOrNil(rp["IntrinsicVpc"+step.kind]),
			asListOrNil(rp["IntrinsicVpce"+step.kind]), step.cond, resources)
	}

	return nil
}

func asListOrNil(v any) []any {
	if v == nil {
		return nil
	}

	return asList(v)
}

// resolvedStringList reports a nil list or a list of only strings.
func resolvedStringList(v any) ([]string, bool) {
	if v == nil {
		return nil, true
	}
	list, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make([]string, 0, len(list))
	for _, e := range list {
		s, isStr := e.(string)
		if !isStr {
			return nil, false
		}
		out = append(out, s)
	}

	return out, true
}

// conditionalContents is the item itself, or the non-NoValue branches of an Fn::If wrapping it.
func conditionalContents(item any) ([]any, bool) {
	m, ok := item.(map[string]any)
	if !ok {
		return []any{item}, true
	}
	cond, has := m["Fn::If"]
	if !has {
		return []any{item}, true
	}
	args, isList := cond.([]any)
	if !isList {
		return nil, false
	}
	var out []any
	for _, c := range args[min(1, len(args)):] {
		if cm := asMap(c); len(cm) == 1 && cm[samKeyRef] == pseudoNoValue {
			continue
		}
		out = append(out, c)
	}

	return out, true
}

func (a *samAPI) policyResources(path string, item, stage any) ([]any, error) {
	contents, ok := conditionalContents(item)
	if !ok {
		return nil, samErr(a.id, "Value of Fn::If must be a list.")
	}
	var methods []string
	for _, c := range contents {
		pm, isMap := c.(map[string]any)
		if !isMap {
			return nil, samErr(a.id, "Value of %s must be a dictionary according to Swagger spec.", path)
		}
		methods = slices.Concat(methods, slices.Sorted(maps.Keys(pm)))
	}
	clean := samPathParamRe.ReplaceAllString(path, "*")
	var out []any
	for _, m := range methods {
		method := strings.ToUpper(m)
		if lm := strings.ToLower(m); lm == samAnyMethodKey || lm == samMethodAny {
			method = "*"
		}
		out = append(out, map[string]any{samKeyFnSub: []any{
			"execute-api:/${__Stage__}/" + method + clean, map[string]any{"__Stage__": stage},
		}})
	}

	return out, nil
}

func appendStatements(policy map[string]any, stmts ...map[string]any) {
	policy[samKeyVersion] = samPolicyVersion
	var list []any
	switch cur := policy[samKeyStatement].(type) {
	case nil:
		for _, s := range stmts {
			list = append(list, s)
		}
		policy[samKeyStatement] = list

		return
	case []any:
		list = cur
	default:
		list = []any{cur}
	}
	for _, s := range stmts {
		if !slices.ContainsFunc(list, func(e any) bool { return sameJSON(e, s) }) {
			list = append(list, s)
		}
	}
	policy[samKeyStatement] = list
}

func addIAMPolicyStatement(policy map[string]any, v any, effect string, resources []any) {
	list := asListOrNil(v)
	if len(list) == 0 {
		return
	}
	stmt := map[string]any{
		samKeyEffect: effect, samKeyAction: samInvokeAction, samKeyResource: resources,
		samKeyPrincipal: map[string]any{samKeyAwsPrincipal: list},
	}
	policy[samKeyVersion] = samPolicyVersion
	if policy[samKeyStatement] == nil {
		policy[samKeyStatement] = stmt

		return
	}
	cur, isList := policy[samKeyStatement].([]any)
	if !isList {
		cur = []any{policy[samKeyStatement]}
	}
	policy[samKeyStatement] = append(cur, stmt)
}

func allowDenyPair(resources []any, denyCondition map[string]any) []map[string]any {
	allow := map[string]any{
		samKeyEffect: "Allow", samKeyAction: samInvokeAction, samKeyResource: resources, samKeyPrincipal: "*",
	}
	deny := map[string]any{
		samKeyEffect: "Deny", samKeyAction: samInvokeAction, samKeyResource: resources, samKeyPrincipal: "*",
		samKeyCondition: denyCondition,
	}

	return []map[string]any{allow, deny}
}

func addIPPolicyStatement(policy map[string]any, v any, cond string, resources []any) {
	list := asListOrNil(v)
	if len(list) == 0 {
		return
	}
	appendStatements(policy, allowDenyPair(resources, map[string]any{cond: map[string]any{"aws:SourceIp": list}})...)
}

func addVPCPolicyStatement(
	policy map[string]any, endpoints []string, intrinsicVpc, intrinsicVpce []any, cond string, resources []any,
) {
	var vpcs, vpces []any
	for _, e := range endpoints {
		if strings.HasPrefix(e, "vpce-") {
			vpces = append(vpces, e)
		}
		if strings.HasPrefix(e, "vpc-") {
			vpcs = append(vpcs, e)
		}
	}
	vpcs = slices.Concat(vpcs, intrinsicVpc)
	vpces = slices.Concat(vpces, intrinsicVpce)
	if len(vpcs) == 0 && len(vpces) == 0 {
		return
	}
	condition := map[string]any{}
	if len(vpcs) > 0 {
		condition[samKeySourceVpc] = vpcs
	}
	if len(vpces) > 0 {
		condition[samKeySourceVpce] = vpces
	}
	appendStatements(policy, allowDenyPair(resources, map[string]any{cond: condition})...)
}

func addCustomStatements(policy map[string]any, custom any) {
	policy[samKeyVersion] = samPolicyVersion
	if policy[samKeyStatement] == nil {
		policy[samKeyStatement] = custom

		return
	}
	var list []any
	switch cur := policy[samKeyStatement].(type) {
	case []any:
		list = cur
	default:
		list = []any{cur}
	}
	for _, s := range asList(custom) {
		if !slices.ContainsFunc(list, func(e any) bool { return sameJSON(e, s) }) {
			list = append(list, s)
		}
	}
	policy[samKeyStatement] = list
}
