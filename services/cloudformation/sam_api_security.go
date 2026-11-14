package cloudformation

import (
	"maps"
	"slices"
)

const (
	samKeyMarkerOff = "api_key_false"
	samSecIAM       = "AWS_IAM"
)

// eventSecurity is what an Api event writes to its operation: the chosen authorizer (NONE is a marker the
// default-authorizer pass consumes) and the api_key / api_key_false marker.
func (a *samAPI) eventSecurity(rt samRoute) []any {
	var sec []any
	if rt.authorizer != "" {
		sec = append(sec, map[string]any{rt.authorizer: a.eventScopes(rt)})
	}
	if rt.apiKey != nil {
		marker := samKeyMarkerOff
		if *rt.apiKey {
			marker = samAPIKeyScheme
		}
		sec = append(sec, map[string]any{marker: []any{}})
	}

	return sec
}

// eventScopes are the event's AuthorizationScopes, else the authorizer's own.
func (a *samAPI) eventScopes(rt samRoute) []any {
	if rt.authorizer == valueNone {
		return []any{}
	}
	if rt.scopes != nil {
		return asList(rt.scopes)
	}
	if s := asMap(a.authorizerConfig(rt.authorizer))["AuthorizationScopes"]; s != nil {
		return asList(s)
	}

	return []any{}
}

func (a *samAPI) authorizerConfig(name string) any {
	return asMap(asMap(a.props[samAuthKey])["Authorizers"])[name]
}

// applyDefaultSecurity runs SAM's API-level pass: the default authorizer, then the default ApiKeyRequired.
func (a *samAPI) applyDefaultSecurity(doc map[string]any) error {
	if a.auth == nil {
		return nil
	}
	if a.auth.defaultAuth != "" {
		if err := a.setDefaultAuthorizer(doc); err != nil {
			return err
		}
	}
	if a.auth.apiKeyDefault {
		addAPIKeyDefinition(doc)
		if err := setDefaultAPIKey(doc, a.auth.apiKeyPreflight); err != nil {
			return err
		}
	}

	return a.applyResourcePolicy(doc)
}

// forEachItemOp visits the integration operations of one path item, including conditional ones.
func forEachItemOp(path string, item map[string]any, fn func(path, method string, op map[string]any) error) error {
	for _, method := range slices.Sorted(maps.Keys(item)) {
		ops, _ := conditionalContents(item[method])
		for _, o := range ops {
			op := asMap(o)
			if op == nil || op[samIntegrationKey] == nil {
				continue
			}
			if err := fn(path, method, op); err != nil {
				return err
			}
		}
	}

	return nil
}

// forEachIntegrationOp visits every operation that has an API Gateway integration, in a stable order.
func forEachIntegrationOp(doc map[string]any, fn func(path, method string, op map[string]any) error) error {
	paths := asMap(doc["paths"])
	for _, path := range slices.Sorted(maps.Keys(paths)) {
		contents, _ := conditionalContents(paths[path])
		for _, c := range contents {
			if err := forEachItemOp(path, asMap(c), fn); err != nil {
				return err
			}
		}
	}

	return nil
}

func securityList(op map[string]any) ([]any, error) {
	v, has := op[samOAPISecurity]
	if !has {
		return nil, nil
	}
	list, ok := v.([]any)
	if !ok {
		return nil, samInvalid("Type of security must be a list")
	}
	for _, s := range list {
		if _, isDict := s.(map[string]any); !isDict {
			return nil, samInvalid("%v in Security is not a valid dictionary.", s)
		}
	}

	return list, nil
}

func (a *samAPI) setDefaultAuthorizer(doc map[string]any) error {
	names := map[string]bool{samSecIAM: true}
	for n := range a.auth.defs {
		names[n] = true
	}
	scopes := []any{}
	if s := asMap(a.authorizerConfig(a.auth.defaultAuth))["AuthorizationScopes"]; s != nil {
		scopes = asList(s)
	}

	return forEachIntegrationOp(doc, func(_, method string, op map[string]any) error {
		if method == samKOptions && !a.auth.corsPreflight {
			return nil
		}
		existing, err := securityList(op)
		if err != nil {
			return err
		}
		sec := defaultAuthSecurity(existing, names, a.auth.defaultAuth, scopes)
		if len(sec) > 0 {
			op[samOAPISecurity] = sec
			if _, iam := asMap(sec[0])[samSecIAM]; iam {
				addIAMDefinition(doc)
			}
		}

		return nil
	})
}

// defaultAuthSecurity applies the default authorizer to one operation's security: a NONE marker opts out (and
// is dropped), an existing authorizer entry wins, otherwise the default is added.
func defaultAuthSecurity(existing []any, names map[string]bool, def string, scopes []any) []any {
	var other, authSec []any
	for _, s := range existing {
		if keysIntersect(asMap(s), names) {
			authSec = append(authSec, s)
		} else {
			other = append(other, s)
		}
	}
	noneAt := slices.IndexFunc(other, func(s any) bool {
		_, has := asMap(s)[valueNone]

		return has
	})
	switch {
	case noneAt >= 0:
		other, authSec = slices.Delete(other, noneAt, noneAt+1), nil
	case len(authSec) == 0:
		authSec = []any{map[string]any{def: scopes}}
	}

	return slices.Concat(other, authSec)
}

func keysIntersect(m map[string]any, set map[string]bool) bool {
	for k := range m {
		if set[k] {
			return true
		}
	}

	return false
}

func setDefaultAPIKey(doc map[string]any, preflight bool) error {
	return forEachIntegrationOp(doc, func(_, method string, op map[string]any) error {
		existing, err := securityList(op)
		if err != nil {
			return err
		}
		var other, keySec []any
		for _, s := range existing {
			m := asMap(s)
			if _, k := m[samAPIKeyScheme]; k {
				keySec = append(keySec, s)
			} else if _, kf := m[samKeyMarkerOff]; kf {
				keySec = append(keySec, s)
			} else {
				other = append(other, s)
			}
		}
		var apiKey []any
		falseAt := slices.IndexFunc(keySec, func(s any) bool {
			_, has := asMap(s)[samKeyMarkerOff]

			return has
		})
		if falseAt < 0 {
			apiKey = []any{map[string]any{samAPIKeyScheme: []any{}}}
		}
		sec := slices.Concat(other, apiKey)
		if method == samKOptions && !preflight {
			sec = other
		}
		if !sameJSON(sec, existing) && (len(sec) > 0 || len(existing) > 0) {
			op[samOAPISecurity] = sec
		}

		return nil
	})
}

func addIAMDefinition(doc map[string]any) {
	defs := securityDefinitionsOf(doc)
	if _, has := defs[samSecIAM]; !has {
		defs[samSecIAM] = map[string]any{samKXAmazonApigatewayAuthtype: "awsSigv4", samKeyTypeLower: samTypeAPIKey,
			samKeyNameLower: samAuthHeader, "in": samLocHeader}
	}
}

func addAPIKeyDefinition(doc map[string]any) {
	defs := securityDefinitionsOf(doc)
	if _, has := defs[samAPIKeyScheme]; !has {
		defs[samAPIKeyScheme] = map[string]any{samKeyTypeLower: samTypeAPIKey, samKeyNameLower: samAPIKeyHeader,
			"in": samLocHeader}
	}
}

func securityDefinitionsOf(doc map[string]any) map[string]any {
	defs := asMap(doc["securityDefinitions"])
	if defs == nil {
		defs = map[string]any{}
		doc["securityDefinitions"] = defs
	}

	return defs
}

// addIAMCredentials sets the integration credentials of an operation secured with AWS_IAM, from the event's or
// the API's InvokeRole (the caller's credentials by default).
func (a *samAPI) addIAMCredentials(rt samRoute, op map[string]any) {
	apiAuth := asMap(a.props[samAuthKey])
	explicit := rt.auth["Authorizer"] == samSecIAM
	byDefault := apiAuth["DefaultAuthorizer"] == samSecIAM && len(rt.auth) == 0
	if !explicit && !byDefault {
		return
	}
	role := func(cfg map[string]any) any {
		v := cfg["InvokeRole"]
		if (v == nil || v == "") && cfg != nil {
			if _, has := cfg["InvokeRole"]; has {
				return valueNone
			}
		}

		return v
	}
	chosen := role(rt.auth)
	if chosen == nil || chosen == "" {
		chosen = role(apiAuth)
	}
	var creds any = map[string]any{samKeyFnSub: "arn:${AWS::Partition}:iam::*:user/*"}
	if chosen != nil && chosen != "" && chosen != "CALLER_CREDENTIALS" {
		creds = chosen
	}
	if creds != valueNone {
		asMap(op[samIntegrationKey])["credentials"] = creds
	}
}
