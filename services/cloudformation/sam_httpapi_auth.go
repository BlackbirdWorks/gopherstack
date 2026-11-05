package cloudformation

import (
	"maps"
	"slices"
	"strings"
)

const (
	samIAMAuthorizer = "AWS_IAM"
	samAuthTypeJWT   = "JWT"
	samAuthTypeReq   = "REQUEST"
)

// samHTTPAuthorizer is one parsed HttpApi Auth.Authorizers entry.
type samHTTPAuthorizer struct {
	cfg    map[string]any
	scopes any
	kind   string
	name   string
}

func samJWTAuthKeys() map[string]bool {
	return keySet("AuthorizationScopes", "JwtConfiguration", "IdentitySource")
}

func samRequestAuthKeys() map[string]bool {
	return keySet("FunctionArn", "FunctionInvokeRole", "Identity", "AuthorizerPayloadFormatVersion",
		"EnableSimpleResponses", "EnableFunctionDefaultPermissions")
}

func (a *samHTTPAPI) authorizers(auth map[string]any) (map[string]*samHTTPAuthorizer, error) {
	if err := rejectUnknown(a.id, auth, keySet("Authorizers", "DefaultAuthorizer", "EnableIamAuthorizer")); err != nil {
		return nil, samErr(a.id, "Invalid value for 'Auth' property")
	}
	out := map[string]*samHTTPAuthorizer{}
	if auth["EnableIamAuthorizer"] == true {
		out[samIAMAuthorizer] = &samHTTPAuthorizer{name: samIAMAuthorizer, kind: samIAMAuthorizer}
	}
	cfgs := auth["Authorizers"]
	if cfgs == nil {
		return out, nil
	}
	m, ok := cfgs.(map[string]any)
	if !ok {
		return nil, samErr(a.id, "Auth.Authorizers must be a map.")
	}
	for _, name := range slices.Sorted(maps.Keys(m)) {
		cfg, isMap := m[name].(map[string]any)
		if !isMap {
			return nil, samErr(a.id, "Auth.Authorizers.%s must be a map.", name)
		}
		az, err := a.newAuthorizer(name, cfg)
		if err != nil {
			return nil, err
		}
		out[name] = az
	}

	return out, nil
}

func (a *samHTTPAPI) newAuthorizer(name string, cfg map[string]any) (*samHTTPAuthorizer, error) {
	if cfg["OpenIdConnectUrl"] != nil {
		return nil, samErr(a.id, "'OpenIdConnectUrl' is no longer a supported property for authorizer '%s'. "+
			"Please refer to the AWS SAM documentation.", name)
	}
	if jwt, has := cfg["JwtConfiguration"]; has && jwt != nil {
		if _, isMap := jwt.(map[string]any); !isMap {
			return nil, samErr(a.id, "Property 'JwtConfiguration' should be a map.")
		}
	}
	az := &samHTTPAuthorizer{name: name, cfg: cfg, scopes: cfg["AuthorizationScopes"], kind: samAuthTypeReq}
	allowed := samRequestAuthKeys()
	if cfg["JwtConfiguration"] != nil {
		az.kind, allowed = samAuthTypeJWT, samJWTAuthKeys()
	}
	for _, k := range slices.Sorted(maps.Keys(cfg)) {
		if !allowed[k] {
			return nil, samErr(a.id, "%s is only supported for %s.", k, authorizerKindLabel(k))
		}
	}
	if _, isList := az.scopes.([]any); az.scopes != nil && !isList {
		return nil, samErr(a.id, "AuthorizationScopes must be a list.")
	}
	if err := az.validateRequired(a.id); err != nil {
		return nil, err
	}

	return az, nil
}

func (az *samHTTPAuthorizer) validateRequired(apiID string) error {
	if az.kind == samAuthTypeJWT {
		if az.cfg["IdentitySource"] == nil {
			return samErr(apiID, "%s OAuth2 Authorizer must define 'IdentitySource'.", az.name)
		}

		return nil
	}
	if az.cfg["FunctionArn"] == nil {
		return samErr(apiID, "%s Lambda Authorizer must define 'FunctionArn'.", az.name)
	}
	if az.cfg["AuthorizerPayloadFormatVersion"] == nil {
		return samErr(apiID, "%s Lambda Authorizer must define 'AuthorizerPayloadFormatVersion'.", az.name)
	}
	if v, ok := az.cfg["EnableFunctionDefaultPermissions"]; ok {
		if _, isBool := v.(bool); !isBool {
			return samErr(apiID, "Authorizers.%s.EnableFunctionDefaultPermissions must be a boolean.", az.name)
		}
	}

	return nil
}

func authorizerKindLabel(key string) string {
	if samJWTAuthKeys()[key] {
		return "OAuth2 Authorizer"
	}

	return "Lambda Authorizer"
}

// scheme renders the securitySchemes entry.
func (az *samHTTPAuthorizer) scheme() map[string]any {
	switch az.kind {
	case samIAMAuthorizer:
		return map[string]any{samKeyTypeLower: samTypeAPIKey, samKeyNameLower: samAuthHeader, "in": samLocHeader,
			samKXAmazonApigatewayAuthtype: "awsSigv4"}
	case samAuthTypeJWT:
		jwt := map[string]any{}
		for k, v := range asMap(az.cfg["JwtConfiguration"]) {
			jwt[strings.ToLower(k)] = v
		}

		return map[string]any{samKeyTypeLower: "oauth2", samOAPIAuthorizer: map[string]any{
			"jwtConfiguration": jwt, "identitySource": az.cfg["IdentitySource"], samKeyTypeLower: "jwt",
		}}
	}

	return az.requestScheme()
}

func (az *samHTTPAuthorizer) requestScheme() map[string]any {
	ext := map[string]any{
		samKeyTypeLower: "request",
		"authorizerUri": map[string]any{samKeyFnSub: []any{
			"arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${__FunctionArn__}/invocations",
			map[string]any{"__FunctionArn__": az.cfg["FunctionArn"]},
		}},
		"authorizerPayloadFormatVersion": az.cfg["AuthorizerPayloadFormatVersion"],
	}
	if role := az.cfg["FunctionInvokeRole"]; role != nil && role != valueNone {
		ext["authorizerCredentials"] = role
	}
	if id := asMap(az.cfg["Identity"]); id != nil {
		if v := id["ReauthorizeEvery"]; v != nil {
			ext["authorizerResultTtlInSeconds"] = v
		}
		ext["identitySource"] = httpIdentitySources(id)
	}
	if az.cfg["EnableSimpleResponses"] == true {
		ext["enableSimpleResponses"] = true
	}

	return map[string]any{samKeyTypeLower: samTypeAPIKey, samKeyNameLower: "Unused", "in": samLocHeader,
		samOAPIAuthorizer: ext}
}

func httpIdentitySources(id map[string]any) []any {
	out := []any{}
	for _, p := range []struct{ key, prefix string }{
		{samKeyHeaders, "$request.header."}, {samKQueryStrings, "$request.querystring."},
		{samKeyStageVars, "$stageVariables."}, {"Context", "$context."},
	} {
		for _, v := range asList(id[p.key]) {
			if s, ok := v.(string); ok {
				out = append(out, p.prefix+s)
			}
		}
	}

	return out
}

func (a *samHTTPAPI) applyAuth(ed *samOpenAPI, auth map[string]any) error {
	authorizers, err := a.authorizers(auth)
	if err != nil {
		return err
	}
	schemes := ed.securitySchemes()
	for name, az := range authorizers {
		schemes[name] = az.scheme()
	}
	def, ok := auth["DefaultAuthorizer"]
	if !ok || def == nil || isNoValue(def) {
		return nil
	}
	name, isStr := def.(string)
	if !isStr {
		return samErr(a.id, "Auth.DefaultAuthorizer must be a string.")
	}
	az := authorizers[name]
	if az == nil {
		return samErr(a.id, "Unable to set DefaultAuthorizer because '%s' was not defined in 'Authorizers'.", name)
	}

	return ed.setDefaultAuthorizer(a.id, az)
}

func isNoValue(v any) bool {
	m, ok := v.(map[string]any)

	return ok && m[samKeyRef] == pseudoNoValue
}

// setDefaultAuthorizer secures every non-OPTIONS operation that has an integration and no security of its own.
func (e *samOpenAPI) setDefaultAuthorizer(id string, az *samHTTPAuthorizer) error {
	scopes := az.scopes
	if scopes == nil {
		scopes = []any{}
	}
	for _, path := range slices.Sorted(maps.Keys(e.paths())) {
		item := asMap(e.paths()[path])
		if item == nil {
			return samErr(id, "Value of '%s' path must be a dictionary according to Swagger spec.", path)
		}
		for method, v := range item {
			op, exempt, err := defaultAuthTarget(id, path, method, v, item)
			if err != nil {
				return err
			}
			if !exempt {
				op[samOAPISecurity] = []any{map[string]any{az.name: scopes}}
			}
		}
	}

	return nil
}

// defaultAuthTarget returns the operation to secure, or exempt when the default authorizer skips it.
func defaultAuthTarget(id, path, method string, v any, item map[string]any) (map[string]any, bool, error) {
	norm := strings.ToLower(method)
	if norm == "any" {
		norm = samAnyMethodKey
	}
	if norm == samKOptions || norm == "parameters" {
		return nil, true, nil
	}
	if _, found := item[norm]; !found {
		return nil, false, samErr(
			id,
			"Structure of the SAM template is invalid. Could not find %s in %s within DefinitionBody.",
			norm,
			path,
		)
	}
	op := asMap(v)
	if op == nil {
		if v == nil {
			return nil, false, samErr(id, "Definition of method '%s' for path '%s' should be a map.", method, path)
		}

		return nil, true, nil
	}
	if op[samIntegrationKey] == nil || len(asList(op[samOAPISecurity])) > 0 {
		return nil, true, nil
	}

	return op, false, nil
}

// addRouteAuth applies an event's Auth to its operation.
func (a *samHTTPAPI) addRouteAuth(ed *samOpenAPI, rt samHTTPRoute, path, method string) error {
	auth := asMap(a.props[samAuthKey])
	authorizers := asMap(auth["Authorizers"])
	name, _ := rt.auth["Authorizer"].(string)
	if name == "" {
		def, _ := auth["DefaultAuthorizer"].(string)
		if def == "" {
			return samErr(rt.event, "'Auth' section requires either an explicit 'Authorizer' set or a "+
				"'DefaultAuthorizer' configured on the HttpApi.")
		}
		name = def
	}
	iam := auth["EnableIamAuthorizer"] == true
	switch {
	case name == valueNone:
		if auth["DefaultAuthorizer"] == nil {
			return samErr(rt.event, "Unable to set Authorizer on API method [%s] for path [%s] because 'NONE' is only "+
				"a valid value when a DefaultAuthorizer on the API is specified.", rt.method, rt.path)
		}
	case name == samIAMAuthorizer && iam:
	case authorizers == nil || authorizers[name] == nil:
		return samErr(rt.event, "Unable to set Authorizer [%s] on API method [%s] for path [%s] because it wasn't "+
			"defined in the API's Authorizers.", name, rt.method, rt.path)
	}
	scopes := []any{}
	if name != valueNone && name != samIAMAuthorizer {
		if s := asMap(authorizers[name])["AuthorizationScopes"]; s != nil {
			scopes = asList(s)
		}
		if s := rt.auth["AuthorizationScopes"]; s != nil {
			l, isList := s.([]any)
			if !isList {
				return samErr(rt.event, "Unable to set Authorizer on API method [%s] for path [%s] because "+
					"'AuthorizationScopes' must be a list of strings.", rt.method, rt.path)
			}
			scopes = l
		}
	}
	op := asMap(asMap(ed.paths()[path])[method])
	op[samOAPISecurity] = append(asList(op[samOAPISecurity]), map[string]any{name: scopes})

	return nil
}

// putHTTPAuthorizerPermissions emits <Api><Authorizer>AuthorizerPermission for Lambda
// authorizers that opted in with EnableFunctionDefaultPermissions.
func (t *samTranslator) putHTTPAuthorizerPermissions(a *samHTTPAPI) error {
	auth := asMap(a.props[samAuthKey])
	if auth == nil {
		return nil
	}
	authorizers, err := a.authorizers(auth)
	if err != nil {
		return err
	}
	for _, name := range slices.Sorted(maps.Keys(authorizers)) {
		az := authorizers[name]
		if az.kind != samAuthTypeReq || az.cfg["EnableFunctionDefaultPermissions"] != true {
			continue
		}
		perm := lambdaPermission("", "apigateway.amazonaws.com", map[string]any{samKeyFnSub: []any{
			"arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${__ApiId__}/authorizers/*",
			map[string]any{samAPIIDMarker: samRef(a.id)},
		}})
		asMap(perm[samKeyProps])[samKeyFnName] = az.cfg["FunctionArn"]
		if err = t.put(a.id+name+"AuthorizerPermission", perm); err != nil {
			return err
		}
	}

	return nil
}
