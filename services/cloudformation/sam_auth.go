package cloudformation

import (
	"maps"
	"slices"
	"strings"
)

const (
	samAuthKey     = "Auth"
	samAuthNone    = "NONE"
	samHeaderParam = "method.request.header."
)

// samAuth is the parsed Api Auth block: swagger securityDefinitions plus the default authorizer.
type samAuth struct {
	defs            map[string]any
	perms           map[string]map[string]any
	defaultAuth     string
	corsPreflight   bool
	apiKeyDefault   bool
	apiKeyPreflight bool
}

func samAuthKeys() map[string]bool {
	return keySet("DefaultAuthorizer", "Authorizers", "AddDefaultAuthorizerToCorsPreflight", "ApiKeyRequired",
		"AddApiKeyRequiredToCorsPreflight", "UsagePlan", "InvokeRole", "ResourcePolicy")
}

func (t *samTranslator) parseAPIAuth(apiID string, props map[string]any) (*samAuth, error) {
	raw := asMap(props[samAuthKey])
	if raw == nil {
		return &samAuth{}, nil
	}
	if v, ok := raw["ApiKeyRequired"]; ok {
		if _, isBool := v.(bool); !isBool {
			return nil, samErr(apiID, "Auth.ApiKeyRequired must be a boolean.")
		}
	}
	if err := rejectUnknown(apiID, raw, samAuthKeys()); err != nil {
		return nil, err
	}
	a := &samAuth{
		defs:            map[string]any{},
		perms:           map[string]map[string]any{},
		corsPreflight:   raw["AddDefaultAuthorizerToCorsPreflight"] != false,
		apiKeyDefault:   raw["ApiKeyRequired"] == true,
		apiKeyPreflight: raw["AddApiKeyRequiredToCorsPreflight"] != false,
	}
	authorizers := asMap(raw["Authorizers"])
	for _, name := range slices.Sorted(maps.Keys(authorizers)) {
		if err := validateAuthorizerShape(apiID, name, authorizers[name]); err != nil {
			return nil, err
		}
		def, perm, err := samAuthorizer(apiID, name, authorizers[name])
		if err != nil {
			return nil, err
		}
		a.defs[name] = def
		if perm != nil {
			a.perms[apiID+name+"AuthorizerPermission"] = perm
		}
	}
	if d, ok := raw["DefaultAuthorizer"]; ok {
		name, _ := d.(string)
		if a.defs[name] == nil && name != samSecIAM {
			return nil, samErr(apiID, "DefaultAuthorizer must name an entry of Auth.Authorizers.")
		}
		a.defaultAuth = name
	}

	return a, nil
}

func samAuthorizer(apiID, name string, v any) (map[string]any, map[string]any, error) {
	cfg := asMap(v)
	if cfg == nil {
		return nil, nil, samErr(
			apiID,
			"Authorizer [%s] must be a Cognito or Lambda authorizer object; AWS_IAM is not supported.",
			name,
		)
	}
	if cfg["UserPoolArn"] != nil {
		def, err := samCognitoAuthorizer(apiID, name, cfg)

		return def, nil, err
	}

	return samLambdaAuthorizer(apiID, name, cfg)
}

func samAuthDef(header string, ext map[string]any, authType string) map[string]any {
	return map[string]any{
		samKeyTypeLower: samTypeAPIKey, samKeyNameLower: header, "in": samLocHeader,
		samKXAmazonApigatewayAuthtype: authType, "x-amazon-apigateway-authorizer": ext,
	}
}

func samCognitoAuthorizer(apiID, name string, cfg map[string]any) (map[string]any, error) {
	if err := rejectUnknown(apiID+name, cfg, keySet("UserPoolArn", "Identity", "AuthorizationScopes")); err != nil {
		return nil, err
	}
	id := asMap(cfg["Identity"])
	if err := rejectUnknown(apiID+name, id, keySet("Header", "ValidationExpression", "ReauthorizeEvery")); err != nil {
		return nil, err
	}
	header := samAuthHeader
	if h, ok := id["Header"].(string); ok {
		header = h
	}
	ext := map[string]any{samKeyTypeLower: "cognito_user_pools", "providerARNs": asList(cfg["UserPoolArn"])}
	if v := id["ValidationExpression"]; v != nil {
		ext["identityValidationExpression"] = v
	}
	if v := id["ReauthorizeEvery"]; v != nil {
		ext["authorizerResultTtlInSeconds"] = v
	}

	return samAuthDef(header, ext, "cognito_user_pools"), nil
}

func samLambdaAuthorizer(apiID, name string, cfg map[string]any) (map[string]any, map[string]any, error) {
	if err := rejectUnknown(apiID+name, cfg, keySet("FunctionArn", "FunctionPayloadType", "FunctionInvokeRole",
		"Identity", "DisableFunctionDefaultPermissions", "AuthorizationScopes", "EnableSimpleResponses",
		"AuthorizerPayloadFormatVersion")); err != nil {
		return nil, nil, err
	}
	fnArn := cfg["FunctionArn"]
	if fnArn == nil {
		return nil, nil, samErr(apiID, "Authorizer [%s] needs 'UserPoolArn' or 'FunctionArn'.", name)
	}
	payload := samTokenType
	if p, ok := cfg["FunctionPayloadType"].(string); ok {
		payload = strings.ToUpper(p)
	}
	id := asMap(cfg["Identity"])
	ext := map[string]any{
		samKeyTypeLower: strings.ToLower(payload),
		"authorizerUri": map[string]any{samKeyFnSub: []any{
			"arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${__FunctionArn__}/invocations",
			map[string]any{"__FunctionArn__": fnArn},
		}},
	}
	if v := cfg["FunctionInvokeRole"]; v != nil && v != valueNone {
		ext["authorizerCredentials"] = v
	}
	header, err := samLambdaIdentity(apiID+name, payload, id, ext)
	if err != nil {
		return nil, nil, err
	}
	if payload == samAuthTypeReq {
		header = "Unused"
	}
	var perm map[string]any
	if cfg["DisableFunctionDefaultPermissions"] != true {
		perm = lambdaPermission("", "apigateway.amazonaws.com", map[string]any{samKeyFnSub: []any{
			samAuthorizersSourceARN,
			map[string]any{samAPIIDMarker: samRef(apiID)},
		}})
		asMap(perm[samKeyProps])[samKeyFnName] = fnArn
	}

	return samAuthDef(header, ext, "custom"), perm, nil
}

func samLambdaIdentity(id, payload string, identity, ext map[string]any) (string, error) {
	header := samAuthHeader
	var err error
	switch payload {
	case samTokenType:
		header = samTokenIdentity(identity, ext)
	case samAuthTypeReq:
		err = samRequestIdentity(id, identity, ext)
	default:
		err = samErr(id, "FunctionPayloadType must be TOKEN or REQUEST.")
	}
	if v := identity["ReauthorizeEvery"]; v != nil && err == nil {
		ext["authorizerResultTtlInSeconds"] = v
	}

	return header, err
}

func samTokenIdentity(identity, ext map[string]any) string {
	header := samAuthHeader
	if h, ok := identity["Header"].(string); ok {
		header = h
	}
	if v := identity["ValidationExpression"]; v != nil {
		ext["identityValidationExpression"] = v
	}

	return header
}

func samRequestIdentity(id string, identity, ext map[string]any) error {
	sources := identitySources(identity)
	if len(sources) == 0 && missingIdentitySource(identity) {
		return samErr(id, "A REQUEST authorizer needs an Identity with at least one source.")
	}
	if len(sources) > 0 {
		ext["identitySource"] = strings.Join(sources, ", ")
	}

	return nil
}

// missingIdentitySource follows SAM: a ReauthorizeEvery of 0 (no caching) needs no identity source.
func missingIdentitySource(identity map[string]any) bool {
	switch ttl := identity["ReauthorizeEvery"].(type) {
	case float64:
		return int(ttl) > 0
	case int:
		return ttl > 0
	}

	return true
}

func identitySources(identity map[string]any) []string {
	var out []string
	for _, p := range []struct{ key, prefix string }{
		{samKeyHeaders, samHeaderParam}, {samKQueryStrings, "method.request.querystring."},
		{samKeyStageVars, "stageVariables."}, {samKeyContext, "context."},
	} {
		for _, v := range asList(identity[p.key]) {
			if s, ok := v.(string); ok {
				out = append(out, p.prefix+s)
			}
		}
	}

	return out
}
