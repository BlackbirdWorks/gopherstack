package cloudformation

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
)

func samInvalid(format string, args ...any) error {
	return fmt.Errorf("%w. Structure of the SAM template is invalid. %s", ErrSAMTransform, fmt.Sprintf(format, args...))
}

func samGlobalsApiKeys() []string {
	return []string{"Auth", "Name", "DefinitionUri", "CacheClusterEnabled", "CacheClusterSize", "MergeDefinitions",
		"Variables", "EndpointConfiguration", "MethodSettings", "BinaryMediaTypes", "MinimumCompressionSize", "Cors",
		"GatewayResponses", "AccessLogSetting", "CanarySetting", "TracingEnabled", "OpenApiVersion", "Domain",
		"AlwaysDeploy", "PropagateTags", "SecurityPolicy", "EndpointAccessMode", "DisableExecuteApiEndpoint",
		"Tags", "Models"}
}

// validateSAMDocument checks the template-level structure the SAM translator insists on.
func validateSAMDocument(doc map[string]any) error {
	if res, has := doc["Resources"]; !has || res == nil {
		return samInvalid("'Resources' section is required")
	} else if _, isMap := res.(map[string]any); !isMap {
		return samInvalid("'Resources' section is required")
	}
	for _, k := range []string{"Mappings", "Parameters"} {
		if v, has := doc[k]; has {
			if _, isMap := v.(map[string]any); !isMap {
				return samInvalid("'Mappings' or 'Parameters' is either null or not a valid dictionary.")
			}
		}
	}
	if g, has := doc["Globals"]; has {
		if m, isMap := g.(map[string]any); !isMap || len(m) == 0 {
			return fmt.Errorf("%w. 'Globals' section is invalid. It must be a non-empty dictionary", ErrSAMTransform)
		}
	}
	res := asMap(doc["Resources"])
	for _, id := range slices.Sorted(maps.Keys(res)) {
		r := asMap(res[id])
		if cond, has := r[samKeyCondition]; has {
			if _, isStr := cond.(string); !isStr {
				return samInvalid("Every Condition member must be a string.")
			}
		}
		if typ, _ := r[samKeyType].(string); strings.HasPrefix(typ, samResPrefix) && (id == "" || !isAlnum(id)) {
			return samErr(id, "Logical ids must be alphanumeric.")
		}
	}

	return nil
}

func validateGlobalsSection(section string, v map[string]any) error {
	if section != "Api" {
		return nil
	}
	for _, k := range slices.Sorted(maps.Keys(v)) {
		if !slices.Contains(samGlobalsApiKeys(), k) {
			return fmt.Errorf("%w. 'Globals' section is invalid. '%s' is not a supported property of 'Api'",
				ErrSAMTransform, k)
		}
	}

	return nil
}

// validateSubVariables rejects an Fn::Sub that names a template parameter whose default is a list.
func (t *samTranslator) validateSubVariables(v any) error {
	switch val := v.(type) {
	case map[string]any:
		if sub, ok := val[samKeyFnSub]; ok && len(val) == 1 {
			s, _ := sub.(string)
			if l, isList := sub.([]any); isList && len(l) > 0 {
				s, _ = l[0].(string)
			}
			for _, m := range samSubVarPattern.FindAllStringSubmatch(s, -1) {
				if _, isList := t.params[m[1]].([]any); isList {
					return samInvalid("Invalid Fn::Sub variable value %v. Fn::Sub expects all variables to be strings.",
						t.params[m[1]])
				}
			}
		}
		for _, k := range slices.Sorted(maps.Keys(val)) {
			if err := t.validateSubVariables(val[k]); err != nil {
				return err
			}
		}
	case []any:
		for _, e := range val {
			if err := t.validateSubVariables(e); err != nil {
				return err
			}
		}
	}

	return nil
}

func isStringList(v any) bool {
	list, ok := v.([]any)
	if !ok {
		return false
	}
	for _, e := range list {
		if _, isStr := e.(string); !isStr {
			return false
		}
	}

	return true
}

// validateFunctionShape applies the type checks SAM's Function model makes before translating anything.
func (t *samTranslator) validateFunctionShape(id string, props map[string]any) error {
	if role, has := props["Role"]; has && role != nil {
		switch role.(type) {
		case string, map[string]any:
		default:
			return samErr(id, "Property 'Role' should be a string.")
		}
	}
	if layers, has := props["Layers"]; has && layers != nil {
		switch layers.(type) {
		case []any, map[string]any:
		default:
			return samErr(id, "Type of property 'Layers' is invalid.")
		}
	}
	if v, has := props["AutoPublishAliasAllProperties"]; has && v != nil {
		if _, isBool := v.(bool); !isBool {
			return samErr(id, "Type of property 'AutoPublishAliasAllProperties' is invalid.")
		}
	}
	if sha, has := props["AutoPublishCodeSha256"]; has && sha != nil {
		if _, isStr := sha.(string); !isStr {
			return samErr(id, "AutoPublishCodeSha256 must be a string")
		}
	}
	if err := validateFunctionPackage(id, props); err != nil {
		return err
	}

	return validateTenancy(id, props)
}

func validateFunctionPackage(id string, props map[string]any) error {
	switch pt := props["PackageType"].(type) {
	case string:
		if pt == samKeyImage {
			if props["ImageUri"] == nil {
				return samErr(id, "'ImageUri' must be set.")
			}

			return nil
		}
	case nil:
	default:
		return nil
	}
	if props["Handler"] == nil || props["Runtime"] == nil {
		if props["CapacityProviderConfig"] == nil || props["Runtime"] == nil {
			return samErr(id, "Runtime and Handler needs to be present when PackageType is of type `Zip`")
		}
	}

	return nil
}

func validateTenancy(id string, props map[string]any) error {
	if props["TenancyConfig"] == nil {
		return nil
	}
	switch {
	case props["FunctionUrlConfig"] != nil:
		return samErr(id, "Function URL is not supported for functions enabled with tenancy configuration.")
	case props["ProvisionedConcurrencyConfig"] != nil:
		return samErr(id, "Provisioned concurrency is not supported for functions enabled with tenancy configuration.")
	case props["SnapStart"] != nil:
		return samErr(id, "SnapStart is not supported for functions enabled with tenancy configuration.")
	}
	events := asMap(props[samKeyEvents])
	for _, name := range slices.Sorted(maps.Keys(events)) {
		typ, _ := asMap(events[name])[samKeyType].(string)
		if typ != samEventAPI && typ != samEventHTTPAPI {
			return samErr(id, "Event source '%s' is not supported for functions enabled with tenancy configuration. "+
				"Only Api and HttpApi event sources are supported.", typ)
		}
	}

	return nil
}

// validateSwaggerDocument checks the parts of an inline Swagger/OpenAPI body the SAM editor walks.
func (a *samAPI) validateSwaggerDocument(doc map[string]any) error {
	if defs, has := doc["securityDefinitions"]; has {
		if _, isMap := defs.(map[string]any); !isMap {
			return samInvalid("securityDefinitions must be a dictionary.")
		}
	}
	paths := asMap(doc["paths"])
	for _, path := range slices.Sorted(maps.Keys(paths)) {
		contents, ok := conditionalContents(paths[path])
		if !ok {
			return samInvalid("Value of Fn::If must be a list.")
		}
		for _, c := range contents {
			item, isMap := c.(map[string]any)
			if !isMap {
				return samInvalid("Value of '%s' path must be a dictionary according to Swagger spec.", path)
			}
			if err := a.validatePathItem(path, item); err != nil {
				return err
			}
		}
	}

	return a.validateEndpointConfig(doc)
}

func (a *samAPI) validateEndpointConfig(doc map[string]any) error {
	if a.props["DisableExecuteApiEndpoint"] == nil {
		return nil
	}
	if v, has := doc[samKeyEndpointExt]; has {
		if _, isMap := v.(map[string]any); !isMap {
			return samInvalid("Invalid OpenAPI definition: The value of 'x-amazon-apigateway-endpoint-configuration' " +
				"should be a map.")
		}
	}
	for i, s := range asList(doc["servers"]) {
		if v, has := asMap(s)[samKeyEndpointExt]; has {
			if _, isMap := v.(map[string]any); !isMap {
				return samInvalid("Invalid OpenAPI definition of 'servers[%d]': The value of "+
					"'x-amazon-apigateway-endpoint-configuration' should be a map.", i)
			}
		}
	}

	return nil
}

func (a *samAPI) validatePathItem(path string, item map[string]any) error {
	if a.props[samAuthKey] == nil && a.props[samKeyCorsProp] == nil {
		return nil
	}
	for _, method := range slices.Sorted(maps.Keys(item)) {
		switch {
		case method == "parameters", method == "summary", method == "description", method == "servers",
			method == "$ref", strings.HasPrefix(method, "x-") && method != samAnyMethodKey:
			continue
		}
		if _, isMap := item[method].(map[string]any); !isMap {
			return samInvalid("Definition of method '%s' for path '%s' should be a map.", method, path)
		}
	}

	return nil
}

func validateBucketShape(id string, b map[string]any) error {
	if raw, has := b[samKeyProps]; has && raw != nil {
		props, isMap := raw.(map[string]any)
		if !isMap {
			return samErr(id, "Properties should be a map.")
		}
		if nc, hasNC := props[samKeyNotifCfg]; hasNC && nc != nil {
			ncm, ncMap := nc.(map[string]any)
			if !ncMap {
				return samErr(id, "Property 'NotificationConfiguration' should be a map.")
			}
			if lc, hasLC := ncm[samKeyLambdaCfg]; hasLC && lc != nil {
				if _, isList := lc.([]any); !isList {
					return samErr(id, "Invalid type for LambdaConfigurations. Must be a list.")
				}
			}
		}
	}
	if deps, has := b[samKeyDependsOn]; has {
		switch d := deps.(type) {
		case string:
		case []any:
			if !isStringList(d) {
				return samErr(id, "Invalid type for field 'DependsOn'. Expected a string or list of strings.")
			}
		default:
			return samErr(id, "Invalid type for field 'DependsOn'. Expected a string or list of strings.")
		}
	}

	return nil
}

func validateGatewayResponse(apiID, typ string, resp map[string]any) error {
	if sc := resp["StatusCode"]; sc != nil {
		switch v := sc.(type) {
		case float64:
		case string:
			if _, err := strconv.Atoi(v); err != nil {
				return samErr(apiID, "Property 'StatusCode' must be numeric")
			}
		case map[string]any:
		default:
			return samErr(apiID, "Property 'StatusCode' must be numeric")
		}
	}
	rp, has := resp["ResponseParameters"]
	if !has || rp == nil {
		return nil
	}
	m, isMap := rp.(map[string]any)
	if !isMap {
		return samErr(apiID, "Property 'GatewayResponses.%s.ResponseParameters' should be a map.", typ)
	}
	for k := range m {
		if !slices.Contains([]string{samKeyHeaders, "Paths", samKQueryStrings}, k) {
			return samErr(apiID, "Invalid gateway response parameter '%s'", k)
		}
	}

	return nil
}

func validateAuthorizerShape(apiID, name string, v any) error {
	cfg := asMap(v)
	if id, has := cfg["Identity"]; has && id != nil {
		if _, isMap := id.(map[string]any); !isMap {
			return samErr(apiID, "Auth.Authorizers.<Authorizer>.Identity must be a dict (LambdaTokenAuthorizationIdentity, "+
				"LambdaRequestAuthorizationIdentity or CognitoAuthorizationIdentity).")
		}
	}
	if sc, has := cfg["AuthorizationScopes"]; has && sc != nil {
		if _, isList := sc.([]any); !isList {
			return samErr(apiID, "Property 'Authorizers.%s.AuthorizationScopes' should be a list.", name)
		}
	}

	return nil
}

func validateOptionsResponses(path string, opts any) error {
	op := asMap(opts)
	resp, has := op[samKeyResponses]
	if !has {
		return nil
	}
	responses, isMap := resp.(map[string]any)
	if !isMap {
		return samInvalid("Invalid responses in options method for path %s: It should be a map.", path)
	}
	for _, code := range slices.Sorted(maps.Keys(responses)) {
		if h, hasHeaders := asMap(responses[code])["headers"]; hasHeaders {
			if _, isDict := h.(map[string]any); !isDict {
				return samInvalid("Value of response's headers in options method for path %s must be a dictionary "+
					"according to Swagger spec.", path)
			}
		}
	}

	return nil
}

func validateHTTPIdentity(apiID, name string, identity any) error {
	if identity == nil {
		return nil
	}
	id, isMap := identity.(map[string]any)
	if !isMap {
		return samErr(apiID, "Property 'Authorizers.%s.Identity' should be a map.", name)
	}
	for _, k := range []string{samKeyHeaders, samKQueryStrings, samKeyStageVars, "Context"} {
		v, has := id[k]
		if !has {
			continue
		}
		list, isList := v.([]any)
		if !isList {
			return samErr(apiID, "Property 'Authorizers.%s.Identity.%s' should be a list.", name, k)
		}
		for i, e := range list {
			if _, isStr := e.(string); !isStr {
				return samErr(apiID, "Property 'Authorizers.%s.Identity.%s[%d]' should be a string.", name, k, i)
			}
		}
	}

	return nil
}

// passthroughCondition resolves DeploymentPreference.PassthroughCondition: a boolean, a Ref to a parameter
// default or an Fn::FindInMap over template Mappings.
func (t *samTranslator) passthroughCondition(id string, v any) (bool, error) {
	if b, ok := v.(bool); ok {
		return b, nil
	}
	m, isMap := v.(map[string]any)
	if !isMap {
		return false, samErr(id, "Invalid value for property PassthroughCondition.")
	}
	resolved, ok := t.resolvePassthrough(m)
	if !ok {
		return false, samErr(id, "Unsupported intrinsic: the only intrinsic functions supported for property "+
			"PassthroughCondition are FindInMap and parameter Refs.")
	}

	return passthroughBool(id, resolved)
}

func passthroughBool(id string, v any) (bool, error) {
	switch b := v.(type) {
	case bool:
		return b, nil
	case string:
		switch strings.ToLower(b) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
	}

	return false, samErr(id, "Invalid value for property PassthroughCondition.")
}

func (t *samTranslator) resolvePassthrough(m map[string]any) (any, bool) {
	if len(m) != 1 {
		return nil, false
	}
	if name, ok := m[samKeyRef].(string); ok {
		def, has := t.params[name]

		return def, has
	}
	args, ok := m["Fn::FindInMap"].([]any)
	if !ok || len(args) != 3 {
		return nil, false
	}
	keys := make([]string, 0, len(args))
	for _, a := range args {
		switch k := a.(type) {
		case string:
			keys = append(keys, k)
		case map[string]any:
			r, resolvedOK := t.resolvePassthrough(k)
			s, isStr := r.(string)
			if !resolvedOK || !isStr {
				return nil, false
			}
			keys = append(keys, s)
		default:
			return nil, false
		}
	}
	val, found := asMap(asMap(t.mappings[keys[0]])[keys[1]])[keys[2]]

	return val, found
}
