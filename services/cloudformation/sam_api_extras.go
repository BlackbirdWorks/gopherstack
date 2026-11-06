package cloudformation

import (
	"regexp"
	"slices"
	"strings"
)

const (
	samKeyBinaryMedia   = "x-amazon-apigateway-binary-media-types"
	samKeyGatewayResp   = "x-amazon-apigateway-gateway-responses"
	samKeyEndpointExt   = "x-amazon-apigateway-endpoint-configuration"
	samKeyDisableExec   = "disableExecuteApiEndpoint"
	samKeyOpenAPIVer    = "OpenApiVersion"
	samTypeUsagePlan    = "AWS::ApiGateway::UsagePlan"
	samTypeGatewayKey   = "AWS::ApiGateway::ApiKey"
	samTypeUsagePlanKey = "AWS::ApiGateway::UsagePlanKey"
	samSharedUsagePlan  = "ServerlessUsagePlan"
	samSharedAPIKey     = "ServerlessApiKey"
	samSharedPlanKey    = "ServerlessUsagePlanKey"
)

var samSwaggerVersionRe = regexp.MustCompile(`\A[2-3](\.\d)(\.\d)?$`)

// samAPIRestKeys are the Api properties copied onto the RestApi besides the handled ones.
func samAPIRestKeys() []string {
	return []string{attrNameName, samKeyDesc, "BinaryMediaTypes", "MinimumCompressionSize", "ApiKeySourceType",
		"FailOnWarnings", samKMode, samKPolicy, samKSecurityPolicy, "EndpointAccessMode"}
}

func samAPIStageKeys() []string {
	return []string{"Variables", "TracingEnabled", "CacheClusterEnabled", "CacheClusterSize", "MethodSettings",
		"AccessLogSetting", "CanarySetting"}
}

// openAPIVersion is the OpenApiVersion property when it names a 3.x version, else "".
func (a *samAPI) openAPIVersion() string {
	switch v := a.props[samKeyOpenAPIVer].(type) {
	case string:
		return v
	case float64:
		return strings.TrimRight(strings.TrimRight(pyJSONNumber(v), "0"), ".")
	}

	return ""
}

func (a *samAPI) validateOpenAPIVersion() error {
	v, has := a.props[samKeyOpenAPIVer]
	if !has {
		return nil
	}
	if a.openAPIVersion() == "" || !samSwaggerVersionRe.MatchString(a.openAPIVersion()) {
		_ = v

		return samErr(a.id, "The OpenApiVersion value must be of the format '3.0.0'.")
	}

	return nil
}

// endpointConfiguration builds RestApi.EndpointConfiguration and the endpointConfigurationTypes parameter.
func (a *samAPI) endpointConfiguration(rest map[string]any) error {
	ep, has := a.props["EndpointConfiguration"]
	if !has || ep == nil {
		return nil
	}
	cfg, isMap := ep.(map[string]any)
	if !isMap || cfg["Type"] == nil {
		rest["EndpointConfiguration"] = map[string]any{samKTypes: []any{ep}}
		rest["Parameters"] = map[string]any{"endpointConfigurationTypes": ep}

		return nil
	}
	rest["Parameters"] = map[string]any{"endpointConfigurationTypes": cfg["Type"]}
	out := map[string]any{samKTypes: []any{cfg["Type"]}}
	if cfg["VPCEndpointIds"] != nil && cfg["VpcEndpointIds"] != nil {
		return samErr(a.id, "'VPCEndpointIds' and 'VpcEndpointIds' cannot be used together in EndpointConfiguration.")
	}
	for _, k := range []string{"VPCEndpointIds", "VpcEndpointIds"} {
		if cfg[k] != nil {
			out["VpcEndpointIds"] = cfg[k]
		}
	}
	if cfg["IpAddressType"] != nil {
		out["IpAddressType"] = cfg["IpAddressType"]
	}
	rest["EndpointConfiguration"] = out

	return nil
}

// addBinaryMediaTypes writes the swagger extension with "~1" unescaped to "/".
func addBinaryMediaTypes(doc map[string]any, media any) {
	doc[samKeyBinaryMedia] = unescapeMedia(media)
}

func unescapeMedia(v any) any {
	switch t := v.(type) {
	case string:
		return strings.ReplaceAll(t, "~1", "/")
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = unescapeMedia(e)
		}

		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[strings.ReplaceAll(k, "~1", "/")] = unescapeMedia(e)
		}

		return out
	}

	return v
}

func (a *samAPI) addModels(doc map[string]any) error {
	models := asMap(a.props[samKModels])
	if models == nil {
		return nil
	}
	defs := asMap(doc["definitions"])
	if _, isV3 := doc["openapi"]; isV3 {
		comps := asMap(doc["components"])
		if comps == nil {
			comps = map[string]any{}
			doc["components"] = comps
		}
		defs = asMap(comps["schemas"])
		if defs == nil {
			defs = map[string]any{}
		}
		comps["schemas"] = defs
	}
	if defs == nil {
		defs = map[string]any{}
	}
	for name, m := range models {
		schema := asMap(m)
		if schema == nil {
			return samErr(a.id, "Invalid value for 'Models' property")
		}
		if schema[samKeyTypeLower] == nil {
			return samErr(a.id, "'Models' schema is missing 'type'.")
		}
		defs[strings.ToLower(name)] = schema
	}
	if _, isV3 := doc["openapi"]; !isV3 {
		doc["definitions"] = defs
	}

	return nil
}

func samGatewayResponseKeys() []string {
	return []string{"ResponseParameters", "ResponseTemplates", "StatusCode"}
}

func (a *samAPI) addGatewayResponses(doc map[string]any) error {
	gw := asMap(a.props["GatewayResponses"])
	if gw == nil {
		return nil
	}
	out := asMap(doc[samKeyGatewayResp])
	if out == nil {
		out = map[string]any{}
	}
	for typ, v := range gw {
		resp := asMap(v)
		if resp == nil {
			return samErr(
				a.id,
				"Invalid property type for GatewayResponses. Expected an object of type 'GatewayResponse'.",
			)
		}
		for k := range resp {
			if !containsString(samGatewayResponseKeys(), k) {
				return samErr(a.id, "Invalid property '%s' in 'GatewayResponses' property '%s'.", k, typ)
			}
		}
		if err := validateGatewayResponse(a.id, typ, resp); err != nil {
			return err
		}
		params := map[string]any{}
		prefixes := map[string]string{samKeyHeaders: "header.", "Paths": "path.", samKQueryStrings: "querystring."}
		for prop, prefix := range prefixes {
			for k, val := range asMap(asMap(resp["ResponseParameters"])[prop]) {
				params["gatewayresponse."+prefix+k] = val
			}
		}
		entry := map[string]any{
			"responseParameters": params,
			"responseTemplates":  orEmptyMap(resp["ResponseTemplates"]),
		}
		if sc := resp["StatusCode"]; sc != nil {
			entry["statusCode"] = pyJSONScalarString(sc)
		}
		out[typ] = entry
	}
	doc[samKeyGatewayResp] = out

	return nil
}

func orEmptyMap(v any) any {
	if v == nil {
		return map[string]any{}
	}

	return v
}

func pyJSONScalarString(v any) any {
	if f, ok := v.(float64); ok {
		return pyJSONNumber(f)
	}

	return v
}

// addDisableExecuteEndpoint sets the swagger extension: under servers for OpenAPI 3, at the top level otherwise.
func addDisableExecuteEndpoint(doc map[string]any, disabled any) {
	if openapi, _ := doc["openapi"].(string); openapi != "" && samOpenAPIVersionRe.MatchString(openapi) {
		servers, _ := doc["servers"].([]any)
		if len(servers) == 0 {
			servers = []any{map[string]any{}}
		}
		for _, s := range servers {
			sm := asMap(s)
			ext := asMap(sm[samKeyEndpointExt])
			if ext == nil {
				ext = map[string]any{}
			}
			ext[samKeyDisableExec] = disabled
			sm[samKeyEndpointExt] = ext
		}
		doc["servers"] = servers

		return
	}
	ext := asMap(doc[samKeyEndpointExt])
	if ext == nil {
		ext = map[string]any{}
	}
	ext[samKeyDisableExec] = disabled
	doc[samKeyEndpointExt] = ext
}

// openAPIPostprocess moves swagger sections to their OpenAPI 3 homes when the document is OpenAPI 3.
func (a *samAPI) openAPIPostprocess(doc map[string]any) {
	if doc["swagger"] != nil {
		return
	}
	version, _ := doc["openapi"].(string)
	if version == "" {
		version = a.openAPIVersion()
	}
	if version == "" || !samOpenAPIVersionRe.MatchString(version) {
		return
	}
	if defs := asMap(doc["securityDefinitions"]); len(defs) > 0 {
		comps := asMap(doc["components"])
		if comps == nil {
			comps = map[string]any{}
		}
		comps["securitySchemes"] = defs
		doc["components"] = comps
		delete(doc, "securityDefinitions")
	}
	if defs := asMap(doc["definitions"]); len(defs) > 0 {
		comps := asMap(doc["components"])
		if comps == nil {
			comps = map[string]any{}
		}
		comps["schemas"] = defs
		doc["components"] = comps
		delete(doc, "definitions")
	}
	for _, v := range asMap(doc["paths"]) {
		options := asMap(asMap(v)[samKOptions])
		if options == nil {
			continue
		}
		delete(options, "produces")
		delete(options, "consumes")
		headers := asMap(asMap(asMap(options[samKeyResponses])["200"])["headers"])
		for h, val := range headers {
			headers[h] = map[string]any{samKeySchema: val}
		}
	}
}

// samUsagePlanSpec is a validated Auth.UsagePlan.
type samUsagePlanSpec struct {
	props map[string]any
	mode  string
}

// parseUsagePlan reads Auth.UsagePlan; a nil spec means no plan is generated.
func (a *samAPI) parseUsagePlan() (samUsagePlanSpec, bool, error) {
	raw, has := asMap(a.props[samAuthKey])["UsagePlan"]
	if !has || raw == nil {
		return samUsagePlanSpec{}, false, nil
	}
	plan := asMap(raw)
	if plan == nil {
		return samUsagePlanSpec{}, false, samErr(a.id, "'UsagePlan' must be a dictionary")
	}
	if err := rejectUnknown(a.id, plan, keySet("CreateUsagePlan", "UsagePlanName", samKeyDesc, "Quota", scopeTags,
		"Throttle")); err != nil {
		return samUsagePlanSpec{}, false, samErr(a.id, "Invalid property for 'UsagePlan'")
	}
	mode, _ := plan["CreateUsagePlan"].(string)
	switch mode {
	case "":
		return samUsagePlanSpec{}, false, samErr(a.id, "'CreateUsagePlan' is a required field for UsagePlan.")
	case "NONE":
		return samUsagePlanSpec{}, false, nil
	case "PER_API", samKSHARED:
		return samUsagePlanSpec{props: plan, mode: mode}, true, nil
	}

	return samUsagePlanSpec{}, false, samErr(a.id, "'CreateUsagePlan' accepts one of [SHARED PER_API NONE].")
}

// putUsagePlan emits the UsagePlan, ApiKey and UsagePlanKey of Auth.UsagePlan.CreateUsagePlan.
func (t *samTranslator) putUsagePlan(a *samAPI, stageID string) error {
	spec, ok, err := a.parseUsagePlan()
	if err != nil || !ok {
		return err
	}
	planID, keyID, planKeyID := a.id+"UsagePlan", a.id+"ApiKey", a.id+"UsagePlanKey"
	apiStages := []any{map[string]any{"ApiId": samRef(a.id), samDeploymentStub: samRef(stageID)}}
	keyStages := []any{map[string]any{samKRestAPIID: samRef(a.id), samKeyStageName: samRef(stageID)}}
	deps := []any{a.id}
	if spec.mode == samKSHARED {
		planID, keyID, planKeyID = samSharedUsagePlan, samSharedAPIKey, samSharedPlanKey
		deps = t.sharedPlan.join(a.id, apiStages[0], keyStages[0])
		apiStages, keyStages = t.sharedPlan.stages, t.sharedPlan.keys
		saved := t.pass
		t.pass = t.sharedPlan.combine(saved, t.conditions)
		defer func() { t.pass = saved }()
	}
	props := map[string]any{"ApiStages": apiStages}
	for _, k := range []string{"UsagePlanName", samKeyDesc, "Quota", scopeTags, "Throttle"} {
		if spec.props[k] != nil {
			props[k] = spec.props[k]
		}
	}
	resources := map[string]map[string]any{
		planID: {samKeyType: samTypeUsagePlan, samKeyProps: props, samKeyDependsOn: deps},
		keyID: {samKeyType: samTypeGatewayKey, samKeyDependsOn: []any{planID},
			samKeyProps: map[string]any{samKeyEnabled: true, "StageKeys": keyStages}},
		planKeyID: {
			samKeyType:      samTypeUsagePlanKey,
			samKeyDependsOn: []any{keyID},
			samKeyProps: map[string]any{
				samKeyKeyID:   samRef(keyID),
				"KeyType":     "API_KEY",
				"UsagePlanId": samRef(planID),
			},
		},
	}
	for id, res := range resources {
		if spec.mode == samKSHARED {
			delete(t.out, id)
		}
		if err = t.put(id, res); err != nil {
			return err
		}
	}

	return nil
}

type samSharedPlan struct {
	delPolicy      string
	updPolicy      string
	apis           []string
	stages         []any
	keys           []any
	conditions     []string
	anyWithoutCond bool
}

const samSharedPlanCondition = "SharedUsagePlanCondition"

// addCondition records one API's Condition (empty for none) and keeps the template's shared condition current.
func (p *samSharedPlan) addCondition(cond string, conditions map[string]any) {
	switch {
	case p.anyWithoutCond:
	case cond == "":
		p.anyWithoutCond = true
		delete(conditions, samSharedPlanCondition)
	case !slices.Contains(p.conditions, cond):
		p.conditions = append(p.conditions, cond)
		if len(p.conditions) == 1 {
			conditions[samSharedPlanCondition] = map[string]any{samKeyCondition: cond}

			break
		}
		or := make([]any, 0, len(p.conditions))
		for _, c := range slices.Sorted(slices.Values(p.conditions)) {
			or = append(or, map[string]any{samKeyCondition: c})
		}
		conditions[samSharedPlanCondition] = map[string]any{samFnOr: or}
	}
}

// combine folds one API's Condition, DeletionPolicy and UpdateReplacePolicy into the shared plan's attributes.
func (p *samSharedPlan) combine(attrs map[string]any, conditions map[string]any) map[string]any {
	if dp := asString(attrs[samKeyDelPolicy]); dp != "" && (p.delPolicy == "" || dp == samDeletionRetain) {
		p.delPolicy = dp
	}
	if up := asString(attrs[samKeyUpdPolicy]); up != "" {
		if p.updPolicy == "" || up == samDeletionRetain ||
			(up == samDeletionSnapshot && p.updPolicy == samDeletionDelete) {
			p.updPolicy = up
		}
	}
	p.addCondition(asString(attrs[samKeyCondition]), conditions)
	out := map[string]any{}
	if p.delPolicy != "" {
		out[samKeyDelPolicy] = p.delPolicy
	}
	if p.updPolicy != "" {
		out[samKeyUpdPolicy] = p.updPolicy
	}
	if len(p.conditions) > 0 && !p.anyWithoutCond {
		out[samKeyCondition] = samSharedPlanCondition
	}

	return out
}

func appendUniqueJSON(list []any, v any) []any {
	if containsJSON(list, v) {
		return list
	}

	return append(list, v)
}

// join registers an API in the shared plan and returns the plan's DependsOn list.
func (p *samSharedPlan) join(apiID string, stage, key any) []any {
	if !slices.Contains(p.apis, apiID) {
		p.apis = append(p.apis, apiID)
	}
	p.stages = appendUniqueJSON(p.stages, stage)
	p.keys = appendUniqueJSON(p.keys, key)
	deps := make([]any, len(p.apis))
	for i, id := range p.apis {
		deps[i] = id
	}

	return deps
}
