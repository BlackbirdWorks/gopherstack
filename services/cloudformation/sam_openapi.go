package cloudformation

import (
	"maps"
	"regexp"
	"slices"
	"strings"
)

const (
	samOAPITagValue    = "x-amazon-apigateway-tag-value"
	samOAPICors        = "x-amazon-apigateway-cors"
	samOAPIEndpointCfg = "x-amazon-apigateway-endpoint-configuration"
	samOAPIAuthorizer  = "x-amazon-apigateway-authorizer"
	samOAPIPaths       = "paths"
	samOAPISecurity    = "security"
	samOAPIVersion     = "3.0.1"
)

var samOpenAPIVersionRe = regexp.MustCompile(`\A3(\.\d)(\.\d)?$`)

// samOpenAPI edits an HTTP API OpenAPI 3 document the way SAM's OpenApiEditor does.
type samOpenAPI struct {
	doc map[string]any
	id  string
}

func samOpenAPISkeleton() map[string]any {
	return map[string]any{
		"openapi":    samOAPIVersion,
		"info":       map[string]any{"version": "1.0", "title": samRef("AWS::StackName")},
		samOAPIPaths: map[string]any{},
	}
}

func samValidOpenAPI(doc map[string]any) bool {
	v, ok := doc["openapi"]
	if !ok || v == nil || v == "" {
		return false
	}
	_, hasPaths := doc[samOAPIPaths].(map[string]any)

	return hasPaths && samOpenAPIVersionRe.MatchString(openAPIVersionString(v))
}

func openAPIVersionString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strings.TrimRight(strings.TrimRight(formatFloat(t), "0"), ".")
	}

	return ""
}

func formatFloat(f float64) string { return pyJSONNumber(f) }

func (e *samOpenAPI) paths() map[string]any { return asMap(e.doc[samOAPIPaths]) }

func (e *samOpenAPI) hasIntegration(path, method string) bool {
	op := asMap(asMap(e.paths()[path])[method])

	return op[samIntegrationKey] != nil
}

// addLambdaIntegration adds the aws_proxy integration for a route unless the document already has one.
func (e *samOpenAPI) addLambdaIntegration(path, method string, rt samHTTPRoute) {
	if e.hasIntegration(path, method) {
		e.tuneIntegration(path, method, rt)

		return
	}
	paths := e.paths()
	item := asMap(paths[path])
	if item == nil {
		item = map[string]any{}
		paths[path] = item
	}
	op := asMap(item[method])
	if op == nil {
		op = map[string]any{}
		item[method] = op
	}
	integ := map[string]any{
		samKeyTypeLower:        "aws_proxy",
		samKeyHTTPMethod:       samHTTPPost,
		"payloadFormatVersion": "2.0",
		samKeyURI:              lambdaInvokeURI(rt.target),
	}
	op[samIntegrationKey] = integ
	e.tuneIntegration(path, method, rt)
	if path == samDefaultStage && method == samAnyMethodKey {
		op["isDefaultRoute"] = true
	}
	if _, ok := op[samKeyResponses]; !ok {
		op[samKeyResponses] = map[string]any{}
	}
	if params := pathParameters(rt.path); len(params) > 0 {
		e.addPathParameters(op, params)
	}
}

// tuneIntegration applies the event's TimeoutInMillis and PayloadFormatVersion to the route's integration.
func (e *samOpenAPI) tuneIntegration(path, method string, rt samHTTPRoute) {
	integ := asMap(asMap(asMap(e.paths()[path])[method])[samIntegrationKey])
	if integ == nil {
		return
	}
	if rt.timeout != nil {
		integ["timeoutInMillis"] = rt.timeout
	}
	if rt.payload != nil && rt.payload != "" {
		integ["payloadFormatVersion"] = rt.payload
	}
}

var samBracePath = regexp.MustCompile(`\{(.*?)\}`)

func pathParameters(path string) []string {
	matches := samBracePath.FindAllStringSubmatch(path, -1)
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		out = append(out, m[1])
	}

	return out
}

func (e *samOpenAPI) addPathParameters(op map[string]any, params []string) {
	existing, _ := op["parameters"].([]any)
	for _, p := range params {
		found := false
		for _, x := range existing {
			if xm := asMap(x); xm["name"] == p {
				xm["in"], xm["required"] = samLocPath, true
				found = true
			}
		}
		if !found {
			existing = append(existing, map[string]any{"name": p, "in": samLocPath, "required": true})
		}
	}
	op["parameters"] = existing
}

// lambdaInvokeURI is the apigateway Lambda invocation Fn::Sub for a function or alias logical id.
func lambdaInvokeURI(target string) map[string]any {
	return map[string]any{samKeyFnSub: "arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/" +
		"2015-03-31/functions/${" + target + "}/invocations"}
}

func (e *samOpenAPI) securitySchemes() map[string]any {
	comps := asMap(e.doc["components"])
	if comps == nil {
		comps = map[string]any{}
		e.doc["components"] = comps
	}
	schemes := asMap(comps["securitySchemes"])
	if schemes == nil {
		schemes = map[string]any{}
		comps["securitySchemes"] = schemes
	}

	return schemes
}

func (e *samOpenAPI) addTags(tags map[string]any) error {
	list, _ := e.doc["tags"].([]any)
	if raw, has := e.doc["tags"]; has && raw != nil && list == nil {
		return samErr(
			e.id,
			"Structure of the SAM template is invalid. Tags in OpenApi DefinitionBody needs to be a list.",
		)
	}
	for _, name := range slices.Sorted(maps.Keys(tags)) {
		if name == samHTTPAPITag {
			continue
		}
		list = setOpenAPITag(list, name, tags[name])
	}
	if v, ok := tags[samHTTPAPITag]; ok {
		list = setOpenAPITag(list, samHTTPAPITag, v)
	}
	e.doc["tags"] = list

	return nil
}

func setOpenAPITag(list []any, name string, value any) []any {
	for _, t := range list {
		if tm := asMap(t); tm["name"] == name {
			tm[samOAPITagValue] = value

			return list
		}
	}

	return append(list, map[string]any{"name": name, samOAPITagValue: value})
}

func (e *samOpenAPI) addEndpointConfig(disabled any) {
	servers, _ := e.doc["servers"].([]any)
	if len(servers) == 0 {
		servers = []any{map[string]any{}}
	}
	for _, s := range servers {
		sm := asMap(s)
		cfg := asMap(sm[samOAPIEndpointCfg])
		if cfg == nil {
			cfg = map[string]any{}
		}
		cfg["disableExecuteApiEndpoint"] = disabled
		sm[samOAPIEndpointCfg] = cfg
	}
	e.doc["servers"] = servers
}

func (e *samOpenAPI) info() map[string]any {
	info := asMap(e.doc["info"])
	if info == nil {
		info = map[string]any{}
		e.doc["info"] = info
	}

	return info
}

func (e *samOpenAPI) setTitle(v any) error {
	info := e.info()
	cur, has := info["title"]
	if has && !sameJSON(cur, samRef("AWS::StackName")) {
		return samErr(e.id, "Unable to set Name because it is already defined within inline OpenAPI specified in the "+
			"'DefinitionBody' property.")
	}
	info["title"] = v

	return nil
}

func (e *samOpenAPI) setDescription(v any) error {
	info := e.info()
	if d := info[samKeyDesc]; d != nil && d != "" {
		return samErr(e.id, "Unable to set Description because it is already defined within inline OpenAPI "+
			"specified in the 'DefinitionBody' property.")
	}
	info[samKeyDesc] = v

	return nil
}

func sameJSON(a, b any) bool { return pyJSON(a) == pyJSON(b) }

// renameDefaultPath turns "$default" into "/$default": with FailOnWarnings the import rejects the bare key.
func (e *samOpenAPI) renameDefaultPath() {
	paths := e.paths()
	if v, ok := paths[samDefaultStage]; ok {
		paths["/"+samDefaultStage] = v
		delete(paths, samDefaultStage)
	}
}

func samCorsKeys() []string {
	return []string{"AllowMethods", "AllowHeaders", samKeyOrigins, "MaxAge", "ExposeHeaders", samKeyAllowCreds}
}

func (e *samOpenAPI) addCors(cors any) error {
	cfg, _ := e.doc[samOAPICors].(map[string]any)
	if cfg == nil {
		cfg = map[string]any{}
	}
	props, intrinsic, err := e.corsProps(cors)
	if err != nil {
		return err
	}
	if intrinsic != nil {
		maps.Copy(cfg, intrinsic)
		e.doc[samOAPICors] = cfg

		return nil
	}
	if props[samKeyAllowCreds] == true && sameJSON(props[samKeyOrigins], []any{"*"}) {
		return samErr(e.id, "Unable to add Cors configuration because 'AllowCredentials' can not be true when "+
			"'AllowOrigin' is \"'*'\" or not set.")
	}
	for _, k := range samCorsKeys() {
		v, ok := props[k]
		if !ok || v == nil || (k == samKeyAllowCreds && v != true) {
			continue
		}
		cfg[strings.ToLower(k[:1])+k[1:]] = v
	}
	e.doc[samOAPICors] = cfg

	return nil
}

// corsProps normalises the Cors shorthand (bool, origin string, map); an intrinsic comes back
// as the already-lowered replacement for the whole configuration.
func (e *samOpenAPI) corsProps(cors any) (map[string]any, map[string]any, error) {
	switch c := cors.(type) {
	case bool:
		return map[string]any{samKeyOrigins: []any{"*"}}, nil, nil
	case string:
		return map[string]any{samKeyOrigins: []any{c}}, nil, nil
	case map[string]any:
		if isSAMIntrinsic(c) {
			lowered, lerr := lowerCorsKeys(c)

			return nil, lowered, lerr
		}
		for k := range c {
			if !slices.Contains(samCorsKeys(), k) {
				return nil, nil, samErr(e.id, "Invalid key '%s' for 'Cors' property.", k)
			}
		}

		return c, nil, nil
	default:
		return nil, nil, samErr(e.id, "Invalid value for 'Cors' property.")
	}
}

// lowerCorsKeys rewrites the capitalised CorsConfiguration keys inside an intrinsic to the OpenAPI spelling.
func lowerCorsKeys(v map[string]any) (map[string]any, error) {
	raw := pyJSON(v)
	for _, k := range samCorsKeys() {
		raw = strings.ReplaceAll(raw, `"`+k+`"`, `"`+strings.ToLower(k[:1])+k[1:]+`"`)
	}

	return decodeJSONMap(raw)
}

// isSAMIntrinsic reports whether m is a single-key Ref, Condition or Fn::* call.
func isSAMIntrinsic(m map[string]any) bool {
	if len(m) != 1 {
		return false
	}
	for k := range m {
		return k == samKeyRef || k == samKeyCondition || strings.HasPrefix(k, "Fn::")
	}

	return false
}

// finish drops empty containers the editor may have created and returns the document.
func (e *samOpenAPI) finish() map[string]any {
	if comps := asMap(e.doc["components"]); comps != nil {
		if len(asMap(comps["securitySchemes"])) == 0 {
			delete(comps, "securitySchemes")
		}
		if len(comps) == 0 {
			delete(e.doc, "components")
		}
	}
	if tags, _ := e.doc["tags"].([]any); len(tags) == 0 {
		delete(e.doc, "tags")
	}

	return e.doc
}
