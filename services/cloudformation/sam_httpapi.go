package cloudformation

import (
	"maps"
	"slices"
	"strings"
)

const (
	samImplicitHTTPAPI = "ServerlessHttpApi"
	samDefaultStage    = "$default"
	samTypeHTTPAPI     = "AWS::ApiGatewayV2::Api"
	samTypeHTTPStage   = "AWS::ApiGatewayV2::Stage"
	samTypeHTTPInteg   = "AWS::ApiGatewayV2::Integration"
	samTypeHTTPRoute   = "AWS::ApiGatewayV2::Route"
	samEventHTTPAPI    = "HttpApi"
	samKeyAPIID        = "ApiId"
	samKeyRouteSetting = "RouteSettings"
	samKeyCors         = "CorsConfiguration"
	samKeyTags         = "Tags"
	samKeyPayload      = "PayloadFormatVersion"
	samKeyTimeout      = "TimeoutInMillis"
	samAnyVerb         = "ANY"
)

func samHTTPAPIProps() map[string]bool {
	return keySet(samKeyStageName, "StageVariables", samKeyCors, samKeyDesc, "DefaultRouteSettings",
		samKeyRouteSetting, "AccessLogSettings", samKeyTags, "DisableExecuteApiEndpoint", "FailOnWarnings", "Name")
}

type samHTTPRoute struct {
	settings map[string]any
	timeout  any
	fnID     string
	event    string
	path     string
	method   string
	payload  string
}

func (r samHTTPRoute) key() string {
	if r.path == "" {
		return samDefaultStage
	}

	return r.method + " " + r.path
}

// samHTTPAPI accumulates one AWS::Serverless::HttpApi (or the implicit
// ServerlessHttpApi) until every HttpApi event has been seen.
type samHTTPAPI struct {
	props     map[string]any
	id        string
	stageName string
	routes    []samHTTPRoute
}

func (t *samTranslator) registerHTTPAPI(id string, r map[string]any) error {
	props := t.resProps("HttpApi", r)
	if err := rejectUnknown(id, props, samHTTPAPIProps()); err != nil {
		return err
	}
	stage := samDefaultStage
	if v, ok := props[samKeyStageName]; ok {
		s, isStr := v.(string)
		if !isStr || s == "" {
			return samErr(id, "StageName must be a literal string.")
		}
		stage = s
	}
	t.httpAPIs[id] = &samHTTPAPI{id: id, stageName: stage, props: props}

	return nil
}

func (t *samTranslator) httpAPIFor(fnID string, apiID any) (*samHTTPAPI, error) {
	if apiID == nil {
		if t.httpAPIs[samImplicitHTTPAPI] == nil {
			t.httpAPIs[samImplicitHTTPAPI] = &samHTTPAPI{
				id: samImplicitHTTPAPI, stageName: samDefaultStage, props: map[string]any{},
			}
		}

		return t.httpAPIs[samImplicitHTTPAPI], nil
	}
	ref, _ := asMap(apiID)[samKeyRef].(string)
	api := t.httpAPIs[ref]
	if api == nil {
		return nil, samErr(fnID, "ApiId must reference an AWS::Serverless::HttpApi in this template.")
	}

	return api, nil
}

func (t *samTranslator) httpAPIEvent(ev *samEventCtx, name string, props map[string]any) error {
	err := rejectUnknown(ev.id+name, props, keySet(samKeyAPIID, "Path", "Method", samKeyPayload,
		samKeyTimeout, samKeyRouteSetting))
	if err != nil {
		return err
	}
	path, _ := props["Path"].(string)
	method, _ := props["Method"].(string)
	if (path == "") != (method == "") {
		return samErr(ev.id, "Event [%s] of type HttpApi requires both 'Path' and 'Method', or neither.", name)
	}
	api, err := t.httpAPIFor(ev.id, props[samKeyAPIID])
	if err != nil {
		return err
	}
	rt := samHTTPRoute{fnID: ev.id, event: name, path: path, method: strings.ToUpper(method),
		timeout: props[samKeyTimeout], settings: asMap(props[samKeyRouteSetting])}
	if rt.payload, _ = props[samKeyPayload].(string); rt.payload == "" {
		rt.payload = "2.0"
	}
	for _, other := range api.routes {
		if other.key() == rt.key() {
			return samErr(ev.id, "Event [%s] duplicates route '%s' on HttpApi [%s].", name, rt.key(), api.id)
		}
	}
	api.routes = append(api.routes, rt)

	return t.put(
		ev.id+name+"Permission",
		lambdaPermission(ev.id, "apigateway.amazonaws.com", httpAPISourceArn(api.id, rt)),
	)
}

func httpAPISourceArn(apiID string, rt samHTTPRoute) map[string]any {
	suffix := "*/*"
	if rt.path != "" {
		suffix = "*/" + permissionMethod(rt.method) + permissionPath(rt.path)
	}

	return map[string]any{samKeyFnSub: "arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${" +
		apiID + "}/" + suffix}
}

func (t *samTranslator) finalizeHTTPAPIs() error {
	for _, id := range slices.Sorted(maps.Keys(t.httpAPIs)) {
		if err := t.emitHTTPAPI(t.httpAPIs[id]); err != nil {
			return err
		}
	}

	return nil
}

func (t *samTranslator) emitHTTPAPI(a *samHTTPAPI) error {
	api := map[string]any{"ProtocolType": "HTTP", "Name": samRef("AWS::StackName")}
	if v, ok := a.props["Name"]; ok {
		api["Name"] = v
	}
	for _, k := range []string{samKeyDesc, "DisableExecuteApiEndpoint"} {
		if v, ok := a.props[k]; ok {
			api[k] = v
		}
	}
	if cors, ok := a.props[samKeyCors]; ok {
		api[samKeyCors] = httpAPICors(cors)
	}
	if err := t.put(a.id, map[string]any{samKeyType: samTypeHTTPAPI, samKeyProps: api}); err != nil {
		return err
	}
	for _, rt := range a.routes {
		if err := t.emitHTTPRoute(a, rt); err != nil {
			return err
		}
	}
	stageID := a.id + "ApiGatewayDefaultStage"
	if a.stageName != samDefaultStage {
		stageID = a.id + a.stageName + "Stage"
	}
	t.refs[a.id+".Stage"] = stageID

	return t.put(stageID, map[string]any{samKeyType: samTypeHTTPStage, samKeyProps: a.stageProps()})
}

// httpAPICors expands the CorsConfiguration shorthand: true allows everything,
// a string names the single allowed origin, a map passes through.
func httpAPICors(v any) any {
	switch c := v.(type) {
	case bool:
		return map[string]any{"AllowOrigins": []any{"*"}, "AllowMethods": []any{"*"}, "AllowHeaders": []any{"*"}}
	case string:
		return map[string]any{"AllowOrigins": []any{c}}
	default:
		return v
	}
}

func (a *samHTTPAPI) stageProps() map[string]any {
	tags := map[string]any{"httpapi:createdBy": "SAM"}
	maps.Copy(tags, asMap(a.props[samKeyTags]))
	stage := map[string]any{samKeyAPIID: samRef(a.id), samKeyStageName: a.stageName, "AutoDeploy": true,
		samKeyTags: tags}
	for _, k := range []string{"StageVariables", "DefaultRouteSettings", "AccessLogSettings"} {
		if v, ok := a.props[k]; ok {
			stage[k] = v
		}
	}
	settings := maps.Clone(asMap(a.props[samKeyRouteSetting]))
	for _, rt := range a.routes {
		if len(rt.settings) == 0 {
			continue
		}
		if settings == nil {
			settings = map[string]any{}
		}
		settings[rt.key()] = mergeSAMGlobals(asMap(settings[rt.key()]), rt.settings)
	}
	if settings != nil {
		stage[samKeyRouteSetting] = settings
	}

	return stage
}

func (t *samTranslator) emitHTTPRoute(a *samHTTPAPI, rt samHTTPRoute) error {
	integID := rt.fnID + rt.event + "Integration"
	integ := map[string]any{
		samKeyAPIID:       samRef(a.id),
		"IntegrationType": "AWS_PROXY",
		"IntegrationUri": map[string]any{samKeyFnSub: "arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/" +
			"2015-03-31/functions/${" + rt.fnID + ".Arn}/invocations"},
		samKeyPayload: rt.payload,
	}
	if rt.timeout != nil {
		integ[samKeyTimeout] = rt.timeout
	}
	if err := t.put(integID, map[string]any{samKeyType: samTypeHTTPInteg, samKeyProps: integ}); err != nil {
		return err
	}

	return t.put(rt.fnID+rt.event+"Route", map[string]any{samKeyType: samTypeHTTPRoute, samKeyProps: map[string]any{
		samKeyAPIID: samRef(a.id),
		"RouteKey":  rt.key(),
		"Target":    map[string]any{samKeyFnSub: "integrations/${" + integID + "}"},
	}})
}
