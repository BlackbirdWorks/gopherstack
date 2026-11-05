package cloudformation

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
)

const (
	samImplicitHTTPAPI = "ServerlessHttpApi"
	samDefaultStage    = "$default"
	samTypeHTTPAPI     = "AWS::ApiGatewayV2::Api"
	samTypeHTTPStage   = "AWS::ApiGatewayV2::Stage"
	samEventHTTPAPI    = "HttpApi"
	samKeyAPIID        = "ApiId"
	samKeyRouteSetting = "RouteSettings"
	samKeyCors         = "CorsConfiguration"
	samKeyTags         = "Tags"
	samKeyPayload      = "PayloadFormatVersion"
	samKeyTimeout      = "TimeoutInMillis"
	samAnyVerb         = "ANY"
	samHTTPAPITag      = "httpapi:createdBy"
)

func samHTTPAPIProps() map[string]bool {
	return keySet(samKeyStageName, samKeyStageVars, samKeyCorsProp, samKeyCors, samKeyDesc, "DefaultRouteSettings",
		samKeyRouteSetting, "AccessLogSettings", samKeyTags, "DisableExecuteApiEndpoint", "FailOnWarnings", "Name",
		samAuthKey, "DefinitionBody", "DefinitionUri", samDomainKey)
}

type samHTTPRoute struct {
	timeout  any
	payload  any
	auth     map[string]any
	settings map[string]any
	fnID     string
	fn       string
	target   string
	event    string
	path     string
	method   string
}

func (r samHTTPRoute) key() string {
	if r.path == "" {
		return samDefaultStage
	}

	return r.method + " " + r.path
}

func (r samHTTPRoute) docPath() string {
	if r.path == "" {
		return samDefaultStage
	}

	return r.path
}

func (r samHTTPRoute) docMethod() string {
	if r.path == "" || r.method == samAnyVerb {
		return samAnyMethodKey
	}

	return strings.ToLower(r.method)
}

// samHTTPAPI accumulates one AWS::Serverless::HttpApi (or the implicit
// ServerlessHttpApi) until every HttpApi event has been seen.
type samHTTPAPI struct {
	raw       map[string]any
	props     map[string]any
	id        string
	stageName string
	routes    []samHTTPRoute
	managed   bool
}

func (t *samTranslator) registerHTTPAPI(id string, r map[string]any) error {
	props := t.resProps("HttpApi", r)
	if err := rejectUnknown(id, props, samHTTPAPIProps()); err != nil {
		return err
	}
	if v, ok := props["DisableExecuteApiEndpoint"]; ok {
		if _, isBool := v.(bool); !isBool && !isIntrinsicValue(v) {
			return samErr(id, "Type of property 'DisableExecuteApiEndpoint' is invalid.")
		}
	}
	if v, ok := props[samKeyRouteSetting]; ok {
		if _, isMap := v.(map[string]any); !isMap {
			return samErr(id, "Property 'RouteSettings' should be a map.")
		}
	}
	for _, k := range []string{samKeyCorsProp, samKeyCors} {
		if v, ok := props[k]; ok {
			props[k] = t.paramDefault(v)
		}
	}
	if props["DefinitionBody"] != nil && props["DefinitionUri"] != nil {
		return samErr(id, "Specify either 'DefinitionUri' or 'DefinitionBody' property and not both.")
	}
	stage := samDefaultStage
	if v, ok := props[samKeyStageName]; ok {
		s, isStr := v.(string)
		if !isStr || s == "" {
			return samErr(id, "StageName must be a literal string.")
		}
		stage = s
	}
	managed := props["DefinitionBody"] == nil && props["DefinitionUri"] == nil
	t.httpAPIs[id] = &samHTTPAPI{id: id, stageName: stage, props: props, managed: managed, raw: r}

	return nil
}

func (t *samTranslator) httpAPIFor(fnID string, apiID any) (*samHTTPAPI, error) {
	if apiID == nil {
		if t.httpAPIs[samImplicitHTTPAPI] == nil {
			props := t.resProps("HttpApi", nil)
			if err := rejectUnknown(samImplicitHTTPAPI, props, samHTTPAPIProps()); err != nil {
				return nil, err
			}
			t.httpAPIs[samImplicitHTTPAPI] = &samHTTPAPI{
				id: samImplicitHTTPAPI, stageName: samDefaultStage, props: props, managed: true,
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
		samKeyTimeout, samKeyRouteSetting, "Stage", samAuthKey))
	if err != nil {
		return err
	}
	path, _ := props["Path"].(string)
	method, _ := props["Method"].(string)
	if (path == "") != (method == "") {
		return samErr(ev.id, "Event [%s] of type HttpApi requires both 'Path' and 'Method', or neither.", name)
	}
	if path == samDefaultStage {
		path = ""
	}
	api, err := t.httpAPIFor(ev.id, props[samKeyAPIID])
	if err != nil {
		return err
	}
	rt := samHTTPRoute{fnID: ev.id, target: ev.invokeRef(), event: name, path: path, method: strings.ToUpper(method),
		timeout: props[samKeyTimeout], settings: asMap(props[samKeyRouteSetting]), auth: asMap(props[samAuthKey])}
	rt.payload = props[samKeyPayload]
	for _, other := range api.routes {
		if other.key() == rt.key() {
			return samErr(ev.id, "Event [%s] duplicates route '%s' on HttpApi [%s].", name, rt.key(), api.id)
		}
	}
	rt.fn = ev.target()
	api.routes = append(api.routes, rt)

	return nil
}

// defaultCoversFunction reports whether the API already routes $default to
// the function, in which case SAM skips that function's per-route permissions.
func (a *samHTTPAPI) defaultCoversFunction(rt samHTTPRoute) bool {
	for _, other := range a.routes {
		if other.path == "" && other.fn == rt.fn {
			return true
		}
	}
	body := asMap(a.props["DefinitionBody"])
	item := asMap(asMap(body["paths"])[samDefaultStage])
	uri := asMap(asMap(asMap(item[samAnyMethodKey])[samIntegrationKey])[samKeyURI])
	sub, _ := uri[samKeyFnSub].(string)

	return strings.Contains(sub, "${"+rt.target+"}")
}

func (t *samTranslator) putHTTPPermissions(a *samHTTPAPI) error {
	for _, rt := range a.routes {
		if rt.path != "" && a.defaultCoversFunction(rt) {
			continue
		}
		perm := lambdaPermission(rt.fn, "apigateway.amazonaws.com", httpAPISourceArn(a.id, rt))
		if err := t.put(samPermissionID(rt.fnID+rt.event, ""), perm); err != nil {
			return err
		}
	}

	return nil
}

func httpAPISourceArn(apiID string, rt samHTTPRoute) map[string]any {
	path := ""
	if rt.path != "" {
		path = permissionPath(rt.path)
	}
	method := permissionMethod(rt.method)
	if rt.path == "" {
		method = "*"
	}

	return executeAPISourceArn(apiID, "*", method, path)
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
	t.pass = passAttrs(a.raw)
	defer func() { t.pass = nil }()
	api, err := a.apiProps()
	if err != nil {
		return err
	}
	httpAPI := map[string]any{samKeyType: samTypeHTTPAPI, samKeyProps: api}
	if a.raw != nil {
		carryAttrs(a.raw, httpAPI)
	}
	if err = t.put(a.id, httpAPI); err != nil {
		return err
	}
	if err = t.putHTTPAuthorizerPermissions(a); err != nil {
		return err
	}
	if err = t.putHTTPPermissions(a); err != nil {
		return err
	}
	stageID := samHTTPStageID(a.id, a.stageName)
	t.refs[a.id+".Stage"] = stageID
	if err = t.put(stageID, map[string]any{samKeyType: samTypeHTTPStage, samKeyProps: a.stageProps()}); err != nil {
		return err
	}

	return t.emitHTTPDomain(a)
}

func samHTTPStageID(apiID, stage string) string {
	switch {
	case stage == samDefaultStage:
		return apiID + "ApiGatewayDefaultStage"
	case isAlnum(stage):
		return apiID + stage + "Stage"
	default:
		return samLogicalID(apiID+"Stage", stage)
	}
}

// apiProps builds the AWS::ApiGatewayV2::Api properties: the OpenAPI Body SAM
// generates (or BodyS3Location) plus FailOnWarnings.
func (a *samHTTPAPI) apiProps() (map[string]any, error) {
	out := map[string]any{}
	if v, ok := a.props["FailOnWarnings"]; ok && v != false {
		out["FailOnWarnings"] = v
	}
	if uri, ok := a.props["DefinitionUri"]; ok {
		if err := a.checkURIOnly(); err != nil {
			return nil, err
		}
		loc, err := s3Location(a.id, "DefinitionUri", uri, "Bucket", "Key", "Version")
		if err != nil {
			return nil, err
		}
		out["BodyS3Location"] = loc
		a.disableEndpointFromProps(out)

		return out, nil
	}
	doc, err := a.openAPI()
	if err != nil {
		return nil, err
	}
	out["Body"] = doc

	return out, nil
}

func (a *samHTTPAPI) disableEndpointFromProps(out map[string]any) {
	if v, ok := a.props["DisableExecuteApiEndpoint"]; ok {
		out["DisableExecuteApiEndpoint"] = v
	}
}

// checkURIOnly rejects properties SAM can only apply to an inline DefinitionBody.
func (a *samHTTPAPI) checkURIOnly() error {
	for _, k := range []string{samKeyCorsProp, samAuthKey, samKeyTags, attrNameName, samKeyDesc} {
		if a.props[k] != nil {
			return samErr(a.id, "%s works only with inline OpenApi specified in the 'DefinitionBody' property.", k)
		}
	}
	if a.props["DisableExecuteApiEndpoint"] != nil {
		return samErr(a.id, "DisableExecuteApiEndpoint works only within 'DefinitionBody' property.")
	}

	return nil
}

func (a *samHTTPAPI) stageProps() map[string]any {
	tags := map[string]any{}
	if a.props["DefinitionUri"] == nil {
		tags[samHTTPAPITag] = samCreatedBy
	}
	maps.Copy(tags, asMap(a.props[samKeyTags]))
	tags[samHTTPAPITag] = samCreatedBy
	stage := map[string]any{samKeyAPIID: samRef(a.id), samKeyStageName: a.stageName, "AutoDeploy": true,
		samKeyTags: tags}
	for _, k := range []string{samKeyStageVars, "DefaultRouteSettings", "AccessLogSettings"} {
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

// openAPI builds the HTTP API OpenAPI document: the user's DefinitionBody or a
// fresh skeleton, with event integrations, Auth, Cors, Tags, Name and
// Description merged in.
func (a *samHTTPAPI) openAPI() (map[string]any, error) {
	doc, err := a.baseDocument()
	if err != nil {
		return nil, err
	}
	ed := &samOpenAPI{doc: doc, id: a.id}
	for _, rt := range a.routes {
		if err = a.addRoute(ed, rt); err != nil {
			return nil, err
		}
	}
	if err = a.applyAPIProperties(ed); err != nil {
		return nil, err
	}

	return ed.finish(), nil
}

func (a *samHTTPAPI) baseDocument() (map[string]any, error) {
	body := a.props["DefinitionBody"]
	if body == nil {
		return samOpenAPISkeleton(), nil
	}
	doc, err := deepCopyMap(body)
	if err != nil || !samValidOpenAPI(doc) {
		return nil, samErr(a.id, "Invalid OpenApi document. Invalid values or missing keys for 'openapi' or "+
			"'paths' in 'DefinitionBody'.")
	}

	return doc, nil
}

func decodeJSONMap(raw string) (map[string]any, error) {
	var out map[string]any
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}

	return out, nil
}

func deepCopyMap(v any) (map[string]any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err = json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}

	return out, nil
}

func (a *samHTTPAPI) addRoute(ed *samOpenAPI, rt samHTTPRoute) error {
	path, method := rt.docPath(), rt.docMethod()
	if a.managed && ed.hasIntegration(path, method) {
		return samErr(rt.event, "API method '%s' defined multiple times for path '%s'.", rt.method, rt.path)
	}
	ed.addLambdaIntegration(path, method, rt)
	if rt.auth != nil {
		if err := a.addRouteAuth(ed, rt, path, method); err != nil {
			return err
		}
	}

	return nil
}

func (a *samHTTPAPI) applyAPIProperties(ed *samOpenAPI) error {
	auth := asMap(a.props[samAuthKey])
	if cors, ok := a.props[samKeyCorsProp]; ok {
		if err := ed.addCors(cors); err != nil {
			return err
		}
	} else if cors, ok = a.props[samKeyCors]; ok {
		if err := ed.addCors(cors); err != nil {
			return err
		}
	}
	if auth != nil {
		if err := a.applyAuth(ed, auth); err != nil {
			return err
		}
	}
	tags := maps.Clone(asMap(a.props[samKeyTags]))
	if tags == nil {
		tags = map[string]any{}
	}
	tags[samHTTPAPITag] = samCreatedBy
	if err := ed.addTags(tags); err != nil {
		return err
	}
	if v, ok := a.props["DisableExecuteApiEndpoint"]; ok {
		ed.addEndpointConfig(v)
	}
	if v, ok := a.props["Name"]; ok {
		if err := ed.setTitle(v); err != nil {
			return err
		}
	}
	if v, ok := a.props[samKeyDesc]; ok {
		if err := ed.setDescription(v); err != nil {
			return err
		}
	}
	if a.props["FailOnWarnings"] == true {
		ed.renameDefaultPath()
	}

	return nil
}
