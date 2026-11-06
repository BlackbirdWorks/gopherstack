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
		samAuthKey, "DefinitionBody", "DefinitionUri", samDomainKey, samKeyPropagateTags)
}

type samHTTPRoute struct {
	timeout  any
	payload  any
	auth     map[string]any
	settings map[string]any
	pass     map[string]any
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
	stageRaw  any
	routes    []samHTTPRoute
	added     map[string]bool
	managed   bool
	plainRef  bool
}

// httpStageName reads StageName: a literal, or an intrinsic whose stage id carries no name.
func httpStageName(id string, props map[string]any) (string, any, error) {
	v, ok := props[samKeyStageName]
	if !ok {
		return samDefaultStage, samDefaultStage, nil
	}
	switch s := v.(type) {
	case string:
		if s != "" {
			return s, v, nil
		}
	case map[string]any:
		return "", v, nil
	}

	return "", nil, samErr(id, "StageName must be a literal string.")
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
	stage, stageRaw, err := httpStageName(id, props)
	if err != nil {
		return err
	}
	managed := props["DefinitionBody"] == nil && props["DefinitionUri"] == nil
	t.httpAPIs[id] = &samHTTPAPI{id: id, stageName: stage, stageRaw: stageRaw, props: props, managed: managed, raw: r}

	return nil
}

func (a *samHTTPAPI) idValue() any {
	if a.plainRef {
		return a.id
	}

	return samRef(a.id)
}

func (t *samTranslator) httpAPIFor(fnID string, apiID any) (*samHTTPAPI, error) {
	if apiID == nil {
		if t.httpAPIs[samImplicitHTTPAPI] == nil {
			props := t.resProps("HttpApi", nil)
			if err := rejectUnknown(samImplicitHTTPAPI, props, samHTTPAPIProps()); err != nil {
				return nil, err
			}
			t.httpAPIs[samImplicitHTTPAPI] = &samHTTPAPI{
				id:        samImplicitHTTPAPI,
				stageName: samDefaultStage,
				stageRaw:  samDefaultStage,
				props:     props,
				managed:   true,
			}
		}

		t.addImplicitTags(t.httpAPIs[samImplicitHTTPAPI].props)

		return t.httpAPIs[samImplicitHTTPAPI], nil
	}
	ref, plain := apiID.(string)
	if !plain {
		ref, _ = asMap(apiID)[samKeyRef].(string)
	}
	api := t.httpAPIs[ref]
	if api != nil && plain {
		api.plainRef = true
	}
	if api == nil {
		return nil, samErr(fnID, "ApiId must reference an AWS::Serverless::HttpApi in this template.")
	}

	return api, nil
}

func (t *samTranslator) httpAPIEvent(ev *samEventCtx, name string, props map[string]any) error {
	err := rejectUnknown(ev.id+name, props, keySet(samKeyAPIID, "Path", "Method", samKeyPayload,
		samKeyTimeout, samKeyRouteSetting, samDeploymentStub, samAuthKey))
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
	rt.pass = maps.Clone(t.pass)
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
		perm := lambdaPermission(rt.fn, "apigateway.amazonaws.com", httpAPISourceArn(a.idValue(), rt))
		saved := t.pass
		t.pass = rt.pass
		err := t.put(samPermissionID(rt.fnID+rt.event, ""), perm)
		t.pass = saved
		if err != nil {
			return err
		}
	}

	return nil
}

func httpAPISourceArn(apiID any, rt samHTTPRoute) map[string]any {
	path := ""
	if rt.path != "" {
		path = permissionPath(rt.path)
	}
	method := permissionMethod(rt.method)
	switch rt.path {
	case "":
		method = "*"
	case "/" + samDefaultStage:
		method, path = "", samDefaultStage
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
	if a.raw == nil {
		maps.Copy(t.pass, t.implicitHTTPAttrs(a))
	}
	api, err := a.apiProps(t)
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
		return apiID + stage + samDeploymentStub
	default:
		return samLogicalID(apiID+samDeploymentStub, stage)
	}
}

// apiProps builds the AWS::ApiGatewayV2::Api properties: the OpenAPI Body SAM
// generates (or BodyS3Location) plus FailOnWarnings.
func (a *samHTTPAPI) apiProps(t *samTranslator) (map[string]any, error) {
	out := map[string]any{}
	if v, ok := a.props["FailOnWarnings"]; ok && v != false {
		out["FailOnWarnings"] = v
	}
	if uri, ok := a.props["DefinitionUri"]; ok {
		if err := a.checkURIOnly(); err != nil {
			return nil, err
		}
		loc, err := s3Location(a.id, "DefinitionUri", uri, "Bucket", samKeyKey, "Version")
		if err != nil {
			return nil, err
		}
		out["BodyS3Location"] = loc
		a.disableEndpointFromProps(out)

		return out, nil
	}
	if body, ok := a.props["DefinitionBody"].(map[string]any); ok && isSAMIntrinsic(body) && a.needsNoEditor() {
		out["Body"] = body

		return out, nil
	}
	doc, err := a.openAPI(t)
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

// v2Tags are the tags of the stage and domain: the user's plus SAM's, only when SAM owns the OpenAPI body.
func (a *samHTTPAPI) v2Tags() map[string]any {
	if a.props["DefinitionUri"] != nil {
		return nil
	}
	tags := maps.Clone(asMap(a.props[samKeyTags]))
	if tags == nil {
		tags = map[string]any{}
	}
	tags[samHTTPAPITag] = samCreatedBy

	return tags
}

func (a *samHTTPAPI) stageProps() map[string]any {
	stage := map[string]any{samKeyAPIID: samRef(a.id), samKeyStageName: a.stageRaw, "AutoDeploy": true}
	if tags := a.v2Tags(); tags != nil {
		stage[samKeyTags] = tags
	}
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
		merged := any(mergeSAMGlobals(asMap(settings[rt.key()]), rt.settings))
		if cond := asString(rt.pass[samKeyCondition]); cond != samCondUnconditional {
			merged = samIf(cond, merged)
		}
		settings[rt.key()] = merged
	}
	if settings != nil {
		stage[samKeyRouteSetting] = settings
	}

	return stage
}

// openAPI builds the HTTP API OpenAPI document: the user's DefinitionBody or a
// fresh skeleton, with event integrations, Auth, Cors, Tags, Name and
// Description merged in.
func (a *samHTTPAPI) openAPI(t *samTranslator) (map[string]any, error) {
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
	a.applyConditions(t, ed)
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
	had := ed.hasIntegration(path, method)
	ed.addLambdaIntegration(path, method, rt)
	if !had {
		if a.added == nil {
			a.added = map[string]bool{}
		}
		a.added[path+" "+method] = true
	}
	if cond := asString(rt.pass[samKeyCondition]); cond != "" && !had {
		integ := asMap(asMap(asMap(ed.paths()[path])[method])[samIntegrationKey])
		integ[samKeyURI] = samIf(cond, integ[samKeyURI])
	}
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

func (a *samHTTPAPI) routeConditions() ([]string, bool) {
	set := map[string]bool{}
	all := len(a.routes) > 0
	for _, rt := range a.routes {
		if cond := asString(rt.pass[samKeyCondition]); cond != samCondUnconditional {
			set[cond] = true
		} else {
			all = false
		}
	}

	return slices.Sorted(maps.Keys(set)), all
}

// implicitHTTPAttrs are the resource attributes SAM derives for the implicit HTTP API from its functions.
func (t *samTranslator) implicitHTTPAttrs(a *samHTTPAPI) map[string]any {
	out := map[string]any{}
	if conds, all := a.routeConditions(); all {
		out[samKeyCondition] = t.combinedOrSingle(conds, samImplicitHTTPAPICond)
	}
	var del, upd []string
	for _, rt := range a.routes {
		del = append(del, asString(rt.pass[samKeyDelPolicy]))
		upd = append(upd, asString(rt.pass[samKeyUpdPolicy]))
	}
	if p := strongestPolicy(del, samDeletionRetain, samDeletionDelete); p != "" {
		out[samKeyDelPolicy] = p
	}
	if p := strongestPolicy(upd, samDeletionRetain, samDeletionSnapshot, samDeletionDelete); p != "" {
		out[samKeyUpdPolicy] = p
	}

	return out
}

// applyConditions wraps conditional functions' methods, and the paths whose methods are all conditional.
func (a *samHTTPAPI) applyConditions(t *samTranslator, ed *samOpenAPI) {
	paths := ed.paths()
	for _, rt := range a.routes {
		cond := asString(rt.pass[samKeyCondition])
		if cond == samCondUnconditional {
			continue
		}
		item := asMap(paths[rt.docPath()])
		if op, ok := item[rt.docMethod()]; ok && a.added[rt.docPath()+" "+rt.docMethod()] {
			item[rt.docMethod()] = samIf(cond, op)
		}
	}
	if !a.managed {
		return
	}
	byPath := map[string][]samHTTPRoute{}
	for _, rt := range a.routes {
		byPath[rt.docPath()] = append(byPath[rt.docPath()], rt)
	}
	for _, path := range slices.Sorted(maps.Keys(byPath)) {
		set := map[string]bool{}
		all := true
		for _, rt := range byPath[path] {
			if cond := asString(rt.pass[samKeyCondition]); cond != samCondUnconditional {
				set[cond] = true
			} else {
				all = false
			}
		}
		if all {
			conds := slices.Sorted(maps.Keys(set))
			paths[path] = samIf(t.combinedOrSingle(conds, samPathConditionName(a.id, path)), paths[path])
		}
	}
}

// needsNoEditor reports whether nothing has to be merged into a DefinitionBody given as an intrinsic.
func (a *samHTTPAPI) needsNoEditor() bool {
	if len(a.routes) > 0 {
		return false
	}
	for _, k := range []string{samKeyCorsProp, samKeyCors, samAuthKey, attrNameName, samKeyDesc,
		"DisableExecuteApiEndpoint"} {
		if a.props[k] != nil {
			return false
		}
	}

	return true
}
