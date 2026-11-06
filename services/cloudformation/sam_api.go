package cloudformation

import (
	"maps"
	"slices"
	"strings"
)

const (
	samImplicitAPI    = "ServerlessRestApi"
	samImplicitStage  = "Prod"
	samAnyMethodKey   = "x-amazon-apigateway-any-method"
	samDeploymentStub = "Stage"
	samDomainKey      = "Domain"
)

func samAPIPassthrough() map[string]bool {
	return keySet(
		slices.Concat(
			samAPIRestKeys(),
			samAPIStageKeys(),
			[]string{"EndpointConfiguration", samKModels, "MergeDefinitions",
				"GatewayResponses", "DisableExecuteApiEndpoint", samKeyTags, "PropagateTags"},
		)...)
}

func samAPIHandled() map[string]bool {
	return keySet(samKeyStageName, "DefinitionBody", "DefinitionUri", samKeyOpenAPIVer, samKeyCorsProp, samAuthKey,
		samDomainKey)
}

type samRoute struct {
	target                         string
	timeout                        any
	scopes                         any
	auth                           map[string]any
	apiKey                         *bool
	fnID, path, method, authorizer string
	// roleID is set for state machine routes: the API Gateway invoke role.
	roleID    string
	condition string
	model     *samRequestModel
	params    []samReqParam
	unescape  bool
}

// samAPI accumulates one AWS::Serverless::Api (or the implicit
// ServerlessRestApi) until every Api event has been seen.
type samAPI struct {
	stageRaw    any
	auth        *samAuth
	raw         map[string]any
	props       map[string]any
	id          string
	stageName   string
	routes      []samRoute
	delPolicies []string
	updPolicies []string
	explicit    bool
}

func (rt samRoute) invokeRef() string {
	if rt.target != "" {
		return rt.target
	}

	return rt.fnID + ".Arn"
}

// permSuffix is the stage part of event permission ids: the literal StageName, or "Stage" for an intrinsic.
func (a *samAPI) permSuffix() string {
	if a.stageName == "" {
		return samDeploymentStub
	}

	return a.stageName
}

// eventIDValue is the API id as one event's permission names it: a literal when that event gave a plain string.
func (a *samAPI) eventIDValue(restAPIID any) any {
	if _, plain := restAPIID.(string); plain {
		return a.id
	}

	return samRef(a.id)
}

// addRoute records an Api event; pass carries the owning function's Condition and policies.
func (a *samAPI) addRoute(rt samRoute, pass map[string]any) {
	rt.condition, _ = pass[samKeyCondition].(string)
	a.routes = append(a.routes, rt)
	dp, _ := pass[samKeyDelPolicy].(string)
	up, _ := pass[samKeyUpdPolicy].(string)
	a.delPolicies = append(a.delPolicies, dp)
	a.updPolicies = append(a.updPolicies, up)
}

func (t *samTranslator) registerAPI(id string, r map[string]any) error {
	props := t.resProps("Api", r)
	if err := rejectUnknown(id, props, samAPIPassthrough(), samAPIHandled()); err != nil {
		return err
	}
	stage := props[samKeyStageName]
	if s, isStr := stage.(string); stage == nil || (isStr && s == "") {
		return samErr(id, "Missing required property 'StageName'.")
	}
	if err := validateAPIProps(id, props); err != nil {
		return err
	}
	if a, has := props[samAuthKey]; has {
		props[samAuthKey] = t.resolveParamRefs(a)
	}
	api := &samAPI{id: id, stageRaw: stage, props: props, explicit: true, raw: r}
	api.stageName, _ = stage.(string)
	if err := api.validateOpenAPIVersion(); err != nil {
		return err
	}
	t.apis[id] = api

	return nil
}

func (t *samTranslator) apiFor(fnID string, restAPIID any) (*samAPI, error) {
	if restAPIID == nil {
		if t.apis[samImplicitAPI] == nil {
			props := t.resProps("Api", nil)
			if err := rejectUnknown(samImplicitAPI, props, samAPIPassthrough(), samAPIHandled()); err != nil {
				return nil, err
			}
			t.apis[samImplicitAPI] = &samAPI{id: samImplicitAPI, stageName: samImplicitStage,
				stageRaw: samImplicitStage, props: props}
		}

		t.addImplicitTags(t.apis[samImplicitAPI].props)

		return t.apis[samImplicitAPI], nil
	}
	ref, plain := restAPIID.(string)
	if !plain {
		ref, _ = asMap(restAPIID)[samKeyRef].(string)
	}
	api := t.apis[ref]
	if api == nil {
		return nil, samErr(fnID, "RestApiId must reference an AWS::Serverless::Api in this template.")
	}

	return api, nil
}

func (t *samTranslator) finalizeAPIs() error {
	for _, id := range slices.Sorted(maps.Keys(t.apis)) {
		if err := t.emitAPI(t.apis[id]); err != nil {
			return err
		}
	}

	return nil
}

func (t *samTranslator) emitAPI(a *samAPI) error {
	t.pass = passAttrs(a.raw)
	defer func() { t.pass = nil }()
	before := t.idSet()
	if tags := propagatedTags(a.props); tags != nil {
		defer func() { t.propagateNewTags(before, tags) }()
	}
	if !a.explicit {
		maps.Copy(t.pass, t.implicitAttrs(a))
	}
	if err := t.prepareAuth(a); err != nil {
		return err
	}
	body, err := a.body(t)
	if err != nil {
		return err
	}
	rest, err := a.restProps()
	if err != nil {
		return err
	}
	if loc, ok := a.props["DefinitionUri"]; ok {
		s3, locErr := s3Location(a.id, "DefinitionUri", loc, "Bucket", samKeyKey, "Version")
		if locErr != nil {
			return locErr
		}
		rest["BodyS3Location"] = s3
	} else {
		rest["Body"] = body
	}
	restAPI := map[string]any{samKeyType: samTypeRestAPI, samKeyProps: rest}
	if a.raw != nil {
		carryAttrs(a.raw, restAPI)
	}
	if err = t.put(a.id, restAPI); err != nil {
		return err
	}
	hashed := a.deploymentHashInput(body, rest["BodyS3Location"])
	depID := samLogicalID(a.id+"Deployment", hashed)
	deployProps := map[string]any{
		samKRestAPIID: samRef(a.id),
		samKeyDesc:    "RestApi deployment id: " + samDigest(hashed),
	}
	if a.props[samKeyOpenAPIVer] == nil {
		deployProps[samKeyStageName] = samDeploymentStub
	}
	if err = t.put(depID, map[string]any{samKeyType: samTypeDeploy, samKeyProps: deployProps}); err != nil {
		return err
	}
	stageID := samRESTStageID(a.id, a.stageName)
	t.refs[a.id+".Stage"] = stageID
	t.refs[a.id+".Deployment"] = depID

	if err = t.put(stageID, map[string]any{samKeyType: samTypeStage, samKeyProps: a.stageProps(depID)}); err != nil {
		return err
	}
	if err = t.putUsagePlan(a, stageID); err != nil {
		return err
	}

	return t.emitRESTDomain(a)
}

func (a *samAPI) restProps() (map[string]any, error) {
	rest := map[string]any{}
	for _, k := range samAPIRestKeys() {
		if v, ok := a.props[k]; ok {
			rest[k] = v
		}
	}
	if err := a.endpointConfiguration(rest); err != nil {
		return nil, err
	}

	return rest, nil
}

// prepareAuth parses Auth, checks every event's authorizer exists and emits authorizer permissions.
func (t *samTranslator) prepareAuth(a *samAPI) error {
	var err error
	if a.auth, err = t.parseAPIAuth(a.id, a.props); err != nil {
		return err
	}
	for _, rt := range a.routes {
		if n := rt.authorizer; n != "" && n != samAuthNone && n != samSecIAM &&
			(a.auth == nil || a.auth.defs[n] == nil) {
			return samErr(rt.fnID, "Authorizer '%s' is not defined in Auth.Authorizers of API [%s].", n, a.id)
		}
		if rt.authorizer == samAuthNone && (a.auth == nil || a.auth.defaultAuth == "") {
			return samErr(rt.fnID, "Event with id [%s] is invalid. Unable to set Authorizer on API method [%s] for "+
				"path [%s] because 'NONE' is only a valid value when a DefaultAuthorizer on the API is specified.",
				rt.fnID, strings.ToLower(rt.method), rt.path)
		}
		if rt.apiKey != nil && !*rt.apiKey && asMap(a.props[samAuthKey])["ApiKeyRequired"] == nil {
			return samErr(rt.fnID, "Event with id [%s] is invalid. Unable to set ApiKeyRequired [False] on API method "+
				"[%s] for path [%s] because the related API does not specify any ApiKeyRequired.",
				rt.fnID, strings.ToLower(rt.method), rt.path)
		}
	}
	perms := a.authPerms()
	for _, id := range slices.Sorted(maps.Keys(perms)) {
		if err = t.put(id, perms[id]); err != nil {
			return err
		}
	}

	return nil
}

func (a *samAPI) stageProps(depID string) map[string]any {
	stage := map[string]any{
		samKRestAPIID:   samRef(a.id),
		"DeploymentId":  samRef(depID),
		samKeyStageName: a.stageRaw,
	}
	for _, k := range samAPIStageKeys() {
		if v, ok := a.props[k]; ok {
			stage[k] = v
		}
	}
	if tags := asMap(a.props[samKeyTags]); tags != nil {
		stage[samKeyTags] = samTagList(tags)
	}

	return stage
}

// managed reports whether SAM owns the swagger: no DefinitionBody or DefinitionUri was given.
func (a *samAPI) managed() bool {
	return a.props["DefinitionBody"] == nil && a.props["DefinitionUri"] == nil
}

// body returns the Swagger/OpenAPI document: the user's DefinitionBody or a fresh skeleton, with Api event
// paths, Cors, Auth, Models, GatewayResponses, BinaryMediaTypes and DisableExecuteApiEndpoint merged in.
func (a *samAPI) body(t *samTranslator) (map[string]any, error) {
	doc, err := a.baseDocument()
	if err != nil {
		return nil, err
	}
	if a.props["DefinitionBody"] != nil {
		if err = a.validateSwaggerDocument(doc); err != nil {
			return nil, err
		}
	}
	paths, isMap := doc["paths"].(map[string]any)
	if doc["paths"] != nil && !isMap {
		return nil, samErr(a.id, "Value of paths must be a dictionary according to Swagger spec.")
	}
	if paths == nil {
		paths = map[string]any{}
	}
	if err = a.addRoutePaths(doc, paths); err != nil {
		return nil, err
	}
	if len(paths) > 0 || doc["paths"] != nil || a.props["DefinitionBody"] == nil {
		doc["paths"] = paths
	}
	if err = a.decorate(t, doc, paths); err != nil {
		return nil, err
	}
	a.applyConditions(t, paths)
	// SAM post-processes once per feature that rewrites the document (Auth, Models) and once more at the end;
	// the CORS header schema wrapper therefore nests once per pass.
	passes := 1
	for _, k := range []string{samAuthKey, samKModels} {
		if a.props[k] != nil {
			passes++
		}
	}
	for range passes {
		a.openAPIPostprocess(doc)
	}

	return doc, nil
}

// decorate adds Cors, Auth and the document-level Api properties to the document.
func (a *samAPI) decorate(t *samTranslator, doc, paths map[string]any) error {
	if cors, ok := a.props[samKeyCorsProp]; ok {
		if err := addCorsPreflight(a.id, paths, cors); err != nil {
			return err
		}
	}
	a.addSecurityDefinitions(doc)
	if err := a.applyDefaultSecurity(doc); err != nil {
		return err
	}

	return a.addDocExtensions(t, doc)
}

// baseDocument is the user's DefinitionBody (copied) or SAM's skeleton.
func (a *samAPI) baseDocument() (map[string]any, error) {
	if def := asMap(a.props["DefinitionBody"]); def != nil {
		return deepCopyMap(def)
	}
	doc := map[string]any{"info": map[string]any{"version": "1.0", "title": samRef("AWS::StackName")}}
	if v := a.openAPIVersion(); v != "" && samOpenAPIVersionRe.MatchString(v) && a.managed() {
		doc["openapi"] = v
	} else {
		doc["swagger"] = "2.0"
	}

	return doc, nil
}

// addDocExtensions applies the Api properties that live in the swagger document.
func (a *samAPI) addDocExtensions(t *samTranslator, doc map[string]any) error {
	if m := a.props["BinaryMediaTypes"]; m != nil && asList(m) != nil {
		addBinaryMediaTypes(doc, m)
		doc[samKeyBinaryMedia] = t.resolveParamRefs(doc[samKeyBinaryMedia])
	}
	if err := a.addGatewayResponses(doc); err != nil {
		return err
	}
	if err := a.addModels(doc); err != nil {
		return err
	}
	if v, ok := a.props["DisableExecuteApiEndpoint"]; ok {
		addDisableExecuteEndpoint(doc, v)
	}

	return nil
}

func (a *samAPI) authPerms() map[string]map[string]any {
	if a.auth == nil {
		return nil
	}

	return a.auth.perms
}

func (a *samAPI) addRoutePaths(doc, paths map[string]any) error {
	merge := a.props["MergeDefinitions"] == true
	if !a.managed() && !merge {
		return nil
	}
	for _, rt := range a.routes {
		if err := a.mergeRoute(doc, paths, rt); err != nil {
			return err
		}
	}

	return nil
}

func (a *samAPI) mergeRoute(doc, paths map[string]any, rt samRoute) error {
	item := asMap(paths[rt.path])
	if item == nil {
		item = map[string]any{}
	}
	verb := routeVerb(rt.method)
	existing := asMap(item[verb])
	if existing != nil && existing[samIntegrationKey] != nil {
		if a.managed() {
			return samErr(rt.fnID, "API method '%s' defined multiple times for path '%s'.", rt.method, rt.path)
		}

		return nil
	}
	op := routeOperation(rt)
	rt.model.apply(doc, op)
	if rt.condition != samCondUnconditional {
		if integ := asMap(op[samIntegrationKey]); integ != nil {
			integ["uri"] = samIf(rt.condition, integ["uri"])
		}
	}
	a.addIAMCredentials(rt, op)
	if sec := a.eventSecurity(rt); len(sec) > 0 {
		op["security"] = sec
	}
	if rt.apiKey != nil && *rt.apiKey {
		addAPIKeyDefinition(doc)
	}
	if rt.authorizer == samSecIAM {
		addIAMDefinition(doc)
	}
	for k, v := range existing {
		if _, has := op[k]; !has {
			op[k] = v
		}
	}
	item[verb] = op
	paths[rt.path] = item

	return a.applyEventResourcePolicy(doc, rt, item)
}

func (a *samAPI) addSecurityDefinitions(doc map[string]any) {
	if a.auth == nil || len(a.auth.defs) == 0 {
		return
	}
	defs := asMap(doc["securityDefinitions"])
	if defs == nil {
		defs = map[string]any{}
	}
	maps.Copy(defs, a.auth.defs)
	doc["securityDefinitions"] = defs
}

const (
	samAPIKeyScheme = "api_key"
	samAPIKeyHeader = "x-api-key"
)

// routeOperation is the swagger operation for one route: a Lambda proxy
// integration, or the Step Functions StartExecution integration.
func routeOperation(rt samRoute) map[string]any {
	if rt.roleID != "" {
		return map[string]any{
			samKeyResponses: map[string]any{
				"200": map[string]any{samKeyDescLower: "OK"}, "400": map[string]any{samKeyDescLower: "Bad Request"},
			},
			samIntegrationKey: smIntegration(rt),
		}
	}
	integ := map[string]any{
		samKeyHTTPMethod: samHTTPPost,
		samKeyTypeLower:  "aws_proxy",
		samKeyURI:        lambdaInvokeURI(rt.invokeRef()),
	}
	op := map[string]any{samKeyResponses: map[string]any{}, samIntegrationKey: integ}
	if rt.timeout != nil {
		integ["timeoutInMillis"] = rt.timeout
	}
	if len(rt.params) > 0 {
		params := make([]any, 0, len(rt.params))
		var cacheKeys []any
		for _, p := range rt.params {
			params = append(params, map[string]any{"in": p.in, samKeyNameLower: p.name, "required": p.required,
				samKeyTypeLower: samSwaggerString})
			if p.caching {
				cacheKeys = append(cacheKeys, p.full)
			}
		}
		op["parameters"] = params
		if len(cacheKeys) > 0 {
			integ["cacheKeyParameters"] = cacheKeys
		}
	}

	return op
}

// deploymentHashInput is the string SAM hashes for the Deployment id: the
// swagger plus the custom domain, "||"-joined. SAM stringifies the swagger with
// Python's dict repr (insertion-ordered); this uses canonical sorted JSON, so
// the hash differs from SAM's while keeping the same sensitivity to changes.
func (a *samAPI) deploymentHashInput(body map[string]any, uri any) string {
	in := pyJSON(samPartitionLiteral(body))
	if uri != nil {
		in = pyJSON(uri)
	}
	if d := a.props[samDomainKey]; d != nil {
		in += "||" + pyJSON(d)
	}

	return in
}

func samRESTStageID(apiID, stage string) string {
	if isAlnum(stage) && stage != "" {
		return apiID + stage + samDeploymentStub
	}

	return samLogicalID(apiID+samDeploymentStub, stage)
}

func validateAPIProps(id string, props map[string]any) error {
	if body, has := props["DefinitionBody"]; has && body != nil {
		m, isMap := body.(map[string]any)
		if !isMap {
			return samErr(id, "Property 'DefinitionBody' should be a map.")
		}
		if isSAMIntrinsic(m) && props["DisableExecuteApiEndpoint"] != nil {
			return samInvalid("Invalid Swagger document or the Swagger document is not explicitly defined in " +
				"'DefinitionBody'.")
		}
	}
	if v, has := props["FailOnWarnings"]; has {
		if _, isBool := v.(bool); !isBool && !isSAMIntrinsic(asMap(v)) {
			return samErr(id, "Type of property 'FailOnWarnings' is invalid.")
		}
	}
	if props["DefinitionUri"] != nil && props["DefinitionBody"] != nil {
		return samErr(id, "Specify either 'DefinitionUri' or 'DefinitionBody' property and not both.")
	}
	if props["DefinitionUri"] != nil {
		for _, k := range []string{samKeyCorsProp, samAuthKey, "GatewayResponses", samKModels} {
			if props[k] != nil {
				return samErr(id, "%s works only with inline Swagger specified in 'DefinitionBody' property.", k)
			}
		}
		if props["DisableExecuteApiEndpoint"] != nil {
			return samErr(id, "DisableExecuteApiEndpoint works only within 'DefinitionBody' property.")
		}
	}

	return nil
}
