package cloudformation

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
)

const (
	samImplicitAPI    = "ServerlessRestApi"
	samImplicitStage  = "Prod"
	samAnyMethodKey   = "x-amazon-apigateway-any-method"
	samDeploymentStub = "Stage"
)

func samAPIPassthrough() map[string]bool {
	return keySet("Name", "Variables", "TracingEnabled", "CacheClusterEnabled", "CacheClusterSize",
		"MethodSettings", "EndpointConfiguration", "BinaryMediaTypes", samKeyDesc)
}

func samAPIHandled() map[string]bool {
	return keySet(samKeyStageName, "DefinitionBody", "DefinitionUri", "OpenApiVersion", "Cors", samAuthKey)
}

type samRoute struct{ fnID, path, method, authorizer string }

// samAPI accumulates one AWS::Serverless::Api (or the implicit
// ServerlessRestApi) until every Api event has been seen.
type samAPI struct {
	auth      *samAuth
	props     map[string]any
	id        string
	stageName string
	routes    []samRoute
	explicit  bool
}

func (a *samAPI) addRoute(rt samRoute) {
	a.routes = append(a.routes, rt)
}

func (t *samTranslator) registerAPI(id string, r map[string]any) error {
	props := t.resProps("Api", r)
	if err := rejectUnknown(id, props, samAPIPassthrough(), samAPIHandled()); err != nil {
		return err
	}
	stage, _ := props[samKeyStageName].(string)
	if stage == "" {
		return samErr(id, "Missing required property 'StageName'.")
	}
	if props["DefinitionUri"] != nil && props["Cors"] != nil {
		return samErr(id, "Cors requires an inline DefinitionBody, not DefinitionUri.")
	}
	t.apis[id] = &samAPI{id: id, stageName: stage, props: props, explicit: true}

	return nil
}

func (t *samTranslator) apiFor(fnID string, restAPIID any) (*samAPI, error) {
	if restAPIID == nil {
		if t.apis[samImplicitAPI] == nil {
			props := t.resProps("Api", nil)
			if err := rejectUnknown(samImplicitAPI, props, samAPIPassthrough(), samAPIHandled()); err != nil {
				return nil, err
			}
			t.apis[samImplicitAPI] = &samAPI{id: samImplicitAPI, stageName: samImplicitStage, props: props}
		}

		return t.apis[samImplicitAPI], nil
	}
	ref, _ := asMap(restAPIID)[samKeyRef].(string)
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
	if err := t.prepareAuth(a); err != nil {
		return err
	}
	body, err := a.body()
	if err != nil {
		return err
	}
	rest := a.restProps()
	if loc, ok := a.props["DefinitionUri"]; ok {
		s3, locErr := s3Location(a.id, "DefinitionUri", loc, "Bucket", "Key", "Version")
		if locErr != nil {
			return locErr
		}
		rest["BodyS3Location"] = s3
	} else {
		rest["Body"] = body
	}
	if err = t.put(a.id, map[string]any{samKeyType: samTypeRestAPI, samKeyProps: rest}); err != nil {
		return err
	}
	depID := a.id + "Deployment" + samHash(body)
	if err = t.put(depID, map[string]any{samKeyType: samTypeDeploy, samKeyProps: map[string]any{
		"RestApiId":     samRef(a.id),
		samKeyDesc:      "RestApi deployment id: " + strings.TrimPrefix(depID, a.id+"Deployment"),
		samKeyStageName: samDeploymentStub,
	}}); err != nil {
		return err
	}
	stageID := a.id + a.stageName + "Stage"
	t.refs[a.id+".Stage"] = stageID
	t.refs[a.id+".Deployment"] = depID

	return t.put(stageID, map[string]any{samKeyType: samTypeStage, samKeyProps: a.stageProps(depID)})
}

func (a *samAPI) restProps() map[string]any {
	rest := map[string]any{}
	for _, k := range []string{attrNameName, "EndpointConfiguration", "BinaryMediaTypes", samKeyDesc} {
		if v, ok := a.props[k]; ok {
			rest[k] = v
		}
	}
	if ep, ok := rest["EndpointConfiguration"].(string); ok {
		rest["EndpointConfiguration"] = map[string]any{"Types": []any{ep}}
	}

	return rest
}

// prepareAuth parses Auth, checks every event's authorizer exists and emits authorizer permissions.
func (t *samTranslator) prepareAuth(a *samAPI) error {
	var err error
	if a.auth, err = t.parseAPIAuth(a.id, a.props); err != nil {
		return err
	}
	for _, rt := range a.routes {
		if n := rt.authorizer; n != "" && n != samAuthNone && (a.auth == nil || a.auth.defs[n] == nil) {
			return samErr(rt.fnID, "Authorizer '%s' is not defined in Auth.Authorizers of API [%s].", n, a.id)
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
		"RestApiId":     samRef(a.id),
		"DeploymentId":  samRef(depID),
		samKeyStageName: a.stageName,
	}
	for _, k := range []string{"Variables", "TracingEnabled", "CacheClusterEnabled", "CacheClusterSize",
		"MethodSettings"} {
		if v, ok := a.props[k]; ok {
			stage[k] = v
		}
	}

	return stage
}

// body returns the Swagger 2.0 document: the user's DefinitionBody with every
// Api event path merged in, or a fresh document built from the events.
func (a *samAPI) body() (map[string]any, error) {
	doc := map[string]any{}
	if def := asMap(a.props["DefinitionBody"]); def != nil {
		raw, err := json.Marshal(def)
		if err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &doc); err != nil {
			return nil, err
		}
	} else {
		doc["swagger"] = "2.0"
		doc["info"] = map[string]any{"version": "1.0", "title": samRef("AWS::StackName")}
	}
	paths := asMap(doc["paths"])
	if paths == nil {
		paths = map[string]any{}
	}
	a.addRoutePaths(paths)
	doc["paths"] = paths
	if cors, ok := a.props["Cors"]; ok {
		preflightAuth := ""
		if a.auth != nil && a.auth.corsPreflight {
			preflightAuth = a.auth.defaultAuth
		}
		if err := addCorsPreflight(a.id, paths, cors, preflightAuth); err != nil {
			return nil, err
		}
	}
	a.addSecurityDefinitions(doc)

	return doc, nil
}

func (a *samAPI) authPerms() map[string]map[string]any {
	if a.auth == nil {
		return nil
	}

	return a.auth.perms
}

func (a *samAPI) addRoutePaths(paths map[string]any) {
	for _, rt := range a.routes {
		item := asMap(paths[rt.path])
		if item == nil {
			item = map[string]any{}
		}
		verb := strings.ToLower(rt.method)
		if verb == "any" {
			verb = samAnyMethodKey
		}
		op := map[string]any{
			samKeyResponses: map[string]any{},
			"x-amazon-apigateway-integration": map[string]any{
				"httpMethod":    "POST",
				samKeyTypeLower: "aws_proxy",
				"uri": map[string]any{samKeyFnSub: "arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/" +
					"2015-03-31/functions/${" + rt.fnID + ".Arn}/invocations"},
			},
		}
		if sec := a.auth.authorizerFor(rt); sec != "" {
			op["security"] = security(sec)
		}
		item[verb] = op
		paths[rt.path] = item
	}
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
