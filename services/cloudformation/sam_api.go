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
	return keySet("StageName", "DefinitionBody", "DefinitionUri", "OpenApiVersion")
}

type samRoute struct{ fnID, path, method string }

// samAPI accumulates one AWS::Serverless::Api (or the implicit
// ServerlessRestApi) until every Api event has been seen.
type samAPI struct {
	props     map[string]any
	id        string
	stageName string
	routes    []samRoute
	explicit  bool
}

func (a *samAPI) addRoute(path, method, fnID string) {
	a.routes = append(a.routes, samRoute{fnID: fnID, path: path, method: method})
}

func (t *samTranslator) registerAPI(id string, r map[string]any) error {
	props := t.resProps("Api", r)
	if err := rejectUnknown(id, props, samAPIPassthrough(), samAPIHandled()); err != nil {
		return err
	}
	stage, _ := props["StageName"].(string)
	if stage == "" {
		return samErr(id, "Missing required property 'StageName'.")
	}
	t.apis[id] = &samAPI{id: id, stageName: stage, props: props, explicit: true}

	return nil
}

func (t *samTranslator) apiFor(fnID string, restAPIID any) (*samAPI, error) {
	if restAPIID == nil {
		if t.apis[samImplicitAPI] == nil {
			t.apis[samImplicitAPI] = &samAPI{id: samImplicitAPI, stageName: samImplicitStage, props: map[string]any{}}
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
	body, err := a.body()
	if err != nil {
		return err
	}
	rest := map[string]any{}
	for _, k := range []string{attrNameName, "EndpointConfiguration", "BinaryMediaTypes", samKeyDesc} {
		if v, ok := a.props[k]; ok {
			rest[k] = v
		}
	}
	if ep, ok := rest["EndpointConfiguration"].(string); ok {
		rest["EndpointConfiguration"] = map[string]any{"Types": []any{ep}}
	}
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
		"RestApiId": samRef(a.id),
		samKeyDesc:  "RestApi deployment id: " + strings.TrimPrefix(depID, a.id+"Deployment"),
		"StageName": samDeploymentStub,
	}}); err != nil {
		return err
	}
	stageID := a.id + a.stageName + "Stage"
	t.refs[a.id+".Stage"] = stageID
	t.refs[a.id+".Deployment"] = depID

	return t.put(stageID, map[string]any{samKeyType: samTypeStage, samKeyProps: a.stageProps(depID)})
}

func (a *samAPI) stageProps(depID string) map[string]any {
	stage := map[string]any{
		"RestApiId":    samRef(a.id),
		"DeploymentId": samRef(depID),
		"StageName":    a.stageName,
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
	for _, rt := range a.routes {
		item := asMap(paths[rt.path])
		if item == nil {
			item = map[string]any{}
		}
		verb := strings.ToLower(rt.method)
		if verb == "any" {
			verb = samAnyMethodKey
		}
		item[verb] = map[string]any{
			"responses": map[string]any{},
			"x-amazon-apigateway-integration": map[string]any{
				"httpMethod": "POST",
				"type":       "aws_proxy",
				"uri": map[string]any{samKeyFnSub: "arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/" +
					"2015-03-31/functions/${" + rt.fnID + ".Arn}/invocations"},
			},
		}
		paths[rt.path] = item
	}
	doc["paths"] = paths

	return doc, nil
}
