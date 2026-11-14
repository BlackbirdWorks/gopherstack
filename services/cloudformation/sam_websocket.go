package cloudformation

import (
	"maps"
	"slices"
	"strings"
)

const (
	samTypeWSRoute       = "AWS::ApiGatewayV2::Route"
	samTypeWSIntegration = "AWS::ApiGatewayV2::Integration"
	samTypeWSAuthorizer  = "AWS::ApiGatewayV2::Authorizer"
	samWSDefaultStage    = "default"
	samWSTag             = "websocketapi:createdBy"
	samWSAuthCustom      = "CUSTOM"
	samWSAuthIAM         = "AWS_IAM"
	samWSConnect         = "$connect"
	samKeyAuthArn        = "AuthArn"
	samKeyFunctionArn    = "FunctionArn"
)

func samWebSocketProps() map[string]bool {
	return keySet("ApiKeySelectionExpression", samKeyAccessLog, samAuthKey, samKeyDefaultRoute, samKeyDesc,
		samKeyDisableEndpoint, samDomainKey, "DisableSchemaValidation", "FailOnWarnings", "IpAddressType",
		attrNameName, samKeyPropagateTags, "Routes", samKeyRouteSetting, "RouteSelectionExpression",
		samKeyStageName, samKeyStageVars, samKeyTags)
}

func (t *samTranslator) translateWebSocketAPI(id string, r map[string]any) error {
	props := t.resProps("WebSocketApi", r)
	if err := rejectUnknown(id, props, samWebSocketProps()); err != nil {
		return err
	}
	routes := asMap(t.resolveParamRefs(props["Routes"]))
	selection, err := validateWebSocketRequired(id, props, routes)
	if err != nil {
		return err
	}
	auth := asMap(t.resolveParamRefs(props[samAuthKey]))
	if auth != nil && routes[samWSConnect] == nil {
		return samErr(id, "Authorization is only available if there is a $connect route.")
	}
	stage := any(samWSDefaultStage)
	if s, has := props[samKeyStageName]; has && s != nil {
		stage = s
	}
	if stage == samDefaultStage {
		return samErr(id, "Stages cannot be named $default for WebSocket APIs.")
	}
	t.pass = passAttrs(r)
	defer func() { t.pass = nil }()

	apiTags := maps.Clone(asMap(props[samKeyTags]))
	if apiTags == nil {
		apiTags = map[string]any{}
	}
	apiTags[samWSTag] = samCreatedBy
	stageTags := map[string]any{samWSTag: samCreatedBy}
	if props[samKeyPropagateTags] == true && len(asMap(props[samKeyTags])) > 0 {
		stageTags = apiTags
	}

	api := map[string]any{
		samKeyProps: t.wsAPIProps(id, props, selection, apiTags),
		samKeyType:  samTypeHTTPAPI,
	}
	carryAttrs(r, api)
	if err = t.put(id, api); err != nil {
		return err
	}
	if err = t.emitV2Domain(id, props[samDomainKey], stageTags, false); err != nil {
		return err
	}
	routeIDs, authorizer, err := t.putWSRoutes(id, routes, auth, stage)
	if err != nil {
		return err
	}
	if err = t.putWSStage(id, stage, stageTags, props, routeIDs); err != nil {
		return err
	}

	return t.putWSAuthorizer(id, auth, authorizer)
}

func validateWebSocketRequired(id string, props, routes map[string]any) (string, error) {
	if len(routes) == 0 {
		if props["Routes"] == nil {
			return "", samErr(id, "Missing required property 'Routes'.")
		}

		return "", samErr(id, "WebSocket API must have at least one route.")
	}
	selection, isStr := props["RouteSelectionExpression"].(string)
	if !isStr || selection == "" {
		return "", samErr(id, "Missing required property 'RouteSelectionExpression'.")
	}

	return selection, validateWebSocketShape(id, props)
}

func (t *samTranslator) putWSStage(id string, stage any, tags, props map[string]any, routeIDs []any) error {
	stageID := id + stageSuffix(stage) + "Stage"
	if stage == samWSDefaultStage {
		stageID = id + "DefaultStage"
	}
	t.refs[id+".Stage"] = stageID
	res := map[string]any{samKeyType: samTypeHTTPStage, samKeyProps: wsStageProps(id, stage, tags, props)}
	if props[samKeyRouteSetting] != nil && len(routeIDs) > 0 {
		res[samKeyDependsOn] = routeIDs
	}

	return t.put(stageID, res)
}

func stageSuffix(stage any) string {
	if s, ok := stage.(string); ok {
		return s
	}

	return ""
}

func validateWebSocketShape(id string, props map[string]any) error {
	if v, has := props["IpAddressType"]; has {
		if s, isStr := v.(string); isStr && s != "ipv4" && s != "dualstack" {
			return samErr(id, "IpAddressType must be 'ipv4' or 'dualstack'.")
		}
	}
	if d := asMap(props[samDomainKey]); d != nil && d["MutualTlsAuthentication"] != nil {
		return samErr(id, "Mutual TLS domain name association is not supported for Websocket APIs.")
	}

	return nil
}

func (t *samTranslator) wsAPIProps(
	id string, props map[string]any, selection string, tags map[string]any,
) map[string]any {
	out := map[string]any{
		attrNameName: id, "ProtocolType": "WEBSOCKET", "RouteSelectionExpression": selection,
		samKeyTags: tags,
	}
	if n, has := props[attrNameName]; has && n != nil {
		out[attrNameName] = n
	}
	for _, k := range []string{"ApiKeySelectionExpression", samKeyDesc, samKeyDisableEndpoint,
		"DisableSchemaValidation", "IpAddressType"} {
		if v, has := props[k]; has && v != nil {
			out[k] = v
		}
	}

	return out
}

func wsStageProps(apiID string, stage any, tags map[string]any, props map[string]any) map[string]any {
	out := map[string]any{samKeyAPIID: samRef(apiID), "AutoDeploy": true, samKeyStageName: stage, samKeyTags: tags}
	for src, dst := range map[string]string{samKeyAccessLog: samKeyAccessLog,
		samKeyDefaultRoute: samKeyDefaultRoute, samKeyRouteSetting: samKeyRouteSetting,
		samKeyStageVars: samKeyStageVars} {
		if v, has := props[src]; has && v != nil {
			out[dst] = v
		}
	}

	return out
}

// wsRouteIDs are the logical ids of a route's Route, Integration and Permission.
func wsRouteIDs(apiID, key string) (string, string, string, error) {
	var clean string
	switch {
	case slices.Contains([]string{"$connect", "$disconnect", "$default"}, key):
		clean = strings.ToUpper(key[1:2]) + strings.ToLower(key[2:])
	case key != "" && isAlnum(key):
		clean = strings.ToUpper(key[:1]) + strings.ToLower(key[1:])
	default:
		return "", "", "", samErr(apiID, "Route key '%s' must be alphanumeric. Only $connect, $disconnect, and "+
			"$default special routes are supported.", key)
	}
	base := apiID + clean

	return base + "Route", base + "Integration", base + "Permission", nil
}

func (t *samTranslator) putWSRoutes(
	apiID string, routes, auth map[string]any, stage any,
) ([]any, map[string]any, error) {
	if auth != nil {
		if err := validateWSAuth(apiID, auth); err != nil {
			return nil, nil, err
		}
	}
	var routeIDs []any
	var authorizer map[string]any
	for _, key := range slices.Sorted(maps.Keys(routes)) {
		spec := asMap(routes[key])
		routeID, integID, permID, err := wsRouteIDs(apiID, key)
		if err != nil {
			return nil, nil, err
		}
		fn := spec[samKeyFunctionArn]
		if fn == nil {
			return nil, nil, samErr(apiID, "Route must have associated function.")
		}
		route, err := wsRoute(apiID, key, integID, spec)
		if err != nil {
			return nil, nil, err
		}
		if auth != nil && key == samWSConnect {
			authorizer = wsApplyAuth(apiID, auth, route)
		}
		integ := map[string]any{samKeyAPIID: samRef(apiID), "IntegrationType": "AWS_PROXY",
			"IntegrationUri": map[string]any{samKeyFnSub: []any{
				"arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${FunctionArn}/invocations",
				map[string]any{"FunctionArn": fn},
			}}}
		if v := spec["IntegrationTimeout"]; v != nil {
			integ["TimeoutInMillis"] = v
		}
		for _, res := range []struct {
			p   map[string]any
			id  string
			typ string
		}{
			{id: routeID, typ: samTypeWSRoute, p: route}, {id: integID, typ: samTypeWSIntegration, p: integ},
			{id: permID, typ: samTypePerm, p: wsPermission(apiID, key, fn, stage)},
		} {
			if err = t.put(res.id, map[string]any{samKeyType: res.typ, samKeyProps: res.p}); err != nil {
				return nil, nil, err
			}
		}
		routeIDs = append(routeIDs, routeID)
	}

	return routeIDs, authorizer, nil
}

func wsRoute(apiID, key, integID string, spec map[string]any) (map[string]any, error) {
	route := map[string]any{
		samKeyAPIID: samRef(apiID), "RouteKey": key,
		"Target": map[string]any{samFnJoin: []any{"/", []any{"integrations", samRef(integID)}}},
	}
	for _, k := range []string{"ApiKeyRequired", "ModelSelectionExpression", "OperationName", "RequestModels",
		"RouteResponseSelectionExpression"} {
		if v := spec[k]; v != nil {
			route[k] = v
		}
	}
	if v := spec["RequestParameters"]; v != nil && v != false {
		if key != samWSConnect {
			return nil, samErr(apiID, "Request parameters are only supported for the $connect route in WebSocket APIs.")
		}
		route["RequestParameters"] = v
	}

	return route, nil
}

func validateWSAuth(apiID string, auth map[string]any) error {
	typ, _ := auth[samKeyAuthType].(string)
	switch {
	case typ == "":
		return samErr(apiID, "AuthType must be specified for additional auth configurations.")
	case !slices.Contains([]string{valueNone, samWSAuthIAM, samWSAuthCustom}, typ):
		return samErr(apiID, "AuthType '%s' is not one of AWS_IAM, CUSTOM or NONE.", typ)
	case typ == samWSAuthCustom && auth[samKeyAuthArn] == nil:
		return samErr(apiID, "AuthArn must be specified if AuthType is CUSTOM.")
	case typ == samWSAuthIAM && len(auth) > 1:
		return samErr(apiID, "No additional configurations supported for AuthType AWS_IAM.")
	case typ == valueNone && len(auth) > 1:
		return samErr(apiID, "No additional configurations supported for AuthType NONE.")
	}

	return nil
}

// wsApplyAuth sets the $connect route's authorization and returns the authorizer it needs, if any.
func wsApplyAuth(apiID string, auth, route map[string]any) map[string]any {
	switch auth[samKeyAuthType] {
	case samWSAuthCustom:
		id := apiID + "ConnectAuthorizer"
		route["AuthorizationType"] = samWSAuthCustom
		route["AuthorizerId"] = samRef(id)
		az := map[string]any{samKeyAPIID: samRef(apiID), "AuthorizerType": "REQUEST", attrNameName: id,
			"AuthorizerUri": map[string]any{samKeyFnSub: []any{
				"arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${AuthArn}/invocations",
				map[string]any{samKeyAuthArn: auth[samKeyAuthArn]},
			}}}
		if v := auth["InvokeRole"]; v != nil {
			az["AuthorizerCredentialsArn"] = v
		}
		if v := auth["IdentitySource"]; v != nil {
			az["IdentitySource"] = v
		}
		if n, ok := auth[attrNameName].(string); ok && n != "" {
			az[attrNameName] = n
		}

		return az
	case samWSAuthIAM:
		route["AuthorizationType"] = samWSAuthIAM
	default:
		route["AuthorizationType"] = valueNone
	}

	return nil
}

func (t *samTranslator) putWSAuthorizer(apiID string, auth, authorizer map[string]any) error {
	if authorizer == nil {
		return nil
	}
	id := apiID + "ConnectAuthorizer"
	if err := t.put(id, map[string]any{samKeyType: samTypeWSAuthorizer, samKeyProps: authorizer}); err != nil {
		return err
	}
	if auth["InvokeRole"] != nil || auth[samKeyAuthArn] == nil {
		return nil
	}
	perm := lambdaPermission("", "apigateway.amazonaws.com", map[string]any{samKeyFnSub: []any{
		samAuthorizersSourceARN,
		map[string]any{samAPIIDMarker: samRef(apiID)},
	}})
	asMap(perm[samKeyProps])[samKeyFnName] = auth[samKeyAuthArn]

	return t.put(apiID+"AuthorizerPermission", perm)
}

func wsPermission(apiID, key string, fn, stage any) map[string]any {
	prefix := "arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${" + apiID + ".ApiId}/"
	var source any
	if s, ok := stage.(string); ok {
		source = map[string]any{samKeyFnSub: prefix + s + "/" + key}
	} else {
		source = map[string]any{samKeyFnSub: []any{prefix + "${__StageName__}/" + key,
			map[string]any{"__StageName__": stage}}}
	}
	perm := lambdaPermission("", "apigateway.amazonaws.com", source)
	asMap(perm[samKeyProps])[samKeyFnName] = fn

	return asMap(perm[samKeyProps])
}
