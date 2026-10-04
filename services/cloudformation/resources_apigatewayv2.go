package cloudformation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// physID for apigwv2 children encodes "<apiID>|<childID>".
const apigwv2PhysIDSep = "|"

func (rc *ResourceCreator) createExtraAPIGatewayV2Resource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case resTypeAPIGatewayV2Integ:
		id, err := rc.createAPIGatewayV2Integration(logicalID, props, params, physicalIDs)

		return id, true, err
	case resTypeAPIGatewayV2Route:
		id, err := rc.createAPIGatewayV2Route(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGatewayV2::Authorizer":
		id, err := rc.createAPIGatewayV2Authorizer(logicalID, props, params, physicalIDs)

		return id, true, err
	default:

		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteExtraAPIGatewayV2Resource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case resTypeAPIGatewayV2Integ:

		return true, rc.deleteAPIGatewayV2Integration(physicalID)
	case resTypeAPIGatewayV2Route:

		return true, rc.deleteAPIGatewayV2Route(physicalID)
	case "AWS::ApiGatewayV2::Authorizer":

		return true, rc.deleteAPIGatewayV2Authorizer(physicalID)
	default:

		return false, nil
	}
}

func (rc *ResourceCreator) createAPIGatewayV2Authorizer(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGatewayV2 == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	var in apigatewayv2backend.CreateAuthorizerInput
	if err := decodeProps(props, params, physicalIDs, &in); err != nil {
		return "", err
	}
	if in.Name == "" {
		in.Name = logicalID
	}
	if in.AuthorizerType == "" {
		in.AuthorizerType = "REQUEST"
	}
	name := in.Name

	auth, err := rc.backends.APIGatewayV2.Backend.CreateAuthorizer(apiID, in)
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 authorizer %s: %w", name, err)
	}

	return apiID + apigwv2PhysIDSep + auth.AuthorizerID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2Authorizer(physicalID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}

	apiID, authID, ok := splitAPIGatewayV2PhysID(physicalID)
	if !ok {
		return nil
	}

	return rc.backends.APIGatewayV2.Backend.DeleteAuthorizer(apiID, authID)
}

// decodeProps resolves intrinsics in props and decodes them into out; the
// backend inputs' JSON tags match CloudFormation's property names case-insensitively.
func decodeProps(props map[string]any, params, physicalIDs map[string]string, out any) error {
	raw, err := json.Marshal(resolveDeep(props, params, physicalIDs))
	if err != nil {
		return fmt.Errorf("encode properties: %w", err)
	}
	if err = json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode properties: %w", err)
	}

	return nil
}

// bareIntegrationTarget turns "integrations/<apiID>/<id>" (Ref of an Integration
// resolves to its composite physical ID) into "integrations/<id>".
func bareIntegrationTarget(target string) string {
	rest, ok := strings.CutPrefix(target, "integrations/")
	if !ok {
		return target
	}

	return "integrations/" + rest[strings.LastIndex(rest, "/")+1:]
}

func splitAPIGatewayV2PhysID(physicalID string) (string, string, bool) {
	const parts = 2
	split := strings.SplitN(physicalID, apigwv2PhysIDSep, parts)
	if len(split) < parts {
		return "", "", false
	}

	return split[0], split[1], true
}

// ---- API Gateway V2 ----

func (rc *ResourceCreator) createAPIGatewayV2API(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGatewayV2 == nil {
		return logicalID + "-stub", nil
	}

	var in apigatewayv2backend.CreateAPIInput
	if err := decodeProps(props, params, physicalIDs, &in); err != nil {
		return "", err
	}
	if in.Name == "" {
		in.Name = logicalID
	}
	if in.ProtocolType == "" {
		in.ProtocolType = "HTTP"
	}
	name := in.Name

	api, err := rc.backends.APIGatewayV2.Backend.CreateAPI(ctx, in)
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 API %s: %w", name, err)
	}

	return api.APIID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2API(apiID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}

	return rc.backends.APIGatewayV2.Backend.DeleteAPI(apiID)
}

func (rc *ResourceCreator) createAPIGatewayV2Stage(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGatewayV2 == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	var in apigatewayv2backend.CreateStageInput
	if err := decodeProps(props, params, physicalIDs, &in); err != nil {
		return "", err
	}
	if in.StageName == "" {
		in.StageName = logicalID
	}
	stageName := in.StageName

	_, err := rc.backends.APIGatewayV2.Backend.CreateStage(apiID, in)
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 stage %s: %w", stageName, err)
	}

	return apiID + "/" + stageName, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2Stage(physicalID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}

	// physicalID format: {apiID}/{stageName}
	idx := strings.LastIndex(physicalID, "/")
	if idx < 0 {
		return nil
	}

	apiID := physicalID[:idx]
	stageName := physicalID[idx+1:]

	return rc.backends.APIGatewayV2.Backend.DeleteStage(apiID, stageName)
}

func (rc *ResourceCreator) createAPIGatewayV2Integration(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGatewayV2 == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	var in apigatewayv2backend.CreateIntegrationInput
	if err := decodeProps(props, params, physicalIDs, &in); err != nil {
		return "", err
	}
	if in.IntegrationType == "" {
		in.IntegrationType = "AWS_PROXY"
	}

	integration, err := rc.backends.APIGatewayV2.Backend.CreateIntegration(apiID, in)
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 integration: %w", err)
	}

	return apiID + "/" + integration.IntegrationID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2Integration(physicalID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}

	idx := strings.LastIndex(physicalID, "/")
	if idx < 0 {
		return nil
	}

	apiID := physicalID[:idx]
	integrationID := physicalID[idx+1:]

	return rc.backends.APIGatewayV2.Backend.DeleteIntegration(apiID, integrationID)
}

func (rc *ResourceCreator) createAPIGatewayV2Route(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGatewayV2 == nil {
		return logicalID + "-stub", nil
	}

	apiID := strProp(props, "ApiId", params, physicalIDs)
	var in apigatewayv2backend.CreateRouteInput
	if err := decodeProps(props, params, physicalIDs, &in); err != nil {
		return "", err
	}
	in.Target = bareIntegrationTarget(in.Target)

	route, err := rc.backends.APIGatewayV2.Backend.CreateRoute(apiID, in)
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 route: %w", err)
	}

	return apiID + "/" + route.RouteID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2Route(physicalID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}

	idx := strings.LastIndex(physicalID, "/")
	if idx < 0 {
		return nil
	}

	apiID := physicalID[:idx]
	routeID := physicalID[idx+1:]

	return rc.backends.APIGatewayV2.Backend.DeleteRoute(apiID, routeID)
}

// ---- API Gateway v2 supplemental ----

// createAPIGatewayV2SupplementalResource handles APIGatewayV2 DomainName and
// ApiMapping resource creation.
func (rc *ResourceCreator) createAPIGatewayV2SupplementalResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::ApiGatewayV2::DomainName":
		id, err := rc.createAPIGatewayV2DomainName(ctx, logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGatewayV2::ApiMapping":
		id, err := rc.createAPIGatewayV2ApiMapping(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteAPIGatewayV2SupplementalResource handles APIGatewayV2 DomainName and
// ApiMapping resource deletion.
func (rc *ResourceCreator) deleteAPIGatewayV2SupplementalResource(
	resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case "AWS::ApiGatewayV2::DomainName":
		return true, rc.deleteAPIGatewayV2DomainName(physicalID)
	case "AWS::ApiGatewayV2::ApiMapping":
		return true, rc.deleteAPIGatewayV2ApiMapping(physicalID)
	default:
		return false, nil
	}
}

func (rc *ResourceCreator) createAPIGatewayV2DomainName(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGatewayV2 == nil {
		return logicalID + "-stub", nil
	}
	domainName := strProp(props, "DomainName", params, physicalIDs)
	if domainName == "" {
		domainName = logicalID
	}
	_, err := rc.backends.APIGatewayV2.Backend.CreateDomainName(
		ctx,
		apigatewayv2backend.CreateDomainNameInput{
			DomainNameValue: domainName,
		},
	)
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 domain name %s: %w", domainName, err)
	}

	return domainName, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2DomainName(physicalID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}

	return rc.backends.APIGatewayV2.Backend.DeleteDomainName(physicalID)
}

func (rc *ResourceCreator) createAPIGatewayV2ApiMapping(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGatewayV2 == nil {
		return logicalID + "-stub", nil
	}
	domainName := strProp(props, "DomainName", params, physicalIDs)
	m, err := rc.backends.APIGatewayV2.Backend.CreateAPIMapping(
		domainName,
		apigatewayv2backend.CreateAPIMappingInput{
			APIID:         strProp(props, "ApiId", params, physicalIDs),
			Stage:         strProp(props, "Stage", params, physicalIDs),
			APIMappingKey: strProp(props, "ApiMappingKey", params, physicalIDs),
		},
	)
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 API mapping: %w", err)
	}

	// physID: "<domainName>|<mappingID>" — reuse the apigwv2PhysIDSep constant.
	return domainName + apigwv2PhysIDSep + m.APIMappingID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2ApiMapping(physicalID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}
	domainName, mappingID, ok := strings.Cut(physicalID, apigwv2PhysIDSep)
	if !ok {
		return nil
	}

	return rc.backends.APIGatewayV2.Backend.DeleteAPIMapping(domainName, mappingID)
}
