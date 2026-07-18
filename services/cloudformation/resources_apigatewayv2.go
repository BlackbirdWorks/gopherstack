package cloudformation

import (
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
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	authType := strProp(props, "AuthorizerType", params, physicalIDs)
	if authType == "" {
		authType = "REQUEST"
	}

	auth, err := rc.backends.APIGatewayV2.Backend.CreateAuthorizer(apiID, apigatewayv2backend.CreateAuthorizerInput{
		Name:           name,
		AuthorizerType: authType,
		AuthorizerURI:  strProp(props, "AuthorizerUri", params, physicalIDs),
	})
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

func splitAPIGatewayV2PhysID(physicalID string) (string, string, bool) {
	const parts = 2
	split := strings.SplitN(physicalID, apigwv2PhysIDSep, parts)
	if len(split) < parts {
		return "", "", false
	}

	return split[0], split[1], true
}
