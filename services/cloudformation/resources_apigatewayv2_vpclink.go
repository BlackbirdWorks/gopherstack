package cloudformation

import (
	"fmt"

	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

const resTypeAPIGatewayV2VpcLink = "AWS::ApiGatewayV2::VpcLink"

// createAPIGatewayV2VpcLinkResource handles AWS::ApiGatewayV2::VpcLink
// creation. Returns handled=false otherwise.
func (rc *ResourceCreator) createAPIGatewayV2VpcLinkResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != resTypeAPIGatewayV2VpcLink {
		return "", false, nil
	}

	id, err := rc.createAPIGatewayV2VpcLink(logicalID, props, params, physicalIDs)

	return id, true, err
}

// deleteAPIGatewayV2VpcLinkResource handles deletion for the type created above.
func (rc *ResourceCreator) deleteAPIGatewayV2VpcLinkResource(resourceType, physicalID string) (bool, error) {
	if resourceType != resTypeAPIGatewayV2VpcLink {
		return false, nil
	}

	if rc.backends.APIGatewayV2 == nil {
		return true, nil
	}

	return true, ignoreNotFound(
		rc.backends.APIGatewayV2.Backend.DeleteVpcLink(physicalID), apigatewayv2backend.ErrVpcLinkNotFound,
	)
}

// ---- AWS::ApiGatewayV2::VpcLink ----
// Ref returns the VPC link's ID (documented). Fn::GetAtt.VpcLinkId is the
// same value, so no side-channel stash is needed.

func (rc *ResourceCreator) createAPIGatewayV2VpcLink(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGatewayV2 == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	link, err := rc.backends.APIGatewayV2.Backend.CreateVpcLink(apigatewayv2backend.CreateVpcLinkInput{
		Name:             name,
		SecurityGroupIDs: strSliceProp(props["SecurityGroupIds"], params, physicalIDs),
		SubnetIDs:        strSliceProp(props["SubnetIds"], params, physicalIDs),
		Tags:             tagListProp(props, params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 VPC link %s: %w", name, err)
	}

	return link.VpcLinkID, nil
}
