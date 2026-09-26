package cloudformation

import (
	"errors"
	"fmt"

	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
)

func (rc *ResourceCreator) createAPIGatewayMoreResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::ApiGateway::VpcLink":
		id, err := rc.createAPIGatewayVpcLink(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGateway::ClientCertificate":
		id, err := rc.createAPIGatewayClientCertificate(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGateway::DocumentationPart":
		id, err := rc.createAPIGatewayDocumentationPart(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGateway::DocumentationVersion":
		id, err := rc.createAPIGatewayDocumentationVersion(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deleteAPIGatewayMoreResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::ApiGateway::VpcLink":
		return true, rc.deleteAPIGatewayVpcLink(physicalID)
	case "AWS::ApiGateway::ClientCertificate":
		return true, rc.deleteAPIGatewayClientCertificate(physicalID)
	default:
		return false, nil
	}
}

// ---- ApiGateway VpcLink ----

func (rc *ResourceCreator) createAPIGatewayVpcLink(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	link, err := rc.backends.APIGateway.Backend.CreateVpcLink(apigwbackend.CreateVpcLinkInput{
		Name:        name,
		Description: strProp(props, "Description", params, physicalIDs),
		TargetARNs:  strSliceProp(props["TargetArns"], params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway VPC link %s: %w", name, err)
	}

	return link.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayVpcLink(id string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	err := rc.backends.APIGateway.Backend.DeleteVpcLink(id)
	if errors.Is(err, apigwbackend.ErrNotFound) {
		return nil
	}

	return err
}

// ---- ApiGateway ClientCertificate ----

func (rc *ResourceCreator) createAPIGatewayClientCertificate(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}

	cert, err := rc.backends.APIGateway.Backend.GenerateClientCertificate(
		apigwbackend.GenerateClientCertificateInput{
			Description: strProp(props, "Description", params, physicalIDs),
		},
	)
	if err != nil {
		return "", fmt.Errorf("generate API Gateway client certificate: %w", err)
	}

	return cert.ClientCertificateID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayClientCertificate(id string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	err := rc.backends.APIGateway.Backend.DeleteClientCertificate(id)
	if errors.Is(err, apigwbackend.ErrNotFound) {
		return nil
	}

	return err
}

// ---- ApiGateway DocumentationPart ----

func documentationLocationProp(
	props map[string]any, params, physicalIDs map[string]string,
) apigwbackend.DocumentationLocation {
	loc, _ := props["Location"].(map[string]any)

	return apigwbackend.DocumentationLocation{
		Type:       strProp(loc, "Type", params, physicalIDs),
		Path:       strProp(loc, "Path", params, physicalIDs),
		Method:     strProp(loc, "Method", params, physicalIDs),
		StatusCode: strProp(loc, "StatusCode", params, physicalIDs),
		Name:       strProp(loc, "Name", params, physicalIDs),
	}
}

func (rc *ResourceCreator) createAPIGatewayDocumentationPart(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}

	restAPIID := strProp(props, "RestApiId", params, physicalIDs)

	part, err := rc.backends.APIGateway.Backend.CreateDocumentationPart(apigwbackend.CreateDocumentationPartInput{
		RestAPIID:  restAPIID,
		Location:   documentationLocationProp(props, params, physicalIDs),
		Properties: strProp(props, "Properties", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway documentation part for %s: %w", restAPIID, err)
	}

	return part.ID, nil
}

// deleteAPIGatewayDocumentationPart needs RestApiId, a real CFN property not
// embedded in the part's physical ID (its bare DocumentationPartId), so it
// comes from props like deleteEKSAccessEntry.
func (rc *ResourceCreator) deleteAPIGatewayDocumentationPart(
	props map[string]any, stackPhysicalIDs map[string]string, physicalID string,
) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	restAPIID := strProp(props, "RestApiId", nil, stackPhysicalIDs)

	err := rc.backends.APIGateway.Backend.DeleteDocumentationPart(restAPIID, physicalID)
	if errors.Is(err, apigwbackend.ErrDocumentationPartNotFound) || errors.Is(err, apigwbackend.ErrRestAPINotFound) {
		return nil
	}

	return err
}

// ---- ApiGateway DocumentationVersion ----

func (rc *ResourceCreator) createAPIGatewayDocumentationVersion(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}

	restAPIID := strProp(props, "RestApiId", params, physicalIDs)
	version := strProp(props, "DocumentationVersion", params, physicalIDs)

	ver, err := rc.backends.APIGateway.Backend.CreateDocumentationVersion(
		apigwbackend.CreateDocumentationVersionInput{
			RestAPIID:   restAPIID,
			Version:     version,
			Description: strProp(props, "Description", params, physicalIDs),
		},
	)
	if err != nil {
		return "", fmt.Errorf("create API Gateway documentation version %s for %s: %w", version, restAPIID, err)
	}

	return ver.Version, nil
}

// deleteAPIGatewayDocumentationVersion needs RestApiId; see
// deleteAPIGatewayDocumentationPart.
func (rc *ResourceCreator) deleteAPIGatewayDocumentationVersion(
	props map[string]any, stackPhysicalIDs map[string]string, physicalID string,
) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	restAPIID := strProp(props, "RestApiId", nil, stackPhysicalIDs)

	err := rc.backends.APIGateway.Backend.DeleteDocumentationVersion(restAPIID, physicalID)
	if errors.Is(err, apigwbackend.ErrDocumentationVersionNotFound) ||
		errors.Is(err, apigwbackend.ErrRestAPINotFound) {
		return nil
	}

	return err
}
