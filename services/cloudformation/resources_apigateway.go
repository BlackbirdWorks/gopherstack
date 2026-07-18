package cloudformation

import (
	"fmt"

	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
)

// cfnKeyTypeAPIKey is the string used for API_KEY across AppSync and APIGW usage plan keys.
const cfnKeyTypeAPIKey = "API_KEY"

// ---- API Gateway v1 supplemental ----

// createAPIGatewayV1SupplementalResource handles API Gateway (v1) Model, RequestValidator,
// Authorizer, ApiKey, UsagePlan, UsagePlanKey, DomainName, BasePathMapping, Account, and
// GatewayResponse resource creation.
func (rc *ResourceCreator) createAPIGatewayV1SupplementalResource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::ApiGateway::Model":
		id, err := rc.createAPIGatewayModel(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGateway::RequestValidator":
		id, err := rc.createAPIGatewayRequestValidator(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGateway::Authorizer":
		id, err := rc.createAPIGatewayAuthorizer(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGateway::ApiKey":
		id, err := rc.createAPIGatewayAPIKey(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGateway::UsagePlan":
		id, err := rc.createAPIGatewayUsagePlan(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGateway::UsagePlanKey":
		id, err := rc.createAPIGatewayUsagePlanKey(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGateway::DomainName":
		id, err := rc.createAPIGatewayV1DomainName(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGateway::BasePathMapping":
		id, err := rc.createAPIGatewayBasePathMapping(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGateway::Account":
		id, err := rc.createAPIGatewayAccount(logicalID, props, params, physicalIDs)

		return id, true, err
	case "AWS::ApiGateway::GatewayResponse":
		id, err := rc.createAPIGatewayGatewayResponse(logicalID, props, params, physicalIDs)

		return id, true, err
	default:
		return "", false, nil
	}
}

// deleteAPIGatewayV1SupplementalResource handles deletion for the API Gateway (v1)
// supplemental resource types.
func (rc *ResourceCreator) deleteAPIGatewayV1SupplementalResource(
	resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case "AWS::ApiGateway::Model":
		return true, rc.deleteAPIGatewayModel(physicalID)
	case "AWS::ApiGateway::RequestValidator":
		return true, rc.deleteAPIGatewayRequestValidator(physicalID)
	case "AWS::ApiGateway::Authorizer":
		return true, rc.deleteAPIGatewayAuthorizer(physicalID)
	case "AWS::ApiGateway::ApiKey":
		return true, rc.deleteAPIGatewayAPIKey(physicalID)
	case "AWS::ApiGateway::UsagePlan":
		return true, rc.deleteAPIGatewayUsagePlan(physicalID)
	case "AWS::ApiGateway::UsagePlanKey":
		return true, rc.deleteAPIGatewayUsagePlanKey(physicalID)
	case "AWS::ApiGateway::DomainName":
		return true, rc.deleteAPIGatewayV1DomainName(physicalID)
	case "AWS::ApiGateway::BasePathMapping":
		return true, rc.deleteAPIGatewayBasePathMapping(physicalID)
	case "AWS::ApiGateway::Account":
		return true, nil // account is singleton — deletion is a no-op
	case "AWS::ApiGateway::GatewayResponse":
		return true, rc.deleteAPIGatewayGatewayResponse(physicalID)
	default:
		return false, nil
	}
}

func (rc *ResourceCreator) createAPIGatewayModel(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}
	restAPIID := strProp(props, "RestApiId", params, physicalIDs)
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}
	contentType := strProp(props, "ContentType", params, physicalIDs)
	if contentType == "" {
		contentType = "application/json"
	}
	m, err := rc.backends.APIGateway.Backend.CreateModel(apigwbackend.CreateModelInput{
		RestAPIID:   restAPIID,
		Name:        name,
		ContentType: contentType,
		Schema:      strProp(props, "Schema", params, physicalIDs),
		Description: strProp(props, "Description", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway model %s: %w", name, err)
	}
	// physID encodes "restApiId:modelName" so delete can address both.

	return restAPIID + ":" + m.Name, nil
}

func (rc *ResourceCreator) deleteAPIGatewayModel(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}
	restAPIID, modelName := splitCompositeID(physicalID)
	if restAPIID == "" {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteModel(restAPIID, modelName)
}

func (rc *ResourceCreator) createAPIGatewayRequestValidator(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}
	restAPIID := strProp(props, "RestApiId", params, physicalIDs)
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}
	validateBody, _ := props["ValidateRequestBody"].(bool)
	validateParams, _ := props["ValidateRequestParameters"].(bool)
	v, err := rc.backends.APIGateway.Backend.CreateRequestValidator(
		restAPIID,
		apigwbackend.CreateRequestValidatorInput{
			Name:                      name,
			ValidateRequestBody:       validateBody,
			ValidateRequestParameters: validateParams,
		},
	)
	if err != nil {
		return "", fmt.Errorf("create API Gateway request validator %s: %w", name, err)
	}

	return restAPIID + ":" + v.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayRequestValidator(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}
	restAPIID, validatorID := splitCompositeID(physicalID)
	if restAPIID == "" {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteRequestValidator(restAPIID, validatorID)
}

func (rc *ResourceCreator) createAPIGatewayAuthorizer(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}
	restAPIID := strProp(props, "RestApiId", params, physicalIDs)
	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}
	authType := strProp(props, "Type", params, physicalIDs)
	if authType == "" {
		authType = "TOKEN"
	}
	auth, err := rc.backends.APIGateway.Backend.CreateAuthorizer(
		restAPIID,
		apigwbackend.CreateAuthorizerInput{
			Name:          name,
			Type:          authType,
			AuthorizerURI: strProp(props, "AuthorizerUri", params, physicalIDs),
			AuthorizerCredentials: strProp(
				props,
				"AuthorizerCredentials",
				params,
				physicalIDs,
			),
			IdentitySource: strProp(props, "IdentitySource", params, physicalIDs),
			IdentityValidationExpression: strProp(
				props,
				"IdentityValidationExpression",
				params,
				physicalIDs,
			),
		},
	)
	if err != nil {
		return "", fmt.Errorf("create API Gateway authorizer %s: %w", name, err)
	}

	return restAPIID + ":" + auth.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayAuthorizer(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}
	restAPIID, authorizerID := splitCompositeID(physicalID)
	if restAPIID == "" {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteAuthorizer(restAPIID, authorizerID)
}

func (rc *ResourceCreator) createAPIGatewayAPIKey(
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
	enabled := true
	if v, ok := props["Enabled"].(bool); ok {
		enabled = v
	}
	key, err := rc.backends.APIGateway.Backend.CreateAPIKey(apigwbackend.CreateAPIKeyInput{
		Name:        name,
		Value:       strProp(props, "Value", params, physicalIDs),
		Description: strProp(props, "Description", params, physicalIDs),
		Enabled:     enabled,
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway API key %s: %w", name, err)
	}

	return key.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayAPIKey(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteAPIKey(physicalID)
}

func (rc *ResourceCreator) createAPIGatewayUsagePlan(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}
	name := strProp(props, "UsagePlanName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}
	plan, err := rc.backends.APIGateway.Backend.CreateUsagePlan(apigwbackend.CreateUsagePlanInput{
		Name:        name,
		Description: strProp(props, "Description", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway usage plan %s: %w", name, err)
	}

	return plan.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayUsagePlan(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteUsagePlan(physicalID)
}

func (rc *ResourceCreator) createAPIGatewayUsagePlanKey(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}
	usagePlanID := strProp(props, "UsagePlanId", params, physicalIDs)
	keyID := strProp(props, "KeyId", params, physicalIDs)
	keyType := strProp(props, "KeyType", params, physicalIDs)
	if keyType == "" {
		keyType = cfnKeyTypeAPIKey
	}
	upk, err := rc.backends.APIGateway.Backend.CreateUsagePlanKey(
		apigwbackend.CreateUsagePlanKeyInput{
			UsagePlanID: usagePlanID,
			KeyID:       keyID,
			KeyType:     keyType,
		},
	)
	if err != nil {
		return "", fmt.Errorf("create API Gateway usage plan key: %w", err)
	}

	return usagePlanID + ":" + upk.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayUsagePlanKey(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}
	usagePlanID, keyID := splitCompositeID(physicalID)
	if usagePlanID == "" {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteUsagePlanKey(usagePlanID, keyID)
}

func (rc *ResourceCreator) createAPIGatewayV1DomainName(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}
	domainName := strProp(props, "DomainName", params, physicalIDs)
	if domainName == "" {
		domainName = logicalID
	}
	_, err := rc.backends.APIGateway.Backend.CreateDomainName(apigwbackend.CreateDomainNameInput{
		DomainName:             domainName,
		CertificateARN:         strProp(props, "CertificateArn", params, physicalIDs),
		RegionalCertificateARN: strProp(props, "RegionalCertificateArn", params, physicalIDs),
		SecurityPolicy:         strProp(props, "SecurityPolicy", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway domain name %s: %w", domainName, err)
	}

	return domainName, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV1DomainName(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteDomainName(physicalID)
}

func (rc *ResourceCreator) createAPIGatewayBasePathMapping(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}
	domainName := strProp(props, "DomainName", params, physicalIDs)
	basePath := strProp(props, "BasePath", params, physicalIDs)
	if basePath == "" {
		basePath = "(none)"
	}
	_, err := rc.backends.APIGateway.Backend.CreateBasePathMapping(
		apigwbackend.CreateBasePathMappingInput{
			DomainName: domainName,
			BasePath:   basePath,
			RestAPIID:  strProp(props, "RestApiId", params, physicalIDs),
			Stage:      strProp(props, "Stage", params, physicalIDs),
		},
	)
	if err != nil {
		return "", fmt.Errorf(
			"create API Gateway base path mapping %s/%s: %w",
			domainName,
			basePath,
			err,
		)
	}

	return domainName + ":" + basePath, nil
}

func (rc *ResourceCreator) deleteAPIGatewayBasePathMapping(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}
	domainName, basePath := splitCompositeID(physicalID)
	if domainName == "" {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteBasePathMapping(domainName, basePath)
}

func (rc *ResourceCreator) createAPIGatewayAccount(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}
	cloudwatchRoleARN := strProp(props, "CloudWatchRoleArn", params, physicalIDs)
	if cloudwatchRoleARN != "" {
		if _, err := rc.backends.APIGateway.Backend.UpdateAccount(apigwbackend.UpdateAccountInput{}); err != nil {
			return "", fmt.Errorf("update API Gateway account: %w", err)
		}
	}

	return "account", nil
}

func (rc *ResourceCreator) createAPIGatewayGatewayResponse(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}
	restAPIID := strProp(props, "RestApiId", params, physicalIDs)
	responseType := strProp(props, "ResponseType", params, physicalIDs)

	var responseParameters map[string]string
	if m, ok := props["ResponseParameters"].(map[string]any); ok {
		responseParameters = make(map[string]string, len(m))
		for k, v := range m {
			responseParameters[k] = fmt.Sprintf("%v", v)
		}
	}
	var responseTemplates map[string]string
	if m, ok := props["ResponseTemplates"].(map[string]any); ok {
		responseTemplates = make(map[string]string, len(m))
		for k, v := range m {
			responseTemplates[k] = fmt.Sprintf("%v", v)
		}
	}
	_, err := rc.backends.APIGateway.Backend.PutGatewayResponse(
		apigwbackend.PutGatewayResponseInput{
			RestAPIID:          restAPIID,
			ResponseType:       responseType,
			StatusCode:         strProp(props, "StatusCode", params, physicalIDs),
			ResponseParameters: responseParameters,
			ResponseTemplates:  responseTemplates,
		},
	)
	if err != nil {
		return "", fmt.Errorf("create API Gateway gateway response %s: %w", responseType, err)
	}

	return restAPIID + ":" + responseType, nil
}

func (rc *ResourceCreator) deleteAPIGatewayGatewayResponse(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}
	restAPIID, responseType := splitCompositeID(physicalID)
	if restAPIID == "" {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteGatewayResponse(restAPIID, responseType)
}
