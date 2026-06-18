package cloudformation

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	cognitoidentitybackend "github.com/blackbirdworks/gopherstack/services/cognitoidentity"
	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

// createPhase6Resource handles phase-6 resource types (APIGW v1 supplemental, APIGW v2 supplemental,
// Events ApiDestination/EventBusPolicy, KMS ReplicaKey, Cognito IdentityPool/Group/Domain,
// EC2 VPCPeering/NetworkAcl/KeyPair/SGRule/FlowLog, ELBv2 ListenerRule, Lambda EventInvokeConfig/Url).
// Returns handled=false when resourceType is not a phase-6 type.
func (rc *ResourceCreator) createPhase6Resource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if id, ok, err := rc.createPhase6APIGatewayResource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createPhase6APIGatewayV2Resource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createPhase6EventsResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createPhase6KMSResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createPhase6CognitoResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createPhase6EC2Resource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createPhase6ELBv2Resource(logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	if id, ok, err := rc.createPhase6LambdaResource(ctx, logicalID, resourceType, props, params, physicalIDs); ok {
		return id, true, err
	}

	return "", false, nil
}

// deletePhase6Resource handles deletion for phase-6 resource types.
func (rc *ResourceCreator) deletePhase6Resource(
	ctx context.Context,
	resourceType, physicalID string,
) (bool, error) {
	if handled, err := rc.deletePhase6APIGatewayResource(resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deletePhase6APIGatewayV2Resource(resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deletePhase6EventsResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deletePhase6KMSResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deletePhase6CognitoResource(ctx, resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deletePhase6EC2Resource(resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deletePhase6ELBv2Resource(resourceType, physicalID); handled {
		return true, err
	}

	if handled, err := rc.deletePhase6LambdaResource(resourceType, physicalID); handled {
		return true, err
	}

	return false, nil
}

// ---- API Gateway v1 supplemental ----

func (rc *ResourceCreator) createPhase6APIGatewayResource(
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
		id, err := rc.createAPIGatewayApiKey(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::ApiGateway::UsagePlan":
		id, err := rc.createAPIGatewayUsagePlan(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::ApiGateway::UsagePlanKey":
		id, err := rc.createAPIGatewayUsagePlanKey(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::ApiGateway::DomainName":
		id, err := rc.createAPIGatewayDomainName(logicalID, props, params, physicalIDs)
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

func (rc *ResourceCreator) deletePhase6APIGatewayResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::ApiGateway::Model":
		return true, rc.deleteAPIGatewayModel(physicalID)
	case "AWS::ApiGateway::RequestValidator":
		return true, rc.deleteAPIGatewayRequestValidator(physicalID)
	case "AWS::ApiGateway::Authorizer":
		return true, rc.deleteAPIGatewayAuthorizer(physicalID)
	case "AWS::ApiGateway::ApiKey":
		return true, rc.deleteAPIGatewayApiKey(physicalID)
	case "AWS::ApiGateway::UsagePlan":
		return true, rc.deleteAPIGatewayUsagePlan(physicalID)
	case "AWS::ApiGateway::UsagePlanKey":
		return true, rc.deleteAPIGatewayUsagePlanKey(physicalID)
	case "AWS::ApiGateway::DomainName":
		return true, rc.deleteAPIGatewayDomainName(physicalID)
	case "AWS::ApiGateway::BasePathMapping":
		return true, rc.deleteAPIGatewayBasePathMapping(physicalID)
	case "AWS::ApiGateway::Account":
		return true, nil // account is a singleton — deletion is a no-op
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
	schema := strProp(props, "Schema", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	m, err := rc.backends.APIGateway.Backend.CreateModel(apigwbackend.CreateModelInput{
		RestAPIID:   restAPIID,
		Name:        name,
		ContentType: contentType,
		Schema:      schema,
		Description: description,
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway model %s: %w", name, err)
	}

	// physID encodes "restApiId:modelName" so delete can address both dimensions.
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
			Name:                         name,
			Type:                         authType,
			AuthorizerURI:                strProp(props, "AuthorizerUri", params, physicalIDs),
			AuthorizerCredentials:        strProp(props, "AuthorizerCredentials", params, physicalIDs),
			IdentitySource:               strProp(props, "IdentitySource", params, physicalIDs),
			IdentityValidationExpression: strProp(props, "IdentityValidationExpression", params, physicalIDs),
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

func (rc *ResourceCreator) createAPIGatewayApiKey(
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
	value := strProp(props, "Value", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)
	enabled := true
	if v, ok := props["Enabled"].(bool); ok {
		enabled = v
	}

	key, err := rc.backends.APIGateway.Backend.CreateAPIKey(apigwbackend.CreateAPIKeyInput{
		Name:        name,
		Value:       value,
		Description: description,
		Enabled:     enabled,
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway API key %s: %w", name, err)
	}

	return key.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayApiKey(physicalID string) error {
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
	description := strProp(props, "Description", params, physicalIDs)

	plan, err := rc.backends.APIGateway.Backend.CreateUsagePlan(apigwbackend.CreateUsagePlanInput{
		Name:        name,
		Description: description,
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
		keyType = "API_KEY"
	}

	upk, err := rc.backends.APIGateway.Backend.CreateUsagePlanKey(apigwbackend.CreateUsagePlanKeyInput{
		UsagePlanID: usagePlanID,
		KeyID:       keyID,
		KeyType:     keyType,
	})
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

func (rc *ResourceCreator) createAPIGatewayDomainName(
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
	certARN := strProp(props, "CertificateArn", params, physicalIDs)
	regionalCertARN := strProp(props, "RegionalCertificateArn", params, physicalIDs)
	securityPolicy := strProp(props, "SecurityPolicy", params, physicalIDs)

	_, err := rc.backends.APIGateway.Backend.CreateDomainName(apigwbackend.CreateDomainNameInput{
		DomainName:             domainName,
		CertificateARN:         certARN,
		RegionalCertificateARN: regionalCertARN,
		SecurityPolicy:         securityPolicy,
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway domain name %s: %w", domainName, err)
	}

	return domainName, nil
}

func (rc *ResourceCreator) deleteAPIGatewayDomainName(physicalID string) error {
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
	restAPIID := strProp(props, "RestApiId", params, physicalIDs)
	stage := strProp(props, "Stage", params, physicalIDs)

	_, err := rc.backends.APIGateway.Backend.CreateBasePathMapping(apigwbackend.CreateBasePathMappingInput{
		DomainName: domainName,
		BasePath:   basePath,
		RestAPIID:  restAPIID,
		Stage:      stage,
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway base path mapping %s/%s: %w", domainName, basePath, err)
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

	// Account is a singleton in API Gateway; update it with the provided CloudWatch role.
	if cloudwatchRoleARN != "" {
		_, err := rc.backends.APIGateway.Backend.UpdateAccount(apigwbackend.UpdateAccountInput{})
		if err != nil {
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
	statusCode := strProp(props, "StatusCode", params, physicalIDs)

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

	_, err := rc.backends.APIGateway.Backend.PutGatewayResponse(apigwbackend.PutGatewayResponseInput{
		RestAPIID:          restAPIID,
		ResponseType:       responseType,
		StatusCode:         statusCode,
		ResponseParameters: responseParameters,
		ResponseTemplates:  responseTemplates,
	})
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

// ---- API Gateway v2 supplemental ----

func (rc *ResourceCreator) createPhase6APIGatewayV2Resource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::ApiGatewayV2::DomainName":
		id, err := rc.createAPIGatewayV2DomainName(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::ApiGatewayV2::ApiMapping":
		id, err := rc.createAPIGatewayV2ApiMapping(logicalID, props, params, physicalIDs)
		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deletePhase6APIGatewayV2Resource(resourceType, physicalID string) (bool, error) {
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

	_, err := rc.backends.APIGatewayV2.Backend.CreateDomainName(apigatewayv2backend.CreateDomainNameInput{
		DomainNameValue: domainName,
	})
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
	apiID := strProp(props, "ApiId", params, physicalIDs)
	stage := strProp(props, "Stage", params, physicalIDs)
	mappingKey := strProp(props, "ApiMappingKey", params, physicalIDs)

	m, err := rc.backends.APIGatewayV2.Backend.CreateAPIMapping(domainName, apigatewayv2backend.CreateAPIMappingInput{
		APIID:         apiID,
		Stage:         stage,
		APIMappingKey: mappingKey,
	})
	if err != nil {
		return "", fmt.Errorf("create API Gateway V2 API mapping: %w", err)
	}

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

// ---- Events supplemental ----

func (rc *ResourceCreator) createPhase6EventsResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::Events::ApiDestination":
		id, err := rc.createEventsAPIDestination(ctx, logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::Events::EventBusPolicy":
		id, err := rc.createEventsEventBusPolicy(ctx, logicalID, props, params, physicalIDs)
		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deletePhase6EventsResource(ctx context.Context, resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::Events::ApiDestination":
		return true, rc.deleteEventsAPIDestination(ctx, physicalID)
	case "AWS::Events::EventBusPolicy":
		return true, nil // EventBusPolicy deletion is not supported via a simple API; treat as no-op
	default:
		return false, nil
	}
}

func (rc *ResourceCreator) createEventsAPIDestination(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EventBridge == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "Name", params, physicalIDs)
	if name == "" {
		name = logicalID
	}
	connectionARN := strProp(props, "ConnectionArn", params, physicalIDs)
	httpMethod := strProp(props, "HttpMethod", params, physicalIDs)
	if httpMethod == "" {
		httpMethod = "POST"
	}
	invocationEndpoint := strProp(props, "InvocationEndpoint", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)

	dest, err := rc.backends.EventBridge.Backend.CreateAPIDestination(ctx, ebbackend.CreateAPIDestinationInput{
		Name:               name,
		ConnectionArn:      connectionARN,
		HTTPMethod:         httpMethod,
		InvocationEndpoint: invocationEndpoint,
		Description:        description,
	})
	if err != nil {
		return "", fmt.Errorf("create Events API destination %s: %w", name, err)
	}

	return dest.APIDestinationArn, nil
}

func (rc *ResourceCreator) deleteEventsAPIDestination(ctx context.Context, physicalID string) error {
	if rc.backends.EventBridge == nil {
		return nil
	}
	// physicalID is the ARN; extract the name from the last segment.
	name := physicalID
	if idx := strings.LastIndex(physicalID, "/"); idx >= 0 {
		name = physicalID[idx+1:]
	}
	return rc.backends.EventBridge.Backend.DeleteAPIDestination(ctx, name)
}

func (rc *ResourceCreator) createEventsEventBusPolicy(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EventBridge == nil {
		return logicalID + "-stub", nil
	}

	eventBusName := strProp(props, "EventBusName", params, physicalIDs)
	if eventBusName == "" {
		eventBusName = "default"
	}
	statement := strProp(props, "Statement", params, physicalIDs)
	statementID := strProp(props, "StatementId", params, physicalIDs)

	// Wrap the single statement in a policy document for the backend.
	policy := statement
	if policy == "" && statementID != "" {
		policy = fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Sid":%q,"Effect":"Allow","Principal":{"AWS":"*"},"Action":"events:PutEvents","Resource":"*"}]}`, statementID)
	}

	if err := rc.backends.EventBridge.Backend.PutEventBusPolicy(ctx, ebbackend.PutEventBusPolicyInput{
		EventBusName: eventBusName,
		Policy:       policy,
	}); err != nil {
		return "", fmt.Errorf("create Events event bus policy on %s: %w", eventBusName, err)
	}

	return eventBusName + ":" + statementID, nil
}

// ---- KMS ReplicaKey ----

func (rc *ResourceCreator) createPhase6KMSResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != "AWS::KMS::ReplicaKey" {
		return "", false, nil
	}
	id, err := rc.createKMSReplicaKey(ctx, logicalID, props, params, physicalIDs)
	return id, true, err
}

func (rc *ResourceCreator) deletePhase6KMSResource(ctx context.Context, resourceType, physicalID string) (bool, error) {
	if resourceType != "AWS::KMS::ReplicaKey" {
		return false, nil
	}
	return true, rc.deleteKMSReplicaKey(ctx, physicalID)
}

func (rc *ResourceCreator) createKMSReplicaKey(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.KMS == nil {
		return logicalID + "-stub", nil
	}

	primaryKeyID := strProp(props, "PrimaryKeyArn", params, physicalIDs)
	description := strProp(props, "Description", params, physicalIDs)
	replicaRegion := strProp(props, "ReplicaRegion", params, physicalIDs)
	if replicaRegion == "" {
		replicaRegion = rc.backends.Region
	}

	out, err := rc.backends.KMS.Backend.ReplicateKey(ctx, &kmsbackend.ReplicateKeyInput{
		KeyID:         primaryKeyID,
		ReplicaRegion: replicaRegion,
		Description:   description,
	})
	if err != nil {
		return "", fmt.Errorf("create KMS replica key: %w", err)
	}

	return out.ReplicaKeyMetadata.KeyID, nil
}

func (rc *ResourceCreator) deleteKMSReplicaKey(ctx context.Context, physicalID string) error {
	if rc.backends.KMS == nil {
		return nil
	}
	_, err := rc.backends.KMS.Backend.ScheduleKeyDeletion(ctx, &kmsbackend.ScheduleKeyDeletionInput{
		KeyID:               physicalID,
		PendingWindowInDays: kmsMinDeletionWindowDays,
	})
	return err
}

// ---- Cognito supplemental ----

func (rc *ResourceCreator) createPhase6CognitoResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::Cognito::IdentityPool":
		id, err := rc.createCognitoIdentityPool(ctx, logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::Cognito::IdentityPoolRoleAttachment":
		id, err := rc.createCognitoIdentityPoolRoleAttachment(ctx, logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::Cognito::UserPoolDomain":
		id, err := rc.createCognitoUserPoolDomain(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::Cognito::UserPoolGroup":
		id, err := rc.createCognitoUserPoolGroup(logicalID, props, params, physicalIDs)
		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deletePhase6CognitoResource(ctx context.Context, resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::Cognito::IdentityPool":
		return true, rc.deleteCognitoIdentityPool(ctx, physicalID)
	case "AWS::Cognito::IdentityPoolRoleAttachment":
		return true, nil // role attachments are soft; deletion is a no-op
	case "AWS::Cognito::UserPoolDomain":
		return true, rc.deleteCognitoUserPoolDomain(physicalID)
	case "AWS::Cognito::UserPoolGroup":
		return true, rc.deleteCognitoUserPoolGroup(physicalID)
	default:
		return false, nil
	}
}

func (rc *ResourceCreator) createCognitoIdentityPool(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIdentity == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "IdentityPoolName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}
	allowUnauthenticated, _ := props["AllowUnauthenticatedIdentities"].(bool)

	pool, err := rc.backends.CognitoIdentity.Backend.CreateIdentityPool(
		ctx,
		name,
		allowUnauthenticated,
		false,
		"",
		nil,
		nil,
		nil,
	)
	if err != nil {
		return "", fmt.Errorf("create Cognito identity pool %s: %w", name, err)
	}

	return pool.IdentityPoolID, nil
}

func (rc *ResourceCreator) deleteCognitoIdentityPool(ctx context.Context, physicalID string) error {
	if rc.backends.CognitoIdentity == nil {
		return nil
	}
	return rc.backends.CognitoIdentity.Backend.DeleteIdentityPool(ctx, physicalID)
}

func (rc *ResourceCreator) createCognitoIdentityPoolRoleAttachment(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIdentity == nil {
		return logicalID + "-stub", nil
	}

	poolID := strProp(props, "IdentityPoolId", params, physicalIDs)

	var authRoleARN, unauthRoleARN string
	if roles, ok := props["Roles"].(map[string]any); ok {
		authRoleARN = resolve(roles["authenticated"], params, physicalIDs)
		unauthRoleARN = resolve(roles["unauthenticated"], params, physicalIDs)
	}

	if err := rc.backends.CognitoIdentity.Backend.SetIdentityPoolRoles(
		ctx,
		poolID,
		authRoleARN,
		unauthRoleARN,
		nil,
	); err != nil {
		return "", fmt.Errorf("create Cognito identity pool role attachment %s: %w", poolID, err)
	}

	return poolID + ":roles", nil
}

// physID for UserPoolDomain encodes "<domain>|<userPoolId>" so delete can address both.
const cognitoUserPoolDomainSep = "|"

func (rc *ResourceCreator) createCognitoUserPoolDomain(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIDP == nil {
		return logicalID + "-stub", nil
	}

	domain := strProp(props, "Domain", params, physicalIDs)
	if domain == "" {
		domain = logicalID
	}
	userPoolID := strProp(props, "UserPoolId", params, physicalIDs)

	_, err := rc.backends.CognitoIDP.Backend.CreateUserPoolDomain(userPoolID, domain)
	if err != nil {
		return "", fmt.Errorf("create Cognito user pool domain %s: %w", domain, err)
	}

	return domain + cognitoUserPoolDomainSep + userPoolID, nil
}

func (rc *ResourceCreator) deleteCognitoUserPoolDomain(physicalID string) error {
	if rc.backends.CognitoIDP == nil {
		return nil
	}
	domain, userPoolID, ok := strings.Cut(physicalID, cognitoUserPoolDomainSep)
	if !ok {
		return nil
	}
	return rc.backends.CognitoIDP.Backend.DeleteUserPoolDomain(userPoolID, domain)
}

func (rc *ResourceCreator) createCognitoUserPoolGroup(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIDP == nil {
		return logicalID + "-stub", nil
	}

	userPoolID := strProp(props, "UserPoolId", params, physicalIDs)
	groupName := strProp(props, "GroupName", params, physicalIDs)
	if groupName == "" {
		groupName = logicalID
	}
	description := strProp(props, "Description", params, physicalIDs)

	_, err := rc.backends.CognitoIDP.Backend.CreateGroup(userPoolID, groupName, description, 0)
	if err != nil {
		return "", fmt.Errorf("create Cognito user pool group %s: %w", groupName, err)
	}

	return userPoolID + ":" + groupName, nil
}

func (rc *ResourceCreator) deleteCognitoUserPoolGroup(physicalID string) error {
	if rc.backends.CognitoIDP == nil {
		return nil
	}
	userPoolID, groupName := splitCompositeID(physicalID)
	if userPoolID == "" {
		return nil
	}
	return rc.backends.CognitoIDP.Backend.DeleteGroup(userPoolID, groupName)
}

// ---- EC2 supplemental ----

func (rc *ResourceCreator) createPhase6EC2Resource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::EC2::VPCPeeringConnection":
		id, err := rc.createEC2VPCPeeringConnection(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::EC2::NetworkAcl":
		id, err := rc.createEC2NetworkACL(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::EC2::NetworkAclEntry":
		id, err := rc.createEC2NetworkACLEntry(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::EC2::KeyPair":
		id, err := rc.createEC2KeyPair(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::EC2::SecurityGroupIngress":
		id, err := rc.createEC2SecurityGroupIngress(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::EC2::SecurityGroupEgress":
		id, err := rc.createEC2SecurityGroupEgress(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::EC2::FlowLog":
		id, err := rc.createEC2FlowLog(logicalID, props, params, physicalIDs)
		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deletePhase6EC2Resource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::EC2::VPCPeeringConnection":
		return true, rc.deleteEC2VPCPeeringConnection(physicalID)
	case "AWS::EC2::NetworkAcl":
		return true, rc.deleteEC2NetworkACL(physicalID)
	case "AWS::EC2::NetworkAclEntry":
		return true, rc.deleteEC2NetworkACLEntry(physicalID)
	case "AWS::EC2::KeyPair":
		return true, rc.deleteEC2KeyPair(physicalID)
	case "AWS::EC2::SecurityGroupIngress", "AWS::EC2::SecurityGroupEgress":
		return true, nil // standalone SG rules are soft; deletion is a no-op
	case "AWS::EC2::FlowLog":
		return true, rc.deleteEC2FlowLog(physicalID)
	default:
		return false, nil
	}
}

func (rc *ResourceCreator) createEC2VPCPeeringConnection(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	vpcID := strProp(props, "VpcId", params, physicalIDs)
	peerVPCID := strProp(props, "PeerVpcId", params, physicalIDs)

	pc, err := rc.backends.EC2.Backend.CreateVpcPeeringConnection(vpcID, peerVPCID)
	if err != nil {
		return "", fmt.Errorf("create EC2 VPC peering connection: %w", err)
	}

	return pc.VpcPeeringConnectionID, nil
}

func (rc *ResourceCreator) deleteEC2VPCPeeringConnection(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}
	return rc.backends.EC2.Backend.DeleteVpcPeeringConnection(physicalID)
}

func (rc *ResourceCreator) createEC2NetworkACL(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	vpcID := strProp(props, "VpcId", params, physicalIDs)

	acl, err := rc.backends.EC2.Backend.CreateNetworkACL(vpcID)
	if err != nil {
		return "", fmt.Errorf("create EC2 network ACL: %w", err)
	}

	return acl.NetworkACLID, nil
}

func (rc *ResourceCreator) deleteEC2NetworkACL(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}
	return rc.backends.EC2.Backend.DeleteNetworkACL(physicalID)
}

// physID for NetworkAclEntry encodes "<aclId>:<ruleNumber>:<egress>".
const naclEntrySep = ":"

func (rc *ResourceCreator) createEC2NetworkACLEntry(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	aclID := strProp(props, "NetworkAclId", params, physicalIDs)
	ruleNumber := intProp(props, "RuleNumber")
	protocol := strProp(props, "Protocol", params, physicalIDs)
	if protocol == "" {
		protocol = "-1"
	}
	ruleAction := strProp(props, "RuleAction", params, physicalIDs)
	if ruleAction == "" {
		ruleAction = "allow"
	}
	cidr := strProp(props, "CidrBlock", params, physicalIDs)
	egress, _ := props["Egress"].(bool)

	var fromPort, toPort int
	if pr, ok := props["PortRange"].(map[string]any); ok {
		fromPort = intProp(pr, "From")
		toPort = intProp(pr, "To")
	}

	if err := rc.backends.EC2.Backend.CreateNetworkACLEntry(
		aclID, ruleNumber, protocol, ruleAction, cidr, egress, fromPort, toPort,
	); err != nil {
		return "", fmt.Errorf("create EC2 network ACL entry %d: %w", ruleNumber, err)
	}

	egressStr := "false"
	if egress {
		egressStr = "true"
	}
	return aclID + naclEntrySep + strconv.Itoa(ruleNumber) + naclEntrySep + egressStr, nil
}

func (rc *ResourceCreator) deleteEC2NetworkACLEntry(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}
	// physID format: "<aclId>:<ruleNumber>:<egress>"
	parts := strings.SplitN(physicalID, naclEntrySep, 3)
	if len(parts) < 3 {
		return nil
	}
	aclID := parts[0]
	ruleNumber, err := strconv.Atoi(parts[1])
	if err != nil {
		return nil
	}
	egress := parts[2] == "true"
	return rc.backends.EC2.Backend.DeleteNetworkACLEntry(aclID, ruleNumber, egress)
}

func (rc *ResourceCreator) createEC2KeyPair(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	keyName := strProp(props, "KeyName", params, physicalIDs)
	if keyName == "" {
		keyName = logicalID
	}
	publicKeyMaterial := strProp(props, "PublicKeyMaterial", params, physicalIDs)

	var err error
	if publicKeyMaterial != "" {
		_, err = rc.backends.EC2.Backend.ImportKeyPair(keyName, publicKeyMaterial)
	} else {
		_, err = rc.backends.EC2.Backend.CreateKeyPair(keyName)
	}
	if err != nil {
		return "", fmt.Errorf("create EC2 key pair %s: %w", keyName, err)
	}

	return keyName, nil
}

func (rc *ResourceCreator) deleteEC2KeyPair(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}
	return rc.backends.EC2.Backend.DeleteKeyPair(physicalID)
}

func (rc *ResourceCreator) createEC2SecurityGroupIngress(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	groupID := strProp(props, "GroupId", params, physicalIDs)
	if groupID == "" {
		return logicalID + "-stub", nil
	}
	protocol := strProp(props, "IpProtocol", params, physicalIDs)
	cidr := strProp(props, "CidrIp", params, physicalIDs)
	fromPort := intProp(props, "FromPort")
	toPort := intProp(props, "ToPort")
	sourceGroupID := strProp(props, "SourceSecurityGroupId", params, physicalIDs)

	rule := ec2backend.SecurityGroupRule{
		Protocol:      protocol,
		IPRange:       cidr,
		FromPort:      fromPort,
		ToPort:        toPort,
		SourceGroupID: sourceGroupID,
	}
	if err := rc.backends.EC2.Backend.AuthorizeSecurityGroupIngress(groupID, []ec2backend.SecurityGroupRule{rule}); err != nil {
		return "", fmt.Errorf("create EC2 security group ingress: %w", err)
	}

	return groupID + ":ingress:" + protocol, nil
}

func (rc *ResourceCreator) createEC2SecurityGroupEgress(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	groupID := strProp(props, "GroupId", params, physicalIDs)
	if groupID == "" {
		return logicalID + "-stub", nil
	}
	protocol := strProp(props, "IpProtocol", params, physicalIDs)
	cidr := strProp(props, "CidrIp", params, physicalIDs)
	fromPort := intProp(props, "FromPort")
	toPort := intProp(props, "ToPort")
	destGroupID := strProp(props, "DestinationSecurityGroupId", params, physicalIDs)

	rule := ec2backend.SecurityGroupRule{
		Protocol:      protocol,
		IPRange:       cidr,
		FromPort:      fromPort,
		ToPort:        toPort,
		SourceGroupID: destGroupID,
	}
	if err := rc.backends.EC2.Backend.AuthorizeSecurityGroupEgress(groupID, []ec2backend.SecurityGroupRule{rule}); err != nil {
		return "", fmt.Errorf("create EC2 security group egress: %w", err)
	}

	return groupID + ":egress:" + protocol, nil
}

func (rc *ResourceCreator) createEC2FlowLog(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	resourceID := strProp(props, "ResourceId", params, physicalIDs)
	trafficType := strProp(props, "TrafficType", params, physicalIDs)
	logDestType := strProp(props, "LogDestinationType", params, physicalIDs)
	logDestination := strProp(props, "LogDestination", params, physicalIDs)

	logs, err := rc.backends.EC2.Backend.CreateFlowLogs(
		[]string{resourceID},
		trafficType,
		logDestType,
		logDestination,
	)
	if err != nil {
		return "", fmt.Errorf("create EC2 flow log: %w", err)
	}
	if len(logs) == 0 {
		return logicalID + "-stub", nil
	}

	return logs[0].FlowLogID, nil
}

func (rc *ResourceCreator) deleteEC2FlowLog(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}
	return rc.backends.EC2.Backend.DeleteFlowLogs([]string{physicalID})
}

// ---- ELBv2 ListenerRule ----

func (rc *ResourceCreator) createPhase6ELBv2Resource(
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	if resourceType != "AWS::ElasticLoadBalancingV2::ListenerRule" {
		return "", false, nil
	}
	id, err := rc.createELBv2ListenerRule(logicalID, props, params, physicalIDs)
	return id, true, err
}

func (rc *ResourceCreator) deletePhase6ELBv2Resource(resourceType, physicalID string) (bool, error) {
	if resourceType != "AWS::ElasticLoadBalancingV2::ListenerRule" {
		return false, nil
	}
	return true, rc.deleteELBv2ListenerRule(physicalID)
}

func (rc *ResourceCreator) createELBv2ListenerRule(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ELBv2 == nil {
		return logicalID + "-stub", nil
	}

	listenerARN := strProp(props, "ListenerArn", params, physicalIDs)
	priority := strProp(props, "Priority", params, physicalIDs)

	// Parse Actions from props.
	actions := parseELBv2Actions(props, params, physicalIDs)
	// Parse Conditions from props.
	conditions := parseELBv2Conditions(props, params, physicalIDs)

	rule, err := rc.backends.ELBv2.Backend.CreateRule(elbv2RuleInput(listenerARN, priority, actions, conditions))
	if err != nil {
		return "", fmt.Errorf("create ELBv2 listener rule: %w", err)
	}

	return rule.RuleArn, nil
}

func (rc *ResourceCreator) deleteELBv2ListenerRule(physicalID string) error {
	if rc.backends.ELBv2 == nil {
		return nil
	}
	return rc.backends.ELBv2.Backend.DeleteRule(physicalID)
}

// ---- Lambda EventInvokeConfig and Url ----

func (rc *ResourceCreator) createPhase6LambdaResource(
	ctx context.Context,
	logicalID, resourceType string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, bool, error) {
	switch resourceType {
	case "AWS::Lambda::EventInvokeConfig":
		id, err := rc.createLambdaEventInvokeConfig(logicalID, props, params, physicalIDs)
		return id, true, err
	case "AWS::Lambda::Url":
		id, err := rc.createLambdaUrl(logicalID, props, params, physicalIDs)
		return id, true, err
	default:
		return "", false, nil
	}
}

func (rc *ResourceCreator) deletePhase6LambdaResource(resourceType, physicalID string) (bool, error) {
	switch resourceType {
	case "AWS::Lambda::EventInvokeConfig":
		return true, rc.deleteLambdaEventInvokeConfig(physicalID)
	case "AWS::Lambda::Url":
		return true, rc.deleteLambdaUrl(physicalID)
	default:
		return false, nil
	}
}

func (rc *ResourceCreator) createLambdaEventInvokeConfig(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Lambda == nil {
		return logicalID + "-stub", nil
	}

	functionName := strProp(props, "FunctionName", params, physicalIDs)

	var maxRetries *int
	if v := intProp(props, "MaximumRetryAttempts"); v >= 0 {
		retries := v
		maxRetries = &retries
	}
	var maxAge *int
	if v := intProp(props, "MaximumEventAgeInSeconds"); v > 0 {
		age := v
		maxAge = &age
	}

	if err := rc.backends.Lambda.Backend.PutFunctionEventInvokeConfig(
		functionName,
		"$LATEST",
		&lambdabackend.PutFunctionEventInvokeConfigInput{
			MaximumRetryAttempts:     maxRetries,
			MaximumEventAgeInSeconds: maxAge,
		},
	); err != nil {
		return "", fmt.Errorf("create Lambda event invoke config %s: %w", functionName, err)
	}

	return functionName, nil
}

func (rc *ResourceCreator) deleteLambdaEventInvokeConfig(physicalID string) error {
	if rc.backends.Lambda == nil {
		return nil
	}
	return rc.backends.Lambda.Backend.DeleteFunctionEventInvokeConfig(physicalID)
}

func (rc *ResourceCreator) createLambdaUrl(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Lambda == nil {
		return logicalID + "-stub", nil
	}

	functionName := strProp(props, "TargetFunctionArn", params, physicalIDs)
	if functionName == "" {
		functionName = logicalID
	}
	authType := strProp(props, "AuthType", params, physicalIDs)
	if authType == "" {
		authType = "AWS_IAM"
	}
	invokeMode := strProp(props, "InvokeMode", params, physicalIDs)

	var cors *lambdabackend.FunctionURLCors
	if corsMap, ok := props["Cors"].(map[string]any); ok {
		cors = &lambdabackend.FunctionURLCors{}
		if origins, ok := corsMap["AllowOrigins"].([]any); ok {
			for _, o := range origins {
				cors.AllowOrigins = append(cors.AllowOrigins, fmt.Sprintf("%v", o))
			}
		}
	}

	cfg, err := rc.backends.Lambda.Backend.CreateFunctionURLConfig(functionName, authType, cors, invokeMode)
	if err != nil {
		return "", fmt.Errorf("create Lambda function URL %s: %w", functionName, err)
	}

	return cfg.FunctionArn, nil
}

func (rc *ResourceCreator) deleteLambdaUrl(physicalID string) error {
	if rc.backends.Lambda == nil {
		return nil
	}
	return rc.backends.Lambda.Backend.DeleteFunctionURLConfig(physicalID)
}

// ---- ELBv2 parse helpers ----

func elbv2RuleInput(
	listenerARN, priority string,
	actions []elbv2Action,
	conditions []elbv2Condition,
) elbv2CreateRuleInput {
	return elbv2CreateRuleInput{
		listenerARN: listenerARN,
		priority:    priority,
		actions:     actions,
		conditions:  conditions,
	}
}

// elbv2CreateRuleInput holds parsed rule parameters — adapts CFN prop shapes to
// the backend's CreateRuleInput without importing the full elbv2 package types here.
type elbv2CreateRuleInput struct {
	actions     []elbv2Action
	conditions  []elbv2Condition
	listenerARN string
	priority    string
}

type elbv2Action struct {
	Type           string
	TargetGroupARN string
}

type elbv2Condition struct {
	Field  string
	Values []string
}

func parseELBv2Actions(props map[string]any, params, physicalIDs map[string]string) []elbv2Action {
	raw, _ := props["Actions"].([]any)
	out := make([]elbv2Action, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		a := elbv2Action{
			Type:           resolve(m["Type"], params, physicalIDs),
			TargetGroupARN: resolve(m["TargetGroupArn"], params, physicalIDs),
		}
		out = append(out, a)
	}
	return out
}

func parseELBv2Conditions(props map[string]any, params, physicalIDs map[string]string) []elbv2Condition {
	raw, _ := props["Conditions"].([]any)
	out := make([]elbv2Condition, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		c := elbv2Condition{
			Field: resolve(m["Field"], params, physicalIDs),
		}
		if vals, ok := m["Values"].([]any); ok {
			for _, v := range vals {
				c.Values = append(c.Values, fmt.Sprintf("%v", v))
			}
		}
		out = append(out, c)
	}
	return out
}
