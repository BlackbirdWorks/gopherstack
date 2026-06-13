package cloudformation

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	apigwbackend "github.com/blackbirdworks/gopherstack/services/apigateway"
	apigatewayv2backend "github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

// ---- AWS::ApiGateway::Model ----

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

	model, err := rc.backends.APIGateway.Backend.CreateModel(apigwbackend.CreateModelInput{
		RestAPIID:   restAPIID,
		Name:        name,
		ContentType: strProp(props, "ContentType", params, physicalIDs),
		Description: strProp(props, "Description", params, physicalIDs),
		Schema:      strProp(props, "Schema", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create ApiGateway Model %s: %w", name, err)
	}

	return restAPIID + "/" + model.Name, nil
}

func (rc *ResourceCreator) deleteAPIGatewayModel(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	const parts = 2

	p := strings.SplitN(physicalID, "/", parts)
	if len(p) < parts {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteModel(p[0], p[1])
}

// ---- AWS::ApiGateway::RequestValidator ----

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

	rv, err := rc.backends.APIGateway.Backend.CreateRequestValidator(
		restAPIID,
		apigwbackend.CreateRequestValidatorInput{
			Name:                      name,
			ValidateRequestBody:       validateBody,
			ValidateRequestParameters: validateParams,
		},
	)
	if err != nil {
		return "", fmt.Errorf("create ApiGateway RequestValidator %s: %w", name, err)
	}

	return restAPIID + "/" + rv.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayRequestValidator(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	const parts = 2

	p := strings.SplitN(physicalID, "/", parts)
	if len(p) < parts {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteRequestValidator(p[0], p[1])
}

// ---- AWS::ApiGateway::Authorizer ----

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
		return "", fmt.Errorf("create ApiGateway Authorizer %s: %w", name, err)
	}

	return restAPIID + "/" + auth.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayAuthorizer(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	const parts = 2

	p := strings.SplitN(physicalID, "/", parts)
	if len(p) < parts {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteAuthorizer(p[0], p[1])
}

// ---- AWS::ApiGateway::ApiKey ----

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

	enabled := true
	if v, ok := props["Enabled"].(bool); ok {
		enabled = v
	}

	key, err := rc.backends.APIGateway.Backend.CreateAPIKey(apigwbackend.CreateAPIKeyInput{
		Name:        name,
		Description: strProp(props, "Description", params, physicalIDs),
		Value:       strProp(props, "Value", params, physicalIDs),
		Enabled:     enabled,
	})
	if err != nil {
		return "", fmt.Errorf("create ApiGateway ApiKey %s: %w", name, err)
	}

	return key.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayApiKey(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteAPIKey(physicalID)
}

// ---- AWS::ApiGateway::UsagePlan ----

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
		return "", fmt.Errorf("create ApiGateway UsagePlan %s: %w", name, err)
	}

	return plan.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayUsagePlan(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteUsagePlan(physicalID)
}

// ---- AWS::ApiGateway::UsagePlanKey ----

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
		return "", fmt.Errorf("create ApiGateway UsagePlanKey for plan %s: %w", usagePlanID, err)
	}

	return usagePlanID + "/" + upk.ID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayUsagePlanKey(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	const parts = 2

	p := strings.SplitN(physicalID, "/", parts)
	if len(p) < parts {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteUsagePlanKey(p[0], p[1])
}

// ---- AWS::ApiGateway::DomainName ----

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

	dn, err := rc.backends.APIGateway.Backend.CreateDomainName(apigwbackend.CreateDomainNameInput{
		DomainName:             domainName,
		CertificateARN:         strProp(props, "CertificateArn", params, physicalIDs),
		RegionalCertificateARN: strProp(props, "RegionalCertificateArn", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create ApiGateway DomainName %s: %w", domainName, err)
	}

	return dn.DomainNameValue, nil
}

func (rc *ResourceCreator) deleteAPIGatewayDomainName(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteDomainName(physicalID)
}

// ---- AWS::ApiGateway::BasePathMapping ----

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
	restAPIID := strProp(props, "RestApiId", params, physicalIDs)
	stage := strProp(props, "Stage", params, physicalIDs)

	_, err := rc.backends.APIGateway.Backend.CreateBasePathMapping(apigwbackend.CreateBasePathMappingInput{
		DomainName: domainName,
		BasePath:   basePath,
		RestAPIID:  restAPIID,
		Stage:      stage,
	})
	if err != nil {
		return "", fmt.Errorf("create ApiGateway BasePathMapping for %s: %w", domainName, err)
	}

	return domainName + "/" + basePath, nil
}

func (rc *ResourceCreator) deleteAPIGatewayBasePathMapping(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	const parts = 2

	p := strings.SplitN(physicalID, "/", parts)
	if len(p) < parts {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteBasePathMapping(p[0], p[1])
}

// ---- AWS::ApiGateway::Account ----

// createAPIGatewayAccount handles AWS::ApiGateway::Account. Account is a singleton per region;
// the physical ID is a fixed string. The CFN resource only sets the CloudWatch logs role ARN.
func (rc *ResourceCreator) createAPIGatewayAccount(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.APIGateway == nil {
		return logicalID + "-stub", nil
	}

	// AWS only supports updating the CloudWatchRoleArn; nothing to create.
	_ = strProp(props, "CloudWatchRoleArn", params, physicalIDs)

	return "account", nil
}

// ---- AWS::ApiGateway::GatewayResponse ----

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

	gr, err := rc.backends.APIGateway.Backend.PutGatewayResponse(apigwbackend.PutGatewayResponseInput{
		RestAPIID:    restAPIID,
		ResponseType: responseType,
		StatusCode:   strProp(props, "StatusCode", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create ApiGateway GatewayResponse %s: %w", responseType, err)
	}

	return restAPIID + "/" + gr.ResponseType, nil
}

func (rc *ResourceCreator) deleteAPIGatewayGatewayResponse(physicalID string) error {
	if rc.backends.APIGateway == nil {
		return nil
	}

	const parts = 2

	p := strings.SplitN(physicalID, "/", parts)
	if len(p) < parts {
		return nil
	}

	return rc.backends.APIGateway.Backend.DeleteGatewayResponse(p[0], p[1])
}

// ---- AWS::ApiGatewayV2::DomainName ----

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

	dn, err := rc.backends.APIGatewayV2.Backend.CreateDomainName(apigatewayv2backend.CreateDomainNameInput{
		DomainNameValue: domainName,
	})
	if err != nil {
		return "", fmt.Errorf("create ApiGatewayV2 DomainName %s: %w", domainName, err)
	}

	return dn.DomainNameValue, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2DomainName(physicalID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}

	return rc.backends.APIGatewayV2.Backend.DeleteDomainName(physicalID)
}

// ---- AWS::ApiGatewayV2::ApiMapping ----

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

	m, err := rc.backends.APIGatewayV2.Backend.CreateAPIMapping(
		domainName,
		apigatewayv2backend.CreateAPIMappingInput{
			APIID:         apiID,
			Stage:         stage,
			APIMappingKey: strProp(props, "ApiMappingKey", params, physicalIDs),
		},
	)
	if err != nil {
		return "", fmt.Errorf("create ApiGatewayV2 ApiMapping for %s: %w", domainName, err)
	}

	return domainName + "/" + m.APIMappingID, nil
}

func (rc *ResourceCreator) deleteAPIGatewayV2ApiMapping(physicalID string) error {
	if rc.backends.APIGatewayV2 == nil {
		return nil
	}

	const parts = 2

	p := strings.SplitN(physicalID, "/", parts)
	if len(p) < parts {
		return nil
	}

	return rc.backends.APIGatewayV2.Backend.DeleteAPIMapping(p[0], p[1])
}

// ---- AWS::Events::ApiDestination ----

func (rc *ResourceCreator) createEventBridgeApiDestination(
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

	httpMethod := strProp(props, "HttpMethod", params, physicalIDs)
	if httpMethod == "" {
		httpMethod = "POST"
	}

	dst, err := rc.backends.EventBridge.Backend.CreateAPIDestination(context.Background(), ebbackend.CreateAPIDestinationInput{
		Name:               name,
		ConnectionArn:      strProp(props, "ConnectionArn", params, physicalIDs),
		HTTPMethod:         httpMethod,
		InvocationEndpoint: strProp(props, "InvocationEndpoint", params, physicalIDs),
		Description:        strProp(props, "Description", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create Events ApiDestination %s: %w", name, err)
	}

	return dst.Name, nil
}

func (rc *ResourceCreator) deleteEventBridgeApiDestination(physicalID string) error {
	if rc.backends.EventBridge == nil {
		return nil
	}

	return rc.backends.EventBridge.Backend.DeleteAPIDestination(context.Background(), physicalID)
}

// ---- AWS::Events::EventBusPolicy ----

// createEventBridgeEventBusPolicy handles AWS::Events::EventBusPolicy.
// The CFN resource sets or replaces a single IAM statement on the event bus policy.
// Physical ID = eventBusName + "/" + statementId to enable targeted delete.
func (rc *ResourceCreator) createEventBridgeEventBusPolicy(
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

	statementID := strProp(props, "StatementId", params, physicalIDs)
	if statementID == "" {
		statementID = logicalID
	}

	// Build a minimal policy doc with the single statement and pass it through
	// PutEventBusPolicy which replaces the whole policy.
	action := strProp(props, "Action", params, physicalIDs)
	if action == "" {
		action = "events:PutEvents"
	}

	principal := strProp(props, "Principal", params, physicalIDs)

	policy := fmt.Sprintf(
		`{"Version":"2012-10-17","Statement":[{"Sid":%q,"Effect":"Allow","Principal":{"AWS":%q},"Action":%q,"Resource":"*"}]}`,
		statementID, principal, action,
	)

	if err := rc.backends.EventBridge.Backend.PutEventBusPolicy(context.Background(), ebbackend.PutEventBusPolicyInput{
		EventBusName: eventBusName,
		Policy:       policy,
	}); err != nil {
		return "", fmt.Errorf("create Events EventBusPolicy on %s: %w", eventBusName, err)
	}

	return eventBusName + "/" + statementID, nil
}

func (rc *ResourceCreator) deleteEventBridgeEventBusPolicy(physicalID string) error {
	if rc.backends.EventBridge == nil {
		return nil
	}

	const parts = 2

	p := strings.SplitN(physicalID, "/", parts)
	if len(p) < parts {
		return nil
	}

	eventBusName := p[0]

	// Replace the policy with an empty one to remove the statement.
	return rc.backends.EventBridge.Backend.PutEventBusPolicy(context.Background(), ebbackend.PutEventBusPolicyInput{
		EventBusName: eventBusName,
		Policy:       `{"Version":"2012-10-17","Statement":[]}`,
	})
}

// ---- AWS::KMS::ReplicaKey ----

func (rc *ResourceCreator) createKMSReplicaKey(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.KMS == nil {
		return logicalID + "-stub", nil
	}

	primaryKeyID := strProp(props, "PrimaryKeyArn", params, physicalIDs)
	replicaRegion := strProp(props, "ReplicaRegion", params, physicalIDs)
	if replicaRegion == "" {
		replicaRegion = rc.backends.Region
	}

	out, err := rc.backends.KMS.Backend.ReplicateKey(context.Background(), &kmsbackend.ReplicateKeyInput{
		KeyID:         primaryKeyID,
		ReplicaRegion: replicaRegion,
		Description:   strProp(props, "Description", params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("create KMS ReplicaKey for %s: %w", primaryKeyID, err)
	}

	return out.ReplicaKeyMetadata.KeyID, nil
}

func (rc *ResourceCreator) deleteKMSReplicaKey(physicalID string) error {
	if rc.backends.KMS == nil {
		return nil
	}

	_, err := rc.backends.KMS.Backend.ScheduleKeyDeletion(context.Background(), &kmsbackend.ScheduleKeyDeletionInput{
		KeyID:               physicalID,
		PendingWindowInDays: kmsMinDeletionWindowDays,
	})

	return err
}

// ---- AWS::Cognito::IdentityPool ----

func (rc *ResourceCreator) createCognitoIdentityPool(
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

	allowUnauthenticated := false
	if v, ok := props["AllowUnauthenticatedIdentities"].(bool); ok {
		allowUnauthenticated = v
	}

	pool, err := rc.backends.CognitoIdentity.Backend.CreateIdentityPool(
		name, allowUnauthenticated, false, "", nil, nil, nil,
	)
	if err != nil {
		return "", fmt.Errorf("create Cognito IdentityPool %s: %w", name, err)
	}

	return pool.IdentityPoolID, nil
}

func (rc *ResourceCreator) deleteCognitoIdentityPool(physicalID string) error {
	if rc.backends.CognitoIdentity == nil {
		return nil
	}

	return rc.backends.CognitoIdentity.Backend.DeleteIdentityPool(physicalID)
}

// ---- AWS::Cognito::IdentityPoolRoleAttachment ----

func (rc *ResourceCreator) createCognitoIdentityPoolRoleAttachment(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIdentity == nil {
		return logicalID + "-stub", nil
	}

	poolID := strProp(props, "IdentityPoolId", params, physicalIDs)

	var authenticatedARN, unauthenticatedARN string
	if rolesRaw, ok := props["Roles"].(map[string]any); ok {
		if v, ok := rolesRaw["authenticated"].(string); ok {
			authenticatedARN = v
		}
		if v, ok := rolesRaw["unauthenticated"].(string); ok {
			unauthenticatedARN = v
		}
	}

	if err := rc.backends.CognitoIdentity.Backend.SetIdentityPoolRoles(
		poolID, authenticatedARN, unauthenticatedARN, nil,
	); err != nil {
		return "", fmt.Errorf("create Cognito IdentityPoolRoleAttachment for %s: %w", poolID, err)
	}

	return poolID + ":roles", nil
}

// ---- AWS::Cognito::UserPoolDomain ----

func (rc *ResourceCreator) createCognitoUserPoolDomain(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CognitoIDP == nil {
		return logicalID + "-stub", nil
	}

	userPoolID := strProp(props, "UserPoolId", params, physicalIDs)
	domain := strProp(props, "Domain", params, physicalIDs)
	if domain == "" {
		domain = logicalID
	}

	certArn := strProp(props, "CertificateArn", params, physicalIDs)

	_, err := rc.backends.CognitoIDP.Backend.CreateUserPoolDomainFull(userPoolID, domain, certArn)
	if err != nil {
		return "", fmt.Errorf("create Cognito UserPoolDomain %s: %w", domain, err)
	}

	return userPoolID + "/" + domain, nil
}

func (rc *ResourceCreator) deleteCognitoUserPoolDomain(physicalID string) error {
	if rc.backends.CognitoIDP == nil {
		return nil
	}

	const parts = 2

	p := strings.SplitN(physicalID, "/", parts)
	if len(p) < parts {
		return nil
	}

	return rc.backends.CognitoIDP.Backend.DeleteUserPoolDomain(p[0], p[1])
}

// ---- AWS::Cognito::UserPoolGroup ----

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

	var precedence int32
	if v, ok := props["Precedence"].(float64); ok {
		precedence = int32(v)
	}

	_, err := rc.backends.CognitoIDP.Backend.CreateGroup(userPoolID, groupName, description, precedence)
	if err != nil {
		return "", fmt.Errorf("create Cognito UserPoolGroup %s: %w", groupName, err)
	}

	return userPoolID + "/" + groupName, nil
}

func (rc *ResourceCreator) deleteCognitoUserPoolGroup(physicalID string) error {
	if rc.backends.CognitoIDP == nil {
		return nil
	}

	const parts = 2

	p := strings.SplitN(physicalID, "/", parts)
	if len(p) < parts {
		return nil
	}

	return rc.backends.CognitoIDP.Backend.DeleteGroup(p[0], p[1])
}

// ---- AWS::EC2::VPCPeeringConnection ----

func (rc *ResourceCreator) createEC2VPCPeeringConnection(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	requesterVPCID := strProp(props, "VpcId", params, physicalIDs)
	accepterVPCID := strProp(props, "PeerVpcId", params, physicalIDs)

	pc, err := rc.backends.EC2.Backend.CreateVpcPeeringConnection(requesterVPCID, accepterVPCID)
	if err != nil {
		return "", fmt.Errorf("create EC2 VPCPeeringConnection %s<->%s: %w", requesterVPCID, accepterVPCID, err)
	}

	return pc.VpcPeeringConnectionID, nil
}

func (rc *ResourceCreator) deleteEC2VPCPeeringConnection(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteVpcPeeringConnection(physicalID)
}

// ---- AWS::EC2::NetworkAcl ----

func (rc *ResourceCreator) createEC2NetworkAcl(
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
		return "", fmt.Errorf("create EC2 NetworkAcl in VPC %s: %w", vpcID, err)
	}

	return acl.ID, nil
}

func (rc *ResourceCreator) deleteEC2NetworkAcl(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteNetworkACL(physicalID)
}

// ---- AWS::EC2::NetworkAclEntry ----

func (rc *ResourceCreator) createEC2NetworkAclEntry(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	aclID := strProp(props, "NetworkAclId", params, physicalIDs)

	ruleNumber := 100
	if v, ok := props["RuleNumber"].(float64); ok {
		ruleNumber = int(v)
	}

	protocol := strProp(props, "Protocol", params, physicalIDs)
	if protocol == "" {
		protocol = "-1"
	}

	ruleAction := strProp(props, "RuleAction", params, physicalIDs)
	if ruleAction == "" {
		ruleAction = "allow"
	}

	cidr := strProp(props, "CidrBlock", params, physicalIDs)

	egress := false
	if v, ok := props["Egress"].(bool); ok {
		egress = v
	}

	fromPort := 0
	toPort := 0

	if pr, ok := props["PortRange"].(map[string]any); ok {
		if v, ok := pr["From"].(float64); ok {
			fromPort = int(v)
		}

		if v, ok := pr["To"].(float64); ok {
			toPort = int(v)
		}
	}

	if err := rc.backends.EC2.Backend.CreateNetworkACLEntry(
		aclID, ruleNumber, protocol, ruleAction, cidr, egress, fromPort, toPort,
	); err != nil {
		return "", fmt.Errorf("create EC2 NetworkAclEntry rule %d on %s: %w", ruleNumber, aclID, err)
	}

	egressStr := "ingress"
	if egress {
		egressStr = "egress"
	}

	return aclID + "/" + egressStr + "/" + strconv.Itoa(ruleNumber), nil
}

func (rc *ResourceCreator) deleteEC2NetworkAclEntry(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	// physicalID = "acl-xxx/ingress|egress/ruleNumber"
	const parts = 3

	p := strings.SplitN(physicalID, "/", parts)
	if len(p) < parts {
		return nil
	}

	aclID := p[0]
	egress := p[1] == "egress"

	ruleNumber, err := strconv.Atoi(p[2])
	if err != nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteNetworkACLEntry(aclID, ruleNumber, egress)
}

// ---- AWS::EC2::KeyPair ----

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

	publicKey := strProp(props, "PublicKeyMaterial", params, physicalIDs)

	if publicKey != "" {
		kp, err := rc.backends.EC2.Backend.ImportKeyPair(keyName, publicKey)
		if err != nil {
			return "", fmt.Errorf("import EC2 KeyPair %s: %w", keyName, err)
		}

		return kp.Name, nil
	}

	kp, err := rc.backends.EC2.Backend.CreateKeyPair(keyName)
	if err != nil {
		return "", fmt.Errorf("create EC2 KeyPair %s: %w", keyName, err)
	}

	return kp.Name, nil
}

func (rc *ResourceCreator) deleteEC2KeyPair(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteKeyPair(physicalID)
}

// ---- AWS::EC2::SecurityGroupIngress ----

// createEC2SecurityGroupIngress handles the standalone SecurityGroupIngress resource.
// Physical ID encodes groupID + protocol + fromPort + toPort + cidr for idempotent delete.
func (rc *ResourceCreator) createEC2SecurityGroupIngress(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	groupID := strProp(props, "GroupId", params, physicalIDs)
	protocol := strProp(props, "IpProtocol", params, physicalIDs)
	cidr := strProp(props, "CidrIp", params, physicalIDs)

	fromPort := 0
	toPort := 0

	if v, ok := props["FromPort"].(float64); ok {
		fromPort = int(v)
	}

	if v, ok := props["ToPort"].(float64); ok {
		toPort = int(v)
	}

	if err := rc.backends.EC2.Backend.AuthorizeSecurityGroupIngress(
		groupID,
		[]ec2backend.SecurityGroupRule{{
			Protocol: protocol,
			IPRange:  cidr,
			FromPort: fromPort,
			ToPort:   toPort,
		}},
	); err != nil {
		return "", fmt.Errorf("create EC2 SecurityGroupIngress on %s: %w", groupID, err)
	}

	return fmt.Sprintf("%s/ingress/%s/%d/%d/%s", groupID, protocol, fromPort, toPort, cidr), nil
}

// ---- AWS::EC2::SecurityGroupEgress ----

func (rc *ResourceCreator) createEC2SecurityGroupEgress(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.EC2 == nil {
		return logicalID + "-stub", nil
	}

	groupID := strProp(props, "GroupId", params, physicalIDs)
	protocol := strProp(props, "IpProtocol", params, physicalIDs)
	cidr := strProp(props, "CidrIp", params, physicalIDs)

	fromPort := 0
	toPort := 0

	if v, ok := props["FromPort"].(float64); ok {
		fromPort = int(v)
	}

	if v, ok := props["ToPort"].(float64); ok {
		toPort = int(v)
	}

	if err := rc.backends.EC2.Backend.AuthorizeSecurityGroupEgress(
		groupID,
		[]ec2backend.SecurityGroupRule{{
			Protocol: protocol,
			IPRange:  cidr,
			FromPort: fromPort,
			ToPort:   toPort,
		}},
	); err != nil {
		return "", fmt.Errorf("create EC2 SecurityGroupEgress on %s: %w", groupID, err)
	}

	return fmt.Sprintf("%s/egress/%s/%d/%d/%s", groupID, protocol, fromPort, toPort, cidr), nil
}

// ---- AWS::EC2::FlowLog ----

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
	logDest := strProp(props, "LogDestination", params, physicalIDs)

	fls, err := rc.backends.EC2.Backend.CreateFlowLogs(
		[]string{resourceID}, trafficType, logDestType, logDest,
	)
	if err != nil {
		return "", fmt.Errorf("create EC2 FlowLog for %s: %w", resourceID, err)
	}

	if len(fls) == 0 {
		return logicalID + "-stub", nil
	}

	return fls[0].FlowLogID, nil
}

func (rc *ResourceCreator) deleteEC2FlowLog(physicalID string) error {
	if rc.backends.EC2 == nil {
		return nil
	}

	return rc.backends.EC2.Backend.DeleteFlowLogs([]string{physicalID})
}

// ---- AWS::ElasticLoadBalancingV2::ListenerRule ----

func (rc *ResourceCreator) createELBv2ListenerRule(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.ELBv2 == nil {
		return logicalID + "-stub", nil
	}

	listenerArn := strProp(props, "ListenerArn", params, physicalIDs)

	priority := "1"
	if v, ok := props["Priority"].(float64); ok {
		priority = strconv.Itoa(int(v))
	} else if s := strProp(props, "Priority", params, physicalIDs); s != "" {
		priority = s
	}

	rule, err := rc.backends.ELBv2.Backend.CreateRule(elbv2backend.CreateRuleInput{
		ListenerArn: listenerArn,
		Priority:    priority,
	})
	if err != nil {
		return "", fmt.Errorf("create ELBv2 ListenerRule on %s: %w", listenerArn, err)
	}

	return rule.RuleArn, nil
}

func (rc *ResourceCreator) deleteELBv2ListenerRule(physicalID string) error {
	if rc.backends.ELBv2 == nil {
		return nil
	}

	return rc.backends.ELBv2.Backend.DeleteRule(physicalID)
}

// ---- AWS::Lambda::EventInvokeConfig ----

func (rc *ResourceCreator) createLambdaEventInvokeConfig(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Lambda == nil {
		return logicalID + "-stub", nil
	}

	functionName := strProp(props, "FunctionName", params, physicalIDs)

	input := &lambdabackend.PutFunctionEventInvokeConfigInput{}

	if v, ok := props["MaximumRetryAttempts"].(float64); ok {
		n := int(v)
		input.MaximumRetryAttempts = &n
	}

	if v, ok := props["MaximumEventAgeInSeconds"].(float64); ok {
		n := int(v)
		input.MaximumEventAgeInSeconds = &n
	}

	ib, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	cfg, err := ib.PutFunctionEventInvokeConfig(functionName, input)
	if err != nil {
		return "", fmt.Errorf("create Lambda EventInvokeConfig for %s: %w", functionName, err)
	}

	return cfg.FunctionArn, nil
}

func (rc *ResourceCreator) deleteLambdaEventInvokeConfig(physicalID string) error {
	if rc.backends.Lambda == nil {
		return nil
	}

	ib, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil
	}

	name := resourceNameFromARN(physicalID)
	if name == "" {
		name = physicalID
	}

	err := ib.DeleteFunctionEventInvokeConfig(name)
	if err != nil && err != lambdabackend.ErrEventInvokeConfigNotFound {
		return err
	}

	return nil
}

// ---- AWS::Lambda::Url ----

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
		functionName = strProp(props, "FunctionName", params, physicalIDs)
	}

	// Extract function name from ARN if needed.
	name := resourceNameFromARN(functionName)
	if name == "" {
		name = functionName
	}

	authType := strProp(props, "AuthType", params, physicalIDs)
	if authType == "" {
		authType = "NONE"
	}

	ib, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return logicalID + "-stub", nil
	}

	cfg, err := ib.CreateFunctionURLConfig(name, authType, nil, "")
	if err != nil {
		return "", fmt.Errorf("create Lambda Url for %s: %w", name, err)
	}

	return cfg.FunctionURL, nil
}

func (rc *ResourceCreator) deleteLambdaUrl(physicalID string) error {
	if rc.backends.Lambda == nil {
		return nil
	}

	ib, ok := rc.backends.Lambda.Backend.(*lambdabackend.InMemoryBackend)
	if !ok {
		return nil
	}

	name := resourceNameFromARN(physicalID)
	if name == "" {
		name = physicalID
	}

	err := ib.DeleteFunctionURLConfig(name)
	if err != nil && err != lambdabackend.ErrFunctionURLNotFound {
		return err
	}

	return nil
}

