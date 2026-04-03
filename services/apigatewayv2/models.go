package apigatewayv2

import (
	"encoding/json"
	"time"
)

// isoTime wraps [time.Time] and marshals to/from a JSON string in RFC3339 format,
// which is the __timestampIso8601 format expected by the AWS SDK v2 API Gateway V2 client.
type isoTime struct {
	time.Time
}

func (t isoTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.UTC().Format(time.RFC3339))
}

func (t *isoTime) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return err
	}

	t.Time = parsed

	return nil
}

// API represents an HTTP API (API Gateway v2).
type API struct {
	CreatedDate              isoTime           `json:"createdDate"`
	Tags                     map[string]string `json:"tags,omitempty"`
	APIID                    string            `json:"apiId"`
	Name                     string            `json:"name"`
	Description              string            `json:"description,omitempty"`
	ProtocolType             string            `json:"protocolType"`
	RouteSelectionExpression string            `json:"routeSelectionExpression,omitempty"`
	APIEndpoint              string            `json:"apiEndpoint,omitempty"`
	Version                  string            `json:"version,omitempty"`
}

// Stage represents a deployment stage for an HTTP API.
type Stage struct {
	CreatedDate     isoTime           `json:"createdDate"`
	LastUpdatedDate isoTime           `json:"lastUpdatedDate"`
	StageVariables  map[string]string `json:"stageVariables,omitempty"`
	StageName       string            `json:"stageName"`
	APIID           string            `json:"-"`
	DeploymentID    string            `json:"deploymentId,omitempty"`
	Description     string            `json:"description,omitempty"`
	AutoDeploy      bool              `json:"autoDeploy"`
}

// Route represents a route in an HTTP API.
type Route struct {
	RouteID           string `json:"routeId"`
	APIID             string `json:"-"`
	RouteKey          string `json:"routeKey"`
	Target            string `json:"target,omitempty"`
	AuthorizationType string `json:"authorizationType,omitempty"`
	AuthorizerID      string `json:"authorizerId,omitempty"`
	OperationName     string `json:"operationName,omitempty"`
}

// Integration represents a backend integration for a route.
type Integration struct {
	IntegrationID        string `json:"integrationId"`
	APIID                string `json:"-"`
	IntegrationType      string `json:"integrationType"`
	IntegrationMethod    string `json:"integrationMethod,omitempty"`
	IntegrationURI       string `json:"integrationUri,omitempty"`
	Description          string `json:"description,omitempty"`
	PayloadFormatVersion string `json:"payloadFormatVersion,omitempty"`
	ConnectionType       string `json:"connectionType,omitempty"`
	ConnectionID         string `json:"connectionId,omitempty"`
	TimeoutInMillis      int32  `json:"timeoutInMillis,omitempty"`
}

// Deployment represents an API deployment.
type Deployment struct {
	CreatedDate      isoTime `json:"createdDate"`
	DeploymentID     string  `json:"deploymentId"`
	APIID            string  `json:"-"`
	Description      string  `json:"description,omitempty"`
	DeploymentStatus string  `json:"deploymentStatus"`
}

// Authorizer represents an authorizer for an HTTP API.
type Authorizer struct {
	AuthorizerID             string `json:"authorizerId"`
	APIID                    string `json:"-"`
	Name                     string `json:"name"`
	AuthorizerType           string `json:"authorizerType"`
	AuthorizerURI            string `json:"authorizerUri,omitempty"`
	IdentitySource           string `json:"identitySource,omitempty"`
	AuthorizerCredentialsArn string `json:"authorizerCredentialsArn,omitempty"`
	// AuthorizerResultTTLInSeconds uses 'Ttl' (not 'TTL') in the JSON tag to match the AWS API wire format.
	AuthorizerResultTTLInSeconds int32 `json:"authorizerResultTtlInSeconds,omitempty"`
}

// CreateAPIInput is the input for CreateAPI.
type CreateAPIInput struct {
	Tags                     map[string]string `json:"tags,omitempty"`
	Name                     string            `json:"name"`
	Description              string            `json:"description,omitempty"`
	ProtocolType             string            `json:"protocolType"`
	RouteSelectionExpression string            `json:"routeSelectionExpression,omitempty"`
	Version                  string            `json:"version,omitempty"`
}

// UpdateAPIInput is the input for UpdateAPI (PATCH).
type UpdateAPIInput struct {
	Tags                     map[string]string `json:"tags,omitempty"`
	Name                     string            `json:"name,omitempty"`
	Description              string            `json:"description,omitempty"`
	RouteSelectionExpression string            `json:"routeSelectionExpression,omitempty"`
	Version                  string            `json:"version,omitempty"`
}

// CreateStageInput is the input for CreateStage.
type CreateStageInput struct {
	StageVariables map[string]string `json:"stageVariables,omitempty"`
	StageName      string            `json:"stageName"`
	DeploymentID   string            `json:"deploymentId,omitempty"`
	Description    string            `json:"description,omitempty"`
	AutoDeploy     bool              `json:"autoDeploy"`
}

// UpdateStageInput is the input for UpdateStage (PATCH).
type UpdateStageInput struct {
	StageVariables map[string]string `json:"stageVariables,omitempty"`
	AutoDeploy     *bool             `json:"autoDeploy,omitempty"`
	DeploymentID   string            `json:"deploymentId,omitempty"`
	Description    string            `json:"description,omitempty"`
}

// CreateRouteInput is the input for CreateRoute.
type CreateRouteInput struct {
	RouteKey          string `json:"routeKey"`
	Target            string `json:"target,omitempty"`
	AuthorizationType string `json:"authorizationType,omitempty"`
	AuthorizerID      string `json:"authorizerId,omitempty"`
	OperationName     string `json:"operationName,omitempty"`
}

// UpdateRouteInput is the input for UpdateRoute (PATCH).
type UpdateRouteInput struct {
	RouteKey          string `json:"routeKey,omitempty"`
	Target            string `json:"target,omitempty"`
	AuthorizationType string `json:"authorizationType,omitempty"`
	AuthorizerID      string `json:"authorizerId,omitempty"`
	OperationName     string `json:"operationName,omitempty"`
}

// CreateIntegrationInput is the input for CreateIntegration.
type CreateIntegrationInput struct {
	IntegrationType      string `json:"integrationType"`
	IntegrationMethod    string `json:"integrationMethod,omitempty"`
	IntegrationURI       string `json:"integrationUri,omitempty"`
	Description          string `json:"description,omitempty"`
	PayloadFormatVersion string `json:"payloadFormatVersion,omitempty"`
	ConnectionType       string `json:"connectionType,omitempty"`
	ConnectionID         string `json:"connectionId,omitempty"`
	TimeoutInMillis      int32  `json:"timeoutInMillis,omitempty"`
}

// UpdateIntegrationInput is the input for UpdateIntegration (PATCH).
type UpdateIntegrationInput struct {
	IntegrationType      string `json:"integrationType,omitempty"`
	IntegrationMethod    string `json:"integrationMethod,omitempty"`
	IntegrationURI       string `json:"integrationUri,omitempty"`
	Description          string `json:"description,omitempty"`
	PayloadFormatVersion string `json:"payloadFormatVersion,omitempty"`
	ConnectionType       string `json:"connectionType,omitempty"`
	ConnectionID         string `json:"connectionId,omitempty"`
	TimeoutInMillis      int32  `json:"timeoutInMillis,omitempty"`
}

// CreateDeploymentInput is the input for CreateDeployment.
type CreateDeploymentInput struct {
	Description string `json:"description,omitempty"`
	StageName   string `json:"stageName,omitempty"`
}

// CreateAuthorizerInput is the input for CreateAuthorizer.
type CreateAuthorizerInput struct {
	Name                         string `json:"name"`
	AuthorizerType               string `json:"authorizerType"`
	AuthorizerURI                string `json:"authorizerUri,omitempty"`
	IdentitySource               string `json:"identitySource,omitempty"`
	AuthorizerCredentialsArn     string `json:"authorizerCredentialsArn,omitempty"`
	AuthorizerResultTTLInSeconds int32  `json:"authorizerResultTtlInSeconds,omitempty"`
}

// UpdateAuthorizerInput is the input for UpdateAuthorizer (PATCH).
type UpdateAuthorizerInput struct {
	Name                         string `json:"name,omitempty"`
	AuthorizerType               string `json:"authorizerType,omitempty"`
	AuthorizerURI                string `json:"authorizerUri,omitempty"`
	IdentitySource               string `json:"identitySource,omitempty"`
	AuthorizerCredentialsArn     string `json:"authorizerCredentialsArn,omitempty"`
	AuthorizerResultTTLInSeconds int32  `json:"authorizerResultTtlInSeconds,omitempty"`
}

// listApisOutput is the response body for GetAPIs.
type listApisOutput struct {
	NextToken string `json:"nextToken,omitempty"`
	Items     []API  `json:"items"`
}

// listStagesOutput is the response body for GetStages.
type listStagesOutput struct {
	NextToken string  `json:"nextToken,omitempty"`
	Items     []Stage `json:"items"`
}

// listRoutesOutput is the response body for GetRoutes.
type listRoutesOutput struct {
	NextToken string  `json:"nextToken,omitempty"`
	Items     []Route `json:"items"`
}

// listIntegrationsOutput is the response body for GetIntegrations.
type listIntegrationsOutput struct {
	NextToken string        `json:"nextToken,omitempty"`
	Items     []Integration `json:"items"`
}

// listDeploymentsOutput is the response body for GetDeployments.
type listDeploymentsOutput struct {
	NextToken string       `json:"nextToken,omitempty"`
	Items     []Deployment `json:"items"`
}

// listAuthorizersOutput is the response body for GetAuthorizers.
type listAuthorizersOutput struct {
	NextToken string       `json:"nextToken,omitempty"`
	Items     []Authorizer `json:"items"`
}

// notFoundResponse is returned when a resource is not found.
type notFoundResponse struct {
	Message string `json:"message"`
}

// DomainName represents a custom domain name for API Gateway v2.
type DomainName struct {
	Tags                          map[string]string `json:"tags,omitempty"`
	DomainNameValue               string            `json:"domainName"`
	APIMappingSelectionExpression string            `json:"apiMappingSelectionExpression,omitempty"`
}

// CreateDomainNameInput is the input for CreateDomainName.
type CreateDomainNameInput struct {
	Tags            map[string]string `json:"tags,omitempty"`
	DomainNameValue string            `json:"domainName"`
}

// APIMapping represents an API mapping for a custom domain name.
type APIMapping struct {
	APIID         string `json:"apiId"`
	APIMappingID  string `json:"apiMappingId"`
	DomainName    string `json:"-"`
	Stage         string `json:"stage"`
	APIMappingKey string `json:"apiMappingKey,omitempty"`
}

// CreateAPIMappingInput is the input for CreateAPIMapping.
type CreateAPIMappingInput struct {
	APIID         string `json:"apiId"`
	Stage         string `json:"stage"`
	APIMappingKey string `json:"apiMappingKey,omitempty"`
}

// IntegrationResponse represents an integration response.
type IntegrationResponse struct {
	ResponseParameters          map[string]string `json:"responseParameters,omitempty"`
	ResponseTemplates           map[string]string `json:"responseTemplates,omitempty"`
	IntegrationResponseID       string            `json:"integrationResponseId"`
	IntegrationResponseKey      string            `json:"integrationResponseKey"`
	APIID                       string            `json:"-"`
	IntegrationID               string            `json:"-"`
	ContentHandlingStrategy     string            `json:"contentHandlingStrategy,omitempty"`
	TemplateSelectionExpression string            `json:"templateSelectionExpression,omitempty"`
}

// CreateIntegrationResponseInput is the input for CreateIntegrationResponse.
type CreateIntegrationResponseInput struct {
	ResponseParameters          map[string]string `json:"responseParameters,omitempty"`
	ResponseTemplates           map[string]string `json:"responseTemplates,omitempty"`
	IntegrationResponseKey      string            `json:"integrationResponseKey"`
	ContentHandlingStrategy     string            `json:"contentHandlingStrategy,omitempty"`
	TemplateSelectionExpression string            `json:"templateSelectionExpression,omitempty"`
}

// Model represents a data model for an API.
type Model struct {
	ModelID     string `json:"modelId"`
	APIID       string `json:"-"`
	Name        string `json:"name"`
	Schema      string `json:"schema,omitempty"`
	ContentType string `json:"contentType,omitempty"`
	Description string `json:"description,omitempty"`
}

// CreateModelInput is the input for CreateModel.
type CreateModelInput struct {
	Name        string `json:"name"`
	Schema      string `json:"schema,omitempty"`
	ContentType string `json:"contentType,omitempty"`
	Description string `json:"description,omitempty"`
}

// Portal represents an API Gateway v2 portal.
type Portal struct {
	Tags     map[string]string `json:"tags,omitempty"`
	PortalID string            `json:"portalId"`
	LogoURI  string            `json:"logoUri,omitempty"`
	Status   string            `json:"status,omitempty"`
}

// CreatePortalInput is the input for CreatePortal.
type CreatePortalInput struct {
	Tags    map[string]string `json:"tags,omitempty"`
	LogoURI string            `json:"logoUri,omitempty"`
}

// PortalProduct represents a portal product.
type PortalProduct struct {
	Tags            map[string]string `json:"tags,omitempty"`
	PortalProductID string            `json:"portalProductId"`
	DisplayName     string            `json:"displayName"`
	Description     string            `json:"description,omitempty"`
}

// CreatePortalProductInput is the input for CreatePortalProduct.
type CreatePortalProductInput struct {
	Tags        map[string]string `json:"tags,omitempty"`
	DisplayName string            `json:"displayName"`
	Description string            `json:"description,omitempty"`
}

// ProductPage represents a product page within a portal product.
type ProductPage struct {
	LastModified    *isoTime `json:"lastModified,omitempty"`
	ProductPageID   string   `json:"productPageId"`
	PortalProductID string   `json:"-"`
}

// CreateProductPageInput is the input for CreateProductPage.
type CreateProductPageInput struct {
	PortalProductID string `json:"portalProductId"`
}

// ProductRestEndpointPage represents a REST endpoint page within a portal product.
type ProductRestEndpointPage struct {
	LastModified              *isoTime `json:"lastModified,omitempty"`
	ProductRestEndpointPageID string   `json:"productRestEndpointPageId"`
	PortalProductID           string   `json:"-"`
}

// CreateProductRestEndpointPageInput is the input for CreateProductRestEndpointPage.
type CreateProductRestEndpointPageInput struct {
	PortalProductID string `json:"portalProductId"`
}

// RouteResponse represents a route response.
type RouteResponse struct {
	ResponseModels           map[string]string `json:"responseModels,omitempty"`
	RouteResponseID          string            `json:"routeResponseId"`
	RouteResponseKey         string            `json:"routeResponseKey"`
	APIID                    string            `json:"-"`
	RouteID                  string            `json:"-"`
	ModelSelectionExpression string            `json:"modelSelectionExpression,omitempty"`
}

// CreateRouteResponseInput is the input for CreateRouteResponse.
type CreateRouteResponseInput struct {
	ResponseModels           map[string]string `json:"responseModels,omitempty"`
	RouteResponseKey         string            `json:"routeResponseKey"`
	ModelSelectionExpression string            `json:"modelSelectionExpression,omitempty"`
}
