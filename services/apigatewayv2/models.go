package apigatewayv2

import (
	"encoding/json"
	"time"
)

// unixEpochTime wraps [time.Time] and marshals to/from a JSON number (Unix seconds),
// which is the format expected by the AWS SDK v2 API Gateway v2 client.
type unixEpochTime struct {
	time.Time
}

func (t unixEpochTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Unix())
}

func (t *unixEpochTime) UnmarshalJSON(b []byte) error {
	var epoch int64
	if err := json.Unmarshal(b, &epoch); err != nil {
		return err
	}

	t.Time = time.Unix(epoch, 0)

	return nil
}

// API represents an HTTP API (API Gateway v2).
type API struct {
	CreatedDate              unixEpochTime     `json:"createdDate"`
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
	CreatedDate     unixEpochTime     `json:"createdDate"`
	LastUpdatedDate unixEpochTime     `json:"lastUpdatedDate"`
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
	CreatedDate      unixEpochTime `json:"createdDate"`
	DeploymentID     string        `json:"deploymentId"`
	APIID            string        `json:"-"`
	Description      string        `json:"description,omitempty"`
	DeploymentStatus string        `json:"deploymentStatus"`
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
