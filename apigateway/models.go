package apigateway

import "time"

// RestApi represents an API Gateway REST API.
type RestApi struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	CreatedDate time.Time         `json:"createdDate"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// Resource represents an API Gateway resource.
type Resource struct {
	ID              string             `json:"id"`
	ParentID        string             `json:"parentId,omitempty"`
	PathPart        string             `json:"pathPart,omitempty"`
	Path            string             `json:"path"`
	RestApiID       string             `json:"-"`
	ResourceMethods map[string]*Method `json:"resourceMethods,omitempty"`
}

// Method represents an API Gateway method on a resource.
type Method struct {
	HttpMethod        string              `json:"httpMethod"`
	AuthorizationType string              `json:"authorizationType"`
	ApiKeyRequired    bool                `json:"apiKeyRequired"`
	RequestParameters map[string]bool     `json:"requestParameters,omitempty"`
	MethodIntegration *Integration        `json:"methodIntegration,omitempty"`
	MethodResponses   map[string]*MethodResponse `json:"methodResponses,omitempty"`
}

// Integration represents a method integration.
type Integration struct {
	Type                 string                       `json:"type"`
	HttpMethod           string                       `json:"httpMethod,omitempty"`
	Uri                  string                       `json:"uri,omitempty"`
	PassthroughBehavior  string                       `json:"passthroughBehavior,omitempty"`
	RequestTemplates     map[string]string            `json:"requestTemplates,omitempty"`
	IntegrationResponses map[string]*IntegrationResponse `json:"integrationResponses,omitempty"`
}

// IntegrationResponse represents a response from an integration.
type IntegrationResponse struct {
	StatusCode         string            `json:"statusCode"`
	ResponseTemplates  map[string]string `json:"responseTemplates,omitempty"`
	ResponseParameters map[string]string `json:"responseParameters,omitempty"`
}

// MethodResponse represents a method response configuration.
type MethodResponse struct {
	StatusCode         string            `json:"statusCode"`
	ResponseModels     map[string]string `json:"responseModels,omitempty"`
	ResponseParameters map[string]bool   `json:"responseParameters,omitempty"`
}

// Stage represents a deployment stage.
type Stage struct {
	StageName       string            `json:"stageName"`
	RestApiID       string            `json:"-"`
	DeploymentID    string            `json:"deploymentId"`
	Description     string            `json:"description,omitempty"`
	CreatedDate     time.Time         `json:"createdDate"`
	LastUpdatedDate time.Time         `json:"lastUpdatedDate"`
	Variables       map[string]string `json:"variables,omitempty"`
}

// Deployment represents a REST API deployment.
type Deployment struct {
	ID          string    `json:"id"`
	RestApiID   string    `json:"-"`
	Description string    `json:"description,omitempty"`
	CreatedDate time.Time `json:"createdDate"`
}

// PutIntegrationInput is the input for PutIntegration.
type PutIntegrationInput struct {
	Type                string            `json:"type"`
	HttpMethod          string            `json:"httpMethod,omitempty"`
	Uri                 string            `json:"uri,omitempty"`
	PassthroughBehavior string            `json:"passthroughBehavior,omitempty"`
	RequestTemplates    map[string]string `json:"requestTemplates,omitempty"`
}

// ErrorResponse is the JSON error format for API Gateway clients.
type ErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}
