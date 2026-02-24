package apigateway

import "time"

// RestApi represents an API Gateway REST API.
type RestAPI struct {
	CreatedDate time.Time         `json:"createdDate"`
	Tags        map[string]string `json:"tags,omitempty"`
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
}

// Resource represents an API Gateway resource.
type Resource struct {
	ResourceMethods map[string]*Method `json:"resourceMethods,omitempty"`
	ID              string             `json:"id"`
	ParentID        string             `json:"parentId,omitempty"`
	PathPart        string             `json:"pathPart,omitempty"`
	Path            string             `json:"path"`
	RestAPIID       string             `json:"-"`
}

// Method represents an API Gateway method on a resource.
type Method struct {
	RequestParameters map[string]bool            `json:"requestParameters,omitempty"`
	MethodIntegration *Integration               `json:"methodIntegration,omitempty"`
	MethodResponses   map[string]*MethodResponse `json:"methodResponses,omitempty"`
	HTTPMethod        string                     `json:"httpMethod"`
	AuthorizationType string                     `json:"authorizationType"`
	APIKeyRequired    bool                       `json:"apiKeyRequired"`
}

// Integration represents a method integration.
type Integration struct {
	RequestTemplates     map[string]string               `json:"requestTemplates,omitempty"`
	IntegrationResponses map[string]*IntegrationResponse `json:"integrationResponses,omitempty"`
	Type                 string                          `json:"type"`
	HTTPMethod           string                          `json:"httpMethod,omitempty"`
	URI                  string                          `json:"uri,omitempty"`
	PassthroughBehavior  string                          `json:"passthroughBehavior,omitempty"`
}

// IntegrationResponse represents a response from an integration.
type IntegrationResponse struct {
	ResponseTemplates  map[string]string `json:"responseTemplates,omitempty"`
	ResponseParameters map[string]string `json:"responseParameters,omitempty"`
	StatusCode         string            `json:"statusCode"`
}

// MethodResponse represents a method response configuration.
type MethodResponse struct {
	ResponseModels     map[string]string `json:"responseModels,omitempty"`
	ResponseParameters map[string]bool   `json:"responseParameters,omitempty"`
	StatusCode         string            `json:"statusCode"`
}

// Stage represents a deployment stage.
type Stage struct {
	CreatedDate     time.Time         `json:"createdDate"`
	LastUpdatedDate time.Time         `json:"lastUpdatedDate"`
	Variables       map[string]string `json:"variables,omitempty"`
	StageName       string            `json:"stageName"`
	RestAPIID       string            `json:"-"`
	DeploymentID    string            `json:"deploymentId"`
	Description     string            `json:"description,omitempty"`
}

// Deployment represents a REST API deployment.
type Deployment struct {
	CreatedDate time.Time `json:"createdDate"`
	ID          string    `json:"id"`
	RestAPIID   string    `json:"-"`
	Description string    `json:"description,omitempty"`
}

// PutIntegrationInput is the input for PutIntegration.
type PutIntegrationInput struct {
	RequestTemplates    map[string]string `json:"requestTemplates,omitempty"`
	Type                string            `json:"type"`
	HTTPMethod          string            `json:"httpMethod,omitempty"`
	URI                 string            `json:"uri,omitempty"`
	PassthroughBehavior string            `json:"passthroughBehavior,omitempty"`
}

// ErrorResponse is the JSON error format for API Gateway clients.
type ErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}
