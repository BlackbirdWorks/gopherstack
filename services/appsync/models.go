package appsync

import "github.com/blackbirdworks/gopherstack/pkgs/tags"

// AuthenticationType represents the authentication type for a GraphQL API.
type AuthenticationType string

const (
	// AuthTypeAPIKey uses API key authentication.
	AuthTypeAPIKey AuthenticationType = "API_KEY"
	// AuthTypeIAM uses IAM authentication.
	AuthTypeIAM AuthenticationType = "AWS_IAM"
	// AuthTypeCognito uses Amazon Cognito user pools authentication.
	AuthTypeCognito AuthenticationType = "AMAZON_COGNITO_USER_POOLS"
	// AuthTypeOIDC uses OpenID Connect authentication.
	AuthTypeOIDC AuthenticationType = "OPENID_CONNECT"
	// AuthTypeLambda uses Lambda function authentication.
	AuthTypeLambda AuthenticationType = "AWS_LAMBDA"
)

// DataSourceType represents the type of a data source.
type DataSourceType string

const (
	// DataSourceTypeNone is a no-op data source.
	DataSourceTypeNone DataSourceType = "NONE"
	// DataSourceTypeLambda invokes a Lambda function.
	DataSourceTypeLambda DataSourceType = "AWS_LAMBDA"
	// DataSourceTypeDynamoDB queries a DynamoDB table.
	DataSourceTypeDynamoDB DataSourceType = "AMAZON_DYNAMODB"
	// DataSourceTypeHTTP forwards requests to an HTTP endpoint.
	DataSourceTypeHTTP DataSourceType = "HTTP"
	// DataSourceTypeOpenSearch queries an OpenSearch domain.
	DataSourceTypeOpenSearch DataSourceType = "AMAZON_OPENSEARCH_SERVICE"
)

// LambdaDataSourceConfig holds the configuration for a Lambda data source.
type LambdaDataSourceConfig struct {
	LambdaFunctionARN string `json:"lambdaFunctionArn"`
}

// DynamoDBDataSourceConfig holds the configuration for a DynamoDB data source.
type DynamoDBDataSourceConfig struct {
	TableName            string `json:"tableName"`
	AWSRegion            string `json:"awsRegion"`
	UseCallerCredentials bool   `json:"useCallerCredentials"`
	Versioned            bool   `json:"versioned"`
}

// DataSource represents an AppSync data source.
type DataSource struct {
	Tags           *tags.Tags                `json:"tags,omitempty"`
	LambdaConfig   *LambdaDataSourceConfig   `json:"lambdaConfig,omitempty"`
	DynamoDBConfig *DynamoDBDataSourceConfig `json:"dynamodbConfig,omitempty"`
	DataSourceARN  string                    `json:"dataSourceArn"`
	Name           string                    `json:"name"`
	Description    string                    `json:"description,omitempty"`
	ServiceRoleARN string                    `json:"serviceRoleArn,omitempty"`
	APIID          string                    `json:"apiId"`
	Type           DataSourceType            `json:"type"`
}

// Resolver represents an AppSync resolver.
type Resolver struct {
	RequestMappingTemplate  string `json:"requestMappingTemplate,omitempty"`
	ResponseMappingTemplate string `json:"responseMappingTemplate,omitempty"`
	DataSourceName          string `json:"dataSourceName,omitempty"`
	ResolverARN             string `json:"resolverArn"`
	TypeName                string `json:"typeName"`
	FieldName               string `json:"fieldName"`
	APIID                   string `json:"apiId"`
	Kind                    string `json:"kind,omitempty"`
}

// GraphqlAPI represents an AppSync GraphQL API.
type GraphqlAPI struct {
	URIs               map[string]string  `json:"uris"`
	Tags               *tags.Tags         `json:"tags,omitempty"`
	Name               string             `json:"name"`
	APIID              string             `json:"apiId"`
	ARN                string             `json:"arn"`
	AuthenticationType AuthenticationType `json:"authenticationType"`
	Region             string             `json:"region"`
}

// SchemaStatus represents the schema creation status.
type SchemaStatus string

const (
	// SchemaStatusProcessing indicates the schema is being processed.
	SchemaStatusProcessing SchemaStatus = "PROCESSING"
	// SchemaStatusActive indicates the schema is active.
	SchemaStatusActive SchemaStatus = "ACTIVE"
	// SchemaStatusDeleting indicates the schema is being deleted.
	SchemaStatusDeleting SchemaStatus = "DELETING"
	// SchemaStatusFailed indicates the schema creation failed.
	SchemaStatusFailed SchemaStatus = "FAILED"
	// SchemaStatusNotApplicable indicates schema creation is not applicable.
	SchemaStatusNotApplicable SchemaStatus = "NOT_APPLICABLE"
)

// Schema represents a stored GraphQL schema.
type Schema struct {
	SDL     string       `json:"sdl"`
	Status  SchemaStatus `json:"status"`
	Details string       `json:"details,omitempty"`
	APIID   string       `json:"apiId"`
}
