package appsync

import (
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

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

// GraphqlAPIVisibility values for the Visibility field on a GraphQL API.
const (
	VisibilityGlobal  = "GLOBAL"
	VisibilityPrivate = "PRIVATE"
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
	// DataSourceTypeRelational queries an RDS database.
	DataSourceTypeRelational DataSourceType = "RELATIONAL_DATABASE"
	// DataSourceTypeOpenSearch queries an OpenSearch domain.
	DataSourceTypeOpenSearch DataSourceType = "AMAZON_OPENSEARCH_SERVICE"
	// DataSourceTypeEventBridge sends events to an Amazon EventBridge bus.
	DataSourceTypeEventBridge DataSourceType = "AMAZON_EVENTBRIDGE"
)

// LambdaDataSourceConfig holds the configuration for a Lambda data source.
type LambdaDataSourceConfig struct {
	LambdaFunctionARN string `json:"lambdaFunctionArn"`
}

// DeltaSyncConfig holds the Delta Sync configuration for a versioned DynamoDB data source.
type DeltaSyncConfig struct {
	DeltaSyncTableName string `json:"deltaSyncTableName,omitempty"`
	BaseTableTTL       int64  `json:"baseTableTTL,omitempty"`
	DeltaSyncTableTTL  int64  `json:"deltaSyncTableTTL,omitempty"`
}

// DynamoDBDataSourceConfig holds the configuration for a DynamoDB data source.
type DynamoDBDataSourceConfig struct {
	DeltaSyncConfig      *DeltaSyncConfig `json:"deltaSyncConfig,omitempty"`
	TableName            string           `json:"tableName"`
	AWSRegion            string           `json:"awsRegion"`
	UseCallerCredentials bool             `json:"useCallerCredentials"`
	Versioned            bool             `json:"versioned"`
}

// HTTPDataSourceConfig holds the configuration for an HTTP endpoint data source.
type HTTPDataSourceConfig struct {
	Endpoint string `json:"endpoint"`
}

// OpenSearchServiceDataSourceConfig holds config for an OpenSearch data source.
type OpenSearchServiceDataSourceConfig struct {
	Endpoint  string `json:"endpoint"`
	AWSRegion string `json:"awsRegion"`
}

// EventBridgeDataSourceConfig holds the configuration for an EventBridge data source.
type EventBridgeDataSourceConfig struct {
	EventBusARN string `json:"eventBusArn"`
}

// RelationalDatabaseDataSourceConfig holds config for a relational database data source.
type RelationalDatabaseDataSourceConfig struct {
	RDSHTTPEndpointConfig        *RDSHTTPEndpointConfig `json:"rdsHttpEndpointConfig,omitempty"`
	RelationalDatabaseSourceType string                 `json:"relationalDatabaseSourceType,omitempty"`
}

// RDSHTTPEndpointConfig holds the RDS HTTP endpoint configuration.
type RDSHTTPEndpointConfig struct {
	DatabaseName        string `json:"databaseName,omitempty"`
	DBClusterIdentifier string `json:"dbClusterIdentifier,omitempty"`
	AWSRegion           string `json:"awsRegion,omitempty"`
	Schema              string `json:"schema,omitempty"`
	AWSSecretStoreARN   string `json:"awsSecretStoreArn,omitempty"`
}

// DataSource represents an AppSync data source.
type DataSource struct {
	Tags                     *tags.Tags                          `json:"tags,omitempty"`
	LambdaConfig             *LambdaDataSourceConfig             `json:"lambdaConfig,omitempty"`
	DynamoDBConfig           *DynamoDBDataSourceConfig           `json:"dynamodbConfig,omitempty"`
	HTTPConfig               *HTTPDataSourceConfig               `json:"httpConfig,omitempty"`
	OpenSearchConfig         *OpenSearchServiceDataSourceConfig  `json:"openSearchServiceConfig,omitempty"`
	EventBridgeConfig        *EventBridgeDataSourceConfig        `json:"eventBridgeConfig,omitempty"`
	RelationalDatabaseConfig *RelationalDatabaseDataSourceConfig `json:"relationalDatabaseConfig,omitempty"`
	DataSourceARN            string                              `json:"dataSourceArn"`
	Name                     string                              `json:"name"`
	Description              string                              `json:"description,omitempty"`
	ServiceRoleARN           string                              `json:"serviceRoleArn,omitempty"`
	APIID                    string                              `json:"apiId"`
	Type                     DataSourceType                      `json:"type"`
}

// CachingConfig holds the caching configuration for a resolver.
type CachingConfig struct {
	CachingKeys []string `json:"cachingKeys,omitempty"`
	TTL         int64    `json:"ttl"`
}

// LambdaConflictHandlerConfig holds config for Lambda conflict resolution.
type LambdaConflictHandlerConfig struct {
	LambdaConflictHandlerARN string `json:"lambdaConflictHandlerArn,omitempty"`
}

// SyncConfig holds the conflict detection/resolution configuration for a resolver.
type SyncConfig struct {
	LambdaConflictHandlerConfig *LambdaConflictHandlerConfig `json:"lambdaConflictHandlerConfig,omitempty"`
	ConflictDetection           string                       `json:"conflictDetection,omitempty"`
	ConflictHandler             string                       `json:"conflictHandler,omitempty"`
}

// Resolver represents an AppSync resolver.
type Resolver struct {
	CachingConfig           *CachingConfig `json:"cachingConfig,omitempty"`
	SyncConfig              *SyncConfig    `json:"syncConfig,omitempty"`
	RequestMappingTemplate  string         `json:"requestMappingTemplate,omitempty"`
	ResponseMappingTemplate string         `json:"responseMappingTemplate,omitempty"`
	DataSourceName          string         `json:"dataSourceName,omitempty"`
	ResolverARN             string         `json:"resolverArn"`
	TypeName                string         `json:"typeName"`
	FieldName               string         `json:"fieldName"`
	APIID                   string         `json:"apiId"`
	Kind                    string         `json:"kind,omitempty"`
	PipelineConfig          []string       `json:"pipelineConfig,omitempty"` // function IDs for PIPELINE resolvers
	MaxBatchSize            int32          `json:"maxBatchSize,omitempty"`
}

// OpenIDConnectConfig holds the OpenID Connect configuration for an API.
type OpenIDConnectConfig struct {
	Issuer   string `json:"issuer"`
	ClientID string `json:"clientId,omitempty"`
	IatTTL   int64  `json:"iatTTL,omitempty"`
	AuthTTL  int64  `json:"authTTL,omitempty"`
}

// CognitoUserPoolConfig holds the Amazon Cognito user pool configuration for an API.
type CognitoUserPoolConfig struct {
	UserPoolID  string `json:"userPoolId"`
	AWSRegion   string `json:"awsRegion"`
	AppIDClient string `json:"appIdClientRegex,omitempty"`
}

// LambdaAuthorizerConfig holds the Lambda authorizer configuration for an API.
type LambdaAuthorizerConfig struct {
	AuthorizerURI                string `json:"authorizerUri"`
	IdentityValidationExpression string `json:"identityValidationExpression,omitempty"`
	AuthorizerResultTTLInSeconds int32  `json:"authorizerResultTtlInSeconds,omitempty"`
}

// AdditionalAuthenticationProvider holds an additional authentication configuration.
type AdditionalAuthenticationProvider struct {
	LambdaAuthorizerConfig *LambdaAuthorizerConfig `json:"lambdaAuthorizerConfig,omitempty"`
	OpenIDConnectConfig    *OpenIDConnectConfig    `json:"openIDConnectConfig,omitempty"`
	UserPoolConfig         *CognitoUserPoolConfig  `json:"userPoolConfig,omitempty"`
	AuthenticationType     AuthenticationType      `json:"authenticationType"`
}

// GraphqlAPI represents an AppSync GraphQL API.
type GraphqlAPI struct {
	URIs                              map[string]string                  `json:"uris"`
	Tags                              *tags.Tags                         `json:"tags,omitempty"`
	EnvironmentVariables              map[string]string                  `json:"environmentVariables,omitempty"`
	Name                              string                             `json:"name"`
	APIID                             string                             `json:"apiId"`
	ARN                               string                             `json:"arn"`
	AuthenticationType                AuthenticationType                 `json:"authenticationType"`
	Visibility                        string                             `json:"visibility,omitempty"`
	Region                            string                             `json:"region"`
	APIType                           string                             `json:"apiType,omitempty"`
	AdditionalAuthenticationProviders []AdditionalAuthenticationProvider `json:"additionalAuthenticationProviders,omitempty"` //nolint:lll // AWS field name is long
	CreatedAt                         int64                              `json:"createdAt,omitempty"`
	UpdatedAt                         int64                              `json:"updatedAt,omitempty"`
	XrayEnabled                       bool                               `json:"xrayEnabled,omitempty"`
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
	// parsedSchema is the compiled AST, populated once in StartSchemaCreation
	// and never modified afterwards.  ExecuteGraphQL copies the *Schema pointer
	// under a read-lock, so concurrent reads of this immutable field are safe
	// without additional synchronization.
	parsedSchema *ast.Schema  // not serialized to JSON
	SDL          string       `json:"sdl"`
	Status       SchemaStatus `json:"status"`
	Details      string       `json:"details,omitempty"`
	APIID        string       `json:"apiId"`
}

// APIKey represents an AppSync API key.
type APIKey struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`
	Expires     int64  `json:"expires,omitempty"`
	Deletes     int64  `json:"deletes,omitempty"`
}

// APICache represents an AppSync API cache configuration.
type APICache struct {
	APIID              string `json:"apiId"`
	Type               string `json:"type"`
	Status             string `json:"status"`
	APICachingBehavior string `json:"apiCachingBehavior"`
	TTL                int64  `json:"ttl"`
	TransitEncryption  bool   `json:"transitEncryptionEnabled,omitempty"`
	AtRestEncryption   bool   `json:"atRestEncryptionEnabled,omitempty"`
	HealthMetrics      bool   `json:"healthMetricsConfig,omitempty"`
}

// Function represents an AppSync pipeline function.
type Function struct {
	FunctionARN             string `json:"functionArn"`
	FunctionID              string `json:"functionId"`
	APIID                   string `json:"apiId"`
	Name                    string `json:"name"`
	Description             string `json:"description,omitempty"`
	DataSourceName          string `json:"dataSourceName"`
	FunctionVersion         string `json:"functionVersion,omitempty"`
	RequestMappingTemplate  string `json:"requestMappingTemplate,omitempty"`
	ResponseMappingTemplate string `json:"responseMappingTemplate,omitempty"`
	Code                    string `json:"code,omitempty"`
	MaxBatchSize            int32  `json:"maxBatchSize,omitempty"`
}

// TypeDefinitionFormat represents the format of a GraphQL type definition.
type TypeDefinitionFormat string

const (
	// TypeFormatSDL represents SDL format.
	TypeFormatSDL TypeDefinitionFormat = "SDL"
	// TypeFormatJSON represents JSON format.
	TypeFormatJSON TypeDefinitionFormat = "JSON"
)

// APIType represents an AppSync GraphQL type.
type APIType struct {
	ARN         string               `json:"arn"`
	Name        string               `json:"name"`
	Definition  string               `json:"definition,omitempty"`
	Description string               `json:"description,omitempty"`
	Format      TypeDefinitionFormat `json:"format"`
	APIID       string               `json:"apiId,omitempty"`
}

// DomainName represents an AppSync custom domain name.
type DomainName struct {
	Tags           map[string]string `json:"tags,omitempty"`
	DomainName     string            `json:"domainName"`
	CertificateARN string            `json:"certificateArn"`
	Description    string            `json:"description,omitempty"`
	APIID          string            `json:"apiId,omitempty"`
	AppsyncDomain  string            `json:"appsyncDomainName,omitempty"`
	HostedZoneID   string            `json:"hostedZoneId,omitempty"`
	DomainNameARN  string            `json:"domainNameArn,omitempty"`
}

// APIAssociation represents an association between an API and a domain name.
type APIAssociation struct {
	DomainName        string `json:"domainName"`
	APIID             string `json:"apiId,omitempty"`
	AssociationStatus string `json:"associationStatus"`
	DeploymentDetail  string `json:"deploymentDetail,omitempty"`
}

// API represents an AppSync Event API.
type API struct {
	Tags         map[string]string `json:"tags,omitempty"`
	Name         string            `json:"name"`
	APIID        string            `json:"apiId"`
	ARN          string            `json:"arn"`
	DNSHTTP      string            `json:"dns,omitempty"`
	OwnerContact string            `json:"ownerContact,omitempty"`
}

// ChannelNamespace represents an AppSync channel namespace.
type ChannelNamespace struct {
	Tags                map[string]string `json:"tags,omitempty"`
	APIID               string            `json:"apiId"`
	Name                string            `json:"name"`
	ChannelNamespaceARN string            `json:"channelNamespaceArn,omitempty"`
	CodeHandlers        string            `json:"codeHandlers,omitempty"`
	Created             int64             `json:"created,omitempty"`
	LastModified        int64             `json:"lastModified,omitempty"`
}

// DataSourceIntrospectionResult holds the result of a data source introspection job.
type DataSourceIntrospectionResult struct {
	IntrospectionID string `json:"introspectionId"`
	Status          string `json:"status"`
	Models          []any  `json:"models,omitempty"`
}

// SourceAPIAssociation represents an association between a source API and a merged API.
type SourceAPIAssociation struct {
	AssociationID     string `json:"associationId"`
	AssociationARN    string `json:"associationArn"`
	SourceAPIID       string `json:"sourceApiId"`
	SourceAPIARN      string `json:"sourceApiArn,omitempty"`
	MergedAPIID       string `json:"mergedApiId"`
	MergedAPIARN      string `json:"mergedApiArn,omitempty"`
	Description       string `json:"description,omitempty"`
	AssociationStatus string `json:"associationStatus"`
}
