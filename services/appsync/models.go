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

// IntrospectionConfigEnabled and IntrospectionConfigDisabled are the valid values for IntrospectionConfig.
const (
	IntrospectionConfigEnabled  = "ENABLED"
	IntrospectionConfigDisabled = "DISABLED"
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

// AwsIamConfig holds IAM SigV4 signing configuration for HTTP data source authorization.
type AwsIamConfig struct {
	SigningRegion      string `json:"signingRegion,omitempty"`
	SigningServiceName string `json:"signingServiceName,omitempty"`
}

// AuthorizationConfig is the authorization configuration for HTTP endpoint data sources.
type AuthorizationConfig struct {
	AwsIamConfig      *AwsIamConfig `json:"awsIamConfig,omitempty"`
	AuthorizationType string        `json:"authorizationType"` // AWS_IAM
}

// HTTPDataSourceConfig holds the configuration for an HTTP endpoint data source.
type HTTPDataSourceConfig struct {
	AuthorizationConfig *AuthorizationConfig `json:"authorizationConfig,omitempty"`
	Endpoint            string               `json:"endpoint"`
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
//
// MetricsConfig ("ENABLED"/"DISABLED") is a real, accepted-and-echoed member
// (types.DataSourceLevelMetricsConfig, verified against
// deserializers.go:13625 response-side and serializers.go request-side) that
// was previously unmodeled entirely -- a real client's CreateDataSource/
// UpdateDataSource MetricsConfig value was silently dropped.
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
	MetricsConfig            string                              `json:"metricsConfig,omitempty"`
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

// Runtime specifies the runtime for APPSYNC_JS resolvers and functions.
type Runtime struct {
	Name           string `json:"name"`           // APPSYNC_JS
	RuntimeVersion string `json:"runtimeVersion"` // 1.0.0
}

// Resolver represents an AppSync resolver.
//
// MetricsConfig ("ENABLED"/"DISABLED") is a real, accepted-and-echoed member
// (types.ResolverLevelMetricsConfig, verified against
// deserializers.go:16248 response-side and the paired request serializer)
// that was previously unmodeled entirely -- a real client's CreateResolver/
// UpdateResolver MetricsConfig value was silently dropped.
type Resolver struct {
	CachingConfig           *CachingConfig `json:"cachingConfig,omitempty"`
	SyncConfig              *SyncConfig    `json:"syncConfig,omitempty"`
	Runtime                 *Runtime       `json:"runtime,omitempty"`
	ResolverARN             string         `json:"resolverArn"`
	ResponseMappingTemplate string         `json:"responseMappingTemplate,omitempty"`
	DataSourceName          string         `json:"dataSourceName,omitempty"`
	RequestMappingTemplate  string         `json:"requestMappingTemplate,omitempty"`
	TypeName                string         `json:"typeName"`
	FieldName               string         `json:"fieldName"`
	APIID                   string         `json:"apiId"`
	Code                    string         `json:"code,omitempty"`
	Kind                    string         `json:"kind,omitempty"`
	MetricsConfig           string         `json:"metricsConfig,omitempty"`
	PipelineConfig          []string       `json:"pipelineConfig,omitempty"`
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

// UserPoolConfig holds the primary Amazon Cognito user pool configuration for a GraphQL API.
// Unlike CognitoUserPoolConfig (used in AdditionalAuthProviders), this has DefaultAction.
type UserPoolConfig struct {
	UserPoolID       string `json:"userPoolId"`
	AWSRegion        string `json:"awsRegion"`
	DefaultAction    string `json:"defaultAction"` // ALLOW or DENY
	AppIDClientRegex string `json:"appIdClientRegex,omitempty"`
}

// LogConfig holds the CloudWatch Logs configuration for a GraphQL API.
type LogConfig struct {
	CloudWatchLogsRoleARN string `json:"cloudWatchLogsRoleArn"`
	FieldLogLevel         string `json:"fieldLogLevel"` // NONE, ERROR, ALL
	ExcludeVerboseContent bool   `json:"excludeVerboseContent,omitempty"`
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
//
// EnvironmentVariables is deliberately excluded from the wire (json:"-"): the
// real GraphqlApi type has no such member (verified against the real
// deserializer, appsync@v1.56.4 deserializers.go:14999-15185, which has no
// "environmentVariables" case) -- env vars are exposed only via the dedicated
// GetGraphqlApiEnvironmentVariables/PutGraphqlApiEnvironmentVariables ops.
// Before this fix, GetGraphqlApi/ListGraphqlApis/CreateGraphqlApi/
// UpdateGraphqlApi all leaked a caller's real environment-variable values
// into a response AWS never puts them in, once PutGraphqlApiEnvironmentVariables
// had been called. Region/CreatedAt/UpdatedAt are also fabricated (not on the
// real type either) but harmless (no customer data) and left on the wire,
// disclosed rather than fixed -- see PARITY.md.
type GraphqlAPI struct {
	URIs                              map[string]string                  `json:"uris"`
	Tags                              *tags.Tags                         `json:"tags,omitempty"`
	EnvironmentVariables              map[string]string                  `json:"-"`
	UserPoolConfig                    *UserPoolConfig                    `json:"userPoolConfig,omitempty"`
	OpenIDConnectConfig               *OpenIDConnectConfig               `json:"openIDConnectConfig,omitempty"`
	LambdaAuthorizerConfig            *LambdaAuthorizerConfig            `json:"lambdaAuthorizerConfig,omitempty"`
	LogConfig                         *LogConfig                         `json:"logConfig,omitempty"`
	AuthenticationType                AuthenticationType                 `json:"authenticationType"`
	IntrospectionConfig               string                             `json:"introspectionConfig,omitempty"`
	ARN                               string                             `json:"arn"`
	Name                              string                             `json:"name"`
	Visibility                        string                             `json:"visibility,omitempty"`
	Region                            string                             `json:"region"`
	APIType                           string                             `json:"apiType,omitempty"`
	APIID                             string                             `json:"apiId"`
	Owner                             string                             `json:"owner,omitempty"`
	AdditionalAuthenticationProviders []AdditionalAuthenticationProvider `json:"additionalAuthenticationProviders,omitempty"` //nolint:lll // AWS field name is long
	CreatedAt                         int64                              `json:"createdAt,omitempty"`
	UpdatedAt                         int64                              `json:"updatedAt,omitempty"`
	QueryDepthLimit                   int32                              `json:"queryDepthLimit,omitempty"`
	ResolverCountLimit                int32                              `json:"resolverCountLimit,omitempty"`
	XrayEnabled                       bool                               `json:"xrayEnabled,omitempty"`
}

// GraphqlAPIConfig bundles optional auth/logging config for CreateGraphqlAPI and UpdateGraphqlAPI.
// Passing nil is equivalent to no config — existing behaviour is preserved.
type GraphqlAPIConfig struct {
	UserPoolConfig         *UserPoolConfig
	OpenIDConnectConfig    *OpenIDConnectConfig
	LambdaAuthorizerConfig *LambdaAuthorizerConfig
	LogConfig              *LogConfig
	IntrospectionConfig    string
	QueryDepthLimit        int32
	ResolverCountLimit     int32
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
	Runtime                 *Runtime    `json:"runtime,omitempty"`
	SyncConfig              *SyncConfig `json:"syncConfig,omitempty"`
	FunctionARN             string      `json:"functionArn"`
	FunctionID              string      `json:"functionId"`
	APIID                   string      `json:"apiId"`
	Name                    string      `json:"name"`
	Description             string      `json:"description,omitempty"`
	DataSourceName          string      `json:"dataSourceName"`
	FunctionVersion         string      `json:"functionVersion,omitempty"`
	RequestMappingTemplate  string      `json:"requestMappingTemplate,omitempty"`
	ResponseMappingTemplate string      `json:"responseMappingTemplate,omitempty"`
	Code                    string      `json:"code,omitempty"`
	MaxBatchSize            int32       `json:"maxBatchSize,omitempty"`
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

// AuthMode specifies an authorization mode for an Event API.
type AuthMode struct {
	AuthType string `json:"authType"` // AuthenticationType value (API_KEY, AWS_IAM, etc.)
}

// CognitoConfig is the Cognito user pool configuration for Event API auth providers.
type CognitoConfig struct {
	UserPoolID       string `json:"userPoolId"`
	AWSRegion        string `json:"awsRegion"`
	AppIDClientRegex string `json:"appIdClientRegex,omitempty"`
}

// AuthProvider holds an authorization provider for an Event API.
type AuthProvider struct {
	CognitoConfig          *CognitoConfig          `json:"cognitoConfig,omitempty"`
	OpenIDConnectConfig    *OpenIDConnectConfig    `json:"openIDConnectConfig,omitempty"`
	LambdaAuthorizerConfig *LambdaAuthorizerConfig `json:"lambdaAuthorizerConfig,omitempty"`
	AuthType               string                  `json:"authType"`
}

// EventLogConfig holds the CloudWatch Logs configuration for an Event API.
// Distinct from GraphqlAPI's LogConfig -- real appsync.types.EventLogConfig
// has only these two members (verified: appsync@v1.56.4 deserializers.go's
// awsRestjson1_deserializeDocumentEventLogConfig case list), no
// excludeVerboseContent field like the GraphqlApi LogConfig has.
type EventLogConfig struct {
	CloudWatchLogsRoleARN string `json:"cloudWatchLogsRoleArn"`
	LogLevel              string `json:"logLevel"`
}

// EventConfig holds the authorization configuration for an Event API.
//
// LogConfig was previously unmodeled entirely: real CreateApiInput/
// UpdateApiInput both accept it nested under eventConfig (serializers.go's
// awsRestjson1_serializeDocumentEventConfig has a "logConfig" case), and the
// real Api response type echoes it back, but gopherstack's EventConfig
// struct had no field for it at all -- json.Unmarshal silently dropped a
// real client's CreateApi/UpdateApi EventConfig.LogConfig every time.
type EventConfig struct {
	LogConfig                 *EventLogConfig `json:"logConfig,omitempty"`
	AuthProviders             []AuthProvider  `json:"authProviders"`
	ConnectionAuthModes       []AuthMode      `json:"connectionAuthModes"`
	DefaultPublishAuthModes   []AuthMode      `json:"defaultPublishAuthModes"`
	DefaultSubscribeAuthModes []AuthMode      `json:"defaultSubscribeAuthModes"`
}

// API represents an AppSync Event API.
type API struct {
	Tags        map[string]string `json:"tags,omitempty"`
	DNS         map[string]string `json:"dns,omitempty"`
	EventConfig *EventConfig      `json:"eventConfig,omitempty"`
	Name        string            `json:"name"`
	APIID       string            `json:"apiId"`
	// ARN's wire key is "apiArn", not "arn" -- verified against the real
	// deserializer (appsync@v1.56.4 deserializers.go:12050), which is the only
	// field name real clients recognize to discover an Event API's ARN.
	ARN          string `json:"apiArn"`
	OwnerContact string `json:"ownerContact,omitempty"`
}

// Integration is the data source integration for an event handler.
type Integration struct {
	LambdaConfig   *LambdaDataSourceConfig `json:"lambdaConfig,omitempty"`
	DataSourceName string                  `json:"dataSourceName"`
}

// HandlerConfig is the configuration for a single event handler (OnPublish or OnSubscribe).
type HandlerConfig struct {
	Integration *Integration `json:"integration,omitempty"`
	Behavior    string       `json:"behavior"` // CODE
}

// HandlerConfigs holds the OnPublish and OnSubscribe handler configurations.
type HandlerConfigs struct {
	OnPublish   *HandlerConfig `json:"onPublish,omitempty"`
	OnSubscribe *HandlerConfig `json:"onSubscribe,omitempty"`
}

// ChannelNamespaceConfig bundles optional auth/handler config for channel namespace operations.
// Passing nil preserves existing behaviour.
type ChannelNamespaceConfig struct {
	HandlerConfigs     *HandlerConfigs `json:"handlerConfigs,omitempty"`
	CodeHandlers       string          `json:"codeHandlers,omitempty"`
	PublishAuthModes   []AuthMode      `json:"publishAuthModes,omitempty"`
	SubscribeAuthModes []AuthMode      `json:"subscribeAuthModes,omitempty"`
}

// ChannelNamespace represents an AppSync channel namespace.
type ChannelNamespace struct {
	Tags                map[string]string `json:"tags,omitempty"`
	HandlerConfigs      *HandlerConfigs   `json:"handlerConfigs,omitempty"`
	APIID               string            `json:"apiId"`
	Name                string            `json:"name"`
	ChannelNamespaceARN string            `json:"channelNamespaceArn,omitempty"`
	CodeHandlers        string            `json:"codeHandlers,omitempty"`
	PublishAuthModes    []AuthMode        `json:"publishAuthModes,omitempty"`
	SubscribeAuthModes  []AuthMode        `json:"subscribeAuthModes,omitempty"`
	Created             int64             `json:"created,omitempty"`
	LastModified        int64             `json:"lastModified,omitempty"`
}

// DataSourceIntrospectionStatus values for a DataSourceIntrospection job, matching
// aws-sdk-go-v2/service/appsync/types.DataSourceIntrospectionStatus exactly.
const (
	DataSourceIntrospectionStatusProcessing = "PROCESSING"
	DataSourceIntrospectionStatusFailed     = "FAILED"
	DataSourceIntrospectionStatusSuccess    = "SUCCESS"
)

// RDSDataAPIConfig carries the metadata needed to introspect an RDS cluster via
// the RDS Data API. Field names match aws-sdk-go-v2/service/appsync/types.RdsDataApiConfig.
type RDSDataAPIConfig struct {
	DatabaseName string `json:"databaseName"`
	ResourceARN  string `json:"resourceArn"`
	SecretARN    string `json:"secretArn"`
}

// DataSourceIntrospectionModelFieldType represents the type data for one introspected
// field. Matches aws-sdk-go-v2/service/appsync/types.DataSourceIntrospectionModelFieldType.
type DataSourceIntrospectionModelFieldType struct {
	Type   *DataSourceIntrospectionModelFieldType `json:"type,omitempty"`
	Kind   string                                 `json:"kind,omitempty"`
	Name   string                                 `json:"name,omitempty"`
	Values []string                               `json:"values,omitempty"`
}

// DataSourceIntrospectionModelField represents one field retrieved from introspected
// data. Matches aws-sdk-go-v2/service/appsync/types.DataSourceIntrospectionModelField.
type DataSourceIntrospectionModelField struct {
	Type   *DataSourceIntrospectionModelFieldType `json:"type,omitempty"`
	Name   string                                 `json:"name,omitempty"`
	Length int64                                  `json:"length,omitempty"`
}

// DataSourceIntrospectionModelIndex is an index retrieved from introspected data.
// Matches aws-sdk-go-v2/service/appsync/types.DataSourceIntrospectionModelIndex.
type DataSourceIntrospectionModelIndex struct {
	Name   string   `json:"name,omitempty"`
	Fields []string `json:"fields,omitempty"`
}

// DataSourceIntrospectionModel is the introspected data for a single model (e.g. a
// database table). Matches aws-sdk-go-v2/service/appsync/types.DataSourceIntrospectionModel.
type DataSourceIntrospectionModel struct {
	PrimaryKey *DataSourceIntrospectionModelIndex   `json:"primaryKey,omitempty"`
	Name       string                               `json:"name,omitempty"`
	SDL        string                               `json:"sdl,omitempty"`
	Fields     []*DataSourceIntrospectionModelField `json:"fields,omitempty"`
	Indexes    []*DataSourceIntrospectionModelIndex `json:"indexes,omitempty"`
}

// DataSourceIntrospectionResult is the populated result of a completed introspection.
// Matches aws-sdk-go-v2/service/appsync/types.DataSourceIntrospectionResult.
type DataSourceIntrospectionResult struct {
	NextToken string                          `json:"nextToken,omitempty"`
	Models    []*DataSourceIntrospectionModel `json:"models,omitempty"`
}

// DataSourceIntrospection is the persisted record of a StartDataSourceIntrospection
// job, keyed by IntrospectionID. Unlike the rest of this service's resources, a real
// introspection job is NOT coupled to any AppSync GraphqlApi/DataSource -- it
// introspects an RDS Data API-backed database directly from the caller-supplied
// RDSDataAPIConfig (see StartDataSourceIntrospectionInput in the real SDK, which
// carries only an optional rdsDataApiConfig, no apiId/dataSourceName). The
// RDSDataAPIConfig field is internal record-keeping only -- it is never part of
// either operation's wire response.
type DataSourceIntrospection struct {
	RDSDataAPIConfig          *RDSDataAPIConfig              `json:"rdsDataApiConfig,omitempty"`
	IntrospectionResult       *DataSourceIntrospectionResult `json:"introspectionResult,omitempty"`
	IntrospectionID           string                         `json:"introspectionId"`
	IntrospectionStatus       string                         `json:"introspectionStatus"`
	IntrospectionStatusDetail string                         `json:"introspectionStatusDetail,omitempty"`
}

// SourceAPIAssociationConfig describes how source API merging is performed.
type SourceAPIAssociationConfig struct {
	MergeType string `json:"mergeType,omitempty"` // MANUAL_MERGE or AUTO_MERGE
}

// SourceAPIAssociationStatus values, matching
// aws-sdk-go-v2/service/appsync/types.SourceApiAssociationStatus exactly.
const (
	SourceAPIAssociationStatusMergeScheduled          = "MERGE_SCHEDULED"
	SourceAPIAssociationStatusMergeFailed             = "MERGE_FAILED"
	SourceAPIAssociationStatusMergeSuccess            = "MERGE_SUCCESS"
	SourceAPIAssociationStatusMergeInProgress         = "MERGE_IN_PROGRESS"
	SourceAPIAssociationStatusAutoMergeScheduleFailed = "AUTO_MERGE_SCHEDULE_FAILED"
	SourceAPIAssociationStatusDeletionScheduled       = "DELETION_SCHEDULED"
	SourceAPIAssociationStatusDeletionInProgress      = "DELETION_IN_PROGRESS"
	SourceAPIAssociationStatusDeletionFailed          = "DELETION_FAILED"
)

// SourceAPIAssociation represents an association between a source API and a merged API.
//
// AssociationStatus's wire key is "sourceApiAssociationStatus", NOT
// "associationStatus" -- verified against the real deserializer
// (appsync@v1.56.4 deserializers.go:16488). This is a sibling trap: the
// similarly-named ApiAssociation type (domain-name associations) genuinely
// does use the plain "associationStatus" key (deserializers.go:12175); a
// real client's SourceApiAssociation.SourceApiAssociationStatus field was
// always empty regardless of backend state before this fix.
type SourceAPIAssociation struct {
	SourceAPIAssociationConfig *SourceAPIAssociationConfig `json:"sourceApiAssociationConfig,omitempty"`
	AssociationID              string                      `json:"associationId"`
	AssociationARN             string                      `json:"associationArn"`
	SourceAPIID                string                      `json:"sourceApiId"`
	SourceAPIARN               string                      `json:"sourceApiArn,omitempty"`
	MergedAPIID                string                      `json:"mergedApiId"`
	MergedAPIARN               string                      `json:"mergedApiArn,omitempty"`
	Description                string                      `json:"description,omitempty"`
	AssociationStatus          string                      `json:"sourceApiAssociationStatus"`
	AssociationStatusDetail    string                      `json:"sourceApiAssociationStatusDetail,omitempty"`
}

// SourceAPIAssociationSummary is the narrow wire shape for ListSourceApiAssociations items.
// Matches aws-sdk-go-v2/service/appsync/types.SourceApiAssociationSummary (drops
// sourceApiAssociationConfig, sourceApiAssociationStatus, and sourceApiAssociationStatusDetail).
type SourceAPIAssociationSummary struct {
	AssociationID  string `json:"associationId"`
	AssociationARN string `json:"associationArn"`
	SourceAPIID    string `json:"sourceApiId"`
	SourceAPIARN   string `json:"sourceApiArn,omitempty"`
	MergedAPIID    string `json:"mergedApiId"`
	MergedAPIARN   string `json:"mergedApiArn,omitempty"`
	Description    string `json:"description,omitempty"`
}

func toSourceAPIAssociationSummary(a *SourceAPIAssociation) *SourceAPIAssociationSummary {
	if a == nil {
		return nil
	}

	return &SourceAPIAssociationSummary{
		AssociationID:  a.AssociationID,
		AssociationARN: a.AssociationARN,
		SourceAPIID:    a.SourceAPIID,
		SourceAPIARN:   a.SourceAPIARN,
		MergedAPIID:    a.MergedAPIID,
		MergedAPIARN:   a.MergedAPIARN,
		Description:    a.Description,
	}
}
