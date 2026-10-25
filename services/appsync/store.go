package appsync

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/binary"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// LambdaInvoker can invoke a Lambda function by name or ARN.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name, invocationType string, payload []byte) ([]byte, int, error)
}

// JWKSProvider resolves RSA public keys for JWT signature verification.
// Implementations return an error when the issuer or key is unknown. Same
// shape as services/apigateway and services/apigatewayv2's JWKSProvider
// (services/cognitoidp's InMemoryBackend.GetJWTPublicKey implements it for
// all three), reused rather than duplicated -- see SetJWKSProvider.
type JWKSProvider interface {
	GetJWTPublicKey(issuerURL, kid string) (*rsa.PublicKey, error)
}

// DDBExpression is a DynamoDB expression with its placeholder substitutions;
// Values are wire-form attribute values ({"S":"x"}).
type DDBExpression struct {
	Names      map[string]string
	Values     map[string]any
	Expression string
}

// DDBQuery is a Query or Scan request; Values are wire-form attribute values.
type DDBQuery struct {
	Names        map[string]string
	Values       map[string]any
	StartKey     map[string]any
	Table        string
	Index        string
	KeyCondition string
	Filter       string
	Limit        int32
	Descending   bool
	Consistent   bool
}

// DDBPage is one Query or Scan page; items and LastKey are in wire form.
type DDBPage struct {
	LastKey      map[string]any
	Items        []map[string]any
	ScannedCount int
}

// DDBWrite is one BatchWriteItem entry: a put (Item) or a delete (Key).
type DDBWrite struct {
	Item map[string]any
	Key  map[string]any
}

// DynamoDBError is a DynamoDB service error surfaced to the resolver; Code is
// the bare error code (e.g. "ConditionalCheckFailedException").
type DynamoDBError struct {
	Result        any
	Code          string
	Message       string
	Cancellations []DDBCancellation
}

// DDBCancellation is one TransactionCanceledException cancellation reason.
type DDBCancellation struct {
	Item    map[string]any
	Type    string
	Message string
}

// DDBTransactGet is one TransactGetItems request item.
type DDBTransactGet struct {
	Key   map[string]any
	Table string
}

// DDBTransactWrite is one TransactWriteItems request item; Operation is
// PutItem, UpdateItem, DeleteItem or ConditionCheck.
type DDBTransactWrite struct {
	Key       map[string]any
	Item      map[string]any
	Update    *DDBExpression
	Condition *DDBExpression
	Table     string
	Operation string
	// ReturnOnFailure asks for the existing item in the cancellation reason.
	ReturnOnFailure bool
}

func (e *DynamoDBError) Error() string {
	return fmt.Sprintf("%s (Service: AmazonDynamoDBv2; Status Code: 400; Error Code: %s)", e.Message, e.Code)
}

// DynamoDBBackend is the DynamoDB interface needed for DynamoDB resolvers.
// Items and keys are wire-form ({"S":"x"}) maps.
type DynamoDBBackend interface {
	GetItemRaw(ctx context.Context, tableName string, key map[string]any) (map[string]any, error)
	PutItemRaw(ctx context.Context, tableName string, item map[string]any, cond *DDBExpression) error
	// UpdateItemRaw returns the item as it is after the update.
	UpdateItemRaw(
		ctx context.Context, tableName string, key map[string]any, update DDBExpression, cond *DDBExpression,
	) (map[string]any, error)
	// DeleteItemRaw returns the deleted item, or an empty map when none existed.
	DeleteItemRaw(
		ctx context.Context, tableName string, key map[string]any, cond *DDBExpression,
	) (map[string]any, error)
	QueryRaw(ctx context.Context, q *DDBQuery) (*DDBPage, error)
	ScanRaw(ctx context.Context, q *DDBQuery) (*DDBPage, error)
	// BatchGetItemRaw returns the items found per table, in no particular order.
	BatchGetItemRaw(ctx context.Context, keys map[string][]map[string]any) (map[string][]map[string]any, error)
	BatchWriteItemRaw(ctx context.Context, writes map[string][]DDBWrite) error
	// TransactGetItemsRaw returns one item (nil when absent) per request, in order.
	TransactGetItemsRaw(ctx context.Context, items []DDBTransactGet) ([]map[string]any, error)
	// TransactWriteItemsRaw applies all items atomically. A cancelled
	// transaction returns a *DynamoDBError whose Cancellations are set.
	TransactWriteItemsRaw(ctx context.Context, items []DDBTransactWrite) error
}

// StorageBackend defines the interface for AppSync storage operations.
type StorageBackend interface {
	CreateGraphqlAPI(
		name string,
		authType AuthenticationType,
		xrayEnabled bool,
		apiType string,
		visibility string,
		additionalAuthProviders []AdditionalAuthenticationProvider,
		tagMap map[string]string,
		cfg *GraphqlAPIConfig,
	) (*GraphqlAPI, error)
	GetGraphqlAPI(apiID string) (*GraphqlAPI, error)
	UpdateGraphqlAPI(
		apiID, name string,
		authType AuthenticationType,
		xrayEnabled *bool,
		visibility string,
		additionalAuthProviders []AdditionalAuthenticationProvider,
		cfg *GraphqlAPIConfig,
	) (*GraphqlAPI, error)
	ListGraphqlAPIs(apiType string) ([]*GraphqlAPI, error)
	DeleteGraphqlAPI(apiID string) error
	StartSchemaCreation(apiID, sdl string) (*Schema, error)
	GetSchemaCreationStatus(apiID string) (*Schema, error)
	GetIntrospectionSchema(apiID, format string, includeDirectives bool) ([]byte, error)
	CreateDataSource(apiID string, ds *DataSource) (*DataSource, error)
	GetDataSource(apiID, name string) (*DataSource, error)
	ListDataSources(apiID string) ([]*DataSource, error)
	DeleteDataSource(apiID, name string) error
	CreateResolver(apiID, typeName string, r *Resolver) (*Resolver, error)
	GetResolver(apiID, typeName, fieldName string) (*Resolver, error)
	ListResolvers(apiID, typeName string) ([]*Resolver, error)
	DeleteResolver(apiID, typeName, fieldName string) error
	ExecuteGraphQL(
		ctx context.Context,
		apiID, query, operationName string,
		variables map[string]any,
		auth GraphQLAuth,
	) (map[string]any, error)
	// New Event API operations.
	CreateAPI(name, ownerContact string, tagMap map[string]string, eventConfig *EventConfig) (*API, error)
	CreateChannelNamespace(
		apiID, name string, tagMap map[string]string, cfg *ChannelNamespaceConfig,
	) (*ChannelNamespace, error)
	// API key operations.
	CreateAPIKey(apiID, description string, expires int64) (*APIKey, error)
	ListAPIKeys(apiID string) ([]*APIKey, error)
	DeleteAPIKey(apiID, keyID string) error
	// API cache operations.
	CreateAPICache(apiID string, cache *APICache) (*APICache, error)
	GetAPICache(apiID string) (*APICache, error)
	DeleteAPICache(apiID string) error
	// Function operations.
	CreateFunction(apiID string, f *Function) (*Function, error)
	GetFunction(apiID, functionID string) (*Function, error)
	ListFunctions(apiID string) ([]*Function, error)
	DeleteFunction(apiID, functionID string) error
	// Type operations.
	CreateType(apiID, definition string, format TypeDefinitionFormat) (*APIType, error)
	GetType(apiID, typeName string) (*APIType, error)
	ListTypes(apiID string) ([]*APIType, error)
	DeleteType(apiID, typeName string) error
	// DataSource update.
	UpdateDataSource(apiID, name string, ds *DataSource) (*DataSource, error)
	// Resolver update.
	UpdateResolver(apiID, typeName string, r *Resolver) (*Resolver, error)
	// Function update.
	UpdateFunction(apiID, functionID string, f *Function) (*Function, error)
	// Type update.
	UpdateType(apiID, typeName, definition string, format TypeDefinitionFormat) (*APIType, error)
	// API key update.
	UpdateAPIKey(apiID, keyID, description string, expires int64) (*APIKey, error)
	// API cache update and flush.
	UpdateAPICache(apiID string, cache *APICache) (*APICache, error)
	FlushAPICache(apiID string) error
	// Tag operations (GraphQL APIs).
	TagResource(apiID string, tagMap map[string]string) error
	UntagResource(apiID string, tagKeys []string) error
	ListTagsForResource(apiID string) (map[string]string, error)
	// Domain name operations.
	CreateDomainName(domainName, certificateARN, description string, tagMap map[string]string) (*DomainName, error)
	GetDomainName(domainName string) (*DomainName, error)
	UpdateDomainName(domainName, description, certificateARN string) (*DomainName, error)
	ListDomainNames() ([]*DomainName, error)
	DeleteDomainName(domainName string) error
	AssociateAPI(domainName, apiID string) (*APIAssociation, error)
	GetAPIAssociation(domainName string) (*APIAssociation, error)
	DisassociateAPI(domainName string) error
	// Event API (v2) operations.
	GetAPI(apiID string) (*API, error)
	ListAPIs() ([]*API, error)
	UpdateAPI(apiID, name, ownerContact string, eventConfig *EventConfig) (*API, error)
	DeleteAPI(apiID string) error
	// Channel namespace operations.
	GetChannelNamespace(apiID, name string) (*ChannelNamespace, error)
	ListChannelNamespaces(apiID string) ([]*ChannelNamespace, error)
	UpdateChannelNamespace(apiID, name string, cfg *ChannelNamespaceConfig) (*ChannelNamespace, error)
	DeleteChannelNamespace(apiID, name string) error
	// Merged/source API association operations.
	AssociateMergedGraphqlAPI(
		sourceAPIIdentifier, mergedAPIIdentifier, description, mergeType string,
	) (*SourceAPIAssociation, error)
	AssociateSourceGraphqlAPI(
		mergedAPIIdentifier, sourceAPIIdentifier, description, mergeType string,
	) (*SourceAPIAssociation, error)
	GetSourceAPIAssociation(mergedAPIID, associationID string) (*SourceAPIAssociation, error)
	ListSourceAPIAssociations(mergedAPIID string) ([]*SourceAPIAssociation, error)
	DisassociateMergedGraphqlAPI(sourceAPIID, associationID string) error
	DisassociateSourceGraphqlAPI(mergedAPIID, associationID string) error
	// ListResolversByFunction - resolvers attached to a function.
	ListResolversByFunction(apiID, functionID string) ([]*Resolver, error)
	// Environment variable operations on GraphQL APIs.
	GetGraphqlAPIEnvironmentVariables(apiID string) (map[string]string, error)
	PutGraphqlAPIEnvironmentVariables(apiID string, envVars map[string]string) (map[string]string, error)
	// EvaluateMappingTemplate evaluates a VTL request/response mapping template.
	EvaluateMappingTemplate(template, context string) (string, error)
	EvaluateMappingTemplateLogs(template, context string) (string, []string, error)
	// EvaluateCode evaluates APPSYNC_JS code.
	EvaluateCode(code, contextJSON, function, runtime string) (string, error)
	EvaluateCodeLogs(code, contextJSON, function, runtime string) (string, []string, error)
	// StartDataSourceIntrospection starts an RDS Data API introspection job. Not
	// scoped to any existing AppSync API/DataSource -- see DataSourceIntrospection's
	// doc comment in models.go.
	StartDataSourceIntrospection(cfg *RDSDataAPIConfig) (*DataSourceIntrospection, error)
	// GetDataSourceIntrospection returns the persisted record of an introspection job.
	GetDataSourceIntrospection(introspectionID string) (*DataSourceIntrospection, error)
	// StartSchemaMerge merges one source API association's schema into its merged API.
	StartSchemaMerge(mergedAPIID, associationID string) (string, error)
	// UpdateSourceAPIAssociation updates a source API association on a merged API.
	UpdateSourceAPIAssociation(mergedAPIID, associationID, description string) (*SourceAPIAssociation, error)
	// UpdateSourceAPIAssociationWithConfig also applies sourceApiAssociationConfig.mergeType when set.
	UpdateSourceAPIAssociationWithConfig(
		mergedAPIID, associationID, description, mergeType string,
	) (*SourceAPIAssociation, error)
	// ListTypesByAssociation lists types for a given merged API source association.
	ListTypesByAssociation(mergedAPIID, associationID, format string) ([]*APIType, error)
}

// apiIDChars is the character set used to generate AppSync API IDs.
// Real AWS AppSync API IDs are lowercase alphanumeric strings without hyphens.
const apiIDChars = "abcdefghijklmnopqrstuvwxyz0123456789"

func randomAPIID() string {
	const length = 26

	b := make([]byte, length)
	charCount := uint64(len(apiIDChars))

	for i := range b {
		var v [8]byte
		_, _ = rand.Read(v[:])
		b[i] = apiIDChars[binary.BigEndian.Uint64(v[:])%charCount]
	}

	return string(b)
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
//
// Resource collections are backed by *store.Table[T] (see store_setup.go for
// the registration list and pkgs/store's package doc). Collections nested
// under a GraphQL/Event API in the original hand-rolled maps (datasources,
// resolvers, functions, types, channelNamespaces) are now single flat tables
// keyed by a composite "<apiID>#<localKey>" string, with a secondary
// [store.Index] grouping by API ID for the "all children of API X" lookups
// the nested maps used to answer directly -- see store_setup.go's doc comment
// for why this is safe (every child value type already carries its own APIID
// field as a real, wire-serialized identity field, unlike some other
// services' internal-only parent-ID fields).
type InMemoryBackend struct {
	registry               *store.Registry
	apis                   *store.Table[GraphqlAPI]
	schemas                *store.Table[Schema]
	datasources            *store.Table[DataSource] // key: apiID#name
	datasourcesByAPI       *store.Index[DataSource] // apiID → datasources
	resolvers              *store.Table[Resolver]   // key: apiID#TypeName.FieldName
	resolversByAPI         *store.Index[Resolver]   // apiID → resolvers
	rtSubs                 map[string]map[*rtSub]struct{}
	respCache              map[string]map[string]cacheEntry // apiID → cache key → response; runtime only
	apiKeys                map[string]map[string]*APIKey    // apiID → keyID → key (raw; see store_setup.go)
	apiCaches              *store.Table[APICache]
	functions              *store.Table[Function] // key: apiID#functionID
	functionsByAPI         *store.Index[Function] // apiID → functions
	types                  *store.Table[APIType]  // key: apiID#typeName
	typesByAPI             *store.Index[APIType]  // apiID → types
	domainNames            *store.Table[DomainName]
	apiAssociations        *store.Table[APIAssociation]
	eventAPIs              *store.Table[API]
	channelNamespaces      *store.Table[ChannelNamespace] // key: apiID#name
	channelNamespacesByAPI *store.Index[ChannelNamespace] // apiID → channel namespaces
	sourceAssocs           *store.Table[SourceAPIAssociation]
	introspections         *store.Table[DataSourceIntrospection]
	lambdaFn               LambdaInvoker
	webACLs                WebACLResolver
	ddbBackend             DynamoDBBackend
	ebPutter               EventBridgePutter
	osRequester            OpenSearchRequester
	rdsExec                RDSDataExecutor
	jwksProvider           JWKSProvider
	mu                     *lockmetrics.RWMutex
	accountID              string
	region                 string
	endpoint               string
	graphqlStrategy        string
	// sigv4Secret is the AWS_IAM verification secret; empty uses the validator default.
	sigv4Secret string
}

// NewInMemoryBackend creates a new in-memory AppSync backend.
func NewInMemoryBackend(accountID, region, endpoint string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		apiKeys:   make(map[string]map[string]*APIKey),
		mu:        lockmetrics.New("appsync"),
		accountID: accountID,
		region:    region,
		endpoint:  endpoint,
	}

	registerAllTables(b)

	return b
}

// SetGraphQLEndpointStrategy selects the GraphQL URI format (see pkgs/endpointstrategy).
func (b *InMemoryBackend) SetGraphQLEndpointStrategy(strategy string) {
	b.mu.Lock("SetGraphQLEndpointStrategy")
	defer b.mu.Unlock()

	b.graphqlStrategy = strategy
}

// Reset clears all state from the backend, returning it to a clean initial state.
// Useful for resetting state between tests.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Close tag resources before discarding.
	for _, api := range b.apis.All() {
		if api.Tags != nil {
			api.Tags.Close()
		}
	}

	for _, ds := range b.datasources.All() {
		if ds != nil && ds.Tags != nil {
			ds.Tags.Close()
		}
	}

	b.registry.ResetAll()
	b.apiKeys = make(map[string]map[string]*APIKey)
}

// SetLambdaInvoker configures the Lambda invoker for LAMBDA data sources.
func (b *InMemoryBackend) SetLambdaInvoker(fn LambdaInvoker) {
	b.lambdaFn = fn
}

// SetSigV4Secret sets the secret AWS_IAM GraphQL auth verifies request signatures against.
func (b *InMemoryBackend) SetSigV4Secret(secret string) {
	b.sigv4Secret = secret
}

// SetDynamoDBBackend configures the DynamoDB backend for DYNAMODB data sources.
func (b *InMemoryBackend) SetDynamoDBBackend(ddb DynamoDBBackend) {
	b.ddbBackend = ddb
}

// SetJWKSProvider configures the JWKS provider AMAZON_COGNITO_USER_POOLS and
// OPENID_CONNECT GraphQL auth use to verify JWT signatures. Wired from cli.go
// (wireAppSyncCognito) to services/cognitoidp's InMemoryBackend, same as
// services/apigateway and services/apigatewayv2.
//
// Unlike checkLambdaAuth/checkIAMAuth, an unset provider does NOT reject --
// checkCognitoAuth/checkOIDCAuth pass the request through instead. The ~150
// other services, and most of this package's own tests, construct an
// InMemoryBackend directly without calling this setter; treating "provider
// never wired" as a hard rejection would make every such construction
// unable to serve a Cognito/OIDC-authenticated GraphQL request at all. A real
// gopherstack server always wires this (see wireAppSyncCognito in cli.go), so
// production traffic gets full signature verification; the permissive path
// only matters for callers that build the backend standalone.
func (b *InMemoryBackend) SetJWKSProvider(p JWKSProvider) {
	b.jwksProvider = p
}
