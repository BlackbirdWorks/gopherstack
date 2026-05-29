package appsync

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	apiTypeGraphQL = "GRAPHQL"
)

var (
	// ErrNotFound is returned when a resource is not found.
	ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("BadRequestException", awserr.ErrAlreadyExists)
	// ErrInvalidSchema is returned when the provided schema SDL is invalid.
	ErrInvalidSchema = errors.New("InvalidSchemaError")
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)

// LambdaInvoker can invoke a Lambda function by name or ARN.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name, invocationType string, payload []byte) ([]byte, int, error)
}

// DynamoDBBackend is the minimal DynamoDB interface needed for DynamoDB resolvers.
type DynamoDBBackend interface {
	// GetItemRaw executes a DynamoDB GetItem and returns raw JSON bytes.
	GetItemRaw(ctx context.Context, tableName string, key map[string]any) (map[string]any, error)
	// PutItemRaw executes a DynamoDB PutItem with the given item.
	PutItemRaw(ctx context.Context, tableName string, item map[string]any) error
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
	GetIntrospectionSchema(apiID, format string) ([]byte, error)
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
	) (map[string]any, error)
	// New Event API operations.
	CreateAPI(name, ownerContact string, tagMap map[string]string, eventConfig *EventConfig) (*API, error)
	CreateChannelNamespace(apiID, name string, tagMap map[string]string, cfg *ChannelNamespaceConfig) (*ChannelNamespace, error)
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
	// EvaluateCode evaluates APPSYNC_JS code.
	EvaluateCode(code, contextJSON, function, runtime string) (string, error)
	// StartDataSourceIntrospection starts an introspection job for a data source.
	StartDataSourceIntrospection(apiID, dataSourceName string) (string, error)
	// GetDataSourceIntrospection returns the result of a data source introspection job.
	GetDataSourceIntrospection(introspectionID string) (*DataSourceIntrospectionResult, error)
	// StartSchemaMerge triggers a schema merge for a merged GraphQL API.
	StartSchemaMerge(apiID string) (SchemaStatus, error)
	// UpdateSourceAPIAssociation updates a source API association on a merged API.
	UpdateSourceAPIAssociation(mergedAPIID, associationID, description string) (*SourceAPIAssociation, error)
	// ListTypesByAssociation lists types for a given merged API source association.
	ListTypesByAssociation(mergedAPIID, associationID, format string) ([]*APIType, error)
}

// apiIDChars is the character set used to generate AppSync API IDs.
// Real AWS AppSync API IDs are lowercase alphanumeric strings without hyphens.
const apiIDChars = "abcdefghijklmnopqrstuvwxyz0123456789"

// apiKeyIDChars is the character set used for API key IDs.
const apiKeyIDChars = "abcdefghijklmnopqrstuvwxyz0123456789"

// apiKeyIDPrefix is the prefix used in AppSync API key IDs.
const apiKeyIDPrefix = "da2-"

// defaultAPIKeyExpiryDays is the default expiry in days when not specified.
const defaultAPIKeyExpiryDays = 365

// maxAPIKeysPerAPI is the AWS-enforced limit of API keys per GraphQL API.
// AWS default quota is 50 API keys per GraphQL API.
const maxAPIKeysPerAPI = 50

// maxEnvironmentVariables is the AWS-enforced limit on environment variables per GraphQL API.
const maxEnvironmentVariables = 25

// defaultFunctionVersion is the default AppSync function runtime version.
const defaultFunctionVersion = "2018-05-29"

// resolverKindUnit is the "UNIT" resolver kind.
const resolverKindUnit = "UNIT"

// resolverKindPipeline is the "PIPELINE" resolver kind.
const resolverKindPipeline = "PIPELINE"

// isValidAPICacheType returns true if the given cache type is a valid AppSync API cache type.
func isValidAPICacheType(t string) bool {
	switch t {
	case "SMALL", "MEDIUM", "LARGE", "XLARGE",
		"LARGE_2X", "LARGE_4X", "LARGE_8X", "LARGE_12X",
		"T2_SMALL", "T2_MEDIUM",
		"R4_1XLARGE", "R4_2XLARGE", "R4_4XLARGE", "R4_8XLARGE":
		return true
	default:
		return false
	}
}

// isValidAPICachingBehavior returns true if the given caching behavior is valid.
func isValidAPICachingBehavior(behavior string) bool {
	switch behavior {
	case "FULL_REQUEST_CACHING", "PER_RESOLVER_CACHING", "FULL_REQUEST_DATA_CACHING":
		return true
	default:
		return false
	}
}

// isValidAuthType returns true if the given authentication type is valid.
func isValidAuthType(a AuthenticationType) bool {
	switch a {
	case AuthTypeAPIKey, AuthTypeIAM, AuthTypeCognito, AuthTypeOIDC, AuthTypeLambda:
		return true
	default:
		return false
	}
}

// isValidDataSourceType returns true if the given data source type is valid.
func isValidDataSourceType(t DataSourceType) bool {
	switch t {
	case DataSourceTypeNone, DataSourceTypeLambda, DataSourceTypeDynamoDB,
		DataSourceTypeHTTP, DataSourceTypeOpenSearch, DataSourceTypeRelational,
		DataSourceTypeEventBridge:
		return true
	default:
		return false
	}
}

// isValidGraphqlAPIType returns true if the given API type is valid.
func isValidGraphqlAPIType(t string) bool {
	return t == apiTypeGraphQL || t == "MERGED"
}

// isValidTypeFormat returns true if the given type format is valid.
func isValidTypeFormat(f TypeDefinitionFormat) bool {
	return f == TypeFormatSDL || f == TypeFormatJSON
}

// isValidResolverKind returns true if the given resolver kind is valid.
func isValidResolverKind(kind string) bool {
	return kind == "" || kind == resolverKindUnit || kind == resolverKindPipeline
}

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

// randomAPIKeyID generates a random API key ID with the "da2-" prefix,
// matching the format of real AWS AppSync API key IDs.
func randomAPIKeyID() string {
	const length = 13

	b := make([]byte, length)
	charCount := uint64(len(apiKeyIDChars))

	for i := range b {
		var v [8]byte
		_, _ = rand.Read(v[:])
		b[i] = apiKeyIDChars[binary.BigEndian.Uint64(v[:])%charCount]
	}

	return apiKeyIDPrefix + string(b)
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	apis              map[string]*GraphqlAPI                  // apiID → api
	schemas           map[string]*Schema                      // apiID → schema
	datasources       map[string]map[string]*DataSource       // apiID → name → ds
	resolvers         map[string]map[string]*Resolver         // apiID → "TypeName.FieldName" → resolver
	apiKeys           map[string]map[string]*APIKey           // apiID → keyID → key
	apiCaches         map[string]*APICache                    // apiID → cache
	functions         map[string]map[string]*Function         // apiID → functionID → function
	types             map[string]map[string]*APIType          // apiID → typeName → type
	domainNames       map[string]*DomainName                  // domainName → domainNameConfig
	apiAssociations   map[string]*APIAssociation              // domainName → association
	eventAPIs         map[string]*API                         // apiID → event api
	channelNamespaces map[string]map[string]*ChannelNamespace // apiID → name → ns
	sourceAssocs      map[string]*SourceAPIAssociation        // associationID → association
	lambdaFn          LambdaInvoker
	ddbBackend        DynamoDBBackend
	mu                *lockmetrics.RWMutex
	accountID         string
	region            string
	endpoint          string
}

// NewInMemoryBackend creates a new in-memory AppSync backend.
func NewInMemoryBackend(accountID, region, endpoint string) *InMemoryBackend {
	return &InMemoryBackend{
		apis:              make(map[string]*GraphqlAPI),
		schemas:           make(map[string]*Schema),
		datasources:       make(map[string]map[string]*DataSource),
		resolvers:         make(map[string]map[string]*Resolver),
		apiKeys:           make(map[string]map[string]*APIKey),
		apiCaches:         make(map[string]*APICache),
		functions:         make(map[string]map[string]*Function),
		types:             make(map[string]map[string]*APIType),
		domainNames:       make(map[string]*DomainName),
		apiAssociations:   make(map[string]*APIAssociation),
		eventAPIs:         make(map[string]*API),
		channelNamespaces: make(map[string]map[string]*ChannelNamespace),
		sourceAssocs:      make(map[string]*SourceAPIAssociation),
		mu:                lockmetrics.New("appsync"),
		accountID:         accountID,
		region:            region,
		endpoint:          endpoint,
	}
}

// Reset clears all state from the backend, returning it to a clean initial state.
// Useful for resetting state between tests.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Close tag resources before discarding.
	for _, api := range b.apis {
		if api.Tags != nil {
			api.Tags.Close()
		}
	}

	for _, dss := range b.datasources {
		for _, ds := range dss {
			if ds != nil && ds.Tags != nil {
				ds.Tags.Close()
			}
		}
	}

	b.apis = make(map[string]*GraphqlAPI)
	b.schemas = make(map[string]*Schema)
	b.datasources = make(map[string]map[string]*DataSource)
	b.resolvers = make(map[string]map[string]*Resolver)
	b.apiKeys = make(map[string]map[string]*APIKey)
	b.apiCaches = make(map[string]*APICache)
	b.functions = make(map[string]map[string]*Function)
	b.types = make(map[string]map[string]*APIType)
	b.domainNames = make(map[string]*DomainName)
	b.apiAssociations = make(map[string]*APIAssociation)
	b.eventAPIs = make(map[string]*API)
	b.channelNamespaces = make(map[string]map[string]*ChannelNamespace)
	b.sourceAssocs = make(map[string]*SourceAPIAssociation)
}

// SetLambdaInvoker configures the Lambda invoker for LAMBDA data sources.
func (b *InMemoryBackend) SetLambdaInvoker(fn LambdaInvoker) {
	b.lambdaFn = fn
}

// SetDynamoDBBackend configures the DynamoDB backend for DYNAMODB data sources.
func (b *InMemoryBackend) SetDynamoDBBackend(ddb DynamoDBBackend) {
	b.ddbBackend = ddb
}

// CreateGraphqlAPI creates a new GraphQL API.
func (b *InMemoryBackend) CreateGraphqlAPI(
	name string,
	authType AuthenticationType,
	xrayEnabled bool,
	apiType string,
	visibility string,
	additionalAuthProviders []AdditionalAuthenticationProvider,
	tagMap map[string]string,
	cfg *GraphqlAPIConfig,
) (*GraphqlAPI, error) {
	b.mu.Lock("CreateGraphqlApi")
	defer b.mu.Unlock()

	if authType != "" && !isValidAuthType(authType) {
		return nil, fmt.Errorf("%w: invalid authenticationType %q", ErrValidation, authType)
	}

	if authType == "" {
		authType = AuthTypeAPIKey
	}

	if apiType == "" {
		apiType = apiTypeGraphQL
	} else if !isValidGraphqlAPIType(apiType) {
		return nil, fmt.Errorf("%w: invalid apiType %q, must be GRAPHQL or MERGED", ErrValidation, apiType)
	}

	if visibility == "" {
		visibility = VisibilityGlobal
	} else if visibility != VisibilityGlobal && visibility != VisibilityPrivate {
		return nil, fmt.Errorf("%w: invalid visibility %q, must be GLOBAL or PRIVATE", ErrValidation, visibility)
	}

	apiID := randomAPIID()
	apiARN := arn.Build("appsync", b.region, b.accountID, "apis/"+apiID)

	graphqlEndpoint := fmt.Sprintf("%s/v1/apis/%s/graphql", b.endpoint, apiID)

	now := time.Now().Unix()

	api := &GraphqlAPI{
		APIID:                             apiID,
		ARN:                               apiARN,
		Name:                              name,
		AuthenticationType:                authType,
		Visibility:                        visibility,
		AdditionalAuthenticationProviders: additionalAuthProviders,
		Region:                            b.region,
		XrayEnabled:                       xrayEnabled,
		APIType:                           apiType,
		CreatedAt:                         now,
		UpdatedAt:                         now,
		URIs: map[string]string{
			apiTypeGraphQL: graphqlEndpoint,
			"REALTIME":     graphqlEndpoint,
		},
		Tags: tags.New("appsync.api." + apiID + ".tags"),
	}

	for k, v := range tagMap {
		api.Tags.Set(k, v)
	}

	b.apis[apiID] = api

	cp := *api

	return &cp, nil
}

// GetGraphqlAPI returns a GraphQL API by ID.
func (b *InMemoryBackend) GetGraphqlAPI(apiID string) (*GraphqlAPI, error) {
	b.mu.RLock("GetGraphqlApi")
	defer b.mu.RUnlock()

	api, ok := b.apis[apiID]
	if !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	cp := *api

	return &cp, nil
}

// UpdateGraphqlAPI updates an existing GraphQL API's name and/or authentication type.
func (b *InMemoryBackend) UpdateGraphqlAPI(
	apiID, name string,
	authType AuthenticationType,
	xrayEnabled *bool,
	visibility string,
	additionalAuthProviders []AdditionalAuthenticationProvider,
) (*GraphqlAPI, error) {
	b.mu.Lock("UpdateGraphqlApi")
	defer b.mu.Unlock()

	api, ok := b.apis[apiID]
	if !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if authType != "" && !isValidAuthType(authType) {
		return nil, fmt.Errorf("%w: invalid authenticationType %q", ErrValidation, authType)
	}

	if visibility != "" && visibility != VisibilityGlobal && visibility != VisibilityPrivate {
		return nil, fmt.Errorf("%w: invalid visibility %q, must be GLOBAL or PRIVATE", ErrValidation, visibility)
	}

	if name != "" {
		api.Name = name
	}

	if authType != "" {
		api.AuthenticationType = authType
	}

	if xrayEnabled != nil {
		api.XrayEnabled = *xrayEnabled
	}

	if visibility != "" {
		api.Visibility = visibility
	}

	if additionalAuthProviders != nil {
		api.AdditionalAuthenticationProviders = additionalAuthProviders
	}

	api.UpdatedAt = time.Now().Unix()

	cp := *api

	return &cp, nil
}

// ListGraphqlAPIs returns all GraphQL APIs, optionally filtered by apiType (apiTypeGraphQL or "MERGED").
func (b *InMemoryBackend) ListGraphqlAPIs(apiType string) ([]*GraphqlAPI, error) {
	b.mu.RLock("ListGraphqlApis")
	defer b.mu.RUnlock()

	out := make([]*GraphqlAPI, 0, len(b.apis))

	for _, api := range b.apis {
		if apiType != "" && api.APIType != apiType {
			continue
		}

		cp := *api
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *GraphqlAPI) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}

// DeleteGraphqlAPI deletes a GraphQL API by ID.
func (b *InMemoryBackend) DeleteGraphqlAPI(apiID string) error {
	b.mu.Lock("DeleteGraphqlApi")
	defer b.mu.Unlock()

	api, ok := b.apis[apiID]
	if !ok {
		return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	// Snapshot data sources before deletion so we can release their tag resources.
	dss := b.datasources[apiID]

	delete(b.apis, apiID)
	delete(b.schemas, apiID)
	delete(b.datasources, apiID)
	delete(b.resolvers, apiID)

	// Cascade-delete sub-resources added as part of issue #842.
	delete(b.apiKeys, apiID)
	delete(b.apiCaches, apiID)
	delete(b.functions, apiID)
	delete(b.types, apiID)

	if api.Tags != nil {
		api.Tags.Close()
	}

	for _, ds := range dss {
		if ds != nil && ds.Tags != nil {
			ds.Tags.Close()
		}
	}

	return nil
}

// StartSchemaCreation parses and stores the schema SDL for an API.
func (b *InMemoryBackend) StartSchemaCreation(apiID, sdl string) (*Schema, error) {
	b.mu.Lock("StartSchemaCreation")
	defer b.mu.Unlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	// Validate and parse the schema.
	parsed, gqlErr := gqlparser.LoadSchema(&ast.Source{
		Name:  "schema.graphql",
		Input: sdl,
	})
	if gqlErr != nil {
		schema := &Schema{
			APIID:   apiID,
			SDL:     sdl,
			Status:  SchemaStatusFailed,
			Details: gqlErr.Error(),
		}
		b.schemas[apiID] = schema

		return nil, fmt.Errorf("%w: %s", ErrInvalidSchema, gqlErr.Error())
	}

	schema := &Schema{
		APIID:        apiID,
		SDL:          sdl,
		Status:       SchemaStatusActive,
		parsedSchema: parsed,
	}
	b.schemas[apiID] = schema

	cp := *schema

	return &cp, nil
}

// GetSchemaCreationStatus returns the current schema creation status for an API.
func (b *InMemoryBackend) GetSchemaCreationStatus(apiID string) (*Schema, error) {
	b.mu.RLock("GetSchemaCreationStatus")
	defer b.mu.RUnlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	schema, ok := b.schemas[apiID]
	if !ok {
		return &Schema{
			APIID:  apiID,
			Status: SchemaStatusNotApplicable,
		}, nil
	}

	cp := *schema

	return &cp, nil
}

// GetIntrospectionSchema returns the schema SDL for an API.
func (b *InMemoryBackend) GetIntrospectionSchema(apiID, _ string) ([]byte, error) {
	b.mu.RLock("GetIntrospectionSchema")
	defer b.mu.RUnlock()

	schema, ok := b.schemas[apiID]
	if !ok {
		return nil, fmt.Errorf("%w: schema not found for api %s", ErrNotFound, apiID)
	}

	return []byte(schema.SDL), nil
}

// CreateDataSource creates a data source for an API.
func (b *InMemoryBackend) CreateDataSource(apiID string, ds *DataSource) (*DataSource, error) {
	b.mu.Lock("CreateDataSource")
	defer b.mu.Unlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if ds.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if ds.Type == "" {
		return nil, fmt.Errorf("%w: type is required", ErrValidation)
	}

	if !isValidDataSourceType(ds.Type) {
		return nil, fmt.Errorf("%w: invalid type %q", ErrValidation, ds.Type)
	}

	if ds.Type == DataSourceTypeHTTP && (ds.HTTPConfig == nil || ds.HTTPConfig.Endpoint == "") {
		return nil, fmt.Errorf("%w: httpConfig.endpoint is required for HTTP data sources", ErrValidation)
	}

	if b.datasources[apiID] == nil {
		b.datasources[apiID] = make(map[string]*DataSource)
	}

	if _, exists := b.datasources[apiID][ds.Name]; exists {
		return nil, fmt.Errorf("%w: datasource %s already exists", ErrAlreadyExists, ds.Name)
	}

	ds.APIID = apiID
	ds.DataSourceARN = arn.Build(
		"appsync",
		b.region,
		b.accountID,
		fmt.Sprintf("apis/%s/datasources/%s", apiID, ds.Name),
	)

	if ds.Tags == nil {
		ds.Tags = tags.New("appsync.ds." + apiID + "." + ds.Name + ".tags")
	}

	b.datasources[apiID][ds.Name] = ds

	cp := *ds

	return &cp, nil
}

// GetDataSource returns a data source by API ID and name.
func (b *InMemoryBackend) GetDataSource(apiID, name string) (*DataSource, error) {
	b.mu.RLock("GetDataSource")
	defer b.mu.RUnlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	dss := b.datasources[apiID]
	ds, ok := dss[name]

	if !ok {
		return nil, fmt.Errorf("%w: datasource %s not found", ErrNotFound, name)
	}

	cp := *ds

	return &cp, nil
}

// ListDataSources returns all data sources for an API.
func (b *InMemoryBackend) ListDataSources(apiID string) ([]*DataSource, error) {
	b.mu.RLock("ListDataSources")
	defer b.mu.RUnlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	dss := b.datasources[apiID]
	out := make([]*DataSource, 0, len(dss))

	for _, ds := range dss {
		cp := *ds
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *DataSource) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}

// DeleteDataSource deletes a data source.
// Returns an error if any resolver in the API still references this data source.
func (b *InMemoryBackend) DeleteDataSource(apiID, name string) error {
	b.mu.Lock("DeleteDataSource")
	defer b.mu.Unlock()

	dss, ok := b.datasources[apiID]
	if !ok || dss[name] == nil {
		return fmt.Errorf("%w: datasource %s not found", ErrNotFound, name)
	}

	// Prevent deletion if any UNIT resolver references this data source.
	for _, r := range b.resolvers[apiID] {
		if r.DataSourceName == name {
			return fmt.Errorf(
				"%w: data source %s is still referenced by resolver %s.%s",
				ErrValidation,
				name,
				r.TypeName,
				r.FieldName,
			)
		}
	}

	// Prevent deletion if any function references this data source.
	for _, fn := range b.functions[apiID] {
		if fn.DataSourceName == name {
			return fmt.Errorf(
				"%w: data source %s is still referenced by function %s",
				ErrValidation,
				name,
				fn.Name,
			)
		}
	}

	ds := dss[name]
	delete(dss, name)

	if ds.Tags != nil {
		ds.Tags.Close()
	}

	return nil
}

// resolverKey builds the map key for a resolver.
func resolverKey(typeName, fieldName string) string {
	return typeName + "." + fieldName
}

// CreateResolver creates a resolver for an API type field.
func (b *InMemoryBackend) CreateResolver(apiID, typeName string, r *Resolver) (*Resolver, error) {
	b.mu.Lock("CreateResolver")
	defer b.mu.Unlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if r.FieldName == "" {
		return nil, fmt.Errorf("%w: fieldName is required", ErrValidation)
	}

	if !isValidResolverKind(r.Kind) {
		return nil, fmt.Errorf("%w: invalid kind %q, must be UNIT or PIPELINE", ErrValidation, r.Kind)
	}

	if r.Kind == resolverKindPipeline && len(r.PipelineConfig) == 0 {
		return nil, fmt.Errorf("%w: pipelineConfig is required for PIPELINE resolvers", ErrValidation)
	}

	if (r.Kind == "" || r.Kind == resolverKindUnit) && r.DataSourceName == "" {
		return nil, fmt.Errorf("%w: dataSourceName is required for UNIT resolvers", ErrValidation)
	}

	if b.resolvers[apiID] == nil {
		b.resolvers[apiID] = make(map[string]*Resolver)
	}

	key := resolverKey(typeName, r.FieldName)
	if _, exists := b.resolvers[apiID][key]; exists {
		return nil, fmt.Errorf("%w: resolver %s.%s already exists", ErrAlreadyExists, typeName, r.FieldName)
	}

	r.APIID = apiID
	r.TypeName = typeName
	r.ResolverARN = arn.Build("appsync", b.region, b.accountID,
		fmt.Sprintf("apis/%s/types/%s/resolvers/%s", apiID, typeName, r.FieldName))

	if r.Kind == "" {
		r.Kind = resolverKindUnit
	}

	b.resolvers[apiID][key] = r

	cp := *r

	return &cp, nil
}

// GetResolver returns a resolver by API ID, type, and field name.
func (b *InMemoryBackend) GetResolver(apiID, typeName, fieldName string) (*Resolver, error) {
	b.mu.RLock("GetResolver")
	defer b.mu.RUnlock()

	res, ok := b.resolvers[apiID]
	if !ok {
		return nil, fmt.Errorf("%w: resolver %s.%s not found", ErrNotFound, typeName, fieldName)
	}

	r, ok := res[resolverKey(typeName, fieldName)]
	if !ok {
		return nil, fmt.Errorf("%w: resolver %s.%s not found", ErrNotFound, typeName, fieldName)
	}

	cp := *r

	return &cp, nil
}

// ListResolvers returns all resolvers for an API type.
func (b *InMemoryBackend) ListResolvers(apiID, typeName string) ([]*Resolver, error) {
	b.mu.RLock("ListResolvers")
	defer b.mu.RUnlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	res := b.resolvers[apiID]
	out := make([]*Resolver, 0, len(res))

	prefix := typeName + "."
	for key, r := range res {
		if strings.HasPrefix(key, prefix) {
			cp := *r
			out = append(out, &cp)
		}
	}

	slices.SortFunc(out, func(a, b *Resolver) int {
		return strings.Compare(a.FieldName, b.FieldName)
	})

	return out, nil
}

// DeleteResolver deletes a resolver.
func (b *InMemoryBackend) DeleteResolver(apiID, typeName, fieldName string) error {
	b.mu.Lock("DeleteResolver")
	defer b.mu.Unlock()

	res, ok := b.resolvers[apiID]
	if !ok {
		return fmt.Errorf("%w: resolver %s.%s not found", ErrNotFound, typeName, fieldName)
	}

	key := resolverKey(typeName, fieldName)
	if _, ok = res[key]; !ok {
		return fmt.Errorf("%w: resolver %s.%s not found", ErrNotFound, typeName, fieldName)
	}

	delete(res, key)

	return nil
}

// ExecuteGraphQL executes a GraphQL query/mutation against the configured resolvers.
func (b *InMemoryBackend) ExecuteGraphQL(
	ctx context.Context,
	apiID, query, operationName string,
	variables map[string]any,
) (map[string]any, error) {
	b.mu.RLock("ExecuteGraphQL")

	api, apiOK := b.apis[apiID]
	schema := b.schemas[apiID]

	// Copy resolver and datasource maps under the lock to avoid data races with
	// concurrent Create/Delete operations.
	resolversCopy := make(map[string]*Resolver, len(b.resolvers[apiID]))
	maps.Copy(resolversCopy, b.resolvers[apiID])

	datasourcesCopy := make(map[string]*DataSource, len(b.datasources[apiID]))
	maps.Copy(datasourcesCopy, b.datasources[apiID])

	b.mu.RUnlock()

	if !apiOK {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	_ = api

	return executeGraphQL(ctx, b, schema, resolversCopy, datasourcesCopy, query, operationName, variables)
}

// CreateAPIKey creates an API key for a GraphQL API.
func (b *InMemoryBackend) CreateAPIKey(apiID, description string, expires int64) (*APIKey, error) {
	b.mu.Lock("CreateAPIKey")
	defer b.mu.Unlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if b.apiKeys[apiID] == nil {
		b.apiKeys[apiID] = make(map[string]*APIKey)
	}

	if len(b.apiKeys[apiID]) >= maxAPIKeysPerAPI {
		return nil, fmt.Errorf(
			"%w: api %s already has the maximum of %d API keys",
			ErrValidation,
			apiID,
			maxAPIKeysPerAPI,
		)
	}

	// Default expiry to 365 days from now if not specified.
	if expires <= 0 {
		expires = time.Now().AddDate(0, 0, defaultAPIKeyExpiryDays).Unix()
	}

	// Cap expiry at 365 days from now (AWS enforces this).
	maxExpires := time.Now().AddDate(0, 0, defaultAPIKeyExpiryDays).Unix()
	if expires > maxExpires {
		expires = maxExpires
	}

	keyID := randomAPIKeyID()

	key := &APIKey{
		ID:          keyID,
		Description: description,
		Expires:     expires,
	}

	b.apiKeys[apiID][keyID] = key

	cp := *key

	return &cp, nil
}

// CreateAPICache creates a cache configuration for a GraphQL API.
func (b *InMemoryBackend) CreateAPICache(apiID string, cache *APICache) (*APICache, error) {
	b.mu.Lock("CreateAPICache")
	defer b.mu.Unlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if cache.TTL <= 0 {
		return nil, fmt.Errorf("%w: ttl must be greater than 0", ErrValidation)
	}

	if cache.Type == "" {
		return nil, fmt.Errorf("%w: type is required", ErrValidation)
	}

	if !isValidAPICacheType(cache.Type) {
		return nil, fmt.Errorf("%w: invalid cache type %q", ErrValidation, cache.Type)
	}

	if cache.APICachingBehavior == "" {
		return nil, fmt.Errorf("%w: apiCachingBehavior is required", ErrValidation)
	}

	if !isValidAPICachingBehavior(cache.APICachingBehavior) {
		return nil, fmt.Errorf("%w: invalid apiCachingBehavior %q", ErrValidation, cache.APICachingBehavior)
	}

	if _, exists := b.apiCaches[apiID]; exists {
		return nil, fmt.Errorf("%w: api cache already exists for api %s", ErrAlreadyExists, apiID)
	}

	cache.APIID = apiID
	if cache.Status == "" {
		cache.Status = "AVAILABLE"
	}

	b.apiCaches[apiID] = cache

	cp := *cache

	return &cp, nil
}

// CreateFunction creates an AppSync pipeline function.
func (b *InMemoryBackend) CreateFunction(apiID string, f *Function) (*Function, error) {
	b.mu.Lock("CreateFunction")
	defer b.mu.Unlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if f.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrValidation)
	}

	if b.functions[apiID] == nil {
		b.functions[apiID] = make(map[string]*Function)
	}

	// Enforce name uniqueness across all functions in the API.
	for _, existing := range b.functions[apiID] {
		if existing.Name == f.Name {
			return nil, fmt.Errorf("%w: function with name %s already exists", ErrAlreadyExists, f.Name)
		}
	}

	funcID := randomAPIID()
	funcARN := arn.Build("appsync", b.region, b.accountID,
		fmt.Sprintf("apis/%s/functions/%s", apiID, funcID))

	f.APIID = apiID
	f.FunctionID = funcID
	f.FunctionARN = funcARN

	// Default to the well-known function runtime version.
	if f.FunctionVersion == "" {
		f.FunctionVersion = defaultFunctionVersion
	}

	b.functions[apiID][funcID] = f

	cp := *f

	return &cp, nil
}

// CreateType creates a GraphQL type for an API.
func (b *InMemoryBackend) CreateType(apiID, definition string, format TypeDefinitionFormat) (*APIType, error) {
	b.mu.Lock("CreateType")
	defer b.mu.Unlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if format != "" && !isValidTypeFormat(format) {
		return nil, fmt.Errorf("%w: invalid format %q, must be SDL or JSON", ErrValidation, format)
	}

	if b.types[apiID] == nil {
		b.types[apiID] = make(map[string]*APIType)
	}

	// Extract type name from definition (assumes SDL format: "type TypeName { ... }").
	name := extractTypeName(definition)
	if name == "" {
		name = randomAPIID()
	}

	if _, exists := b.types[apiID][name]; exists {
		return nil, fmt.Errorf("%w: type %s already exists for api %s", ErrAlreadyExists, name, apiID)
	}

	typeARN := arn.Build("appsync", b.region, b.accountID,
		fmt.Sprintf("apis/%s/types/%s", apiID, name))

	t := &APIType{
		ARN:        typeARN,
		Name:       name,
		Definition: definition,
		Format:     format,
		APIID:      apiID,
	}

	b.types[apiID][name] = t

	cp := *t

	return &cp, nil
}

// extractTypeName extracts the type name from a SDL type definition.
func extractTypeName(definition string) string {
	definition = strings.TrimSpace(definition)
	for _, prefix := range []string{"type ", "input ", "enum ", "interface ", "union ", "scalar "} {
		if after, ok := strings.CutPrefix(definition, prefix); ok {
			rest := after
			// Take the first word (the type name).
			end := strings.IndexAny(rest, " \t\n\r{")
			if end > 0 {
				return rest[:end]
			}

			return rest
		}
	}

	return ""
}

// isValidDomainName returns true if the given domain name looks like a valid DNS name.
func isValidDomainName(domain string) bool {
	n := len(domain)
	if n == 0 || n > 253 {
		return false
	}

	// Must not have leading or trailing dots.
	if domain[0] == '.' || domain[n-1] == '.' {
		return false
	}

	// Must contain at least one dot (ruling out bare hostnames).
	return strings.Contains(domain, ".")
}

// CreateDomainName creates a custom domain name.
func (b *InMemoryBackend) CreateDomainName(
	domainName, certificateARN, description string,
	tagMap map[string]string,
) (*DomainName, error) {
	b.mu.Lock("CreateDomainName")
	defer b.mu.Unlock()

	if !isValidDomainName(domainName) {
		return nil, fmt.Errorf("%w: invalid domain name %q", ErrValidation, domainName)
	}

	if _, exists := b.domainNames[domainName]; exists {
		return nil, fmt.Errorf("%w: domain name %s already exists", ErrAlreadyExists, domainName)
	}

	domainNameARN := arn.Build("appsync", b.region, b.accountID, "domainnames/"+domainName)

	dn := &DomainName{
		DomainName:     domainName,
		CertificateARN: certificateARN,
		Description:    description,
		Tags:           tagMap,
		AppsyncDomain:  domainName + ".appsync-api." + b.region + ".amazonaws.com",
		HostedZoneID:   "Z2FDTNDATAQYW2",
		DomainNameARN:  domainNameARN,
	}

	b.domainNames[domainName] = dn

	cp := *dn

	return &cp, nil
}

// AssociateAPI associates an API with a custom domain name.
func (b *InMemoryBackend) AssociateAPI(domainName, apiID string) (*APIAssociation, error) {
	b.mu.Lock("AssociateAPI")
	defer b.mu.Unlock()

	dn, ok := b.domainNames[domainName]
	if !ok {
		return nil, fmt.Errorf("%w: domain name %s not found", ErrNotFound, domainName)
	}

	// Validate that the API being associated exists.
	if _, exists := b.apis[apiID]; !exists {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	assoc := &APIAssociation{
		DomainName:        domainName,
		APIID:             apiID,
		AssociationStatus: "SUCCESS",
	}

	b.apiAssociations[domainName] = assoc

	// Update the domain name record to reflect the associated API.
	dn.APIID = apiID

	cp := *assoc

	return &cp, nil
}

// CreateAPI creates a new Event API.
func (b *InMemoryBackend) CreateAPI(name, ownerContact string, tagMap map[string]string) (*API, error) {
	b.mu.Lock("CreateAPI")
	defer b.mu.Unlock()

	apiID := randomAPIID()
	apiARN := arn.Build("appsync", b.region, b.accountID, "apis/"+apiID)

	api := &API{
		APIID:        apiID,
		ARN:          apiARN,
		Name:         name,
		Tags:         tagMap,
		OwnerContact: ownerContact,
		DNSHTTP:      fmt.Sprintf("%s.appsync-api.%s.amazonaws.com", apiID, b.region),
	}

	b.eventAPIs[apiID] = api

	cp := *api

	return &cp, nil
}

// CreateChannelNamespace creates a channel namespace for an Event API.
func (b *InMemoryBackend) CreateChannelNamespace(
	apiID, name string,
	tagMap map[string]string,
) (*ChannelNamespace, error) {
	b.mu.Lock("CreateChannelNamespace")
	defer b.mu.Unlock()

	if _, ok := b.eventAPIs[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if b.channelNamespaces[apiID] == nil {
		b.channelNamespaces[apiID] = make(map[string]*ChannelNamespace)
	}

	if _, exists := b.channelNamespaces[apiID][name]; exists {
		return nil, fmt.Errorf("%w: channel namespace %s already exists", ErrAlreadyExists, name)
	}

	nsARN := arn.Build("appsync", b.region, b.accountID,
		fmt.Sprintf("apis/%s/channelNamespaces/%s", apiID, name))

	now := time.Now().Unix()

	ns := &ChannelNamespace{
		APIID:               apiID,
		Name:                name,
		Tags:                tagMap,
		ChannelNamespaceARN: nsARN,
		Created:             now,
		LastModified:        now,
	}

	b.channelNamespaces[apiID][name] = ns

	cp := *ns

	return &cp, nil
}

// AssociateMergedGraphqlAPI creates an association from a source API to a merged API.
func (b *InMemoryBackend) AssociateMergedGraphqlAPI(
	sourceAPIIdentifier, mergedAPIIdentifier, description string,
) (*SourceAPIAssociation, error) {
	b.mu.Lock("AssociateMergedGraphqlAPI")
	defer b.mu.Unlock()

	if _, ok := b.apis[sourceAPIIdentifier]; !ok {
		return nil, fmt.Errorf("%w: source api %s not found", ErrNotFound, sourceAPIIdentifier)
	}

	if _, ok := b.apis[mergedAPIIdentifier]; !ok {
		return nil, fmt.Errorf("%w: merged api %s not found", ErrNotFound, mergedAPIIdentifier)
	}

	assocID := randomAPIID()
	assocARN := arn.Build("appsync", b.region, b.accountID,
		fmt.Sprintf("sourceApis/%s/mergedApiAssociations/%s", sourceAPIIdentifier, assocID))

	assoc := &SourceAPIAssociation{
		AssociationID:     assocID,
		AssociationARN:    assocARN,
		SourceAPIID:       sourceAPIIdentifier,
		MergedAPIID:       mergedAPIIdentifier,
		Description:       description,
		AssociationStatus: "MERGE_SCHEDULED",
	}

	b.sourceAssocs[assocID] = assoc

	cp := *assoc

	return &cp, nil
}

// AssociateSourceGraphqlAPI creates an association from a merged API to a source API.
func (b *InMemoryBackend) AssociateSourceGraphqlAPI(
	mergedAPIIdentifier, sourceAPIIdentifier, description string,
) (*SourceAPIAssociation, error) {
	b.mu.Lock("AssociateSourceGraphqlAPI")
	defer b.mu.Unlock()

	if _, ok := b.apis[mergedAPIIdentifier]; !ok {
		return nil, fmt.Errorf("%w: merged api %s not found", ErrNotFound, mergedAPIIdentifier)
	}

	if _, ok := b.apis[sourceAPIIdentifier]; !ok {
		return nil, fmt.Errorf("%w: source api %s not found", ErrNotFound, sourceAPIIdentifier)
	}

	assocID := randomAPIID()
	assocARN := arn.Build("appsync", b.region, b.accountID,
		fmt.Sprintf("mergedApis/%s/sourceApiAssociations/%s", mergedAPIIdentifier, assocID))

	assoc := &SourceAPIAssociation{
		AssociationID:     assocID,
		AssociationARN:    assocARN,
		SourceAPIID:       sourceAPIIdentifier,
		MergedAPIID:       mergedAPIIdentifier,
		Description:       description,
		AssociationStatus: "MERGE_SCHEDULED",
	}

	b.sourceAssocs[assocID] = assoc

	cp := *assoc

	return &cp, nil
}

// ListAPIKeys returns all non-expired API keys for a GraphQL API.
func (b *InMemoryBackend) ListAPIKeys(apiID string) ([]*APIKey, error) {
	b.mu.RLock("ListApiKeys")
	defer b.mu.RUnlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	now := time.Now().Unix()
	keys := b.apiKeys[apiID]
	out := make([]*APIKey, 0, len(keys))

	for _, k := range keys {
		// Skip expired keys — AWS does not return expired keys in ListApiKeys.
		if k.Expires > 0 && k.Expires <= now {
			continue
		}

		cp := *k
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *APIKey) int {
		return strings.Compare(a.ID, b.ID)
	})

	return out, nil
}

// DeleteAPIKey deletes an API key from a GraphQL API.
func (b *InMemoryBackend) DeleteAPIKey(apiID, keyID string) error {
	b.mu.Lock("DeleteApiKey")
	defer b.mu.Unlock()

	if _, ok := b.apis[apiID]; !ok {
		return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	keys := b.apiKeys[apiID]
	if keys == nil || keys[keyID] == nil {
		return fmt.Errorf("%w: api key %s not found", ErrNotFound, keyID)
	}

	delete(keys, keyID)

	return nil
}

// GetAPICache returns the cache configuration for a GraphQL API.
func (b *InMemoryBackend) GetAPICache(apiID string) (*APICache, error) {
	b.mu.RLock("GetApiCache")
	defer b.mu.RUnlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	cache, ok := b.apiCaches[apiID]
	if !ok {
		return nil, fmt.Errorf("%w: api cache not found for api %s", ErrNotFound, apiID)
	}

	cp := *cache

	return &cp, nil
}

// DeleteAPICache deletes the cache configuration for a GraphQL API.
func (b *InMemoryBackend) DeleteAPICache(apiID string) error {
	b.mu.Lock("DeleteApiCache")
	defer b.mu.Unlock()

	if _, ok := b.apis[apiID]; !ok {
		return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if _, ok := b.apiCaches[apiID]; !ok {
		return fmt.Errorf("%w: api cache not found for api %s", ErrNotFound, apiID)
	}

	delete(b.apiCaches, apiID)

	return nil
}

// GetFunction returns a pipeline function by ID.
func (b *InMemoryBackend) GetFunction(apiID, functionID string) (*Function, error) {
	b.mu.RLock("GetFunction")
	defer b.mu.RUnlock()

	fns := b.functions[apiID]
	if fns == nil {
		return nil, fmt.Errorf("%w: function %s not found", ErrNotFound, functionID)
	}

	fn, ok := fns[functionID]
	if !ok {
		return nil, fmt.Errorf("%w: function %s not found", ErrNotFound, functionID)
	}

	cp := *fn

	return &cp, nil
}

// ListFunctions returns all pipeline functions for a GraphQL API.
func (b *InMemoryBackend) ListFunctions(apiID string) ([]*Function, error) {
	b.mu.RLock("ListFunctions")
	defer b.mu.RUnlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	fns := b.functions[apiID]
	out := make([]*Function, 0, len(fns))

	for _, fn := range fns {
		cp := *fn
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *Function) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}

// DeleteFunction deletes a pipeline function.
// Returns an error if any resolver's pipeline config still references this function.
func (b *InMemoryBackend) DeleteFunction(apiID, functionID string) error {
	b.mu.Lock("DeleteFunction")
	defer b.mu.Unlock()

	fns := b.functions[apiID]
	if fns == nil || fns[functionID] == nil {
		return fmt.Errorf("%w: function %s not found", ErrNotFound, functionID)
	}

	// Prevent deletion if any resolver still references this function.
	for _, r := range b.resolvers[apiID] {
		if slices.Contains(r.PipelineConfig, functionID) {
			return fmt.Errorf(
				"%w: function %s is still referenced by resolver %s.%s",
				ErrValidation,
				functionID,
				r.TypeName,
				r.FieldName,
			)
		}
	}

	delete(fns, functionID)

	return nil
}

// GetType returns a GraphQL type by name.
func (b *InMemoryBackend) GetType(apiID, typeName string) (*APIType, error) {
	b.mu.RLock("GetType")
	defer b.mu.RUnlock()

	types := b.types[apiID]
	if types == nil {
		return nil, fmt.Errorf("%w: type %s not found", ErrNotFound, typeName)
	}

	t, ok := types[typeName]
	if !ok {
		return nil, fmt.Errorf("%w: type %s not found", ErrNotFound, typeName)
	}

	cp := *t

	return &cp, nil
}

// ListTypes returns all GraphQL types for an API.
func (b *InMemoryBackend) ListTypes(apiID string) ([]*APIType, error) {
	b.mu.RLock("ListTypes")
	defer b.mu.RUnlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	types := b.types[apiID]
	out := make([]*APIType, 0, len(types))

	for _, t := range types {
		cp := *t
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *APIType) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}

// DeleteType deletes a GraphQL type.
func (b *InMemoryBackend) DeleteType(apiID, typeName string) error {
	b.mu.Lock("DeleteType")
	defer b.mu.Unlock()

	types := b.types[apiID]
	if types == nil || types[typeName] == nil {
		return fmt.Errorf("%w: type %s not found", ErrNotFound, typeName)
	}

	delete(types, typeName)

	return nil
}

// GetDomainName returns a custom domain name configuration.
func (b *InMemoryBackend) GetDomainName(domainName string) (*DomainName, error) {
	b.mu.RLock("GetDomainName")
	defer b.mu.RUnlock()

	dn, ok := b.domainNames[domainName]
	if !ok {
		return nil, fmt.Errorf("%w: domain name %s not found", ErrNotFound, domainName)
	}

	cp := *dn

	return &cp, nil
}

// ListDomainNames returns all custom domain name configurations.
func (b *InMemoryBackend) ListDomainNames() ([]*DomainName, error) {
	b.mu.RLock("ListDomainNames")
	defer b.mu.RUnlock()

	out := make([]*DomainName, 0, len(b.domainNames))

	for _, dn := range b.domainNames {
		cp := *dn
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *DomainName) int {
		return strings.Compare(a.DomainName, b.DomainName)
	})

	return out, nil
}

// DeleteDomainName deletes a custom domain name configuration and its API association.
func (b *InMemoryBackend) DeleteDomainName(domainName string) error {
	b.mu.Lock("DeleteDomainName")
	defer b.mu.Unlock()

	if _, ok := b.domainNames[domainName]; !ok {
		return fmt.Errorf("%w: domain name %s not found", ErrNotFound, domainName)
	}

	delete(b.domainNames, domainName)
	delete(b.apiAssociations, domainName)

	return nil
}

// GetAPIAssociation returns the API association for a domain name.
func (b *InMemoryBackend) GetAPIAssociation(domainName string) (*APIAssociation, error) {
	b.mu.RLock("GetApiAssociation")
	defer b.mu.RUnlock()

	if _, ok := b.domainNames[domainName]; !ok {
		return nil, fmt.Errorf("%w: domain name %s not found", ErrNotFound, domainName)
	}

	assoc, ok := b.apiAssociations[domainName]
	if !ok {
		return &APIAssociation{
			DomainName:        domainName,
			AssociationStatus: "NOT_FOUND",
		}, nil
	}

	cp := *assoc

	return &cp, nil
}

// UpdateDataSource updates an existing data source.
func (b *InMemoryBackend) UpdateDataSource(apiID, name string, ds *DataSource) (*DataSource, error) {
	b.mu.Lock("UpdateDataSource")
	defer b.mu.Unlock()

	dss := b.datasources[apiID]
	if dss == nil || dss[name] == nil {
		return nil, fmt.Errorf("%w: data source %s not found", ErrNotFound, name)
	}

	existing := dss[name]

	if ds.Description != "" {
		existing.Description = ds.Description
	}

	if ds.Type != "" {
		existing.Type = ds.Type
	}

	if ds.ServiceRoleARN != "" {
		existing.ServiceRoleARN = ds.ServiceRoleARN
	}

	if ds.LambdaConfig != nil {
		existing.LambdaConfig = ds.LambdaConfig
	}

	if ds.DynamoDBConfig != nil {
		existing.DynamoDBConfig = ds.DynamoDBConfig
	}

	if ds.HTTPConfig != nil {
		existing.HTTPConfig = ds.HTTPConfig
	}

	if ds.OpenSearchConfig != nil {
		existing.OpenSearchConfig = ds.OpenSearchConfig
	}

	if ds.EventBridgeConfig != nil {
		existing.EventBridgeConfig = ds.EventBridgeConfig
	}

	if ds.RelationalDatabaseConfig != nil {
		existing.RelationalDatabaseConfig = ds.RelationalDatabaseConfig
	}

	cp := *existing

	return &cp, nil
}

// UpdateResolver updates an existing resolver.
func (b *InMemoryBackend) UpdateResolver(apiID, typeName string, r *Resolver) (*Resolver, error) {
	b.mu.Lock("UpdateResolver")
	defer b.mu.Unlock()

	key := typeName + "." + r.FieldName
	res := b.resolvers[apiID]

	if res == nil || res[key] == nil {
		return nil, fmt.Errorf("%w: resolver %s not found", ErrNotFound, key)
	}

	existing := res[key]

	if r.RequestMappingTemplate != "" {
		existing.RequestMappingTemplate = r.RequestMappingTemplate
	}

	if r.ResponseMappingTemplate != "" {
		existing.ResponseMappingTemplate = r.ResponseMappingTemplate
	}

	if r.DataSourceName != "" {
		existing.DataSourceName = r.DataSourceName
	}

	if r.Kind != "" {
		existing.Kind = r.Kind
	}

	if len(r.PipelineConfig) > 0 {
		existing.PipelineConfig = r.PipelineConfig
	}

	if r.CachingConfig != nil {
		existing.CachingConfig = r.CachingConfig
	}

	if r.SyncConfig != nil {
		existing.SyncConfig = r.SyncConfig
	}

	if r.MaxBatchSize != 0 {
		existing.MaxBatchSize = r.MaxBatchSize
	}

	cp := *existing

	return &cp, nil
}

// UpdateFunction updates an existing pipeline function.
func (b *InMemoryBackend) UpdateFunction(apiID, functionID string, f *Function) (*Function, error) {
	b.mu.Lock("UpdateFunction")
	defer b.mu.Unlock()

	fns := b.functions[apiID]
	if fns == nil || fns[functionID] == nil {
		return nil, fmt.Errorf("%w: function %s not found", ErrNotFound, functionID)
	}

	existing := fns[functionID]

	if f.Name != "" {
		existing.Name = f.Name
	}

	if f.Description != "" {
		existing.Description = f.Description
	}

	if f.DataSourceName != "" {
		existing.DataSourceName = f.DataSourceName
	}

	if f.RequestMappingTemplate != "" {
		existing.RequestMappingTemplate = f.RequestMappingTemplate
	}

	if f.ResponseMappingTemplate != "" {
		existing.ResponseMappingTemplate = f.ResponseMappingTemplate
	}

	if f.Code != "" {
		existing.Code = f.Code
	}

	cp := *existing

	return &cp, nil
}

// UpdateType updates an existing GraphQL type definition.
func (b *InMemoryBackend) UpdateType(
	apiID, typeName, definition string,
	format TypeDefinitionFormat,
) (*APIType, error) {
	b.mu.Lock("UpdateType")
	defer b.mu.Unlock()

	types := b.types[apiID]
	if types == nil || types[typeName] == nil {
		return nil, fmt.Errorf("%w: type %s not found", ErrNotFound, typeName)
	}

	existing := types[typeName]

	if definition != "" {
		existing.Definition = definition
	}

	if format != "" {
		existing.Format = format
	}

	cp := *existing

	return &cp, nil
}

// UpdateAPIKey updates an existing API key's description and/or expiry.
func (b *InMemoryBackend) UpdateAPIKey(apiID, keyID, description string, expires int64) (*APIKey, error) {
	b.mu.Lock("UpdateApiKey")
	defer b.mu.Unlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	keys := b.apiKeys[apiID]
	if keys == nil || keys[keyID] == nil {
		return nil, fmt.Errorf("%w: api key %s not found", ErrNotFound, keyID)
	}

	existing := keys[keyID]

	if description != "" {
		existing.Description = description
	}

	if expires > 0 {
		// Cap expiry at 365 days from now (AWS enforces this on update too).
		maxExpires := time.Now().AddDate(0, 0, defaultAPIKeyExpiryDays).Unix()
		if expires > maxExpires {
			expires = maxExpires
		}

		existing.Expires = expires
	}

	cp := *existing

	return &cp, nil
}

// UpdateAPICache updates the cache configuration for a GraphQL API.
func (b *InMemoryBackend) UpdateAPICache(apiID string, cache *APICache) (*APICache, error) {
	b.mu.Lock("UpdateApiCache")
	defer b.mu.Unlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	existing, ok := b.apiCaches[apiID]
	if !ok {
		return nil, fmt.Errorf("%w: api cache not found for api %s", ErrNotFound, apiID)
	}

	if cache.TTL > 0 {
		existing.TTL = cache.TTL
	}

	if cache.Type != "" {
		if !isValidAPICacheType(cache.Type) {
			return nil, fmt.Errorf("%w: invalid cache type %q", ErrValidation, cache.Type)
		}

		existing.Type = cache.Type
	}

	if cache.APICachingBehavior != "" {
		if !isValidAPICachingBehavior(cache.APICachingBehavior) {
			return nil, fmt.Errorf("%w: invalid apiCachingBehavior %q", ErrValidation, cache.APICachingBehavior)
		}

		existing.APICachingBehavior = cache.APICachingBehavior
	}

	cp := *existing

	return &cp, nil
}

// FlushAPICache flushes the cache for a GraphQL API.
func (b *InMemoryBackend) FlushAPICache(apiID string) error {
	b.mu.Lock("FlushApiCache")
	defer b.mu.Unlock()

	if _, ok := b.apis[apiID]; !ok {
		return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if _, ok := b.apiCaches[apiID]; !ok {
		return fmt.Errorf("%w: api cache not found for api %s", ErrNotFound, apiID)
	}

	return nil
}

// TagResource adds or updates tags on a GraphQL API.
func (b *InMemoryBackend) TagResource(apiID string, tagMap map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	api, ok := b.apis[apiID]
	if !ok {
		return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if api.Tags == nil {
		api.Tags = tags.New("appsync.api." + apiID + ".tags")
	}

	for k, v := range tagMap {
		api.Tags.Set(k, v)
	}

	return nil
}

// UntagResource removes tags from a GraphQL API.
func (b *InMemoryBackend) UntagResource(apiID string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	api, ok := b.apis[apiID]
	if !ok {
		return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if api.Tags != nil {
		api.Tags.DeleteKeys(tagKeys)
	}

	return nil
}

// ListTagsForResource returns all tags on a GraphQL API.
func (b *InMemoryBackend) ListTagsForResource(apiID string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	api, ok := b.apis[apiID]
	if !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if api.Tags == nil {
		return map[string]string{}, nil
	}

	return api.Tags.Clone(), nil
}

// UpdateDomainName updates an existing custom domain name configuration.
func (b *InMemoryBackend) UpdateDomainName(domainName, description, certificateARN string) (*DomainName, error) {
	b.mu.Lock("UpdateDomainName")
	defer b.mu.Unlock()

	dn, ok := b.domainNames[domainName]
	if !ok {
		return nil, fmt.Errorf("%w: domain name %s not found", ErrNotFound, domainName)
	}

	if description != "" {
		dn.Description = description
	}

	if certificateARN != "" {
		dn.CertificateARN = certificateARN
	}

	cp := *dn

	return &cp, nil
}

// DisassociateAPI removes the API association from a domain name.
func (b *InMemoryBackend) DisassociateAPI(domainName string) error {
	b.mu.Lock("DisassociateApi")
	defer b.mu.Unlock()

	if _, ok := b.domainNames[domainName]; !ok {
		return fmt.Errorf("%w: domain name %s not found", ErrNotFound, domainName)
	}

	if _, ok := b.apiAssociations[domainName]; !ok {
		return fmt.Errorf("%w: no api associated with domain %s", ErrNotFound, domainName)
	}

	// Clear APIID from domain name and remove association.
	if dn, ok := b.domainNames[domainName]; ok {
		dn.APIID = ""
	}

	delete(b.apiAssociations, domainName)

	return nil
}

// GetAPI returns an Event API by ID.
func (b *InMemoryBackend) GetAPI(apiID string) (*API, error) {
	b.mu.RLock("GetApi")
	defer b.mu.RUnlock()

	api, ok := b.eventAPIs[apiID]
	if !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	cp := *api

	return &cp, nil
}

// ListAPIs returns all Event APIs.
func (b *InMemoryBackend) ListAPIs() ([]*API, error) {
	b.mu.RLock("ListApis")
	defer b.mu.RUnlock()

	out := make([]*API, 0, len(b.eventAPIs))

	for _, api := range b.eventAPIs {
		cp := *api
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *API) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}

// DeleteAPI deletes an Event API and all its channel namespaces.
func (b *InMemoryBackend) DeleteAPI(apiID string) error {
	b.mu.Lock("DeleteApi")
	defer b.mu.Unlock()

	if _, ok := b.eventAPIs[apiID]; !ok {
		return fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	delete(b.eventAPIs, apiID)
	delete(b.channelNamespaces, apiID)

	return nil
}

// UpdateAPI updates an Event API's name or owner contact.
func (b *InMemoryBackend) UpdateAPI(apiID, name, ownerContact string) (*API, error) {
	b.mu.Lock("UpdateApi")
	defer b.mu.Unlock()

	api, ok := b.eventAPIs[apiID]
	if !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if name != "" {
		api.Name = name
	}

	if ownerContact != "" {
		api.OwnerContact = ownerContact
	}

	cp := *api

	return &cp, nil
}

// GetChannelNamespace returns a channel namespace by API ID and name.
func (b *InMemoryBackend) GetChannelNamespace(apiID, name string) (*ChannelNamespace, error) {
	b.mu.RLock("GetChannelNamespace")
	defer b.mu.RUnlock()

	nss := b.channelNamespaces[apiID]
	if nss == nil || nss[name] == nil {
		return nil, fmt.Errorf("%w: channel namespace %s not found", ErrNotFound, name)
	}

	cp := *nss[name]

	return &cp, nil
}

// ListChannelNamespaces returns all channel namespaces for an Event API.
func (b *InMemoryBackend) ListChannelNamespaces(apiID string) ([]*ChannelNamespace, error) {
	b.mu.RLock("ListChannelNamespaces")
	defer b.mu.RUnlock()

	if _, ok := b.eventAPIs[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	nss := b.channelNamespaces[apiID]
	out := make([]*ChannelNamespace, 0, len(nss))

	for _, ns := range nss {
		cp := *ns
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *ChannelNamespace) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}

// UpdateChannelNamespace updates a channel namespace's code handlers.
func (b *InMemoryBackend) UpdateChannelNamespace(apiID, name, codeHandlers string) (*ChannelNamespace, error) {
	b.mu.Lock("UpdateChannelNamespace")
	defer b.mu.Unlock()

	nss := b.channelNamespaces[apiID]
	if nss == nil || nss[name] == nil {
		return nil, fmt.Errorf("%w: channel namespace %s not found", ErrNotFound, name)
	}

	existing := nss[name]

	if codeHandlers != "" {
		existing.CodeHandlers = codeHandlers
	}

	existing.LastModified = time.Now().Unix()

	cp := *existing

	return &cp, nil
}

// DeleteChannelNamespace removes a channel namespace from an Event API.
func (b *InMemoryBackend) DeleteChannelNamespace(apiID, name string) error {
	b.mu.Lock("DeleteChannelNamespace")
	defer b.mu.Unlock()

	nss := b.channelNamespaces[apiID]
	if nss == nil || nss[name] == nil {
		return fmt.Errorf("%w: channel namespace %s not found", ErrNotFound, name)
	}

	delete(nss, name)

	return nil
}

// GetSourceAPIAssociation returns a source API association by merged API ID and association ID.
func (b *InMemoryBackend) GetSourceAPIAssociation(mergedAPIID, associationID string) (*SourceAPIAssociation, error) {
	b.mu.RLock("GetSourceApiAssociation")
	defer b.mu.RUnlock()

	assoc, ok := b.sourceAssocs[associationID]
	if !ok || assoc.MergedAPIID != mergedAPIID {
		return nil, fmt.Errorf("%w: source api association %s not found", ErrNotFound, associationID)
	}

	cp := *assoc

	return &cp, nil
}

// ListSourceAPIAssociations returns all source API associations for a merged API.
func (b *InMemoryBackend) ListSourceAPIAssociations(mergedAPIID string) ([]*SourceAPIAssociation, error) {
	b.mu.RLock("ListSourceApiAssociations")
	defer b.mu.RUnlock()

	out := make([]*SourceAPIAssociation, 0, len(b.sourceAssocs))

	for _, assoc := range b.sourceAssocs {
		if assoc.MergedAPIID == mergedAPIID {
			cp := *assoc
			out = append(out, &cp)
		}
	}

	slices.SortFunc(out, func(a, b *SourceAPIAssociation) int {
		return strings.Compare(a.AssociationID, b.AssociationID)
	})

	return out, nil
}

// DisassociateMergedGraphqlAPI removes a merged API association from a source API.
func (b *InMemoryBackend) DisassociateMergedGraphqlAPI(sourceAPIID, associationID string) error {
	b.mu.Lock("DisassociateMergedGraphqlApi")
	defer b.mu.Unlock()

	assoc, ok := b.sourceAssocs[associationID]
	if !ok || assoc.SourceAPIID != sourceAPIID {
		return fmt.Errorf("%w: merged api association %s not found", ErrNotFound, associationID)
	}

	delete(b.sourceAssocs, associationID)

	return nil
}

// DisassociateSourceGraphqlAPI removes a source API association from a merged API.
func (b *InMemoryBackend) DisassociateSourceGraphqlAPI(mergedAPIID, associationID string) error {
	b.mu.Lock("DisassociateSourceGraphqlApi")
	defer b.mu.Unlock()

	assoc, ok := b.sourceAssocs[associationID]
	if !ok || assoc.MergedAPIID != mergedAPIID {
		return fmt.Errorf("%w: source api association %s not found", ErrNotFound, associationID)
	}

	delete(b.sourceAssocs, associationID)

	return nil
}

// ListResolversByFunction returns all resolvers that use a given function.
func (b *InMemoryBackend) ListResolversByFunction(apiID, functionID string) ([]*Resolver, error) {
	b.mu.RLock("ListResolversByFunction")
	defer b.mu.RUnlock()

	if _, ok := b.apis[apiID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	var out []*Resolver

	for _, r := range b.resolvers[apiID] {
		if slices.Contains(r.PipelineConfig, functionID) {
			cp := *r
			out = append(out, &cp)
		}
	}

	slices.SortFunc(out, func(a, b *Resolver) int {
		key := func(r *Resolver) string { return r.TypeName + "." + r.FieldName }

		return strings.Compare(key(a), key(b))
	})

	return out, nil
}

// GetGraphqlAPIEnvironmentVariables returns environment variables for a GraphQL API.
func (b *InMemoryBackend) GetGraphqlAPIEnvironmentVariables(apiID string) (map[string]string, error) {
	b.mu.RLock("GetGraphqlApiEnvironmentVariables")
	defer b.mu.RUnlock()

	api, ok := b.apis[apiID]
	if !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if api.EnvironmentVariables == nil {
		return map[string]string{}, nil
	}

	out := maps.Clone(api.EnvironmentVariables)

	return out, nil
}

// PutGraphqlAPIEnvironmentVariables replaces the environment variables for a GraphQL API.
func (b *InMemoryBackend) PutGraphqlAPIEnvironmentVariables(
	apiID string,
	envVars map[string]string,
) (map[string]string, error) {
	b.mu.Lock("PutGraphqlApiEnvironmentVariables")
	defer b.mu.Unlock()

	api, ok := b.apis[apiID]
	if !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if len(envVars) > maxEnvironmentVariables {
		return nil, fmt.Errorf(
			"%w: environment variables cannot exceed %d entries",
			ErrValidation,
			maxEnvironmentVariables,
		)
	}

	api.EnvironmentVariables = maps.Clone(envVars)

	return maps.Clone(api.EnvironmentVariables), nil
}

// EvaluateMappingTemplate evaluates a VTL mapping template with the provided context JSON.
// The context is expected to be a JSON object with optional "arguments" and "result" keys.
func (b *InMemoryBackend) EvaluateMappingTemplate(template, contextJSON string) (string, error) {
	var ctx struct {
		Arguments map[string]any `json:"arguments"`
		Result    any            `json:"result"`
	}

	if contextJSON != "" {
		if err := json.Unmarshal([]byte(contextJSON), &ctx); err != nil {
			return "", fmt.Errorf("%w: invalid context JSON", ErrValidation)
		}
	}

	out, err := renderVTL(template, ctx.Arguments, ctx.Result)
	if err != nil {
		return "", err
	}

	return out, nil
}

// EvaluateCode evaluates APPSYNC_JS code (basic stub — returns empty result).
func (b *InMemoryBackend) EvaluateCode(code, _, _, _ string) (string, error) {
	if code == "" {
		return "", fmt.Errorf("%w: code is required", ErrValidation)
	}

	// Stub: return an empty evaluationResult.
	result := `{"evaluationResult":"{}"}`

	return result, nil
}

// StartDataSourceIntrospection starts an introspection job for a data source.
// Returns an introspection ID that can be polled via GetDataSourceIntrospection.
func (b *InMemoryBackend) StartDataSourceIntrospection(apiID, dataSourceName string) (string, error) {
	b.mu.RLock("StartDataSourceIntrospection")
	defer b.mu.RUnlock()

	if _, ok := b.apis[apiID]; !ok {
		return "", fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	if _, ok := b.datasources[apiID][dataSourceName]; !ok {
		return "", fmt.Errorf("%w: datasource %s not found", ErrNotFound, dataSourceName)
	}

	id := randomAPIID()

	return id, nil
}

// GetDataSourceIntrospection returns the result of a data source introspection job.
// Since introspection is a no-op stub, this always returns a COMPLETED result.
func (b *InMemoryBackend) GetDataSourceIntrospection(introspectionID string) (*DataSourceIntrospectionResult, error) {
	if introspectionID == "" {
		return nil, fmt.Errorf("%w: introspectionId is required", ErrValidation)
	}

	return &DataSourceIntrospectionResult{
		IntrospectionID: introspectionID,
		Status:          "SUCCESS",
		Models:          []any{},
	}, nil
}

// StartSchemaMerge triggers a schema merge for a merged GraphQL API.
func (b *InMemoryBackend) StartSchemaMerge(apiID string) (SchemaStatus, error) {
	b.mu.RLock("StartSchemaMerge")
	defer b.mu.RUnlock()

	if _, ok := b.apis[apiID]; !ok {
		return "", fmt.Errorf("%w: api %s not found", ErrNotFound, apiID)
	}

	return SchemaStatusActive, nil
}

// UpdateSourceAPIAssociation updates the description of a source API association.
func (b *InMemoryBackend) UpdateSourceAPIAssociation(
	mergedAPIID, associationID, description string,
) (*SourceAPIAssociation, error) {
	b.mu.Lock("UpdateSourceApiAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.sourceAssocs[associationID]
	if !ok || assoc.MergedAPIID != mergedAPIID {
		return nil, fmt.Errorf("%w: source api association %s not found", ErrNotFound, associationID)
	}

	cp := *assoc
	cp.Description = description

	b.sourceAssocs[associationID] = &cp

	return &cp, nil
}

// ListTypesByAssociation lists types associated with a given merged API source association.
// Since types are stored per-API and not per-association in the in-memory backend,
// this returns types from the merged API.
func (b *InMemoryBackend) ListTypesByAssociation(mergedAPIID, associationID, _ string) ([]*APIType, error) {
	b.mu.RLock("ListTypesByAssociation")
	defer b.mu.RUnlock()

	if _, ok := b.apis[mergedAPIID]; !ok {
		return nil, fmt.Errorf("%w: api %s not found", ErrNotFound, mergedAPIID)
	}

	if _, ok := b.sourceAssocs[associationID]; !ok {
		return nil, fmt.Errorf("%w: source api association %s not found", ErrNotFound, associationID)
	}

	ts := b.types[mergedAPIID]
	out := make([]*APIType, 0, len(ts))

	for _, t := range ts {
		cp := *t
		out = append(out, &cp)
	}

	slices.SortFunc(out, func(a, b *APIType) int {
		return strings.Compare(a.Name, b.Name)
	})

	return out, nil
}

// SweepExpiredAPIKeys removes all expired API keys across all GraphQL APIs.
func (b *InMemoryBackend) SweepExpiredAPIKeys() int {
	b.mu.Lock("SweepExpiredAPIKeys")
	defer b.mu.Unlock()

	now := time.Now().Unix()
	totalEvicted := 0

	for apiID, keys := range b.apiKeys {
		for keyID, k := range keys {
			if k.Expires > 0 && k.Expires <= now {
				delete(keys, keyID)
				totalEvicted++
			}
		}
		if len(keys) == 0 {
			delete(b.apiKeys, apiID)
		}
	}

	return totalEvicted
}
