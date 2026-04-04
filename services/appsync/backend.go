package appsync

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/vektah/gqlparser/v2"
	"github.com/vektah/gqlparser/v2/ast"
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
	CreateGraphqlAPI(name string, authType AuthenticationType, tagMap map[string]string) (*GraphqlAPI, error)
	GetGraphqlAPI(apiID string) (*GraphqlAPI, error)
	ListGraphqlAPIs() ([]*GraphqlAPI, error)
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
	CreateAPI(name string, tagMap map[string]string) (*API, error)
	CreateChannelNamespace(apiID, name string, tagMap map[string]string) (*ChannelNamespace, error)
	// API key operations.
	CreateAPIKey(apiID, description string, expires int64) (*APIKey, error)
	// API cache operations.
	CreateAPICache(apiID string, cache *APICache) (*APICache, error)
	// Function operations.
	CreateFunction(apiID string, f *Function) (*Function, error)
	// Type operations.
	CreateType(apiID, definition string, format TypeDefinitionFormat) (*APIType, error)
	// Domain name operations.
	CreateDomainName(domainName, certificateARN, description string, tagMap map[string]string) (*DomainName, error)
	AssociateAPI(domainName, apiID string) (*APIAssociation, error)
	// Merged/source API association operations.
	AssociateMergedGraphqlAPI(
		sourceAPIIdentifier, mergedAPIIdentifier, description string,
	) (*SourceAPIAssociation, error)
	AssociateSourceGraphqlAPI(
		mergedAPIIdentifier, sourceAPIIdentifier, description string,
	) (*SourceAPIAssociation, error)
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

// defaultFunctionVersion is the default AppSync function runtime version.
const defaultFunctionVersion = "2018-05-29"

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

// randomAPIID generates a cryptographically random 26-character alphanumeric ID,
// matching the format of real AWS AppSync API IDs (no hyphens).
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
	tagMap map[string]string,
) (*GraphqlAPI, error) {
	b.mu.Lock("CreateGraphqlApi")
	defer b.mu.Unlock()

	if authType != "" && !isValidAuthType(authType) {
		return nil, fmt.Errorf("%w: invalid authenticationType %q", ErrValidation, authType)
	}

	if authType == "" {
		authType = AuthTypeAPIKey
	}

	apiID := randomAPIID()
	apiARN := arn.Build("appsync", b.region, b.accountID, "apis/"+apiID)

	graphqlEndpoint := fmt.Sprintf("%s/v1/apis/%s/graphql", b.endpoint, apiID)

	api := &GraphqlAPI{
		APIID:              apiID,
		ARN:                apiARN,
		Name:               name,
		AuthenticationType: authType,
		Region:             b.region,
		URIs: map[string]string{
			"GRAPHQL":  graphqlEndpoint,
			"REALTIME": graphqlEndpoint,
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

// ListGraphqlAPIs returns all GraphQL APIs.
func (b *InMemoryBackend) ListGraphqlAPIs() ([]*GraphqlAPI, error) {
	b.mu.RLock("ListGraphqlApis")
	defer b.mu.RUnlock()

	out := make([]*GraphqlAPI, 0, len(b.apis))
	for _, api := range b.apis {
		cp := *api
		out = append(out, &cp)
	}

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

	dss, ok := b.datasources[apiID]
	if !ok {
		return nil, fmt.Errorf("%w: datasource %s not found", ErrNotFound, name)
	}

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

	return out, nil
}

// DeleteDataSource deletes a data source.
func (b *InMemoryBackend) DeleteDataSource(apiID, name string) error {
	b.mu.Lock("DeleteDataSource")
	defer b.mu.Unlock()

	dss, ok := b.datasources[apiID]
	if !ok || dss[name] == nil {
		return fmt.Errorf("%w: datasource %s not found", ErrNotFound, name)
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
		r.Kind = "UNIT"
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
	out := make([]*Resolver, 0)

	prefix := typeName + "."
	for key, r := range res {
		if strings.HasPrefix(key, prefix) {
			cp := *r
			out = append(out, &cp)
		}
	}

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

	// Default expiry to 365 days from now if not specified.
	if expires <= 0 {
		expires = time.Now().AddDate(0, 0, defaultAPIKeyExpiryDays).Unix()
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

	if b.functions[apiID] == nil {
		b.functions[apiID] = make(map[string]*Function)
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
	if domain == "" || len(domain) > 253 {
		return false
	}

	// Must contain at least one dot and no leading/trailing dots.
	if domain[0] == '.' || domain[len(domain)-1] == '.' {
		return false
	}

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
func (b *InMemoryBackend) CreateAPI(name string, tagMap map[string]string) (*API, error) {
	b.mu.Lock("CreateAPI")
	defer b.mu.Unlock()

	apiID := randomAPIID()
	apiARN := arn.Build("appsync", b.region, b.accountID, "apis/"+apiID)

	api := &API{
		APIID:   apiID,
		ARN:     apiARN,
		Name:    name,
		Tags:    tagMap,
		DNSHTTP: fmt.Sprintf("%s.appsync-api.%s.amazonaws.com", apiID, b.region),
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

	ns := &ChannelNamespace{
		APIID:               apiID,
		Name:                name,
		Tags:                tagMap,
		ChannelNamespaceARN: nsARN,
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
