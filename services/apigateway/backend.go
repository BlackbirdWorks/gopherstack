package apigateway

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	ErrRestAPINotFound                     = errors.New("NotFoundException")
	ErrResourceNotFound                    = errors.New("NotFoundException")
	ErrMethodNotFound                      = errors.New("NotFoundException")
	ErrMethodResponseNotFound              = errors.New("NotFoundException")
	ErrIntegrationResponseNotFound         = errors.New("NotFoundException")
	ErrDeploymentNotFound                  = errors.New("NotFoundException")
	ErrAuthorizerNotFound                  = errors.New("NotFoundException")
	ErrValidatorNotFound                   = errors.New("NotFoundException")
	ErrAPIKeyNotFound                      = errors.New("NotFoundException")
	ErrBasePathMappingNotFound             = errors.New("NotFoundException")
	ErrDocumentationPartNotFound           = errors.New("NotFoundException")
	ErrDocumentationVersionNotFound        = errors.New("NotFoundException")
	ErrDomainNameNotFound                  = errors.New("NotFoundException")
	ErrDomainNameAccessAssociationNotFound = errors.New("NotFoundException")
	ErrModelNotFound                       = errors.New("NotFoundException")
	ErrUsagePlanNotFound                   = errors.New("NotFoundException")
	ErrUsagePlanKeyNotFound                = errors.New("NotFoundException")
	ErrStageNotFound                       = errors.New("NotFoundException")
	ErrNotFound                            = errors.New("NotFoundException")
	ErrAlreadyExists                       = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	ErrInvalidParameter                    = errors.New("BadRequestException")
)

// StorageBackend is the interface for the API Gateway in-memory store.
type StorageBackend interface {
	// REST APIs
	CreateRestAPI(input CreateRestAPIInput) (*RestAPI, error)
	DeleteRestAPI(restAPIID string) error
	GetRestAPI(restAPIID string) (*RestAPI, error)
	GetRestAPIs(limit int, position string) ([]RestAPI, string, error)
	UpdateRestAPI(restAPIID string, input UpdateRestAPIInput) (*RestAPI, error)

	// Resources
	GetResources(restAPIID, position string, limit int) ([]Resource, string, error)
	GetResource(restAPIID, resourceID string) (*Resource, error)
	CreateResource(restAPIID, parentID, pathPart string) (*Resource, error)
	DeleteResource(restAPIID, resourceID string) error
	UpdateResource(restAPIID, resourceID string, input UpdateResourceInput) (*Resource, error)

	// Methods
	PutMethod(input PutMethodInput) (*Method, error)
	GetMethod(restAPIID, resourceID, httpMethod string) (*Method, error)
	DeleteMethod(restAPIID, resourceID, httpMethod string) error

	// Method Responses
	PutMethodResponse(
		restAPIID, resourceID, httpMethod, statusCode string,
		input PutMethodResponseInput,
	) (*MethodResponse, error)
	GetMethodResponse(restAPIID, resourceID, httpMethod, statusCode string) (*MethodResponse, error)
	DeleteMethodResponse(restAPIID, resourceID, httpMethod, statusCode string) error

	// Integrations
	PutIntegration(restAPIID, resourceID, httpMethod string, input PutIntegrationInput) (*Integration, error)
	GetIntegration(restAPIID, resourceID, httpMethod string) (*Integration, error)
	DeleteIntegration(restAPIID, resourceID, httpMethod string) error

	// Integration Responses
	PutIntegrationResponse(
		restAPIID, resourceID, httpMethod, statusCode string,
		input PutIntegrationResponseInput,
	) (*IntegrationResponse, error)
	GetIntegrationResponse(restAPIID, resourceID, httpMethod, statusCode string) (*IntegrationResponse, error)
	DeleteIntegrationResponse(restAPIID, resourceID, httpMethod, statusCode string) error

	// Deployments
	CreateDeployment(restAPIID, stageName, description string) (*Deployment, error)
	GetDeployment(restAPIID, deploymentID string) (*Deployment, error)
	GetDeployments(restAPIID string) ([]Deployment, error)
	DeleteDeployment(restAPIID, deploymentID string) error
	UpdateDeployment(restAPIID, deploymentID string, input UpdateDeploymentInput) (*Deployment, error)

	// Stages
	GetStages(restAPIID string) ([]Stage, error)
	GetStage(restAPIID, stageName string) (*Stage, error)
	DeleteStage(restAPIID, stageName string) error

	// Authorizers
	CreateAuthorizer(restAPIID string, input CreateAuthorizerInput) (*Authorizer, error)
	GetAuthorizer(restAPIID, authorizerID string) (*Authorizer, error)
	GetAuthorizers(restAPIID string) ([]Authorizer, error)
	UpdateAuthorizer(restAPIID, authorizerID string, input UpdateAuthorizerInput) (*Authorizer, error)
	DeleteAuthorizer(restAPIID, authorizerID string) error

	// Request Validators
	CreateRequestValidator(restAPIID string, input CreateRequestValidatorInput) (*RequestValidator, error)
	GetRequestValidator(restAPIID, validatorID string) (*RequestValidator, error)
	GetRequestValidators(restAPIID string) ([]RequestValidator, error)
	UpdateRequestValidator(restAPIID, validatorID string, input UpdateRequestValidatorInput) (*RequestValidator, error)
	DeleteRequestValidator(restAPIID, validatorID string) error

	// API Keys
	CreateAPIKey(input CreateAPIKeyInput) (*APIKey, error)
	GetAPIKey(id string) (*APIKey, error)
	GetAPIKeyByValue(value string) (*APIKey, error)
	GetAPIKeys() ([]APIKey, error)
	GetAPIKeysPage(limit int, position string) ([]APIKey, string, error)
	DeleteAPIKey(id string) error
	UpdateAPIKey(id string, input UpdateAPIKeyInput) (*APIKey, error)

	// Base Path Mappings
	CreateBasePathMapping(input CreateBasePathMappingInput) (*BasePathMapping, error)
	GetBasePathMapping(domainName, basePath string) (*BasePathMapping, error)
	GetBasePathMappings(domainName string) ([]BasePathMapping, error)
	DeleteBasePathMapping(domainName, basePath string) error

	// Documentation Parts (per-API)
	CreateDocumentationPart(input CreateDocumentationPartInput) (*DocumentationPart, error)
	GetDocumentationPart(restAPIID, docPartID string) (*DocumentationPart, error)
	GetDocumentationParts(restAPIID string) ([]DocumentationPart, error)
	DeleteDocumentationPart(restAPIID, docPartID string) error

	// Documentation Versions (per-API)
	CreateDocumentationVersion(input CreateDocumentationVersionInput) (*DocumentationVersion, error)
	GetDocumentationVersion(restAPIID, version string) (*DocumentationVersion, error)
	GetDocumentationVersions(restAPIID string) ([]DocumentationVersion, error)
	DeleteDocumentationVersion(restAPIID, version string) error

	// Domain Names
	CreateDomainName(input CreateDomainNameInput) (*DomainName, error)
	GetDomainName(name string) (*DomainName, error)
	GetDomainNames() ([]DomainName, error)
	GetDomainNamesPage(limit int, position string) ([]DomainName, string, error)
	DeleteDomainName(name string) error

	// Domain Name Access Associations
	CreateDomainNameAccessAssociation(
		input CreateDomainNameAccessAssociationInput,
	) (*DomainNameAccessAssociation, error)

	// Models (per-API)
	CreateModel(input CreateModelInput) (*Model, error)
	GetModel(restAPIID, modelName string) (*Model, error)
	GetModels(restAPIID string) ([]Model, error)
	DeleteModel(restAPIID, modelName string) error
	UpdateModel(restAPIID, modelName string, input UpdateModelInput) (*Model, error)

	// Standalone Stage creation
	CreateStage(input CreateStageInput) (*Stage, error)
	UpdateStage(restAPIID, stageName string, input UpdateStageInput) (*Stage, error)

	// Usage Plans
	CreateUsagePlan(input CreateUsagePlanInput) (*UsagePlan, error)
	GetUsagePlan(id string) (*UsagePlan, error)
	GetUsagePlans() ([]UsagePlan, error)
	GetUsagePlansPage(limit int, position string) ([]UsagePlan, string, error)
	DeleteUsagePlan(id string) error

	// Usage Plan Keys
	CreateUsagePlanKey(input CreateUsagePlanKeyInput) (*UsagePlanKey, error)
	GetUsagePlanKey(usagePlanID, keyID string) (*UsagePlanKey, error)
	GetUsagePlanKeys(usagePlanID string) ([]UsagePlanKey, error)
	DeleteUsagePlanKey(usagePlanID, keyID string) error

	// Account
	GetAccount() (*Account, error)

	// Tags
	GetResourceTags(resourceARN string) (map[string]string, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error

	// Test Invocation
	TestInvokeMethod(input TestInvokeMethodInput) (*TestInvokeMethodOutput, error)

	// Update operations.
	UpdateUsagePlan(input UpdateUsagePlanInput) (*UsagePlan, error)
	UpdateDomainName(input UpdateDomainNameInput) (*DomainName, error)
	UpdateBasePathMapping(input UpdateBasePathMappingInput) (*BasePathMapping, error)
	UpdateDocumentationPart(input UpdateDocumentationPartInput) (*DocumentationPart, error)
	UpdateDocumentationVersion(input UpdateDocumentationVersionInput) (*DocumentationVersion, error)
	UpdateMethod(input UpdateMethodInput) (*Method, error)
	UpdateIntegration(input UpdateIntegrationInput) (*Integration, error)
	UpdateIntegrationResponse(input UpdateIntegrationResponseInput) (*IntegrationResponse, error)
	UpdateMethodResponse(input UpdateMethodResponseInput) (*MethodResponse, error)
	UpdateAccount(input UpdateAccountInput) (*Account, error)
	TestInvokeAuthorizer(input TestInvokeAuthorizerInput) (*TestInvokeAuthorizerOutput, error)
	GetModelTemplate(restAPIID, modelName string) (string, error)

	// Gateway response operations.
	GetGatewayResponse(restAPIID, responseType string) (*GatewayResponse, error)
	GetGatewayResponses(restAPIID string) ([]GatewayResponse, error)
	PutGatewayResponse(input PutGatewayResponseInput) (*GatewayResponse, error)
	DeleteGatewayResponse(restAPIID, responseType string) error

	// Client certificate operations.
	GenerateClientCertificate(input GenerateClientCertificateInput) (*ClientCertificate, error)
	GetClientCertificate(id string) (*ClientCertificate, error)
	GetClientCertificates() ([]ClientCertificate, error)
	DeleteClientCertificate(id string) error

	// Usage operations.
	GetUsage(input GetUsageInput) (*UsageData, error)

	// VPC Link operations.
	CreateVpcLink(input CreateVpcLinkInput) (*VpcLink, error)
	GetVpcLink(id string) (*VpcLink, error)
	GetVpcLinks() ([]VpcLink, error)
	DeleteVpcLink(id string) error
	UpdateVpcLink(input UpdateVpcLinkInput) (*VpcLink, error)

	// Client certificate update.
	UpdateClientCertificate(input UpdateClientCertificateInput) (*ClientCertificate, error)

	// OpenAPI export.
	GetExport(restAPIID, stageName, exportType string) (map[string]any, error)
}

const apiIDChars = "abcdefghijklmnopqrstuvwxyz0123456789"

const (
	apiIDLength       = 10
	resourceIDLength  = 6
	apiKeyValueLength = 40 // AWS generates a 40-character alphanumeric key value

	// defaultBurstLimit and defaultRateLimit match AWS API Gateway defaults.
	defaultBurstLimit = 5000
	defaultRateLimit  = 10000.0

	// arnSplitParts is used when splitting ARNs at a specific substring.
	arnSplitParts = 2

	// defaultPageSize is used when no limit is specified in paginated list operations.
	defaultPageSize = 500

	// clientCertValidityDays is the number of days a generated client certificate is valid.
	// AWS issues certificates with a 2-year validity period.
	clientCertValidityDays = 730
)

// contentTypeJSON is the standard JSON content type used in integration templates and responses.
const contentTypeJSON = "application/json"

// Constants for OpenAPI export document construction.
const (
	exportKeyAPIKey      = "api_key"
	exportKeyType        = "type"
	exportKeyDescription = "description"
	exportKeySchema      = "schema"
	exportKeyObject      = "object"
	exportKeyBody        = "body"
)

// stageInvokeURL returns the gopherstack proxy path for a deployed stage.
// The full URL is relative — clients prepend their gopherstack base URL.
func stageInvokeURL(restAPIID, stageName string) string {
	return "/proxy/" + restAPIID + "/" + stageName
}

// paginatePage applies limit/position-based cursor pagination to a pre-sorted slice.
// It returns the page slice and the next position cursor (empty string if last page).
func paginatePage[T any](all []T, limit int, position string) ([]T, string) {
	startIdx := parsePosition(position)
	if startIdx >= len(all) {
		return []T{}, ""
	}

	if limit <= 0 {
		limit = defaultPageSize
	}
	end := startIdx + limit

	var outPosition string
	if end < len(all) {
		outPosition = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outPosition
}

// randomID generates a cryptographically random alphanumeric ID of the given length.
func randomID(length int) string {
	b := make([]byte, length)
	charCount := uint64(len(apiIDChars))

	for i := range b {
		var v [8]byte
		_, _ = rand.Read(v[:])
		b[i] = apiIDChars[binary.BigEndian.Uint64(v[:])%charCount]
	}

	return string(b)
}

// initTagsFromInput returns a new tags.Tags store seeded from inputTags (if non-nil)
// or an empty store, using the given name prefix for the backing store label.
func initTagsFromInput(name string, inputTags *tags.Tags) *tags.Tags {
	if inputTags == nil {
		return tags.New(name)
	}

	return tags.FromMap(name, inputTags.Clone())
}

// apiData holds per-REST-API state.
type apiData struct {
	resources             map[string]*Resource
	deployments           map[string]*Deployment
	stages                map[string]*Stage
	authorizers           map[string]*Authorizer
	requestValidators     map[string]*RequestValidator
	documentationParts    map[string]*DocumentationPart
	documentationVersions map[string]*DocumentationVersion
	models                map[string]*Model
	api                   RestAPI
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	account                      *Account
	apis                         map[string]*apiData
	apiKeys                      map[string]*APIKey
	basePathMappings             map[string]*BasePathMapping // key: domainName + "#" + basePath
	domainNames                  map[string]*DomainName
	domainNameAccessAssociations map[string]*DomainNameAccessAssociation
	usagePlans                   map[string]*UsagePlan
	usagePlanKeys                map[string]map[string]*UsagePlanKey // usagePlanID → keyID → key
	gatewayResponses             map[string]*GatewayResponse         // key: restAPIID + "#" + responseType
	clientCertificates           map[string]*ClientCertificate       // key: clientCertificateID
	vpcLinks                     map[string]*VpcLink
	mu                           *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		account: &Account{
			ThrottleSettings: &ThrottleSettings{
				BurstLimit: defaultBurstLimit,
				RateLimit:  defaultRateLimit,
			},
			Features:      []string{"UsagePlans"},
			APIKeyVersion: "1",
		},
		apis:                         make(map[string]*apiData),
		apiKeys:                      make(map[string]*APIKey),
		basePathMappings:             make(map[string]*BasePathMapping),
		domainNames:                  make(map[string]*DomainName),
		domainNameAccessAssociations: make(map[string]*DomainNameAccessAssociation),
		usagePlans:                   make(map[string]*UsagePlan),
		usagePlanKeys:                make(map[string]map[string]*UsagePlanKey),
		gatewayResponses:             make(map[string]*GatewayResponse),
		clientCertificates:           make(map[string]*ClientCertificate),
		vpcLinks:                     make(map[string]*VpcLink),
		mu:                           lockmetrics.New("apigateway"),
	}
}

// CreateRestAPI creates a new REST API and its root resource.
func (b *InMemoryBackend) CreateRestAPI(input CreateRestAPIInput) (*RestAPI, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateRestAPI")
	defer b.mu.Unlock()

	id := randomID(apiIDLength)
	backendTags := initTagsFromInput("apigw.api."+id+".tags", input.Tags)
	rootID := randomID(resourceIDLength)

	api := RestAPI{
		ID:                     id,
		Name:                   input.Name,
		Description:            input.Description,
		CreatedDate:            unixEpochTime{time.Now()},
		Tags:                   backendTags,
		RootResourceID:         rootID,
		BinaryMediaTypes:       input.BinaryMediaTypes,
		EndpointConfiguration:  input.EndpointConfiguration,
		Policy:                 input.Policy,
		APIKeySource:           input.APIKeySource,
		MinimumCompressionSize: input.MinimumCompressionSize,
	}

	root := &Resource{
		ID:              rootID,
		ParentID:        "",
		PathPart:        "",
		Path:            "/",
		RestAPIID:       id,
		ResourceMethods: make(map[string]*Method),
	}

	b.apis[id] = &apiData{
		api:                   api,
		resources:             map[string]*Resource{rootID: root},
		deployments:           make(map[string]*Deployment),
		stages:                make(map[string]*Stage),
		authorizers:           make(map[string]*Authorizer),
		requestValidators:     make(map[string]*RequestValidator),
		documentationParts:    make(map[string]*DocumentationPart),
		documentationVersions: make(map[string]*DocumentationVersion),
		models:                make(map[string]*Model),
	}

	return &api, nil
}

// DeleteRestAPI removes a REST API and all its resources.
func (b *InMemoryBackend) DeleteRestAPI(restAPIID string) error {
	b.mu.Lock("DeleteRestAPI")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	d.api.Tags.Close()
	delete(b.apis, restAPIID)

	return nil
}

// GetRestAPI returns a single REST API.
func (b *InMemoryBackend) GetRestAPI(restAPIID string) (*RestAPI, error) {
	b.mu.RLock("GetRestAPI")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	cp := d.api

	return &cp, nil
}

// GetRestAPIs returns all REST APIs with pagination.
func (b *InMemoryBackend) GetRestAPIs(limit int, position string) ([]RestAPI, string, error) {
	b.mu.RLock("GetRestAPIs")
	defer b.mu.RUnlock()

	all := make([]RestAPI, 0, len(b.apis))
	for _, d := range b.apis {
		all = append(all, d.api)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	startIdx := parsePosition(position)
	if startIdx >= len(all) {
		return []RestAPI{}, "", nil
	}

	if limit <= 0 {
		limit = 500
	}
	end := startIdx + limit
	var outPosition string
	if end < len(all) {
		outPosition = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outPosition, nil
}

// GetResources returns all resources for a REST API with pagination.
func (b *InMemoryBackend) GetResources(restAPIID, position string, limit int) ([]Resource, string, error) {
	b.mu.RLock("GetResources")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, "", fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	all := make([]Resource, 0, len(d.resources))
	for _, r := range d.resources {
		all = append(all, *r)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	startIdx := parsePosition(position)
	if startIdx >= len(all) {
		return []Resource{}, "", nil
	}

	if limit <= 0 {
		limit = 500
	}
	end := startIdx + limit
	var outPosition string
	if end < len(all) {
		outPosition = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outPosition, nil
}

// GetResource returns a single resource.
func (b *InMemoryBackend) GetResource(restAPIID, resourceID string) (*Resource, error) {
	b.mu.RLock("GetResource")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	cp := *r

	return &cp, nil
}

// CreateResource creates a new resource under a parent.
func (b *InMemoryBackend) CreateResource(restAPIID, parentID, pathPart string) (*Resource, error) {
	if pathPart == "" {
		return nil, fmt.Errorf("%w: pathPart is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateResource")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	parent, ok := d.resources[parentID]
	if !ok {
		return nil, fmt.Errorf("%w: parent resource %s not found", ErrResourceNotFound, parentID)
	}

	path := computePath(parent.Path, pathPart)

	id := randomID(resourceIDLength)
	res := &Resource{
		ID:              id,
		ParentID:        parentID,
		PathPart:        pathPart,
		Path:            path,
		RestAPIID:       restAPIID,
		ResourceMethods: make(map[string]*Method),
	}
	d.resources[id] = res

	cp := *res

	return &cp, nil
}

// DeleteResource removes a resource.
func (b *InMemoryBackend) DeleteResource(restAPIID, resourceID string) error {
	b.mu.Lock("DeleteResource")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	if _, exists := d.resources[resourceID]; !exists {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	delete(d.resources, resourceID)

	return nil
}

// PutMethod creates or replaces a method on a resource.
func (b *InMemoryBackend) PutMethod(input PutMethodInput) (*Method, error) {
	b.mu.Lock("PutMethod")
	defer b.mu.Unlock()

	d, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}
	r, ok := d.resources[input.ResourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, input.ResourceID)
	}

	reqParams := input.RequestParameters
	if reqParams == nil {
		reqParams = make(map[string]bool)
	}

	m := &Method{
		HTTPMethod:         input.HTTPMethod,
		AuthorizationType:  input.AuthorizationType,
		AuthorizerID:       input.AuthorizerID,
		RequestValidatorID: input.RequestValidatorID,
		APIKeyRequired:     input.APIKeyRequired,
		RequestParameters:  reqParams,
		RequestModels:      input.RequestModels,
		MethodResponses:    make(map[string]*MethodResponse),
		OperationName:      input.OperationName,
	}
	r.ResourceMethods[input.HTTPMethod] = m

	cp := *m

	return &cp, nil
}

// GetMethod retrieves a method on a resource.
func (b *InMemoryBackend) GetMethod(restAPIID, resourceID, httpMethod string) (*Method, error) {
	b.mu.RLock("GetMethod")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	cp := *m

	return &cp, nil
}

// DeleteMethod removes a method from a resource.
func (b *InMemoryBackend) DeleteMethod(restAPIID, resourceID, httpMethod string) error {
	b.mu.Lock("DeleteMethod")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	if _, exists := r.ResourceMethods[httpMethod]; !exists {
		return fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	delete(r.ResourceMethods, httpMethod)

	return nil
}

// PutIntegration creates or replaces an integration on a method.
func (b *InMemoryBackend) PutIntegration(
	restAPIID, resourceID, httpMethod string,
	input PutIntegrationInput,
) (*Integration, error) {
	b.mu.Lock("PutIntegration")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}

	integ := &Integration{
		Type:                 input.Type,
		HTTPMethod:           input.HTTPMethod,
		URI:                  input.URI,
		PassthroughBehavior:  input.PassthroughBehavior,
		RequestTemplates:     input.RequestTemplates,
		RequestParameters:    input.RequestParameters,
		CacheKeyParameters:   input.CacheKeyParameters,
		ConnectionType:       input.ConnectionType,
		ConnectionId:         input.ConnectionId,
		ContentHandling:      input.ContentHandling,
		Credentials:          input.Credentials,
		CacheNamespace:       input.CacheNamespace,
		TimeoutInMillis:      input.TimeoutInMillis,
		IntegrationResponses: make(map[string]*IntegrationResponse),
	}
	m.MethodIntegration = integ

	cp := *integ

	return &cp, nil
}

// GetIntegration retrieves the integration for a method.
func (b *InMemoryBackend) GetIntegration(restAPIID, resourceID, httpMethod string) (*Integration, error) {
	b.mu.RLock("GetIntegration")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	if m.MethodIntegration == nil {
		return nil, fmt.Errorf("%w: integration not found for method %s", ErrMethodNotFound, httpMethod)
	}
	cp := *m.MethodIntegration

	return &cp, nil
}

// DeleteIntegration removes the integration from a method.
func (b *InMemoryBackend) DeleteIntegration(restAPIID, resourceID, httpMethod string) error {
	b.mu.Lock("DeleteIntegration")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	if m.MethodIntegration == nil {
		return fmt.Errorf("%w: integration not found for method %s", ErrMethodNotFound, httpMethod)
	}
	m.MethodIntegration = nil

	return nil
}

// PutMethodResponse creates or replaces a method response on a method.
func (b *InMemoryBackend) PutMethodResponse(
	restAPIID, resourceID, httpMethod, statusCode string,
	input PutMethodResponseInput,
) (*MethodResponse, error) {
	b.mu.Lock("PutMethodResponse")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}

	mr := &MethodResponse{
		StatusCode:         statusCode,
		ResponseModels:     input.ResponseModels,
		ResponseParameters: input.ResponseParameters,
	}
	if m.MethodResponses == nil {
		m.MethodResponses = make(map[string]*MethodResponse)
	}
	m.MethodResponses[statusCode] = mr

	cp := *mr

	return &cp, nil
}

// GetMethodResponse retrieves a method response for a given status code.
func (b *InMemoryBackend) GetMethodResponse(
	restAPIID, resourceID, httpMethod, statusCode string,
) (*MethodResponse, error) {
	b.mu.RLock("GetMethodResponse")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	mr, ok := m.MethodResponses[statusCode]
	if !ok {
		return nil, fmt.Errorf("%w: method response %s not found", ErrMethodResponseNotFound, statusCode)
	}
	cp := *mr

	return &cp, nil
}

// DeleteMethodResponse removes a method response from a method.
func (b *InMemoryBackend) DeleteMethodResponse(restAPIID, resourceID, httpMethod, statusCode string) error {
	b.mu.Lock("DeleteMethodResponse")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	if _, exists := m.MethodResponses[statusCode]; !exists {
		return fmt.Errorf("%w: method response %s not found", ErrMethodResponseNotFound, statusCode)
	}
	delete(m.MethodResponses, statusCode)

	return nil
}

// PutIntegrationResponse creates or replaces an integration response.
func (b *InMemoryBackend) PutIntegrationResponse(
	restAPIID, resourceID, httpMethod, statusCode string,
	input PutIntegrationResponseInput,
) (*IntegrationResponse, error) {
	b.mu.Lock("PutIntegrationResponse")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	if m.MethodIntegration == nil {
		return nil, fmt.Errorf("%w: integration not found for method %s", ErrMethodNotFound, httpMethod)
	}

	ir := &IntegrationResponse{
		StatusCode:         statusCode,
		ResponseTemplates:  input.ResponseTemplates,
		ResponseParameters: input.ResponseParameters,
		SelectionPattern:   input.SelectionPattern,
		ContentHandling:    input.ContentHandling,
	}
	if m.MethodIntegration.IntegrationResponses == nil {
		m.MethodIntegration.IntegrationResponses = make(map[string]*IntegrationResponse)
	}
	m.MethodIntegration.IntegrationResponses[statusCode] = ir

	cp := *ir

	return &cp, nil
}

// GetIntegrationResponse retrieves an integration response for a given status code.
func (b *InMemoryBackend) GetIntegrationResponse(
	restAPIID, resourceID, httpMethod, statusCode string,
) (*IntegrationResponse, error) {
	b.mu.RLock("GetIntegrationResponse")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	if m.MethodIntegration == nil {
		return nil, fmt.Errorf("%w: integration not found for method %s", ErrMethodNotFound, httpMethod)
	}
	ir, ok := m.MethodIntegration.IntegrationResponses[statusCode]
	if !ok {
		return nil, fmt.Errorf("%w: integration response %s not found", ErrIntegrationResponseNotFound, statusCode)
	}
	cp := *ir

	return &cp, nil
}

// DeleteIntegrationResponse removes an integration response from a method integration.
func (b *InMemoryBackend) DeleteIntegrationResponse(restAPIID, resourceID, httpMethod, statusCode string) error {
	b.mu.Lock("DeleteIntegrationResponse")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	if m.MethodIntegration == nil {
		return fmt.Errorf("%w: integration not found for method %s", ErrMethodNotFound, httpMethod)
	}
	if _, exists := m.MethodIntegration.IntegrationResponses[statusCode]; !exists {
		return fmt.Errorf("%w: integration response %s not found", ErrIntegrationResponseNotFound, statusCode)
	}
	delete(m.MethodIntegration.IntegrationResponses, statusCode)

	return nil
}

// CreateDeployment creates a deployment and associated stage.
func (b *InMemoryBackend) CreateDeployment(restAPIID, stageName, description string) (*Deployment, error) {
	b.mu.Lock("CreateDeployment")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	now := unixEpochTime{time.Now()}
	deplID := randomID(apiIDLength)
	depl := &Deployment{
		ID:          deplID,
		RestAPIID:   restAPIID,
		Description: description,
		CreatedDate: now,
	}
	d.deployments[deplID] = depl

	if stageName != "" {
		stage := &Stage{
			StageName:       stageName,
			RestAPIID:       restAPIID,
			DeploymentID:    deplID,
			Description:     description,
			CreatedDate:     now,
			LastUpdatedDate: now,
			Variables:       make(map[string]string),
		}
		d.stages[stageName] = stage
	}

	cp := *depl

	return &cp, nil
}

// GetDeployments returns all deployments for a REST API.
func (b *InMemoryBackend) GetDeployments(restAPIID string) ([]Deployment, error) {
	b.mu.RLock("GetDeployments")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	all := make([]Deployment, 0, len(d.deployments))
	for _, dep := range d.deployments {
		all = append(all, *dep)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// GetDeployment returns a single deployment by ID.
func (b *InMemoryBackend) GetDeployment(restAPIID, deploymentID string) (*Deployment, error) {
	b.mu.RLock("GetDeployment")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	dep, ok := d.deployments[deploymentID]
	if !ok {
		return nil, fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	cp := *dep

	return &cp, nil
}

// DeleteDeployment removes a deployment from a REST API.
func (b *InMemoryBackend) DeleteDeployment(restAPIID, deploymentID string) error {
	b.mu.Lock("DeleteDeployment")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	_, exists := d.deployments[deploymentID]
	if !exists {
		return fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	delete(d.deployments, deploymentID)

	return nil
}

// GetStages returns all stages for a REST API.
func (b *InMemoryBackend) GetStages(restAPIID string) ([]Stage, error) {
	b.mu.RLock("GetStages")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	all := make([]Stage, 0, len(d.stages))
	for _, s := range d.stages {
		cp := *s
		cp.InvokeURL = stageInvokeURL(restAPIID, s.StageName)
		all = append(all, cp)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].StageName < all[j].StageName })

	return all, nil
}

// GetStage returns a single stage.
func (b *InMemoryBackend) GetStage(restAPIID, stageName string) (*Stage, error) {
	b.mu.RLock("GetStage")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	s, stageOK := d.stages[stageName]
	if !stageOK {
		return nil, fmt.Errorf("%w: stage %s not found", ErrResourceNotFound, stageName)
	}
	cp := *s
	cp.InvokeURL = stageInvokeURL(restAPIID, stageName)

	return &cp, nil
}

// DeleteStage removes a stage.
func (b *InMemoryBackend) DeleteStage(restAPIID, stageName string) error {
	b.mu.Lock("DeleteStage")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	if _, stageOK := d.stages[stageName]; !stageOK {
		return fmt.Errorf("%w: stage %s not found", ErrResourceNotFound, stageName)
	}
	delete(d.stages, stageName)

	return nil
}

// CreateAuthorizer creates a new authorizer for a REST API.
func (b *InMemoryBackend) CreateAuthorizer(restAPIID string, input CreateAuthorizerInput) (*Authorizer, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}
	if input.Type == "" {
		return nil, fmt.Errorf("%w: type is required", ErrInvalidParameter)
	}

	// AWS rejects AuthorizerResultTtlInSeconds outside [0, 3600].
	const maxAuthorizerTTL = 3600
	if input.AuthorizerResultTTLInSeconds < 0 || input.AuthorizerResultTTLInSeconds > maxAuthorizerTTL {
		return nil, fmt.Errorf("%w: authorizerResultTtlInSeconds must be in [0, %d]",
			ErrInvalidParameter, maxAuthorizerTTL)
	}

	b.mu.Lock("CreateAuthorizer")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	id := randomID(resourceIDLength)
	auth := &Authorizer{
		ID:                           id,
		Name:                         input.Name,
		Type:                         input.Type,
		AuthorizerURI:                input.AuthorizerURI,
		AuthorizerCredentials:        input.AuthorizerCredentials,
		IdentitySource:               input.IdentitySource,
		IdentityValidationExpression: input.IdentityValidationExpression,
		AuthorizerResultTTLInSeconds: input.AuthorizerResultTTLInSeconds,
		ProviderARNs:                 input.ProviderARNs,
	}
	d.authorizers[id] = auth

	cp := *auth

	return &cp, nil
}

// GetAuthorizer retrieves an authorizer by ID.
func (b *InMemoryBackend) GetAuthorizer(restAPIID, authorizerID string) (*Authorizer, error) {
	b.mu.RLock("GetAuthorizer")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	auth, ok := d.authorizers[authorizerID]
	if !ok {
		return nil, fmt.Errorf("%w: authorizer %s not found", ErrAuthorizerNotFound, authorizerID)
	}
	cp := *auth

	return &cp, nil
}

// GetAuthorizers returns all authorizers for a REST API.
func (b *InMemoryBackend) GetAuthorizers(restAPIID string) ([]Authorizer, error) {
	b.mu.RLock("GetAuthorizers")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	all := make([]Authorizer, 0, len(d.authorizers))
	for _, auth := range d.authorizers {
		all = append(all, *auth)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// UpdateAuthorizer updates fields on an existing authorizer.
func (b *InMemoryBackend) UpdateAuthorizer(
	restAPIID, authorizerID string,
	input UpdateAuthorizerInput,
) (*Authorizer, error) {
	b.mu.Lock("UpdateAuthorizer")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	auth, ok := d.authorizers[authorizerID]
	if !ok {
		return nil, fmt.Errorf("%w: authorizer %s not found", ErrAuthorizerNotFound, authorizerID)
	}

	if input.Name != "" {
		auth.Name = input.Name
	}
	if input.Type != "" {
		auth.Type = input.Type
	}
	if input.AuthorizerURI != "" {
		auth.AuthorizerURI = input.AuthorizerURI
	}
	if input.AuthorizerCredentials != "" {
		auth.AuthorizerCredentials = input.AuthorizerCredentials
	}
	if input.IdentitySource != "" {
		auth.IdentitySource = input.IdentitySource
	}
	if input.IdentityValidationExpression != "" {
		auth.IdentityValidationExpression = input.IdentityValidationExpression
	}
	if input.AuthorizerResultTTLInSeconds != 0 {
		const maxAuthorizerTTL = 3600
		if input.AuthorizerResultTTLInSeconds < 0 || input.AuthorizerResultTTLInSeconds > maxAuthorizerTTL {
			return nil, fmt.Errorf("%w: authorizerResultTtlInSeconds must be in [0, %d]",
				ErrInvalidParameter, maxAuthorizerTTL)
		}
		auth.AuthorizerResultTTLInSeconds = input.AuthorizerResultTTLInSeconds
	}
	if len(input.ProviderARNs) > 0 {
		auth.ProviderARNs = input.ProviderARNs
	}

	cp := *auth

	return &cp, nil
}

// DeleteAuthorizer removes an authorizer from a REST API.
func (b *InMemoryBackend) DeleteAuthorizer(restAPIID, authorizerID string) error {
	b.mu.Lock("DeleteAuthorizer")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	if _, exists := d.authorizers[authorizerID]; !exists {
		return fmt.Errorf("%w: authorizer %s not found", ErrAuthorizerNotFound, authorizerID)
	}
	delete(d.authorizers, authorizerID)

	return nil
}

// CreateRequestValidator creates a new request validator for a REST API.
func (b *InMemoryBackend) CreateRequestValidator(
	restAPIID string,
	input CreateRequestValidatorInput,
) (*RequestValidator, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateRequestValidator")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	id := randomID(resourceIDLength)
	rv := &RequestValidator{
		ID:                        id,
		Name:                      input.Name,
		ValidateRequestBody:       input.ValidateRequestBody,
		ValidateRequestParameters: input.ValidateRequestParameters,
	}
	d.requestValidators[id] = rv

	cp := *rv

	return &cp, nil
}

// GetRequestValidator retrieves a request validator by ID.
func (b *InMemoryBackend) GetRequestValidator(restAPIID, validatorID string) (*RequestValidator, error) {
	b.mu.RLock("GetRequestValidator")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	rv, ok := d.requestValidators[validatorID]
	if !ok {
		return nil, fmt.Errorf("%w: request validator %s not found", ErrValidatorNotFound, validatorID)
	}
	cp := *rv

	return &cp, nil
}

// GetRequestValidators returns all request validators for a REST API.
func (b *InMemoryBackend) GetRequestValidators(restAPIID string) ([]RequestValidator, error) {
	b.mu.RLock("GetRequestValidators")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	all := make([]RequestValidator, 0, len(d.requestValidators))
	for _, rv := range d.requestValidators {
		all = append(all, *rv)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// UpdateRequestValidator updates fields on an existing request validator.
func (b *InMemoryBackend) UpdateRequestValidator(
	restAPIID, validatorID string,
	input UpdateRequestValidatorInput,
) (*RequestValidator, error) {
	b.mu.Lock("UpdateRequestValidator")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	rv, ok := d.requestValidators[validatorID]
	if !ok {
		return nil, fmt.Errorf("%w: request validator %s not found", ErrValidatorNotFound, validatorID)
	}

	if input.Name != "" {
		rv.Name = input.Name
	}
	if input.ValidateRequestBody != nil {
		rv.ValidateRequestBody = *input.ValidateRequestBody
	}
	if input.ValidateRequestParameters != nil {
		rv.ValidateRequestParameters = *input.ValidateRequestParameters
	}

	cp := *rv

	return &cp, nil
}

// DeleteRequestValidator removes a request validator from a REST API.
func (b *InMemoryBackend) DeleteRequestValidator(restAPIID, validatorID string) error {
	b.mu.Lock("DeleteRequestValidator")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	if _, exists := d.requestValidators[validatorID]; !exists {
		return fmt.Errorf("%w: request validator %s not found", ErrValidatorNotFound, validatorID)
	}
	delete(d.requestValidators, validatorID)

	return nil
}

func computePath(parentPath, pathPart string) string {
	if parentPath == "/" {
		return "/" + pathPart
	}

	return strings.TrimRight(parentPath, "/") + "/" + pathPart
}

func parsePosition(position string) int {
	if position == "" {
		return 0
	}
	idx, err := strconv.Atoi(position)
	if err != nil || idx < 0 {
		return 0
	}

	return idx
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Close all REST API tag stores to prevent resource leaks.
	for _, d := range b.apis {
		if d.api.Tags != nil {
			d.api.Tags.Close()
		}
	}

	for _, k := range b.apiKeys {
		if k.Tags != nil {
			k.Tags.Close()
		}
	}

	for _, dn := range b.domainNames {
		if dn.Tags != nil {
			dn.Tags.Close()
		}
	}

	for _, p := range b.usagePlans {
		if p.Tags != nil {
			p.Tags.Close()
		}
	}

	b.apis = make(map[string]*apiData)
	b.apiKeys = make(map[string]*APIKey)
	b.basePathMappings = make(map[string]*BasePathMapping)
	b.domainNames = make(map[string]*DomainName)
	b.domainNameAccessAssociations = make(map[string]*DomainNameAccessAssociation)
	b.usagePlans = make(map[string]*UsagePlan)
	b.usagePlanKeys = make(map[string]map[string]*UsagePlanKey)
	b.vpcLinks = make(map[string]*VpcLink)
	b.clientCertificates = make(map[string]*ClientCertificate)
	b.gatewayResponses = make(map[string]*GatewayResponse)
}

// CreateAPIKey creates a new API key with an optional auto-generated value.
func (b *InMemoryBackend) CreateAPIKey(input CreateAPIKeyInput) (*APIKey, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateAPIKey")
	defer b.mu.Unlock()

	for _, k := range b.apiKeys {
		if k.Name == input.Name {
			return nil, fmt.Errorf("%w: API key with name %q already exists", ErrAlreadyExists, input.Name)
		}
	}

	now := unixEpochTime{time.Now()}
	id := randomID(apiIDLength)

	backendTags := initTagsFromInput("apigw.apikey."+id+".tags", input.Tags)

	value := input.Value
	if value == "" {
		// AWS generates a 40-character alphanumeric key value when none is provided.
		value = randomID(apiKeyValueLength)
	}

	key := &APIKey{
		ID:              id,
		Name:            input.Name,
		Description:     input.Description,
		Value:           value,
		Enabled:         input.Enabled,
		Tags:            backendTags,
		CreatedDate:     now,
		LastUpdatedDate: now,
	}
	b.apiKeys[id] = key

	cp := *key

	return &cp, nil
}

// CreateBasePathMapping creates a new base path mapping for a domain name.
func (b *InMemoryBackend) CreateBasePathMapping(input CreateBasePathMappingInput) (*BasePathMapping, error) {
	if input.DomainName == "" {
		return nil, fmt.Errorf("%w: domainName is required", ErrInvalidParameter)
	}

	if input.RestAPIID == "" {
		return nil, fmt.Errorf("%w: restApiId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateBasePathMapping")
	defer b.mu.Unlock()

	mapKey := input.DomainName + "#" + input.BasePath
	if _, exists := b.basePathMappings[mapKey]; exists {
		return nil, fmt.Errorf("%w: base path mapping already exists for domain %q path %q",
			ErrAlreadyExists, input.DomainName, input.BasePath)
	}

	bpm := &BasePathMapping{
		DomainName: input.DomainName,
		BasePath:   input.BasePath,
		RestAPIID:  input.RestAPIID,
		Stage:      input.Stage,
	}
	b.basePathMappings[mapKey] = bpm

	cp := *bpm

	return &cp, nil
}

// CreateDocumentationPart creates a documentation part for a REST API.
func (b *InMemoryBackend) CreateDocumentationPart(input CreateDocumentationPartInput) (*DocumentationPart, error) {
	if input.RestAPIID == "" {
		return nil, fmt.Errorf("%w: restApiId is required", ErrInvalidParameter)
	}

	if input.Location.Type == "" {
		return nil, fmt.Errorf("%w: location.type is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDocumentationPart")
	defer b.mu.Unlock()

	d, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	id := randomID(resourceIDLength)
	part := &DocumentationPart{
		ID:         id,
		RestAPIID:  input.RestAPIID,
		Location:   input.Location,
		Properties: input.Properties,
	}
	d.documentationParts[id] = part

	cp := *part

	return &cp, nil
}

// CreateDocumentationVersion creates a documentation version snapshot for a REST API.
func (b *InMemoryBackend) CreateDocumentationVersion(
	input CreateDocumentationVersionInput,
) (*DocumentationVersion, error) {
	if input.RestAPIID == "" {
		return nil, fmt.Errorf("%w: restApiId is required", ErrInvalidParameter)
	}

	if input.Version == "" {
		return nil, fmt.Errorf("%w: documentationVersion is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDocumentationVersion")
	defer b.mu.Unlock()

	d, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	if _, exists := d.documentationVersions[input.Version]; exists {
		return nil, fmt.Errorf("%w: documentation version %q already exists", ErrAlreadyExists, input.Version)
	}

	ver := &DocumentationVersion{
		RestAPIID:   input.RestAPIID,
		Version:     input.Version,
		Description: input.Description,
		CreatedDate: unixEpochTime{time.Now()},
	}
	d.documentationVersions[input.Version] = ver

	cp := *ver

	return &cp, nil
}

// CreateDomainName creates a new custom domain name.
func (b *InMemoryBackend) CreateDomainName(input CreateDomainNameInput) (*DomainName, error) {
	if input.DomainName == "" {
		return nil, fmt.Errorf("%w: domainName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDomainName")
	defer b.mu.Unlock()

	if _, exists := b.domainNames[input.DomainName]; exists {
		return nil, fmt.Errorf("%w: domain name %q already exists", ErrAlreadyExists, input.DomainName)
	}

	now := unixEpochTime{time.Now()}
	backendTags := initTagsFromInput("apigw.domain."+input.DomainName+".tags", input.Tags)

	securityPolicy := input.SecurityPolicy
	if securityPolicy == "" {
		securityPolicy = "TLS_1_2"
	}

	endpointType := "REGIONAL"
	if input.EndpointConfiguration != nil && len(input.EndpointConfiguration.Types) > 0 {
		endpointType = input.EndpointConfiguration.Types[0]
	}

	var epConfig *EndpointConfiguration
	if input.EndpointConfiguration != nil {
		epConfig = input.EndpointConfiguration
	} else {
		epConfig = &EndpointConfiguration{Types: []string{endpointType}}
	}

	regionalDomain := input.DomainName + ".execute-api.us-east-1.amazonaws.com"
	distributionDomain := input.DomainName + ".cloudfront.net"

	dn := &DomainName{
		DomainNameValue:          input.DomainName,
		CertificateARN:           input.CertificateARN,
		RegionalCertificateARN:   input.RegionalCertificateARN,
		SecurityPolicy:           securityPolicy,
		EndpointConfiguration:    epConfig,
		RegionalDomainName:       regionalDomain,
		RegionalHostedZoneId:     "Z2FDTNDATAQYW2",
		DistributionDomainName:   distributionDomain,
		DistributionHostedZoneId: "Z2FDTNDATAQYW2",
		DomainNameStatus:         "AVAILABLE",
		Tags:                     backendTags,
		CreatedDate:              &now,
	}
	b.domainNames[input.DomainName] = dn

	cp := *dn

	return &cp, nil
}

// CreateDomainNameAccessAssociation creates an access association for a domain name.
func (b *InMemoryBackend) CreateDomainNameAccessAssociation(
	input CreateDomainNameAccessAssociationInput,
) (*DomainNameAccessAssociation, error) {
	if input.DomainNameARN == "" {
		return nil, fmt.Errorf("%w: domainNameArn is required", ErrInvalidParameter)
	}

	if input.AccessAssociationSource == "" {
		return nil, fmt.Errorf("%w: accessAssociationSource is required", ErrInvalidParameter)
	}

	if input.AccessAssociationSourceType == "" {
		return nil, fmt.Errorf("%w: accessAssociationSourceType is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateDomainNameAccessAssociation")
	defer b.mu.Unlock()

	assocARN := "arn:aws:apigateway:us-east-1::/accessassociations/" + randomID(apiIDLength)
	assoc := &DomainNameAccessAssociation{
		DomainNameAccessAssociationARN: assocARN,
		DomainNameARN:                  input.DomainNameARN,
		AccessAssociationSource:        input.AccessAssociationSource,
		AccessAssociationSourceType:    input.AccessAssociationSourceType,
	}
	b.domainNameAccessAssociations[assocARN] = assoc

	cp := *assoc

	return &cp, nil
}

// CreateModel creates a data model for a REST API.
func (b *InMemoryBackend) CreateModel(input CreateModelInput) (*Model, error) {
	if input.RestAPIID == "" {
		return nil, fmt.Errorf("%w: restApiId is required", ErrInvalidParameter)
	}

	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	if input.ContentType == "" {
		return nil, fmt.Errorf("%w: contentType is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateModel")
	defer b.mu.Unlock()

	d, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	for _, m := range d.models {
		if m.Name == input.Name {
			return nil, fmt.Errorf(
				"%w: model %q already exists in REST API %s",
				ErrAlreadyExists,
				input.Name,
				input.RestAPIID,
			)
		}
	}

	id := randomID(resourceIDLength)
	model := &Model{
		ID:          id,
		RestAPIID:   input.RestAPIID,
		Name:        input.Name,
		Description: input.Description,
		ContentType: input.ContentType,
		Schema:      input.Schema,
	}
	d.models[id] = model

	cp := *model

	return &cp, nil
}

// CreateStage creates a new deployment stage for a REST API without creating a deployment.
func (b *InMemoryBackend) CreateStage(input CreateStageInput) (*Stage, error) {
	if input.RestAPIID == "" {
		return nil, fmt.Errorf("%w: restApiId is required", ErrInvalidParameter)
	}

	if input.StageName == "" {
		return nil, fmt.Errorf("%w: stageName is required", ErrInvalidParameter)
	}

	if input.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateStage")
	defer b.mu.Unlock()

	d, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	if _, exists := d.deployments[input.DeploymentID]; !exists {
		return nil, fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, input.DeploymentID)
	}

	if _, exists := d.stages[input.StageName]; exists {
		return nil, fmt.Errorf("%w: stage %q already exists", ErrAlreadyExists, input.StageName)
	}

	variables := input.Variables
	if variables == nil {
		variables = make(map[string]string)
	}

	now := unixEpochTime{time.Now()}
	stage := &Stage{
		StageName:           input.StageName,
		RestAPIID:           input.RestAPIID,
		DeploymentID:        input.DeploymentID,
		Description:         input.Description,
		Variables:           variables,
		CreatedDate:         now,
		LastUpdatedDate:     now,
		CanarySettings:      input.CanarySettings,
		AccessLogSettings:   input.AccessLogSettings,
		MethodSettings:      input.MethodSettings,
		TracingEnabled:      input.TracingEnabled,
		ClientCertificateId: input.ClientCertificateId,
	}
	d.stages[input.StageName] = stage

	cp := *stage

	return &cp, nil
}

// CreateUsagePlan creates a new usage plan.
func (b *InMemoryBackend) CreateUsagePlan(input CreateUsagePlanInput) (*UsagePlan, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateUsagePlan")
	defer b.mu.Unlock()

	id := randomID(apiIDLength)
	backendTags := initTagsFromInput("apigw.usageplan."+id+".tags", input.Tags)

	plan := &UsagePlan{
		ID:          id,
		Name:        input.Name,
		Description: input.Description,
		Throttle:    input.Throttle,
		Quota:       input.Quota,
		ApiStages:   input.ApiStages,
		Tags:        backendTags,
	}
	b.usagePlans[id] = plan
	b.usagePlanKeys[id] = make(map[string]*UsagePlanKey)

	cp := *plan

	return &cp, nil
}

// CreateUsagePlanKey associates an API key with a usage plan.
func (b *InMemoryBackend) CreateUsagePlanKey(input CreateUsagePlanKeyInput) (*UsagePlanKey, error) {
	if input.UsagePlanID == "" {
		return nil, fmt.Errorf("%w: usagePlanId is required", ErrInvalidParameter)
	}

	if input.KeyID == "" {
		return nil, fmt.Errorf("%w: keyId is required", ErrInvalidParameter)
	}

	if input.KeyType == "" {
		return nil, fmt.Errorf("%w: keyType is required", ErrInvalidParameter)
	}

	if input.KeyType != "API_KEY" {
		return nil, fmt.Errorf("%w: keyType must be API_KEY, got %q", ErrInvalidParameter, input.KeyType)
	}

	b.mu.Lock("CreateUsagePlanKey")
	defer b.mu.Unlock()

	if _, exists := b.usagePlans[input.UsagePlanID]; !exists {
		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, input.UsagePlanID)
	}

	apiKey, exists := b.apiKeys[input.KeyID]
	if !exists {
		return nil, fmt.Errorf("%w: API key %s not found", ErrAPIKeyNotFound, input.KeyID)
	}

	keys := b.usagePlanKeys[input.UsagePlanID]
	if _, alreadyAssoc := keys[input.KeyID]; alreadyAssoc {
		return nil, fmt.Errorf("%w: key %s already associated with usage plan", ErrAlreadyExists, input.KeyID)
	}

	upk := &UsagePlanKey{
		ID:    apiKey.ID,
		Type:  input.KeyType,
		Value: apiKey.Value,
		Name:  apiKey.Name,
	}
	keys[input.KeyID] = upk

	cp := *upk

	return &cp, nil
}

// GetAPIKey retrieves an API key by ID.
func (b *InMemoryBackend) GetAPIKey(id string) (*APIKey, error) {
	b.mu.RLock("GetAPIKey")
	defer b.mu.RUnlock()
	key, ok := b.apiKeys[id]
	if !ok {
		return nil, fmt.Errorf("%w: API key %s not found", ErrAPIKeyNotFound, id)
	}
	cp := *key

	return &cp, nil
}

// GetAPIKeyByValue retrieves an API key by its value (the secret string sent in x-api-key).
func (b *InMemoryBackend) GetAPIKeyByValue(value string) (*APIKey, error) {
	b.mu.RLock("GetAPIKeyByValue")
	defer b.mu.RUnlock()
	for _, k := range b.apiKeys {
		if k.Value == value {
			cp := *k
			return &cp, nil
		}
	}
	return nil, fmt.Errorf("%w: API key with value not found", ErrAPIKeyNotFound)
}

// GetAPIKeys returns all API keys sorted by ID.
func (b *InMemoryBackend) GetAPIKeys() ([]APIKey, error) {
	b.mu.RLock("GetAPIKeys")
	defer b.mu.RUnlock()
	all := make([]APIKey, 0, len(b.apiKeys))
	for _, k := range b.apiKeys {
		all = append(all, *k)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// DeleteAPIKey removes an API key by ID.
func (b *InMemoryBackend) DeleteAPIKey(id string) error {
	b.mu.Lock("DeleteAPIKey")
	defer b.mu.Unlock()
	if _, ok := b.apiKeys[id]; !ok {
		return fmt.Errorf("%w: API key %s not found", ErrAPIKeyNotFound, id)
	}
	delete(b.apiKeys, id)

	return nil
}

// UpdateAPIKey updates mutable fields on an existing API key.
func (b *InMemoryBackend) UpdateAPIKey(id string, input UpdateAPIKeyInput) (*APIKey, error) {
	b.mu.Lock("UpdateAPIKey")
	defer b.mu.Unlock()
	key, ok := b.apiKeys[id]
	if !ok {
		return nil, fmt.Errorf("%w: API key %s not found", ErrAPIKeyNotFound, id)
	}
	if input.Name != "" {
		key.Name = input.Name
	}
	if input.Description != "" {
		key.Description = input.Description
	}
	if input.Enabled != nil {
		key.Enabled = *input.Enabled
	}
	key.LastUpdatedDate = unixEpochTime{time.Now()}
	cp := *key

	return &cp, nil
}

// GetDomainName retrieves a domain name by value.
func (b *InMemoryBackend) GetDomainName(name string) (*DomainName, error) {
	b.mu.RLock("GetDomainName")
	defer b.mu.RUnlock()
	dn, ok := b.domainNames[name]
	if !ok {
		return nil, fmt.Errorf("%w: domain name %s not found", ErrDomainNameNotFound, name)
	}
	cp := *dn

	return &cp, nil
}

// GetDomainNames returns all domain names sorted by name.
func (b *InMemoryBackend) GetDomainNames() ([]DomainName, error) {
	b.mu.RLock("GetDomainNames")
	defer b.mu.RUnlock()
	all := make([]DomainName, 0, len(b.domainNames))
	for _, dn := range b.domainNames {
		all = append(all, *dn)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].DomainNameValue < all[j].DomainNameValue })

	return all, nil
}

// DeleteDomainName removes a domain name by value.
func (b *InMemoryBackend) DeleteDomainName(name string) error {
	b.mu.Lock("DeleteDomainName")
	defer b.mu.Unlock()
	if _, ok := b.domainNames[name]; !ok {
		return fmt.Errorf("%w: domain name %s not found", ErrDomainNameNotFound, name)
	}
	delete(b.domainNames, name)

	return nil
}

// GetBasePathMapping retrieves a base path mapping by domain + path.
func (b *InMemoryBackend) GetBasePathMapping(domainName, basePath string) (*BasePathMapping, error) {
	b.mu.RLock("GetBasePathMapping")
	defer b.mu.RUnlock()
	mapKey := domainName + "#" + basePath
	bpm, ok := b.basePathMappings[mapKey]
	if !ok {
		return nil, fmt.Errorf(
			"%w: base path mapping not found for domain %q path %q",
			ErrBasePathMappingNotFound,
			domainName,
			basePath,
		)
	}
	cp := *bpm

	return &cp, nil
}

// GetBasePathMappings returns all base path mappings for a domain name.
func (b *InMemoryBackend) GetBasePathMappings(domainName string) ([]BasePathMapping, error) {
	b.mu.RLock("GetBasePathMappings")
	defer b.mu.RUnlock()
	var all []BasePathMapping
	prefix := domainName + "#"
	for k, bpm := range b.basePathMappings {
		if strings.HasPrefix(k, prefix) {
			all = append(all, *bpm)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].BasePath < all[j].BasePath })

	return all, nil
}

// DeleteBasePathMapping removes a base path mapping by domain + path.
func (b *InMemoryBackend) DeleteBasePathMapping(domainName, basePath string) error {
	b.mu.Lock("DeleteBasePathMapping")
	defer b.mu.Unlock()
	mapKey := domainName + "#" + basePath
	if _, ok := b.basePathMappings[mapKey]; !ok {
		return fmt.Errorf(
			"%w: base path mapping not found for domain %q path %q",
			ErrBasePathMappingNotFound,
			domainName,
			basePath,
		)
	}
	delete(b.basePathMappings, mapKey)

	return nil
}

// GetModel retrieves a model by name within a REST API.
func (b *InMemoryBackend) GetModel(restAPIID, modelName string) (*Model, error) {
	b.mu.RLock("GetModel")
	defer b.mu.RUnlock()
	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	for _, m := range d.models {
		if m.Name == modelName {
			cp := *m

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: model %q not found", ErrModelNotFound, modelName)
}

// GetModels returns all models for a REST API sorted by name.
func (b *InMemoryBackend) GetModels(restAPIID string) ([]Model, error) {
	b.mu.RLock("GetModels")
	defer b.mu.RUnlock()
	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	all := make([]Model, 0, len(d.models))
	for _, m := range d.models {
		all = append(all, *m)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	return all, nil
}

// DeleteModel removes a model from a REST API by name.
func (b *InMemoryBackend) DeleteModel(restAPIID, modelName string) error {
	b.mu.Lock("DeleteModel")
	defer b.mu.Unlock()
	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	for id, m := range d.models {
		if m.Name == modelName {
			delete(d.models, id)

			return nil
		}
	}

	return fmt.Errorf("%w: model %q not found", ErrModelNotFound, modelName)
}

// UpdateModel updates description and schema on a model.
func (b *InMemoryBackend) UpdateModel(restAPIID, modelName string, input UpdateModelInput) (*Model, error) {
	b.mu.Lock("UpdateModel")
	defer b.mu.Unlock()
	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	for _, m := range d.models {
		if m.Name == modelName {
			if input.Description != "" {
				m.Description = input.Description
			}
			if input.Schema != "" {
				m.Schema = input.Schema
			}
			cp := *m

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: model %q not found", ErrModelNotFound, modelName)
}

// UpdateStage updates mutable fields on a deployment stage.
func (b *InMemoryBackend) UpdateStage(restAPIID, stageName string, input UpdateStageInput) (*Stage, error) {
	b.mu.Lock("UpdateStage")
	defer b.mu.Unlock()
	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	stage, ok := d.stages[stageName]
	if !ok {
		return nil, fmt.Errorf("%w: stage %q not found", ErrStageNotFound, stageName)
	}
	if input.Description != "" {
		stage.Description = input.Description
	}
	if input.DeploymentID != "" {
		stage.DeploymentID = input.DeploymentID
	}
	if input.Variables != nil {
		stage.Variables = input.Variables
	}
	if input.CanarySettings != nil {
		stage.CanarySettings = input.CanarySettings
	}
	if input.AccessLogSettings != nil {
		stage.AccessLogSettings = input.AccessLogSettings
	}
	if input.MethodSettings != nil {
		stage.MethodSettings = input.MethodSettings
	}
	if input.TracingEnabled != nil {
		stage.TracingEnabled = *input.TracingEnabled
	}
	if input.ClientCertificateId != "" {
		stage.ClientCertificateId = input.ClientCertificateId
	}
	stage.LastUpdatedDate = unixEpochTime{time.Now()}
	cp := *stage

	return &cp, nil
}

// GetUsagePlan retrieves a usage plan by ID.
func (b *InMemoryBackend) GetUsagePlan(id string) (*UsagePlan, error) {
	b.mu.RLock("GetUsagePlan")
	defer b.mu.RUnlock()
	p, ok := b.usagePlans[id]
	if !ok {
		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, id)
	}
	cp := *p

	return &cp, nil
}

// GetUsagePlans returns all usage plans sorted by ID.
func (b *InMemoryBackend) GetUsagePlans() ([]UsagePlan, error) {
	b.mu.RLock("GetUsagePlans")
	defer b.mu.RUnlock()
	all := make([]UsagePlan, 0, len(b.usagePlans))
	for _, p := range b.usagePlans {
		all = append(all, *p)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// DeleteUsagePlan removes a usage plan by ID along with its key associations.
func (b *InMemoryBackend) DeleteUsagePlan(id string) error {
	b.mu.Lock("DeleteUsagePlan")
	defer b.mu.Unlock()
	if _, ok := b.usagePlans[id]; !ok {
		return fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, id)
	}
	delete(b.usagePlans, id)
	delete(b.usagePlanKeys, id)

	return nil
}

// GetUsagePlanKey retrieves a single key from a usage plan.
func (b *InMemoryBackend) GetUsagePlanKey(usagePlanID, keyID string) (*UsagePlanKey, error) {
	b.mu.RLock("GetUsagePlanKey")
	defer b.mu.RUnlock()
	keys, ok := b.usagePlanKeys[usagePlanID]
	if !ok {
		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, usagePlanID)
	}
	k, ok := keys[keyID]
	if !ok {
		return nil, fmt.Errorf("%w: usage plan key %s not found", ErrUsagePlanKeyNotFound, keyID)
	}
	cp := *k

	return &cp, nil
}

// GetUsagePlanKeys returns all keys for a usage plan sorted by ID.
func (b *InMemoryBackend) GetUsagePlanKeys(usagePlanID string) ([]UsagePlanKey, error) {
	b.mu.RLock("GetUsagePlanKeys")
	defer b.mu.RUnlock()
	keys, ok := b.usagePlanKeys[usagePlanID]
	if !ok {
		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, usagePlanID)
	}
	all := make([]UsagePlanKey, 0, len(keys))
	for _, k := range keys {
		all = append(all, *k)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// DeleteUsagePlanKey removes a key from a usage plan.
func (b *InMemoryBackend) DeleteUsagePlanKey(usagePlanID, keyID string) error {
	b.mu.Lock("DeleteUsagePlanKey")
	defer b.mu.Unlock()
	keys, ok := b.usagePlanKeys[usagePlanID]
	if !ok {
		return fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, usagePlanID)
	}
	if _, exists := keys[keyID]; !exists {
		return fmt.Errorf("%w: usage plan key %s not found", ErrUsagePlanKeyNotFound, keyID)
	}
	delete(keys, keyID)

	return nil
}

// GetDocumentationPart retrieves a documentation part by ID.
func (b *InMemoryBackend) GetDocumentationPart(restAPIID, docPartID string) (*DocumentationPart, error) {
	b.mu.RLock("GetDocumentationPart")
	defer b.mu.RUnlock()
	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	p, ok := d.documentationParts[docPartID]
	if !ok {
		return nil, fmt.Errorf("%w: documentation part %s not found", ErrDocumentationPartNotFound, docPartID)
	}
	cp := *p

	return &cp, nil
}

// GetDocumentationParts returns all documentation parts for a REST API sorted by ID.
func (b *InMemoryBackend) GetDocumentationParts(restAPIID string) ([]DocumentationPart, error) {
	b.mu.RLock("GetDocumentationParts")
	defer b.mu.RUnlock()
	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	all := make([]DocumentationPart, 0, len(d.documentationParts))
	for _, p := range d.documentationParts {
		all = append(all, *p)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// DeleteDocumentationPart removes a documentation part by ID.
func (b *InMemoryBackend) DeleteDocumentationPart(restAPIID, docPartID string) error {
	b.mu.Lock("DeleteDocumentationPart")
	defer b.mu.Unlock()
	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	if _, exists := d.documentationParts[docPartID]; !exists {
		return fmt.Errorf("%w: documentation part %s not found", ErrDocumentationPartNotFound, docPartID)
	}
	delete(d.documentationParts, docPartID)

	return nil
}

// GetDocumentationVersion retrieves a documentation version by version string.
func (b *InMemoryBackend) GetDocumentationVersion(restAPIID, version string) (*DocumentationVersion, error) {
	b.mu.RLock("GetDocumentationVersion")
	defer b.mu.RUnlock()
	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	v, ok := d.documentationVersions[version]
	if !ok {
		return nil, fmt.Errorf("%w: documentation version %q not found", ErrDocumentationVersionNotFound, version)
	}
	cp := *v

	return &cp, nil
}

// GetDocumentationVersions returns all documentation versions for a REST API sorted by version.
func (b *InMemoryBackend) GetDocumentationVersions(restAPIID string) ([]DocumentationVersion, error) {
	b.mu.RLock("GetDocumentationVersions")
	defer b.mu.RUnlock()
	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	all := make([]DocumentationVersion, 0, len(d.documentationVersions))
	for _, v := range d.documentationVersions {
		all = append(all, *v)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Version < all[j].Version })

	return all, nil
}

// DeleteDocumentationVersion removes a documentation version by version string.
func (b *InMemoryBackend) DeleteDocumentationVersion(restAPIID, version string) error {
	b.mu.Lock("DeleteDocumentationVersion")
	defer b.mu.Unlock()
	d, ok := b.apis[restAPIID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	if _, exists := d.documentationVersions[version]; !exists {
		return fmt.Errorf("%w: documentation version %q not found", ErrDocumentationVersionNotFound, version)
	}
	delete(d.documentationVersions, version)

	return nil
}

// UpdateRestAPI updates the name and/or description of a REST API.
func (b *InMemoryBackend) UpdateRestAPI(restAPIID string, input UpdateRestAPIInput) (*RestAPI, error) {
	b.mu.Lock("UpdateRestAPI")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	if input.Name != "" {
		d.api.Name = input.Name
	}

	if input.Description != "" {
		d.api.Description = input.Description
	}

	if input.Policy != "" {
		d.api.Policy = input.Policy
	}

	if input.APIKeySource != "" {
		d.api.APIKeySource = input.APIKeySource
	}

	if input.BinaryMediaTypes != nil {
		d.api.BinaryMediaTypes = input.BinaryMediaTypes
	}

	if input.EndpointConfiguration != nil {
		d.api.EndpointConfiguration = input.EndpointConfiguration
	}

	if input.MinimumCompressionSize != nil {
		d.api.MinimumCompressionSize = *input.MinimumCompressionSize
	}

	cp := d.api

	return &cp, nil
}

// UpdateResource updates the pathPart of a resource (recomputes path if changed).
func (b *InMemoryBackend) UpdateResource(restAPIID, resourceID string, input UpdateResourceInput) (*Resource, error) {
	b.mu.Lock("UpdateResource")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	res, ok := d.resources[resourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}

	if input.PathPart != "" {
		var parentPath string
		if res.ParentID != "" {
			if parent, exists := d.resources[res.ParentID]; exists {
				parentPath = parent.Path
			}
		}

		res.PathPart = input.PathPart
		res.Path = computePath(parentPath, input.PathPart)
	}

	if input.CorsConfiguration != nil {
		res.CorsConfiguration = input.CorsConfiguration
	}

	cp := *res

	return &cp, nil
}

// UpdateDeployment updates the description of a deployment.
func (b *InMemoryBackend) UpdateDeployment(
	restAPIID, deploymentID string,
	input UpdateDeploymentInput,
) (*Deployment, error) {
	b.mu.Lock("UpdateDeployment")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	depl, ok := d.deployments[deploymentID]
	if !ok {
		return nil, fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	if input.Description != "" {
		depl.Description = input.Description
	}

	cp := *depl

	return &cp, nil
}

// GetAccount returns the mock API Gateway account settings.
func (b *InMemoryBackend) GetAccount() (*Account, error) {
	b.mu.RLock("GetAccount")
	defer b.mu.RUnlock()

	return b.account, nil
}

// GetResourceTags returns the tags for a resource identified by its ARN.
// For simplicity, we parse the ARN to extract the resource type and ID.
func (b *InMemoryBackend) GetResourceTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetResourceTags")
	defer b.mu.RUnlock()

	// ARN format: arn:aws:apigateway:{region}::/restapis/{id}
	// We strip down to find the resource.
	parts := strings.SplitN(resourceARN, "/restapis/", arnSplitParts)
	if len(parts) != arnSplitParts {
		return map[string]string{}, nil
	}

	apiID := strings.Split(parts[1], "/")[0]

	d, ok := b.apis[apiID]
	if !ok {
		return map[string]string{}, nil
	}

	return d.api.Tags.Clone(), nil
}

// TagResource adds or updates tags on a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, newTags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	parts := strings.SplitN(resourceARN, "/restapis/", arnSplitParts)
	if len(parts) != arnSplitParts {
		return fmt.Errorf("%w: unsupported resource ARN format", ErrInvalidParameter)
	}

	apiID := strings.Split(parts[1], "/")[0]

	d, ok := b.apis[apiID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, apiID)
	}

	for k, v := range newTags {
		d.api.Tags.Set(k, v)
	}

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	parts := strings.SplitN(resourceARN, "/restapis/", arnSplitParts)
	if len(parts) != arnSplitParts {
		return fmt.Errorf("%w: unsupported resource ARN format", ErrInvalidParameter)
	}

	apiID := strings.Split(parts[1], "/")[0]

	d, ok := b.apis[apiID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, apiID)
	}

	for _, k := range tagKeys {
		d.api.Tags.Delete(k)
	}

	return nil
}

// TestInvokeMethod performs a test invocation of a method, returning a mock 200 response.
func (b *InMemoryBackend) TestInvokeMethod(input TestInvokeMethodInput) (*TestInvokeMethodOutput, error) {
	b.mu.RLock("TestInvokeMethod")
	defer b.mu.RUnlock()

	d, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	r, ok := d.resources[input.ResourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, input.ResourceID)
	}

	m, ok := r.ResourceMethods[input.HTTPMethod]
	if !ok {
		return nil, fmt.Errorf(
			"%w: method %s not found on resource %s",
			ErrMethodNotFound,
			input.HTTPMethod,
			input.ResourceID,
		)
	}

	body := "{}"
	if m.MethodIntegration != nil && m.MethodIntegration.Type == "MOCK" {
		body = `{"statusCode": 200}`
	}

	return &TestInvokeMethodOutput{
		Status:  http.StatusOK,
		Body:    body,
		Latency: 1,
		Log:     "Test invocation (mock)",
		Headers: map[string]string{"Content-Type": contentTypeJSON},
	}, nil
}

// GetAPIKeysPage returns API keys with cursor-based pagination.
func (b *InMemoryBackend) GetAPIKeysPage(limit int, position string) ([]APIKey, string, error) {
	b.mu.RLock("GetAPIKeysPage")
	defer b.mu.RUnlock()

	all := make([]APIKey, 0, len(b.apiKeys))
	for _, k := range b.apiKeys {
		all = append(all, *k)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	page, pos := paginatePage(all, limit, position)

	return page, pos, nil
}

// GetDomainNamesPage returns domain names with cursor-based pagination.
func (b *InMemoryBackend) GetDomainNamesPage(limit int, position string) ([]DomainName, string, error) {
	b.mu.RLock("GetDomainNamesPage")
	defer b.mu.RUnlock()

	all := make([]DomainName, 0, len(b.domainNames))
	for _, d := range b.domainNames {
		all = append(all, *d)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].DomainNameValue < all[j].DomainNameValue })
	page, pos := paginatePage(all, limit, position)

	return page, pos, nil
}

// GetUsagePlansPage returns usage plans with cursor-based pagination.
func (b *InMemoryBackend) GetUsagePlansPage(limit int, position string) ([]UsagePlan, string, error) {
	b.mu.RLock("GetUsagePlansPage")
	defer b.mu.RUnlock()

	all := make([]UsagePlan, 0, len(b.usagePlans))
	for _, p := range b.usagePlans {
		all = append(all, *p)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	page, pos := paginatePage(all, limit, position)

	return page, pos, nil
}

// UpdateUsagePlan updates a usage plan's name, description, throttle, or quota.
func (b *InMemoryBackend) UpdateUsagePlan(input UpdateUsagePlanInput) (*UsagePlan, error) {
	b.mu.Lock("UpdateUsagePlan")
	defer b.mu.Unlock()

	p, ok := b.usagePlans[input.UsagePlanID]
	if !ok {
		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, input.UsagePlanID)
	}

	if input.Name != "" {
		p.Name = input.Name
	}

	if input.Description != "" {
		p.Description = input.Description
	}

	if input.Throttle != nil {
		p.Throttle = input.Throttle
	}

	if input.Quota != nil {
		p.Quota = input.Quota
	}

	if len(input.ApiStages) > 0 {
		p.ApiStages = input.ApiStages
	}

	return p, nil
}

// UpdateDomainName updates a domain name's certificate ARN.
func (b *InMemoryBackend) UpdateDomainName(input UpdateDomainNameInput) (*DomainName, error) {
	b.mu.Lock("UpdateDomainName")
	defer b.mu.Unlock()

	d, ok := b.domainNames[input.DomainName]
	if !ok {
		return nil, fmt.Errorf("%w: domain name %s not found", ErrDomainNameNotFound, input.DomainName)
	}

	if input.CertificateARN != "" {
		d.CertificateARN = input.CertificateARN
	}

	if input.RegionalCertificateARN != "" {
		d.RegionalCertificateARN = input.RegionalCertificateARN
	}

	if input.SecurityPolicy != "" {
		d.SecurityPolicy = input.SecurityPolicy
	}

	if input.EndpointConfiguration != nil {
		d.EndpointConfiguration = input.EndpointConfiguration
	}

	return d, nil
}

// UpdateBasePathMapping updates an existing base path mapping.
func (b *InMemoryBackend) UpdateBasePathMapping(input UpdateBasePathMappingInput) (*BasePathMapping, error) {
	b.mu.Lock("UpdateBasePathMapping")
	defer b.mu.Unlock()

	key := input.DomainName + "#" + input.BasePath
	m, ok := b.basePathMappings[key]
	if !ok {
		return nil, fmt.Errorf("%w: base path mapping %s/%s not found", ErrNotFound, input.DomainName, input.BasePath)
	}

	if input.RestAPIID != "" {
		m.RestAPIID = input.RestAPIID
	}

	if input.Stage != "" {
		m.Stage = input.Stage
	}

	return m, nil
}

// UpdateDocumentationPart updates the properties of a documentation part.
func (b *InMemoryBackend) UpdateDocumentationPart(input UpdateDocumentationPartInput) (*DocumentationPart, error) {
	b.mu.Lock("UpdateDocumentationPart")
	defer b.mu.Unlock()

	d, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	part, ok := d.documentationParts[input.DocPartID]
	if !ok {
		return nil, fmt.Errorf("%w: documentation part %s not found", ErrNotFound, input.DocPartID)
	}

	if input.Properties != "" {
		part.Properties = input.Properties
	}

	return part, nil
}

// UpdateDocumentationVersion updates a documentation version's description.
func (b *InMemoryBackend) UpdateDocumentationVersion(
	input UpdateDocumentationVersionInput,
) (*DocumentationVersion, error) {
	b.mu.Lock("UpdateDocumentationVersion")
	defer b.mu.Unlock()

	d, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	ver, ok := d.documentationVersions[input.DocumentationVersion]
	if !ok {
		return nil, fmt.Errorf("%w: documentation version %s not found", ErrNotFound, input.DocumentationVersion)
	}

	if input.Description != "" {
		ver.Description = input.Description
	}

	return ver, nil
}

// UpdateMethod updates method settings (authorization, API key requirement, etc.)
func (b *InMemoryBackend) UpdateMethod(input UpdateMethodInput) (*Method, error) {
	b.mu.Lock("UpdateMethod")
	defer b.mu.Unlock()

	d, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	r, ok := d.resources[input.ResourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, input.ResourceID)
	}

	m, ok := r.ResourceMethods[input.HTTPMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, input.HTTPMethod)
	}

	if input.AuthorizationType != "" {
		m.AuthorizationType = input.AuthorizationType
	}

	if input.AuthorizerID != "" {
		m.AuthorizerID = input.AuthorizerID
	}

	if input.APIKeyRequired != nil {
		m.APIKeyRequired = *input.APIKeyRequired
	}

	if input.OperationName != "" {
		m.OperationName = input.OperationName
	}

	if len(input.RequestModels) > 0 {
		m.RequestModels = input.RequestModels
	}

	return m, nil
}

// UpdateIntegration updates an integration's URI or type.
func (b *InMemoryBackend) UpdateIntegration(input UpdateIntegrationInput) (*Integration, error) {
	b.mu.Lock("UpdateIntegration")
	defer b.mu.Unlock()

	d, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	r, ok := d.resources[input.ResourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, input.ResourceID)
	}

	m, ok := r.ResourceMethods[input.HTTPMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, input.HTTPMethod)
	}

	if m.MethodIntegration == nil {
		m.MethodIntegration = &Integration{}
	}

	if input.URI != "" {
		m.MethodIntegration.URI = input.URI
	}

	if input.IntegrationType != "" {
		m.MethodIntegration.Type = input.IntegrationType
	}

	if input.IntegrationHTTPMethod != "" {
		m.MethodIntegration.HTTPMethod = input.IntegrationHTTPMethod
	}

	if len(input.RequestTemplates) > 0 {
		m.MethodIntegration.RequestTemplates = input.RequestTemplates
	}

	if len(input.RequestParameters) > 0 {
		m.MethodIntegration.RequestParameters = input.RequestParameters
	}

	if len(input.CacheKeyParameters) > 0 {
		m.MethodIntegration.CacheKeyParameters = input.CacheKeyParameters
	}

	if input.PassthroughBehavior != "" {
		m.MethodIntegration.PassthroughBehavior = input.PassthroughBehavior
	}

	if input.ConnectionType != "" {
		m.MethodIntegration.ConnectionType = input.ConnectionType
	}

	if input.ConnectionId != "" {
		m.MethodIntegration.ConnectionId = input.ConnectionId
	}

	if input.ContentHandling != "" {
		m.MethodIntegration.ContentHandling = input.ContentHandling
	}

	if input.Credentials != "" {
		m.MethodIntegration.Credentials = input.Credentials
	}

	if input.CacheNamespace != "" {
		m.MethodIntegration.CacheNamespace = input.CacheNamespace
	}

	if input.TimeoutInMillis > 0 {
		m.MethodIntegration.TimeoutInMillis = input.TimeoutInMillis
	}

	return m.MethodIntegration, nil
}

// UpdateIntegrationResponse updates an integration response's templates or selection pattern.
func (b *InMemoryBackend) UpdateIntegrationResponse(
	input UpdateIntegrationResponseInput,
) (*IntegrationResponse, error) {
	b.mu.Lock("UpdateIntegrationResponse")
	defer b.mu.Unlock()

	d, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	r, ok := d.resources[input.ResourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, input.ResourceID)
	}

	m, ok := r.ResourceMethods[input.HTTPMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, input.HTTPMethod)
	}

	if m.MethodIntegration == nil {
		return nil, fmt.Errorf("%w: no integration on method %s", ErrNotFound, input.HTTPMethod)
	}

	ir, ok := m.MethodIntegration.IntegrationResponses[input.StatusCode]
	if !ok {
		return nil, fmt.Errorf("%w: integration response %s not found", ErrNotFound, input.StatusCode)
	}

	if input.SelectionPattern != "" {
		ir.SelectionPattern = input.SelectionPattern
	}

	if len(input.ResponseTemplates) > 0 {
		ir.ResponseTemplates = input.ResponseTemplates
	}

	if len(input.ResponseParameters) > 0 {
		ir.ResponseParameters = input.ResponseParameters
	}

	if input.ContentHandling != "" {
		ir.ContentHandling = input.ContentHandling
	}

	m.MethodIntegration.IntegrationResponses[input.StatusCode] = ir

	return ir, nil
}

// UpdateMethodResponse updates a method response's models or parameters.
func (b *InMemoryBackend) UpdateMethodResponse(input UpdateMethodResponseInput) (*MethodResponse, error) {
	b.mu.Lock("UpdateMethodResponse")
	defer b.mu.Unlock()

	d, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	r, ok := d.resources[input.ResourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, input.ResourceID)
	}

	m, ok := r.ResourceMethods[input.HTTPMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, input.HTTPMethod)
	}

	mr, ok := m.MethodResponses[input.StatusCode]
	if !ok {
		return nil, fmt.Errorf("%w: method response %s not found", ErrNotFound, input.StatusCode)
	}

	if len(input.ResponseModels) > 0 {
		mr.ResponseModels = input.ResponseModels
	}

	if len(input.ResponseParameters) > 0 {
		mr.ResponseParameters = input.ResponseParameters
	}

	m.MethodResponses[input.StatusCode] = mr

	return mr, nil
}

// UpdateAccount updates the account's throttle settings.
func (b *InMemoryBackend) UpdateAccount(input UpdateAccountInput) (*Account, error) {
	b.mu.Lock("UpdateAccount")
	defer b.mu.Unlock()

	if input.ThrottleSettings != nil {
		b.account.ThrottleSettings = input.ThrottleSettings
	}

	return b.account, nil
}

// TestInvokeAuthorizer performs a mock test invocation of an authorizer.
func (b *InMemoryBackend) TestInvokeAuthorizer(input TestInvokeAuthorizerInput) (*TestInvokeAuthorizerOutput, error) {
	b.mu.RLock("TestInvokeAuthorizer")
	defer b.mu.RUnlock()

	d, ok := b.apis[input.RestAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	if _, authOK := d.authorizers[input.AuthorizerID]; !authOK {
		return nil, fmt.Errorf("%w: authorizer %s not found", ErrNotFound, input.AuthorizerID)
	}

	return &TestInvokeAuthorizerOutput{
		PrincipalID:         "test-principal",
		AuthorizationStatus: http.StatusOK,
		ClientStatus:        http.StatusOK,
		Latency:             1,
		Log:                 "Test authorizer invocation (mock)",
		Context:             map[string]string{"principalId": "test-principal"},
	}, nil
}

// GetModelTemplate returns the default template for a model.
func (b *InMemoryBackend) GetModelTemplate(restAPIID, modelName string) (string, error) {
	b.mu.RLock("GetModelTemplate")
	defer b.mu.RUnlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return "", fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	m, ok := d.models[modelName]
	if !ok {
		return "", fmt.Errorf("%w: model %s not found", ErrNotFound, modelName)
	}

	if m.Schema != "" {
		return m.Schema, nil
	}

	return "#set($inputRoot = $input.path('$'))\n{}", nil
}

// GatewayResponseKey generates a storage key for gateway responses.
func gatewayResponseKey(restAPIID, responseType string) string {
	return restAPIID + "#" + responseType
}

// GetGatewayResponse retrieves a gateway response by type.
func (b *InMemoryBackend) GetGatewayResponse(restAPIID, responseType string) (*GatewayResponse, error) {
	b.mu.RLock("GetGatewayResponse")
	defer b.mu.RUnlock()

	key := gatewayResponseKey(restAPIID, responseType)
	gr, ok := b.gatewayResponses[key]
	if !ok {
		// Return default response (AWS returns default responses even when not explicitly set).
		return &GatewayResponse{
			RestAPIID:       restAPIID,
			ResponseType:    responseType,
			DefaultResponse: true,
			StatusCode:      gatewayResponseDefaultStatus(responseType),
		}, nil
	}

	return gr, nil
}

// gatewayResponseDefaultStatus returns the default HTTP status for a gateway response type.
func gatewayResponseDefaultStatus(responseType string) string {
	switch responseType {
	case "UNAUTHORIZED", "ACCESS_DENIED":
		return "401"
	case "RESOURCE_NOT_FOUND":
		return "404"
	case "THROTTLED", "QUOTA_EXCEEDED":
		return "429"
	case "BAD_REQUEST_BODY", "BAD_REQUEST_PARAMETERS":
		return "400"
	case "REQUEST_TOO_LARGE":
		return "413"
	case "AUTHORIZER_FAILURE", "AUTHORIZER_CONFIGURATION_ERROR":
		return "500"
	case "DEFAULT_4XX":
		return "400"
	case "DEFAULT_5XX":
		return "500"
	default:
		return "500"
	}
}

// GetGatewayResponses retrieves all gateway responses for a REST API.
func (b *InMemoryBackend) GetGatewayResponses(restAPIID string) ([]GatewayResponse, error) {
	b.mu.RLock("GetGatewayResponses")
	defer b.mu.RUnlock()

	if _, ok := b.apis[restAPIID]; !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	defaultTypes := []string{
		"UNAUTHORIZED", "ACCESS_DENIED", "RESOURCE_NOT_FOUND",
		"THROTTLED", "QUOTA_EXCEEDED", "BAD_REQUEST_BODY",
		"BAD_REQUEST_PARAMETERS", "REQUEST_TOO_LARGE",
		"AUTHORIZER_FAILURE", "AUTHORIZER_CONFIGURATION_ERROR",
		"DEFAULT_4XX", "DEFAULT_5XX",
	}

	result := make([]GatewayResponse, 0, len(defaultTypes))

	for _, rt := range defaultTypes {
		key := gatewayResponseKey(restAPIID, rt)
		if gr, ok := b.gatewayResponses[key]; ok {
			result = append(result, *gr)
		} else {
			result = append(result, GatewayResponse{
				RestAPIID:       restAPIID,
				ResponseType:    rt,
				DefaultResponse: true,
				StatusCode:      gatewayResponseDefaultStatus(rt),
			})
		}
	}

	return result, nil
}

// PutGatewayResponse creates or updates a gateway response.
func (b *InMemoryBackend) PutGatewayResponse(input PutGatewayResponseInput) (*GatewayResponse, error) {
	b.mu.Lock("PutGatewayResponse")
	defer b.mu.Unlock()

	if _, ok := b.apis[input.RestAPIID]; !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	key := gatewayResponseKey(input.RestAPIID, input.ResponseType)

	gr := &GatewayResponse{
		RestAPIID:          input.RestAPIID,
		ResponseType:       input.ResponseType,
		StatusCode:         input.StatusCode,
		ResponseParameters: input.ResponseParameters,
		ResponseTemplates:  input.ResponseTemplates,
		DefaultResponse:    false,
	}

	if gr.StatusCode == "" {
		gr.StatusCode = gatewayResponseDefaultStatus(input.ResponseType)
	}

	b.gatewayResponses[key] = gr

	return gr, nil
}

// DeleteGatewayResponse removes a custom gateway response, reverting to default.
func (b *InMemoryBackend) DeleteGatewayResponse(restAPIID, responseType string) error {
	b.mu.Lock("DeleteGatewayResponse")
	defer b.mu.Unlock()

	if _, ok := b.apis[restAPIID]; !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	key := gatewayResponseKey(restAPIID, responseType)
	delete(b.gatewayResponses, key)

	return nil
}

// GenerateClientCertificate creates a new client certificate for mutual TLS.
func (b *InMemoryBackend) GenerateClientCertificate(input GenerateClientCertificateInput) (*ClientCertificate, error) {
	b.mu.Lock("GenerateClientCertificate")
	defer b.mu.Unlock()

	id := randomID(apiIDLength)
	now := time.Now()
	cert := &ClientCertificate{
		ClientCertificateID:   id,
		Description:           input.Description,
		PemEncodedCertificate: "-----BEGIN CERTIFICATE-----\nMIICpDCCAYwCCQDU...(mock)...\n-----END CERTIFICATE-----",
		CreatedDate:           unixEpochTime{now},
		ExpirationDate:        unixEpochTime{now.AddDate(0, 0, clientCertValidityDays)},
	}

	b.clientCertificates[id] = cert

	return cert, nil
}

// GetClientCertificate returns a client certificate by ID.
func (b *InMemoryBackend) GetClientCertificate(id string) (*ClientCertificate, error) {
	b.mu.RLock("GetClientCertificate")
	defer b.mu.RUnlock()

	cert, ok := b.clientCertificates[id]
	if !ok {
		return nil, fmt.Errorf("%w: client certificate %s not found", ErrNotFound, id)
	}

	return cert, nil
}

// GetClientCertificates returns all client certificates.
func (b *InMemoryBackend) GetClientCertificates() ([]ClientCertificate, error) {
	b.mu.RLock("GetClientCertificates")
	defer b.mu.RUnlock()

	result := make([]ClientCertificate, 0, len(b.clientCertificates))
	for _, c := range b.clientCertificates {
		result = append(result, *c)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ClientCertificateID < result[j].ClientCertificateID
	})

	return result, nil
}

// DeleteClientCertificate removes a client certificate.
func (b *InMemoryBackend) DeleteClientCertificate(id string) error {
	b.mu.Lock("DeleteClientCertificate")
	defer b.mu.Unlock()

	if _, ok := b.clientCertificates[id]; !ok {
		return fmt.Errorf("%w: client certificate %s not found", ErrNotFound, id)
	}

	delete(b.clientCertificates, id)

	return nil
}

// GetUsage returns mock usage data for a usage plan.
func (b *InMemoryBackend) GetUsage(input GetUsageInput) (*UsageData, error) {
	b.mu.RLock("GetUsage")
	defer b.mu.RUnlock()

	if _, ok := b.usagePlans[input.UsagePlanID]; !ok {
		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, input.UsagePlanID)
	}

	return &UsageData{
		UsagePlanID: input.UsagePlanID,
		StartDate:   input.StartDate,
		EndDate:     input.EndDate,
		Items:       map[string][]any{},
	}, nil
}

// UpdateClientCertificate updates the description of a client certificate.
func (b *InMemoryBackend) UpdateClientCertificate(input UpdateClientCertificateInput) (*ClientCertificate, error) {
	b.mu.Lock("UpdateClientCertificate")
	defer b.mu.Unlock()

	cert, ok := b.clientCertificates[input.ClientCertificateID]
	if !ok {
		return nil, fmt.Errorf("%w: client certificate %s not found", ErrNotFound, input.ClientCertificateID)
	}

	cert.Description = input.Description

	return cert, nil
}

// CreateVpcLink creates a new VPC link.
func (b *InMemoryBackend) CreateVpcLink(input CreateVpcLinkInput) (*VpcLink, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	id := randomID(apiIDLength)

	link := &VpcLink{
		ID:          id,
		Name:        input.Name,
		Description: input.Description,
		Status:      vpcLinkStatusAvailable,
		TargetARNs:  input.TargetARNs,
		Tags:        input.Tags,
	}

	b.mu.Lock("CreateVpcLink")
	defer b.mu.Unlock()

	b.vpcLinks[id] = link

	return link, nil
}

// GetVpcLink retrieves a VPC link by ID.
func (b *InMemoryBackend) GetVpcLink(id string) (*VpcLink, error) {
	b.mu.RLock("GetVpcLink")
	defer b.mu.RUnlock()

	link, ok := b.vpcLinks[id]
	if !ok {
		return nil, fmt.Errorf("%w: VPC link %s not found", ErrNotFound, id)
	}

	return link, nil
}

// GetVpcLinks retrieves all VPC links.
func (b *InMemoryBackend) GetVpcLinks() ([]VpcLink, error) {
	b.mu.RLock("GetVpcLinks")
	defer b.mu.RUnlock()

	result := make([]VpcLink, 0, len(b.vpcLinks))
	for _, link := range b.vpcLinks {
		result = append(result, *link)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})

	return result, nil
}

// DeleteVpcLink removes a VPC link.
func (b *InMemoryBackend) DeleteVpcLink(id string) error {
	b.mu.Lock("DeleteVpcLink")
	defer b.mu.Unlock()

	if _, ok := b.vpcLinks[id]; !ok {
		return fmt.Errorf("%w: VPC link %s not found", ErrNotFound, id)
	}

	delete(b.vpcLinks, id)

	return nil
}

// UpdateVpcLink updates the name or description of a VPC link.
func (b *InMemoryBackend) UpdateVpcLink(input UpdateVpcLinkInput) (*VpcLink, error) {
	b.mu.Lock("UpdateVpcLink")
	defer b.mu.Unlock()

	link, ok := b.vpcLinks[input.VpcLinkID]
	if !ok {
		return nil, fmt.Errorf("%w: VPC link %s not found", ErrNotFound, input.VpcLinkID)
	}

	if input.Name != "" {
		link.Name = input.Name
	}

	if input.Description != "" {
		link.Description = input.Description
	}

	return link, nil
}

// GetExport generates an OpenAPI 2.0 (Swagger) or OAS 3.0 export of the REST API.
// exportType "oas30" produces OpenAPI 3.0.1; any other value produces Swagger 2.0.
func (b *InMemoryBackend) GetExport(restAPIID, stageName, exportType string) (map[string]any, error) {
	b.mu.RLock("GetExport")
	defer b.mu.RUnlock()

	data, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	if exportType == "oas30" {
		return buildOAS30Export(data, stageName), nil
	}

	return buildSwagger20Export(data, stageName), nil
}

// buildSwagger20Export constructs a Swagger 2.0 export document.
func buildSwagger20Export(data *apiData, stageName string) map[string]any {
	paths := buildExportPaths(data, false)

	secDefs := map[string]any{
		exportKeyAPIKey: map[string]any{
			exportKeyType: "apiKey",
			keyAPIName:    "x-api-key",
			"in":          "header",
		},
	}

	return map[string]any{
		"swagger":             "2.0",
		"info":                map[string]any{"title": data.api.Name, "version": "1.0"},
		"basePath":            "/" + stageName,
		"paths":               paths,
		"securityDefinitions": secDefs,
	}
}

// buildOAS30Export constructs an OpenAPI 3.0.1 export document.
func buildOAS30Export(data *apiData, stageName string) map[string]any {
	paths := buildExportPaths(data, true)

	components := map[string]any{
		"securitySchemes": map[string]any{
			exportKeyAPIKey: map[string]any{
				exportKeyType: "apiKey",
				keyAPIName:    "x-api-key",
				"in":          "header",
			},
		},
	}

	// Include model schemas in components.
	if len(data.models) > 0 {
		schemas := make(map[string]any, len(data.models))
		for name, m := range data.models {
			schemas[name] = map[string]any{
				exportKeyDescription: m.Description,
				exportKeyType:        exportKeyObject,
			}
		}
		components["schemas"] = schemas
	}

	return map[string]any{
		"openapi":    "3.0.1",
		"info":       map[string]any{"title": data.api.Name, "version": "1.0"},
		"servers":    []map[string]any{{"url": "/" + stageName}},
		"paths":      paths,
		"components": components,
	}
}

// buildExportPaths constructs the paths object for an OpenAPI export.
// oas30=true emits OAS 3.0 operation objects; false emits Swagger 2.0.
func buildExportPaths(data *apiData, oas30 bool) map[string]any {
	paths := make(map[string]any)

	for _, res := range data.resources {
		if res.Path == "/" || len(res.ResourceMethods) == 0 {
			continue
		}

		pathItem := make(map[string]any)

		for httpMethod, method := range res.ResourceMethods {
			if method == nil {
				continue
			}

			op := buildExportOperation(data, method, oas30)
			pathItem[strings.ToLower(httpMethod)] = op
		}

		if len(pathItem) > 0 {
			paths[res.Path] = pathItem
		}
	}

	return paths
}

// buildExportOperation constructs a single OAS operation object for a method.
func buildExportOperation(data *apiData, method *Method, oas30 bool) map[string]any {
	op := make(map[string]any)
	op["responses"] = buildExportResponses(data, method, oas30)
	buildExportRequestBody(op, data, method, oas30)
	buildExportSecurity(op, method)

	if method.OperationName != "" {
		op["operationId"] = method.OperationName
	}

	if method.MethodIntegration != nil {
		op["x-amazon-apigateway-integration"] = buildExportIntegration(method.MethodIntegration)
	}

	if !oas30 {
		op["produces"] = []string{contentTypeJSON}
	}

	return op
}

// buildExportResponses constructs the responses map for an OAS operation.
func buildExportResponses(data *apiData, method *Method, oas30 bool) map[string]any {
	responses := make(map[string]any)

	for statusCode, mr := range method.MethodResponses {
		rsp := map[string]any{exportKeyDescription: statusCode + " response"}

		if len(mr.ResponseModels) > 0 {
			if oas30 {
				content := make(map[string]any)
				for ct, modelName := range mr.ResponseModels {
					content[ct] = map[string]any{exportKeySchema: buildModelRef(data, modelName, oas30)}
				}
				rsp["content"] = content
			} else {
				for _, modelName := range mr.ResponseModels {
					rsp[exportKeySchema] = buildModelRef(data, modelName, oas30)

					break
				}
			}
		}

		responses[statusCode] = rsp
	}

	if len(responses) == 0 {
		responses["200"] = map[string]any{exportKeyDescription: "200 response"}
	}

	return responses
}

// buildExportRequestBody adds request body / request model entries to an operation map.
func buildExportRequestBody(op map[string]any, data *apiData, method *Method, oas30 bool) {
	if len(method.RequestModels) == 0 {
		return
	}

	if oas30 {
		content := make(map[string]any)
		for ct, modelName := range method.RequestModels {
			content[ct] = map[string]any{exportKeySchema: buildModelRef(data, modelName, oas30)}
		}
		op["requestBody"] = map[string]any{"content": content}
	} else {
		for _, modelName := range method.RequestModels {
			op["consumes"] = []string{contentTypeJSON}
			op["parameters"] = []map[string]any{
				{
					"in":            exportKeyBody,
					"name":          exportKeyBody,
					exportKeySchema: buildModelRef(data, modelName, oas30),
				},
			}

			break
		}
	}
}

// buildExportSecurity adds the security requirement to an operation when API key or authorizer is configured.
func buildExportSecurity(op map[string]any, method *Method) {
	if method.AuthorizerID != "" {
		scheme := "lambda_authorizer"
		if method.AuthorizationType == "COGNITO_USER_POOLS" {
			scheme = "cognito"
		}
		op["security"] = []map[string]any{{scheme: []string{}}}

		return
	}

	if method.APIKeyRequired {
		op["security"] = []map[string]any{{exportKeyAPIKey: []string{}}}
	}
}

// buildExportIntegration constructs the x-amazon-apigateway-integration extension.
func buildExportIntegration(integ *Integration) map[string]any {
	xInteg := map[string]any{
		exportKeyType:         integ.Type,
		"httpMethod":          integ.HTTPMethod,
		"uri":                 integ.URI,
		"passthroughBehavior": integ.PassthroughBehavior,
	}

	if len(integ.RequestTemplates) > 0 {
		xInteg["requestTemplates"] = integ.RequestTemplates
	}

	if len(integ.IntegrationResponses) > 0 {
		xResponses := make(map[string]any, len(integ.IntegrationResponses))
		for sc, ir := range integ.IntegrationResponses {
			xIR := map[string]any{"statusCode": ir.StatusCode}
			if ir.SelectionPattern != "" {
				xIR["selectionPattern"] = ir.SelectionPattern
			}

			if len(ir.ResponseTemplates) > 0 {
				xIR["responseTemplates"] = ir.ResponseTemplates
			}

			if ir.ContentHandling != "" {
				xIR["contentHandling"] = ir.ContentHandling
			}

			xResponses[sc] = xIR
		}

		xInteg["responses"] = xResponses
	}

	return xInteg
}

// buildModelRef returns a schema reference or inline schema for a model name.
func buildModelRef(data *apiData, modelName string, oas30 bool) map[string]any {
	m, ok := data.models[modelName]
	if !ok {
		return map[string]any{exportKeyType: exportKeyObject}
	}

	if m.Schema != "" {
		if oas30 {
			return map[string]any{"$ref": "#/components/schemas/" + modelName}
		}

		return map[string]any{"$ref": "#/definitions/" + modelName}
	}

	return map[string]any{exportKeyType: exportKeyObject, exportKeyDescription: m.Description}
}
