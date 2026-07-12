package apigateway

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"

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

	// ErrQuotaExceeded is returned by the data plane when an API key has exhausted
	// its usage-plan quota for the current period (AWS maps this to HTTP 429).
	ErrQuotaExceeded = errors.New("LimitExceededException")
	// ErrThrottled is returned by the data plane when an API key exceeds the
	// usage-plan rate/burst throttle (AWS maps this to HTTP 429).
	ErrThrottled = errors.New("TooManyRequestsException")
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
	// ResourcesForRouting returns every resource for an API together with a version
	// counter that changes whenever the API's resource set is mutated. The data-plane
	// proxy uses it to build and cache a routing trie without re-copying the full
	// resource set (and paging past AWS's default page size) on every request.
	ResourcesForRouting(restAPIID string) ([]Resource, uint64, error)
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
	GetDomainNameAccessAssociations(resourceOwner string) ([]DomainNameAccessAssociation, error)
	DeleteDomainNameAccessAssociation(arn string) error
	RejectDomainNameAccessAssociation(arn, domainNameARN string) error

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
	UpdateGatewayResponse(input PutGatewayResponseInput) (*GatewayResponse, error)
	DeleteGatewayResponse(restAPIID, responseType string) error

	// Client certificate operations.
	GenerateClientCertificate(input GenerateClientCertificateInput) (*ClientCertificate, error)
	GetClientCertificate(id string) (*ClientCertificate, error)
	GetClientCertificates() ([]ClientCertificate, error)
	DeleteClientCertificate(id string) error

	// Usage operations.
	GetUsage(input GetUsageInput) (*UsageData, error)
	// EnforceUsagePlan applies usage-plan quota and throttle/burst limits for an API
	// key on the given API stage. It returns nil when the request is allowed (or when
	// the key is not associated with a usage plan for the stage), ErrQuotaExceeded when
	// the period quota is exhausted, or ErrThrottled when the rate/burst limit is hit.
	EnforceUsagePlan(apiID, stageName, keyID string) error

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

	// SDK generation.
	GetSdkTypes() []SdkType
	GetSdkType(id string) (*SdkType, error)
	GetSdk(restAPIID, stageName, sdkType string) (*SdkExport, error)

	// API key / documentation part bulk import.
	ImportAPIKeys(body []byte, format string, failOnWarnings bool) ([]string, []string, error)
	ImportDocumentationParts(
		restAPIID string,
		body []byte,
		mode string,
		failOnWarnings bool,
	) ([]string, []string, error)

	// Usage update.
	UpdateUsage(usagePlanID, keyID string, dateValues map[string]string) (*UsageData, error)

	// OpenAPI import.
	ImportRestAPI(input ImportRestAPIInput) (*RestAPI, error)
	PutRestAPI(input PutRestAPIInput) (*RestAPI, error)
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
	// AWS API Gateway defaults list operations to a page size of 25.
	defaultPageSize = 25

	// clientCertValidityDays is the number of days a generated client certificate is valid.
	// AWS issues certificates with a 2-year validity period.
	clientCertValidityDays = 730

	// defaultIntegrationTimeoutMs is the AWS default integration timeout in milliseconds.
	// AWS API Gateway applies this when timeoutInMillis is not specified on PutIntegration.
	defaultIntegrationTimeoutMs = 29000
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

const (
	paramLocationHeader = "header"
	paramLocationPath   = "path"
	paramLocationQuery  = "querystring"
)

// stageInvokeURL returns the gopherstack proxy path for a deployed stage.
// The full URL is relative — clients prepend their gopherstack base URL.
func stageInvokeURL(restAPIID, stageName string) string {
	return "/proxy/" + restAPIID + "/" + stageName
}

// encodePosition returns an opaque, mutation-stable pagination cursor for the given
// sort key. The cursor encodes the key of the last item on the current page (rather
// than a fragile numeric offset), so concurrent inserts/deletes do not cause items
// to be skipped or repeated across pages. AWS returns similarly opaque tokens.
func encodePosition(key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte("k:" + key))
}

// decodePosition reverses encodePosition. It returns ("", false) for an empty or
// malformed cursor, in which case pagination restarts from the beginning.
func decodePosition(position string) (string, bool) {
	if position == "" {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(position)
	if err != nil {
		return "", false
	}
	s := string(raw)
	after, ok := strings.CutPrefix(s, "k:")
	if !ok {
		return "", false
	}

	return after, true
}

// paginatePageByKey applies limit/cursor pagination to a slice that is already sorted
// ascending by the key returned by keyOf. It returns the page and an opaque next-page
// cursor (empty when the last page is reached). The cursor is mutation-stable: it is
// derived from the last returned item's key, so resuming does the right thing even if
// the underlying collection changed between calls.
func paginatePageByKey[T any](all []T, limit int, position string, keyOf func(T) string) ([]T, string) {
	if limit <= 0 {
		limit = defaultPageSize
	}

	startIdx := 0
	if cursor, ok := decodePosition(position); ok {
		// Resume at the first item strictly after the cursor key.
		startIdx = sort.Search(len(all), func(i int) bool { return keyOf(all[i]) > cursor })
	}

	if startIdx >= len(all) {
		return []T{}, ""
	}

	end := startIdx + limit
	var outPosition string
	if end < len(all) {
		outPosition = encodePosition(keyOf(all[end-1]))
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

// InMemoryBackend implements StorageBackend using in-memory maps, with every
// resource collection registered as a *store.Table on registry (see
// store_setup.go). Resource families that AWS scopes to a REST API
// (resources, deployments, stages, authorizers, requestValidators,
// documentationParts, documentationVersions, models) are flat tables keyed by
// a composite "<restAPIID>#<childID>" string (see resourceKey et al in
// store_setup.go), with a secondary "byAPI" [store.Index] answering "all
// children of REST API X" -- replacing the old map[string]*apiData nesting.
type InMemoryBackend struct {
	account *Account

	restApis *store.Table[RestAPI]

	resources      *store.Table[Resource]
	resourcesByAPI *store.Index[Resource]

	deployments      *store.Table[Deployment]
	deploymentsByAPI *store.Index[Deployment]

	stages      *store.Table[Stage]
	stagesByAPI *store.Index[Stage]

	authorizers      *store.Table[Authorizer]
	authorizersByAPI *store.Index[Authorizer]

	requestValidators      *store.Table[RequestValidator]
	requestValidatorsByAPI *store.Index[RequestValidator]

	documentationParts      *store.Table[DocumentationPart]
	documentationPartsByAPI *store.Index[DocumentationPart]

	documentationVersions      *store.Table[DocumentationVersion]
	documentationVersionsByAPI *store.Index[DocumentationVersion]

	models      *store.Table[Model]
	modelsByAPI *store.Index[Model]

	// resourceVersions (restAPIID → counter) is bumped whenever a REST API's
	// resource set is mutated. The data-plane proxy uses it to invalidate its
	// cached routing trie. Left as a plain map: not a resource collection, so
	// it doesn't fit store.Table's shape (see store_setup.go's
	// registerAllTables doc).
	resourceVersions map[string]uint64

	apiKeys        *store.Table[APIKey]
	apiKeysByValue map[string]string // key value → key ID, O(1) data-plane lookup

	usage *usageTracker // usage-plan quota + throttle state

	basePathMappings             *store.Table[BasePathMapping] // key: domainName + "#" + basePath
	domainNames                  *store.Table[DomainName]
	domainNameAccessAssociations *store.Table[DomainNameAccessAssociation]

	usagePlans          *store.Table[UsagePlan]
	usagePlanKeys       *store.Table[UsagePlanKey] // key: usagePlanID + "#" + keyID
	usagePlanKeysByPlan *store.Index[UsagePlanKey]

	gatewayResponses   *store.Table[GatewayResponse]   // key: restAPIID + "#" + responseType
	clientCertificates *store.Table[ClientCertificate] // key: clientCertificateID
	vpcLinks           *store.Table[VpcLink]

	// usageOverrides (usagePlanID → keyID → remaining quota) is set via
	// UpdateUsage. Left as a plain map: the value (int64) carries no identity
	// field of its own to key a store.Table by.
	usageOverrides map[string]map[string]int64

	registry *store.Registry
	mu       *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		account: &Account{
			ThrottleSettings: &ThrottleSettings{
				BurstLimit: defaultBurstLimit,
				RateLimit:  defaultRateLimit,
			},
			Features:      []string{"UsagePlans"},
			APIKeyVersion: "1",
		},
		registry:         store.NewRegistry(),
		resourceVersions: make(map[string]uint64),
		apiKeysByValue:   make(map[string]string),
		usage:            newUsageTracker(),
		usageOverrides:   make(map[string]map[string]int64),
		mu:               lockmetrics.New("apigateway"),
	}
	registerAllTables(b)

	return b
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

	api := &RestAPI{
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

	b.restApis.Put(api)
	b.resources.Put(root)

	cp := *api

	return &cp, nil
}

// DeleteRestAPI removes a REST API and all its resources.
func (b *InMemoryBackend) DeleteRestAPI(restAPIID string) error {
	b.mu.Lock("DeleteRestAPI")
	defer b.mu.Unlock()

	api, ok := b.restApis.Get(restAPIID)
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	api.Tags.Close()
	b.restApis.Delete(restAPIID)
	b.deleteAPIChildrenLocked(restAPIID)

	return nil
}

// deleteAPIChildrenLocked removes every resource-family entry scoped to
// restAPIID (resources, deployments, stages, authorizers, requestValidators,
// documentationParts, documentationVersions, models) via each table's "byAPI"
// index. Callers must hold b.mu.
func (b *InMemoryBackend) deleteAPIChildrenLocked(restAPIID string) {
	for _, r := range append([]*Resource{}, b.resourcesByAPI.Get(restAPIID)...) {
		b.resources.Delete(resourceKeyFn(r))
	}
	for _, d := range append([]*Deployment{}, b.deploymentsByAPI.Get(restAPIID)...) {
		b.deployments.Delete(deploymentKeyFn(d))
	}
	for _, s := range append([]*Stage{}, b.stagesByAPI.Get(restAPIID)...) {
		b.stages.Delete(stageKeyFn(s))
	}
	for _, a := range append([]*Authorizer{}, b.authorizersByAPI.Get(restAPIID)...) {
		b.authorizers.Delete(authorizerKeyFn(a))
	}
	for _, v := range append([]*RequestValidator{}, b.requestValidatorsByAPI.Get(restAPIID)...) {
		b.requestValidators.Delete(requestValidatorKeyFn(v))
	}
	for _, p := range append([]*DocumentationPart{}, b.documentationPartsByAPI.Get(restAPIID)...) {
		b.documentationParts.Delete(documentationPartKeyFn(p))
	}
	for _, v := range append([]*DocumentationVersion{}, b.documentationVersionsByAPI.Get(restAPIID)...) {
		b.documentationVersions.Delete(documentationVersionKeyFn(v))
	}
	for _, m := range append([]*Model{}, b.modelsByAPI.Get(restAPIID)...) {
		b.models.Delete(modelKeyFn(m))
	}
	delete(b.resourceVersions, restAPIID)
}

// GetRestAPI returns a single REST API.
func (b *InMemoryBackend) GetRestAPI(restAPIID string) (*RestAPI, error) {
	b.mu.RLock("GetRestAPI")
	defer b.mu.RUnlock()

	api, ok := b.restApis.Get(restAPIID)
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	cp := *api

	return &cp, nil
}

// GetRestAPIs returns all REST APIs with pagination.
func (b *InMemoryBackend) GetRestAPIs(limit int, position string) ([]RestAPI, string, error) {
	b.mu.RLock("GetRestAPIs")
	defer b.mu.RUnlock()

	all := make([]RestAPI, 0, b.restApis.Len())
	for _, api := range b.restApis.All() {
		all = append(all, *api)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	page, pos := paginatePageByKey(all, limit, position, func(a RestAPI) string { return a.ID })

	return page, pos, nil
}

// GetResources returns all resources for a REST API with pagination.
func (b *InMemoryBackend) GetResources(restAPIID, position string, limit int) ([]Resource, string, error) {
	b.mu.RLock("GetResources")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, "", fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	group := b.resourcesByAPI.Get(restAPIID)
	all := make([]Resource, 0, len(group))
	for _, r := range group {
		all = append(all, *r)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	page, pos := paginatePageByKey(all, limit, position, func(r Resource) string { return r.ID })

	return page, pos, nil
}

// ResourcesForRouting returns every resource for the API plus a version counter that
// changes on any resource-set mutation. Unlike GetResources it is not paginated: the
// data-plane proxy needs the complete set to build a routing trie, and it uses the
// version to cache that trie across requests instead of rebuilding it every time.
func (b *InMemoryBackend) ResourcesForRouting(restAPIID string) ([]Resource, uint64, error) {
	b.mu.RLock("ResourcesForRouting")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, 0, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	group := b.resourcesByAPI.Get(restAPIID)
	all := make([]Resource, 0, len(group))
	for _, r := range group {
		all = append(all, *r)
	}

	return all, b.resourceVersions[restAPIID], nil
}

// GetResource returns a single resource.
func (b *InMemoryBackend) GetResource(restAPIID, resourceID string) (*Resource, error) {
	b.mu.RLock("GetResource")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	parent, ok := b.resources.Get(resourceKey(restAPIID, parentID))
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
	b.resources.Put(res)
	b.resourceVersions[restAPIID]++

	cp := *res

	return &cp, nil
}

// DeleteResource removes a resource.
func (b *InMemoryBackend) DeleteResource(restAPIID, resourceID string) error {
	b.mu.Lock("DeleteResource")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	if !b.resources.Delete(resourceKey(restAPIID, resourceID)) {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	b.resourceVersions[restAPIID]++

	return nil
}

// PutMethod creates or replaces a method on a resource.
func (b *InMemoryBackend) PutMethod(input PutMethodInput) (*Method, error) {
	b.mu.Lock("PutMethod")
	defer b.mu.Unlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}
	r, ok := b.resources.Get(resourceKey(input.RestAPIID, input.ResourceID))
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
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

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	m, ok := r.ResourceMethods[httpMethod]
	if !ok {
		return nil, fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}

	timeout := input.TimeoutInMillis
	if timeout == 0 {
		timeout = defaultIntegrationTimeoutMs
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
		ConnectionID:         input.ConnectionID,
		ContentHandling:      input.ContentHandling,
		Credentials:          input.Credentials,
		CacheNamespace:       input.CacheNamespace,
		TimeoutInMillis:      timeout,
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
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

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
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

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
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

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
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

	if !b.restApis.Has(restAPIID) {
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
	b.deployments.Put(depl)

	if stageName != "" {
		// AWS: stage description comes from stageDescription (a separate field),
		// not from the deployment description. New stages start with empty description.
		stage := &Stage{
			StageName:       stageName,
			RestAPIID:       restAPIID,
			DeploymentID:    deplID,
			CreatedDate:     now,
			LastUpdatedDate: now,
			Variables:       make(map[string]string),
		}
		b.stages.Put(stage)
	}

	cp := *depl

	return &cp, nil
}

// GetDeployments returns all deployments for a REST API.
func (b *InMemoryBackend) GetDeployments(restAPIID string) ([]Deployment, error) {
	b.mu.RLock("GetDeployments")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	group := b.deploymentsByAPI.Get(restAPIID)
	all := make([]Deployment, 0, len(group))
	for _, dep := range group {
		all = append(all, *dep)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// GetDeployment returns a single deployment by ID.
func (b *InMemoryBackend) GetDeployment(restAPIID, deploymentID string) (*Deployment, error) {
	b.mu.RLock("GetDeployment")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	dep, ok := b.deployments.Get(deploymentKey(restAPIID, deploymentID))
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

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	if !b.deployments.Has(deploymentKey(restAPIID, deploymentID)) {
		return fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	for _, s := range b.stagesByAPI.Get(restAPIID) {
		if s.DeploymentID == deploymentID {
			return fmt.Errorf(
				"%w: deployment %s is referenced by stage %s and cannot be deleted",
				ErrInvalidParameter, deploymentID, s.StageName,
			)
		}
	}

	b.deployments.Delete(deploymentKey(restAPIID, deploymentID))

	return nil
}

// GetStages returns all stages for a REST API.
func (b *InMemoryBackend) GetStages(restAPIID string) ([]Stage, error) {
	b.mu.RLock("GetStages")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	group := b.stagesByAPI.Get(restAPIID)
	all := make([]Stage, 0, len(group))
	for _, s := range group {
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	s, stageOK := b.stages.Get(stageKey(restAPIID, stageName))
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

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	if !b.stages.Delete(stageKey(restAPIID, stageName)) {
		return fmt.Errorf("%w: stage %s not found", ErrResourceNotFound, stageName)
	}

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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	id := randomID(resourceIDLength)
	auth := &Authorizer{
		ID:                           id,
		RestAPIID:                    restAPIID,
		Name:                         input.Name,
		Type:                         input.Type,
		AuthorizerURI:                input.AuthorizerURI,
		AuthorizerCredentials:        input.AuthorizerCredentials,
		IdentitySource:               input.IdentitySource,
		IdentityValidationExpression: input.IdentityValidationExpression,
		AuthorizerResultTTLInSeconds: input.AuthorizerResultTTLInSeconds,
		ProviderARNs:                 input.ProviderARNs,
	}
	b.authorizers.Put(auth)

	cp := *auth

	return &cp, nil
}

// GetAuthorizer retrieves an authorizer by ID.
func (b *InMemoryBackend) GetAuthorizer(restAPIID, authorizerID string) (*Authorizer, error) {
	b.mu.RLock("GetAuthorizer")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	auth, ok := b.authorizers.Get(authorizerKey(restAPIID, authorizerID))
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	group := b.authorizersByAPI.Get(restAPIID)
	all := make([]Authorizer, 0, len(group))
	for _, auth := range group {
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	auth, ok := b.authorizers.Get(authorizerKey(restAPIID, authorizerID))
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

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	if !b.authorizers.Delete(authorizerKey(restAPIID, authorizerID)) {
		return fmt.Errorf("%w: authorizer %s not found", ErrAuthorizerNotFound, authorizerID)
	}

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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	id := randomID(resourceIDLength)
	rv := &RequestValidator{
		ID:                        id,
		RestAPIID:                 restAPIID,
		Name:                      input.Name,
		ValidateRequestBody:       input.ValidateRequestBody,
		ValidateRequestParameters: input.ValidateRequestParameters,
	}
	b.requestValidators.Put(rv)

	cp := *rv

	return &cp, nil
}

// GetRequestValidator retrieves a request validator by ID.
func (b *InMemoryBackend) GetRequestValidator(restAPIID, validatorID string) (*RequestValidator, error) {
	b.mu.RLock("GetRequestValidator")
	defer b.mu.RUnlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	rv, ok := b.requestValidators.Get(requestValidatorKey(restAPIID, validatorID))
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	group := b.requestValidatorsByAPI.Get(restAPIID)
	all := make([]RequestValidator, 0, len(group))
	for _, rv := range group {
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	rv, ok := b.requestValidators.Get(requestValidatorKey(restAPIID, validatorID))
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

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	if !b.requestValidators.Delete(requestValidatorKey(restAPIID, validatorID)) {
		return fmt.Errorf("%w: request validator %s not found", ErrValidatorNotFound, validatorID)
	}

	return nil
}

func computePath(parentPath, pathPart string) string {
	if parentPath == "/" {
		return "/" + pathPart
	}

	return strings.TrimRight(parentPath, "/") + "/" + pathPart
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Close all REST API tag stores to prevent resource leaks.
	for _, api := range b.restApis.All() {
		if api.Tags != nil {
			api.Tags.Close()
		}
	}

	for _, k := range b.apiKeys.All() {
		if k.Tags != nil {
			k.Tags.Close()
		}
	}

	for _, dn := range b.domainNames.All() {
		if dn.Tags != nil {
			dn.Tags.Close()
		}
	}

	for _, p := range b.usagePlans.All() {
		if p.Tags != nil {
			p.Tags.Close()
		}
	}

	b.registry.ResetAll()
	// The "dirty" tables (see store_setup.go's registerAllTables doc) are
	// deliberately NOT on b.registry, so each needs its own Reset() call here.
	b.resources.Reset()
	b.deployments.Reset()
	b.stages.Reset()
	b.authorizers.Reset()
	b.requestValidators.Reset()
	b.documentationParts.Reset()
	b.documentationVersions.Reset()
	b.models.Reset()
	b.usagePlanKeys.Reset()
	b.resourceVersions = make(map[string]uint64)
	b.apiKeysByValue = make(map[string]string)
	b.usage = newUsageTracker()
	b.usageOverrides = make(map[string]map[string]int64)
}

// CreateAPIKey creates a new API key with an optional auto-generated value.
func (b *InMemoryBackend) CreateAPIKey(input CreateAPIKeyInput) (*APIKey, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateAPIKey")
	defer b.mu.Unlock()

	for _, k := range b.apiKeys.All() {
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
		CustomerID:      input.CustomerID,
		Enabled:         input.Enabled,
		Tags:            backendTags,
		CreatedDate:     now,
		LastUpdatedDate: now,
	}
	b.apiKeys.Put(key)
	b.apiKeysByValue[value] = id

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

	mapKey := basePathMappingKey(input.DomainName, input.BasePath)
	if b.basePathMappings.Has(mapKey) {
		return nil, fmt.Errorf("%w: base path mapping already exists for domain %q path %q",
			ErrAlreadyExists, input.DomainName, input.BasePath)
	}

	bpm := &BasePathMapping{
		DomainName: input.DomainName,
		BasePath:   input.BasePath,
		RestAPIID:  input.RestAPIID,
		Stage:      input.Stage,
	}
	b.basePathMappings.Put(bpm)

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

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	id := randomID(resourceIDLength)
	part := &DocumentationPart{
		ID:         id,
		RestAPIID:  input.RestAPIID,
		Location:   input.Location,
		Properties: input.Properties,
	}
	b.documentationParts.Put(part)

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

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	if b.documentationVersions.Has(documentationVersionKey(input.RestAPIID, input.Version)) {
		return nil, fmt.Errorf("%w: documentation version %q already exists", ErrAlreadyExists, input.Version)
	}

	ver := &DocumentationVersion{
		RestAPIID:   input.RestAPIID,
		Version:     input.Version,
		Description: input.Description,
		CreatedDate: unixEpochTime{time.Now()},
	}
	b.documentationVersions.Put(ver)

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

	if b.domainNames.Has(input.DomainName) {
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
		RegionalHostedZoneID:     "Z2FDTNDATAQYW2",
		DistributionDomainName:   distributionDomain,
		DistributionHostedZoneID: "Z2FDTNDATAQYW2",
		DomainNameStatus:         "AVAILABLE",
		Tags:                     backendTags,
		CreatedDate:              &now,
	}
	b.domainNames.Put(dn)

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
	b.domainNameAccessAssociations.Put(assoc)

	cp := *assoc

	return &cp, nil
}

// resourceOwnerSelf and resourceOwnerOther are the two valid values of the
// resourceOwner query parameter accepted by GetDomainNameAccessAssociations.
const (
	resourceOwnerSelf  = "SELF"
	resourceOwnerOther = "OTHER_ACCOUNTS"
)

// GetDomainNameAccessAssociations lists domain name access associations owned by
// this account. resourceOwner selects SELF (default) or OTHER_ACCOUNTS; since this
// backend only ever creates associations under the caller's own account,
// OTHER_ACCOUNTS always returns an empty list.
func (b *InMemoryBackend) GetDomainNameAccessAssociations(resourceOwner string) ([]DomainNameAccessAssociation, error) {
	b.mu.RLock("GetDomainNameAccessAssociations")
	defer b.mu.RUnlock()

	if resourceOwner == resourceOwnerOther {
		return []DomainNameAccessAssociation{}, nil
	}

	all := b.domainNameAccessAssociations.All()
	result := make([]DomainNameAccessAssociation, 0, len(all))
	for _, assoc := range all {
		result = append(result, *assoc)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].DomainNameAccessAssociationARN < result[j].DomainNameAccessAssociationARN
	})

	return result, nil
}

// DeleteDomainNameAccessAssociation removes a domain name access association by ARN.
func (b *InMemoryBackend) DeleteDomainNameAccessAssociation(arn string) error {
	b.mu.Lock("DeleteDomainNameAccessAssociation")
	defer b.mu.Unlock()

	if !b.domainNameAccessAssociations.Delete(arn) {
		return fmt.Errorf("%w: domain name access association %s not found", ErrNotFound, arn)
	}

	return nil
}

// RejectDomainNameAccessAssociation rejects (removes) a domain name access
// association, validating that it belongs to the given domain name ARN.
func (b *InMemoryBackend) RejectDomainNameAccessAssociation(arn, domainNameARN string) error {
	b.mu.Lock("RejectDomainNameAccessAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.domainNameAccessAssociations.Get(arn)
	if !ok {
		return fmt.Errorf("%w: domain name access association %s not found", ErrNotFound, arn)
	}

	if domainNameARN != "" && assoc.DomainNameARN != domainNameARN {
		return fmt.Errorf("%w: domainNameArn does not match association %s", ErrInvalidParameter, arn)
	}

	b.domainNameAccessAssociations.Delete(arn)

	return nil
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

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	for _, m := range b.modelsByAPI.Get(input.RestAPIID) {
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
	b.models.Put(model)

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

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	if !b.deployments.Has(deploymentKey(input.RestAPIID, input.DeploymentID)) {
		return nil, fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, input.DeploymentID)
	}

	if b.stages.Has(stageKey(input.RestAPIID, input.StageName)) {
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
		ClientCertificateID: input.ClientCertificateID,
		CacheClusterEnabled: input.CacheClusterEnabled,
		CacheClusterSize:    input.CacheClusterSize,
		CacheClusterStatus:  cacheClusterStatusFor(input.CacheClusterEnabled),
	}
	b.stages.Put(stage)

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
		APIStages:   input.APIStages,
		Tags:        backendTags,
	}
	b.usagePlans.Put(plan)

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

	if !b.usagePlans.Has(input.UsagePlanID) {
		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, input.UsagePlanID)
	}

	apiKey, exists := b.apiKeys.Get(input.KeyID)
	if !exists {
		return nil, fmt.Errorf("%w: API key %s not found", ErrAPIKeyNotFound, input.KeyID)
	}

	if b.usagePlanKeys.Has(usagePlanKeyKey(input.UsagePlanID, input.KeyID)) {
		return nil, fmt.Errorf("%w: key %s already associated with usage plan", ErrAlreadyExists, input.KeyID)
	}

	upk := &UsagePlanKey{
		ID:          apiKey.ID,
		Type:        input.KeyType,
		Value:       apiKey.Value,
		Name:        apiKey.Name,
		UsagePlanID: input.UsagePlanID,
	}
	b.usagePlanKeys.Put(upk)

	cp := *upk

	return &cp, nil
}

// GetAPIKey retrieves an API key by ID.
func (b *InMemoryBackend) GetAPIKey(id string) (*APIKey, error) {
	b.mu.RLock("GetAPIKey")
	defer b.mu.RUnlock()
	key, ok := b.apiKeys.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: API key %s not found", ErrAPIKeyNotFound, id)
	}
	cp := *key

	return &cp, nil
}

// GetAPIKeyByValue retrieves an API key by its value (the secret string sent in
// x-api-key). It resolves the key in O(1) via the value→ID index instead of a
// linear scan, because it runs on the hot data-plane path for every apiKey-required
// request.
func (b *InMemoryBackend) GetAPIKeyByValue(value string) (*APIKey, error) {
	b.mu.RLock("GetAPIKeyByValue")
	defer b.mu.RUnlock()
	id, ok := b.apiKeysByValue[value]
	if !ok {
		return nil, fmt.Errorf("%w: API key with value not found", ErrAPIKeyNotFound)
	}
	k, ok := b.apiKeys.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: API key with value not found", ErrAPIKeyNotFound)
	}
	cp := *k

	return &cp, nil
}

// GetAPIKeys returns all API keys sorted by ID.
func (b *InMemoryBackend) GetAPIKeys() ([]APIKey, error) {
	b.mu.RLock("GetAPIKeys")
	defer b.mu.RUnlock()
	all := make([]APIKey, 0, b.apiKeys.Len())
	for _, k := range b.apiKeys.All() {
		all = append(all, *k)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// DeleteAPIKey removes an API key by ID.
func (b *InMemoryBackend) DeleteAPIKey(id string) error {
	b.mu.Lock("DeleteAPIKey")
	defer b.mu.Unlock()
	key, ok := b.apiKeys.Get(id)
	if !ok {
		return fmt.Errorf("%w: API key %s not found", ErrAPIKeyNotFound, id)
	}
	delete(b.apiKeysByValue, key.Value)
	b.apiKeys.Delete(id)

	return nil
}

// UpdateAPIKey updates mutable fields on an existing API key.
func (b *InMemoryBackend) UpdateAPIKey(id string, input UpdateAPIKeyInput) (*APIKey, error) {
	b.mu.Lock("UpdateAPIKey")
	defer b.mu.Unlock()
	key, ok := b.apiKeys.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: API key %s not found", ErrAPIKeyNotFound, id)
	}
	if input.Name != "" {
		key.Name = input.Name
	}
	if input.Description != "" {
		key.Description = input.Description
	}
	if input.CustomerID != "" {
		key.CustomerID = input.CustomerID
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
	dn, ok := b.domainNames.Get(name)
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
	all := make([]DomainName, 0, b.domainNames.Len())
	for _, dn := range b.domainNames.All() {
		all = append(all, *dn)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].DomainNameValue < all[j].DomainNameValue })

	return all, nil
}

// DeleteDomainName removes a domain name by value.
func (b *InMemoryBackend) DeleteDomainName(name string) error {
	b.mu.Lock("DeleteDomainName")
	defer b.mu.Unlock()
	if !b.domainNames.Delete(name) {
		return fmt.Errorf("%w: domain name %s not found", ErrDomainNameNotFound, name)
	}

	return nil
}

// GetBasePathMapping retrieves a base path mapping by domain + path.
func (b *InMemoryBackend) GetBasePathMapping(domainName, basePath string) (*BasePathMapping, error) {
	b.mu.RLock("GetBasePathMapping")
	defer b.mu.RUnlock()
	bpm, ok := b.basePathMappings.Get(basePathMappingKey(domainName, basePath))
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
	for _, bpm := range b.basePathMappings.All() {
		if strings.HasPrefix(basePathMappingKeyFn(bpm), prefix) {
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
	if !b.basePathMappings.Delete(basePathMappingKey(domainName, basePath)) {
		return fmt.Errorf(
			"%w: base path mapping not found for domain %q path %q",
			ErrBasePathMappingNotFound,
			domainName,
			basePath,
		)
	}

	return nil
}

// GetModel retrieves a model by name within a REST API.
func (b *InMemoryBackend) GetModel(restAPIID, modelName string) (*Model, error) {
	b.mu.RLock("GetModel")
	defer b.mu.RUnlock()
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	for _, m := range b.modelsByAPI.Get(restAPIID) {
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
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	group := b.modelsByAPI.Get(restAPIID)
	all := make([]Model, 0, len(group))
	for _, m := range group {
		all = append(all, *m)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Name < all[j].Name })

	return all, nil
}

// DeleteModel removes a model from a REST API by name.
func (b *InMemoryBackend) DeleteModel(restAPIID, modelName string) error {
	b.mu.Lock("DeleteModel")
	defer b.mu.Unlock()
	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	for _, m := range b.modelsByAPI.Get(restAPIID) {
		if m.Name == modelName {
			b.models.Delete(modelKeyFn(m))

			return nil
		}
	}

	return fmt.Errorf("%w: model %q not found", ErrModelNotFound, modelName)
}

// UpdateModel updates description and schema on a model.
func (b *InMemoryBackend) UpdateModel(restAPIID, modelName string, input UpdateModelInput) (*Model, error) {
	b.mu.Lock("UpdateModel")
	defer b.mu.Unlock()
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	for _, m := range b.modelsByAPI.Get(restAPIID) {
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
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	stage, ok := b.stages.Get(stageKey(restAPIID, stageName))
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
	if input.ClientCertificateID != "" {
		stage.ClientCertificateID = input.ClientCertificateID
	}
	if input.CacheClusterEnabled != nil {
		stage.CacheClusterEnabled = *input.CacheClusterEnabled
		stage.CacheClusterStatus = cacheClusterStatusFor(stage.CacheClusterEnabled)
	}
	if input.CacheClusterSize != "" {
		stage.CacheClusterSize = input.CacheClusterSize
	}
	stage.LastUpdatedDate = unixEpochTime{time.Now()}
	cp := *stage

	return &cp, nil
}

// cacheClusterStatusFor derives the AWS CacheClusterStatus enum value
// ("AVAILABLE"/"NOT_AVAILABLE") from whether the stage's cache cluster is enabled.
func cacheClusterStatusFor(enabled bool) string {
	if enabled {
		return "AVAILABLE"
	}

	return "NOT_AVAILABLE"
}

// GetUsagePlan retrieves a usage plan by ID.
func (b *InMemoryBackend) GetUsagePlan(id string) (*UsagePlan, error) {
	b.mu.RLock("GetUsagePlan")
	defer b.mu.RUnlock()
	p, ok := b.usagePlans.Get(id)
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
	all := make([]UsagePlan, 0, b.usagePlans.Len())
	for _, p := range b.usagePlans.All() {
		all = append(all, *p)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// DeleteUsagePlan removes a usage plan by ID along with its key associations.
func (b *InMemoryBackend) DeleteUsagePlan(id string) error {
	b.mu.Lock("DeleteUsagePlan")
	defer b.mu.Unlock()
	if !b.usagePlans.Delete(id) {
		return fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, id)
	}
	for _, k := range append([]*UsagePlanKey{}, b.usagePlanKeysByPlan.Get(id)...) {
		b.usagePlanKeys.Delete(usagePlanKeyKeyFn(k))
	}

	return nil
}

// GetUsagePlanKey retrieves a single key from a usage plan.
func (b *InMemoryBackend) GetUsagePlanKey(usagePlanID, keyID string) (*UsagePlanKey, error) {
	b.mu.RLock("GetUsagePlanKey")
	defer b.mu.RUnlock()
	if !b.usagePlans.Has(usagePlanID) {
		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, usagePlanID)
	}
	k, ok := b.usagePlanKeys.Get(usagePlanKeyKey(usagePlanID, keyID))
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
	if !b.usagePlans.Has(usagePlanID) {
		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, usagePlanID)
	}
	group := b.usagePlanKeysByPlan.Get(usagePlanID)
	all := make([]UsagePlanKey, 0, len(group))
	for _, k := range group {
		all = append(all, *k)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// DeleteUsagePlanKey removes a key from a usage plan.
func (b *InMemoryBackend) DeleteUsagePlanKey(usagePlanID, keyID string) error {
	b.mu.Lock("DeleteUsagePlanKey")
	defer b.mu.Unlock()
	if !b.usagePlans.Has(usagePlanID) {
		return fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, usagePlanID)
	}
	if !b.usagePlanKeys.Delete(usagePlanKeyKey(usagePlanID, keyID)) {
		return fmt.Errorf("%w: usage plan key %s not found", ErrUsagePlanKeyNotFound, keyID)
	}

	return nil
}

// GetDocumentationPart retrieves a documentation part by ID.
func (b *InMemoryBackend) GetDocumentationPart(restAPIID, docPartID string) (*DocumentationPart, error) {
	b.mu.RLock("GetDocumentationPart")
	defer b.mu.RUnlock()
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	p, ok := b.documentationParts.Get(documentationPartKey(restAPIID, docPartID))
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
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	group := b.documentationPartsByAPI.Get(restAPIID)
	all := make([]DocumentationPart, 0, len(group))
	for _, p := range group {
		all = append(all, *p)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	return all, nil
}

// DeleteDocumentationPart removes a documentation part by ID.
func (b *InMemoryBackend) DeleteDocumentationPart(restAPIID, docPartID string) error {
	b.mu.Lock("DeleteDocumentationPart")
	defer b.mu.Unlock()
	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	if !b.documentationParts.Delete(documentationPartKey(restAPIID, docPartID)) {
		return fmt.Errorf("%w: documentation part %s not found", ErrDocumentationPartNotFound, docPartID)
	}

	return nil
}

// GetDocumentationVersion retrieves a documentation version by version string.
func (b *InMemoryBackend) GetDocumentationVersion(restAPIID, version string) (*DocumentationVersion, error) {
	b.mu.RLock("GetDocumentationVersion")
	defer b.mu.RUnlock()
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	v, ok := b.documentationVersions.Get(documentationVersionKey(restAPIID, version))
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
	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	group := b.documentationVersionsByAPI.Get(restAPIID)
	all := make([]DocumentationVersion, 0, len(group))
	for _, v := range group {
		all = append(all, *v)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Version < all[j].Version })

	return all, nil
}

// DeleteDocumentationVersion removes a documentation version by version string.
func (b *InMemoryBackend) DeleteDocumentationVersion(restAPIID, version string) error {
	b.mu.Lock("DeleteDocumentationVersion")
	defer b.mu.Unlock()
	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: %s", ErrRestAPINotFound, restAPIID)
	}
	if !b.documentationVersions.Delete(documentationVersionKey(restAPIID, version)) {
		return fmt.Errorf("%w: documentation version %q not found", ErrDocumentationVersionNotFound, version)
	}

	return nil
}

// UpdateRestAPI updates the name and/or description of a REST API.
func (b *InMemoryBackend) UpdateRestAPI(restAPIID string, input UpdateRestAPIInput) (*RestAPI, error) {
	b.mu.Lock("UpdateRestAPI")
	defer b.mu.Unlock()

	api, ok := b.restApis.Get(restAPIID)
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	if input.Name != "" {
		api.Name = input.Name
	}

	if input.Description != "" {
		api.Description = input.Description
	}

	if input.Policy != "" {
		api.Policy = input.Policy
	}

	if input.APIKeySource != "" {
		api.APIKeySource = input.APIKeySource
	}

	if input.BinaryMediaTypes != nil {
		api.BinaryMediaTypes = input.BinaryMediaTypes
	}

	if input.EndpointConfiguration != nil {
		api.EndpointConfiguration = input.EndpointConfiguration
	}

	if input.MinimumCompressionSize != nil {
		api.MinimumCompressionSize = *input.MinimumCompressionSize
	}

	cp := *api

	return &cp, nil
}

// UpdateResource updates the pathPart of a resource (recomputes path if changed).
func (b *InMemoryBackend) UpdateResource(restAPIID, resourceID string, input UpdateResourceInput) (*Resource, error) {
	b.mu.Lock("UpdateResource")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	res, ok := b.resources.Get(resourceKey(restAPIID, resourceID))
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}

	if input.PathPart != "" {
		var parentPath string
		if res.ParentID != "" {
			if parent, exists := b.resources.Get(resourceKey(restAPIID, res.ParentID)); exists {
				parentPath = parent.Path
			}
		}

		res.PathPart = input.PathPart
		res.Path = computePath(parentPath, input.PathPart)
		b.resourceVersions[restAPIID]++
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

	if !b.restApis.Has(restAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	depl, ok := b.deployments.Get(deploymentKey(restAPIID, deploymentID))
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

	api, ok := b.restApis.Get(apiID)
	if !ok {
		return map[string]string{}, nil
	}

	return api.Tags.Clone(), nil
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

	api, ok := b.restApis.Get(apiID)
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, apiID)
	}

	for k, v := range newTags {
		api.Tags.Set(k, v)
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

	api, ok := b.restApis.Get(apiID)
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, apiID)
	}

	for _, k := range tagKeys {
		api.Tags.Delete(k)
	}

	return nil
}

// TestInvokeMethod performs a test invocation of a method, returning a mock 200 response.
func (b *InMemoryBackend) TestInvokeMethod(input TestInvokeMethodInput) (*TestInvokeMethodOutput, error) {
	b.mu.RLock("TestInvokeMethod")
	defer b.mu.RUnlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	r, ok := b.resources.Get(resourceKey(input.RestAPIID, input.ResourceID))
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
	if m.MethodIntegration != nil && m.MethodIntegration.Type == IntegrationTypeMock {
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

	all := make([]APIKey, 0, b.apiKeys.Len())
	for _, k := range b.apiKeys.All() {
		all = append(all, *k)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	page, pos := paginatePageByKey(all, limit, position, func(k APIKey) string { return k.ID })

	return page, pos, nil
}

// GetDomainNamesPage returns domain names with cursor-based pagination.
func (b *InMemoryBackend) GetDomainNamesPage(limit int, position string) ([]DomainName, string, error) {
	b.mu.RLock("GetDomainNamesPage")
	defer b.mu.RUnlock()

	all := make([]DomainName, 0, b.domainNames.Len())
	for _, d := range b.domainNames.All() {
		all = append(all, *d)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].DomainNameValue < all[j].DomainNameValue })
	page, pos := paginatePageByKey(all, limit, position, func(d DomainName) string { return d.DomainNameValue })

	return page, pos, nil
}

// GetUsagePlansPage returns usage plans with cursor-based pagination.
func (b *InMemoryBackend) GetUsagePlansPage(limit int, position string) ([]UsagePlan, string, error) {
	b.mu.RLock("GetUsagePlansPage")
	defer b.mu.RUnlock()

	all := make([]UsagePlan, 0, b.usagePlans.Len())
	for _, p := range b.usagePlans.All() {
		all = append(all, *p)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	page, pos := paginatePageByKey(all, limit, position, func(p UsagePlan) string { return p.ID })

	return page, pos, nil
}

// UpdateUsagePlan updates a usage plan's name, description, throttle, or quota.
func (b *InMemoryBackend) UpdateUsagePlan(input UpdateUsagePlanInput) (*UsagePlan, error) {
	b.mu.Lock("UpdateUsagePlan")
	defer b.mu.Unlock()

	p, ok := b.usagePlans.Get(input.UsagePlanID)
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

	if input.APIStages != nil {
		p.APIStages = input.APIStages
	}

	return p, nil
}

// UpdateDomainName updates a domain name's certificate ARN.
func (b *InMemoryBackend) UpdateDomainName(input UpdateDomainNameInput) (*DomainName, error) {
	b.mu.Lock("UpdateDomainName")
	defer b.mu.Unlock()

	d, ok := b.domainNames.Get(input.DomainName)
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

	key := basePathMappingKey(input.DomainName, input.BasePath)
	m, ok := b.basePathMappings.Get(key)
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

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	part, ok := b.documentationParts.Get(documentationPartKey(input.RestAPIID, input.DocPartID))
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

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	ver, ok := b.documentationVersions.Get(documentationVersionKey(input.RestAPIID, input.DocumentationVersion))
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

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	r, ok := b.resources.Get(resourceKey(input.RestAPIID, input.ResourceID))
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

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	r, ok := b.resources.Get(resourceKey(input.RestAPIID, input.ResourceID))
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

	applyIntegrationFields(m.MethodIntegration, input)

	return m.MethodIntegration, nil
}

func applyIntegrationFields(intg *Integration, input UpdateIntegrationInput) {
	if input.URI != "" {
		intg.URI = input.URI
	}
	if input.IntegrationType != "" {
		intg.Type = input.IntegrationType
	}
	if input.IntegrationHTTPMethod != "" {
		intg.HTTPMethod = input.IntegrationHTTPMethod
	}
	if len(input.RequestTemplates) > 0 {
		intg.RequestTemplates = input.RequestTemplates
	}
	if len(input.RequestParameters) > 0 {
		intg.RequestParameters = input.RequestParameters
	}
	if len(input.CacheKeyParameters) > 0 {
		intg.CacheKeyParameters = input.CacheKeyParameters
	}
	if input.PassthroughBehavior != "" {
		intg.PassthroughBehavior = input.PassthroughBehavior
	}
	if input.ConnectionType != "" {
		intg.ConnectionType = input.ConnectionType
	}
	if input.ConnectionID != "" {
		intg.ConnectionID = input.ConnectionID
	}
	if input.ContentHandling != "" {
		intg.ContentHandling = input.ContentHandling
	}
	if input.Credentials != "" {
		intg.Credentials = input.Credentials
	}
	if input.CacheNamespace != "" {
		intg.CacheNamespace = input.CacheNamespace
	}
	if input.TimeoutInMillis > 0 {
		intg.TimeoutInMillis = input.TimeoutInMillis
	}
}

// UpdateIntegrationResponse updates an integration response's templates or selection pattern.
func (b *InMemoryBackend) UpdateIntegrationResponse(
	input UpdateIntegrationResponseInput,
) (*IntegrationResponse, error) {
	b.mu.Lock("UpdateIntegrationResponse")
	defer b.mu.Unlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	r, ok := b.resources.Get(resourceKey(input.RestAPIID, input.ResourceID))
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

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	r, ok := b.resources.Get(resourceKey(input.RestAPIID, input.ResourceID))
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
	if input.CloudwatchRoleARN != "" {
		b.account.CloudwatchRoleARN = input.CloudwatchRoleARN
	}

	return b.account, nil
}

// TestInvokeAuthorizer performs a mock test invocation of an authorizer.
func (b *InMemoryBackend) TestInvokeAuthorizer(input TestInvokeAuthorizerInput) (*TestInvokeAuthorizerOutput, error) {
	b.mu.RLock("TestInvokeAuthorizer")
	defer b.mu.RUnlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	if !b.authorizers.Has(authorizerKey(input.RestAPIID, input.AuthorizerID)) {
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

	if !b.restApis.Has(restAPIID) {
		return "", fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	var model *Model
	for _, m := range b.modelsByAPI.Get(restAPIID) {
		if m.Name == modelName {
			model = m

			break
		}
	}

	if model == nil {
		return "", fmt.Errorf("%w: model %s not found", ErrNotFound, modelName)
	}

	if model.Schema != "" {
		return model.Schema, nil
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
	gr, ok := b.gatewayResponses.Get(key)
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

	if !b.restApis.Has(restAPIID) {
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
		if gr, ok := b.gatewayResponses.Get(key); ok {
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

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

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

	b.gatewayResponses.Put(gr)

	return gr, nil
}

// UpdateGatewayResponse applies a partial (PATCH) update to a gateway response,
// merging only the fields present in input with the existing response (or with
// AWS's implicit default response for responseType, if none has been customized
// yet). Unlike PutGatewayResponse — which is a full wholesale replace used by the
// real PUT operation — UpdateGatewayResponse must not clobber
// ResponseParameters/ResponseTemplates/StatusCode that weren't part of this PATCH
// document, matching AWS's PATCH-operation semantics for this resource.
func (b *InMemoryBackend) UpdateGatewayResponse(input PutGatewayResponseInput) (*GatewayResponse, error) {
	b.mu.Lock("UpdateGatewayResponse")
	defer b.mu.Unlock()

	if !b.restApis.Has(input.RestAPIID) {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, input.RestAPIID)
	}

	key := gatewayResponseKey(input.RestAPIID, input.ResponseType)

	existing, ok := b.gatewayResponses.Get(key)
	if !ok {
		existing = &GatewayResponse{
			RestAPIID:       input.RestAPIID,
			ResponseType:    input.ResponseType,
			StatusCode:      gatewayResponseDefaultStatus(input.ResponseType),
			DefaultResponse: true,
		}
	}

	cp := *existing
	if input.StatusCode != "" {
		cp.StatusCode = input.StatusCode
	}
	if input.ResponseParameters != nil {
		cp.ResponseParameters = input.ResponseParameters
	}
	if input.ResponseTemplates != nil {
		cp.ResponseTemplates = input.ResponseTemplates
	}
	cp.DefaultResponse = false
	b.gatewayResponses.Put(&cp)

	return &cp, nil
}

// DeleteGatewayResponse removes a custom gateway response, reverting to default.
func (b *InMemoryBackend) DeleteGatewayResponse(restAPIID, responseType string) error {
	b.mu.Lock("DeleteGatewayResponse")
	defer b.mu.Unlock()

	if !b.restApis.Has(restAPIID) {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}

	key := gatewayResponseKey(restAPIID, responseType)
	b.gatewayResponses.Delete(key)

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

	b.clientCertificates.Put(cert)

	return cert, nil
}

// GetClientCertificate returns a client certificate by ID.
func (b *InMemoryBackend) GetClientCertificate(id string) (*ClientCertificate, error) {
	b.mu.RLock("GetClientCertificate")
	defer b.mu.RUnlock()

	cert, ok := b.clientCertificates.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: client certificate %s not found", ErrNotFound, id)
	}

	return cert, nil
}

// GetClientCertificates returns all client certificates.
func (b *InMemoryBackend) GetClientCertificates() ([]ClientCertificate, error) {
	b.mu.RLock("GetClientCertificates")
	defer b.mu.RUnlock()

	all := b.clientCertificates.All()
	result := make([]ClientCertificate, 0, len(all))
	for _, c := range all {
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

	if !b.clientCertificates.Delete(id) {
		return fmt.Errorf("%w: client certificate %s not found", ErrNotFound, id)
	}

	return nil
}

// GetUsage returns real per-key usage data for a usage plan. Each item is keyed
// by API key ID and contains a [used, remaining] pair reflecting the requests
// the data plane has actually metered against the plan's quota. If a key's
// remaining quota was explicitly overridden via UpdateUsage, that override
// takes precedence over the computed remaining value (mirroring AWS's
// "temporary extension to the remaining quota" semantics for UpdateUsage).
func (b *InMemoryBackend) GetUsage(input GetUsageInput) (*UsageData, error) {
	b.mu.RLock("GetUsage")
	defer b.mu.RUnlock()

	plan, ok := b.usagePlans.Get(input.UsagePlanID)
	if !ok {
		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, input.UsagePlanID)
	}

	items := make(map[string][]any)

	for _, upk := range b.usagePlanKeysByPlan.Get(input.UsagePlanID) {
		keyID := upk.ID
		used, remaining := b.usage.usageForKey(plan, keyID)
		if override, hasOverride := b.usageOverrides[input.UsagePlanID][keyID]; hasOverride {
			remaining = int(override)
		}
		items[keyID] = []any{[]int{used, remaining}}
	}

	return &UsageData{
		UsagePlanID: input.UsagePlanID,
		StartDate:   input.StartDate,
		EndDate:     input.EndDate,
		Items:       items,
	}, nil
}

// EnforceUsagePlan applies usage-plan quota and throttle limits for an API key on the
// given API stage. It returns nil when the request is allowed or when the key is not
// associated with a usage plan for the stage (unmetered, matching a bare API key),
// ErrQuotaExceeded when the period quota is exhausted, or ErrThrottled when the
// rate/burst limit is exceeded.
func (b *InMemoryBackend) EnforceUsagePlan(apiID, stageName, keyID string) error {
	b.mu.Lock("EnforceUsagePlan")
	defer b.mu.Unlock()

	plan := b.usagePlanForKeyLocked(keyID, apiID, stageName)
	if plan == nil {
		return nil
	}

	return b.usage.enforce(plan, apiID, stageName, keyID, time.Now())
}

// usagePlanForKeyLocked returns the usage plan that both contains keyID and is
// associated with the given api:stage, or nil. Callers must hold b.mu.
func (b *InMemoryBackend) usagePlanForKeyLocked(keyID, apiID, stageName string) *UsagePlan {
	// Deterministic iteration by plan ID so a key in multiple plans resolves stably.
	all := b.usagePlans.All()
	ids := make([]string, 0, len(all))
	for _, p := range all {
		ids = append(ids, p.ID)
	}
	sort.Strings(ids)

	for _, id := range ids {
		plan, _ := b.usagePlans.Get(id)
		if !b.usagePlanKeys.Has(usagePlanKeyKey(id, keyID)) {
			continue
		}
		for i := range plan.APIStages {
			st := plan.APIStages[i]
			if st.RestAPIID == apiID && st.Stage == stageName {
				return plan
			}
		}
	}

	return nil
}

// UpdateClientCertificate updates the description of a client certificate.
func (b *InMemoryBackend) UpdateClientCertificate(input UpdateClientCertificateInput) (*ClientCertificate, error) {
	b.mu.Lock("UpdateClientCertificate")
	defer b.mu.Unlock()

	cert, ok := b.clientCertificates.Get(input.ClientCertificateID)
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

	b.vpcLinks.Put(link)

	return link, nil
}

// GetVpcLink retrieves a VPC link by ID.
func (b *InMemoryBackend) GetVpcLink(id string) (*VpcLink, error) {
	b.mu.RLock("GetVpcLink")
	defer b.mu.RUnlock()

	link, ok := b.vpcLinks.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: VPC link %s not found", ErrNotFound, id)
	}

	return link, nil
}

// GetVpcLinks retrieves all VPC links.
func (b *InMemoryBackend) GetVpcLinks() ([]VpcLink, error) {
	b.mu.RLock("GetVpcLinks")
	defer b.mu.RUnlock()

	all := b.vpcLinks.All()
	result := make([]VpcLink, 0, len(all))
	for _, link := range all {
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

	if !b.vpcLinks.Delete(id) {
		return fmt.Errorf("%w: VPC link %s not found", ErrNotFound, id)
	}

	return nil
}

// UpdateVpcLink updates the name or description of a VPC link.
func (b *InMemoryBackend) UpdateVpcLink(input UpdateVpcLinkInput) (*VpcLink, error) {
	b.mu.Lock("UpdateVpcLink")
	defer b.mu.Unlock()

	link, ok := b.vpcLinks.Get(input.VpcLinkID)
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

	api, ok := b.restApis.Get(restAPIID)
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	ctx := exportContext{b: b, restAPIID: restAPIID, apiName: api.Name}

	if exportType == "oas30" {
		return buildOAS30Export(ctx, stageName), nil
	}

	return buildSwagger20Export(ctx, stageName), nil
}

// exportContext carries the read-only context buildSwagger20Export/buildOAS30Export
// and their helpers need to look up a REST API's resources/models from the
// backend's flat store.Table collections. It replaces the old *apiData
// parameter that these free functions took before the pkgs/store conversion
// flattened per-API nested maps into backend-level tables keyed by
// "<restAPIID>#<childID>".
type exportContext struct {
	b         *InMemoryBackend
	restAPIID string
	apiName   string
}

// buildSwagger20Export constructs a Swagger 2.0 export document.
func buildSwagger20Export(ctx exportContext, stageName string) map[string]any {
	paths := buildExportPaths(ctx, false)

	secDefs := map[string]any{
		exportKeyAPIKey: map[string]any{
			exportKeyType: "apiKey",
			keyAPIName:    "x-api-key",
			"in":          paramLocationHeader,
		},
	}

	return map[string]any{
		"swagger":             "2.0",
		"info":                map[string]any{"title": ctx.apiName, "version": "1.0"},
		"basePath":            "/" + stageName,
		"paths":               paths,
		"securityDefinitions": secDefs,
	}
}

// buildOAS30Export constructs an OpenAPI 3.0.1 export document.
func buildOAS30Export(ctx exportContext, stageName string) map[string]any {
	paths := buildExportPaths(ctx, true)

	components := map[string]any{
		"securitySchemes": map[string]any{
			exportKeyAPIKey: map[string]any{
				exportKeyType: "apiKey",
				keyAPIName:    "x-api-key",
				"in":          paramLocationHeader,
			},
		},
	}

	// Include model schemas in components.
	models := ctx.b.modelsByAPI.Get(ctx.restAPIID)
	if len(models) > 0 {
		schemas := make(map[string]any, len(models))
		for _, m := range models {
			schemas[m.Name] = map[string]any{
				exportKeyDescription: m.Description,
				exportKeyType:        exportKeyObject,
			}
		}
		components["schemas"] = schemas
	}

	return map[string]any{
		"openapi":    "3.0.1",
		"info":       map[string]any{"title": ctx.apiName, "version": "1.0"},
		"servers":    []map[string]any{{"url": "/" + stageName}},
		"paths":      paths,
		"components": components,
	}
}

// buildExportPaths constructs the paths object for an OpenAPI export.
// oas30=true emits OAS 3.0 operation objects; false emits Swagger 2.0.
func buildExportPaths(ctx exportContext, oas30 bool) map[string]any {
	paths := make(map[string]any)

	for _, res := range ctx.b.resourcesByAPI.Get(ctx.restAPIID) {
		if res.Path == "/" || len(res.ResourceMethods) == 0 {
			continue
		}

		pathItem := make(map[string]any)

		for httpMethod, method := range res.ResourceMethods {
			if method == nil {
				continue
			}

			op := buildExportOperation(ctx, method, oas30)
			pathItem[strings.ToLower(httpMethod)] = op
		}

		if len(pathItem) > 0 {
			paths[res.Path] = pathItem
		}
	}

	return paths
}

// buildExportOperation constructs a single OAS operation object for a method.
func buildExportOperation(ctx exportContext, method *Method, oas30 bool) map[string]any {
	op := make(map[string]any)
	op["responses"] = buildExportResponses(ctx, method, oas30)
	buildExportRequestBody(op, ctx, method, oas30)
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
func buildExportResponses(ctx exportContext, method *Method, oas30 bool) map[string]any {
	responses := make(map[string]any)

	for statusCode, mr := range method.MethodResponses {
		rsp := map[string]any{exportKeyDescription: statusCode + " response"}

		if len(mr.ResponseModels) > 0 {
			if oas30 {
				content := make(map[string]any)
				for ct, modelName := range mr.ResponseModels {
					content[ct] = map[string]any{exportKeySchema: buildModelRef(ctx, modelName, oas30)}
				}
				rsp["content"] = content
			} else {
				for _, modelName := range mr.ResponseModels {
					rsp[exportKeySchema] = buildModelRef(ctx, modelName, oas30)

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
func buildExportRequestBody(op map[string]any, ctx exportContext, method *Method, oas30 bool) {
	if len(method.RequestModels) == 0 {
		return
	}

	if oas30 {
		content := make(map[string]any)
		for ct, modelName := range method.RequestModels {
			content[ct] = map[string]any{exportKeySchema: buildModelRef(ctx, modelName, oas30)}
		}
		op["requestBody"] = map[string]any{"content": content}
	} else {
		for _, modelName := range method.RequestModels {
			op["consumes"] = []string{contentTypeJSON}
			op["parameters"] = []map[string]any{
				{
					"in":            exportKeyBody,
					"name":          exportKeyBody,
					exportKeySchema: buildModelRef(ctx, modelName, oas30),
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
		if method.AuthorizationType == AuthTypeCognitoUserPool {
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
//
// NOTE: this preserves a pre-existing quirk carried over mechanically from the
// map[string]*apiData days: the backend's models table is keyed by model ID
// (see modelKeyFn), but modelName here is the model's *Name*, not its ID -- so
// this lookup only succeeds when a model's Name happens to equal its ID. This
// was already the case before the store.Table conversion (the old
// data.models[modelName] indexed an ID-keyed map by name) and is preserved
// byte-for-byte rather than "fixed" as part of a storage-layer swap.
func buildModelRef(ctx exportContext, modelName string, oas30 bool) map[string]any {
	m, ok := ctx.b.models.Get(modelKey(ctx.restAPIID, modelName))
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
