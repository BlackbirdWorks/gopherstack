package apigatewayv2

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// defaultRegion is used for ARNs and execute-api endpoints when the request
// context carries no region (e.g. an unsigned request).
const defaultRegion = "us-east-1"

// regionFromCtx returns the request-scoped region from the ctxbag, falling back
// to the service default so endpoints/ARNs are always well-formed.
func regionFromCtx(ctx context.Context) string {
	if r := awsmeta.Region(ctx); r != "" {
		return r
	}

	return defaultRegion
}

func applyDomainNameDefaults(
	in []DomainNameConfiguration,
	domain, region string,
) []DomainNameConfiguration {
	configs := make([]DomainNameConfiguration, len(in))
	copy(configs, in)

	for i := range configs {
		if configs[i].DomainNameStatus == "" {
			configs[i].DomainNameStatus = "AVAILABLE"
		}

		if configs[i].SecurityPolicy == "" {
			configs[i].SecurityPolicy = "TLS_1_2"
		}

		if configs[i].EndpointType == "" {
			configs[i].EndpointType = "REGIONAL"
		}

		if configs[i].APIGatewayDomainName == "" {
			configs[i].APIGatewayDomainName = domain + ".execute-api." + region + ".amazonaws.com"
		}

		if configs[i].HostedZoneID == "" {
			configs[i].HostedZoneID = "Z2FDTNDATAQYW2"
		}
	}

	return configs
}

const (
	apiIDChars  = "abcdefghijklmnopqrstuvwxyz0123456789"
	apiIDLength = 10

	authorizerTypeJWT     = "JWT"
	authorizationTypeNone = "NONE"
	protocolTypeHTTP      = "HTTP"
	integrationTypeHTTP   = "HTTP"

	integrationTimeoutMin = int32(50)
	integrationTimeoutMax = int32(29000)
)

// isValidHTTPRouteKeyMethod reports whether method is accepted in an HTTP API route key.
func isValidHTTPRouteKeyMethod(method string) bool {
	switch method {
	case "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "ANY":
		return true
	default:
		return false
	}
}

// validateHTTPRouteKey returns ErrBadRequest if key is invalid for an HTTP API.
// Valid forms: "$default" or "METHOD /path" (e.g. "GET /items").
func validateHTTPRouteKey(key string) error {
	if key == "$default" {
		return nil
	}

	const maxParts = 2
	parts := strings.SplitN(key, " ", maxParts)
	if len(parts) != maxParts || !isValidHTTPRouteKeyMethod(parts[0]) || !strings.HasPrefix(parts[1], "/") {
		return fmt.Errorf(
			"%w: routeKey must be $default or start with a valid HTTP method and a forward slash, e.g. GET /items",
			ErrBadRequest,
		)
	}

	return nil
}

// validateTimeoutInMillis returns ErrBadRequest if ms is outside [50, 29000].
func validateTimeoutInMillis(ms int32) error {
	if ms < integrationTimeoutMin || ms > integrationTimeoutMax {
		return fmt.Errorf(
			"%w: timeoutInMillis must be between %d and %d",
			ErrBadRequest, integrationTimeoutMin, integrationTimeoutMax,
		)
	}

	return nil
}

var (
	// ErrAPINotFound is returned when a requested API does not exist.
	ErrAPINotFound = errors.New("NotFoundException")
	// ErrStageNotFound is returned when a requested stage does not exist.
	ErrStageNotFound = errors.New("NotFoundException")
	// ErrRouteNotFound is returned when a requested route does not exist.
	ErrRouteNotFound = errors.New("NotFoundException")
	// ErrIntegrationNotFound is returned when a requested integration does not exist.
	ErrIntegrationNotFound = errors.New("NotFoundException")
	// ErrDeploymentNotFound is returned when a requested deployment does not exist.
	ErrDeploymentNotFound = errors.New("NotFoundException")
	// ErrAuthorizerNotFound is returned when a requested authorizer does not exist.
	ErrAuthorizerNotFound = errors.New("NotFoundException")
	// ErrDomainNameNotFound is returned when a requested domain name does not exist.
	ErrDomainNameNotFound = errors.New("NotFoundException")
	// ErrAPIMappingNotFound is returned when a requested API mapping does not exist.
	ErrAPIMappingNotFound = errors.New("NotFoundException")
	// ErrIntegrationResponseNotFound is returned when a requested integration response does not exist.
	ErrIntegrationResponseNotFound = errors.New("NotFoundException")
	// ErrModelNotFound is returned when a requested model does not exist.
	ErrModelNotFound = errors.New("NotFoundException")
	// ErrRouteResponseNotFound is returned when a requested route response does not exist.
	ErrRouteResponseNotFound = errors.New("NotFoundException")
	// ErrPortalNotFound is returned when a requested portal does not exist.
	ErrPortalNotFound = errors.New("NotFoundException")
	// ErrPortalProductNotFound is returned when a requested portal product does not exist.
	ErrPortalProductNotFound = errors.New("NotFoundException")
	// ErrBadRequest is returned when required fields are missing or invalid.
	ErrBadRequest = errors.New("BadRequestException")
	// ErrAlreadyExists is returned when a resource with the same identifier already exists.
	ErrAlreadyExists = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	// ErrProductPageNotFound is returned when a requested product page does not exist.
	ErrProductPageNotFound = errors.New("NotFoundException")
	// ErrProductREPageNotFound is returned when a requested product REST endpoint page does not exist.
	ErrProductREPageNotFound = errors.New("NotFoundException")
	// ErrVpcLinkNotFound is returned when a requested VPC link does not exist.
	ErrVpcLinkNotFound = errors.New("NotFoundException")
	// ErrRoutingRuleNotFound is returned when a requested routing rule does not exist.
	ErrRoutingRuleNotFound = errors.New("NotFoundException")
)

const (
	// IntegrationTypeAWSProxy is the AWS_PROXY integration type.
	IntegrationTypeAWSProxy = "AWS_PROXY"
	// integrationTypeHTTPProxy is the HTTP_PROXY integration type.
	integrationTypeHTTPProxy = "HTTP_PROXY"
)

// StorageBackend is the interface for the API Gateway v2 in-memory store.
type StorageBackend interface {
	// APIs
	CreateAPI(ctx context.Context, input CreateAPIInput) (*API, error)
	GetAPI(apiID string) (*API, error)
	GetAPIs() ([]API, error)
	DeleteAPI(apiID string) error
	UpdateAPI(apiID string, input UpdateAPIInput) (*API, error)

	// Stages
	CreateStage(apiID string, input CreateStageInput) (*Stage, error)
	GetStage(apiID, stageName string) (*Stage, error)
	GetStages(apiID string) ([]Stage, error)
	DeleteStage(apiID, stageName string) error
	UpdateStage(apiID, stageName string, input UpdateStageInput) (*Stage, error)

	// Routes
	CreateRoute(apiID string, input CreateRouteInput) (*Route, error)
	GetRoute(apiID, routeID string) (*Route, error)
	GetRoutes(apiID string) ([]Route, error)
	DeleteRoute(apiID, routeID string) error
	UpdateRoute(apiID, routeID string, input UpdateRouteInput) (*Route, error)

	// Integrations
	CreateIntegration(apiID string, input CreateIntegrationInput) (*Integration, error)
	GetIntegration(apiID, integrationID string) (*Integration, error)
	GetIntegrations(apiID string) ([]Integration, error)
	DeleteIntegration(apiID, integrationID string) error
	UpdateIntegration(apiID, integrationID string, input UpdateIntegrationInput) (*Integration, error)

	// Deployments
	CreateDeployment(apiID string, input CreateDeploymentInput) (*Deployment, error)
	GetDeployment(apiID, deploymentID string) (*Deployment, error)
	GetDeployments(apiID string) ([]Deployment, error)
	DeleteDeployment(apiID, deploymentID string) error

	// Authorizers
	CreateAuthorizer(apiID string, input CreateAuthorizerInput) (*Authorizer, error)
	GetAuthorizer(apiID, authorizerID string) (*Authorizer, error)
	GetAuthorizers(apiID string) ([]Authorizer, error)
	DeleteAuthorizer(apiID, authorizerID string) error
	UpdateAuthorizer(apiID, authorizerID string, input UpdateAuthorizerInput) (*Authorizer, error)

	// Domain Names
	CreateDomainName(ctx context.Context, input CreateDomainNameInput) (*DomainName, error)

	// API Mappings
	CreateAPIMapping(domainName string, input CreateAPIMappingInput) (*APIMapping, error)

	// Integration Responses
	CreateIntegrationResponse(
		apiID, integrationID string,
		input CreateIntegrationResponseInput,
	) (*IntegrationResponse, error)

	// Models
	CreateModel(apiID string, input CreateModelInput) (*Model, error)

	// Route Responses
	CreateRouteResponse(apiID, routeID string, input CreateRouteResponseInput) (*RouteResponse, error)

	// Portals
	CreatePortal(input CreatePortalInput) (*Portal, error)

	// Portal Products
	CreatePortalProduct(input CreatePortalProductInput) (*PortalProduct, error)

	// Product Pages
	CreateProductPage(portalProductID string, input CreateProductPageInput) (*ProductPage, error)

	// Product REST Endpoint Pages
	CreateProductRestEndpointPage(
		portalProductID string,
		input CreateProductRestEndpointPageInput,
	) (*ProductRestEndpointPage, error)

	// VPC links
	CreateVpcLink(input CreateVpcLinkInput) (*VpcLink, error)
	GetVpcLink(vpcLinkID string) (*VpcLink, error)
	GetVpcLinks() ([]VpcLink, error)
	UpdateVpcLink(vpcLinkID string, input UpdateVpcLinkInput) (*VpcLink, error)
	DeleteVpcLink(vpcLinkID string) error

	// Routing rules
	CreateRoutingRule(
		ctx context.Context,
		domainName string,
		input CreateRoutingRuleInput,
	) (*RoutingRule, error)
	GetRoutingRule(domainName, routingRuleID string) (*RoutingRule, error)
	ListRoutingRules(domainName string) ([]RoutingRule, error)
	PutRoutingRule(domainName, routingRuleID string, input PutRoutingRuleInput) (*RoutingRule, error)
	DeleteRoutingRule(domainName, routingRuleID string) error

	// Portal sharing policy
	GetPortalProductSharingPolicy(portalProductID string) (*PortalProductSharingPolicy, error)
	PutPortalProductSharingPolicy(portalProductID, policyDocument string) (*PortalProductSharingPolicy, error)
	DeletePortalProductSharingPolicy(portalProductID string) error

	// Domain Names - Get/Delete
	GetDomainName(domainName string) (*DomainName, error)
	GetDomainNames() ([]DomainName, error)
	DeleteDomainName(domainName string) error

	// API Mappings - Get/Delete
	GetAPIMapping(domainName, mappingID string) (*APIMapping, error)
	GetAPIMappings(domainName string) ([]APIMapping, error)
	DeleteAPIMapping(domainName, mappingID string) error

	// Integration Responses - Get/Delete
	GetIntegrationResponse(apiID, integrationID, responseID string) (*IntegrationResponse, error)
	GetIntegrationResponses(apiID, integrationID string) ([]IntegrationResponse, error)
	DeleteIntegrationResponse(apiID, integrationID, responseID string) error

	// Models - Get/Delete
	GetModel(apiID, modelID string) (*Model, error)
	GetModels(apiID string) ([]Model, error)
	DeleteModel(apiID, modelID string) error

	// Route Responses - Get/Delete
	GetRouteResponse(apiID, routeID, responseID string) (*RouteResponse, error)
	GetRouteResponses(apiID, routeID string) ([]RouteResponse, error)
	DeleteRouteResponse(apiID, routeID, responseID string) error

	// Portals - Get/List
	GetPortal(portalID string) (*Portal, error)
	ListPortals() ([]Portal, error)

	// Portal Products - Get/List
	GetPortalProduct(portalProductID string) (*PortalProduct, error)
	ListPortalProducts() ([]PortalProduct, error)

	// Product Pages - List
	ListProductPages(portalProductID string) ([]ProductPage, error)

	// Product REST Endpoint Pages - List
	ListProductRestEndpointPages(portalProductID string) ([]ProductRestEndpointPage, error)

	// Tags
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	GetTags(resourceARN string) (map[string]string, error)

	// ExportAPI generates an OpenAPI specification for the API's routes.
	ExportAPI(apiID string) (map[string]any, error)

	// UpdateAPIMapping
	UpdateAPIMapping(domainName, mappingID string, input UpdateAPIMappingInput) (*APIMapping, error)

	// UpdateDeployment
	UpdateDeployment(apiID, deploymentID string, input UpdateDeploymentInput) (*Deployment, error)

	// UpdateDomainName
	UpdateDomainName(domainName string, input UpdateDomainNameInput) (*DomainName, error)

	// UpdateIntegrationResponse
	UpdateIntegrationResponse(
		apiID, integrationID, responseID string,
		input UpdateIntegrationResponseInput,
	) (*IntegrationResponse, error)

	// UpdateModel
	UpdateModel(apiID, modelID string, input UpdateModelInput) (*Model, error)

	// UpdateRouteResponse
	UpdateRouteResponse(apiID, routeID, responseID string, input UpdateRouteResponseInput) (*RouteResponse, error)

	// UpdatePortal
	UpdatePortal(portalID string, input UpdatePortalInput) (*Portal, error)

	// UpdatePortalProduct
	UpdatePortalProduct(portalProductID string, input UpdatePortalProductInput) (*PortalProduct, error)

	// UpdateProductPage
	UpdateProductPage(portalProductID, pageID string, input UpdateProductPageInput) (*ProductPage, error)

	// UpdateProductRestEndpointPage
	UpdateProductRestEndpointPage(
		portalProductID, pageID string,
		input UpdateProductRestEndpointPageInput,
	) (*ProductRestEndpointPage, error)

	// DeletePortal
	DeletePortal(portalID string) error

	// DeletePortalProduct
	DeletePortalProduct(portalProductID string) error

	// GetProductPage
	GetProductPage(portalProductID, pageID string) (*ProductPage, error)

	// GetProductRestEndpointPage
	GetProductRestEndpointPage(portalProductID, pageID string) (*ProductRestEndpointPage, error)

	// DeleteProductPage
	DeleteProductPage(portalProductID, pageID string) error

	// DeleteProductRestEndpointPage
	DeleteProductRestEndpointPage(portalProductID, pageID string) error

	// ResetAuthorizersCache
	ResetAuthorizersCache(apiID, stageName string) error

	// DeleteCorsConfiguration clears the CORS configuration on an API.
	DeleteCorsConfiguration(apiID string) error

	// DeleteAccessLogSettings clears the access log settings on a stage.
	DeleteAccessLogSettings(apiID, stageName string) error

	// DeleteRouteSettings removes per-route settings for a specific routeKey from a stage.
	DeleteRouteSettings(apiID, stageName, routeKey string) error

	// DeleteRouteRequestParameter removes a specific request parameter from a route.
	DeleteRouteRequestParameter(apiID, routeID, requestParameterKey string) error
}

// apiData holds per-API state.
type apiData struct {
	stages               map[string]*Stage
	routes               map[string]*Route
	integrations         map[string]*Integration
	deployments          map[string]*Deployment
	authorizers          map[string]*Authorizer
	integrationResponses map[string]map[string]*IntegrationResponse
	models               map[string]*Model
	routeResponses       map[string]map[string]*RouteResponse
	api                  API
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	apis                         map[string]*apiData
	domainNames                  map[string]*DomainName
	apiMappings                  map[string]map[string]*APIMapping
	portals                      map[string]*Portal
	portalProducts               map[string]*PortalProduct
	portalProductSharingPolicies map[string]string
	productPages                 map[string][]*ProductPage             // key: portalProductID
	productREPages               map[string][]*ProductRestEndpointPage // key: portalProductID
	vpcLinks                     map[string]*VpcLink
	routingRules                 map[string]map[string]*RoutingRule
	mu                           *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		apis:                         make(map[string]*apiData),
		domainNames:                  make(map[string]*DomainName),
		apiMappings:                  make(map[string]map[string]*APIMapping),
		portals:                      make(map[string]*Portal),
		portalProducts:               make(map[string]*PortalProduct),
		portalProductSharingPolicies: make(map[string]string),
		productPages:                 make(map[string][]*ProductPage),
		productREPages:               make(map[string][]*ProductRestEndpointPage),
		vpcLinks:                     make(map[string]*VpcLink),
		routingRules:                 make(map[string]map[string]*RoutingRule),
		mu:                           lockmetrics.New("apigatewayv2"),
	}
}

// copyTags returns a deep copy of a tags map, guarding against nil.
func copyTags(src map[string]string) map[string]string {
	if src == nil {
		return nil
	}

	return maps.Clone(src)
}

// Reset clears all backend state, reinitialising all maps.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.apis = make(map[string]*apiData)
	b.domainNames = make(map[string]*DomainName)
	b.apiMappings = make(map[string]map[string]*APIMapping)
	b.portals = make(map[string]*Portal)
	b.portalProducts = make(map[string]*PortalProduct)
	b.portalProductSharingPolicies = make(map[string]string)
	b.productPages = make(map[string][]*ProductPage)
	b.productREPages = make(map[string][]*ProductRestEndpointPage)
	b.vpcLinks = make(map[string]*VpcLink)
	b.routingRules = make(map[string]map[string]*RoutingRule)
}

// randomID generates a cryptographically random 10-character alphanumeric ID.
func randomID() string {
	b := make([]byte, apiIDLength)
	charCount := uint64(len(apiIDChars))

	for i := range b {
		var v [8]byte
		// crypto/rand.Read always fills the buffer and never returns a non-nil error.
		_, _ = rand.Read(v[:])
		b[i] = apiIDChars[binary.BigEndian.Uint64(v[:])%charCount]
	}

	return string(b)
}

// --- APIs ---

// CreateAPI creates a new HTTP API.
func (b *InMemoryBackend) CreateAPI(ctx context.Context, input CreateAPIInput) (*API, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrBadRequest)
	}

	b.mu.Lock("CreateAPI")
	defer b.mu.Unlock()

	validProtocols := map[string]bool{protocolTypeHTTP: true, protocolTypeWebSocket: true}
	if !validProtocols[input.ProtocolType] {
		return nil, fmt.Errorf("%w: protocolType must be HTTP or WEBSOCKET", ErrBadRequest)
	}

	// Apply AWS-realistic default RouteSelectionExpression when not provided.
	rse := input.RouteSelectionExpression
	if rse == "" {
		if input.ProtocolType == protocolTypeWebSocket {
			rse = "$request.body.action"
		} else {
			rse = "${request.method} ${request.path}"
		}
	}

	// Apply AWS-realistic default APIKeySelectionExpression when not provided.
	keySelExpr := input.APIKeySelectionExpression
	if keySelExpr == "" {
		if input.ProtocolType == protocolTypeWebSocket {
			keySelExpr = "$context.authorizer.usageIdentifierKey"
		} else {
			keySelExpr = "$request.header.x-api-key"
		}
	}

	id := randomID()
	api := API{
		APIID:                     id,
		Name:                      input.Name,
		Description:               input.Description,
		ProtocolType:              input.ProtocolType,
		RouteSelectionExpression:  rse,
		Version:                   input.Version,
		Tags:                      copyTags(input.Tags),
		APIEndpoint:               "https://" + id + ".execute-api." + regionFromCtx(ctx) + ".amazonaws.com",
		CreatedDate:               isoTime{time.Now()},
		APIKeySelectionExpression: keySelExpr,
		DisableSchemaValidation:   input.DisableSchemaValidation,
		DisableExecuteAPIEndpoint: input.DisableExecuteAPIEndpoint,
	}

	if input.CorsConfiguration != nil {
		clone := *input.CorsConfiguration
		api.CorsConfiguration = &clone
	}

	b.apis[id] = &apiData{
		api:                  api,
		stages:               make(map[string]*Stage),
		routes:               make(map[string]*Route),
		integrations:         make(map[string]*Integration),
		deployments:          make(map[string]*Deployment),
		authorizers:          make(map[string]*Authorizer),
		integrationResponses: make(map[string]map[string]*IntegrationResponse),
		models:               make(map[string]*Model),
		routeResponses:       make(map[string]map[string]*RouteResponse),
	}

	return &api, nil
}

// GetAPI retrieves an API by ID.
func (b *InMemoryBackend) GetAPI(apiID string) (*API, error) {
	b.mu.RLock("GetAPI")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	cp := d.api

	return &cp, nil
}

// GetAPIs retrieves all APIs.
func (b *InMemoryBackend) GetAPIs() ([]API, error) {
	b.mu.RLock("GetAPIs")
	defer b.mu.RUnlock()

	result := make([]API, 0, len(b.apis))
	for _, d := range b.apis {
		result = append(result, d.api)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].APIID < result[j].APIID
	})

	return result, nil
}

// DeleteAPI removes an API by ID.
func (b *InMemoryBackend) DeleteAPI(apiID string) error {
	b.mu.Lock("DeleteAPI")
	defer b.mu.Unlock()

	if _, ok := b.apis[apiID]; !ok {
		return ErrAPINotFound
	}

	delete(b.apis, apiID)

	// Clean up stale API mappings pointing to this API.
	for _, mappings := range b.apiMappings {
		for id, m := range mappings {
			if m.APIID == apiID {
				delete(mappings, id)
			}
		}
	}

	return nil
}

// UpdateAPI updates fields on an existing API.
func (b *InMemoryBackend) UpdateAPI(apiID string, input UpdateAPIInput) (*API, error) {
	b.mu.Lock("UpdateAPI")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	if input.Name != "" {
		d.api.Name = input.Name
	}

	if input.Description != "" {
		d.api.Description = input.Description
	}

	if input.RouteSelectionExpression != "" {
		d.api.RouteSelectionExpression = input.RouteSelectionExpression
	}

	if input.Version != "" {
		d.api.Version = input.Version
	}

	if input.Tags != nil {
		d.api.Tags = copyTags(input.Tags)
	}

	if input.APIKeySelectionExpression != "" {
		d.api.APIKeySelectionExpression = input.APIKeySelectionExpression
	}

	if input.CorsConfiguration != nil {
		clone := *input.CorsConfiguration
		d.api.CorsConfiguration = &clone
	}

	if input.DisableSchemaValidation != nil {
		d.api.DisableSchemaValidation = *input.DisableSchemaValidation
	}

	if input.DisableExecuteAPIEndpoint != nil {
		d.api.DisableExecuteAPIEndpoint = *input.DisableExecuteAPIEndpoint
	}

	cp := d.api

	return &cp, nil
}

// --- Stages ---

// CreateStage creates a new stage for an API.
func (b *InMemoryBackend) CreateStage(apiID string, input CreateStageInput) (*Stage, error) {
	b.mu.Lock("CreateStage")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	if input.StageName == "" {
		return nil, fmt.Errorf("%w: stageName is required", ErrBadRequest)
	}

	if _, exists := d.stages[input.StageName]; exists {
		return nil, fmt.Errorf("%w: stage %q already exists", ErrAlreadyExists, input.StageName)
	}

	now := isoTime{time.Now()}
	stage := &Stage{
		StageName:            input.StageName,
		APIID:                apiID,
		DeploymentID:         input.DeploymentID,
		Description:          input.Description,
		AutoDeploy:           input.AutoDeploy,
		StageVariables:       input.StageVariables,
		CreatedDate:          now,
		LastUpdatedDate:      now,
		AccessLogSettings:    input.AccessLogSettings,
		DefaultRouteSettings: input.DefaultRouteSettings,
		RouteSettings:        input.RouteSettings,
	}

	d.stages[input.StageName] = stage

	cp := *stage

	return &cp, nil
}

// GetStage retrieves a stage by name.
func (b *InMemoryBackend) GetStage(apiID, stageName string) (*Stage, error) {
	b.mu.RLock("GetStage")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	s, ok := d.stages[stageName]
	if !ok {
		return nil, ErrStageNotFound
	}

	cp := *s

	return &cp, nil
}

// GetStages retrieves all stages for an API.
func (b *InMemoryBackend) GetStages(apiID string) ([]Stage, error) {
	b.mu.RLock("GetStages")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	result := make([]Stage, 0, len(d.stages))
	for _, s := range d.stages {
		result = append(result, *s)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].StageName < result[j].StageName
	})

	return result, nil
}

// DeleteStage removes a stage from an API.
func (b *InMemoryBackend) DeleteStage(apiID, stageName string) error {
	b.mu.Lock("DeleteStage")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	if _, exists := d.stages[stageName]; !exists {
		return ErrStageNotFound
	}

	delete(d.stages, stageName)

	return nil
}

// UpdateStage updates fields on an existing stage.
func (b *InMemoryBackend) UpdateStage(apiID, stageName string, input UpdateStageInput) (*Stage, error) {
	b.mu.Lock("UpdateStage")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	s, ok := d.stages[stageName]
	if !ok {
		return nil, ErrStageNotFound
	}

	if input.DeploymentID != "" {
		s.DeploymentID = input.DeploymentID
	}

	if input.Description != "" {
		s.Description = input.Description
	}

	if input.AutoDeploy != nil {
		s.AutoDeploy = *input.AutoDeploy
	}

	if input.StageVariables != nil {
		s.StageVariables = input.StageVariables
	}

	if input.AccessLogSettings != nil {
		clone := *input.AccessLogSettings
		s.AccessLogSettings = &clone
	}

	if input.DefaultRouteSettings != nil {
		clone := *input.DefaultRouteSettings
		s.DefaultRouteSettings = &clone
	}

	if input.RouteSettings != nil {
		s.RouteSettings = input.RouteSettings
	}

	s.LastUpdatedDate = isoTime{time.Now()}

	cp := *s

	return &cp, nil
}

// --- Routes ---

// CreateRoute creates a new route for an API.
func (b *InMemoryBackend) CreateRoute(apiID string, input CreateRouteInput) (*Route, error) {
	b.mu.Lock("CreateRoute")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	if input.RouteKey == "" {
		return nil, fmt.Errorf("%w: routeKey is required", ErrBadRequest)
	}

	if d.api.ProtocolType == protocolTypeHTTP {
		if err := validateHTTPRouteKey(input.RouteKey); err != nil {
			return nil, err
		}
	}

	for _, existing := range d.routes {
		if existing.RouteKey == input.RouteKey {
			return nil, fmt.Errorf("%w: route key %q already exists", ErrAlreadyExists, input.RouteKey)
		}
	}

	authType := input.AuthorizationType
	if authType == "" {
		authType = authorizationTypeNone
	}

	if authType == authorizerTypeJWT && input.AuthorizerID == "" {
		return nil, fmt.Errorf("%w: authorizerId is required for JWT authorization", ErrBadRequest)
	}

	authScopes := input.AuthorizationScopes
	if authScopes == nil {
		authScopes = []string{}
	}

	id := randomID()
	route := &Route{
		RouteID:                  id,
		APIID:                    apiID,
		RouteKey:                 input.RouteKey,
		Target:                   input.Target,
		AuthorizationType:        authType,
		AuthorizerID:             input.AuthorizerID,
		OperationName:            input.OperationName,
		ModelSelectionExpression: input.ModelSelectionExpression,
		RequestModels:            input.RequestModels,
		RequestParameters:        input.RequestParameters,
		AuthorizationScopes:      authScopes,
		APIKeyRequired:           input.APIKeyRequired,
	}

	d.routes[id] = route

	cp := *route

	return &cp, nil
}

// GetRoute retrieves a route by ID.
func (b *InMemoryBackend) GetRoute(apiID, routeID string) (*Route, error) {
	b.mu.RLock("GetRoute")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	r, ok := d.routes[routeID]
	if !ok {
		return nil, ErrRouteNotFound
	}

	cp := *r

	return &cp, nil
}

// GetRoutes retrieves all routes for an API.
func (b *InMemoryBackend) GetRoutes(apiID string) ([]Route, error) {
	b.mu.RLock("GetRoutes")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	result := make([]Route, 0, len(d.routes))
	for _, r := range d.routes {
		result = append(result, *r)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RouteID < result[j].RouteID
	})

	return result, nil
}

// DeleteRoute removes a route from an API.
func (b *InMemoryBackend) DeleteRoute(apiID, routeID string) error {
	b.mu.Lock("DeleteRoute")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	if _, exists := d.routes[routeID]; !exists {
		return ErrRouteNotFound
	}

	delete(d.routes, routeID)
	delete(d.routeResponses, routeID)

	return nil
}

// setRouteKey validates newKey for protocolType and ensures it is not a duplicate
// among routes (excluding the route being updated), then sets r.RouteKey.
func setRouteKey(r *Route, routes map[string]*Route, routeID, newKey, protocolType string) error {
	if protocolType == protocolTypeHTTP {
		if err := validateHTTPRouteKey(newKey); err != nil {
			return err
		}
	}

	for id, existing := range routes {
		if id != routeID && existing.RouteKey == newKey {
			return fmt.Errorf("%w: route key %q already exists", ErrAlreadyExists, newKey)
		}
	}

	r.RouteKey = newKey

	return nil
}

// UpdateRoute updates fields on an existing route.
func (b *InMemoryBackend) UpdateRoute(apiID, routeID string, input UpdateRouteInput) (*Route, error) {
	b.mu.Lock("UpdateRoute")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	r, ok := d.routes[routeID]
	if !ok {
		return nil, ErrRouteNotFound
	}

	if input.RouteKey != "" {
		if err := setRouteKey(r, d.routes, routeID, input.RouteKey, d.api.ProtocolType); err != nil {
			return nil, err
		}
	}

	if input.Target != "" {
		r.Target = input.Target
	}

	if input.AuthorizationType != "" {
		r.AuthorizationType = input.AuthorizationType
		if input.AuthorizationType == authorizationTypeNone {
			r.AuthorizerID = ""
		}
	}

	if input.AuthorizerID != "" {
		r.AuthorizerID = input.AuthorizerID
	}

	if input.OperationName != "" {
		r.OperationName = input.OperationName
	}

	if input.ModelSelectionExpression != "" {
		r.ModelSelectionExpression = input.ModelSelectionExpression
	}

	if input.RequestModels != nil {
		r.RequestModels = input.RequestModels
	}

	if input.RequestParameters != nil {
		r.RequestParameters = input.RequestParameters
	}

	if input.AuthorizationScopes != nil {
		r.AuthorizationScopes = input.AuthorizationScopes
	}

	if input.APIKeyRequired != nil {
		r.APIKeyRequired = *input.APIKeyRequired
	}

	cp := *r

	return &cp, nil
}

// --- Integrations ---

// CreateIntegration creates a new integration for an API.
func (b *InMemoryBackend) CreateIntegration(apiID string, input CreateIntegrationInput) (*Integration, error) {
	b.mu.Lock("CreateIntegration")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	validTypes := map[string]bool{
		"AWS":                    true,
		integrationTypeHTTP:      true,
		"MOCK":                   true,
		IntegrationTypeAWSProxy:  true,
		integrationTypeHTTPProxy: true,
	}
	if !validTypes[input.IntegrationType] {
		return nil, fmt.Errorf(
			"%w: integrationType must be one of AWS, HTTP, MOCK, AWS_PROXY, HTTP_PROXY",
			ErrBadRequest,
		)
	}

	// Apply AWS-realistic defaults.
	payloadFmtVer := input.PayloadFormatVersion
	if payloadFmtVer == "" && input.IntegrationType == IntegrationTypeAWSProxy {
		payloadFmtVer = "1.0"
	}

	passthroughBehavior := input.PassthroughBehavior
	if passthroughBehavior == "" && input.IntegrationType == integrationTypeHTTPProxy {
		passthroughBehavior = "WHEN_NO_MATCH"
	}

	timeoutMs := input.TimeoutInMillis
	if timeoutMs == 0 {
		timeoutMs = integrationTimeoutMax
	} else if err := validateTimeoutInMillis(timeoutMs); err != nil {
		return nil, err
	}

	id := randomID()
	integration := &Integration{
		IntegrationID:               id,
		APIID:                       apiID,
		IntegrationType:             input.IntegrationType,
		IntegrationSubtype:          input.IntegrationSubtype,
		IntegrationMethod:           input.IntegrationMethod,
		IntegrationURI:              input.IntegrationURI,
		Description:                 input.Description,
		PayloadFormatVersion:        payloadFmtVer,
		ConnectionType:              input.ConnectionType,
		ConnectionID:                input.ConnectionID,
		TimeoutInMillis:             timeoutMs,
		RequestParameters:           input.RequestParameters,
		RequestTemplates:            input.RequestTemplates,
		TemplateSelectionExpression: input.TemplateSelectionExpression,
		PassthroughBehavior:         passthroughBehavior,
	}

	d.integrations[id] = integration

	cp := *integration

	return &cp, nil
}

// GetIntegration retrieves an integration by ID.
func (b *InMemoryBackend) GetIntegration(apiID, integrationID string) (*Integration, error) {
	b.mu.RLock("GetIntegration")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	i, ok := d.integrations[integrationID]
	if !ok {
		return nil, ErrIntegrationNotFound
	}

	cp := *i

	return &cp, nil
}

// GetIntegrations retrieves all integrations for an API.
func (b *InMemoryBackend) GetIntegrations(apiID string) ([]Integration, error) {
	b.mu.RLock("GetIntegrations")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	result := make([]Integration, 0, len(d.integrations))
	for _, i := range d.integrations {
		result = append(result, *i)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].IntegrationID < result[j].IntegrationID
	})

	return result, nil
}

// DeleteIntegration removes an integration from an API.
func (b *InMemoryBackend) DeleteIntegration(apiID, integrationID string) error {
	b.mu.Lock("DeleteIntegration")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	if _, exists := d.integrations[integrationID]; !exists {
		return ErrIntegrationNotFound
	}

	delete(d.integrations, integrationID)
	delete(d.integrationResponses, integrationID)

	return nil
}

// UpdateIntegration updates fields on an existing integration.
// applyIntegrationUpdate copies non-zero fields from input onto the integration.
func applyIntegrationUpdate(i *Integration, input UpdateIntegrationInput) {
	if input.IntegrationType != "" {
		i.IntegrationType = input.IntegrationType
	}

	if input.IntegrationSubtype != "" {
		i.IntegrationSubtype = input.IntegrationSubtype
	}

	if input.IntegrationMethod != "" {
		i.IntegrationMethod = input.IntegrationMethod
	}

	if input.IntegrationURI != "" {
		i.IntegrationURI = input.IntegrationURI
	}

	if input.Description != "" {
		i.Description = input.Description
	}

	if input.PayloadFormatVersion != "" {
		i.PayloadFormatVersion = input.PayloadFormatVersion
	}

	if input.ConnectionType != "" {
		i.ConnectionType = input.ConnectionType
	}

	if input.ConnectionID != "" {
		i.ConnectionID = input.ConnectionID
	}

	if input.TimeoutInMillis != 0 {
		i.TimeoutInMillis = input.TimeoutInMillis
	}

	if input.RequestParameters != nil {
		i.RequestParameters = input.RequestParameters
	}

	if input.RequestTemplates != nil {
		i.RequestTemplates = input.RequestTemplates
	}

	if input.TemplateSelectionExpression != "" {
		i.TemplateSelectionExpression = input.TemplateSelectionExpression
	}

	if input.PassthroughBehavior != "" {
		i.PassthroughBehavior = input.PassthroughBehavior
	}
}

// UpdateIntegration updates fields on an existing integration.
func (b *InMemoryBackend) UpdateIntegration(
	apiID, integrationID string,
	input UpdateIntegrationInput,
) (*Integration, error) {
	b.mu.Lock("UpdateIntegration")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	i, ok := d.integrations[integrationID]
	if !ok {
		return nil, ErrIntegrationNotFound
	}

	if input.TimeoutInMillis != 0 {
		if err := validateTimeoutInMillis(input.TimeoutInMillis); err != nil {
			return nil, err
		}
	}

	applyIntegrationUpdate(i, input)

	cp := *i

	return &cp, nil
}

// --- Deployments ---

// CreateDeployment creates a new deployment for an API.
func (b *InMemoryBackend) CreateDeployment(apiID string, input CreateDeploymentInput) (*Deployment, error) {
	b.mu.Lock("CreateDeployment")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	id := randomID()
	deployment := &Deployment{
		DeploymentID:     id,
		APIID:            apiID,
		Description:      input.Description,
		DeploymentStatus: "DEPLOYED",
		CreatedDate:      isoTime{time.Now()},
	}

	d.deployments[id] = deployment

	// When a stage name is provided, link the deployment to that stage (AWS behaviour).
	if input.StageName != "" {
		s, stageExists := d.stages[input.StageName]
		if !stageExists {
			return nil, ErrStageNotFound
		}

		s.DeploymentID = id
	}

	cp := *deployment

	return &cp, nil
}

// GetDeployment retrieves a deployment by ID.
func (b *InMemoryBackend) GetDeployment(apiID, deploymentID string) (*Deployment, error) {
	b.mu.RLock("GetDeployment")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	dep, ok := d.deployments[deploymentID]
	if !ok {
		return nil, ErrDeploymentNotFound
	}

	cp := *dep

	return &cp, nil
}

// GetDeployments retrieves all deployments for an API.
func (b *InMemoryBackend) GetDeployments(apiID string) ([]Deployment, error) {
	b.mu.RLock("GetDeployments")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	result := make([]Deployment, 0, len(d.deployments))
	for _, dep := range d.deployments {
		result = append(result, *dep)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].DeploymentID < result[j].DeploymentID
	})

	return result, nil
}

// DeleteDeployment removes a deployment from an API.
func (b *InMemoryBackend) DeleteDeployment(apiID, deploymentID string) error {
	b.mu.Lock("DeleteDeployment")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	if _, exists := d.deployments[deploymentID]; !exists {
		return ErrDeploymentNotFound
	}

	delete(d.deployments, deploymentID)

	return nil
}

// --- Authorizers ---

// CreateAuthorizer creates a new authorizer for an API.
func (b *InMemoryBackend) CreateAuthorizer(apiID string, input CreateAuthorizerInput) (*Authorizer, error) {
	b.mu.Lock("CreateAuthorizer")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrBadRequest)
	}

	validTypes := map[string]bool{authorizerTypeJWT: true, "REQUEST": true, "CUSTOM": true}
	if !validTypes[input.AuthorizerType] {
		return nil, fmt.Errorf("%w: authorizerType must be JWT, REQUEST, or CUSTOM", ErrBadRequest)
	}

	// Validate JWT authorizer requires JwtConfiguration.
	if input.AuthorizerType == authorizerTypeJWT && input.JwtConfiguration == nil {
		return nil, fmt.Errorf("%w: jwtConfiguration is required for JWT authorizers", ErrBadRequest)
	}

	// Apply AWS-realistic defaults for IdentitySource.
	identitySource := input.IdentitySource
	if len(identitySource) == 0 && input.AuthorizerType == authorizerTypeJWT {
		identitySource = []string{"$request.header.Authorization"}
	}

	id := randomID()
	authorizer := &Authorizer{
		AuthorizerID:                   id,
		APIID:                          apiID,
		Name:                           input.Name,
		AuthorizerType:                 input.AuthorizerType,
		AuthorizerURI:                  input.AuthorizerURI,
		IdentitySource:                 identitySource,
		AuthorizerCredentialsArn:       input.AuthorizerCredentialsArn,
		AuthorizerResultTTLInSeconds:   input.AuthorizerResultTTLInSeconds,
		AuthorizerPayloadFormatVersion: input.AuthorizerPayloadFormatVersion,
		EnableSimpleResponses:          input.EnableSimpleResponses,
	}

	if input.JwtConfiguration != nil {
		clone := *input.JwtConfiguration
		authorizer.JwtConfiguration = &clone
	}

	d.authorizers[id] = authorizer

	cp := *authorizer

	return &cp, nil
}

// GetAuthorizer retrieves an authorizer by ID.
func (b *InMemoryBackend) GetAuthorizer(apiID, authorizerID string) (*Authorizer, error) {
	b.mu.RLock("GetAuthorizer")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	a, ok := d.authorizers[authorizerID]
	if !ok {
		return nil, ErrAuthorizerNotFound
	}

	cp := *a

	return &cp, nil
}

// GetAuthorizers retrieves all authorizers for an API.
func (b *InMemoryBackend) GetAuthorizers(apiID string) ([]Authorizer, error) {
	b.mu.RLock("GetAuthorizers")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	result := make([]Authorizer, 0, len(d.authorizers))
	for _, a := range d.authorizers {
		result = append(result, *a)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AuthorizerID < result[j].AuthorizerID
	})

	return result, nil
}

// DeleteAuthorizer removes an authorizer from an API.
func (b *InMemoryBackend) DeleteAuthorizer(apiID, authorizerID string) error {
	b.mu.Lock("DeleteAuthorizer")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	if _, exists := d.authorizers[authorizerID]; !exists {
		return ErrAuthorizerNotFound
	}

	delete(d.authorizers, authorizerID)

	return nil
}

// UpdateAuthorizer updates fields on an existing authorizer.
func (b *InMemoryBackend) UpdateAuthorizer(
	apiID, authorizerID string,
	input UpdateAuthorizerInput,
) (*Authorizer, error) {
	b.mu.Lock("UpdateAuthorizer")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	a, ok := d.authorizers[authorizerID]
	if !ok {
		return nil, ErrAuthorizerNotFound
	}

	if input.Name != "" {
		a.Name = input.Name
	}

	if input.AuthorizerType != "" {
		a.AuthorizerType = input.AuthorizerType
	}

	if input.AuthorizerURI != "" {
		a.AuthorizerURI = input.AuthorizerURI
	}

	if len(input.IdentitySource) > 0 {
		a.IdentitySource = input.IdentitySource
	}

	if input.AuthorizerCredentialsArn != "" {
		a.AuthorizerCredentialsArn = input.AuthorizerCredentialsArn
	}

	if input.AuthorizerResultTTLInSeconds != 0 {
		a.AuthorizerResultTTLInSeconds = input.AuthorizerResultTTLInSeconds
	}

	if input.AuthorizerPayloadFormatVersion != "" {
		a.AuthorizerPayloadFormatVersion = input.AuthorizerPayloadFormatVersion
	}

	if input.EnableSimpleResponses {
		a.EnableSimpleResponses = input.EnableSimpleResponses
	}

	if input.JwtConfiguration != nil {
		clone := *input.JwtConfiguration
		a.JwtConfiguration = &clone
	}

	cp := *a

	return &cp, nil
}

// --- Domain Names ---

// CreateDomainName creates a new custom domain name.
func (b *InMemoryBackend) CreateDomainName(
	ctx context.Context,
	input CreateDomainNameInput,
) (*DomainName, error) {
	if input.DomainNameValue == "" {
		return nil, fmt.Errorf("%w: domainName is required", ErrBadRequest)
	}

	b.mu.Lock("CreateDomainName")
	defer b.mu.Unlock()

	if _, exists := b.domainNames[input.DomainNameValue]; exists {
		return nil, fmt.Errorf("%w: domain name %q already exists", ErrAlreadyExists, input.DomainNameValue)
	}

	domainNameConfigs := []DomainNameConfiguration{}
	if len(input.DomainNameConfigurations) > 0 {
		domainNameConfigs = applyDomainNameDefaults(
			input.DomainNameConfigurations, input.DomainNameValue, regionFromCtx(ctx))
	}

	dn := &DomainName{
		DomainNameValue:          input.DomainNameValue,
		Tags:                     copyTags(input.Tags),
		DomainNameConfigurations: domainNameConfigs,
	}

	b.domainNames[input.DomainNameValue] = dn
	b.apiMappings[input.DomainNameValue] = make(map[string]*APIMapping)

	cp := *dn

	return &cp, nil
}

// --- API Mappings ---

// CreateAPIMapping creates a new API mapping for a custom domain name.
func (b *InMemoryBackend) CreateAPIMapping(domainName string, input CreateAPIMappingInput) (*APIMapping, error) {
	if input.APIID == "" {
		return nil, fmt.Errorf("%w: apiId is required", ErrBadRequest)
	}

	if input.Stage == "" {
		return nil, fmt.Errorf("%w: stage is required", ErrBadRequest)
	}

	b.mu.Lock("CreateAPIMapping")
	defer b.mu.Unlock()

	if _, ok := b.domainNames[domainName]; !ok {
		return nil, ErrDomainNameNotFound
	}

	d, ok := b.apis[input.APIID]
	if !ok {
		return nil, ErrAPINotFound
	}

	if _, stageExists := d.stages[input.Stage]; !stageExists {
		return nil, ErrStageNotFound
	}

	id := randomID()
	mapping := &APIMapping{
		APIMappingID:  id,
		DomainName:    domainName,
		APIID:         input.APIID,
		Stage:         input.Stage,
		APIMappingKey: input.APIMappingKey,
	}

	b.apiMappings[domainName][id] = mapping

	cp := *mapping

	return &cp, nil
}

// --- Integration Responses ---

// CreateIntegrationResponse creates a new integration response.
func (b *InMemoryBackend) CreateIntegrationResponse(
	apiID, integrationID string,
	input CreateIntegrationResponseInput,
) (*IntegrationResponse, error) {
	if input.IntegrationResponseKey == "" {
		return nil, fmt.Errorf("%w: integrationResponseKey is required", ErrBadRequest)
	}

	b.mu.Lock("CreateIntegrationResponse")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	if _, exists := d.integrations[integrationID]; !exists {
		return nil, ErrIntegrationNotFound
	}

	if _, exists := d.integrationResponses[integrationID]; !exists {
		d.integrationResponses[integrationID] = make(map[string]*IntegrationResponse)
	}

	for _, existing := range d.integrationResponses[integrationID] {
		if existing.IntegrationResponseKey == input.IntegrationResponseKey {
			return nil, fmt.Errorf(
				"%w: integration response key %q already exists",
				ErrAlreadyExists,
				input.IntegrationResponseKey,
			)
		}
	}

	id := randomID()
	ir := &IntegrationResponse{
		IntegrationResponseID:       id,
		IntegrationResponseKey:      input.IntegrationResponseKey,
		APIID:                       apiID,
		IntegrationID:               integrationID,
		ContentHandlingStrategy:     input.ContentHandlingStrategy,
		TemplateSelectionExpression: input.TemplateSelectionExpression,
		ResponseParameters:          input.ResponseParameters,
		ResponseTemplates:           input.ResponseTemplates,
	}

	d.integrationResponses[integrationID][id] = ir

	cp := *ir

	return &cp, nil
}

// --- Models ---

// CreateModel creates a new model for an API.
func (b *InMemoryBackend) CreateModel(apiID string, input CreateModelInput) (*Model, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrBadRequest)
	}

	b.mu.Lock("CreateModel")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	for _, existing := range d.models {
		if existing.Name == input.Name {
			return nil, fmt.Errorf("%w: model name %q already exists", ErrAlreadyExists, input.Name)
		}
	}

	id := randomID()
	model := &Model{
		ModelID:     id,
		APIID:       apiID,
		Name:        input.Name,
		Schema:      input.Schema,
		ContentType: input.ContentType,
		Description: input.Description,
	}

	d.models[id] = model

	cp := *model

	return &cp, nil
}

// --- Route Responses ---

// CreateRouteResponse creates a new route response.
func (b *InMemoryBackend) CreateRouteResponse(
	apiID, routeID string,
	input CreateRouteResponseInput,
) (*RouteResponse, error) {
	if input.RouteResponseKey == "" {
		return nil, fmt.Errorf("%w: routeResponseKey is required", ErrBadRequest)
	}

	b.mu.Lock("CreateRouteResponse")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	if _, exists := d.routes[routeID]; !exists {
		return nil, ErrRouteNotFound
	}

	if _, exists := d.routeResponses[routeID]; !exists {
		d.routeResponses[routeID] = make(map[string]*RouteResponse)
	}

	for _, existing := range d.routeResponses[routeID] {
		if existing.RouteResponseKey == input.RouteResponseKey {
			return nil, fmt.Errorf("%w: route response key %q already exists", ErrAlreadyExists, input.RouteResponseKey)
		}
	}

	id := randomID()
	rr := &RouteResponse{
		RouteResponseID:          id,
		RouteResponseKey:         input.RouteResponseKey,
		APIID:                    apiID,
		RouteID:                  routeID,
		ModelSelectionExpression: input.ModelSelectionExpression,
		ResponseModels:           input.ResponseModels,
	}

	d.routeResponses[routeID][id] = rr

	cp := *rr

	return &cp, nil
}

// --- Portals ---

// CreatePortal creates a new portal.
func (b *InMemoryBackend) CreatePortal(input CreatePortalInput) (*Portal, error) {
	b.mu.Lock("CreatePortal")
	defer b.mu.Unlock()

	id := randomID()
	portal := &Portal{
		PortalID: id,
		LogoURI:  input.LogoURI,
		Tags:     copyTags(input.Tags),
		Status:   "ACTIVE",
	}

	b.portals[id] = portal

	cp := *portal

	return &cp, nil
}

// --- Portal Products ---

// CreatePortalProduct creates a new portal product.
func (b *InMemoryBackend) CreatePortalProduct(input CreatePortalProductInput) (*PortalProduct, error) {
	if input.DisplayName == "" {
		return nil, fmt.Errorf("%w: displayName is required", ErrBadRequest)
	}

	b.mu.Lock("CreatePortalProduct")
	defer b.mu.Unlock()

	id := randomID()
	product := &PortalProduct{
		PortalProductID: id,
		DisplayName:     input.DisplayName,
		Description:     input.Description,
		Tags:            copyTags(input.Tags),
	}

	b.portalProducts[id] = product

	cp := *product

	return &cp, nil
}

// --- Product Pages ---

// CreateProductPage creates a new product page for a portal product.
func (b *InMemoryBackend) CreateProductPage(
	portalProductID string,
	_ CreateProductPageInput,
) (*ProductPage, error) {
	b.mu.Lock("CreateProductPage")
	defer b.mu.Unlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return nil, ErrPortalProductNotFound
	}

	now := isoTime{time.Now()}
	id := randomID()
	page := &ProductPage{
		ProductPageID:   id,
		PortalProductID: portalProductID,
		LastModified:    &now,
	}

	b.productPages[portalProductID] = append(b.productPages[portalProductID], page)

	cp := *page

	return &cp, nil
}

// --- Product REST Endpoint Pages ---

// CreateProductRestEndpointPage creates a new product REST endpoint page for a portal product.
func (b *InMemoryBackend) CreateProductRestEndpointPage(
	portalProductID string,
	_ CreateProductRestEndpointPageInput,
) (*ProductRestEndpointPage, error) {
	b.mu.Lock("CreateProductRestEndpointPage")
	defer b.mu.Unlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return nil, ErrPortalProductNotFound
	}

	now := isoTime{time.Now()}
	id := randomID()
	page := &ProductRestEndpointPage{
		ProductRestEndpointPageID: id,
		PortalProductID:           portalProductID,
		LastModified:              &now,
	}

	b.productREPages[portalProductID] = append(b.productREPages[portalProductID], page)

	cp := *page

	return &cp, nil
}

// --- VPC Links ---

// CreateVpcLink creates a new VPC link.
func (b *InMemoryBackend) CreateVpcLink(input CreateVpcLinkInput) (*VpcLink, error) {
	if input.Name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrBadRequest)
	}

	if len(input.SubnetIDs) == 0 {
		return nil, fmt.Errorf("%w: subnetIds is required", ErrBadRequest)
	}

	b.mu.Lock("CreateVpcLink")
	defer b.mu.Unlock()

	securityGroupIDs := input.SecurityGroupIDs
	if securityGroupIDs == nil {
		securityGroupIDs = []string{}
	}

	now := isoTime{time.Now()}
	id := randomID()
	vpcLink := &VpcLink{
		CreatedDate:      now,
		VpcLinkID:        id,
		Name:             input.Name,
		SecurityGroupIDs: securityGroupIDs,
		SubnetIDs:        input.SubnetIDs,
		Tags:             copyTags(input.Tags),
		VpcLinkStatus:    "AVAILABLE",
	}
	b.vpcLinks[id] = vpcLink

	cp := *vpcLink

	return &cp, nil
}

// GetVpcLink retrieves a VPC link by ID.
func (b *InMemoryBackend) GetVpcLink(vpcLinkID string) (*VpcLink, error) {
	b.mu.RLock("GetVpcLink")
	defer b.mu.RUnlock()

	vpcLink, ok := b.vpcLinks[vpcLinkID]
	if !ok {
		return nil, ErrVpcLinkNotFound
	}

	cp := *vpcLink

	return &cp, nil
}

// GetVpcLinks retrieves all VPC links.
func (b *InMemoryBackend) GetVpcLinks() ([]VpcLink, error) {
	b.mu.RLock("GetVpcLinks")
	defer b.mu.RUnlock()

	out := make([]VpcLink, 0, len(b.vpcLinks))
	for _, item := range b.vpcLinks {
		out = append(out, *item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VpcLinkID < out[j].VpcLinkID })

	return out, nil
}

// UpdateVpcLink updates a VPC link.
func (b *InMemoryBackend) UpdateVpcLink(vpcLinkID string, input UpdateVpcLinkInput) (*VpcLink, error) {
	b.mu.Lock("UpdateVpcLink")
	defer b.mu.Unlock()

	vpcLink, ok := b.vpcLinks[vpcLinkID]
	if !ok {
		return nil, ErrVpcLinkNotFound
	}
	if input.Name != "" {
		vpcLink.Name = input.Name
	}

	cp := *vpcLink

	return &cp, nil
}

// DeleteVpcLink removes a VPC link.
func (b *InMemoryBackend) DeleteVpcLink(vpcLinkID string) error {
	b.mu.Lock("DeleteVpcLink")
	defer b.mu.Unlock()

	if _, ok := b.vpcLinks[vpcLinkID]; !ok {
		return ErrVpcLinkNotFound
	}
	delete(b.vpcLinks, vpcLinkID)

	return nil
}

// --- Routing Rules ---

// CreateRoutingRule creates a routing rule under a domain name.
func (b *InMemoryBackend) CreateRoutingRule(
	ctx context.Context,
	domainName string,
	input CreateRoutingRuleInput,
) (*RoutingRule, error) {
	b.mu.Lock("CreateRoutingRule")
	defer b.mu.Unlock()

	if _, ok := b.domainNames[domainName]; !ok {
		return nil, ErrDomainNameNotFound
	}
	if _, ok := b.routingRules[domainName]; !ok {
		b.routingRules[domainName] = make(map[string]*RoutingRule)
	}
	id := randomID()
	rule := &RoutingRule{
		RoutingRuleID: id,
		RoutingRuleARN: "arn:aws:apigateway:" + regionFromCtx(ctx) +
			"::/domainnames/" + domainName + "/routingrules/" + id,
		DomainName: domainName,
		Priority:   input.Priority,
		Actions:    input.Actions,
		Conditions: input.Conditions,
	}
	b.routingRules[domainName][id] = rule

	cp := *rule

	return &cp, nil
}

// GetRoutingRule retrieves a routing rule.
func (b *InMemoryBackend) GetRoutingRule(domainName, routingRuleID string) (*RoutingRule, error) {
	b.mu.RLock("GetRoutingRule")
	defer b.mu.RUnlock()

	rules, ok := b.routingRules[domainName]
	if !ok {
		return nil, ErrDomainNameNotFound
	}
	rule, ok := rules[routingRuleID]
	if !ok {
		return nil, ErrRoutingRuleNotFound
	}

	cp := *rule

	return &cp, nil
}

// ListRoutingRules lists routing rules for a domain.
func (b *InMemoryBackend) ListRoutingRules(domainName string) ([]RoutingRule, error) {
	b.mu.RLock("ListRoutingRules")
	defer b.mu.RUnlock()

	if _, ok := b.domainNames[domainName]; !ok {
		return nil, ErrDomainNameNotFound
	}
	rules := b.routingRules[domainName]
	out := make([]RoutingRule, 0, len(rules))
	for _, rule := range rules {
		out = append(out, *rule)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RoutingRuleID < out[j].RoutingRuleID })

	return out, nil
}

// PutRoutingRule updates an existing routing rule.
func (b *InMemoryBackend) PutRoutingRule(
	domainName, routingRuleID string,
	input PutRoutingRuleInput,
) (*RoutingRule, error) {
	b.mu.Lock("PutRoutingRule")
	defer b.mu.Unlock()

	rules, ok := b.routingRules[domainName]
	if !ok {
		return nil, ErrDomainNameNotFound
	}
	rule, ok := rules[routingRuleID]
	if !ok {
		return nil, ErrRoutingRuleNotFound
	}
	rule.Priority = input.Priority
	rule.Actions = input.Actions
	rule.Conditions = input.Conditions

	cp := *rule

	return &cp, nil
}

// DeleteRoutingRule deletes a routing rule.
func (b *InMemoryBackend) DeleteRoutingRule(domainName, routingRuleID string) error {
	b.mu.Lock("DeleteRoutingRule")
	defer b.mu.Unlock()

	rules, ok := b.routingRules[domainName]
	if !ok {
		return ErrDomainNameNotFound
	}
	if _, exists := rules[routingRuleID]; !exists {
		return ErrRoutingRuleNotFound
	}
	delete(rules, routingRuleID)

	return nil
}

// --- Portal Product Sharing Policy ---

// GetPortalProductSharingPolicy gets sharing policy for a portal product.
func (b *InMemoryBackend) GetPortalProductSharingPolicy(portalProductID string) (*PortalProductSharingPolicy, error) {
	b.mu.RLock("GetPortalProductSharingPolicy")
	defer b.mu.RUnlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return nil, ErrPortalProductNotFound
	}

	return &PortalProductSharingPolicy{PolicyDocument: b.portalProductSharingPolicies[portalProductID]}, nil
}

// PutPortalProductSharingPolicy stores sharing policy for a portal product.
func (b *InMemoryBackend) PutPortalProductSharingPolicy(
	portalProductID, policyDocument string,
) (*PortalProductSharingPolicy, error) {
	b.mu.Lock("PutPortalProductSharingPolicy")
	defer b.mu.Unlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return nil, ErrPortalProductNotFound
	}
	b.portalProductSharingPolicies[portalProductID] = policyDocument

	return &PortalProductSharingPolicy{PolicyDocument: policyDocument}, nil
}

// DeletePortalProductSharingPolicy deletes sharing policy for a portal product.
func (b *InMemoryBackend) DeletePortalProductSharingPolicy(portalProductID string) error {
	b.mu.Lock("DeletePortalProductSharingPolicy")
	defer b.mu.Unlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return ErrPortalProductNotFound
	}
	delete(b.portalProductSharingPolicies, portalProductID)

	return nil
}

// --- Get/Delete Domain Names ---

// GetDomainName retrieves a domain name by name.
func (b *InMemoryBackend) GetDomainName(domainName string) (*DomainName, error) {
	b.mu.RLock("GetDomainName")
	defer b.mu.RUnlock()

	dn, ok := b.domainNames[domainName]
	if !ok {
		return nil, ErrDomainNameNotFound
	}

	cp := *dn

	return &cp, nil
}

// GetDomainNames retrieves all custom domain names.
func (b *InMemoryBackend) GetDomainNames() ([]DomainName, error) {
	b.mu.RLock("GetDomainNames")
	defer b.mu.RUnlock()

	result := make([]DomainName, 0, len(b.domainNames))
	for _, dn := range b.domainNames {
		result = append(result, *dn)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].DomainNameValue < result[j].DomainNameValue
	})

	return result, nil
}

// DeleteDomainName removes a custom domain name and all its API mappings.
func (b *InMemoryBackend) DeleteDomainName(domainName string) error {
	b.mu.Lock("DeleteDomainName")
	defer b.mu.Unlock()

	if _, ok := b.domainNames[domainName]; !ok {
		return ErrDomainNameNotFound
	}

	delete(b.domainNames, domainName)
	delete(b.apiMappings, domainName)
	delete(b.routingRules, domainName)

	return nil
}

// --- Get/Delete API Mappings ---

// GetAPIMapping retrieves a specific API mapping.
func (b *InMemoryBackend) GetAPIMapping(domainName, mappingID string) (*APIMapping, error) {
	b.mu.RLock("GetAPIMapping")
	defer b.mu.RUnlock()

	mappings, ok := b.apiMappings[domainName]
	if !ok {
		return nil, ErrDomainNameNotFound
	}

	m, ok := mappings[mappingID]
	if !ok {
		return nil, ErrAPIMappingNotFound
	}

	cp := *m

	return &cp, nil
}

// GetAPIMappings retrieves all API mappings for a domain name.
func (b *InMemoryBackend) GetAPIMappings(domainName string) ([]APIMapping, error) {
	b.mu.RLock("GetAPIMappings")
	defer b.mu.RUnlock()

	if _, ok := b.domainNames[domainName]; !ok {
		return nil, ErrDomainNameNotFound
	}

	mappings := b.apiMappings[domainName]
	result := make([]APIMapping, 0, len(mappings))
	for _, m := range mappings {
		result = append(result, *m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].APIMappingID < result[j].APIMappingID
	})

	return result, nil
}

// DeleteAPIMapping removes an API mapping from a domain name.
func (b *InMemoryBackend) DeleteAPIMapping(domainName, mappingID string) error {
	b.mu.Lock("DeleteAPIMapping")
	defer b.mu.Unlock()

	mappings, ok := b.apiMappings[domainName]
	if !ok {
		return ErrDomainNameNotFound
	}

	if _, exists := mappings[mappingID]; !exists {
		return ErrAPIMappingNotFound
	}

	delete(b.apiMappings[domainName], mappingID)

	return nil
}

// --- Get/Delete Integration Responses ---

// GetIntegrationResponse retrieves a specific integration response.
func (b *InMemoryBackend) GetIntegrationResponse(
	apiID, integrationID, responseID string,
) (*IntegrationResponse, error) {
	b.mu.RLock("GetIntegrationResponse")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	if _, exists := d.integrations[integrationID]; !exists {
		return nil, ErrIntegrationNotFound
	}

	responses, hasResponses := d.integrationResponses[integrationID]
	if !hasResponses {
		return nil, ErrIntegrationResponseNotFound
	}

	ir, exists := responses[responseID]
	if !exists {
		return nil, ErrIntegrationResponseNotFound
	}

	cp := *ir

	return &cp, nil
}

// GetIntegrationResponses retrieves all integration responses for an integration.
func (b *InMemoryBackend) GetIntegrationResponses(apiID, integrationID string) ([]IntegrationResponse, error) {
	b.mu.RLock("GetIntegrationResponses")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	if _, exists := d.integrations[integrationID]; !exists {
		return nil, ErrIntegrationNotFound
	}

	responses := d.integrationResponses[integrationID]
	result := make([]IntegrationResponse, 0, len(responses))

	for _, ir := range responses {
		result = append(result, *ir)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].IntegrationResponseID < result[j].IntegrationResponseID
	})

	return result, nil
}

// DeleteIntegrationResponse removes an integration response.
func (b *InMemoryBackend) DeleteIntegrationResponse(apiID, integrationID, responseID string) error {
	b.mu.Lock("DeleteIntegrationResponse")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	if _, exists := d.integrations[integrationID]; !exists {
		return ErrIntegrationNotFound
	}

	responses, hasResponses := d.integrationResponses[integrationID]
	if !hasResponses {
		return ErrIntegrationResponseNotFound
	}

	if _, exists := responses[responseID]; !exists {
		return ErrIntegrationResponseNotFound
	}

	delete(d.integrationResponses[integrationID], responseID)

	return nil
}

// --- Get/Delete Models ---

// GetModel retrieves a model by ID.
func (b *InMemoryBackend) GetModel(apiID, modelID string) (*Model, error) {
	b.mu.RLock("GetModel")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	m, ok := d.models[modelID]
	if !ok {
		return nil, ErrModelNotFound
	}

	cp := *m

	return &cp, nil
}

// GetModels retrieves all models for an API.
func (b *InMemoryBackend) GetModels(apiID string) ([]Model, error) {
	b.mu.RLock("GetModels")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	result := make([]Model, 0, len(d.models))
	for _, m := range d.models {
		result = append(result, *m)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ModelID < result[j].ModelID
	})

	return result, nil
}

// DeleteModel removes a model from an API.
func (b *InMemoryBackend) DeleteModel(apiID, modelID string) error {
	b.mu.Lock("DeleteModel")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	if _, exists := d.models[modelID]; !exists {
		return ErrModelNotFound
	}

	delete(d.models, modelID)

	return nil
}

// --- Get/Delete Route Responses ---

// GetRouteResponse retrieves a specific route response.
func (b *InMemoryBackend) GetRouteResponse(apiID, routeID, responseID string) (*RouteResponse, error) {
	b.mu.RLock("GetRouteResponse")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	if _, exists := d.routes[routeID]; !exists {
		return nil, ErrRouteNotFound
	}

	responses, hasResponses := d.routeResponses[routeID]
	if !hasResponses {
		return nil, ErrRouteResponseNotFound
	}

	rr, exists := responses[responseID]
	if !exists {
		return nil, ErrRouteResponseNotFound
	}

	cp := *rr

	return &cp, nil
}

// GetRouteResponses retrieves all route responses for a route.
func (b *InMemoryBackend) GetRouteResponses(apiID, routeID string) ([]RouteResponse, error) {
	b.mu.RLock("GetRouteResponses")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	if _, exists := d.routes[routeID]; !exists {
		return nil, ErrRouteNotFound
	}

	responses := d.routeResponses[routeID]
	result := make([]RouteResponse, 0, len(responses))

	for _, rr := range responses {
		result = append(result, *rr)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RouteResponseID < result[j].RouteResponseID
	})

	return result, nil
}

// DeleteRouteResponse removes a route response.
func (b *InMemoryBackend) DeleteRouteResponse(apiID, routeID, responseID string) error {
	b.mu.Lock("DeleteRouteResponse")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	if _, exists := d.routes[routeID]; !exists {
		return ErrRouteNotFound
	}

	responses, hasResponses := d.routeResponses[routeID]
	if !hasResponses {
		return ErrRouteResponseNotFound
	}

	if _, exists := responses[responseID]; !exists {
		return ErrRouteResponseNotFound
	}

	delete(d.routeResponses[routeID], responseID)

	return nil
}

// --- Get/List Portals ---

// GetPortal retrieves a portal by ID.
func (b *InMemoryBackend) GetPortal(portalID string) (*Portal, error) {
	b.mu.RLock("GetPortal")
	defer b.mu.RUnlock()

	p, ok := b.portals[portalID]
	if !ok {
		return nil, ErrPortalNotFound
	}

	cp := *p

	return &cp, nil
}

// ListPortals retrieves all portals.
func (b *InMemoryBackend) ListPortals() ([]Portal, error) {
	b.mu.RLock("ListPortals")
	defer b.mu.RUnlock()

	result := make([]Portal, 0, len(b.portals))
	for _, p := range b.portals {
		result = append(result, *p)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].PortalID < result[j].PortalID
	})

	return result, nil
}

// --- Get/List Portal Products ---

// GetPortalProduct retrieves a portal product by ID.
func (b *InMemoryBackend) GetPortalProduct(portalProductID string) (*PortalProduct, error) {
	b.mu.RLock("GetPortalProduct")
	defer b.mu.RUnlock()

	pp, ok := b.portalProducts[portalProductID]
	if !ok {
		return nil, ErrPortalProductNotFound
	}

	cp := *pp

	return &cp, nil
}

// ListPortalProducts retrieves all portal products.
func (b *InMemoryBackend) ListPortalProducts() ([]PortalProduct, error) {
	b.mu.RLock("ListPortalProducts")
	defer b.mu.RUnlock()

	result := make([]PortalProduct, 0, len(b.portalProducts))
	for _, pp := range b.portalProducts {
		result = append(result, *pp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].PortalProductID < result[j].PortalProductID
	})

	return result, nil
}

// --- List Product Pages ---

// ListProductPages retrieves all product pages for a portal product.
func (b *InMemoryBackend) ListProductPages(portalProductID string) ([]ProductPage, error) {
	b.mu.RLock("ListProductPages")
	defer b.mu.RUnlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return nil, ErrPortalProductNotFound
	}

	pages := b.productPages[portalProductID]
	result := make([]ProductPage, 0, len(pages))

	for _, p := range pages {
		result = append(result, *p)
	}

	return result, nil
}

// --- List Product REST Endpoint Pages ---

// ListProductRestEndpointPages retrieves all product REST endpoint pages for a portal product.
func (b *InMemoryBackend) ListProductRestEndpointPages(portalProductID string) ([]ProductRestEndpointPage, error) {
	b.mu.RLock("ListProductRestEndpointPages")
	defer b.mu.RUnlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return nil, ErrPortalProductNotFound
	}

	pages := b.productREPages[portalProductID]
	result := make([]ProductRestEndpointPage, 0, len(pages))

	for _, p := range pages {
		result = append(result, *p)
	}

	return result, nil
}

// --- Tags ---

const (
	arnResourceTypeAPIs        = "apis"
	arnResourceTypeVpcLinks    = "vpclinks"
	arnResourceTypeDomainNames = "domainnames"

	// arnMinPartsWithResourceType is the minimum number of slash-separated
	// parts in an ARN that carries an explicit resource type segment.
	arnMinPartsWithResourceType = 2
)

// arnResourceType returns the resource type and ID extracted from an ARN.
// For ARNs like "arn:aws:apigateway:us-east-1::/apis/abc123" the resource
// type would be "apis" and the ID "abc123".
// For a bare resource ID (no slashes besides the leading one) the type
// defaults to "apis" to preserve backwards-compatible behaviour.
func arnResourceType(arn string) (string, string) {
	parts := strings.Split(arn, "/")
	if len(parts) >= arnMinPartsWithResourceType {
		return parts[len(parts)-2], parts[len(parts)-1]
	}

	return arnResourceTypeAPIs, arn
}

// TagResource adds tags to a resource identified by ARN.
// Supports APIs, VPC links, and domain names.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	resourceType, resourceID := arnResourceType(resourceARN)

	switch resourceType {
	case arnResourceTypeAPIs:
		d, ok := b.apis[resourceID]
		if !ok {
			return ErrAPINotFound
		}

		if d.api.Tags == nil {
			d.api.Tags = make(map[string]string)
		}

		maps.Copy(d.api.Tags, tags)
	case arnResourceTypeVpcLinks:
		v, ok := b.vpcLinks[resourceID]
		if !ok {
			return ErrVpcLinkNotFound
		}

		if v.Tags == nil {
			v.Tags = make(map[string]string)
		}

		maps.Copy(v.Tags, tags)
	case arnResourceTypeDomainNames:
		dn, ok := b.domainNames[resourceID]
		if !ok {
			return ErrDomainNameNotFound
		}

		if dn.Tags == nil {
			dn.Tags = make(map[string]string)
		}

		maps.Copy(dn.Tags, tags)
	default:
		return ErrAPINotFound
	}

	return nil
}

// UntagResource removes tag keys from a resource identified by ARN.
// Supports APIs, VPC links, and domain names.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	resourceType, resourceID := arnResourceType(resourceARN)

	switch resourceType {
	case arnResourceTypeAPIs:
		d, ok := b.apis[resourceID]
		if !ok {
			return ErrAPINotFound
		}

		for _, k := range tagKeys {
			delete(d.api.Tags, k)
		}
	case arnResourceTypeVpcLinks:
		v, ok := b.vpcLinks[resourceID]
		if !ok {
			return ErrVpcLinkNotFound
		}

		for _, k := range tagKeys {
			delete(v.Tags, k)
		}
	case arnResourceTypeDomainNames:
		dn, ok := b.domainNames[resourceID]
		if !ok {
			return ErrDomainNameNotFound
		}

		for _, k := range tagKeys {
			delete(dn.Tags, k)
		}
	default:
		return ErrAPINotFound
	}

	return nil
}

// GetTags retrieves all tags for a resource identified by ARN.
// Supports APIs, VPC links, and domain names.
func (b *InMemoryBackend) GetTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	resourceType, resourceID := arnResourceType(resourceARN)

	switch resourceType {
	case arnResourceTypeAPIs:
		d, ok := b.apis[resourceID]
		if !ok {
			return nil, ErrAPINotFound
		}

		return copyTags(d.api.Tags), nil
	case arnResourceTypeVpcLinks:
		v, ok := b.vpcLinks[resourceID]
		if !ok {
			return nil, ErrVpcLinkNotFound
		}

		return copyTags(v.Tags), nil
	case arnResourceTypeDomainNames:
		dn, ok := b.domainNames[resourceID]
		if !ok {
			return nil, ErrDomainNameNotFound
		}

		return copyTags(dn.Tags), nil
	default:
		return nil, ErrAPINotFound
	}
}

// UpdateAPIMapping updates fields on an existing API mapping.
func (b *InMemoryBackend) UpdateAPIMapping(
	domainName, mappingID string,
	input UpdateAPIMappingInput,
) (*APIMapping, error) {
	b.mu.Lock("UpdateAPIMapping")
	defer b.mu.Unlock()

	if _, ok := b.domainNames[domainName]; !ok {
		return nil, ErrDomainNameNotFound
	}

	mappings, ok := b.apiMappings[domainName]
	if !ok {
		return nil, ErrDomainNameNotFound
	}

	m, ok := mappings[mappingID]
	if !ok {
		return nil, ErrAPIMappingNotFound
	}

	if input.APIID != "" {
		d, apiExists := b.apis[input.APIID]
		if !apiExists {
			return nil, ErrAPINotFound
		}
		stageToCheck := m.Stage
		if input.Stage != "" {
			stageToCheck = input.Stage
		}
		if _, exists := d.stages[stageToCheck]; !exists {
			return nil, ErrStageNotFound
		}
		m.APIID = input.APIID
	}

	if input.Stage != "" {
		m.Stage = input.Stage
	}

	if input.APIMappingKey != "" {
		m.APIMappingKey = input.APIMappingKey
	}

	cp := *m

	return &cp, nil
}

// UpdateDeployment updates fields on an existing deployment.
func (b *InMemoryBackend) UpdateDeployment(
	apiID, deploymentID string,
	input UpdateDeploymentInput,
) (*Deployment, error) {
	b.mu.Lock("UpdateDeployment")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	dep, ok := d.deployments[deploymentID]
	if !ok {
		return nil, ErrDeploymentNotFound
	}

	if input.Description != "" {
		dep.Description = input.Description
	}

	cp := *dep

	return &cp, nil
}

// UpdateDomainName updates fields on an existing domain name.
func (b *InMemoryBackend) UpdateDomainName(domainName string, input UpdateDomainNameInput) (*DomainName, error) {
	b.mu.Lock("UpdateDomainName")
	defer b.mu.Unlock()

	dn, ok := b.domainNames[domainName]
	if !ok {
		return nil, ErrDomainNameNotFound
	}

	if input.Tags != nil {
		if dn.Tags == nil {
			dn.Tags = make(map[string]string)
		}
		maps.Copy(dn.Tags, input.Tags)
	}

	if len(input.DomainNameConfigurations) > 0 {
		configs := make([]DomainNameConfiguration, len(input.DomainNameConfigurations))
		copy(configs, input.DomainNameConfigurations)
		dn.DomainNameConfigurations = configs
	}

	cp := *dn

	return &cp, nil
}

// UpdateIntegrationResponse updates fields on an existing integration response.
func (b *InMemoryBackend) UpdateIntegrationResponse(
	apiID, integrationID, responseID string,
	input UpdateIntegrationResponseInput,
) (*IntegrationResponse, error) {
	b.mu.Lock("UpdateIntegrationResponse")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	if _, exists := d.integrations[integrationID]; !exists {
		return nil, ErrIntegrationNotFound
	}

	responses, hasResponses := d.integrationResponses[integrationID]
	if !hasResponses {
		return nil, ErrIntegrationResponseNotFound
	}

	ir, exists := responses[responseID]
	if !exists {
		return nil, ErrIntegrationResponseNotFound
	}

	if input.IntegrationResponseKey != "" {
		ir.IntegrationResponseKey = input.IntegrationResponseKey
	}

	if input.ContentHandlingStrategy != "" {
		ir.ContentHandlingStrategy = input.ContentHandlingStrategy
	}

	if input.TemplateSelectionExpression != "" {
		ir.TemplateSelectionExpression = input.TemplateSelectionExpression
	}

	if input.ResponseParameters != nil {
		ir.ResponseParameters = input.ResponseParameters
	}

	if input.ResponseTemplates != nil {
		ir.ResponseTemplates = input.ResponseTemplates
	}

	cp := *ir

	return &cp, nil
}

// UpdateModel updates fields on an existing model.
func (b *InMemoryBackend) UpdateModel(apiID, modelID string, input UpdateModelInput) (*Model, error) {
	b.mu.Lock("UpdateModel")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	m, ok := d.models[modelID]
	if !ok {
		return nil, ErrModelNotFound
	}

	if input.Name != "" {
		m.Name = input.Name
	}

	if input.Schema != "" {
		m.Schema = input.Schema
	}

	if input.ContentType != "" {
		m.ContentType = input.ContentType
	}

	if input.Description != "" {
		m.Description = input.Description
	}

	cp := *m

	return &cp, nil
}

// UpdateRouteResponse updates fields on an existing route response.
func (b *InMemoryBackend) UpdateRouteResponse(
	apiID, routeID, responseID string,
	input UpdateRouteResponseInput,
) (*RouteResponse, error) {
	b.mu.Lock("UpdateRouteResponse")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	if _, exists := d.routes[routeID]; !exists {
		return nil, ErrRouteNotFound
	}

	responses, hasResponses := d.routeResponses[routeID]
	if !hasResponses {
		return nil, ErrRouteResponseNotFound
	}

	rr, exists := responses[responseID]
	if !exists {
		return nil, ErrRouteResponseNotFound
	}

	if input.RouteResponseKey != "" {
		rr.RouteResponseKey = input.RouteResponseKey
	}

	if input.ModelSelectionExpression != "" {
		rr.ModelSelectionExpression = input.ModelSelectionExpression
	}

	if input.ResponseModels != nil {
		rr.ResponseModels = input.ResponseModels
	}

	cp := *rr

	return &cp, nil
}

// UpdatePortal updates fields on an existing portal.
func (b *InMemoryBackend) UpdatePortal(portalID string, input UpdatePortalInput) (*Portal, error) {
	b.mu.Lock("UpdatePortal")
	defer b.mu.Unlock()

	p, ok := b.portals[portalID]
	if !ok {
		return nil, ErrPortalNotFound
	}

	if input.Tags != nil {
		if p.Tags == nil {
			p.Tags = make(map[string]string)
		}
		maps.Copy(p.Tags, input.Tags)
	}

	if input.LogoURI != "" {
		p.LogoURI = input.LogoURI
	}
	if input.Status != "" {
		p.Status = input.Status
	}

	cp := *p

	return &cp, nil
}

// UpdatePortalProduct updates fields on an existing portal product.
func (b *InMemoryBackend) UpdatePortalProduct(
	portalProductID string,
	input UpdatePortalProductInput,
) (*PortalProduct, error) {
	b.mu.Lock("UpdatePortalProduct")
	defer b.mu.Unlock()

	pp, ok := b.portalProducts[portalProductID]
	if !ok {
		return nil, ErrPortalProductNotFound
	}

	if input.Tags != nil {
		if pp.Tags == nil {
			pp.Tags = make(map[string]string)
		}
		maps.Copy(pp.Tags, input.Tags)
	}

	if input.DisplayName != "" {
		pp.DisplayName = input.DisplayName
	}

	if input.Description != "" {
		pp.Description = input.Description
	}

	cp := *pp

	return &cp, nil
}

// UpdateProductPage updates a product page.
func (b *InMemoryBackend) UpdateProductPage(
	portalProductID, pageID string,
	input UpdateProductPageInput,
) (*ProductPage, error) {
	b.mu.Lock("UpdateProductPage")
	defer b.mu.Unlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return nil, ErrPortalProductNotFound
	}
	for _, page := range b.productPages[portalProductID] {
		if page.ProductPageID == pageID {
			now := isoTime{time.Now()}
			if input.DisplayContent != nil {
				page.DisplayContent = input.DisplayContent
			}
			page.LastModified = &now
			cp := *page

			return &cp, nil
		}
	}

	return nil, ErrProductPageNotFound
}

// UpdateProductRestEndpointPage updates a product REST endpoint page.
func (b *InMemoryBackend) UpdateProductRestEndpointPage(
	portalProductID, pageID string,
	input UpdateProductRestEndpointPageInput,
) (*ProductRestEndpointPage, error) {
	b.mu.Lock("UpdateProductRestEndpointPage")
	defer b.mu.Unlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return nil, ErrPortalProductNotFound
	}
	for _, page := range b.productREPages[portalProductID] {
		if page.ProductRestEndpointPageID == pageID {
			now := isoTime{time.Now()}
			if input.DisplayContent != nil {
				page.DisplayContent = input.DisplayContent
			}
			page.LastModified = &now
			cp := *page

			return &cp, nil
		}
	}

	return nil, ErrProductREPageNotFound
}

// DeletePortal removes a portal by ID.
func (b *InMemoryBackend) DeletePortal(portalID string) error {
	b.mu.Lock("DeletePortal")
	defer b.mu.Unlock()

	if _, ok := b.portals[portalID]; !ok {
		return ErrPortalNotFound
	}

	delete(b.portals, portalID)

	return nil
}

// DeletePortalProduct removes a portal product and all its associated pages.
func (b *InMemoryBackend) DeletePortalProduct(portalProductID string) error {
	b.mu.Lock("DeletePortalProduct")
	defer b.mu.Unlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return ErrPortalProductNotFound
	}

	delete(b.portalProducts, portalProductID)
	delete(b.productPages, portalProductID)
	delete(b.productREPages, portalProductID)

	return nil
}

// GetProductPage retrieves a specific product page.
func (b *InMemoryBackend) GetProductPage(portalProductID, pageID string) (*ProductPage, error) {
	b.mu.RLock("GetProductPage")
	defer b.mu.RUnlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return nil, ErrPortalProductNotFound
	}

	for _, p := range b.productPages[portalProductID] {
		if p.ProductPageID == pageID {
			cp := *p

			return &cp, nil
		}
	}

	return nil, ErrProductPageNotFound
}

// GetProductRestEndpointPage retrieves a specific product REST endpoint page.
func (b *InMemoryBackend) GetProductRestEndpointPage(portalProductID, pageID string) (*ProductRestEndpointPage, error) {
	b.mu.RLock("GetProductRestEndpointPage")
	defer b.mu.RUnlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return nil, ErrPortalProductNotFound
	}

	for _, p := range b.productREPages[portalProductID] {
		if p.ProductRestEndpointPageID == pageID {
			cp := *p

			return &cp, nil
		}
	}

	return nil, ErrProductREPageNotFound
}

// DeleteProductPage removes a product page from a portal product.
func (b *InMemoryBackend) DeleteProductPage(portalProductID, pageID string) error {
	b.mu.Lock("DeleteProductPage")
	defer b.mu.Unlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return ErrPortalProductNotFound
	}

	pages := b.productPages[portalProductID]
	for i, p := range pages {
		if p.ProductPageID == pageID {
			b.productPages[portalProductID] = append(pages[:i], pages[i+1:]...)

			return nil
		}
	}

	return ErrProductPageNotFound
}

// DeleteProductRestEndpointPage removes a product REST endpoint page from a portal product.
func (b *InMemoryBackend) DeleteProductRestEndpointPage(portalProductID, pageID string) error {
	b.mu.Lock("DeleteProductRestEndpointPage")
	defer b.mu.Unlock()

	if _, ok := b.portalProducts[portalProductID]; !ok {
		return ErrPortalProductNotFound
	}

	pages := b.productREPages[portalProductID]
	for i, p := range pages {
		if p.ProductRestEndpointPageID == pageID {
			b.productREPages[portalProductID] = append(pages[:i], pages[i+1:]...)

			return nil
		}
	}

	return ErrProductREPageNotFound
}

// ResetAuthorizersCache is a no-op for the in-memory backend (caching is not simulated).
func (b *InMemoryBackend) ResetAuthorizersCache(apiID, stageName string) error {
	b.mu.RLock("ResetAuthorizersCache")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	if _, exists := d.stages[stageName]; !exists {
		return ErrStageNotFound
	}

	return nil
}

// DeleteCorsConfiguration clears the CORS configuration on an API.
func (b *InMemoryBackend) DeleteCorsConfiguration(apiID string) error {
	b.mu.Lock("DeleteCorsConfiguration")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	d.api.CorsConfiguration = nil

	return nil
}

// DeleteAccessLogSettings clears the access log settings on a stage.
func (b *InMemoryBackend) DeleteAccessLogSettings(apiID, stageName string) error {
	b.mu.Lock("DeleteAccessLogSettings")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	s, ok := d.stages[stageName]
	if !ok {
		return ErrStageNotFound
	}

	s.AccessLogSettings = nil
	s.LastUpdatedDate = isoTime{time.Now()}

	return nil
}

// DeleteRouteSettings removes per-route settings for a specific routeKey from a stage.
func (b *InMemoryBackend) DeleteRouteSettings(apiID, stageName, routeKey string) error {
	b.mu.Lock("DeleteRouteSettings")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	s, ok := d.stages[stageName]
	if !ok {
		return ErrStageNotFound
	}

	if s.RouteSettings != nil {
		delete(s.RouteSettings, routeKey)
	}

	s.LastUpdatedDate = isoTime{time.Now()}

	return nil
}

// ExportAPI generates a basic OpenAPI 3.0 specification for the API's routes.
func (b *InMemoryBackend) ExportAPI(apiID string) (map[string]any, error) {
	b.mu.RLock("ExportAPI")
	defer b.mu.RUnlock()

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	const routeKeyParts = 2

	paths := map[string]any{}

	for _, route := range d.routes {
		// Parse route key: e.g. "GET /items" or "$connect" (WebSocket)
		parts := strings.SplitN(route.RouteKey, " ", routeKeyParts)

		var method, routePath string

		if len(parts) == routeKeyParts {
			method = strings.ToLower(parts[0])
			routePath = parts[1]
		} else {
			// WebSocket route like $connect, $disconnect, $default
			method = "get"
			routePath = "/" + strings.TrimPrefix(route.RouteKey, "$")
		}

		if _, exists := paths[routePath]; !exists {
			paths[routePath] = map[string]any{}
		}

		pathItem, _ := paths[routePath].(map[string]any)

		op := map[string]any{
			"operationId": route.RouteID,
			"responses":   map[string]any{"200": map[string]any{"description": "Success"}},
		}

		if route.OperationName != "" {
			op["summary"] = route.OperationName
		}

		if route.AuthorizationType != "" && route.AuthorizationType != authorizationTypeNone {
			op["security"] = []any{map[string]any{route.AuthorizationType: []any{}}}
		}

		pathItem[method] = op
	}

	info := map[string]any{
		"title":   d.api.Name,
		"version": d.api.Version,
	}

	if d.api.Description != "" {
		info["description"] = d.api.Description
	}

	spec := map[string]any{
		"openapi": "3.0.1",
		"info":    info,
		"paths":   paths,
	}

	return spec, nil
}

// DeleteRouteRequestParameter removes a specific request parameter from a route.
func (b *InMemoryBackend) DeleteRouteRequestParameter(apiID, routeID, requestParameterKey string) error {
	b.mu.Lock("DeleteRouteRequestParameter")
	defer b.mu.Unlock()

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	r, ok := d.routes[routeID]
	if !ok {
		return ErrRouteNotFound
	}

	if r.RequestParameters != nil {
		delete(r.RequestParameters, requestParameterKey)
	}

	return nil
}
