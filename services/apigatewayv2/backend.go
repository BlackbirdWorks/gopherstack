package apigatewayv2

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	apiIDChars  = "abcdefghijklmnopqrstuvwxyz0123456789"
	apiIDLength = 10
)

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
)

// StorageBackend is the interface for the API Gateway v2 in-memory store.
type StorageBackend interface {
	// APIs
	CreateAPI(input CreateAPIInput) (*API, error)
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
	CreateDomainName(input CreateDomainNameInput) (*DomainName, error)

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
	apis           map[string]*apiData
	domainNames    map[string]*DomainName
	apiMappings    map[string]map[string]*APIMapping
	portals        map[string]*Portal
	portalProducts map[string]*PortalProduct
	productPages   map[string][]*ProductPage             // key: portalProductID
	productREPages map[string][]*ProductRestEndpointPage // key: portalProductID
	mu             *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		apis:           make(map[string]*apiData),
		domainNames:    make(map[string]*DomainName),
		apiMappings:    make(map[string]map[string]*APIMapping),
		portals:        make(map[string]*Portal),
		portalProducts: make(map[string]*PortalProduct),
		productPages:   make(map[string][]*ProductPage),
		productREPages: make(map[string][]*ProductRestEndpointPage),
		mu:             lockmetrics.New("apigatewayv2"),
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
	b.productPages = make(map[string][]*ProductPage)
	b.productREPages = make(map[string][]*ProductRestEndpointPage)
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
func (b *InMemoryBackend) CreateAPI(input CreateAPIInput) (*API, error) {
	b.mu.Lock("CreateAPI")
	defer b.mu.Unlock()

	id := randomID()
	api := API{
		APIID:                    id,
		Name:                     input.Name,
		Description:              input.Description,
		ProtocolType:             input.ProtocolType,
		RouteSelectionExpression: input.RouteSelectionExpression,
		Version:                  input.Version,
		Tags:                     input.Tags,
		APIEndpoint:              "https://" + id + ".execute-api.us-east-1.amazonaws.com",
		CreatedDate:              isoTime{time.Now()},
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
		d.api.Tags = input.Tags
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

	now := isoTime{time.Now()}
	stage := &Stage{
		StageName:       input.StageName,
		APIID:           apiID,
		DeploymentID:    input.DeploymentID,
		Description:     input.Description,
		AutoDeploy:      input.AutoDeploy,
		StageVariables:  input.StageVariables,
		CreatedDate:     now,
		LastUpdatedDate: now,
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

	id := randomID()
	route := &Route{
		RouteID:           id,
		APIID:             apiID,
		RouteKey:          input.RouteKey,
		Target:            input.Target,
		AuthorizationType: input.AuthorizationType,
		AuthorizerID:      input.AuthorizerID,
		OperationName:     input.OperationName,
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
		r.RouteKey = input.RouteKey
	}

	if input.Target != "" {
		r.Target = input.Target
	}

	if input.AuthorizationType != "" {
		r.AuthorizationType = input.AuthorizationType
	}

	if input.AuthorizerID != "" {
		r.AuthorizerID = input.AuthorizerID
	}

	if input.OperationName != "" {
		r.OperationName = input.OperationName
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

	id := randomID()
	integration := &Integration{
		IntegrationID:        id,
		APIID:                apiID,
		IntegrationType:      input.IntegrationType,
		IntegrationMethod:    input.IntegrationMethod,
		IntegrationURI:       input.IntegrationURI,
		Description:          input.Description,
		PayloadFormatVersion: input.PayloadFormatVersion,
		ConnectionType:       input.ConnectionType,
		ConnectionID:         input.ConnectionID,
		TimeoutInMillis:      input.TimeoutInMillis,
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

	return nil
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

	if input.IntegrationType != "" {
		i.IntegrationType = input.IntegrationType
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

	id := randomID()
	authorizer := &Authorizer{
		AuthorizerID:                 id,
		APIID:                        apiID,
		Name:                         input.Name,
		AuthorizerType:               input.AuthorizerType,
		AuthorizerURI:                input.AuthorizerURI,
		IdentitySource:               input.IdentitySource,
		AuthorizerCredentialsArn:     input.AuthorizerCredentialsArn,
		AuthorizerResultTTLInSeconds: input.AuthorizerResultTTLInSeconds,
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

	if input.IdentitySource != "" {
		a.IdentitySource = input.IdentitySource
	}

	if input.AuthorizerCredentialsArn != "" {
		a.AuthorizerCredentialsArn = input.AuthorizerCredentialsArn
	}

	if input.AuthorizerResultTTLInSeconds != 0 {
		a.AuthorizerResultTTLInSeconds = input.AuthorizerResultTTLInSeconds
	}

	cp := *a

	return &cp, nil
}

// --- Domain Names ---

// CreateDomainName creates a new custom domain name.
func (b *InMemoryBackend) CreateDomainName(input CreateDomainNameInput) (*DomainName, error) {
	if input.DomainNameValue == "" {
		return nil, fmt.Errorf("%w: domainName is required", ErrBadRequest)
	}

	b.mu.Lock("CreateDomainName")
	defer b.mu.Unlock()

	if _, exists := b.domainNames[input.DomainNameValue]; exists {
		return nil, fmt.Errorf("%w: domain name %q already exists", ErrAlreadyExists, input.DomainNameValue)
	}

	dn := &DomainName{
		DomainNameValue: input.DomainNameValue,
		Tags:            copyTags(input.Tags),
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

	mappings, ok := b.apiMappings[domainName]
	if !ok {
		return nil, ErrDomainNameNotFound
	}

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

// TagResource adds tags to a resource identified by ARN. For the in-memory
// backend the last path segment of the ARN is treated as the API ID.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	apiID := arnToAPIID(resourceARN)

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	if d.api.Tags == nil {
		d.api.Tags = make(map[string]string)
	}

	maps.Copy(d.api.Tags, tags)

	return nil
}

// UntagResource removes tag keys from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	apiID := arnToAPIID(resourceARN)

	d, ok := b.apis[apiID]
	if !ok {
		return ErrAPINotFound
	}

	for _, k := range tagKeys {
		delete(d.api.Tags, k)
	}

	return nil
}

// GetTags retrieves all tags for a resource identified by ARN.
func (b *InMemoryBackend) GetTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	apiID := arnToAPIID(resourceARN)

	d, ok := b.apis[apiID]
	if !ok {
		return nil, ErrAPINotFound
	}

	return copyTags(d.api.Tags), nil
}

// arnToAPIID extracts the API ID from a resource ARN. For the in-memory
// backend the last path segment of the ARN is used as the API ID.
// [strings.Split] always returns at least one element so len(parts)-1 is safe.
func arnToAPIID(arn string) string {
	if arn == "" {
		return ""
	}

	parts := strings.Split(arn, "/")

	return parts[len(parts)-1]
}
