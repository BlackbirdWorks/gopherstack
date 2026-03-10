package apigatewayv2

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sort"
	"time"

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
}

// apiData holds per-API state.
type apiData struct {
	stages       map[string]*Stage
	routes       map[string]*Route
	integrations map[string]*Integration
	deployments  map[string]*Deployment
	authorizers  map[string]*Authorizer
	api          API
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	apis map[string]*apiData
	mu   *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		apis: make(map[string]*apiData),
		mu:   lockmetrics.New("apigatewayv2"),
	}
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
		api:          api,
		stages:       make(map[string]*Stage),
		routes:       make(map[string]*Route),
		integrations: make(map[string]*Integration),
		deployments:  make(map[string]*Deployment),
		authorizers:  make(map[string]*Authorizer),
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
