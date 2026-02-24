package apigateway

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrRestApiNotFound  = errors.New("NotFoundException")
	ErrResourceNotFound = errors.New("NotFoundException")
	ErrMethodNotFound   = errors.New("NotFoundException")
	ErrAlreadyExists    = errors.New("ConflictException")
	ErrInvalidParameter = errors.New("BadRequestException")
)

// StorageBackend is the interface for the API Gateway in-memory store.
type StorageBackend interface {
	// REST APIs
	CreateRestApi(name, description string, tags map[string]string) (*RestApi, error)
	DeleteRestApi(restApiID string) error
	GetRestApi(restApiID string) (*RestApi, error)
	GetRestApis(limit int, position string) ([]RestApi, string, error)

	// Resources
	GetResources(restApiID, position string, limit int) ([]Resource, string, error)
	GetResource(restApiID, resourceID string) (*Resource, error)
	CreateResource(restApiID, parentID, pathPart string) (*Resource, error)
	DeleteResource(restApiID, resourceID string) error

	// Methods
	PutMethod(restApiID, resourceID, httpMethod, authType string, apiKeyRequired bool) (*Method, error)
	GetMethod(restApiID, resourceID, httpMethod string) (*Method, error)
	DeleteMethod(restApiID, resourceID, httpMethod string) error

	// Integrations
	PutIntegration(restApiID, resourceID, httpMethod string, input PutIntegrationInput) (*Integration, error)
	GetIntegration(restApiID, resourceID, httpMethod string) (*Integration, error)
	DeleteIntegration(restApiID, resourceID, httpMethod string) error

	// Deployments
	CreateDeployment(restApiID, stageName, description string) (*Deployment, error)
	GetDeployments(restApiID string) ([]Deployment, error)

	// Stages
	GetStages(restApiID string) ([]Stage, error)
	GetStage(restApiID, stageName string) (*Stage, error)
	DeleteStage(restApiID, stageName string) error
}

const (
	apiIDChars      = "abcdefghijklmnopqrstuvwxyz0123456789"
	apiIDLength     = 10
	resourceIDLength = 6
)

func randomID(length int) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = apiIDChars[rand.Intn(len(apiIDChars))]
	}
	return string(b)
}

// apiData holds per-REST-API state.
type apiData struct {
	api         RestApi
	resources   map[string]*Resource   // resourceID -> Resource
	deployments map[string]*Deployment // deploymentID -> Deployment
	stages      map[string]*Stage      // stageName -> Stage
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	mu   sync.RWMutex
	apis map[string]*apiData // restApiID -> apiData
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		apis: make(map[string]*apiData),
	}
}

// CreateRestApi creates a new REST API and its root resource.
func (b *InMemoryBackend) CreateRestApi(name, description string, tags map[string]string) (*RestApi, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	id := randomID(apiIDLength)
	api := RestApi{
		ID:          id,
		Name:        name,
		Description: description,
		CreatedDate: time.Now(),
		Tags:        tags,
	}

	rootID := randomID(resourceIDLength)
	root := &Resource{
		ID:              rootID,
		ParentID:        "",
		PathPart:        "",
		Path:            "/",
		RestApiID:       id,
		ResourceMethods: make(map[string]*Method),
	}

	b.apis[id] = &apiData{
		api:         api,
		resources:   map[string]*Resource{rootID: root},
		deployments: make(map[string]*Deployment),
		stages:      make(map[string]*Stage),
	}

	return &api, nil
}

// DeleteRestApi removes a REST API and all its resources.
func (b *InMemoryBackend) DeleteRestApi(restApiID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.apis[restApiID]; !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
	}
	delete(b.apis, restApiID)
	return nil
}

// GetRestApi returns a single REST API.
func (b *InMemoryBackend) GetRestApi(restApiID string) (*RestApi, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
	}
	cp := d.api
	return &cp, nil
}

// GetRestApis returns all REST APIs with pagination.
func (b *InMemoryBackend) GetRestApis(limit int, position string) ([]RestApi, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := make([]RestApi, 0, len(b.apis))
	for _, d := range b.apis {
		all = append(all, d.api)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })

	startIdx := parsePosition(position)
	if startIdx >= len(all) {
		return []RestApi{}, "", nil
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
func (b *InMemoryBackend) GetResources(restApiID, position string, limit int) ([]Resource, string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return nil, "", fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
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
func (b *InMemoryBackend) GetResource(restApiID, resourceID string) (*Resource, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	cp := *r
	return &cp, nil
}

// CreateResource creates a new resource under a parent.
func (b *InMemoryBackend) CreateResource(restApiID, parentID, pathPart string) (*Resource, error) {
	if pathPart == "" {
		return nil, fmt.Errorf("%w: pathPart is required", ErrInvalidParameter)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
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
		RestApiID:       restApiID,
		ResourceMethods: make(map[string]*Method),
	}
	d.resources[id] = res

	cp := *res
	return &cp, nil
}

// DeleteResource removes a resource.
func (b *InMemoryBackend) DeleteResource(restApiID, resourceID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
	}
	if _, ok := d.resources[resourceID]; !ok {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	delete(d.resources, resourceID)
	return nil
}

// PutMethod creates or replaces a method on a resource.
func (b *InMemoryBackend) PutMethod(restApiID, resourceID, httpMethod, authType string, apiKeyRequired bool) (*Method, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}

	m := &Method{
		HttpMethod:        httpMethod,
		AuthorizationType: authType,
		ApiKeyRequired:    apiKeyRequired,
		RequestParameters: make(map[string]bool),
		MethodResponses:   make(map[string]*MethodResponse),
	}
	r.ResourceMethods[httpMethod] = m

	cp := *m
	return &cp, nil
}

// GetMethod retrieves a method on a resource.
func (b *InMemoryBackend) GetMethod(restApiID, resourceID, httpMethod string) (*Method, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
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
func (b *InMemoryBackend) DeleteMethod(restApiID, resourceID, httpMethod string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}
	if _, ok := r.ResourceMethods[httpMethod]; !ok {
		return fmt.Errorf("%w: method %s not found", ErrMethodNotFound, httpMethod)
	}
	delete(r.ResourceMethods, httpMethod)
	return nil
}

// PutIntegration creates or replaces an integration on a method.
func (b *InMemoryBackend) PutIntegration(restApiID, resourceID, httpMethod string, input PutIntegrationInput) (*Integration, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
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
		Type:                input.Type,
		HttpMethod:          input.HttpMethod,
		Uri:                 input.Uri,
		PassthroughBehavior: input.PassthroughBehavior,
		RequestTemplates:    input.RequestTemplates,
		IntegrationResponses: make(map[string]*IntegrationResponse),
	}
	m.MethodIntegration = integ

	cp := *integ
	return &cp, nil
}

// GetIntegration retrieves the integration for a method.
func (b *InMemoryBackend) GetIntegration(restApiID, resourceID, httpMethod string) (*Integration, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
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
func (b *InMemoryBackend) DeleteIntegration(restApiID, resourceID, httpMethod string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
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

// CreateDeployment creates a deployment and associated stage.
func (b *InMemoryBackend) CreateDeployment(restApiID, stageName, description string) (*Deployment, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
	}

	now := time.Now()
	deplID := randomID(apiIDLength)
	depl := &Deployment{
		ID:          deplID,
		RestApiID:   restApiID,
		Description: description,
		CreatedDate: now,
	}
	d.deployments[deplID] = depl

	if stageName != "" {
		stage := &Stage{
			StageName:       stageName,
			RestApiID:       restApiID,
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
func (b *InMemoryBackend) GetDeployments(restApiID string) ([]Deployment, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
	}

	all := make([]Deployment, 0, len(d.deployments))
	for _, dep := range d.deployments {
		all = append(all, *dep)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].ID < all[j].ID })
	return all, nil
}

// GetStages returns all stages for a REST API.
func (b *InMemoryBackend) GetStages(restApiID string) ([]Stage, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
	}

	all := make([]Stage, 0, len(d.stages))
	for _, s := range d.stages {
		all = append(all, *s)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].StageName < all[j].StageName })
	return all, nil
}

// GetStage returns a single stage.
func (b *InMemoryBackend) GetStage(restApiID, stageName string) (*Stage, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
	}
	s, ok := d.stages[stageName]
	if !ok {
		return nil, fmt.Errorf("%w: stage %s not found", ErrResourceNotFound, stageName)
	}
	cp := *s
	return &cp, nil
}

// DeleteStage removes a stage.
func (b *InMemoryBackend) DeleteStage(restApiID, stageName string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.apis[restApiID]
	if !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestApiNotFound, restApiID)
	}
	if _, ok := d.stages[stageName]; !ok {
		return fmt.Errorf("%w: stage %s not found", ErrResourceNotFound, stageName)
	}
	delete(d.stages, stageName)
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
