package apigateway

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	ErrRestAPINotFound    = errors.New("NotFoundException")
	ErrResourceNotFound   = errors.New("NotFoundException")
	ErrMethodNotFound     = errors.New("NotFoundException")
	ErrDeploymentNotFound = errors.New("NotFoundException")
	ErrAlreadyExists      = awserr.New("ConflictException", awserr.ErrAlreadyExists)
	ErrInvalidParameter   = errors.New("BadRequestException")
)

// StorageBackend is the interface for the API Gateway in-memory store.
type StorageBackend interface {
	// REST APIs
	CreateRestAPI(name, description string, inputTags *tags.Tags) (*RestAPI, error)
	DeleteRestAPI(restAPIID string) error
	GetRestAPI(restAPIID string) (*RestAPI, error)
	GetRestAPIs(limit int, position string) ([]RestAPI, string, error)

	// Resources
	GetResources(restAPIID, position string, limit int) ([]Resource, string, error)
	GetResource(restAPIID, resourceID string) (*Resource, error)
	CreateResource(restAPIID, parentID, pathPart string) (*Resource, error)
	DeleteResource(restAPIID, resourceID string) error

	// Methods
	PutMethod(restAPIID, resourceID, httpMethod, authType string, apiKeyRequired bool) (*Method, error)
	GetMethod(restAPIID, resourceID, httpMethod string) (*Method, error)
	DeleteMethod(restAPIID, resourceID, httpMethod string) error

	// Integrations
	PutIntegration(restAPIID, resourceID, httpMethod string, input PutIntegrationInput) (*Integration, error)
	GetIntegration(restAPIID, resourceID, httpMethod string) (*Integration, error)
	DeleteIntegration(restAPIID, resourceID, httpMethod string) error

	// Deployments
	CreateDeployment(restAPIID, stageName, description string) (*Deployment, error)
	GetDeployment(restAPIID, deploymentID string) (*Deployment, error)
	GetDeployments(restAPIID string) ([]Deployment, error)
	DeleteDeployment(restAPIID, deploymentID string) error

	// Stages
	GetStages(restAPIID string) ([]Stage, error)
	GetStage(restAPIID, stageName string) (*Stage, error)
	DeleteStage(restAPIID, stageName string) error
}

const apiIDChars = "abcdefghijklmnopqrstuvwxyz0123456789"

const (
	apiIDLength      = 10
	resourceIDLength = 6
)

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

// apiData holds per-REST-API state.
type apiData struct {
	resources   map[string]*Resource   // resourceID -> Resource
	deployments map[string]*Deployment // deploymentID -> Deployment
	stages      map[string]*Stage      // stageName -> Stage
	api         RestAPI
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
		mu:   lockmetrics.New("apigateway"),
	}
}

// CreateRestAPI creates a new REST API and its root resource.
func (b *InMemoryBackend) CreateRestAPI(name, description string, inputTags *tags.Tags) (*RestAPI, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateRestAPI")
	defer b.mu.Unlock()

	id := randomID(apiIDLength)

	var backendTags *tags.Tags
	if inputTags == nil {
		backendTags = tags.New("apigw.api." + id + ".tags")
	} else {
		backendTags = tags.FromMap("apigw.api."+id+".tags", inputTags.Clone())
	}

	rootID := randomID(resourceIDLength)

	api := RestAPI{
		ID:             id,
		Name:           name,
		Description:    description,
		CreatedDate:    unixEpochTime{time.Now()},
		Tags:           backendTags,
		RootResourceID: rootID,
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
		api:         api,
		resources:   map[string]*Resource{rootID: root},
		deployments: make(map[string]*Deployment),
		stages:      make(map[string]*Stage),
	}

	return &api, nil
}

// DeleteRestAPI removes a REST API and all its resources.
func (b *InMemoryBackend) DeleteRestAPI(restAPIID string) error {
	b.mu.Lock("DeleteRestAPI")
	defer b.mu.Unlock()

	if _, ok := b.apis[restAPIID]; !ok {
		return fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
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
func (b *InMemoryBackend) PutMethod(
	restAPIID, resourceID, httpMethod, authType string,
	apiKeyRequired bool,
) (*Method, error) {
	b.mu.Lock("PutMethod")
	defer b.mu.Unlock()

	d, ok := b.apis[restAPIID]
	if !ok {
		return nil, fmt.Errorf("%w: REST API %s not found", ErrRestAPINotFound, restAPIID)
	}
	r, ok := d.resources[resourceID]
	if !ok {
		return nil, fmt.Errorf("%w: resource %s not found", ErrResourceNotFound, resourceID)
	}

	m := &Method{
		HTTPMethod:        httpMethod,
		AuthorizationType: authType,
		APIKeyRequired:    apiKeyRequired,
		RequestParameters: make(map[string]bool),
		MethodResponses:   make(map[string]*MethodResponse),
	}
	r.ResourceMethods[httpMethod] = m

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
		all = append(all, *s)
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
