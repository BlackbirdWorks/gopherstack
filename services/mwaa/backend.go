package mwaa

import (
	"fmt"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const (
	defaultAirflowVersion      = "2.10.3"
	defaultEnvironmentClass    = "mw1.small"
	defaultMaxWorkers          = int32(10)
	defaultMinWorkers          = int32(1)
	defaultWebserverAccessMode = "PUBLIC_ONLY"
	environmentStatusAvailable = "AVAILABLE"
)

// Errors used by the backend.
var (
	// ErrEnvironmentNotFound is returned when an environment does not exist.
	ErrEnvironmentNotFound = awserr.New("ResourceNotFoundException: environment not found", awserr.ErrNotFound)
	// ErrEnvironmentAlreadyExists is returned when an environment already exists.
	ErrEnvironmentAlreadyExists = awserr.New(
		"AlreadyExistsException: environment already exists",
		awserr.ErrAlreadyExists,
	)
)

// StorageBackend is the interface for the MWAA in-memory backend.
type StorageBackend interface {
	CreateEnvironment(region, accountID, name string, req *createEnvironmentRequest) (*Environment, error)
	GetEnvironment(name string) (*Environment, error)
	DeleteEnvironment(name string) (*Environment, error)
	UpdateEnvironment(name string, req *updateEnvironmentRequest) (*Environment, error)
	ListEnvironments() ([]string, error)
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)
}

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	environments map[string]*Environment
	region       string
	accountID    string
	mu           sync.RWMutex
}

// NewInMemoryBackend creates a new MWAA in-memory backend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	return &InMemoryBackend{
		region:       region,
		accountID:    accountID,
		environments: make(map[string]*Environment),
	}
}

// CreateEnvironment creates a new MWAA environment.
func (b *InMemoryBackend) CreateEnvironment(
	region, accountID, name string,
	req *createEnvironmentRequest,
) (*Environment, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.environments[name]; exists {
		return nil, ErrEnvironmentAlreadyExists
	}

	airflowVersion := req.AirflowVersion
	if airflowVersion == "" {
		airflowVersion = defaultAirflowVersion
	}

	envClass := req.EnvironmentClass
	if envClass == "" {
		envClass = defaultEnvironmentClass
	}

	maxWorkers := req.MaxWorkers
	if maxWorkers == 0 {
		maxWorkers = defaultMaxWorkers
	}

	minWorkers := req.MinWorkers
	if minWorkers == 0 {
		minWorkers = defaultMinWorkers
	}

	accessMode := req.WebserverAccessMode
	if accessMode == "" {
		accessMode = defaultWebserverAccessMode
	}

	envARN := arn.Build("airflow", region, accountID, fmt.Sprintf("environment/%s", name))

	// Generate a deterministic-looking unique ID for the webserver URL.
	uniqueID := fmt.Sprintf("%x", len(name)+len(region))

	tags := make(map[string]string)
	maps.Copy(tags, req.Tags)

	env := &Environment{
		Name:                 name,
		ARN:                  envARN,
		Status:               environmentStatusAvailable,
		DagS3Path:            req.DagS3Path,
		ExecutionRoleArn:     req.ExecutionRoleArn,
		SourceBucketArn:      req.SourceBucketArn,
		AirflowVersion:       airflowVersion,
		EnvironmentClass:     envClass,
		MaxWorkers:           maxWorkers,
		MinWorkers:           minWorkers,
		WebserverURL:         fmt.Sprintf("https://%s.airflow.%s.amazonaws.com", uniqueID, region),
		WebserverAccessMode:  accessMode,
		NetworkConfiguration: req.NetworkConfiguration,
		Tags:                 tags,
		CreatedAt:            time.Now(),
	}

	b.environments[name] = env

	return env, nil
}

// GetEnvironment retrieves an MWAA environment by name.
func (b *InMemoryBackend) GetEnvironment(name string) (*Environment, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	env, ok := b.environments[name]
	if !ok {
		return nil, ErrEnvironmentNotFound
	}

	return env, nil
}

// DeleteEnvironment deletes an MWAA environment by name.
func (b *InMemoryBackend) DeleteEnvironment(name string) (*Environment, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	env, ok := b.environments[name]
	if !ok {
		return nil, ErrEnvironmentNotFound
	}

	delete(b.environments, name)

	return env, nil
}

// UpdateEnvironment updates an existing MWAA environment.
func (b *InMemoryBackend) UpdateEnvironment(name string, req *updateEnvironmentRequest) (*Environment, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	env, ok := b.environments[name]
	if !ok {
		return nil, ErrEnvironmentNotFound
	}

	if req.DagS3Path != "" {
		env.DagS3Path = req.DagS3Path
	}

	if req.ExecutionRoleArn != "" {
		env.ExecutionRoleArn = req.ExecutionRoleArn
	}

	if req.SourceBucketArn != "" {
		env.SourceBucketArn = req.SourceBucketArn
	}

	if req.AirflowVersion != "" {
		env.AirflowVersion = req.AirflowVersion
	}

	if req.EnvironmentClass != "" {
		env.EnvironmentClass = req.EnvironmentClass
	}

	if req.MaxWorkers != 0 {
		env.MaxWorkers = req.MaxWorkers
	}

	if req.MinWorkers != 0 {
		env.MinWorkers = req.MinWorkers
	}

	if req.WebserverAccessMode != "" {
		env.WebserverAccessMode = req.WebserverAccessMode
	}

	if req.NetworkConfiguration != nil {
		env.NetworkConfiguration = req.NetworkConfiguration
	}

	return env, nil
}

// ListEnvironments returns a sorted list of environment names.
func (b *InMemoryBackend) ListEnvironments() ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.environments))

	for name := range b.environments {
		names = append(names, name)
	}

	sort.Strings(names)

	return names, nil
}

// TagResource adds or updates tags on a resource identified by its ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	env := b.findByARN(resourceARN)
	if env == nil {
		return ErrEnvironmentNotFound
	}

	if env.Tags == nil {
		env.Tags = make(map[string]string)
	}

	maps.Copy(env.Tags, tags)

	return nil
}

// UntagResource removes tags from a resource identified by its ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	env := b.findByARN(resourceARN)
	if env == nil {
		return ErrEnvironmentNotFound
	}

	for _, k := range tagKeys {
		delete(env.Tags, k)
	}

	return nil
}

// ListTagsForResource returns all tags for a resource identified by its ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	env := b.findByARN(resourceARN)
	if env == nil {
		return nil, ErrEnvironmentNotFound
	}

	result := make(map[string]string, len(env.Tags))
	maps.Copy(result, env.Tags)

	return result, nil
}

// findByARN looks up an environment by its ARN. Must be called with lock held.
func (b *InMemoryBackend) findByARN(resourceARN string) *Environment {
	for _, env := range b.environments {
		if env.ARN == resourceARN {
			return env
		}
	}

	return nil
}
