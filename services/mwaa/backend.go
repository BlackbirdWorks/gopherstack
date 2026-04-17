package mwaa

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defaultAirflowVersion      = "2.10.3"
	defaultEnvironmentClass    = "mw1.small"
	defaultMaxWorkers          = int32(10)
	defaultMinWorkers          = int32(1)
	defaultWebserverAccessMode = "PUBLIC_ONLY"
	restAPISuccessCode         = int32(200)
	maxMetricsPerEnv           = 1000

	// Environment status constants.
	envStatusAvailable = "AVAILABLE"
	envStatusCreating  = "CREATING"
	envStatusDeleting  = "DELETING"
	envStatusUpdating  = "UPDATING"
	envStatusError     = "ERROR"

	// WebserverAccessMode constants.
	accessModePublic  = "PUBLIC_ONLY"
	accessModePrivate = "PRIVATE_ONLY"
)

// validEnvironmentClasses returns the set of valid environment class values.
func validEnvironmentClasses() map[string]struct{} {
	return map[string]struct{}{
		"mw1.small":   {},
		"mw1.medium":  {},
		"mw1.large":   {},
		"mw1.xlarge":  {},
		"mw1.2xlarge": {},
	}
}

// Errors used by the backend.
var (
	// ErrEnvironmentNotFound is returned when an environment does not exist.
	ErrEnvironmentNotFound = awserr.New("ResourceNotFoundException: environment not found", awserr.ErrNotFound)
	// ErrEnvironmentAlreadyExists is returned when an environment already exists.
	ErrEnvironmentAlreadyExists = awserr.New(
		"AlreadyExistsException: environment already exists",
		awserr.ErrAlreadyExists,
	)
	// ErrInvalidParameter is returned when an invalid or missing parameter is provided.
	ErrInvalidParameter = awserr.New("ValidationException: invalid parameter", awserr.ErrInvalidParameter)
)

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// InMemoryBackend is the in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	environments map[string]*Environment
	arnIndex     map[string]string
	metrics      map[string][]MetricDatum
	mu           *lockmetrics.RWMutex
	region       string
	accountID    string
}

// NewInMemoryBackend creates a new MWAA in-memory backend.
func NewInMemoryBackend(region, accountID string) *InMemoryBackend {
	return &InMemoryBackend{
		region:       region,
		accountID:    accountID,
		environments: make(map[string]*Environment),
		arnIndex:     make(map[string]string),
		metrics:      make(map[string][]MetricDatum),
		mu:           lockmetrics.New("mwaa"),
	}
}

// Region returns the configured region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the configured account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset closes the current mutex and reinitialises all maps.
func (b *InMemoryBackend) Reset() {
	b.mu.Close()
	b.mu = lockmetrics.New("mwaa")
	b.environments = make(map[string]*Environment)
	b.arnIndex = make(map[string]string)
	b.metrics = make(map[string][]MetricDatum)
}

// AddEnvironmentInternal creates an environment with minimal defaults, bypassing
// validation, intended for use in tests only.
func (b *InMemoryBackend) AddEnvironmentInternal(name string) *Environment {
	b.mu.Lock("AddEnvironmentInternal")
	defer b.mu.Unlock()

	envARN := arn.Build("airflow", b.region, b.accountID, "environment/"+name)
	env := &Environment{
		Name:      name,
		ARN:       envARN,
		Status:    envStatusAvailable,
		Tags:      make(map[string]string),
		CreatedAt: epochSecondsNow(),
	}

	b.environments[name] = env
	b.arnIndex[envARN] = name

	return env
}

// validateCreateRequest validates required fields and enumerated values for CreateEnvironment.
func validateCreateRequest(req *createEnvironmentRequest) error {
	if req.DagS3Path == "" {
		return fmt.Errorf("%w: DagS3Path is required", ErrInvalidParameter)
	}

	if req.ExecutionRoleArn == "" {
		return fmt.Errorf("%w: ExecutionRoleArn is required", ErrInvalidParameter)
	}

	if req.SourceBucketArn == "" {
		return fmt.Errorf("%w: SourceBucketArn is required", ErrInvalidParameter)
	}

	if req.WebserverAccessMode != "" &&
		req.WebserverAccessMode != accessModePublic &&
		req.WebserverAccessMode != accessModePrivate {
		return fmt.Errorf(
			"%w: WebserverAccessMode must be %s or %s",
			ErrInvalidParameter, accessModePublic, accessModePrivate,
		)
	}

	if req.EnvironmentClass != "" {
		if _, ok := validEnvironmentClasses()[req.EnvironmentClass]; !ok {
			return fmt.Errorf("%w: invalid EnvironmentClass %q", ErrInvalidParameter, req.EnvironmentClass)
		}
	}

	return nil
}

// CreateEnvironment creates a new MWAA environment.
func (b *InMemoryBackend) CreateEnvironment(
	region, accountID, name string,
	req *createEnvironmentRequest,
) (*Environment, error) {
	if err := validateCreateRequest(req); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateEnvironment")
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

	if minWorkers > maxWorkers {
		return nil, fmt.Errorf(
			"%w: MinWorkers (%d) must be <= MaxWorkers (%d)",
			ErrInvalidParameter,
			minWorkers,
			maxWorkers,
		)
	}

	accessMode := req.WebserverAccessMode
	if accessMode == "" {
		accessMode = defaultWebserverAccessMode
	}

	envARN := arn.Build("airflow", region, accountID, "environment/"+name)

	// Generate a deterministic unique ID for the webserver URL based on the environment name.
	sum := sha256.Sum256([]byte(name))
	uniqueID := hex.EncodeToString(sum[:8])

	tags := make(map[string]string)
	maps.Copy(tags, req.Tags)

	env := &Environment{
		Name:                 name,
		ARN:                  envARN,
		Status:               envStatusAvailable,
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
		CreatedAt:            epochSecondsNow(),
	}

	b.environments[name] = env
	b.arnIndex[envARN] = name

	return env, nil
}

// GetEnvironment retrieves a deep copy of an MWAA environment by name.
func (b *InMemoryBackend) GetEnvironment(name string) (*Environment, error) {
	b.mu.RLock("GetEnvironment")
	defer b.mu.RUnlock()

	env, ok := b.environments[name]
	if !ok {
		return nil, ErrEnvironmentNotFound
	}

	return cloneEnvironment(env), nil
}

// DeleteEnvironment deletes an MWAA environment by name and cascades to metrics.
func (b *InMemoryBackend) DeleteEnvironment(name string) (*Environment, error) {
	b.mu.Lock("DeleteEnvironment")
	defer b.mu.Unlock()

	env, ok := b.environments[name]
	if !ok {
		return nil, ErrEnvironmentNotFound
	}

	delete(b.environments, name)
	delete(b.arnIndex, env.ARN)
	delete(b.metrics, name)

	return env, nil
}

// UpdateEnvironment updates an existing MWAA environment.
func (b *InMemoryBackend) UpdateEnvironment(name string, req *updateEnvironmentRequest) (*Environment, error) {
	b.mu.Lock("UpdateEnvironment")
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

	if env.MinWorkers > env.MaxWorkers {
		return nil, fmt.Errorf(
			"%w: MinWorkers (%d) must be <= MaxWorkers (%d)",
			ErrInvalidParameter,
			env.MinWorkers,
			env.MaxWorkers,
		)
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
	b.mu.RLock("ListEnvironments")
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
	b.mu.Lock("TagResource")
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
	b.mu.Lock("UntagResource")
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
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	env := b.findByARN(resourceARN)
	if env == nil {
		return nil, ErrEnvironmentNotFound
	}

	result := make(map[string]string, len(env.Tags))
	maps.Copy(result, env.Tags)

	return result, nil
}

// findByARN looks up an environment by its ARN using the ARN index. Must be called with lock held.
func (b *InMemoryBackend) findByARN(resourceARN string) *Environment {
	name, ok := b.arnIndex[resourceARN]
	if !ok {
		return nil
	}

	return b.environments[name]
}

// InvokeRestAPI simulates calling the Apache Airflow REST API on the specified environment's webserver.
func (b *InMemoryBackend) InvokeRestAPI(envName string, req *invokeRestAPIRequest) (*InvokeRestAPIResponse, error) {
	b.mu.RLock("InvokeRestAPI")
	defer b.mu.RUnlock()

	if _, ok := b.environments[envName]; !ok {
		return nil, ErrEnvironmentNotFound
	}

	if req.Method == "" {
		return nil, fmt.Errorf("%w: Method is required", ErrInvalidParameter)
	}

	if req.Path == "" {
		return nil, fmt.Errorf("%w: Path is required", ErrInvalidParameter)
	}

	return &InvokeRestAPIResponse{
		RestAPIStatusCode: restAPISuccessCode,
		RestAPIResponse:   map[string]any{},
	}, nil
}

// PublishMetrics stores internal environment metrics for the specified environment.
// The total number of metrics per environment is capped at maxMetricsPerEnv.
func (b *InMemoryBackend) PublishMetrics(envName string, req *publishMetricsRequest) error {
	b.mu.Lock("PublishMetrics")
	defer b.mu.Unlock()

	if _, ok := b.environments[envName]; !ok {
		return ErrEnvironmentNotFound
	}

	b.metrics[envName] = append(b.metrics[envName], req.MetricData...)

	if len(b.metrics[envName]) > maxMetricsPerEnv {
		b.metrics[envName] = b.metrics[envName][len(b.metrics[envName])-maxMetricsPerEnv:]
	}

	return nil
}

// CreateCliToken validates that the environment exists and returns a stub CLI token.
func (b *InMemoryBackend) CreateCliToken(envName string) (string, error) {
	b.mu.RLock("CreateCliToken")
	defer b.mu.RUnlock()

	if _, ok := b.environments[envName]; !ok {
		return "", ErrEnvironmentNotFound
	}

	return "stub-cli-token-" + envName, nil
}

// CreateWebLoginToken validates that the environment exists and returns a stub web login token.
func (b *InMemoryBackend) CreateWebLoginToken(envName string) (string, error) {
	b.mu.RLock("CreateWebLoginToken")
	defer b.mu.RUnlock()

	if _, ok := b.environments[envName]; !ok {
		return "", ErrEnvironmentNotFound
	}

	return "stub-web-token-" + envName, nil
}

// cloneEnvironment returns a deep copy of the given environment.
func cloneEnvironment(env *Environment) *Environment {
	clone := *env

	if env.Tags != nil {
		clone.Tags = make(map[string]string, len(env.Tags))
		maps.Copy(clone.Tags, env.Tags)
	}

	if env.NetworkConfiguration != nil {
		nc := *env.NetworkConfiguration
		clone.NetworkConfiguration = &nc
	}

	return &clone
}
