package appconfig

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const appConfigIDChars = "abcdefghijklmnopqrstuvwxyz0123456789"

// newResourceID generates a cryptographically random 7-character lowercase alphanumeric ID,
// matching the format of real AWS AppConfig resource IDs (4-7 chars required by the provider).
func newResourceID() string {
	const length = 7
	b := make([]byte, length)
	charCount := uint64(len(appConfigIDChars))

	for i := range b {
		var v [8]byte
		_, _ = rand.Read(v[:])
		b[i] = appConfigIDChars[binary.BigEndian.Uint64(v[:])%charCount]
	}

	return string(b)
}

// InMemoryBackend implements StorageBackend for AppConfig using in-memory maps.
type InMemoryBackend struct {
	applications         map[string]*Application
	environments         map[string]map[string]*Environment
	configProfiles       map[string]map[string]*ConfigurationProfile
	hostedConfigVersions map[string]map[string]map[int32]*HostedConfigurationVersion
	deploymentStrategies map[string]*DeploymentStrategy
	deployments          map[string]map[string]map[int32]*Deployment
	tags                 map[string]map[string]string
	versionCounters      map[string]map[string]int32
	deploymentCounters   map[string]map[string]int32
	mu                   *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend for AppConfig.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		applications:         make(map[string]*Application),
		environments:         make(map[string]map[string]*Environment),
		configProfiles:       make(map[string]map[string]*ConfigurationProfile),
		hostedConfigVersions: make(map[string]map[string]map[int32]*HostedConfigurationVersion),
		deploymentStrategies: make(map[string]*DeploymentStrategy),
		deployments:          make(map[string]map[string]map[int32]*Deployment),
		tags:                 make(map[string]map[string]string),
		versionCounters:      make(map[string]map[string]int32),
		deploymentCounters:   make(map[string]map[string]int32),
		mu:                   lockmetrics.New("appconfig"),
	}
}

// CreateApplication creates a new AppConfig application.
func (b *InMemoryBackend) CreateApplication(name, description string) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	now := time.Now()
	app := &Application{
		ID:          newResourceID(),
		Name:        name,
		Description: description,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	b.applications[app.ID] = app
	cp := *app

	return &cp, nil
}

// GetApplication retrieves an application by ID.
func (b *InMemoryBackend) GetApplication(applicationID string) (*Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	app, ok := b.applications[applicationID]
	if !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	cp := *app

	return &cp, nil
}

// ListApplications returns all applications.
func (b *InMemoryBackend) ListApplications() []Application {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	out := make([]Application, 0, len(b.applications))
	for _, app := range b.applications {
		out = append(out, *app)
	}

	return out
}

// UpdateApplication updates an application's name and description.
func (b *InMemoryBackend) UpdateApplication(applicationID, name, description string) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[applicationID]
	if !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	if name != "" {
		app.Name = name
	}

	app.Description = description
	app.UpdatedAt = time.Now()
	cp := *app

	return &cp, nil
}

// DeleteApplication deletes an application by ID.
func (b *InMemoryBackend) DeleteApplication(applicationID string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationID]; !ok {
		return fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	delete(b.applications, applicationID)
	delete(b.environments, applicationID)
	delete(b.configProfiles, applicationID)
	delete(b.hostedConfigVersions, applicationID)
	delete(b.deployments, applicationID)
	delete(b.versionCounters, applicationID)
	delete(b.deploymentCounters, applicationID)

	return nil
}

// CreateEnvironment creates a new environment within an application.
func (b *InMemoryBackend) CreateEnvironment(applicationID, name, description string) (*Environment, error) {
	b.mu.Lock("CreateEnvironment")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	if b.environments[applicationID] == nil {
		b.environments[applicationID] = make(map[string]*Environment)
	}

	now := time.Now()
	env := &Environment{
		ID:            newResourceID(),
		ApplicationID: applicationID,
		Name:          name,
		Description:   description,
		State:         "ReadyForDeployment",
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	b.environments[applicationID][env.ID] = env
	cp := *env

	return &cp, nil
}

// GetEnvironment retrieves an environment by application and environment ID.
func (b *InMemoryBackend) GetEnvironment(applicationID, environmentID string) (*Environment, error) {
	b.mu.RLock("GetEnvironment")
	defer b.mu.RUnlock()

	envs, ok := b.environments[applicationID]
	if !ok {
		return nil, fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	env, ok := envs[environmentID]
	if !ok {
		return nil, fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	cp := *env

	return &cp, nil
}

// ListEnvironments returns all environments for an application.
func (b *InMemoryBackend) ListEnvironments(applicationID string) ([]Environment, error) {
	b.mu.RLock("ListEnvironments")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	envs := b.environments[applicationID]
	out := make([]Environment, 0, len(envs))

	for _, e := range envs {
		out = append(out, *e)
	}

	return out, nil
}

// UpdateEnvironment updates an environment's name and description.
func (b *InMemoryBackend) UpdateEnvironment(
	applicationID, environmentID, name, description string,
) (*Environment, error) {
	b.mu.Lock("UpdateEnvironment")
	defer b.mu.Unlock()

	envs, ok := b.environments[applicationID]
	if !ok {
		return nil, fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	env, ok := envs[environmentID]
	if !ok {
		return nil, fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	if name != "" {
		env.Name = name
	}

	env.Description = description
	env.UpdatedAt = time.Now()
	cp := *env

	return &cp, nil
}

// DeleteEnvironment deletes an environment.
func (b *InMemoryBackend) DeleteEnvironment(applicationID, environmentID string) error {
	b.mu.Lock("DeleteEnvironment")
	defer b.mu.Unlock()

	envs, ok := b.environments[applicationID]
	if !ok {
		return fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	if _, exists := envs[environmentID]; !exists {
		return fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	delete(envs, environmentID)

	return nil
}

// CreateConfigurationProfile creates a new configuration profile.
func (b *InMemoryBackend) CreateConfigurationProfile(
	applicationID, name, description, locationURI, profileType string,
) (*ConfigurationProfile, error) {
	b.mu.Lock("CreateConfigurationProfile")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	if b.configProfiles[applicationID] == nil {
		b.configProfiles[applicationID] = make(map[string]*ConfigurationProfile)
	}

	profile := &ConfigurationProfile{
		ID:            newResourceID(),
		ApplicationID: applicationID,
		Name:          name,
		Description:   description,
		LocationURI:   locationURI,
		Type:          profileType,
	}
	b.configProfiles[applicationID][profile.ID] = profile
	cp := *profile

	return &cp, nil
}

// GetConfigurationProfile retrieves a configuration profile.
func (b *InMemoryBackend) GetConfigurationProfile(applicationID, profileID string) (*ConfigurationProfile, error) {
	b.mu.RLock("GetConfigurationProfile")
	defer b.mu.RUnlock()

	profiles, ok := b.configProfiles[applicationID]
	if !ok {
		return nil, fmt.Errorf("%w: configuration profile %s", ErrConfigurationProfileNotFound, profileID)
	}

	profile, ok := profiles[profileID]
	if !ok {
		return nil, fmt.Errorf("%w: configuration profile %s", ErrConfigurationProfileNotFound, profileID)
	}

	cp := *profile

	return &cp, nil
}

// ListConfigurationProfiles returns all profiles for an application.
func (b *InMemoryBackend) ListConfigurationProfiles(applicationID string) ([]ConfigurationProfile, error) {
	b.mu.RLock("ListConfigurationProfiles")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	profiles := b.configProfiles[applicationID]
	out := make([]ConfigurationProfile, 0, len(profiles))

	for _, p := range profiles {
		out = append(out, *p)
	}

	return out, nil
}

// UpdateConfigurationProfile updates a configuration profile.
func (b *InMemoryBackend) UpdateConfigurationProfile(
	applicationID, profileID, name, description string,
) (*ConfigurationProfile, error) {
	b.mu.Lock("UpdateConfigurationProfile")
	defer b.mu.Unlock()

	profiles, ok := b.configProfiles[applicationID]
	if !ok {
		return nil, fmt.Errorf("%w: configuration profile %s", ErrConfigurationProfileNotFound, profileID)
	}

	profile, ok := profiles[profileID]
	if !ok {
		return nil, fmt.Errorf("%w: configuration profile %s", ErrConfigurationProfileNotFound, profileID)
	}

	if name != "" {
		profile.Name = name
	}

	profile.Description = description
	cp := *profile

	return &cp, nil
}

// DeleteConfigurationProfile deletes a configuration profile.
func (b *InMemoryBackend) DeleteConfigurationProfile(applicationID, profileID string) error {
	b.mu.Lock("DeleteConfigurationProfile")
	defer b.mu.Unlock()

	profiles, ok := b.configProfiles[applicationID]
	if !ok {
		return fmt.Errorf("%w: configuration profile %s", ErrConfigurationProfileNotFound, profileID)
	}

	if _, exists := profiles[profileID]; !exists {
		return fmt.Errorf("%w: configuration profile %s", ErrConfigurationProfileNotFound, profileID)
	}

	delete(profiles, profileID)

	return nil
}

// CreateHostedConfigurationVersion creates a hosted configuration version.
func (b *InMemoryBackend) CreateHostedConfigurationVersion(
	applicationID, profileID, contentType string,
	content []byte,
) (*HostedConfigurationVersion, error) {
	b.mu.Lock("CreateHostedConfigurationVersion")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	profiles, ok := b.configProfiles[applicationID]
	if !ok || profiles[profileID] == nil {
		return nil, fmt.Errorf("%w: configuration profile %s", ErrConfigurationProfileNotFound, profileID)
	}

	if b.hostedConfigVersions[applicationID] == nil {
		b.hostedConfigVersions[applicationID] = make(map[string]map[int32]*HostedConfigurationVersion)
	}

	if b.hostedConfigVersions[applicationID][profileID] == nil {
		b.hostedConfigVersions[applicationID][profileID] = make(map[int32]*HostedConfigurationVersion)
	}

	if b.versionCounters[applicationID] == nil {
		b.versionCounters[applicationID] = make(map[string]int32)
	}

	b.versionCounters[applicationID][profileID]++
	versionNumber := b.versionCounters[applicationID][profileID]

	v := &HostedConfigurationVersion{
		ApplicationID:          applicationID,
		ConfigurationProfileID: profileID,
		ContentType:            contentType,
		Content:                content,
		VersionNumber:          versionNumber,
		CreatedAt:              time.Now(),
	}
	b.hostedConfigVersions[applicationID][profileID][versionNumber] = v
	cp := *v

	return &cp, nil
}

// GetHostedConfigurationVersion retrieves a hosted configuration version.
func (b *InMemoryBackend) GetHostedConfigurationVersion(
	applicationID, profileID string,
	versionNumber int32,
) (*HostedConfigurationVersion, error) {
	b.mu.RLock("GetHostedConfigurationVersion")
	defer b.mu.RUnlock()

	versions, ok := b.hostedConfigVersions[applicationID]
	if !ok {
		return nil, fmt.Errorf("%w: version %d", ErrHostedConfigVersionNotFound, versionNumber)
	}

	profileVersions, ok := versions[profileID]
	if !ok {
		return nil, fmt.Errorf("%w: version %d", ErrHostedConfigVersionNotFound, versionNumber)
	}

	v, ok := profileVersions[versionNumber]
	if !ok {
		return nil, fmt.Errorf("%w: version %d", ErrHostedConfigVersionNotFound, versionNumber)
	}

	cp := *v

	return &cp, nil
}

// ListHostedConfigurationVersions returns all versions for a profile.
func (b *InMemoryBackend) ListHostedConfigurationVersions(
	applicationID, profileID string,
) ([]HostedConfigurationVersion, error) {
	b.mu.RLock("ListHostedConfigurationVersions")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	appProfiles, hasProfiles := b.configProfiles[applicationID]
	if !hasProfiles {
		return nil, fmt.Errorf("%w: configuration profile %s", ErrConfigurationProfileNotFound, profileID)
	}

	if _, ok := appProfiles[profileID]; !ok {
		return nil, fmt.Errorf("%w: configuration profile %s", ErrConfigurationProfileNotFound, profileID)
	}

	versions := b.hostedConfigVersions[applicationID]
	profileVersions := versions[profileID]

	out := make([]HostedConfigurationVersion, 0, len(profileVersions))
	for _, v := range profileVersions {
		out = append(out, *v)
	}

	return out, nil
}

// DeleteHostedConfigurationVersion deletes a hosted configuration version.
func (b *InMemoryBackend) DeleteHostedConfigurationVersion(applicationID, profileID string, versionNumber int32) error {
	b.mu.Lock("DeleteHostedConfigurationVersion")
	defer b.mu.Unlock()

	versions, ok := b.hostedConfigVersions[applicationID]
	if !ok {
		return fmt.Errorf("%w: version %d", ErrHostedConfigVersionNotFound, versionNumber)
	}

	profileVersions, ok := versions[profileID]
	if !ok {
		return fmt.Errorf("%w: version %d", ErrHostedConfigVersionNotFound, versionNumber)
	}

	if _, exists := profileVersions[versionNumber]; !exists {
		return fmt.Errorf("%w: version %d", ErrHostedConfigVersionNotFound, versionNumber)
	}

	delete(profileVersions, versionNumber)

	return nil
}

// CreateDeploymentStrategy creates a new deployment strategy.
func (b *InMemoryBackend) CreateDeploymentStrategy(
	name, description string,
	deploymentDuration, bakeTime int32,
	growthFactor float32,
	growthType, replicateTo string,
) (*DeploymentStrategy, error) {
	b.mu.Lock("CreateDeploymentStrategy")
	defer b.mu.Unlock()

	now := time.Now()
	strategy := &DeploymentStrategy{
		ID:                          newResourceID(),
		Name:                        name,
		Description:                 description,
		DeploymentDurationInMinutes: deploymentDuration,
		FinalBakeTimeInMinutes:      bakeTime,
		GrowthFactor:                growthFactor,
		GrowthType:                  growthType,
		ReplicateTo:                 replicateTo,
		CreatedAt:                   now,
		UpdatedAt:                   now,
	}
	b.deploymentStrategies[strategy.ID] = strategy
	cp := *strategy

	return &cp, nil
}

// GetDeploymentStrategy retrieves a deployment strategy by ID.
func (b *InMemoryBackend) GetDeploymentStrategy(strategyID string) (*DeploymentStrategy, error) {
	b.mu.RLock("GetDeploymentStrategy")
	defer b.mu.RUnlock()

	strategy, ok := b.deploymentStrategies[strategyID]
	if !ok {
		return nil, fmt.Errorf("%w: deployment strategy %s", ErrDeploymentStrategyNotFound, strategyID)
	}

	cp := *strategy

	return &cp, nil
}

// ListDeploymentStrategies returns all deployment strategies.
func (b *InMemoryBackend) ListDeploymentStrategies() []DeploymentStrategy {
	b.mu.RLock("ListDeploymentStrategies")
	defer b.mu.RUnlock()

	out := make([]DeploymentStrategy, 0, len(b.deploymentStrategies))
	for _, s := range b.deploymentStrategies {
		out = append(out, *s)
	}

	return out
}

// UpdateDeploymentStrategy updates a deployment strategy.
func (b *InMemoryBackend) UpdateDeploymentStrategy(
	strategyID, name, description string,
	deploymentDuration, bakeTime int32,
	growthFactor float32,
) (*DeploymentStrategy, error) {
	b.mu.Lock("UpdateDeploymentStrategy")
	defer b.mu.Unlock()

	strategy, ok := b.deploymentStrategies[strategyID]
	if !ok {
		return nil, fmt.Errorf("%w: deployment strategy %s", ErrDeploymentStrategyNotFound, strategyID)
	}

	if name != "" {
		strategy.Name = name
	}

	strategy.Description = description
	strategy.DeploymentDurationInMinutes = deploymentDuration
	strategy.FinalBakeTimeInMinutes = bakeTime
	strategy.GrowthFactor = growthFactor
	strategy.UpdatedAt = time.Now()
	cp := *strategy

	return &cp, nil
}

// DeleteDeploymentStrategy deletes a deployment strategy.
func (b *InMemoryBackend) DeleteDeploymentStrategy(strategyID string) error {
	b.mu.Lock("DeleteDeploymentStrategy")
	defer b.mu.Unlock()

	if _, ok := b.deploymentStrategies[strategyID]; !ok {
		return fmt.Errorf("%w: deployment strategy %s", ErrDeploymentStrategyNotFound, strategyID)
	}

	delete(b.deploymentStrategies, strategyID)

	return nil
}

// StartDeployment starts a deployment.
func (b *InMemoryBackend) StartDeployment(
	applicationID, environmentID, configProfileID, strategyID, configVersion string,
) (*Deployment, error) {
	b.mu.Lock("StartDeployment")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	if b.deployments[applicationID] == nil {
		b.deployments[applicationID] = make(map[string]map[int32]*Deployment)
	}

	if b.deployments[applicationID][environmentID] == nil {
		b.deployments[applicationID][environmentID] = make(map[int32]*Deployment)
	}

	if b.deploymentCounters[applicationID] == nil {
		b.deploymentCounters[applicationID] = make(map[string]int32)
	}

	b.deploymentCounters[applicationID][environmentID]++
	deploymentNumber := b.deploymentCounters[applicationID][environmentID]

	now := time.Now()
	deployment := &Deployment{
		ApplicationID:          applicationID,
		EnvironmentID:          environmentID,
		ConfigurationProfileID: configProfileID,
		DeploymentStrategyID:   strategyID,
		ConfigurationVersion:   configVersion,
		State:                  "COMPLETE",
		DeploymentNumber:       deploymentNumber,
		StartedAt:              now,
		CompletedAt:            now,
	}
	b.deployments[applicationID][environmentID][deploymentNumber] = deployment
	cp := *deployment

	return &cp, nil
}

// GetDeployment retrieves a deployment.
func (b *InMemoryBackend) GetDeployment(
	applicationID, environmentID string,
	deploymentNumber int32,
) (*Deployment, error) {
	b.mu.RLock("GetDeployment")
	defer b.mu.RUnlock()

	envDeployments, ok := b.deployments[applicationID]
	if !ok {
		return nil, fmt.Errorf("%w: deployment %d", ErrDeploymentNotFound, deploymentNumber)
	}

	deploys, ok := envDeployments[environmentID]
	if !ok {
		return nil, fmt.Errorf("%w: deployment %d", ErrDeploymentNotFound, deploymentNumber)
	}

	d, ok := deploys[deploymentNumber]
	if !ok {
		return nil, fmt.Errorf("%w: deployment %d", ErrDeploymentNotFound, deploymentNumber)
	}

	cp := *d

	return &cp, nil
}

// ListDeployments returns all deployments for an environment.
func (b *InMemoryBackend) ListDeployments(applicationID, environmentID string) ([]Deployment, error) {
	b.mu.RLock("ListDeployments")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	appEnvs, hasEnvs := b.environments[applicationID]
	if !hasEnvs {
		return nil, fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	if _, ok := appEnvs[environmentID]; !ok {
		return nil, fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	envDeployments := b.deployments[applicationID]
	deploys := envDeployments[environmentID]

	out := make([]Deployment, 0, len(deploys))
	for _, d := range deploys {
		out = append(out, *d)
	}

	return out, nil
}

// StopDeployment stops an in-progress deployment.
func (b *InMemoryBackend) StopDeployment(applicationID, environmentID string, deploymentNumber int32) error {
	b.mu.Lock("StopDeployment")
	defer b.mu.Unlock()

	envDeployments, ok := b.deployments[applicationID]
	if !ok {
		return fmt.Errorf("%w: deployment %d", ErrDeploymentNotFound, deploymentNumber)
	}

	deploys, ok := envDeployments[environmentID]
	if !ok {
		return fmt.Errorf("%w: deployment %d", ErrDeploymentNotFound, deploymentNumber)
	}

	d, ok := deploys[deploymentNumber]
	if !ok {
		return fmt.Errorf("%w: deployment %d", ErrDeploymentNotFound, deploymentNumber)
	}

	d.State = "ROLLEDBACK"
	d.CompletedAt = time.Now()

	return nil
}

// ListTagsForResource returns the tags for the given resource ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t := b.tags[resourceArn]
	result := make(map[string]string, len(t))
	maps.Copy(result, t)

	return result, nil
}

// TagResource adds or replaces tags on the given resource ARN.
func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceArn], tags)

	return nil
}

// UntagResource removes the specified tag keys from the given resource ARN.
func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	for _, k := range tagKeys {
		delete(b.tags[resourceArn], k)
	}

	return nil
}
