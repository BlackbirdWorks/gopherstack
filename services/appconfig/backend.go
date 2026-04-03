package appconfig

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"maps"
	"sort"
	"strconv"
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
	applications          map[string]*Application
	environments          map[string]map[string]*Environment
	configProfiles        map[string]map[string]*ConfigurationProfile
	hostedConfigVersions  map[string]map[string]map[int32]*HostedConfigurationVersion
	deploymentStrategies  map[string]*DeploymentStrategy
	deployments           map[string]map[string]map[int32]*Deployment
	extensions            map[string]*Extension
	extensionAssociations map[string]*ExtensionAssociation
	tags                  map[string]map[string]string
	accountSettings       AccountSettings
	versionCounters       map[string]map[string]int32
	deploymentCounters    map[string]map[string]int32
	mu                    *lockmetrics.RWMutex
	accountID             string
	region                string
}

// NewInMemoryBackend creates a new InMemoryBackend for AppConfig.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		applications:          make(map[string]*Application),
		environments:          make(map[string]map[string]*Environment),
		configProfiles:        make(map[string]map[string]*ConfigurationProfile),
		hostedConfigVersions:  make(map[string]map[string]map[int32]*HostedConfigurationVersion),
		deploymentStrategies:  make(map[string]*DeploymentStrategy),
		deployments:           make(map[string]map[string]map[int32]*Deployment),
		extensions:            make(map[string]*Extension),
		extensionAssociations: make(map[string]*ExtensionAssociation),
		tags:                  make(map[string]map[string]string),
		versionCounters:       make(map[string]map[string]int32),
		deploymentCounters:    make(map[string]map[string]int32),
		mu:                    lockmetrics.New("appconfig"),
		accountID:             accountID,
		region:                region,
	}
}

// appconfigARN builds an AppConfig resource ARN for tag lookup/cleanup.
func (b *InMemoryBackend) appconfigARN(resourcePath string) string {
	return fmt.Sprintf("arn:aws:appconfig:%s:%s:%s", b.region, b.accountID, resourcePath)
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

// ListApplications returns paginated applications.
func (b *InMemoryBackend) ListApplications(nextToken string, maxResults int) ([]Application, string) {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	out := make([]Application, 0, len(b.applications))
	for _, app := range b.applications {
		out = append(out, *app)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, maxResults)

	return page, token
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

	// Clean up tags for the application and all its child resources.
	delete(b.tags, b.appconfigARN("application/"+applicationID))

	for envID := range b.environments[applicationID] {
		delete(b.tags, b.appconfigARN("application/"+applicationID+"/environment/"+envID))
	}

	for profileID := range b.configProfiles[applicationID] {
		delete(b.tags, b.appconfigARN("application/"+applicationID+"/configurationprofile/"+profileID))
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

// ListEnvironments returns paginated environments for an application.
func (b *InMemoryBackend) ListEnvironments(
	applicationID, nextToken string,
	maxResults int,
) ([]Environment, string, error) {
	b.mu.RLock("ListEnvironments")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, "", fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	envs := b.environments[applicationID]
	out := make([]Environment, 0, len(envs))

	for _, e := range envs {
		out = append(out, *e)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, maxResults)

	return page, token, nil
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
	delete(b.tags, b.appconfigARN("application/"+applicationID+"/environment/"+environmentID))

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

// ListConfigurationProfiles returns paginated profiles for an application.
func (b *InMemoryBackend) ListConfigurationProfiles(
	applicationID, nextToken string,
	maxResults int,
) ([]ConfigurationProfile, string, error) {
	b.mu.RLock("ListConfigurationProfiles")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, "", fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	profiles := b.configProfiles[applicationID]
	out := make([]ConfigurationProfile, 0, len(profiles))

	for _, p := range profiles {
		out = append(out, *p)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, maxResults)

	return page, token, nil
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
	delete(b.tags, b.appconfigARN("application/"+applicationID+"/configurationprofile/"+profileID))

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

// ListHostedConfigurationVersions returns paginated versions for a profile.
func (b *InMemoryBackend) ListHostedConfigurationVersions(
	applicationID, profileID, nextToken string, maxResults int,
) ([]HostedConfigurationVersion, string, error) {
	b.mu.RLock("ListHostedConfigurationVersions")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, "", fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	if _, ok := b.configProfiles[applicationID][profileID]; !ok {
		return nil, "", fmt.Errorf("%w: configuration profile %s", ErrConfigurationProfileNotFound, profileID)
	}

	profileVersions := b.hostedConfigVersions[applicationID][profileID]

	out := make([]HostedConfigurationVersion, 0, len(profileVersions))
	for _, v := range profileVersions {
		out = append(out, *v)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].VersionNumber < out[j].VersionNumber })

	page, token := appConfigPaginate(out, nextToken, maxResults)

	return page, token, nil
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

// ListDeploymentStrategies returns paginated deployment strategies.
func (b *InMemoryBackend) ListDeploymentStrategies(nextToken string, maxResults int) ([]DeploymentStrategy, string) {
	b.mu.RLock("ListDeploymentStrategies")
	defer b.mu.RUnlock()

	out := make([]DeploymentStrategy, 0, len(b.deploymentStrategies))
	for _, s := range b.deploymentStrategies {
		out = append(out, *s)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, maxResults)

	return page, token
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
	delete(b.tags, b.appconfigARN("deploymentstrategy/"+strategyID))

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

// ListDeployments returns paginated deployments for an environment.
func (b *InMemoryBackend) ListDeployments(
	applicationID, environmentID, nextToken string,
	maxResults int,
) ([]Deployment, string, error) {
	b.mu.RLock("ListDeployments")
	defer b.mu.RUnlock()

	// Single lookup — returns a clear error for app-not-found or env-not-found.
	if _, ok := b.environments[applicationID][environmentID]; !ok {
		if _, appOk := b.applications[applicationID]; !appOk {
			return nil, "", fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
		}

		return nil, "", fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	deploys := b.deployments[applicationID][environmentID]

	out := make([]Deployment, 0, len(deploys))
	for _, d := range deploys {
		out = append(out, *d)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].DeploymentNumber < out[j].DeploymentNumber })

	page, token := appConfigPaginate(out, nextToken, maxResults)

	return page, token, nil
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

	t := b.tags[resourceArn]
	if t == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(t, k)
	}

	return nil
}

// CreateExtension creates a new AppConfig extension.
func (b *InMemoryBackend) CreateExtension(
	name, description string,
	actions map[string][]ExtensionAction,
	parameters map[string]ExtensionParameter,
) (*Extension, error) {
	b.mu.Lock("CreateExtension")
	defer b.mu.Unlock()

	id := newResourceID()
	ext := &Extension{
		ID:            id,
		Name:          name,
		Description:   description,
		Arn:           b.appconfigARN("extension/" + id),
		VersionNumber: 1,
		Actions:       actions,
		Parameters:    parameters,
	}
	b.extensions[ext.ID] = ext
	cp := *ext

	return &cp, nil
}

// resolveExtension finds an extension by ID or name.
func (b *InMemoryBackend) resolveExtension(identifier string) *Extension {
	if ext, ok := b.extensions[identifier]; ok {
		return ext
	}

	for _, ext := range b.extensions {
		if ext.Name == identifier {
			return ext
		}
	}

	return nil
}

// GetExtension retrieves an extension by identifier (ID or name).
func (b *InMemoryBackend) GetExtension(extensionIdentifier string) (*Extension, error) {
	b.mu.RLock("GetExtension")
	defer b.mu.RUnlock()

	ext := b.resolveExtension(extensionIdentifier)
	if ext == nil {
		return nil, fmt.Errorf("%w: extension %s", ErrExtensionNotFound, extensionIdentifier)
	}

	cp := *ext

	return &cp, nil
}

// ListExtensions returns paginated extensions.
func (b *InMemoryBackend) ListExtensions(nextToken string, maxResults int) ([]Extension, string) {
	b.mu.RLock("ListExtensions")
	defer b.mu.RUnlock()

	out := make([]Extension, 0, len(b.extensions))
	for _, ext := range b.extensions {
		out = append(out, *ext)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, maxResults)

	return page, token
}

// DeleteExtension deletes an extension by identifier (ID or name).
func (b *InMemoryBackend) DeleteExtension(extensionIdentifier string) error {
	b.mu.Lock("DeleteExtension")
	defer b.mu.Unlock()

	ext := b.resolveExtension(extensionIdentifier)
	if ext == nil {
		return fmt.Errorf("%w: extension %s", ErrExtensionNotFound, extensionIdentifier)
	}

	delete(b.extensions, ext.ID)
	delete(b.tags, ext.Arn)

	return nil
}

// CreateExtensionAssociation creates an association between an extension and a resource.
func (b *InMemoryBackend) CreateExtensionAssociation(
	extensionIdentifier, resourceIdentifier string,
	parameters map[string]string,
	extensionVersionNumber *int32,
) (*ExtensionAssociation, error) {
	b.mu.Lock("CreateExtensionAssociation")
	defer b.mu.Unlock()

	ext := b.resolveExtension(extensionIdentifier)
	if ext == nil {
		return nil, fmt.Errorf("%w: extension %s", ErrExtensionNotFound, extensionIdentifier)
	}

	versionNum := ext.VersionNumber
	if extensionVersionNumber != nil {
		versionNum = *extensionVersionNumber
	}

	id := newResourceID()
	assoc := &ExtensionAssociation{
		ID:                     id,
		Arn:                    b.appconfigARN("extensionassociation/" + id),
		ExtensionArn:           ext.Arn,
		ResourceArn:            resourceIdentifier,
		ExtensionVersionNumber: versionNum,
		Parameters:             parameters,
	}
	b.extensionAssociations[assoc.ID] = assoc
	cp := *assoc

	return &cp, nil
}

// GetExtensionAssociation retrieves an extension association by ID.
func (b *InMemoryBackend) GetExtensionAssociation(extensionAssociationID string) (*ExtensionAssociation, error) {
	b.mu.RLock("GetExtensionAssociation")
	defer b.mu.RUnlock()

	assoc, ok := b.extensionAssociations[extensionAssociationID]
	if !ok {
		return nil, fmt.Errorf("%w: extension association %s", ErrExtensionAssociationNotFound, extensionAssociationID)
	}

	cp := *assoc

	return &cp, nil
}

// ListExtensionAssociations returns paginated extension associations.
func (b *InMemoryBackend) ListExtensionAssociations(nextToken string, maxResults int) ([]ExtensionAssociation, string) {
	b.mu.RLock("ListExtensionAssociations")
	defer b.mu.RUnlock()

	out := make([]ExtensionAssociation, 0, len(b.extensionAssociations))
	for _, a := range b.extensionAssociations {
		out = append(out, *a)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, maxResults)

	return page, token
}

// DeleteExtensionAssociation deletes an extension association by ID.
func (b *InMemoryBackend) DeleteExtensionAssociation(extensionAssociationID string) error {
	b.mu.Lock("DeleteExtensionAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.extensionAssociations[extensionAssociationID]
	if !ok {
		return fmt.Errorf("%w: extension association %s", ErrExtensionAssociationNotFound, extensionAssociationID)
	}

	delete(b.extensionAssociations, extensionAssociationID)
	delete(b.tags, assoc.Arn)

	return nil
}

// GetAccountSettings returns the account-level AppConfig settings.
func (b *InMemoryBackend) GetAccountSettings() (*AccountSettings, error) {
	b.mu.RLock("GetAccountSettings")
	defer b.mu.RUnlock()

	cp := b.accountSettings

	return &cp, nil
}

// GetConfiguration retrieves the latest deployed configuration for the given application,
// environment, and configuration profile (deprecated API).
func (b *InMemoryBackend) GetConfiguration(
	application, environment, configuration string,
) (*HostedConfigurationVersion, error) {
	b.mu.RLock("GetConfiguration")
	defer b.mu.RUnlock()

	appID, err := b.resolveAppID(application)
	if err != nil {
		return nil, err
	}

	if _, err = b.resolveEnvID(appID, environment); err != nil {
		return nil, err
	}

	profileID, err := b.resolveProfileID(appID, configuration)
	if err != nil {
		return nil, err
	}

	return b.latestConfigVersion(appID, profileID), nil
}

// resolveAppID finds an application ID by ID or name. Must be called under lock.
func (b *InMemoryBackend) resolveAppID(identifier string) (string, error) {
	for id, app := range b.applications {
		if id == identifier || app.Name == identifier {
			return id, nil
		}
	}

	return "", fmt.Errorf("%w: application %s", ErrApplicationNotFound, identifier)
}

// resolveEnvID finds an environment ID by ID or name within an application. Must be called under lock.
func (b *InMemoryBackend) resolveEnvID(appID, identifier string) (string, error) {
	for id, env := range b.environments[appID] {
		if id == identifier || env.Name == identifier {
			return id, nil
		}
	}

	return "", fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, identifier)
}

// resolveProfileID finds a configuration profile ID by ID or name. Must be called under lock.
func (b *InMemoryBackend) resolveProfileID(appID, identifier string) (string, error) {
	for id, profile := range b.configProfiles[appID] {
		if id == identifier || profile.Name == identifier {
			return id, nil
		}
	}

	return "", fmt.Errorf("%w: configuration profile %s", ErrConfigurationProfileNotFound, identifier)
}

// latestConfigVersion returns the latest hosted configuration version for a profile. Must be called under lock.
func (b *InMemoryBackend) latestConfigVersion(appID, profileID string) *HostedConfigurationVersion {
	profileVersions := b.hostedConfigVersions[appID][profileID]
	if len(profileVersions) == 0 {
		return &HostedConfigurationVersion{
			ApplicationID:          appID,
			ConfigurationProfileID: profileID,
			ContentType:            "application/octet-stream",
			Content:                []byte{},
		}
	}

	var latest *HostedConfigurationVersion

	for _, v := range profileVersions {
		if latest == nil || v.VersionNumber > latest.VersionNumber {
			vCopy := *v
			latest = &vCopy
		}
	}

	return latest
}

// appConfigPaginate applies token-based pagination to a sorted slice.
func appConfigPaginate[T any](all []T, nextToken string, maxResults int) ([]T, string) {
	const defaultLimit = 50

	startIdx := 0
	if nextToken != "" {
		if idx, err := strconv.Atoi(nextToken); err == nil && idx >= 0 {
			startIdx = idx
		}
	}

	if startIdx >= len(all) {
		return []T{}, ""
	}

	limit := defaultLimit
	if maxResults > 0 {
		limit = maxResults
	}

	end := startIdx + limit

	var outToken string
	if end < len(all) {
		outToken = strconv.Itoa(end)
	} else {
		end = len(all)
	}

	return all[startIdx:end], outToken
}
