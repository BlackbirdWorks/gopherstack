package appconfig

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	contentTypeOctetStream = "application/octet-stream"
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
	// Name-to-ID indexes for O(1) uniqueness checks and name-based resolution.
	applicationsByName         map[string]string            // name → ID
	environmentsByName         map[string]map[string]string // appID → name → envID
	configProfilesByName       map[string]map[string]string // appID → name → profileID
	deploymentStrategiesByName map[string]string            // name → ID
	extensionsByName           map[string]string            // name → ID
	// versionLabelIndex enables O(1) label uniqueness checks for hosted config versions.
	versionLabelIndex map[string]map[string]map[string]struct{} // appID → profileID → label → {}
	mu                *lockmetrics.RWMutex
	paginationSecret  string
	accountID         string
	region            string
}

// NewInMemoryBackend creates a new InMemoryBackend for AppConfig.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		applications:               make(map[string]*Application),
		environments:               make(map[string]map[string]*Environment),
		configProfiles:             make(map[string]map[string]*ConfigurationProfile),
		hostedConfigVersions:       make(map[string]map[string]map[int32]*HostedConfigurationVersion),
		deploymentStrategies:       make(map[string]*DeploymentStrategy),
		deployments:                make(map[string]map[string]map[int32]*Deployment),
		extensions:                 make(map[string]*Extension),
		extensionAssociations:      make(map[string]*ExtensionAssociation),
		tags:                       make(map[string]map[string]string),
		versionCounters:            make(map[string]map[string]int32),
		deploymentCounters:         make(map[string]map[string]int32),
		applicationsByName:         make(map[string]string),
		environmentsByName:         make(map[string]map[string]string),
		configProfilesByName:       make(map[string]map[string]string),
		deploymentStrategiesByName: make(map[string]string),
		extensionsByName:           make(map[string]string),
		versionLabelIndex:          make(map[string]map[string]map[string]struct{}),
		mu:                         lockmetrics.New("appconfig"),
		paginationSecret:           uuid.NewString(),
		accountID:                  accountID,
		region:                     region,
	}
}

// PaginationSecret returns the HMAC secret for pagination tokens.
func (b *InMemoryBackend) PaginationSecret() string { return b.paginationSecret }

// appconfigARN builds an AppConfig resource ARN for tag lookup/cleanup.
func (b *InMemoryBackend) appconfigARN(resourcePath string) string {
	return arn.Build("appconfig", b.region, b.accountID, resourcePath)
}

// CreateApplication creates a new AppConfig application.
func (b *InMemoryBackend) CreateApplication(name, description string) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrBadRequest)
	}

	if _, exists := b.applicationsByName[name]; exists {
		return nil, fmt.Errorf("%w: application with name %q already exists", ErrConflict, name)
	}

	now := time.Now()
	app := &Application{
		ID:          newResourceID(),
		Name:        name,
		Description: description,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	b.applications[app.ID] = app
	b.applicationsByName[name] = app.ID
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
func (b *InMemoryBackend) ListApplications(
	nextToken string,
	maxResults int,
) ([]Application, string) {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	out := make([]Application, 0, len(b.applications))
	for _, app := range b.applications {
		out = append(out, *app)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token
}

// UpdateApplication updates an application's name and description.
func (b *InMemoryBackend) UpdateApplication(
	applicationID, name, description string,
) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[applicationID]
	if !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	if name != "" && name != app.Name {
		delete(b.applicationsByName, app.Name)
		b.applicationsByName[name] = applicationID
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

	for envID, env := range b.environments[applicationID] {
		delete(b.tags, b.appconfigARN("application/"+applicationID+"/environment/"+envID))
		if envsByName, ok := b.environmentsByName[applicationID]; ok {
			delete(envsByName, env.Name)
		}
	}

	for profileID, profile := range b.configProfiles[applicationID] {
		delete(
			b.tags,
			b.appconfigARN("application/"+applicationID+"/configurationprofile/"+profileID),
		)
		if profilesByName, ok := b.configProfilesByName[applicationID]; ok {
			delete(profilesByName, profile.Name)
		}
	}

	if app, ok := b.applications[applicationID]; ok {
		delete(b.applicationsByName, app.Name)
	}

	delete(b.applications, applicationID)
	delete(b.environments, applicationID)
	delete(b.environmentsByName, applicationID)
	delete(b.configProfiles, applicationID)
	delete(b.configProfilesByName, applicationID)
	delete(b.hostedConfigVersions, applicationID)
	delete(b.versionLabelIndex, applicationID)
	delete(b.deployments, applicationID)
	delete(b.versionCounters, applicationID)
	delete(b.deploymentCounters, applicationID)

	return nil
}

// CreateEnvironment creates a new environment within an application.
func (b *InMemoryBackend) CreateEnvironment(
	applicationID, name, description string,
	monitors []Monitor,
) (*Environment, error) {
	b.mu.Lock("CreateEnvironment")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrBadRequest)
	}

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	if b.environments[applicationID] == nil {
		b.environments[applicationID] = make(map[string]*Environment)
	}

	if b.environmentsByName[applicationID] == nil {
		b.environmentsByName[applicationID] = make(map[string]string)
	}

	if _, exists := b.environmentsByName[applicationID][name]; exists {
		return nil, fmt.Errorf(
			"%w: environment with name %q already exists in application %s",
			ErrConflict,
			name,
			applicationID,
		)
	}

	now := time.Now()
	env := &Environment{
		ID:            newResourceID(),
		ApplicationID: applicationID,
		Name:          name,
		Description:   description,
		State:         "READY_FOR_DEPLOYMENT",
		Monitors:      monitors,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	b.environments[applicationID][env.ID] = env
	b.environmentsByName[applicationID][name] = env.ID
	cp := *env

	return &cp, nil
}

// GetEnvironment retrieves an environment by application and environment ID.
func (b *InMemoryBackend) GetEnvironment(
	applicationID, environmentID string,
) (*Environment, error) {
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

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

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

	if name != "" && name != env.Name {
		if envsByName := b.environmentsByName[applicationID]; envsByName != nil {
			delete(envsByName, env.Name)
			envsByName[name] = environmentID
		}
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

	env, exists := envs[environmentID]
	if !exists {
		return fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	if envsByName := b.environmentsByName[applicationID]; envsByName != nil {
		delete(envsByName, env.Name)
	}

	delete(envs, environmentID)
	delete(b.tags, b.appconfigARN("application/"+applicationID+"/environment/"+environmentID))

	return nil
}

// CreateConfigurationProfile creates a new configuration profile.
func (b *InMemoryBackend) CreateConfigurationProfile(
	applicationID, name, description, locationURI, profileType, retrievalRoleArn string,
	validators []Validator,
) (*ConfigurationProfile, error) {
	b.mu.Lock("CreateConfigurationProfile")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrBadRequest)
	}

	if locationURI == "" {
		return nil, fmt.Errorf("%w: LocationUri is required", ErrBadRequest)
	}

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	if b.configProfiles[applicationID] == nil {
		b.configProfiles[applicationID] = make(map[string]*ConfigurationProfile)
	}

	if b.configProfilesByName[applicationID] == nil {
		b.configProfilesByName[applicationID] = make(map[string]string)
	}

	if _, exists := b.configProfilesByName[applicationID][name]; exists {
		return nil, fmt.Errorf(
			"%w: configuration profile with name %q already exists in application %s",
			ErrConflict,
			name,
			applicationID,
		)
	}

	profile := &ConfigurationProfile{
		ID:               newResourceID(),
		ApplicationID:    applicationID,
		Name:             name,
		Description:      description,
		LocationURI:      locationURI,
		Type:             profileType,
		RetrievalRoleArn: retrievalRoleArn,
		Validators:       validators,
	}
	b.configProfiles[applicationID][profile.ID] = profile
	b.configProfilesByName[applicationID][name] = profile.ID
	cp := *profile

	return &cp, nil
}

// GetConfigurationProfile retrieves a configuration profile.
func (b *InMemoryBackend) GetConfigurationProfile(
	applicationID, profileID string,
) (*ConfigurationProfile, error) {
	b.mu.RLock("GetConfigurationProfile")
	defer b.mu.RUnlock()

	profiles, ok := b.configProfiles[applicationID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	profile, ok := profiles[profileID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
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

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

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
		return nil, fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	profile, ok := profiles[profileID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	if name != "" && name != profile.Name {
		if profilesByName := b.configProfilesByName[applicationID]; profilesByName != nil {
			delete(profilesByName, profile.Name)
			profilesByName[name] = profileID
		}
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
		return fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	profile, exists := profiles[profileID]
	if !exists {
		return fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	if profilesByName := b.configProfilesByName[applicationID]; profilesByName != nil {
		delete(profilesByName, profile.Name)
	}

	delete(profiles, profileID)
	delete(b.tags, b.appconfigARN("application/"+applicationID+"/configurationprofile/"+profileID))

	return nil
}

const maxHostedConfigSizeBytes = 1024 * 1024 // 1 MiB, matching AWS limit

// CreateHostedConfigurationVersion creates a hosted configuration version.
func (b *InMemoryBackend) CreateHostedConfigurationVersion(
	applicationID, profileID, contentType, description, versionLabel string,
	content []byte,
) (*HostedConfigurationVersion, error) {
	b.mu.Lock("CreateHostedConfigurationVersion")
	defer b.mu.Unlock()

	if len(content) > maxHostedConfigSizeBytes {
		return nil, fmt.Errorf(
			"%w: content exceeds maximum size of %d bytes",
			ErrPayloadTooLarge,
			maxHostedConfigSizeBytes,
		)
	}

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	profiles, ok := b.configProfiles[applicationID]
	if !ok || profiles[profileID] == nil {
		return nil, fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	if b.hostedConfigVersions[applicationID] == nil {
		b.hostedConfigVersions[applicationID] = make(
			map[string]map[int32]*HostedConfigurationVersion,
		)
	}

	if b.hostedConfigVersions[applicationID][profileID] == nil {
		b.hostedConfigVersions[applicationID][profileID] = make(
			map[int32]*HostedConfigurationVersion,
		)
	}

	if b.versionCounters[applicationID] == nil {
		b.versionCounters[applicationID] = make(map[string]int32)
	}

	// VersionLabel must be unique across versions for this profile.
	if versionLabel != "" {
		if labels := b.versionLabelIndex[applicationID][profileID]; labels != nil {
			if _, exists := labels[versionLabel]; exists {
				return nil, fmt.Errorf(
					"%w: version label %q already exists for profile %s",
					ErrConflict,
					versionLabel,
					profileID,
				)
			}
		}
	}

	b.versionCounters[applicationID][profileID]++
	versionNumber := b.versionCounters[applicationID][profileID]

	v := &HostedConfigurationVersion{
		ApplicationID:          applicationID,
		ConfigurationProfileID: profileID,
		ContentType:            contentType,
		Description:            description,
		VersionLabel:           versionLabel,
		Content:                content,
		VersionNumber:          versionNumber,
		CreatedAt:              time.Now(),
	}
	b.hostedConfigVersions[applicationID][profileID][versionNumber] = v

	if versionLabel != "" {
		if b.versionLabelIndex[applicationID] == nil {
			b.versionLabelIndex[applicationID] = make(map[string]map[string]struct{})
		}
		if b.versionLabelIndex[applicationID][profileID] == nil {
			b.versionLabelIndex[applicationID][profileID] = make(map[string]struct{})
		}
		b.versionLabelIndex[applicationID][profileID][versionLabel] = struct{}{}
	}

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

// ListHostedConfigurationVersions returns paginated versions for a profile, optionally filtered by versionLabel.
func (b *InMemoryBackend) ListHostedConfigurationVersions(
	applicationID, profileID, nextToken, versionLabel string, maxResults int,
) ([]HostedConfigurationVersion, string, error) {
	b.mu.RLock("ListHostedConfigurationVersions")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, "", fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	if _, ok := b.configProfiles[applicationID][profileID]; !ok {
		return nil, "", fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	profileVersions := b.hostedConfigVersions[applicationID][profileID]

	out := make([]HostedConfigurationVersion, 0, len(profileVersions))
	for _, v := range profileVersions {
		if versionLabel != "" && v.VersionLabel != versionLabel {
			continue
		}

		out = append(out, *v)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].VersionNumber < out[j].VersionNumber })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token, nil
}

// DeleteHostedConfigurationVersion deletes a hosted configuration version.
func (b *InMemoryBackend) DeleteHostedConfigurationVersion(
	applicationID, profileID string,
	versionNumber int32,
) error {
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

	v, exists := profileVersions[versionNumber]
	if !exists {
		return fmt.Errorf("%w: version %d", ErrHostedConfigVersionNotFound, versionNumber)
	}

	if v.VersionLabel != "" {
		if labels := b.versionLabelIndex[applicationID][profileID]; labels != nil {
			delete(labels, v.VersionLabel)
		}
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

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrBadRequest)
	}

	if _, exists := b.deploymentStrategiesByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: deployment strategy with name %q already exists",
			ErrConflict,
			name,
		)
	}

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
	b.deploymentStrategiesByName[name] = strategy.ID
	cp := *strategy

	return &cp, nil
}

// GetDeploymentStrategy retrieves a deployment strategy by ID.
func (b *InMemoryBackend) GetDeploymentStrategy(strategyID string) (*DeploymentStrategy, error) {
	b.mu.RLock("GetDeploymentStrategy")
	defer b.mu.RUnlock()

	strategy, ok := b.deploymentStrategies[strategyID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: deployment strategy %s",
			ErrDeploymentStrategyNotFound,
			strategyID,
		)
	}

	cp := *strategy

	return &cp, nil
}

// ListDeploymentStrategies returns paginated deployment strategies.
func (b *InMemoryBackend) ListDeploymentStrategies(
	nextToken string,
	maxResults int,
) ([]DeploymentStrategy, string) {
	b.mu.RLock("ListDeploymentStrategies")
	defer b.mu.RUnlock()

	out := make([]DeploymentStrategy, 0, len(b.deploymentStrategies))
	for _, s := range b.deploymentStrategies {
		out = append(out, *s)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

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
		return nil, fmt.Errorf(
			"%w: deployment strategy %s",
			ErrDeploymentStrategyNotFound,
			strategyID,
		)
	}

	if name != "" && name != strategy.Name {
		delete(b.deploymentStrategiesByName, strategy.Name)
		b.deploymentStrategiesByName[name] = strategyID
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

	strategy, ok := b.deploymentStrategies[strategyID]
	if !ok {
		return fmt.Errorf("%w: deployment strategy %s", ErrDeploymentStrategyNotFound, strategyID)
	}

	delete(b.deploymentStrategiesByName, strategy.Name)
	delete(b.deploymentStrategies, strategyID)
	delete(b.tags, b.appconfigARN("deploymentstrategy/"+strategyID))

	return nil
}

// StartDeployment starts a deployment.
func (b *InMemoryBackend) StartDeployment(
	applicationID, environmentID, configProfileID, strategyID, configVersion, description string,
) (*Deployment, error) {
	b.mu.Lock("StartDeployment")
	defer b.mu.Unlock()

	if _, ok := b.applications[applicationID]; !ok {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	if _, ok := b.environments[applicationID][environmentID]; !ok {
		return nil, fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	if profiles := b.configProfiles[applicationID]; profiles == nil ||
		profiles[configProfileID] == nil {
		return nil, fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			configProfileID,
		)
	}

	if _, ok := b.deploymentStrategies[strategyID]; !ok {
		return nil, fmt.Errorf(
			"%w: deployment strategy %s",
			ErrDeploymentStrategyNotFound,
			strategyID,
		)
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
		Description:            description,
		State:                  "COMPLETE",
		TriggeredBy:            "USER",
		PercentageComplete:     100.0, //nolint:mnd // 100% complete
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

	sort.Slice(
		out,
		func(i, j int) bool { return out[i].DeploymentNumber < out[j].DeploymentNumber },
	)

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token, nil
}

// stoppableDeploymentStates are the states from which a deployment can be stopped.
var stoppableDeploymentStates = map[string]bool{ //nolint:gochecknoglobals // compile-time constant map
	"BAKING":     true,
	"DEPLOYING":  true,
	"VALIDATING": true,
}

// StopDeployment stops an in-progress deployment.
func (b *InMemoryBackend) StopDeployment(
	applicationID, environmentID string,
	deploymentNumber int32,
) error {
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

	// Allow stopping from any non-terminal state to keep in-memory stub pragmatic.
	// (Real deployments complete instantly here so we still accept the request.)
	if d.State != "COMPLETE" && d.State != "ROLLED_BACK" && !stoppableDeploymentStates[d.State] {
		return fmt.Errorf("%w: cannot stop deployment in state %s", ErrBadRequest, d.State)
	}

	d.State = "ROLLED_BACK"
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

	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrBadRequest)
	}

	// Enforce name uniqueness.
	if _, exists := b.extensionsByName[name]; exists {
		return nil, fmt.Errorf(
			"%w: extension with name %s already exists",
			ErrExtensionAlreadyExists,
			name,
		)
	}

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
	b.extensionsByName[name] = ext.ID
	cp := *ext

	return &cp, nil
}

// resolveExtension finds an extension by ID or name.
func (b *InMemoryBackend) resolveExtension(identifier string) *Extension {
	if ext, ok := b.extensions[identifier]; ok {
		return ext
	}

	if id, ok := b.extensionsByName[identifier]; ok {
		return b.extensions[id]
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

// ListExtensions returns paginated extensions, optionally filtered by name and/or version number.
func (b *InMemoryBackend) ListExtensions(
	nextToken string,
	maxResults int,
	nameFilter string,
	versionNumber int32,
) ([]Extension, string) {
	b.mu.RLock("ListExtensions")
	defer b.mu.RUnlock()

	out := make([]Extension, 0, len(b.extensions))

	for _, ext := range b.extensions {
		if nameFilter != "" && ext.Name != nameFilter {
			continue
		}

		if versionNumber > 0 && ext.VersionNumber != versionNumber {
			continue
		}

		out = append(out, *ext)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token
}

// UpdateExtension updates an extension's description, actions, and parameters.
func (b *InMemoryBackend) UpdateExtension(
	extensionIdentifier, description string,
	actions map[string][]ExtensionAction,
	parameters map[string]ExtensionParameter,
) (*Extension, error) {
	b.mu.Lock("UpdateExtension")
	defer b.mu.Unlock()

	ext := b.resolveExtension(extensionIdentifier)
	if ext == nil {
		return nil, fmt.Errorf("%w: extension %s", ErrExtensionNotFound, extensionIdentifier)
	}

	ext.Description = description

	if actions != nil {
		ext.Actions = actions
	}

	if parameters != nil {
		ext.Parameters = parameters
	}

	ext.VersionNumber++
	cp := *ext

	return &cp, nil
}

// DeleteExtension deletes an extension by identifier (ID or name).
func (b *InMemoryBackend) DeleteExtension(extensionIdentifier string) error {
	b.mu.Lock("DeleteExtension")
	defer b.mu.Unlock()

	ext := b.resolveExtension(extensionIdentifier)
	if ext == nil {
		return fmt.Errorf("%w: extension %s", ErrExtensionNotFound, extensionIdentifier)
	}

	delete(b.extensionsByName, ext.Name)
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

	if extensionIdentifier == "" {
		return nil, fmt.Errorf("%w: ExtensionIdentifier is required", ErrBadRequest)
	}

	if resourceIdentifier == "" {
		return nil, fmt.Errorf("%w: ResourceIdentifier is required", ErrBadRequest)
	}

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
func (b *InMemoryBackend) GetExtensionAssociation(
	extensionAssociationID string,
) (*ExtensionAssociation, error) {
	b.mu.RLock("GetExtensionAssociation")
	defer b.mu.RUnlock()

	assoc, ok := b.extensionAssociations[extensionAssociationID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: extension association %s",
			ErrExtensionAssociationNotFound,
			extensionAssociationID,
		)
	}

	cp := *assoc

	return &cp, nil
}

// ListExtensionAssociations returns paginated extension associations,
// optionally filtered by extensionIdentifier (ARN prefix) and/or resourceIdentifier (ARN prefix).
func (b *InMemoryBackend) ListExtensionAssociations(
	nextToken, extensionIdentifier, resourceIdentifier string,
	maxResults int,
) ([]ExtensionAssociation, string) {
	b.mu.RLock("ListExtensionAssociations")
	defer b.mu.RUnlock()

	out := make([]ExtensionAssociation, 0, len(b.extensionAssociations))
	for _, a := range b.extensionAssociations {
		if extensionIdentifier != "" && a.ExtensionArn != extensionIdentifier {
			continue
		}

		if resourceIdentifier != "" && a.ResourceArn != resourceIdentifier {
			continue
		}

		out = append(out, *a)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token
}

// DeleteExtensionAssociation deletes an extension association by ID.
func (b *InMemoryBackend) DeleteExtensionAssociation(extensionAssociationID string) error {
	b.mu.Lock("DeleteExtensionAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.extensionAssociations[extensionAssociationID]
	if !ok {
		return fmt.Errorf(
			"%w: extension association %s",
			ErrExtensionAssociationNotFound,
			extensionAssociationID,
		)
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

// UpdateAccountSettings updates account-level AppConfig settings.
func (b *InMemoryBackend) UpdateAccountSettings(
	deletionProtection *DeletionProtectionSettings,
) (*AccountSettings, error) {
	b.mu.Lock("UpdateAccountSettings")
	defer b.mu.Unlock()

	if deletionProtection != nil {
		b.accountSettings.DeletionProtection = deletionProtection
	}

	cp := b.accountSettings

	return &cp, nil
}

// UpdateExtensionAssociation updates an extension association's parameters.
func (b *InMemoryBackend) UpdateExtensionAssociation(
	extensionAssociationID string,
	parameters map[string]string,
) (*ExtensionAssociation, error) {
	b.mu.Lock("UpdateExtensionAssociation")
	defer b.mu.Unlock()

	assoc, ok := b.extensionAssociations[extensionAssociationID]
	if !ok {
		return nil, fmt.Errorf(
			"%w: extension association %s",
			ErrExtensionAssociationNotFound,
			extensionAssociationID,
		)
	}

	if parameters != nil {
		assoc.Parameters = parameters
	}

	cp := *assoc

	return &cp, nil
}

// ValidateConfiguration validates a configuration version against its validators.
// In this implementation, all well-formed configurations are considered valid.
// The configurationVersion parameter is accepted for API compatibility but not evaluated.
func (b *InMemoryBackend) ValidateConfiguration(applicationID, profileID, _ string) error {
	b.mu.RLock("ValidateConfiguration")
	defer b.mu.RUnlock()

	if _, ok := b.applications[applicationID]; !ok {
		return fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	profiles, ok := b.configProfiles[applicationID]
	if !ok || profiles[profileID] == nil {
		return fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			profileID,
		)
	}

	return nil
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
	if _, ok := b.applications[identifier]; ok {
		return identifier, nil
	}

	if id, ok := b.applicationsByName[identifier]; ok {
		return id, nil
	}

	return "", fmt.Errorf("%w: application %s", ErrApplicationNotFound, identifier)
}

// resolveEnvID finds an environment ID by ID or name within an application. Must be called under lock.
func (b *InMemoryBackend) resolveEnvID(appID, identifier string) (string, error) {
	if envs := b.environments[appID]; envs != nil {
		if _, ok := envs[identifier]; ok {
			return identifier, nil
		}
	}

	if envsByName := b.environmentsByName[appID]; envsByName != nil {
		if id, ok := envsByName[identifier]; ok {
			return id, nil
		}
	}

	return "", fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, identifier)
}

// resolveProfileID finds a configuration profile ID by ID or name. Must be called under lock.
func (b *InMemoryBackend) resolveProfileID(appID, identifier string) (string, error) {
	if profiles := b.configProfiles[appID]; profiles != nil {
		if _, ok := profiles[identifier]; ok {
			return identifier, nil
		}
	}

	if profilesByName := b.configProfilesByName[appID]; profilesByName != nil {
		if id, ok := profilesByName[identifier]; ok {
			return id, nil
		}
	}

	return "", fmt.Errorf(
		"%w: configuration profile %s",
		ErrConfigurationProfileNotFound,
		identifier,
	)
}

// latestConfigVersion returns the latest hosted configuration version for a profile. Must be called under lock.
// It walks version numbers from the counter downward to skip any deleted versions, so the
// common case (no deletes) is O(1) and deletions from the top add at most one step each.
func (b *InMemoryBackend) latestConfigVersion(appID, profileID string) *HostedConfigurationVersion {
	profileVersions := b.hostedConfigVersions[appID][profileID]
	empty := &HostedConfigurationVersion{
		ApplicationID:          appID,
		ConfigurationProfileID: profileID,
		ContentType:            contentTypeOctetStream,
		Content:                []byte{},
	}

	if len(profileVersions) == 0 {
		return empty
	}

	counter := b.versionCounters[appID][profileID]

	for n := counter; n >= 1; n-- {
		if v, ok := profileVersions[n]; ok {
			cp := *v

			return &cp
		}
	}

	return empty
}

// appConfigPaginate applies HMAC-signed token-based pagination to a sorted slice.
func appConfigPaginate[T any](all []T, nextToken, secret string, maxResults int) ([]T, string) {
	const defaultLimit = 50

	p := page.NewHMAC(all, nextToken, secret, maxResults, defaultLimit)

	return p.Data, p.Next
}
