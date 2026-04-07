package elasticbeanstalk

import (
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ClientException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("ClientException", awserr.ErrAlreadyExists)
	// ErrUnknownAction is returned when an unknown action is requested.
	ErrUnknownAction = awserr.New("UnknownOperationException", awserr.ErrInvalidParameter)
	// ErrInvalidParameter is returned when a required parameter is missing or invalid.
	ErrInvalidParameter = awserr.New("InvalidParameterValue", awserr.ErrInvalidParameter)
	// ErrValidation is returned when request input fails validation.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// Application represents an Elastic Beanstalk application.
type Application struct {
	Tags            map[string]string `json:"tags,omitempty"`
	ApplicationName string            `json:"applicationName"`
	ApplicationARN  string            `json:"applicationArn"`
	Description     string            `json:"description,omitempty"`
}

// Environment represents an Elastic Beanstalk environment.
type Environment struct {
	Tags              map[string]string `json:"tags,omitempty"`
	ApplicationName   string            `json:"applicationName"`
	EnvironmentName   string            `json:"environmentName"`
	EnvironmentID     string            `json:"environmentId"`
	EnvironmentARN    string            `json:"environmentArn"`
	SolutionStackName string            `json:"solutionStackName,omitempty"`
	Description       string            `json:"description,omitempty"`
	OperationsRole    string            `json:"operationsRole,omitempty"`
	Status            string            `json:"status"`
	Health            string            `json:"health"`
	Tier              string            `json:"tier,omitempty"`
}

// ApplicationVersion represents an Elastic Beanstalk application version.
type ApplicationVersion struct {
	Tags                  map[string]string `json:"tags,omitempty"`
	ApplicationName       string            `json:"applicationName"`
	VersionLabel          string            `json:"versionLabel"`
	ApplicationVersionARN string            `json:"applicationVersionArn"`
	Description           string            `json:"description,omitempty"`
	Status                string            `json:"status"`
}

// ConfigurationTemplate represents an Elastic Beanstalk configuration template.
type ConfigurationTemplate struct {
	Tags              map[string]string `json:"tags,omitempty"`
	ApplicationName   string            `json:"applicationName"`
	TemplateName      string            `json:"templateName"`
	Description       string            `json:"description,omitempty"`
	SolutionStackName string            `json:"solutionStackName,omitempty"`
}

// PlatformVersion represents an Elastic Beanstalk platform version.
type PlatformVersion struct {
	Tags            map[string]string `json:"tags,omitempty"`
	PlatformArn     string            `json:"platformArn"`
	PlatformName    string            `json:"platformName"`
	PlatformVersion string            `json:"platformVersion"`
	PlatformStatus  string            `json:"platformStatus"`
}

// InMemoryBackend stores AWS Elastic Beanstalk state in memory.
type InMemoryBackend struct {
	applications     map[string]*Application
	environments     map[string]*Environment
	appVersions      map[string]*ApplicationVersion
	configTemplates  map[string]*ConfigurationTemplate // configTemplateKey → template
	platformVersions map[string]*PlatformVersion       // platformARN → version
	appARNIndex      map[string]string                 // ARN → app name
	envARNIndex      map[string]string                 // ARN → envKey
	verARNIndex      map[string]string                 // ARN → appVersionKey
	mu               *lockmetrics.RWMutex
	accountID        string
	region           string
	storageLocation  string
	envCounter       int
}

// copyTags creates a shallow copy of the given tags map.
func copyTags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out
}

// cloneApplication returns a deep copy of the given Application (including Tags).
func cloneApplication(app *Application) *Application {
	cp := *app
	cp.Tags = copyTags(app.Tags)

	return &cp
}

// cloneEnvironment returns a deep copy of the given Environment (including Tags).
func cloneEnvironment(env *Environment) *Environment {
	cp := *env
	cp.Tags = copyTags(env.Tags)

	return &cp
}

// cloneApplicationVersion returns a deep copy of the given ApplicationVersion (including Tags).
func cloneApplicationVersion(ver *ApplicationVersion) *ApplicationVersion {
	cp := *ver
	cp.Tags = copyTags(ver.Tags)

	return &cp
}

// cloneConfigurationTemplate returns a deep copy of the given ConfigurationTemplate.
func cloneConfigurationTemplate(tmpl *ConfigurationTemplate) *ConfigurationTemplate {
	cp := *tmpl
	cp.Tags = copyTags(tmpl.Tags)

	return &cp
}

// clonePlatformVersion returns a deep copy of the given PlatformVersion.
func clonePlatformVersion(pv *PlatformVersion) *PlatformVersion {
	cp := *pv
	cp.Tags = copyTags(pv.Tags)

	return &cp
}

// configTemplateKey returns the map key for a configuration template.
func configTemplateKey(appName, templateName string) string {
	return appName + ":" + templateName
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		applications:     make(map[string]*Application),
		environments:     make(map[string]*Environment),
		appVersions:      make(map[string]*ApplicationVersion),
		configTemplates:  make(map[string]*ConfigurationTemplate),
		platformVersions: make(map[string]*PlatformVersion),
		appARNIndex:      make(map[string]string),
		envARNIndex:      make(map[string]string),
		verARNIndex:      make(map[string]string),
		accountID:        accountID,
		region:           region,
		storageLocation:  "elasticbeanstalk-" + region + "-" + accountID,
		mu:               lockmetrics.New("elasticbeanstalk"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// envKey returns the map key for an environment (applicationName + ":" + environmentName).
func envKey(appName, envName string) string {
	return appName + ":" + envName
}

// appVersionKey returns the map key for an application version.
func appVersionKey(appName, versionLabel string) string {
	return appName + ":" + versionLabel
}

// CreateApplication creates a new Elastic Beanstalk application.
func (b *InMemoryBackend) CreateApplication(
	name, description string,
	tags map[string]string,
) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if _, ok := b.applications[name]; ok {
		return nil, fmt.Errorf("%w: application %s already exists", ErrAlreadyExists, name)
	}

	appARN := arn.Build("elasticbeanstalk", b.region, b.accountID, "application/"+name)

	app := &Application{
		ApplicationName: name,
		ApplicationARN:  appARN,
		Description:     description,
		Tags:            copyTags(tags),
	}
	b.applications[name] = app
	b.appARNIndex[appARN] = name

	return cloneApplication(app), nil
}

// DescribeApplications returns applications, optionally filtered by names.
// Results are sorted by ApplicationName for deterministic output.
func (b *InMemoryBackend) DescribeApplications(names []string) []*Application {
	b.mu.RLock("DescribeApplications")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		list := make([]*Application, 0, len(b.applications))

		for _, app := range b.applications {
			list = append(list, cloneApplication(app))
		}

		sort.Slice(list, func(i, j int) bool {
			return list[i].ApplicationName < list[j].ApplicationName
		})

		return list
	}

	list := make([]*Application, 0, len(names))

	for _, name := range names {
		if app, ok := b.applications[name]; ok {
			list = append(list, cloneApplication(app))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ApplicationName < list[j].ApplicationName
	})

	return list
}

// UpdateApplication updates an application's description.
func (b *InMemoryBackend) UpdateApplication(name, description string) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
	if !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, name)
	}

	app.Description = description

	return cloneApplication(app), nil
}

// DeleteApplication removes an application and all associated environments and versions.
func (b *InMemoryBackend) DeleteApplication(name string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	app, ok := b.applications[name]
	if !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, name)
	}

	// Cascade: remove all environments belonging to this application.
	for key, env := range b.environments {
		if env.ApplicationName == name {
			delete(b.envARNIndex, env.EnvironmentARN)
			delete(b.environments, key)
		}
	}

	// Cascade: remove all application versions belonging to this application.
	for key, ver := range b.appVersions {
		if ver.ApplicationName == name {
			delete(b.verARNIndex, ver.ApplicationVersionARN)
			delete(b.appVersions, key)
		}
	}

	// Cascade: remove all configuration templates belonging to this application.
	for key, tmpl := range b.configTemplates {
		if tmpl.ApplicationName == name {
			delete(b.configTemplates, key)
		}
	}

	delete(b.appARNIndex, app.ApplicationARN)
	delete(b.applications, name)

	return nil
}

// CreateEnvironment creates a new Elastic Beanstalk environment.
func (b *InMemoryBackend) CreateEnvironment(
	appName, envName, solutionStack, description string,
	tags map[string]string,
) (*Environment, error) {
	b.mu.Lock("CreateEnvironment")
	defer b.mu.Unlock()

	key := envKey(appName, envName)
	if _, ok := b.environments[key]; ok {
		return nil, fmt.Errorf("%w: environment %s already exists", ErrAlreadyExists, envName)
	}

	b.envCounter++
	envID := fmt.Sprintf("e-%08d", b.envCounter)
	envARN := arn.Build("elasticbeanstalk", b.region, b.accountID, "environment/"+appName+"/"+envName)

	env := &Environment{
		ApplicationName:   appName,
		EnvironmentName:   envName,
		EnvironmentID:     envID,
		EnvironmentARN:    envARN,
		SolutionStackName: solutionStack,
		Description:       description,
		Status:            "Ready",
		Health:            "Green",
		Tier:              "WebServer",
		Tags:              copyTags(tags),
	}
	b.environments[key] = env
	b.envARNIndex[envARN] = key

	return cloneEnvironment(env), nil
}

// DescribeEnvironments returns environments, optionally filtered by app/environment names or IDs.
// Results are sorted by EnvironmentName for deterministic output.
func (b *InMemoryBackend) DescribeEnvironments(appName string, envNames []string, envIDs []string) []*Environment {
	b.mu.RLock("DescribeEnvironments")
	defer b.mu.RUnlock()

	list := make([]*Environment, 0, len(b.environments))

	for _, env := range b.environments {
		if appName != "" && env.ApplicationName != appName {
			continue
		}

		if len(envNames) > 0 {
			found := slices.Contains(envNames, env.EnvironmentName)

			if !found {
				continue
			}
		}

		if len(envIDs) > 0 {
			found := slices.Contains(envIDs, env.EnvironmentID)

			if !found {
				continue
			}
		}

		list = append(list, cloneEnvironment(env))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].EnvironmentName < list[j].EnvironmentName
	})

	return list
}

// UpdateEnvironment updates an environment's description or solution stack.
func (b *InMemoryBackend) UpdateEnvironment(appName, envName, description, solutionStack string) (*Environment, error) {
	b.mu.Lock("UpdateEnvironment")
	defer b.mu.Unlock()

	key := envKey(appName, envName)

	env, ok := b.environments[key]
	if !ok {
		return nil, fmt.Errorf("%w: environment %s not found", ErrNotFound, envName)
	}

	if description != "" {
		env.Description = description
	}

	if solutionStack != "" {
		env.SolutionStackName = solutionStack
	}

	return cloneEnvironment(env), nil
}

// TerminateEnvironment marks an environment as Terminated and removes it from storage.
func (b *InMemoryBackend) TerminateEnvironment(appName, envName string) (*Environment, error) {
	b.mu.Lock("TerminateEnvironment")
	defer b.mu.Unlock()

	key := envKey(appName, envName)

	env, ok := b.environments[key]
	if !ok {
		return nil, fmt.Errorf("%w: environment %s not found", ErrNotFound, envName)
	}

	env.Status = "Terminated"
	out := cloneEnvironment(env)
	delete(b.envARNIndex, env.EnvironmentARN)
	delete(b.environments, key)

	return out, nil
}

// CreateApplicationVersion creates a new application version.
func (b *InMemoryBackend) CreateApplicationVersion(
	appName, versionLabel, description string,
	tags map[string]string,
) (*ApplicationVersion, error) {
	b.mu.Lock("CreateApplicationVersion")
	defer b.mu.Unlock()

	key := appVersionKey(appName, versionLabel)
	if _, ok := b.appVersions[key]; ok {
		return nil, fmt.Errorf("%w: application version %s already exists", ErrAlreadyExists, versionLabel)
	}

	vARN := arn.Build("elasticbeanstalk", b.region, b.accountID,
		"applicationversion/"+appName+"/"+versionLabel)

	ver := &ApplicationVersion{
		ApplicationName:       appName,
		VersionLabel:          versionLabel,
		ApplicationVersionARN: vARN,
		Description:           description,
		Status:                "Processed",
		Tags:                  copyTags(tags),
	}
	b.appVersions[key] = ver
	b.verARNIndex[ver.ApplicationVersionARN] = key

	return cloneApplicationVersion(ver), nil
}

// DescribeApplicationVersions returns application versions, optionally filtered.
// Results are sorted by VersionLabel for deterministic output.
func (b *InMemoryBackend) DescribeApplicationVersions(appName string, versionLabels []string) []*ApplicationVersion {
	b.mu.RLock("DescribeApplicationVersions")
	defer b.mu.RUnlock()

	list := make([]*ApplicationVersion, 0, len(b.appVersions))

	for _, ver := range b.appVersions {
		if appName != "" && ver.ApplicationName != appName {
			continue
		}

		if len(versionLabels) > 0 {
			found := slices.Contains(versionLabels, ver.VersionLabel)

			if !found {
				continue
			}
		}

		list = append(list, cloneApplicationVersion(ver))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].VersionLabel < list[j].VersionLabel
	})

	return list
}

// DeleteApplicationVersion removes an application version.
func (b *InMemoryBackend) DeleteApplicationVersion(appName, versionLabel string) error {
	b.mu.Lock("DeleteApplicationVersion")
	defer b.mu.Unlock()

	key := appVersionKey(appName, versionLabel)
	if _, ok := b.appVersions[key]; !ok {
		return fmt.Errorf("%w: application version %s not found", ErrNotFound, versionLabel)
	}

	delete(b.verARNIndex, b.appVersions[key].ApplicationVersionARN)
	delete(b.appVersions, key)

	return nil
}

// sortedTagKeys returns the keys of a tags map in sorted order.
func sortedTagKeys(tags map[string]string) []string {
	keys := make([]string, 0, len(tags))

	for k := range tags {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// ListTagsForResource returns the tags for a resource identified by ARN.
// Tags are returned sorted by key for deterministic output.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if tags, ok := b.lookupTagsByARN(resourceARN); ok {
		return copyTags(tags), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UpdateTagsForResource updates tags on a resource identified by ARN.
func (b *InMemoryBackend) UpdateTagsForResource(resourceARN string, addTags, removeTags map[string]string) error {
	b.mu.Lock("UpdateTagsForResource")
	defer b.mu.Unlock()

	existing, ok := b.lookupTagsByARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	if existing == nil {
		b.ensureTagsByARN(resourceARN)
		existing, _ = b.lookupTagsByARN(resourceARN)
	}

	maps.Copy(existing, addTags)

	for k := range removeTags {
		delete(existing, k)
	}

	return nil
}

// lookupTagsByARN looks up the tags map for a resource by ARN using O(1) index lookups.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupTagsByARN(resourceARN string) (map[string]string, bool) {
	if name, ok := b.appARNIndex[resourceARN]; ok {
		return b.applications[name].Tags, true
	}

	if key, ok := b.envARNIndex[resourceARN]; ok {
		return b.environments[key].Tags, true
	}

	if key, ok := b.verARNIndex[resourceARN]; ok {
		return b.appVersions[key].Tags, true
	}

	return nil, false
}

// ensureTagsByARN ensures a resource has an initialised tags map.
// Caller must hold the write lock.
func (b *InMemoryBackend) ensureTagsByARN(resourceARN string) {
	if name, ok := b.appARNIndex[resourceARN]; ok {
		if b.applications[name].Tags == nil {
			b.applications[name].Tags = make(map[string]string)
		}

		return
	}

	if key, ok := b.envARNIndex[resourceARN]; ok {
		if b.environments[key].Tags == nil {
			b.environments[key].Tags = make(map[string]string)
		}

		return
	}

	if key, ok := b.verARNIndex[resourceARN]; ok {
		if b.appVersions[key].Tags == nil {
			b.appVersions[key].Tags = make(map[string]string)
		}
	}
}

// Reset clears all backend state, resetting to an empty store.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.applications = make(map[string]*Application)
	b.environments = make(map[string]*Environment)
	b.appVersions = make(map[string]*ApplicationVersion)
	b.configTemplates = make(map[string]*ConfigurationTemplate)
	b.platformVersions = make(map[string]*PlatformVersion)
	b.appARNIndex = make(map[string]string)
	b.envARNIndex = make(map[string]string)
	b.verARNIndex = make(map[string]string)
	b.storageLocation = "elasticbeanstalk-" + b.region + "-" + b.accountID
	b.envCounter = 0
}

// --- New operations ---

// AbortEnvironmentUpdate aborts an in-progress environment configuration update.
// This is a no-op in the in-memory backend since updates complete instantly.
func (b *InMemoryBackend) AbortEnvironmentUpdate(_ string) error {
	return nil
}

// ApplyEnvironmentManagedAction applies a scheduled managed action immediately.
// This is a no-op stub that succeeds unconditionally.
func (b *InMemoryBackend) ApplyEnvironmentManagedAction(_, _ string) error {
	return nil
}

// AssociateEnvironmentOperationsRole associates an operations IAM role with an environment.
func (b *InMemoryBackend) AssociateEnvironmentOperationsRole(envName, role string) error {
	b.mu.Lock("AssociateEnvironmentOperationsRole")
	defer b.mu.Unlock()

	for _, env := range b.environments {
		if env.EnvironmentName == envName {
			env.OperationsRole = role

			return nil
		}
	}

	return fmt.Errorf("%w: environment %s not found", ErrNotFound, envName)
}

// CheckDNSAvailability checks whether the specified CNAME prefix is available.
// Returns available=true when no existing environment uses that prefix as its CNAME.
func (b *InMemoryBackend) CheckDNSAvailability(cnamePrefix string) (bool, string) {
	b.mu.RLock("CheckDNSAvailability")
	defer b.mu.RUnlock()

	fqcname := cnamePrefix + "." + b.region + ".elasticbeanstalk.com"

	for _, env := range b.environments {
		if env.EnvironmentName == cnamePrefix {
			return false, fqcname
		}
	}

	return true, fqcname
}

// ComposeEnvironments returns existing environments for an application.
// In a real deployment this would create multiple environments; the stub
// returns the already-running environments for the given application.
// Results are sorted by EnvironmentName for deterministic output.
func (b *InMemoryBackend) ComposeEnvironments(appName string) []*Environment {
	b.mu.RLock("ComposeEnvironments")
	defer b.mu.RUnlock()

	list := make([]*Environment, 0)

	for _, env := range b.environments {
		if env.ApplicationName == appName {
			list = append(list, cloneEnvironment(env))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].EnvironmentName < list[j].EnvironmentName
	})

	return list
}

// CreateConfigurationTemplate creates a new configuration template for an application.
func (b *InMemoryBackend) CreateConfigurationTemplate(
	appName, templateName, description, solutionStack string,
	tags map[string]string,
) (*ConfigurationTemplate, error) {
	b.mu.Lock("CreateConfigurationTemplate")
	defer b.mu.Unlock()

	key := configTemplateKey(appName, templateName)
	if _, ok := b.configTemplates[key]; ok {
		return nil, fmt.Errorf("%w: configuration template %s already exists", ErrAlreadyExists, templateName)
	}

	tmpl := &ConfigurationTemplate{
		ApplicationName:   appName,
		TemplateName:      templateName,
		Description:       description,
		SolutionStackName: solutionStack,
		Tags:              copyTags(tags),
	}
	b.configTemplates[key] = tmpl

	return cloneConfigurationTemplate(tmpl), nil
}

// CreatePlatformVersion creates a new custom platform version.
func (b *InMemoryBackend) CreatePlatformVersion(
	platformName, platformVersion string,
	tags map[string]string,
) (*PlatformVersion, error) {
	b.mu.Lock("CreatePlatformVersion")
	defer b.mu.Unlock()

	platformARN := arn.Build("elasticbeanstalk", b.region, "", "platform/"+platformName+"/"+platformVersion)

	if _, ok := b.platformVersions[platformARN]; ok {
		return nil, fmt.Errorf(
			"%w: platform version %s/%s already exists",
			ErrAlreadyExists,
			platformName,
			platformVersion,
		)
	}

	pv := &PlatformVersion{
		PlatformArn:     platformARN,
		PlatformName:    platformName,
		PlatformVersion: platformVersion,
		PlatformStatus:  "Ready",
		Tags:            copyTags(tags),
	}
	b.platformVersions[platformARN] = pv

	return clonePlatformVersion(pv), nil
}

// CreateStorageLocation returns the S3 bucket used for storing Elastic Beanstalk data.
// The bucket name is fixed per region and account, and creation is idempotent.
func (b *InMemoryBackend) CreateStorageLocation() string {
	return b.storageLocation
}

// DeleteConfigurationTemplate removes a configuration template.
func (b *InMemoryBackend) DeleteConfigurationTemplate(appName, templateName string) error {
	b.mu.Lock("DeleteConfigurationTemplate")
	defer b.mu.Unlock()

	key := configTemplateKey(appName, templateName)
	if _, ok := b.configTemplates[key]; !ok {
		return fmt.Errorf("%w: configuration template %s not found", ErrNotFound, templateName)
	}

	delete(b.configTemplates, key)

	return nil
}

// DeleteEnvironmentConfiguration deletes the draft configuration associated with an environment.
// This is a no-op in the in-memory backend.
func (b *InMemoryBackend) DeleteEnvironmentConfiguration(_, _ string) error {
	return nil
}

// --- Seed helpers (used in tests via export_test.go) ---

// addApplicationInternal seeds an application directly into the backend, bypassing validation.
// Caller must hold the write lock.
func (b *InMemoryBackend) addApplicationInternal(app *Application) {
	b.applications[app.ApplicationName] = cloneApplication(app)
	b.appARNIndex[app.ApplicationARN] = app.ApplicationName
}

// addEnvironmentInternal seeds an environment directly into the backend, bypassing validation.
// Caller must hold the write lock.
func (b *InMemoryBackend) addEnvironmentInternal(env *Environment) {
	key := envKey(env.ApplicationName, env.EnvironmentName)
	b.environments[key] = cloneEnvironment(env)
	b.envARNIndex[env.EnvironmentARN] = key
}

// addAppVersionInternal seeds an application version directly into the backend, bypassing validation.
// Caller must hold the write lock.
func (b *InMemoryBackend) addAppVersionInternal(ver *ApplicationVersion) {
	key := appVersionKey(ver.ApplicationName, ver.VersionLabel)
	b.appVersions[key] = cloneApplicationVersion(ver)
	b.verARNIndex[ver.ApplicationVersionARN] = key
}

// addConfigTemplateInternal seeds a configuration template directly into the backend.
// Caller must hold the write lock.
func (b *InMemoryBackend) addConfigTemplateInternal(tmpl *ConfigurationTemplate) {
	key := configTemplateKey(tmpl.ApplicationName, tmpl.TemplateName)
	b.configTemplates[key] = cloneConfigurationTemplate(tmpl)
}

// addPlatformVersionInternal seeds a platform version directly into the backend.
// Caller must hold the write lock.
func (b *InMemoryBackend) addPlatformVersionInternal(pv *PlatformVersion) {
	b.platformVersions[pv.PlatformArn] = clonePlatformVersion(pv)
}
