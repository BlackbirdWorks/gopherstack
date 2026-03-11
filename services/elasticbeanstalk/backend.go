package elasticbeanstalk

import (
	"fmt"
	"maps"
	"slices"

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

// InMemoryBackend stores AWS Elastic Beanstalk state in memory.
type InMemoryBackend struct {
	applications map[string]*Application
	environments map[string]*Environment
	appVersions  map[string]*ApplicationVersion
	mu           *lockmetrics.RWMutex
	accountID    string
	region       string
	envCounter   int
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

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		applications: make(map[string]*Application),
		environments: make(map[string]*Environment),
		appVersions:  make(map[string]*ApplicationVersion),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("elasticbeanstalk"),
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

	return cloneApplication(app), nil
}

// DescribeApplications returns applications, optionally filtered by names.
func (b *InMemoryBackend) DescribeApplications(names []string) []*Application {
	b.mu.RLock("DescribeApplications")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		list := make([]*Application, 0, len(b.applications))

		for _, app := range b.applications {
			list = append(list, cloneApplication(app))
		}

		return list
	}

	list := make([]*Application, 0, len(names))

	for _, name := range names {
		if app, ok := b.applications[name]; ok {
			list = append(list, cloneApplication(app))
		}
	}

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

// DeleteApplication removes an application.
func (b *InMemoryBackend) DeleteApplication(name string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	if _, ok := b.applications[name]; !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, name)
	}

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

	return cloneEnvironment(env), nil
}

// DescribeEnvironments returns environments, optionally filtered by app/environment names or IDs.
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

	return cloneApplicationVersion(ver), nil
}

// DescribeApplicationVersions returns application versions, optionally filtered.
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

	delete(b.appVersions, key)

	return nil
}

// ListTagsForResource returns the tags for a resource identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if tags, ok := b.findTagsByARN(resourceARN); ok {
		return copyTags(tags), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UpdateTagsForResource updates tags on a resource identified by ARN.
func (b *InMemoryBackend) UpdateTagsForResource(resourceARN string, addTags, removeTags map[string]string) error {
	b.mu.Lock("UpdateTagsForResource")
	defer b.mu.Unlock()

	existing, ok := b.findTagsByARN(resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	if existing == nil {
		b.initTagsByARN(resourceARN)
		existing, _ = b.findTagsByARN(resourceARN)
	}

	maps.Copy(existing, addTags)

	for k := range removeTags {
		delete(existing, k)
	}

	return nil
}

// findTagsByARN looks up the tags map for a resource by ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findTagsByARN(resourceARN string) (map[string]string, bool) {
	for _, app := range b.applications {
		if app.ApplicationARN == resourceARN {
			return app.Tags, true
		}
	}

	for _, env := range b.environments {
		if env.EnvironmentARN == resourceARN {
			return env.Tags, true
		}
	}

	for _, ver := range b.appVersions {
		if ver.ApplicationVersionARN == resourceARN {
			return ver.Tags, true
		}
	}

	return nil, false
}

// initTagsByARN ensures a resource has an initialised tags map.
// Caller must hold the write lock.
func (b *InMemoryBackend) initTagsByARN(resourceARN string) {
	for _, app := range b.applications {
		if app.ApplicationARN == resourceARN {
			app.Tags = make(map[string]string)

			return
		}
	}

	for _, env := range b.environments {
		if env.EnvironmentARN == resourceARN {
			env.Tags = make(map[string]string)

			return
		}
	}

	for _, ver := range b.appVersions {
		if ver.ApplicationVersionARN == resourceARN {
			ver.Tags = make(map[string]string)
		}
	}
}
