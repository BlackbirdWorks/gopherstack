package elasticbeanstalk

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
// Elastic Beanstalk resources are isolated per region: every backend operation resolves
// the caller's region from the request context and operates only on that region's store.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

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

const (
	// arnPrefixIAM is the prefix for IAM ARNs used in instance profile validation.
	arnPrefixIAM = "arn:aws:iam::"
	// envStatusReady is the status value for a ready environment.
	envStatusReady = "Ready"
	// envHealthGreen is the health value for a healthy environment.
	envHealthGreen = "Green"
	// managedActionFinishedTime is a placeholder timestamp for managed action history.
	managedActionFinishedTime = "2026-01-01T00:00:00Z"
	// appVersionStatusProcessed is returned for application versions requested with processing enabled.
	appVersionStatusProcessed = "Processed"
	// appVersionStatusUnprocessed is returned for application versions without processing enabled.
	appVersionStatusUnprocessed = "Unprocessed"
	// defaultEnvironmentTierName is the AWS default web application tier.
	defaultEnvironmentTierName = "WebServer"
	// defaultEnvironmentTierType is the AWS default standard tier type.
	defaultEnvironmentTierType = "Standard"
	// resourceCreatedAt is the fixed creation timestamp returned for all resources.
	resourceCreatedAt = "2026-01-01T00:00:00Z"
	// eventSeverityInfo is the severity level for informational events.
	eventSeverityInfo = "INFO"
)

// Application represents an Elastic Beanstalk application.
type Application struct {
	Tags                         map[string]string `json:"tags,omitempty"`
	ApplicationName              string            `json:"applicationName"`
	ApplicationARN               string            `json:"applicationArn"`
	Description                  string            `json:"description,omitempty"`
	DateCreated                  string            `json:"dateCreated,omitempty"`
	ResourceLifecycleServiceRole string            `json:"resourceLifecycleServiceRole,omitempty"`
}

// Environment represents an Elastic Beanstalk environment.
type Environment struct {
	Tags              map[string]string `json:"tags,omitempty"`
	Status            string            `json:"status"`
	Health            string            `json:"health"`
	EnvironmentName   string            `json:"environmentName"`
	EnvironmentID     string            `json:"environmentId"`
	EnvironmentARN    string            `json:"environmentArn"`
	SolutionStackName string            `json:"solutionStackName,omitempty"`
	CustomAMI         string            `json:"customAMI,omitempty"`
	TemplateName      string            `json:"templateName,omitempty"`
	VersionLabel      string            `json:"versionLabel,omitempty"`
	Description       string            `json:"description,omitempty"`
	ApplicationName   string            `json:"applicationName"`
	PlatformARN       string            `json:"platformArn,omitempty"`
	TierVersion       string            `json:"tierVersion,omitempty"`
	Tier              string            `json:"tier,omitempty"`
	TierType          string            `json:"tierType,omitempty"`
	TierName          string            `json:"tierName,omitempty"`
	OperationsRole    string            `json:"operationsRole,omitempty"`
	CNAME             string            `json:"cname,omitempty"`
	CNAMEPrefix       string            `json:"cnamePrefix,omitempty"`
	LoadBalancerType  string            `json:"loadBalancerType,omitempty"`
	VPCID             string            `json:"vpcId,omitempty"`
	Subnets           string            `json:"subnets,omitempty"`
	InstanceProfile   string            `json:"instanceProfile,omitempty"`
	DateCreated       string            `json:"dateCreated,omitempty"`
	Region            string            `json:"region"`
	OptionSettings    []OptionSetting   `json:"optionSettings,omitempty"`
}

// ApplicationVersion represents an Elastic Beanstalk application version.
type ApplicationVersion struct {
	Tags                   map[string]string       `json:"tags,omitempty"`
	SourceBuildInformation *SourceBuildInformation `json:"sourceBuildInformation,omitempty"`
	ApplicationName        string                  `json:"applicationName"`
	VersionLabel           string                  `json:"versionLabel"`
	ApplicationVersionARN  string                  `json:"applicationVersionArn"`
	Description            string                  `json:"description,omitempty"`
	DateCreated            string                  `json:"dateCreated,omitempty"`
	Status                 string                  `json:"status"`
	S3Bucket               string                  `json:"s3Bucket,omitempty"`
	S3Key                  string                  `json:"s3Key,omitempty"`
	Process                bool                    `json:"process"`
}

// SourceBuildInformation identifies a CodeCommit source for an application version.
type SourceBuildInformation struct {
	SourceType       string `json:"sourceType,omitempty"`
	SourceRepository string `json:"sourceRepository,omitempty"`
	SourceLocation   string `json:"sourceLocation,omitempty"`
}

// OptionSetting is a configured Elastic Beanstalk environment option.
type OptionSetting struct {
	Namespace    string `json:"namespace"`
	OptionName   string `json:"optionName"`
	ResourceName string `json:"resourceName,omitempty"`
	Value        string `json:"value,omitempty"`
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

// ManagedActionHistory represents a record of a managed action that was applied (improvement #4).
type ManagedActionHistory struct {
	ActionID          string `json:"actionId"`
	ActionType        string `json:"actionType"`
	ActionDescription string `json:"actionDescription"`
	Status            string `json:"status"`
	FinishedTime      string `json:"finishedTime"`
}

// EventRecord represents a single Elastic Beanstalk event.
type EventRecord struct {
	ApplicationName string `json:"applicationName,omitempty"`
	EnvironmentName string `json:"environmentName,omitempty"`
	EventDate       string `json:"eventDate"`
	Message         string `json:"message"`
	Severity        string `json:"severity"`
}

// InMemoryBackend stores AWS Elastic Beanstalk state in memory.
// All maps are nested by region: map[region]map[key]*Resource.
type InMemoryBackend struct {
	applications         map[string]map[string]*Application
	environments         map[string]map[string]*Environment
	appVersions          map[string]map[string]*ApplicationVersion
	configTemplates      map[string]map[string]*ConfigurationTemplate  // region → configTemplateKey → template
	platformVersions     map[string]map[string]*PlatformVersion        // region → platformARN → version
	managedActionHistory map[string]map[string][]*ManagedActionHistory // region → envName → history items
	appARNIndex          map[string]map[string]string                  // region → ARN → app name
	envARNIndex          map[string]map[string]string                  // region → ARN → envKey
	verARNIndex          map[string]map[string]string                  // region → ARN → appVersionKey
	events               map[string][]*EventRecord                     // region → events
	envCounters          map[string]int                                // region → counter
	mu                   *lockmetrics.RWMutex
	accountID            string
	region               string // default region
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
	cp.OptionSettings = slices.Clone(env.OptionSettings)

	return &cp
}

// cloneApplicationVersion returns a deep copy of the given ApplicationVersion (including Tags).
func cloneApplicationVersion(ver *ApplicationVersion) *ApplicationVersion {
	cp := *ver
	cp.Tags = copyTags(ver.Tags)
	if ver.SourceBuildInformation != nil {
		source := *ver.SourceBuildInformation
		cp.SourceBuildInformation = &source
	}

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
		applications:         make(map[string]map[string]*Application),
		environments:         make(map[string]map[string]*Environment),
		appVersions:          make(map[string]map[string]*ApplicationVersion),
		configTemplates:      make(map[string]map[string]*ConfigurationTemplate),
		platformVersions:     make(map[string]map[string]*PlatformVersion),
		managedActionHistory: make(map[string]map[string][]*ManagedActionHistory),
		appARNIndex:          make(map[string]map[string]string),
		envARNIndex:          make(map[string]map[string]string),
		verARNIndex:          make(map[string]map[string]string),
		events:               make(map[string][]*EventRecord),
		envCounters:          make(map[string]int),
		accountID:            accountID,
		region:               region,
		mu:                   lockmetrics.New("elasticbeanstalk"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// --- Per-region store helpers. Callers must hold b.mu. ---

func (b *InMemoryBackend) applicationsStore(region string) map[string]*Application {
	if b.applications[region] == nil {
		b.applications[region] = make(map[string]*Application)
	}

	return b.applications[region]
}

func (b *InMemoryBackend) environmentsStore(region string) map[string]*Environment {
	if b.environments[region] == nil {
		b.environments[region] = make(map[string]*Environment)
	}

	return b.environments[region]
}

func (b *InMemoryBackend) appVersionsStore(region string) map[string]*ApplicationVersion {
	if b.appVersions[region] == nil {
		b.appVersions[region] = make(map[string]*ApplicationVersion)
	}

	return b.appVersions[region]
}

func (b *InMemoryBackend) configTemplatesStore(region string) map[string]*ConfigurationTemplate {
	if b.configTemplates[region] == nil {
		b.configTemplates[region] = make(map[string]*ConfigurationTemplate)
	}

	return b.configTemplates[region]
}

func (b *InMemoryBackend) platformVersionsStore(region string) map[string]*PlatformVersion {
	if b.platformVersions[region] == nil {
		b.platformVersions[region] = make(map[string]*PlatformVersion)
	}

	return b.platformVersions[region]
}

func (b *InMemoryBackend) managedActionHistoryStore(region string) map[string][]*ManagedActionHistory {
	if b.managedActionHistory[region] == nil {
		b.managedActionHistory[region] = make(map[string][]*ManagedActionHistory)
	}

	return b.managedActionHistory[region]
}

func (b *InMemoryBackend) appARNIndexStore(region string) map[string]string {
	if b.appARNIndex[region] == nil {
		b.appARNIndex[region] = make(map[string]string)
	}

	return b.appARNIndex[region]
}

func (b *InMemoryBackend) envARNIndexStore(region string) map[string]string {
	if b.envARNIndex[region] == nil {
		b.envARNIndex[region] = make(map[string]string)
	}

	return b.envARNIndex[region]
}

func (b *InMemoryBackend) verARNIndexStore(region string) map[string]string {
	if b.verARNIndex[region] == nil {
		b.verARNIndex[region] = make(map[string]string)
	}

	return b.verARNIndex[region]
}

func (b *InMemoryBackend) eventsSlice(region string) []*EventRecord {
	if b.events[region] == nil {
		b.events[region] = make([]*EventRecord, 0)
	}

	return b.events[region]
}

func (b *InMemoryBackend) nextEnvID(region string) string {
	b.envCounters[region]++

	return fmt.Sprintf("e-%08d", b.envCounters[region])
}

// --- Application operations ---

// CreateApplication creates a new Elastic Beanstalk application.
func (b *InMemoryBackend) CreateApplication(
	ctx context.Context,
	name, description string,
	tags map[string]string,
) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.applicationsStore(region)[name]; ok {
		return nil, fmt.Errorf("%w: application %s already exists", ErrAlreadyExists, name)
	}

	appARN := arn.Build("elasticbeanstalk", region, b.accountID, "application/"+name)

	app := &Application{
		ApplicationName: name,
		ApplicationARN:  appARN,
		Description:     description,
		DateCreated:     resourceCreatedAt,
		Tags:            copyTags(tags),
	}
	b.applicationsStore(region)[name] = app
	b.appARNIndexStore(region)[appARN] = name

	return cloneApplication(app), nil
}

// DescribeApplications returns applications, optionally filtered by names.
// Results are sorted by ApplicationName for deterministic output.
func (b *InMemoryBackend) DescribeApplications(ctx context.Context, names []string) []*Application {
	b.mu.RLock("DescribeApplications")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.applicationsStore(region)

	if len(names) == 0 {
		list := make([]*Application, 0, len(store))

		for _, app := range store {
			list = append(list, cloneApplication(app))
		}

		sort.Slice(list, func(i, j int) bool {
			return list[i].ApplicationName < list[j].ApplicationName
		})

		return list
	}

	list := make([]*Application, 0, len(names))

	for _, name := range names {
		if app, ok := store[name]; ok {
			list = append(list, cloneApplication(app))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].ApplicationName < list[j].ApplicationName
	})

	return list
}

// UpdateApplication updates an application's description.
func (b *InMemoryBackend) UpdateApplication(ctx context.Context, name, description string) (*Application, error) {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	app, ok := b.applicationsStore(region)[name]
	if !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, name)
	}

	app.Description = description

	return cloneApplication(app), nil
}

// UpdateApplicationResourceLifecycle stores the resource lifecycle service role on the application (improvement #7).
func (b *InMemoryBackend) UpdateApplicationResourceLifecycle(
	ctx context.Context,
	appName, serviceRole string,
) (*Application, error) {
	b.mu.Lock("UpdateApplicationResourceLifecycle")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	app, ok := b.applicationsStore(region)[appName]
	if !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	app.ResourceLifecycleServiceRole = serviceRole

	return cloneApplication(app), nil
}

// DeleteApplication removes an application and all associated environments and versions.
func (b *InMemoryBackend) DeleteApplication(ctx context.Context, name string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	app, ok := b.applicationsStore(region)[name]
	if !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, name)
	}

	// Cascade: remove all environments belonging to this application.
	for key, env := range b.environmentsStore(region) {
		if env.ApplicationName == name {
			delete(b.envARNIndexStore(region), env.EnvironmentARN)
			delete(b.environmentsStore(region), key)
		}
	}

	// Cascade: remove all application versions belonging to this application.
	for key, ver := range b.appVersionsStore(region) {
		if ver.ApplicationName == name {
			delete(b.verARNIndexStore(region), ver.ApplicationVersionARN)
			delete(b.appVersionsStore(region), key)
		}
	}

	// Cascade: remove all configuration templates belonging to this application.
	for key, tmpl := range b.configTemplatesStore(region) {
		if tmpl.ApplicationName == name {
			delete(b.configTemplatesStore(region), key)
		}
	}

	delete(b.appARNIndexStore(region), app.ApplicationARN)
	delete(b.applicationsStore(region), name)

	return nil
}

// CreateEnvironmentParams holds optional parameters for CreateEnvironment (improvements #1, #5, #14, #15, #16).
type CreateEnvironmentParams struct {
	TierType         string
	TierName         string
	TierVersion      string
	CNAMEPrefix      string
	PlatformARN      string
	TemplateName     string
	VersionLabel     string
	OperationsRole   string
	LoadBalancerType string
	VPCID            string
	Subnets          string
	InstanceProfile  string
	CustomAMI        string
	OptionSettings   []OptionSetting
}

// UpdateEnvironmentParams holds state changes accepted by UpdateEnvironment.
type UpdateEnvironmentParams struct {
	SolutionStackName string
	PlatformARN       string
	TemplateName      string
	VersionLabel      string
	Description       string
	TierType          string
	TierName          string
	TierVersion       string
	OptionSettings    []OptionSetting
	OptionsToRemove   []OptionSetting
}

// ValidateInstanceProfileARN validates that an instance profile ARN has the correct format (improvement #16).
func ValidateInstanceProfileARN(instanceProfile string) error {
	if instanceProfile == "" {
		return nil
	}

	if !strings.HasPrefix(instanceProfile, arnPrefixIAM) {
		return fmt.Errorf(
			"%w: InstanceProfile must be a valid IAM ARN starting with %s",
			ErrInvalidParameter,
			arnPrefixIAM,
		)
	}

	return nil
}

// CreateEnvironment creates a new Elastic Beanstalk environment.
func (b *InMemoryBackend) CreateEnvironment(
	ctx context.Context,
	appName, envName, solutionStack, description string,
	tags map[string]string,
	params CreateEnvironmentParams,
) (*Environment, error) {
	b.mu.Lock("CreateEnvironment")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := envKey(appName, envName)

	if _, ok := b.environmentsStore(region)[key]; ok {
		return nil, fmt.Errorf("%w: environment %s already exists", ErrAlreadyExists, envName)
	}

	envID := b.nextEnvID(region)
	envARN := arn.Build("elasticbeanstalk", region, b.accountID, "environment/"+appName+"/"+envName)

	// Resolve tier fields (improvement #1)
	tierName := params.TierName
	if tierName == "" {
		tierName = defaultEnvironmentTierName
	}

	tierType := params.TierType
	if tierType == "" {
		tierType = defaultEnvironmentTierType
	}

	cnamePrefix := params.CNAMEPrefix
	if cnamePrefix == "" {
		cnamePrefix = envName
	}
	cname := cnamePrefix + "." + region + ".elasticbeanstalk.com"

	env := &Environment{
		OptionSettings:    slices.Clone(params.OptionSettings),
		ApplicationName:   appName,
		EnvironmentName:   envName,
		EnvironmentID:     envID,
		EnvironmentARN:    envARN,
		SolutionStackName: solutionStack,
		PlatformARN:       params.PlatformARN,
		TemplateName:      params.TemplateName,
		VersionLabel:      params.VersionLabel,
		Description:       description,
		OperationsRole:    params.OperationsRole,
		Status:            envStatusReady,
		Health:            envHealthGreen,
		Tier:              tierName,
		TierType:          tierType,
		TierName:          tierName,
		TierVersion:       params.TierVersion,
		CNAME:             cname,
		CNAMEPrefix:       cnamePrefix,
		LoadBalancerType:  params.LoadBalancerType,
		VPCID:             params.VPCID,
		Subnets:           params.Subnets,
		InstanceProfile:   params.InstanceProfile,
		CustomAMI:         params.CustomAMI,
		DateCreated:       resourceCreatedAt,
		Region:            region,
		Tags:              copyTags(tags),
	}
	b.environmentsStore(region)[key] = env
	b.envARNIndexStore(region)[envARN] = key

	b.appendEvent(region, appName, envName, "Successfully launched environment: "+envName+".", eventSeverityInfo)

	return cloneEnvironment(env), nil
}

// DescribeEnvironments returns environments, optionally filtered by app/environment names or IDs.
// Results are sorted by EnvironmentName for deterministic output.
func (b *InMemoryBackend) DescribeEnvironments(
	ctx context.Context,
	appName string,
	envNames []string,
	envIDs []string,
) []*Environment {
	b.mu.RLock("DescribeEnvironments")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.environmentsStore(region)

	list := make([]*Environment, 0, len(store))

	for _, env := range store {
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
func (b *InMemoryBackend) UpdateEnvironment(
	ctx context.Context,
	appName, envName, description, solutionStack string,
) (*Environment, error) {
	return b.UpdateEnvironmentWithParams(ctx, appName, envName, UpdateEnvironmentParams{
		Description:       description,
		SolutionStackName: solutionStack,
	})
}

// UpdateEnvironmentWithParams applies all mutable environment properties.
func (b *InMemoryBackend) UpdateEnvironmentWithParams(
	ctx context.Context,
	appName, envName string,
	params UpdateEnvironmentParams,
) (*Environment, error) {
	b.mu.Lock("UpdateEnvironment")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := envKey(appName, envName)

	env, ok := b.environmentsStore(region)[key]
	if !ok {
		return nil, fmt.Errorf("%w: environment %s not found", ErrNotFound, envName)
	}

	if params.Description != "" {
		env.Description = params.Description
	}

	if params.SolutionStackName != "" {
		env.SolutionStackName = params.SolutionStackName
		env.PlatformARN = ""
		env.TemplateName = ""
	}

	if params.PlatformARN != "" {
		env.PlatformARN = params.PlatformARN
		env.SolutionStackName = ""
		env.TemplateName = ""
	}

	if params.TemplateName != "" {
		env.TemplateName = params.TemplateName
		env.SolutionStackName = ""
		env.PlatformARN = ""
	}

	if params.VersionLabel != "" {
		env.VersionLabel = params.VersionLabel
	}

	if params.TierName != "" {
		env.Tier = params.TierName
		env.TierName = params.TierName
	}

	if params.TierType != "" {
		env.TierType = params.TierType
	}

	if params.TierVersion != "" {
		env.TierVersion = params.TierVersion
	}

	env.OptionSettings = updateOptionSettings(
		env.OptionSettings,
		params.OptionSettings,
		params.OptionsToRemove,
	)

	b.appendEvent(region, appName, envName, "Environment update completed successfully.", eventSeverityInfo)

	return cloneEnvironment(env), nil
}

// updateOptionSettings applies updates and removals while preserving deterministic output ordering.
func updateOptionSettings(current, updates, removals []OptionSetting) []OptionSetting {
	byKey := make(map[string]OptionSetting, len(current)+len(updates))
	for _, setting := range current {
		byKey[optionSettingKey(setting)] = setting
	}
	for _, setting := range updates {
		byKey[optionSettingKey(setting)] = setting
	}
	for _, setting := range removals {
		delete(byKey, optionSettingKey(setting))
	}

	result := make([]OptionSetting, 0, len(byKey))
	for _, setting := range byKey {
		result = append(result, setting)
	}
	sort.Slice(result, func(i, j int) bool {
		return optionSettingKey(result[i]) < optionSettingKey(result[j])
	})

	return result
}

func optionSettingKey(setting OptionSetting) string {
	return setting.Namespace + "\x00" + setting.OptionName + "\x00" + setting.ResourceName
}

// TerminateEnvironment marks an environment as Terminated and removes it from storage.
func (b *InMemoryBackend) TerminateEnvironment(ctx context.Context, appName, envName string) (*Environment, error) {
	b.mu.Lock("TerminateEnvironment")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := envKey(appName, envName)

	env, ok := b.environmentsStore(region)[key]
	if !ok {
		return nil, fmt.Errorf("%w: environment %s not found", ErrNotFound, envName)
	}

	env.Status = "Terminated"
	out := cloneEnvironment(env)
	delete(b.envARNIndexStore(region), env.EnvironmentARN)
	delete(b.environmentsStore(region), key)

	b.appendEvent(region, appName, envName, "terminateEnvironment completed successfully.", eventSeverityInfo)

	return out, nil
}

// CloneEnvironment creates a new environment by copying an existing one (improvement #9).
func (b *InMemoryBackend) CloneEnvironment(
	ctx context.Context,
	srcAppName, srcEnvName, newEnvName string,
) (*Environment, error) {
	b.mu.Lock("CloneEnvironment")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	srcKey := envKey(srcAppName, srcEnvName)

	src, ok := b.environmentsStore(region)[srcKey]
	if !ok {
		return nil, fmt.Errorf("%w: source environment %s not found", ErrNotFound, srcEnvName)
	}

	destKey := envKey(srcAppName, newEnvName)
	if _, exists := b.environmentsStore(region)[destKey]; exists {
		return nil, fmt.Errorf("%w: environment %s already exists", ErrAlreadyExists, newEnvName)
	}

	envID := b.nextEnvID(region)
	envARN := arn.Build("elasticbeanstalk", region, b.accountID, "environment/"+srcAppName+"/"+newEnvName)
	cname := newEnvName + "." + region + ".elasticbeanstalk.com"

	env := &Environment{
		ApplicationName:   srcAppName,
		EnvironmentName:   newEnvName,
		EnvironmentID:     envID,
		EnvironmentARN:    envARN,
		SolutionStackName: src.SolutionStackName,
		Description:       src.Description,
		Status:            envStatusReady,
		Health:            envHealthGreen,
		Tier:              src.Tier,
		TierType:          src.TierType,
		TierName:          src.TierName,
		TierVersion:       src.TierVersion,
		CNAME:             cname,
		CNAMEPrefix:       newEnvName,
		LoadBalancerType:  src.LoadBalancerType,
		VPCID:             src.VPCID,
		Subnets:           src.Subnets,
		InstanceProfile:   src.InstanceProfile,
		CustomAMI:         src.CustomAMI,
		OptionSettings:    slices.Clone(src.OptionSettings),
		PlatformARN:       src.PlatformARN,
		TemplateName:      src.TemplateName,
		VersionLabel:      src.VersionLabel,
		OperationsRole:    src.OperationsRole,
		Region:            region,
		Tags:              copyTags(src.Tags),
	}
	b.environmentsStore(region)[destKey] = env
	b.envARNIndexStore(region)[envARN] = destKey

	return cloneEnvironment(env), nil
}

// CreateApplicationVersion creates a new application version.
func (b *InMemoryBackend) CreateApplicationVersion(
	ctx context.Context,
	appName, versionLabel, description string,
	s3Bucket, s3Key string,
	tags map[string]string,
) (*ApplicationVersion, error) {
	return b.CreateApplicationVersionWithParams(ctx, appName, versionLabel, ApplicationVersionParams{
		Description: description,
		S3Bucket:    s3Bucket,
		S3Key:       s3Key,
		Tags:        tags,
		Process:     true,
	})
}

// ApplicationVersionParams holds optional CreateApplicationVersion properties.
type ApplicationVersionParams struct {
	SourceBuildInformation *SourceBuildInformation
	Tags                   map[string]string
	Description            string
	S3Bucket               string
	S3Key                  string
	Process                bool
	AutoCreateApplication  bool
}

// CreateApplicationVersionWithParams creates a new application version with source and processing state.
func (b *InMemoryBackend) CreateApplicationVersionWithParams(
	ctx context.Context,
	appName, versionLabel string,
	params ApplicationVersionParams,
) (*ApplicationVersion, error) {
	b.mu.Lock("CreateApplicationVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := appVersionKey(appName, versionLabel)

	if _, ok := b.appVersionsStore(region)[key]; ok {
		return nil, fmt.Errorf(
			"%w: application version %s already exists",
			ErrAlreadyExists,
			versionLabel,
		)
	}

	vARN := arn.Build("elasticbeanstalk", region, b.accountID,
		"applicationversion/"+appName+"/"+versionLabel)

	if params.AutoCreateApplication {
		if _, ok := b.applicationsStore(region)[appName]; !ok {
			appARN := arn.Build("elasticbeanstalk", region, b.accountID, "application/"+appName)
			b.applicationsStore(region)[appName] = &Application{
				ApplicationName: appName,
				ApplicationARN:  appARN,
				Tags:            map[string]string{},
			}
			b.appARNIndexStore(region)[appARN] = appName
		}
	}

	status := appVersionStatusUnprocessed
	if params.Process {
		status = appVersionStatusProcessed
	}

	ver := &ApplicationVersion{
		ApplicationName:        appName,
		VersionLabel:           versionLabel,
		ApplicationVersionARN:  vARN,
		Description:            params.Description,
		DateCreated:            resourceCreatedAt,
		Status:                 status,
		Process:                params.Process,
		S3Bucket:               params.S3Bucket,
		S3Key:                  params.S3Key,
		SourceBuildInformation: params.SourceBuildInformation,
		Tags:                   copyTags(params.Tags),
	}
	b.appVersionsStore(region)[key] = ver
	b.verARNIndexStore(region)[ver.ApplicationVersionARN] = key

	return cloneApplicationVersion(ver), nil
}

// DescribeApplicationVersions returns application versions, optionally filtered.
// Results are sorted by VersionLabel for deterministic output.
func (b *InMemoryBackend) DescribeApplicationVersions(
	ctx context.Context,
	appName string,
	versionLabels []string,
) []*ApplicationVersion {
	b.mu.RLock("DescribeApplicationVersions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.appVersionsStore(region)

	list := make([]*ApplicationVersion, 0, len(store))

	for _, ver := range store {
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
func (b *InMemoryBackend) DeleteApplicationVersion(ctx context.Context, appName, versionLabel string) error {
	b.mu.Lock("DeleteApplicationVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := appVersionKey(appName, versionLabel)

	if _, ok := b.appVersionsStore(region)[key]; !ok {
		return fmt.Errorf("%w: application version %s not found", ErrNotFound, versionLabel)
	}

	delete(b.verARNIndexStore(region), b.appVersionsStore(region)[key].ApplicationVersionARN)
	delete(b.appVersionsStore(region), key)

	return nil
}

// sortedTagKeys returns the keys of a tags map in sorted order.
func sortedTagKeys(tags map[string]string) []string {
	keys := collections.SortedKeys(tags)

	return keys
}

// ListTagsForResource returns the tags for a resource identified by ARN.
// Tags are returned sorted by key for deterministic output.
func (b *InMemoryBackend) ListTagsForResource(ctx context.Context, resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if tags, ok := b.lookupTagsByARN(region, resourceARN); ok {
		return copyTags(tags), nil
	}

	return nil, fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
}

// UpdateTagsForResource updates tags on a resource identified by ARN.
func (b *InMemoryBackend) UpdateTagsForResource(
	ctx context.Context,
	resourceARN string,
	addTags, removeTags map[string]string,
) error {
	b.mu.Lock("UpdateTagsForResource")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	existing, ok := b.lookupTagsByARN(region, resourceARN)
	if !ok {
		return fmt.Errorf("%w: resource %s not found", ErrNotFound, resourceARN)
	}

	if existing == nil {
		b.ensureTagsByARN(region, resourceARN)
		existing, _ = b.lookupTagsByARN(region, resourceARN)
	}

	maps.Copy(existing, addTags)

	for k := range removeTags {
		delete(existing, k)
	}

	return nil
}

// lookupTagsByARN looks up the tags map for a resource by ARN using O(1) index lookups.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) lookupTagsByARN(region, resourceARN string) (map[string]string, bool) {
	if name, ok := b.appARNIndexStore(region)[resourceARN]; ok {
		return b.applicationsStore(region)[name].Tags, true
	}

	if key, ok := b.envARNIndexStore(region)[resourceARN]; ok {
		return b.environmentsStore(region)[key].Tags, true
	}

	if key, ok := b.verARNIndexStore(region)[resourceARN]; ok {
		return b.appVersionsStore(region)[key].Tags, true
	}

	return nil, false
}

// ensureTagsByARN ensures a resource has an initialised tags map.
// Caller must hold the write lock.
func (b *InMemoryBackend) ensureTagsByARN(region, resourceARN string) {
	if name, ok := b.appARNIndexStore(region)[resourceARN]; ok {
		if b.applicationsStore(region)[name].Tags == nil {
			b.applicationsStore(region)[name].Tags = make(map[string]string)
		}

		return
	}

	if key, ok := b.envARNIndexStore(region)[resourceARN]; ok {
		if b.environmentsStore(region)[key].Tags == nil {
			b.environmentsStore(region)[key].Tags = make(map[string]string)
		}

		return
	}

	if key, ok := b.verARNIndexStore(region)[resourceARN]; ok {
		if b.appVersionsStore(region)[key].Tags == nil {
			b.appVersionsStore(region)[key].Tags = make(map[string]string)
		}
	}
}

// Reset clears all backend state, resetting to an empty store.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.applications = make(map[string]map[string]*Application)
	b.environments = make(map[string]map[string]*Environment)
	b.appVersions = make(map[string]map[string]*ApplicationVersion)
	b.configTemplates = make(map[string]map[string]*ConfigurationTemplate)
	b.platformVersions = make(map[string]map[string]*PlatformVersion)
	b.managedActionHistory = make(map[string]map[string][]*ManagedActionHistory)
	b.events = make(map[string][]*EventRecord)
	b.appARNIndex = make(map[string]map[string]string)
	b.envARNIndex = make(map[string]map[string]string)
	b.verARNIndex = make(map[string]map[string]string)
	b.envCounters = make(map[string]int)
}

// --- New operations ---

// AbortEnvironmentUpdate aborts an in-progress environment configuration update.
// This is a no-op in the in-memory backend since updates complete instantly.
func (b *InMemoryBackend) AbortEnvironmentUpdate(_ context.Context, _ string) error {
	return nil
}

// ApplyEnvironmentManagedAction applies a scheduled managed action immediately.
// Records the action in the managed action history (improvement #4).
func (b *InMemoryBackend) ApplyEnvironmentManagedAction(ctx context.Context, envName, actionID string) error {
	b.mu.Lock("ApplyEnvironmentManagedAction")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	item := &ManagedActionHistory{
		ActionID:          actionID,
		ActionType:        "InstanceRefresh",
		ActionDescription: "Managed action applied",
		Status:            "Succeeded",
		FinishedTime:      managedActionFinishedTime,
	}
	store := b.managedActionHistoryStore(region)
	store[envName] = append(store[envName], item)

	return nil
}

// AddManagedActionHistory records a managed action history item for an environment (improvement #4).
func (b *InMemoryBackend) AddManagedActionHistory(
	ctx context.Context,
	envName, actionID, actionType, actionDesc, status string,
) {
	b.mu.Lock("AddManagedActionHistory")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	item := &ManagedActionHistory{
		ActionID:          actionID,
		ActionType:        actionType,
		ActionDescription: actionDesc,
		Status:            status,
		FinishedTime:      managedActionFinishedTime,
	}
	store := b.managedActionHistoryStore(region)
	store[envName] = append(store[envName], item)
}

// DescribeEnvironmentManagedActionHistory returns stored managed action history for an environment (improvement #4).
func (b *InMemoryBackend) DescribeEnvironmentManagedActionHistory(
	ctx context.Context,
	envName string,
) []*ManagedActionHistory {
	b.mu.RLock("DescribeEnvironmentManagedActionHistory")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	items := b.managedActionHistoryStore(region)[envName]

	if len(items) == 0 {
		return []*ManagedActionHistory{}
	}

	out := make([]*ManagedActionHistory, len(items))
	for i, item := range items {
		cp := *item
		out[i] = &cp
	}

	return out
}

// AssociateEnvironmentOperationsRole associates an operations IAM role with an environment.
func (b *InMemoryBackend) AssociateEnvironmentOperationsRole(
	ctx context.Context,
	envName, role string,
) error {
	b.mu.Lock("AssociateEnvironmentOperationsRole")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	for _, env := range b.environmentsStore(region) {
		if env.EnvironmentName == envName {
			env.OperationsRole = role

			return nil
		}
	}

	return fmt.Errorf("%w: environment %s not found", ErrNotFound, envName)
}

// CheckDNSAvailability checks whether the specified CNAME prefix is available.
// Returns available=true when no existing environment in the request region uses that prefix.
func (b *InMemoryBackend) CheckDNSAvailability(ctx context.Context, cnamePrefix string) (bool, string) {
	b.mu.RLock("CheckDNSAvailability")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	fqcname := cnamePrefix + "." + region + ".elasticbeanstalk.com"

	for _, env := range b.environmentsStore(region) {
		if env.EnvironmentName == cnamePrefix || env.CNAME == fqcname {
			return false, fqcname
		}
	}

	return true, fqcname
}

// ComposeEnvironments returns existing environments for an application.
// In a real deployment this would create multiple environments; the stub
// returns the already-running environments for the given application.
// Results are sorted by EnvironmentName for deterministic output.
func (b *InMemoryBackend) ComposeEnvironments(ctx context.Context, appName string) []*Environment {
	b.mu.RLock("ComposeEnvironments")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	list := make([]*Environment, 0, len(b.environmentsStore(region)))

	for _, env := range b.environmentsStore(region) {
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
	ctx context.Context,
	appName, templateName, description, solutionStack string,
	tags map[string]string,
) (*ConfigurationTemplate, error) {
	b.mu.Lock("CreateConfigurationTemplate")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := configTemplateKey(appName, templateName)

	if _, ok := b.configTemplatesStore(region)[key]; ok {
		return nil, fmt.Errorf(
			"%w: configuration template %s already exists",
			ErrAlreadyExists,
			templateName,
		)
	}

	tmpl := &ConfigurationTemplate{
		ApplicationName:   appName,
		TemplateName:      templateName,
		Description:       description,
		SolutionStackName: solutionStack,
		Tags:              copyTags(tags),
	}
	b.configTemplatesStore(region)[key] = tmpl

	return cloneConfigurationTemplate(tmpl), nil
}

// DescribeConfigurationTemplates returns all configuration templates for an application (improvement #17).
func (b *InMemoryBackend) DescribeConfigurationTemplates(ctx context.Context, appName string) []*ConfigurationTemplate {
	b.mu.RLock("DescribeConfigurationTemplates")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	list := make([]*ConfigurationTemplate, 0, len(b.configTemplatesStore(region)))

	for _, tmpl := range b.configTemplatesStore(region) {
		if appName == "" || tmpl.ApplicationName == appName {
			list = append(list, cloneConfigurationTemplate(tmpl))
		}
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].TemplateName < list[j].TemplateName
	})

	return list
}

// CreatePlatformVersion creates a new custom platform version.
func (b *InMemoryBackend) CreatePlatformVersion(
	ctx context.Context,
	platformName, platformVersion string,
	tags map[string]string,
) (*PlatformVersion, error) {
	b.mu.Lock("CreatePlatformVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	platformARN := arn.Build("elasticbeanstalk", region, "", "platform/"+platformName+"/"+platformVersion)

	if _, ok := b.platformVersionsStore(region)[platformARN]; ok {
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
		PlatformStatus:  envStatusReady,
		Tags:            copyTags(tags),
	}
	b.platformVersionsStore(region)[platformARN] = pv

	return clonePlatformVersion(pv), nil
}

// CreateStorageLocation returns the S3 bucket used for storing Elastic Beanstalk data.
// The bucket name is fixed per region and account, and creation is idempotent.
func (b *InMemoryBackend) CreateStorageLocation(ctx context.Context) string {
	region := getRegion(ctx, b.region)

	return "elasticbeanstalk-" + region + "-" + b.accountID
}

// DeleteConfigurationTemplate removes a configuration template.
func (b *InMemoryBackend) DeleteConfigurationTemplate(ctx context.Context, appName, templateName string) error {
	b.mu.Lock("DeleteConfigurationTemplate")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := configTemplateKey(appName, templateName)

	if _, ok := b.configTemplatesStore(region)[key]; !ok {
		return fmt.Errorf("%w: configuration template %s not found", ErrNotFound, templateName)
	}

	delete(b.configTemplatesStore(region), key)

	return nil
}

// DeleteEnvironmentConfiguration deletes the draft configuration associated with an environment.
// This is a no-op in the in-memory backend.
func (b *InMemoryBackend) DeleteEnvironmentConfiguration(_ context.Context, _, _ string) error {
	return nil
}

// DeletePlatformVersion removes a platform version by ARN and returns the deleted version.
func (b *InMemoryBackend) DeletePlatformVersion(ctx context.Context, platformARN string) (*PlatformVersion, error) {
	b.mu.Lock("DeletePlatformVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	pv, ok := b.platformVersionsStore(region)[platformARN]
	if !ok {
		return nil, fmt.Errorf("%w: platform version %s not found", ErrNotFound, platformARN)
	}

	out := clonePlatformVersion(pv)
	delete(b.platformVersionsStore(region), platformARN)

	return out, nil
}

// DescribePlatformVersion returns a platform version by ARN.
func (b *InMemoryBackend) DescribePlatformVersion(ctx context.Context, platformARN string) (*PlatformVersion, error) {
	b.mu.RLock("DescribePlatformVersion")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	pv, ok := b.platformVersionsStore(region)[platformARN]
	if !ok {
		return nil, fmt.Errorf("%w: platform version %s not found", ErrNotFound, platformARN)
	}

	return clonePlatformVersion(pv), nil
}

// DescribeEnvironmentHealth returns the health and status of an environment by name.
func (b *InMemoryBackend) DescribeEnvironmentHealth(ctx context.Context, envName string) (string, string) {
	b.mu.RLock("DescribeEnvironmentHealth")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	for _, env := range b.environmentsStore(region) {
		if env.EnvironmentName == envName {
			return env.Health, env.Status
		}
	}

	return "Grey", "Terminated"
}

// DisassociateEnvironmentOperationsRole removes the operations role from an environment.
func (b *InMemoryBackend) DisassociateEnvironmentOperationsRole(ctx context.Context, envName string) error {
	b.mu.Lock("DisassociateEnvironmentOperationsRole")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	for _, env := range b.environmentsStore(region) {
		if env.EnvironmentName == envName {
			env.OperationsRole = ""

			return nil
		}
	}

	return fmt.Errorf("%w: environment %s not found", ErrNotFound, envName)
}

// ListPlatformVersions returns all stored platform versions sorted by ARN.
func (b *InMemoryBackend) ListPlatformVersions(ctx context.Context) []*PlatformVersion {
	b.mu.RLock("ListPlatformVersions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	list := make([]*PlatformVersion, 0, len(b.platformVersionsStore(region)))

	for _, pv := range b.platformVersionsStore(region) {
		list = append(list, clonePlatformVersion(pv))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].PlatformArn < list[j].PlatformArn
	})

	return list
}

// SwapEnvironmentCNAMEs swaps the CNAME values between two environments (improvement #10).
func (b *InMemoryBackend) SwapEnvironmentCNAMEs(ctx context.Context, sourceEnvName, destEnvName string) error {
	b.mu.Lock("SwapEnvironmentCNAMEs")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	var srcEnv, dstEnv *Environment

	for _, env := range b.environmentsStore(region) {
		switch env.EnvironmentName {
		case sourceEnvName:
			srcEnv = env
		case destEnvName:
			dstEnv = env
		}
	}

	if srcEnv == nil {
		return fmt.Errorf("%w: source environment %s not found", ErrNotFound, sourceEnvName)
	}

	if dstEnv == nil {
		return fmt.Errorf("%w: destination environment %s not found", ErrNotFound, destEnvName)
	}

	srcEnv.CNAME, dstEnv.CNAME = dstEnv.CNAME, srcEnv.CNAME

	return nil
}

// UpdateApplicationVersion updates an application version's description.
func (b *InMemoryBackend) UpdateApplicationVersion(
	ctx context.Context,
	appName, versionLabel, description string,
) (*ApplicationVersion, error) {
	b.mu.Lock("UpdateApplicationVersion")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := appVersionKey(appName, versionLabel)

	ver, ok := b.appVersionsStore(region)[key]
	if !ok {
		return nil, fmt.Errorf("%w: application version %s not found", ErrNotFound, versionLabel)
	}

	ver.Description = description

	return cloneApplicationVersion(ver), nil
}

// UpdateConfigurationTemplate updates a configuration template's description.
func (b *InMemoryBackend) UpdateConfigurationTemplate(
	ctx context.Context,
	appName, templateName, description string,
) (*ConfigurationTemplate, error) {
	b.mu.Lock("UpdateConfigurationTemplate")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	key := configTemplateKey(appName, templateName)

	tmpl, ok := b.configTemplatesStore(region)[key]
	if !ok {
		return nil, fmt.Errorf("%w: configuration template %s not found", ErrNotFound, templateName)
	}

	tmpl.Description = description

	return cloneConfigurationTemplate(tmpl), nil
}

// --- Event helpers ---

// appendEvent appends an event record to the backend's event log.
// Caller must hold at least a write lock.
func (b *InMemoryBackend) appendEvent(region, appName, envName, message, severity string) {
	b.events[region] = append(b.eventsSlice(region), &EventRecord{
		ApplicationName: appName,
		EnvironmentName: envName,
		EventDate:       resourceCreatedAt,
		Message:         message,
		Severity:        severity,
	})
}

// DescribeEvents returns event records filtered by optional application and environment name.
// The most recent events are returned first (reverse insertion order).
func (b *InMemoryBackend) DescribeEvents(ctx context.Context, appName, envName string) []*EventRecord {
	b.mu.RLock("DescribeEvents")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	events := b.eventsSlice(region)

	out := make([]*EventRecord, 0, len(events))

	for _, e := range slices.Backward(events) {
		if appName != "" && e.ApplicationName != appName {
			continue
		}

		if envName != "" && e.EnvironmentName != envName {
			continue
		}

		cp := *e
		out = append(out, &cp)
	}

	return out
}

// --- Key helpers ---

// envKey returns the map key for an environment (applicationName + ":" + environmentName).
func envKey(appName, envName string) string {
	return appName + ":" + envName
}

// appVersionKey returns the map key for an application version.
func appVersionKey(appName, versionLabel string) string {
	return appName + ":" + versionLabel
}

// --- Seed helpers (used in tests via export_test.go) ---

// addApplicationInternal seeds an application directly into the backend, bypassing validation.
// Caller must hold the write lock.
func (b *InMemoryBackend) addApplicationInternal(region string, app *Application) {
	b.applicationsStore(region)[app.ApplicationName] = cloneApplication(app)
	b.appARNIndexStore(region)[app.ApplicationARN] = app.ApplicationName
}

// addEnvironmentInternal seeds an environment directly into the backend, bypassing validation.
// Caller must hold the write lock.
func (b *InMemoryBackend) addEnvironmentInternal(region string, env *Environment) {
	key := envKey(env.ApplicationName, env.EnvironmentName)
	b.environmentsStore(region)[key] = cloneEnvironment(env)
	b.envARNIndexStore(region)[env.EnvironmentARN] = key
}

// addAppVersionInternal seeds an application version directly into the backend, bypassing validation.
// Caller must hold the write lock.
func (b *InMemoryBackend) addAppVersionInternal(region string, ver *ApplicationVersion) {
	key := appVersionKey(ver.ApplicationName, ver.VersionLabel)
	b.appVersionsStore(region)[key] = cloneApplicationVersion(ver)
	b.verARNIndexStore(region)[ver.ApplicationVersionARN] = key
}

// addConfigTemplateInternal seeds a configuration template directly into the backend.
// Caller must hold the write lock.
func (b *InMemoryBackend) addConfigTemplateInternal(region string, tmpl *ConfigurationTemplate) {
	key := configTemplateKey(tmpl.ApplicationName, tmpl.TemplateName)
	b.configTemplatesStore(region)[key] = cloneConfigurationTemplate(tmpl)
}

// addPlatformVersionInternal seeds a platform version directly into the backend.
// Caller must hold the write lock.
func (b *InMemoryBackend) addPlatformVersionInternal(region string, pv *PlatformVersion) {
	b.platformVersionsStore(region)[pv.PlatformArn] = clonePlatformVersion(pv)
}
