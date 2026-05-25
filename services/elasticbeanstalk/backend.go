package elasticbeanstalk

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

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
)

// Application represents an Elastic Beanstalk application.
type Application struct {
	Tags                         map[string]string `json:"tags,omitempty"`
	ApplicationName              string            `json:"applicationName"`
	ApplicationARN               string            `json:"applicationArn"`
	Description                  string            `json:"description,omitempty"`
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

// InMemoryBackend stores AWS Elastic Beanstalk state in memory.
type InMemoryBackend struct {
	applications         map[string]*Application
	environments         map[string]*Environment
	appVersions          map[string]*ApplicationVersion
	configTemplates      map[string]*ConfigurationTemplate  // configTemplateKey → template
	platformVersions     map[string]*PlatformVersion        // platformARN → version
	managedActionHistory map[string][]*ManagedActionHistory // envName → history items
	appARNIndex          map[string]string                  // ARN → app name
	envARNIndex          map[string]string                  // ARN → envKey
	verARNIndex          map[string]string                  // ARN → appVersionKey
	mu                   *lockmetrics.RWMutex
	accountID            string
	region               string
	storageLocation      string
	envCounter           int
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
		applications:         make(map[string]*Application),
		environments:         make(map[string]*Environment),
		appVersions:          make(map[string]*ApplicationVersion),
		configTemplates:      make(map[string]*ConfigurationTemplate),
		platformVersions:     make(map[string]*PlatformVersion),
		managedActionHistory: make(map[string][]*ManagedActionHistory),
		appARNIndex:          make(map[string]string),
		envARNIndex:          make(map[string]string),
		verARNIndex:          make(map[string]string),
		accountID:            accountID,
		region:               region,
		storageLocation:      "elasticbeanstalk-" + region + "-" + accountID,
		mu:                   lockmetrics.New("elasticbeanstalk"),
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

// UpdateApplicationResourceLifecycle stores the resource lifecycle service role on the application (improvement #7).
func (b *InMemoryBackend) UpdateApplicationResourceLifecycle(appName, serviceRole string) (*Application, error) {
	b.mu.Lock("UpdateApplicationResourceLifecycle")
	defer b.mu.Unlock()

	app, ok := b.applications[appName]
	if !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	app.ResourceLifecycleServiceRole = serviceRole

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
	appName, envName, solutionStack, description string,
	tags map[string]string,
	params CreateEnvironmentParams,
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
	cname := cnamePrefix + "." + b.region + ".elasticbeanstalk.com"

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
	return b.UpdateEnvironmentWithParams(appName, envName, UpdateEnvironmentParams{
		Description:       description,
		SolutionStackName: solutionStack,
	})
}

// UpdateEnvironmentWithParams applies all mutable environment properties.
func (b *InMemoryBackend) UpdateEnvironmentWithParams(
	appName, envName string,
	params UpdateEnvironmentParams,
) (*Environment, error) {
	b.mu.Lock("UpdateEnvironment")
	defer b.mu.Unlock()

	key := envKey(appName, envName)

	env, ok := b.environments[key]
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

	env.OptionSettings = updateOptionSettings(env.OptionSettings, params.OptionSettings, params.OptionsToRemove)

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

// CloneEnvironment creates a new environment by copying an existing one (improvement #9).
func (b *InMemoryBackend) CloneEnvironment(srcAppName, srcEnvName, newEnvName string) (*Environment, error) {
	b.mu.Lock("CloneEnvironment")
	defer b.mu.Unlock()

	srcKey := envKey(srcAppName, srcEnvName)

	src, ok := b.environments[srcKey]
	if !ok {
		return nil, fmt.Errorf("%w: source environment %s not found", ErrNotFound, srcEnvName)
	}

	destKey := envKey(srcAppName, newEnvName)
	if _, exists := b.environments[destKey]; exists {
		return nil, fmt.Errorf("%w: environment %s already exists", ErrAlreadyExists, newEnvName)
	}

	b.envCounter++
	envID := fmt.Sprintf("e-%08d", b.envCounter)
	envARN := arn.Build("elasticbeanstalk", b.region, b.accountID, "environment/"+srcAppName+"/"+newEnvName)
	cname := newEnvName + "." + b.region + ".elasticbeanstalk.com"

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
		Tags:              copyTags(src.Tags),
	}
	b.environments[destKey] = env
	b.envARNIndex[envARN] = destKey

	return cloneEnvironment(env), nil
}

// CreateApplicationVersion creates a new application version.
func (b *InMemoryBackend) CreateApplicationVersion(
	appName, versionLabel, description string,
	s3Bucket, s3Key string,
	tags map[string]string,
) (*ApplicationVersion, error) {
	return b.CreateApplicationVersionWithParams(appName, versionLabel, ApplicationVersionParams{
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
	appName, versionLabel string,
	params ApplicationVersionParams,
) (*ApplicationVersion, error) {
	b.mu.Lock("CreateApplicationVersion")
	defer b.mu.Unlock()

	key := appVersionKey(appName, versionLabel)
	if _, ok := b.appVersions[key]; ok {
		return nil, fmt.Errorf("%w: application version %s already exists", ErrAlreadyExists, versionLabel)
	}

	vARN := arn.Build("elasticbeanstalk", b.region, b.accountID,
		"applicationversion/"+appName+"/"+versionLabel)

	if params.AutoCreateApplication {
		if _, ok := b.applications[appName]; !ok {
			appARN := arn.Build("elasticbeanstalk", b.region, b.accountID, "application/"+appName)
			b.applications[appName] = &Application{
				ApplicationName: appName,
				ApplicationARN:  appARN,
				Tags:            map[string]string{},
			}
			b.appARNIndex[appARN] = appName
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
		Status:                 status,
		Process:                params.Process,
		S3Bucket:               params.S3Bucket,
		S3Key:                  params.S3Key,
		SourceBuildInformation: params.SourceBuildInformation,
		Tags:                   copyTags(params.Tags),
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
	b.managedActionHistory = make(map[string][]*ManagedActionHistory)
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
// Records the action in the managed action history (improvement #4).
func (b *InMemoryBackend) ApplyEnvironmentManagedAction(envName, actionID string) error {
	b.mu.Lock("ApplyEnvironmentManagedAction")
	defer b.mu.Unlock()

	item := &ManagedActionHistory{
		ActionID:          actionID,
		ActionType:        "InstanceRefresh",
		ActionDescription: "Managed action applied",
		Status:            "Succeeded",
		FinishedTime:      managedActionFinishedTime,
	}
	b.managedActionHistory[envName] = append(b.managedActionHistory[envName], item)

	return nil
}

// AddManagedActionHistory records a managed action history item for an environment (improvement #4).
func (b *InMemoryBackend) AddManagedActionHistory(envName, actionID, actionType, actionDesc, status string) {
	b.mu.Lock("AddManagedActionHistory")
	defer b.mu.Unlock()

	item := &ManagedActionHistory{
		ActionID:          actionID,
		ActionType:        actionType,
		ActionDescription: actionDesc,
		Status:            status,
		FinishedTime:      managedActionFinishedTime,
	}
	b.managedActionHistory[envName] = append(b.managedActionHistory[envName], item)
}

// DescribeEnvironmentManagedActionHistory returns stored managed action history for an environment (improvement #4).
func (b *InMemoryBackend) DescribeEnvironmentManagedActionHistory(envName string) []*ManagedActionHistory {
	b.mu.RLock("DescribeEnvironmentManagedActionHistory")
	defer b.mu.RUnlock()

	items := b.managedActionHistory[envName]
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
func (b *InMemoryBackend) ComposeEnvironments(appName string) []*Environment {
	b.mu.RLock("ComposeEnvironments")
	defer b.mu.RUnlock()

	list := make([]*Environment, 0, len(b.environments))

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

// DescribeConfigurationTemplates returns all configuration templates for an application (improvement #17).
func (b *InMemoryBackend) DescribeConfigurationTemplates(appName string) []*ConfigurationTemplate {
	b.mu.RLock("DescribeConfigurationTemplates")
	defer b.mu.RUnlock()

	list := make([]*ConfigurationTemplate, 0, len(b.configTemplates))

	for _, tmpl := range b.configTemplates {
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
		PlatformStatus:  envStatusReady,
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

// DeletePlatformVersion removes a platform version by ARN and returns the deleted version.
func (b *InMemoryBackend) DeletePlatformVersion(platformARN string) (*PlatformVersion, error) {
	b.mu.Lock("DeletePlatformVersion")
	defer b.mu.Unlock()

	pv, ok := b.platformVersions[platformARN]
	if !ok {
		return nil, fmt.Errorf("%w: platform version %s not found", ErrNotFound, platformARN)
	}

	out := clonePlatformVersion(pv)
	delete(b.platformVersions, platformARN)

	return out, nil
}

// DescribePlatformVersion returns a platform version by ARN.
func (b *InMemoryBackend) DescribePlatformVersion(platformARN string) (*PlatformVersion, error) {
	b.mu.RLock("DescribePlatformVersion")
	defer b.mu.RUnlock()

	pv, ok := b.platformVersions[platformARN]
	if !ok {
		return nil, fmt.Errorf("%w: platform version %s not found", ErrNotFound, platformARN)
	}

	return clonePlatformVersion(pv), nil
}

// DescribeEnvironmentHealth returns the health and status of an environment by name.
func (b *InMemoryBackend) DescribeEnvironmentHealth(envName string) (string, string) {
	b.mu.RLock("DescribeEnvironmentHealth")
	defer b.mu.RUnlock()

	for _, env := range b.environments {
		if env.EnvironmentName == envName {
			return env.Health, env.Status
		}
	}

	return "Grey", "Terminated"
}

// DisassociateEnvironmentOperationsRole removes the operations role from an environment.
func (b *InMemoryBackend) DisassociateEnvironmentOperationsRole(envName string) error {
	b.mu.Lock("DisassociateEnvironmentOperationsRole")
	defer b.mu.Unlock()

	for _, env := range b.environments {
		if env.EnvironmentName == envName {
			env.OperationsRole = ""

			return nil
		}
	}

	return fmt.Errorf("%w: environment %s not found", ErrNotFound, envName)
}

// ListPlatformVersions returns all stored platform versions sorted by ARN.
func (b *InMemoryBackend) ListPlatformVersions() []*PlatformVersion {
	b.mu.RLock("ListPlatformVersions")
	defer b.mu.RUnlock()

	list := make([]*PlatformVersion, 0, len(b.platformVersions))

	for _, pv := range b.platformVersions {
		list = append(list, clonePlatformVersion(pv))
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].PlatformArn < list[j].PlatformArn
	})

	return list
}

// SwapEnvironmentCNAMEs swaps the CNAME values between two environments (improvement #10).
func (b *InMemoryBackend) SwapEnvironmentCNAMEs(sourceEnvName, destEnvName string) error {
	b.mu.Lock("SwapEnvironmentCNAMEs")
	defer b.mu.Unlock()

	var srcEnv, dstEnv *Environment

	for _, env := range b.environments {
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
	appName, versionLabel, description string,
) (*ApplicationVersion, error) {
	b.mu.Lock("UpdateApplicationVersion")
	defer b.mu.Unlock()

	key := appVersionKey(appName, versionLabel)

	ver, ok := b.appVersions[key]
	if !ok {
		return nil, fmt.Errorf("%w: application version %s not found", ErrNotFound, versionLabel)
	}

	ver.Description = description

	return cloneApplicationVersion(ver), nil
}

// UpdateConfigurationTemplate updates a configuration template's description.
func (b *InMemoryBackend) UpdateConfigurationTemplate(
	appName, templateName, description string,
) (*ConfigurationTemplate, error) {
	b.mu.Lock("UpdateConfigurationTemplate")
	defer b.mu.Unlock()

	key := configTemplateKey(appName, templateName)

	tmpl, ok := b.configTemplates[key]
	if !ok {
		return nil, fmt.Errorf("%w: configuration template %s not found", ErrNotFound, templateName)
	}

	tmpl.Description = description

	return cloneConfigurationTemplate(tmpl), nil
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
