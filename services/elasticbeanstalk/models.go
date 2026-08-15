package elasticbeanstalk

import (
	"maps"
	"slices"
	"time"
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
	// eventSeverityInfo is the severity level for informational events.
	eventSeverityInfo = "INFO"
	// maxEventsPerRegion caps the events slice to prevent unbounded growth.
	maxEventsPerRegion = 1000
	// defaultConfigTemplateName is the configuration template AWS auto-creates
	// alongside every new application (see CreateApplication's documented
	// behavior: "Creates an application that has one configuration template
	// named default"; the API's own example response renders it capitalized
	// as "Default" -- see
	// https://docs.aws.amazon.com/elasticbeanstalk/latest/api/API_CreateApplication.html).
	defaultConfigTemplateName = "Default"
	// configDeploymentStatusDeployed is the ConfigurationSettingsDescription
	// DeploymentStatus value used for a configuration set currently attached
	// to a running environment; this backend applies environment updates
	// synchronously, so an environment's live configuration set is always
	// "deployed" and never observed in "pending"/"failed" transition states.
	configDeploymentStatusDeployed = "deployed"
)

// Application represents an Elastic Beanstalk application.
type Application struct {
	Tags                         map[string]string `json:"tags,omitempty"`
	ApplicationName              string            `json:"applicationName"`
	ApplicationARN               string            `json:"applicationArn"`
	Description                  string            `json:"description,omitempty"`
	DateCreated                  string            `json:"dateCreated,omitempty"`
	DateUpdated                  string            `json:"dateUpdated,omitempty"`
	ResourceLifecycleServiceRole string            `json:"resourceLifecycleServiceRole,omitempty"`
	// region is the store.Table composite-key qualifier (see regionKey in
	// store.go); it is unexported so it is never marshaled by a plain
	// json.Marshal(Application) and is instead carried through persistence
	// via regionalDTO (see persistence.go).
	region string
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
	DateUpdated       string            `json:"dateUpdated,omitempty"`
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
	DateUpdated            string                  `json:"dateUpdated,omitempty"`
	Status                 string                  `json:"status"`
	S3Bucket               string                  `json:"s3Bucket,omitempty"`
	S3Key                  string                  `json:"s3Key,omitempty"`
	// region is the store.Table composite-key qualifier (see regionKey in
	// store.go); unexported, carried through persistence via regionalDTO
	// (see persistence.go).
	region  string
	Process bool `json:"process"`
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
	DateCreated       string            `json:"dateCreated,omitempty"`
	DateUpdated       string            `json:"dateUpdated,omitempty"`
	SolutionStackName string            `json:"solutionStackName,omitempty"`
	PlatformArn       string            `json:"platformArn,omitempty"`
	region            string
	OptionSettings    []OptionSetting `json:"optionSettings,omitempty"`
}

// PlatformVersion represents an Elastic Beanstalk platform version.
type PlatformVersion struct {
	Tags            map[string]string `json:"tags,omitempty"`
	PlatformArn     string            `json:"platformArn"`
	PlatformName    string            `json:"platformName"`
	PlatformVersion string            `json:"platformVersion"`
	PlatformStatus  string            `json:"platformStatus"`
	// region is the store.Table composite-key qualifier (see regionKey in
	// store.go); unexported, carried through persistence via regionalDTO
	// (see persistence.go).
	region string
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
	PlatformArn     string `json:"platformArn,omitempty"`
	TemplateName    string `json:"templateName,omitempty"`
	VersionLabel    string `json:"versionLabel,omitempty"`
	EventDate       string `json:"eventDate"`
	Message         string `json:"message"`
	Severity        string `json:"severity"`
}

// copyTags creates a shallow copy of the given tags map.
func copyTags(tags map[string]string) map[string]string {
	out := make(map[string]string, len(tags))
	maps.Copy(out, tags)

	return out
}

// nowISO8601 returns the current UTC time as an ISO 8601 string.
func nowISO8601() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05Z")
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
	cp.OptionSettings = slices.Clone(tmpl.OptionSettings)

	return &cp
}

// clonePlatformVersion returns a deep copy of the given PlatformVersion.
func clonePlatformVersion(pv *PlatformVersion) *PlatformVersion {
	cp := *pv
	cp.Tags = copyTags(pv.Tags)

	return &cp
}
