package elasticbeanstalk

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
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
	// eventSeverityInfo is the severity level for informational events.
	eventSeverityInfo = "INFO"
	// maxEventsPerRegion caps the events slice to prevent unbounded growth.
	maxEventsPerRegion = 1000
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
	// backend.go); it is unexported so it is never marshaled by a plain
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
	// backend.go); unexported, carried through persistence via regionalDTO
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
	// region is the store.Table composite-key qualifier (see regionKey in
	// backend.go); unexported, carried through persistence via regionalDTO
	// (see persistence.go).
	region string
}

// PlatformVersion represents an Elastic Beanstalk platform version.
type PlatformVersion struct {
	Tags            map[string]string `json:"tags,omitempty"`
	PlatformArn     string            `json:"platformArn"`
	PlatformName    string            `json:"platformName"`
	PlatformVersion string            `json:"platformVersion"`
	PlatformStatus  string            `json:"platformStatus"`
	// region is the store.Table composite-key qualifier (see regionKey in
	// backend.go); unexported, carried through persistence via regionalDTO
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
	EventDate       string `json:"eventDate"`
	Message         string `json:"message"`
	Severity        string `json:"severity"`
}

// InMemoryBackend stores AWS Elastic Beanstalk state in memory.
//
// applications, environments, appVersions, configTemplates, and
// platformVersions were previously nested by region (outer key = region,
// e.g. map[string]map[string]*Application). Phase 3.3 of the datalayer
// refactor replaces each with a flat *store.Table, keyed by the composite
// "region|id" string (see regionKey), with companion *store.Index instances
// for per-region scans and ARN/name/CNAME reverse lookups -- see
// store_setup.go for the full rationale and every keyFn.
// managedActionHistory, events, and envCounters are deliberately NOT
// converted: they are slice- or scalar-valued maps with no single-value-
// per-key shape to model as a store.Table, so they remain plain region-nested
// maps, unchanged by this refactor.
type InMemoryBackend struct {
	applications             *store.Table[Application]
	applicationsByRegion     *store.Index[Application]
	applicationsByARN        *store.Index[Application]
	environments             *store.Table[Environment]
	environmentsByRegion     *store.Index[Environment]
	environmentsByARN        *store.Index[Environment]
	environmentsByName       *store.Index[Environment]
	environmentsByCNAME      *store.Index[Environment]
	appVersions              *store.Table[ApplicationVersion]
	appVersionsByRegion      *store.Index[ApplicationVersion]
	appVersionsByARN         *store.Index[ApplicationVersion]
	configTemplates          *store.Table[ConfigurationTemplate]
	configTemplatesByRegion  *store.Index[ConfigurationTemplate]
	platformVersions         *store.Table[PlatformVersion]
	platformVersionsByRegion *store.Index[PlatformVersion]
	registry                 *store.Registry
	managedActionHistory     map[string]map[string][]*ManagedActionHistory // region → envName → history items
	events                   map[string][]*EventRecord                     // region → events
	envCounters              map[string]int                                // region → counter
	mu                       *lockmetrics.RWMutex
	accountID                string
	region                   string // default region
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
	return appName + "\x00" + templateName
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		managedActionHistory: make(map[string]map[string][]*ManagedActionHistory),
		events:               make(map[string][]*EventRecord),
		envCounters:          make(map[string]int),
		accountID:            accountID,
		region:               region,
		mu:                   lockmetrics.New("elasticbeanstalk"),
		registry:             store.NewRegistry(),
	}
	registerAllTables(b)
	b.initRegion(region)

	return b
}

// initRegion pre-initializes the raw (non-store.Table) per-region sub-map so
// that managedActionHistoryStore never writes under an RLock (which would
// race with parallel readers). The five store.Table-backed collections need
// no such pre-initialization: their composite keys are created lazily and
// safely by store.Table itself.
func (b *InMemoryBackend) initRegion(region string) {
	if b.managedActionHistory[region] == nil {
		b.managedActionHistory[region] = make(map[string][]*ManagedActionHistory)
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// regionKey builds the composite store.Table primary key ("region|id") shared
// by every region-qualified table registered in store_setup.go.
func regionKey(region, id string) string { return region + "|" + id }

// --- store.Table/Index helpers. Callers must hold b.mu. ---

func (b *InMemoryBackend) applicationGet(region, name string) (*Application, bool) {
	return b.applications.Get(regionKey(region, name))
}

func (b *InMemoryBackend) applicationPut(v *Application) { b.applications.Put(v) }

func (b *InMemoryBackend) applicationDelete(region, name string) {
	b.applications.Delete(regionKey(region, name))
}

func (b *InMemoryBackend) applicationsInRegion(region string) []*Application {
	return b.applicationsByRegion.Get(region)
}

// applicationByARN looks up an application by ARN, scoped to region: an
// application created in one region must never resolve when queried from
// another (see TestEBTagRegionIsolation), so the index key is the composite
// "region|ARN", not ARN alone.
func (b *InMemoryBackend) applicationByARN(region, resourceARN string) (*Application, bool) {
	list := b.applicationsByARN.Get(regionKey(region, resourceARN))
	if len(list) == 0 {
		return nil, false
	}

	return list[0], true
}

func (b *InMemoryBackend) environmentGet(region, appName, envName string) (*Environment, bool) {
	return b.environments.Get(regionKey(region, envKey(appName, envName)))
}

func (b *InMemoryBackend) environmentPut(v *Environment) { b.environments.Put(v) }

func (b *InMemoryBackend) environmentDeleteKey(region, appName, envName string) {
	b.environments.Delete(regionKey(region, envKey(appName, envName)))
}

func (b *InMemoryBackend) environmentsInRegion(region string) []*Environment {
	return b.environmentsByRegion.Get(region)
}

func (b *InMemoryBackend) environmentByARN(region, resourceARN string) (*Environment, bool) {
	list := b.environmentsByARN.Get(regionKey(region, resourceARN))
	if len(list) == 0 {
		return nil, false
	}

	return list[0], true
}

func (b *InMemoryBackend) environmentByName(region, envName string) (*Environment, bool) {
	list := b.environmentsByName.Get(regionKey(region, envName))
	if len(list) == 0 {
		return nil, false
	}

	return list[0], true
}

func (b *InMemoryBackend) environmentCNAMETaken(region, cname string) bool {
	return len(b.environmentsByCNAME.Get(regionKey(region, cname))) > 0
}

func (b *InMemoryBackend) appVersionGet(region, appName, versionLabel string) (*ApplicationVersion, bool) {
	return b.appVersions.Get(regionKey(region, appVersionKey(appName, versionLabel)))
}

func (b *InMemoryBackend) appVersionPut(v *ApplicationVersion) { b.appVersions.Put(v) }

func (b *InMemoryBackend) appVersionDelete(region, appName, versionLabel string) {
	b.appVersions.Delete(regionKey(region, appVersionKey(appName, versionLabel)))
}

func (b *InMemoryBackend) appVersionsInRegion(region string) []*ApplicationVersion {
	return b.appVersionsByRegion.Get(region)
}

func (b *InMemoryBackend) appVersionByARN(region, resourceARN string) (*ApplicationVersion, bool) {
	list := b.appVersionsByARN.Get(regionKey(region, resourceARN))
	if len(list) == 0 {
		return nil, false
	}

	return list[0], true
}

func (b *InMemoryBackend) configTemplateGet(region, appName, templateName string) (*ConfigurationTemplate, bool) {
	return b.configTemplates.Get(regionKey(region, configTemplateKey(appName, templateName)))
}

func (b *InMemoryBackend) configTemplatePut(v *ConfigurationTemplate) { b.configTemplates.Put(v) }

func (b *InMemoryBackend) configTemplateDelete(region, appName, templateName string) {
	b.configTemplates.Delete(regionKey(region, configTemplateKey(appName, templateName)))
}

func (b *InMemoryBackend) configTemplatesInRegion(region string) []*ConfigurationTemplate {
	return b.configTemplatesByRegion.Get(region)
}

func (b *InMemoryBackend) platformVersionGet(region, platformARN string) (*PlatformVersion, bool) {
	return b.platformVersions.Get(regionKey(region, platformARN))
}

func (b *InMemoryBackend) platformVersionPut(v *PlatformVersion) { b.platformVersions.Put(v) }

func (b *InMemoryBackend) platformVersionDelete(region, platformARN string) {
	b.platformVersions.Delete(regionKey(region, platformARN))
}

func (b *InMemoryBackend) platformVersionsInRegion(region string) []*PlatformVersion {
	return b.platformVersionsByRegion.Get(region)
}

func (b *InMemoryBackend) managedActionHistoryStore(region string) map[string][]*ManagedActionHistory {
	if b.managedActionHistory[region] == nil {
		b.managedActionHistory[region] = make(map[string][]*ManagedActionHistory)
	}

	return b.managedActionHistory[region]
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

	if _, ok := b.applicationGet(region, name); ok {
		return nil, fmt.Errorf("%w: application %s already exists", ErrAlreadyExists, name)
	}

	appARN := arn.Build("elasticbeanstalk", region, b.accountID, "application/"+name)

	app := &Application{
		ApplicationName: name,
		ApplicationARN:  appARN,
		Description:     description,
		DateCreated:     nowISO8601(),
		DateUpdated:     nowISO8601(),
		Tags:            copyTags(tags),
		region:          region,
	}
	b.applicationPut(app)

	return cloneApplication(app), nil
}

// DescribeApplications returns applications, optionally filtered by names.
// Results are sorted by ApplicationName for deterministic output.
func (b *InMemoryBackend) DescribeApplications(ctx context.Context, names []string) []*Application {
	b.mu.RLock("DescribeApplications")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	if len(names) == 0 {
		apps := b.applicationsInRegion(region)
		list := make([]*Application, 0, len(apps))

		for _, app := range apps {
			list = append(list, cloneApplication(app))
		}

		sort.Slice(list, func(i, j int) bool {
			return list[i].ApplicationName < list[j].ApplicationName
		})

		return list
	}

	list := make([]*Application, 0, len(names))

	for _, name := range names {
		if app, ok := b.applicationGet(region, name); ok {
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

	app, ok := b.applicationGet(region, name)
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

	app, ok := b.applicationGet(region, appName)
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

	if _, ok := b.applicationGet(region, name); !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, name)
	}

	// Cascade: remove all environments belonging to this application. The
	// index result is cloned before the delete loop since store.Index
	// slices mutate under Delete (see pkgs/store gotcha).
	for _, env := range slices.Clone(b.environmentsInRegion(region)) {
		if env.ApplicationName == name {
			b.environmentDeleteKey(region, env.ApplicationName, env.EnvironmentName)
		}
	}

	// Cascade: remove all application versions belonging to this application.
	for _, ver := range slices.Clone(b.appVersionsInRegion(region)) {
		if ver.ApplicationName == name {
			b.appVersionDelete(region, ver.ApplicationName, ver.VersionLabel)
		}
	}

	// Cascade: remove all configuration templates belonging to this application.
	for _, tmpl := range slices.Clone(b.configTemplatesInRegion(region)) {
		if tmpl.ApplicationName == name {
			b.configTemplateDelete(region, tmpl.ApplicationName, tmpl.TemplateName)
		}
	}

	// applicationDelete also removes app from every registered index
	// (byRegion, byARN) automatically -- see store.Table.Delete.
	b.applicationDelete(region, name)

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

	if _, ok := b.environmentGet(region, appName, envName); ok {
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
		DateCreated:       nowISO8601(),
		DateUpdated:       nowISO8601(),
		Region:            region,
		Tags:              copyTags(tags),
	}
	b.environmentPut(env)

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
	envs := b.environmentsInRegion(region)

	list := make([]*Environment, 0, len(envs))

	for _, env := range envs {
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

	env, ok := b.environmentGet(region, appName, envName)
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

	env.DateUpdated = nowISO8601()

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

	env, ok := b.environmentGet(region, appName, envName)
	if !ok {
		return nil, fmt.Errorf("%w: environment %s not found", ErrNotFound, envName)
	}

	env.Status = "Terminated"
	out := cloneEnvironment(env)
	b.environmentDeleteKey(region, appName, envName)

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

	src, ok := b.environmentGet(region, srcAppName, srcEnvName)
	if !ok {
		return nil, fmt.Errorf("%w: source environment %s not found", ErrNotFound, srcEnvName)
	}

	if _, exists := b.environmentGet(region, srcAppName, newEnvName); exists {
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
		DateCreated:       nowISO8601(),
		DateUpdated:       nowISO8601(),
		Region:            region,
		Tags:              copyTags(src.Tags),
	}
	b.environmentPut(env)

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

	if _, ok := b.appVersionGet(region, appName, versionLabel); ok {
		return nil, fmt.Errorf(
			"%w: application version %s already exists",
			ErrAlreadyExists,
			versionLabel,
		)
	}

	vARN := arn.Build("elasticbeanstalk", region, b.accountID,
		"applicationversion/"+appName+"/"+versionLabel)

	if params.AutoCreateApplication {
		if _, ok := b.applicationGet(region, appName); !ok {
			appARN := arn.Build("elasticbeanstalk", region, b.accountID, "application/"+appName)
			b.applicationPut(&Application{
				ApplicationName: appName,
				ApplicationARN:  appARN,
				Tags:            map[string]string{},
				region:          region,
			})
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
		DateCreated:            nowISO8601(),
		DateUpdated:            nowISO8601(),
		Status:                 status,
		Process:                params.Process,
		S3Bucket:               params.S3Bucket,
		S3Key:                  params.S3Key,
		SourceBuildInformation: params.SourceBuildInformation,
		Tags:                   copyTags(params.Tags),
		region:                 region,
	}
	b.appVersionPut(ver)

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
	vers := b.appVersionsInRegion(region)

	list := make([]*ApplicationVersion, 0, len(vers))

	for _, ver := range vers {
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

	if _, ok := b.appVersionGet(region, appName, versionLabel); !ok {
		return fmt.Errorf("%w: application version %s not found", ErrNotFound, versionLabel)
	}

	b.appVersionDelete(region, appName, versionLabel)

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
	if app, ok := b.applicationByARN(region, resourceARN); ok {
		return app.Tags, true
	}

	if env, ok := b.environmentByARN(region, resourceARN); ok {
		return env.Tags, true
	}

	if ver, ok := b.appVersionByARN(region, resourceARN); ok {
		return ver.Tags, true
	}

	return nil, false
}

// ensureTagsByARN ensures a resource has an initialised tags map.
// Caller must hold the write lock.
func (b *InMemoryBackend) ensureTagsByARN(region, resourceARN string) {
	if app, ok := b.applicationByARN(region, resourceARN); ok {
		if app.Tags == nil {
			app.Tags = make(map[string]string)
		}

		return
	}

	if env, ok := b.environmentByARN(region, resourceARN); ok {
		if env.Tags == nil {
			env.Tags = make(map[string]string)
		}

		return
	}

	if ver, ok := b.appVersionByARN(region, resourceARN); ok {
		if ver.Tags == nil {
			ver.Tags = make(map[string]string)
		}
	}
}

// Reset clears all backend state, resetting to an empty store.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.managedActionHistory = make(map[string]map[string][]*ManagedActionHistory)
	b.events = make(map[string][]*EventRecord)
	b.envCounters = make(map[string]int)
	b.initRegion(b.region)
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

	env, ok := b.environmentByName(region, envName)
	if !ok {
		return fmt.Errorf("%w: environment %s not found", ErrNotFound, envName)
	}

	env.OperationsRole = role

	return nil
}

// CheckDNSAvailability checks whether the specified CNAME prefix is available.
// Returns available=true when no existing environment in the request region uses that prefix.
func (b *InMemoryBackend) CheckDNSAvailability(ctx context.Context, cnamePrefix string) (bool, string) {
	b.mu.RLock("CheckDNSAvailability")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	fqcname := cnamePrefix + "." + region + ".elasticbeanstalk.com"

	if b.environmentCNAMETaken(region, fqcname) {
		return false, fqcname
	}

	if _, ok := b.environmentByName(region, cnamePrefix); ok {
		return false, fqcname
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
	envs := b.environmentsInRegion(region)
	list := make([]*Environment, 0, len(envs))

	for _, env := range envs {
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

	if _, ok := b.configTemplateGet(region, appName, templateName); ok {
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
		DateCreated:       nowISO8601(),
		DateUpdated:       nowISO8601(),
		SolutionStackName: solutionStack,
		Tags:              copyTags(tags),
		region:            region,
	}
	b.configTemplatePut(tmpl)

	return cloneConfigurationTemplate(tmpl), nil
}

// DescribeConfigurationTemplates returns all configuration templates for an application (improvement #17).
func (b *InMemoryBackend) DescribeConfigurationTemplates(ctx context.Context, appName string) []*ConfigurationTemplate {
	b.mu.RLock("DescribeConfigurationTemplates")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	tmpls := b.configTemplatesInRegion(region)
	list := make([]*ConfigurationTemplate, 0, len(tmpls))

	for _, tmpl := range tmpls {
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

	if _, ok := b.platformVersionGet(region, platformARN); ok {
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
		region:          region,
	}
	b.platformVersionPut(pv)

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

	if _, ok := b.configTemplateGet(region, appName, templateName); !ok {
		return fmt.Errorf("%w: configuration template %s not found", ErrNotFound, templateName)
	}

	b.configTemplateDelete(region, appName, templateName)

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

	pv, ok := b.platformVersionGet(region, platformARN)
	if !ok {
		return nil, fmt.Errorf("%w: platform version %s not found", ErrNotFound, platformARN)
	}

	out := clonePlatformVersion(pv)
	b.platformVersionDelete(region, platformARN)

	return out, nil
}

// DescribePlatformVersion returns a platform version by ARN.
func (b *InMemoryBackend) DescribePlatformVersion(ctx context.Context, platformARN string) (*PlatformVersion, error) {
	b.mu.RLock("DescribePlatformVersion")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	pv, ok := b.platformVersionGet(region, platformARN)
	if !ok {
		return nil, fmt.Errorf("%w: platform version %s not found", ErrNotFound, platformARN)
	}

	return clonePlatformVersion(pv), nil
}

// DescribeEnvironmentHealth returns the health and status of an environment by name.
func (b *InMemoryBackend) DescribeEnvironmentHealth(ctx context.Context, envName string) (string, string, error) {
	b.mu.RLock("DescribeEnvironmentHealth")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	env, ok := b.environmentByName(region, envName)
	if !ok {
		return "", "", fmt.Errorf("%w: environment %s not found", ErrNotFound, envName)
	}

	return env.Health, env.Status, nil
}

// DisassociateEnvironmentOperationsRole removes the operations role from an environment.
func (b *InMemoryBackend) DisassociateEnvironmentOperationsRole(ctx context.Context, envName string) error {
	b.mu.Lock("DisassociateEnvironmentOperationsRole")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	env, ok := b.environmentByName(region, envName)
	if !ok {
		return fmt.Errorf("%w: environment %s not found", ErrNotFound, envName)
	}

	env.OperationsRole = ""

	return nil
}

// ListPlatformVersions returns all stored platform versions sorted by ARN.
func (b *InMemoryBackend) ListPlatformVersions(ctx context.Context) []*PlatformVersion {
	b.mu.RLock("ListPlatformVersions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	pvs := b.platformVersionsInRegion(region)
	list := make([]*PlatformVersion, 0, len(pvs))

	for _, pv := range pvs {
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

	for _, env := range b.environmentsInRegion(region) {
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

	// CNAME is an indexed field (environmentsByCNAME); mutating it in place
	// would leave a stale index entry (see pkgs/store gotcha), so both
	// entries are deleted, mutated, and re-Put to rebuild every index.
	b.environmentDeleteKey(region, srcEnv.ApplicationName, srcEnv.EnvironmentName)
	b.environmentDeleteKey(region, dstEnv.ApplicationName, dstEnv.EnvironmentName)

	srcEnv.CNAME, dstEnv.CNAME = dstEnv.CNAME, srcEnv.CNAME

	b.environmentPut(srcEnv)
	b.environmentPut(dstEnv)

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

	ver, ok := b.appVersionGet(region, appName, versionLabel)
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

	tmpl, ok := b.configTemplateGet(region, appName, templateName)
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
	events := append(b.eventsSlice(region), &EventRecord{
		ApplicationName: appName,
		EnvironmentName: envName,
		EventDate:       nowISO8601(),
		Message:         message,
		Severity:        severity,
	})
	if len(events) > maxEventsPerRegion {
		events = events[len(events)-maxEventsPerRegion:]
	}
	b.events[region] = events
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

// envKey returns the map key for an environment.
func envKey(appName, envName string) string {
	return appName + "\x00" + envName
}

// appVersionKey returns the map key for an application version.
func appVersionKey(appName, versionLabel string) string {
	return appName + "\x00" + versionLabel
}

// --- Seed helpers (used in tests via export_test.go) ---

// addApplicationInternal seeds an application directly into the backend, bypassing validation.
// Caller must hold the write lock.
func (b *InMemoryBackend) addApplicationInternal(region string, app *Application) {
	cp := cloneApplication(app)
	cp.region = region
	b.applicationPut(cp)
}

// addEnvironmentInternal seeds an environment directly into the backend, bypassing validation.
// Caller must hold the write lock.
func (b *InMemoryBackend) addEnvironmentInternal(region string, env *Environment) {
	cp := cloneEnvironment(env)
	cp.Region = region
	b.environmentPut(cp)
}

// addAppVersionInternal seeds an application version directly into the backend, bypassing validation.
// Caller must hold the write lock.
func (b *InMemoryBackend) addAppVersionInternal(region string, ver *ApplicationVersion) {
	cp := cloneApplicationVersion(ver)
	cp.region = region
	b.appVersionPut(cp)
}

// addConfigTemplateInternal seeds a configuration template directly into the backend.
// Caller must hold the write lock.
func (b *InMemoryBackend) addConfigTemplateInternal(region string, tmpl *ConfigurationTemplate) {
	cp := cloneConfigurationTemplate(tmpl)
	cp.region = region
	b.configTemplatePut(cp)
}

// addPlatformVersionInternal seeds a platform version directly into the backend.
// Caller must hold the write lock.
func (b *InMemoryBackend) addPlatformVersionInternal(region string, pv *PlatformVersion) {
	cp := clonePlatformVersion(pv)
	cp.region = region
	b.platformVersionPut(cp)
}
