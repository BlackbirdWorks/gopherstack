// Package codedeploy provides an in-memory implementation of the AWS CodeDeploy service.
package codedeploy

import (
	"fmt"
	"math/rand/v2"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	statusSucceeded       = "Succeeded"
	statusStopped         = "Stopped"
	computePlatformServer = "Server"
	computePlatformLambda = "Lambda"
	computePlatformECS    = "ECS"

	maxTagsPerResource = 50
	maxTagKeyLen       = 128
	maxTagValueLen     = 256
	tagReservedPrefix  = "aws:"

	deployIDChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	deployIDLen   = 9

	// Default config values for built-in deployment configurations.
	defaultHealthyHostPct = 50
	defaultCanaryPct      = 10
	defaultCanaryInterval = 5
	defaultLinearPct      = 10
	defaultLinearInterval = 1
	arnSegmentCount       = 7
)

// simulatedDeployDuration is the simulated time for a deployment to complete.
const simulatedDeployDuration = 5 * time.Second

// maxBatchRevisions is the maximum number of revisions accepted by BatchGetApplicationRevisions.
const maxBatchRevisions = 25

var (
	ErrNotFound                      = awserr.New("ApplicationDoesNotExistException", awserr.ErrNotFound)
	ErrDeploymentGroupNotFound       = awserr.New("DeploymentGroupDoesNotExistException", awserr.ErrNotFound)
	ErrDeploymentNotFound            = awserr.New("DeploymentDoesNotExistException", awserr.ErrNotFound)
	ErrAlreadyExists                 = awserr.New("ApplicationAlreadyExistsException", awserr.ErrConflict)
	ErrDeploymentGroupAlreadyExists  = awserr.New("DeploymentGroupAlreadyExistsException", awserr.ErrConflict)
	ErrDeploymentConfigNotFound      = awserr.New("DeploymentConfigDoesNotExistException", awserr.ErrNotFound)
	ErrDeploymentConfigAlreadyExists = awserr.New("DeploymentConfigAlreadyExistsException", awserr.ErrConflict)
	ErrOnPremisesInstanceNotFound    = awserr.New("InstanceNameRequiredException", awserr.ErrNotFound)
	ErrValidation                    = awserr.New("InvalidParameterValueException", awserr.ErrInvalidParameter)
	ErrTagLimitExceeded              = awserr.New("TagLimitExceededException", awserr.ErrInvalidParameter)
	ErrInvalidComputePlatform        = awserr.New("InvalidComputePlatformException", awserr.ErrInvalidParameter)
	ErrIamArnRequired                = awserr.New("IamArnRequiredException", awserr.ErrInvalidParameter)
	ErrMultipleIamArns               = awserr.New("MultipleIamArnsProvidedException", awserr.ErrInvalidParameter)
	ErrDeploymentConfigInUse         = awserr.New("DeploymentConfigInUseException", awserr.ErrConflict)
	ErrGitHubAccountTokenNotFound    = awserr.New("GitHubAccountTokenDoesNotExistException", awserr.ErrNotFound)

	// onPremInstanceNameRe validates on-premises instance names per AWS spec.
	onPremInstanceNameRe = regexp.MustCompile(`^[A-Za-z0-9._\-]{1,100}$`)
)

// TagFilter represents a key/value tag filter for on-premises or EC2 instances.
type TagFilter struct {
	Key   string `json:"Key,omitempty"`
	Value string `json:"Value,omitempty"`
	Type  string `json:"Type,omitempty"` // EQUALS | KEY_ONLY | VALUE_ONLY
}

// TagSet is a list of tag filters combined with AND logic.
type TagSet struct {
	OnPremisesTagSetList [][]TagFilter `json:"onPremisesTagSetList,omitempty"`
}

// Ec2TagSet is a list of EC2 tag filter groups combined with AND logic.
type Ec2TagSet struct {
	Ec2TagSetList [][]TagFilter `json:"ec2TagSetList,omitempty"`
}

// AutoScalingGroup references an Auto Scaling group for a deployment group.
type AutoScalingGroup struct {
	Name string `json:"name,omitempty"`
	Hook string `json:"hook,omitempty"`
}

// ElbInfo references an ELB for load-balancer-based routing.
type ElbInfo struct {
	Name string `json:"name,omitempty"`
}

// TargetGroupInfo references a target group for ALB/NLB routing.
type TargetGroupInfo struct {
	Name string `json:"name,omitempty"`
}

// TargetGroupPairInfo is an ALB/NLB target group pair for blue/green deployments.
type TargetGroupPairInfo struct {
	ProdTrafficRoute *TrafficRoute     `json:"prodTrafficRoute,omitempty"`
	TestTrafficRoute *TrafficRoute     `json:"testTrafficRoute,omitempty"`
	TargetGroups     []TargetGroupInfo `json:"targetGroups,omitempty"`
}

// TrafficRoute specifies a listener ARN for traffic routing.
type TrafficRoute struct {
	ListenerArns []string `json:"listenerArns,omitempty"`
}

// LoadBalancerInfo holds load balancer configuration for a deployment group.
type LoadBalancerInfo struct {
	ElbInfoList             []ElbInfo             `json:"elbInfoList,omitempty"`
	TargetGroupInfoList     []TargetGroupInfo     `json:"targetGroupInfoList,omitempty"`
	TargetGroupPairInfoList []TargetGroupPairInfo `json:"targetGroupPairInfoList,omitempty"`
}

// DeploymentStyle describes the type and traffic control option for a deployment.
type DeploymentStyle struct {
	DeploymentType   string `json:"deploymentType,omitempty"`   // IN_PLACE | BLUE_GREEN
	DeploymentOption string `json:"deploymentOption,omitempty"` // WITH_TRAFFIC_CONTROL | WITHOUT_TRAFFIC_CONTROL
}

// TerminateBlueInstancesOnDeploymentSuccess holds blue-instance termination config.
type TerminateBlueInstancesOnDeploymentSuccess struct {
	Action                       string `json:"action,omitempty"` // TERMINATE | KEEP_ALIVE
	TerminationWaitTimeInMinutes int    `json:"terminationWaitTimeInMinutes,omitempty"`
}

// DeploymentReadyOption configures behavior when blue/green instances are ready.
type DeploymentReadyOption struct {
	ActionOnTimeout   string `json:"actionOnTimeout,omitempty"` // CONTINUE_DEPLOYMENT | STOP_DEPLOYMENT
	WaitTimeInMinutes int    `json:"waitTimeInMinutes,omitempty"`
}

// GreenFleetProvisioningOption configures how replacement instances are provisioned.
type GreenFleetProvisioningOption struct {
	Action string `json:"action,omitempty"` // DISCOVER_EXISTING | COPY_AUTO_SCALING_GROUP
}

// BlueGreenDeploymentConfiguration holds blue/green deployment configuration.
type BlueGreenDeploymentConfiguration struct {
	TerminateBlueInstancesOnDeploymentSuccess *TerminateBlueInstancesOnDeploymentSuccess `json:"terminateBlueInstancesOnDeploymentSuccess,omitempty"` //nolint:lll // long AWS name
	DeploymentReadyOption                     *DeploymentReadyOption                     `json:"deploymentReadyOption,omitempty"`                     //nolint:lll // aligned with AWS field above
	GreenFleetProvisioningOption              *GreenFleetProvisioningOption              `json:"greenFleetProvisioningOption,omitempty"`              //nolint:lll // aligned with AWS field above
}

// Alarm references a CloudWatch alarm.
type Alarm struct {
	Name string `json:"name,omitempty"`
}

// AlarmConfiguration holds alarm-based stop configuration.
type AlarmConfiguration struct {
	Alarms                 []Alarm `json:"alarms,omitempty"`
	Enabled                bool    `json:"enabled,omitempty"`
	IgnorePollAlarmFailure bool    `json:"ignorePollAlarmFailure,omitempty"`
}

// AutoRollbackConfiguration holds auto-rollback event configuration.
type AutoRollbackConfiguration struct {
	Events  []string `json:"events,omitempty"` // rollback event types
	Enabled bool     `json:"enabled,omitempty"`
}

// TriggerConfiguration holds SNS trigger configuration for deployment events.
type TriggerConfiguration struct {
	TriggerName      string   `json:"triggerName,omitempty"`
	TriggerTargetArn string   `json:"triggerTargetArn,omitempty"`
	TriggerEvents    []string `json:"triggerEvents,omitempty"`
}

// ECSService references an ECS service for ECS-platform deployments.
type ECSService struct {
	ServiceName string `json:"serviceName,omitempty"`
	ClusterName string `json:"clusterName,omitempty"`
}

// Application represents an AWS CodeDeploy application.
type Application struct {
	CreationTime    time.Time         `json:"createTime"`
	Tags            *tags.Tags        `json:"-"`
	TagsMap         map[string]string `json:"tagsMap,omitempty"`
	ApplicationName string            `json:"applicationName"`
	ApplicationID   string            `json:"applicationId"`
	ComputePlatform string            `json:"computePlatform"`
	AccountID       string            `json:"-"`
	Region          string            `json:"-"`
}

// DeploymentGroup represents a CodeDeploy deployment group.
type DeploymentGroup struct {
	Tags                             *tags.Tags                        `json:"-"`
	TagsMap                          map[string]string                 `json:"tagsMap,omitempty"`
	BlueGreenDeploymentConfiguration *BlueGreenDeploymentConfiguration `json:"blueGreenDeploymentConfiguration,omitempty"`
	AlarmConfiguration               *AlarmConfiguration               `json:"alarmConfiguration,omitempty"`
	AutoRollbackConfiguration        *AutoRollbackConfiguration        `json:"autoRollbackConfiguration,omitempty"`
	LoadBalancerInfo                 *LoadBalancerInfo                 `json:"loadBalancerInfo,omitempty"`
	DeploymentStyle                  *DeploymentStyle                  `json:"deploymentStyle,omitempty"`
	Ec2TagSet                        *Ec2TagSet                        `json:"ec2TagSet,omitempty"`
	OnPremisesTagSet                 *TagSet                           `json:"onPremisesTagSet,omitempty"`
	ApplicationName                  string                            `json:"applicationName"`
	DeploymentGroupName              string                            `json:"deploymentGroupName"`
	DeploymentGroupID                string                            `json:"deploymentGroupId"`
	ServiceRoleArn                   string                            `json:"serviceRoleArn"`
	DeploymentConfigName             string                            `json:"deploymentConfigName"`
	ComputePlatform                  string                            `json:"computePlatform"`
	OutdatedInstancesStrategy        string                            `json:"outdatedInstancesStrategy,omitempty"`
	AccountID                        string                            `json:"-"`
	Region                           string                            `json:"-"`
	Ec2TagFilters                    []TagFilter                       `json:"ec2TagFilters,omitempty"`
	OnPremisesInstanceTagFilters     []TagFilter                       `json:"onPremisesInstanceTagFilters,omitempty"`
	AutoScalingGroups                []AutoScalingGroup                `json:"autoScalingGroups,omitempty"`
	TriggerConfigurations            []TriggerConfiguration            `json:"triggerConfigurations,omitempty"`
	ECSServices                      []ECSService                      `json:"ecsServices,omitempty"`
	TerminationHookEnabled           bool                              `json:"terminationHookEnabled,omitempty"`
}

// RevisionS3Location holds the S3 location fields for a deployment revision.
type RevisionS3Location struct {
	Bucket     string `json:"bucket,omitempty"`
	Key        string `json:"key,omitempty"`
	BundleType string `json:"bundleType,omitempty"`
	ETag       string `json:"eTag,omitempty"`
	Version    string `json:"version,omitempty"`
}

// RevisionGitHubLocation holds the GitHub location fields for a deployment revision.
type RevisionGitHubLocation struct {
	Repository string `json:"repository,omitempty"`
	CommitID   string `json:"commitId,omitempty"`
}

// RevisionAppSpecContent holds the AppSpec content for a string/AppSpecContent revision.
type RevisionAppSpecContent struct {
	Content string `json:"content,omitempty"`
	Sha256  string `json:"sha256,omitempty"`
}

// RevisionLocation represents a deployment revision source location.
type RevisionLocation struct {
	S3Location     *RevisionS3Location     `json:"s3Location,omitempty"`
	GitHubLocation *RevisionGitHubLocation `json:"gitHubLocation,omitempty"`
	AppSpecContent *RevisionAppSpecContent `json:"appSpecContent,omitempty"`
	RevisionType   string                  `json:"revisionType,omitempty"`
}

// Deployment represents a CodeDeploy deployment.
type Deployment struct {
	CreateTime                    time.Time         `json:"createTime"`
	CompleteTime                  *time.Time        `json:"completeTime,omitempty"`
	Revision                      *RevisionLocation `json:"revision,omitempty"`
	Status                        string            `json:"status"`
	ApplicationName               string            `json:"applicationName"`
	DeploymentGroupName           string            `json:"deploymentGroupName"`
	DeploymentConfigName          string            `json:"deploymentConfigName"`
	DeploymentID                  string            `json:"deploymentId"`
	Creator                       string            `json:"creator"`
	Description                   string            `json:"description,omitempty"`
	FileExistsBehavior            string            `json:"fileExistsBehavior,omitempty"`
	AccountID                     string            `json:"-"`
	Region                        string            `json:"-"`
	UpdateOutdatedInstancesOnly   bool              `json:"updateOutdatedInstancesOnly,omitempty"`
	IgnoreApplicationStopFailures bool              `json:"ignoreApplicationStopFailures,omitempty"`
}

// OnPremisesInstance represents an on-premises instance registered with CodeDeploy.
type OnPremisesInstance struct {
	RegisterTime   time.Time         `json:"registerTime"`
	DeregisterTime *time.Time        `json:"deregisterTime,omitempty"`
	Tags           *tags.Tags        `json:"-"`
	TagsMap        map[string]string `json:"tagsMap,omitempty"`
	InstanceName   string            `json:"instanceName"`
	IamSessionArn  string            `json:"iamSessionArn,omitempty"`
	IamUserArn     string            `json:"iamUserArn,omitempty"`
}

// MinimumHealthyHosts specifies the minimum number/percentage of healthy instances.
type MinimumHealthyHosts struct {
	Type  string `json:"type,omitempty"` // HOST_COUNT | FLEET_PERCENT
	Value int    `json:"value,omitempty"`
}

// TimeBasedCanary holds canary traffic shifting configuration.
type TimeBasedCanary struct {
	CanaryPercentage int `json:"canaryPercentage,omitempty"`
	CanaryInterval   int `json:"canaryInterval,omitempty"`
}

// TimeBasedLinear holds linear traffic shifting configuration.
type TimeBasedLinear struct {
	LinearPercentage int `json:"linearPercentage,omitempty"`
	LinearInterval   int `json:"linearInterval,omitempty"`
}

// TrafficRoutingConfig holds traffic routing configuration for a deployment config.
type TrafficRoutingConfig struct {
	TimeBasedCanary *TimeBasedCanary `json:"timeBasedCanary,omitempty"`
	TimeBasedLinear *TimeBasedLinear `json:"timeBasedLinear,omitempty"`
	Type            string           `json:"type,omitempty"`
}

// ZonalConfig holds availability-zone-based deployment configuration.
type ZonalConfig struct {
	MinimumHealthyHostsPerZone        *MinimumHealthyHosts `json:"minimumHealthyHostsPerZone,omitempty"`
	FirstZoneMonitorDurationInSeconds int                  `json:"firstZoneMonitorDurationInSeconds,omitempty"`
	MonitorDurationInSeconds          int                  `json:"monitorDurationInSeconds,omitempty"`
}

// DeploymentConfig represents a CodeDeploy deployment configuration.
type DeploymentConfig struct {
	CreateTime           time.Time             `json:"createTime"`
	MinimumHealthyHosts  *MinimumHealthyHosts  `json:"minimumHealthyHosts,omitempty"`
	TrafficRoutingConfig *TrafficRoutingConfig `json:"trafficRoutingConfig,omitempty"`
	ZonalConfig          *ZonalConfig          `json:"zonalConfig,omitempty"`
	DeploymentConfigName string                `json:"deploymentConfigName"`
	DeploymentConfigID   string                `json:"deploymentConfigId"`
	ComputePlatform      string                `json:"computePlatform"`
	IsDefault            bool                  `json:"isDefault,omitempty"`
}

// InMemoryBackend is the in-memory store for CodeDeploy resources.
//
// deployments and deploymentConfigs carry a real, wire-visible identity
// field and no live *tags.Tags field, so each registers directly on
// b.registry as a "clean" *store.Table.
//
// applications, deploymentGroups, and onPremisesInstances each carry a live
// *tags.Tags field marked json:"-", so each is a "dirty" table (store.New
// only, NOT store.Register-ed onto b.registry -- see store_setup.go)
// round-tripped through a DTO wrapper in persistence.go. deploymentGroups
// was previously nested by application; it flattens to one *store.Table
// keyed by the composite "appName/dgName" string (see dgKey in
// store_setup.go), with deploymentGroupsByApp replacing the old
// map[string]map[string]*DeploymentGroup nesting for per-application scans.
//
// githubTokens is deliberately NOT converted: it is an identity-less set
// (map[string]struct{}), so there is no *T value for store.Table to key on.
// It remains a plain map, unchanged by this refactor.
type InMemoryBackend struct {
	registry              *store.Registry
	applications          *store.Table[Application]
	deploymentGroups      *store.Table[DeploymentGroup]
	deploymentGroupsByApp *store.Index[DeploymentGroup]
	deployments           *store.Table[Deployment]
	onPremisesInstances   *store.Table[OnPremisesInstance]
	deploymentConfigs     *store.Table[DeploymentConfig]
	githubTokens          map[string]struct{}
	mu                    *lockmetrics.RWMutex
	accountID             string
	region                string
}

// NewInMemoryBackend creates a new in-memory CodeDeploy backend with pre-seeded default configs.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:     store.NewRegistry(),
		githubTokens: make(map[string]struct{}),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("codedeploy"),
	}

	registerAllTables(b)
	b.seedDefaultConfigs()

	return b
}

// seedDefaultConfigs pre-populates the standard AWS CodeDeploy built-in deployment configurations.
func (b *InMemoryBackend) seedDefaultConfigs() {
	allAtOnce := &TrafficRoutingConfig{Type: "AllAtOnce"}

	defaults := []*DeploymentConfig{
		{
			DeploymentConfigName: "CodeDeployDefault.AllAtOnce",
			ComputePlatform:      computePlatformServer,
			MinimumHealthyHosts:  &MinimumHealthyHosts{Type: "FLEET_PERCENT", Value: 0},
			TrafficRoutingConfig: allAtOnce,
			IsDefault:            true,
		},
		{
			DeploymentConfigName: "CodeDeployDefault.OneAtATime",
			ComputePlatform:      computePlatformServer,
			MinimumHealthyHosts:  &MinimumHealthyHosts{Type: "HOST_COUNT", Value: 1},
			TrafficRoutingConfig: allAtOnce,
			IsDefault:            true,
		},
		{
			DeploymentConfigName: "CodeDeployDefault.HalfAtATime",
			ComputePlatform:      computePlatformServer,
			MinimumHealthyHosts:  &MinimumHealthyHosts{Type: "FLEET_PERCENT", Value: defaultHealthyHostPct},
			TrafficRoutingConfig: allAtOnce,
			IsDefault:            true,
		},
		{
			DeploymentConfigName: "CodeDeployDefault.LambdaAllAtOnce",
			ComputePlatform:      computePlatformLambda,
			TrafficRoutingConfig: allAtOnce,
			IsDefault:            true,
		},
		{
			DeploymentConfigName: "CodeDeployDefault.LambdaCanary10Percent5Minutes",
			ComputePlatform:      computePlatformLambda,
			TrafficRoutingConfig: &TrafficRoutingConfig{
				Type: "TimeBasedCanary",
				TimeBasedCanary: &TimeBasedCanary{
					CanaryPercentage: defaultCanaryPct,
					CanaryInterval:   defaultCanaryInterval,
				},
			},
			IsDefault: true,
		},
		{
			DeploymentConfigName: "CodeDeployDefault.LambdaLinear10PercentEvery1Minute",
			ComputePlatform:      computePlatformLambda,
			TrafficRoutingConfig: &TrafficRoutingConfig{
				Type: "TimeBasedLinear",
				TimeBasedLinear: &TimeBasedLinear{
					LinearPercentage: defaultLinearPct,
					LinearInterval:   defaultLinearInterval,
				},
			},
			IsDefault: true,
		},
		{
			DeploymentConfigName: "CodeDeployDefault.ECSAllAtOnce",
			ComputePlatform:      computePlatformECS,
			TrafficRoutingConfig: allAtOnce,
			IsDefault:            true,
		},
		{
			DeploymentConfigName: "CodeDeployDefault.ECSCanary10Percent5Minutes",
			ComputePlatform:      computePlatformECS,
			TrafficRoutingConfig: &TrafficRoutingConfig{
				Type: "TimeBasedCanary",
				TimeBasedCanary: &TimeBasedCanary{
					CanaryPercentage: defaultCanaryPct,
					CanaryInterval:   defaultCanaryInterval,
				},
			},
			IsDefault: true,
		},
		{
			DeploymentConfigName: "CodeDeployDefault.ECSLinear10PercentEvery1Minute",
			ComputePlatform:      computePlatformECS,
			TrafficRoutingConfig: &TrafficRoutingConfig{
				Type: "TimeBasedLinear",
				TimeBasedLinear: &TimeBasedLinear{
					LinearPercentage: defaultLinearPct,
					LinearInterval:   defaultLinearInterval,
				},
			},
			IsDefault: true,
		},
	}

	now := time.Now().UTC()
	for _, cfg := range defaults {
		cfg.DeploymentConfigID = uuid.NewString()
		cfg.CreateTime = now
		b.deploymentConfigs.Put(cfg)
	}
}

// Reset clears all state, returning the backend to a fresh empty state (with default configs re-seeded).
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, app := range b.applications.All() {
		if app.Tags != nil {
			app.Tags.Close()
		}
	}

	for _, dg := range b.deploymentGroups.All() {
		if dg.Tags != nil {
			dg.Tags.Close()
		}
	}

	for _, inst := range b.onPremisesInstances.All() {
		if inst.Tags != nil {
			inst.Tags.Close()
		}
	}

	b.registry.ResetAll()
	b.applications.Reset()
	b.deploymentGroups.Reset()
	b.onPremisesInstances.Reset()
	b.githubTokens = make(map[string]struct{})

	b.seedDefaultConfigs()
}

// ensureTags returns the given tags if non-nil, or creates a new tags.Tags with the given key.
func ensureTags(existing *tags.Tags, key string) *tags.Tags {
	if existing != nil {
		return existing
	}

	return tags.New(key)
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// generateDeploymentID produces an AWS-format deployment ID: d- followed by 9 uppercase alphanumeric chars.
func generateDeploymentID() string {
	b := make([]byte, deployIDLen)
	for i := range b {
		b[i] = deployIDChars[rand.IntN(len(deployIDChars))] //nolint:gosec // non-crypto ID for test mock
	}

	return "d-" + string(b)
}

// validateComputePlatform returns an error if the given platform is not a valid CodeDeploy compute platform.
func validateComputePlatform(platform string) error {
	if _, ok := validComputePlatforms()[platform]; !ok {
		return fmt.Errorf("%w: invalid computePlatform %q, must be Server, Lambda, or ECS",
			ErrInvalidComputePlatform, platform)
	}

	return nil
}

// CreateApplication creates a new CodeDeploy application.
func (b *InMemoryBackend) CreateApplication(name, computePlatform string, kv map[string]string) (*Application, error) {
	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if b.applications.Has(name) {
		return nil, fmt.Errorf("%w: application %s already exists", ErrAlreadyExists, name)
	}

	if computePlatform == "" {
		computePlatform = computePlatformServer
	}

	if err := validateComputePlatform(computePlatform); err != nil {
		return nil, err
	}

	appID := uuid.NewString()
	t := tags.New("codedeploy.application." + name + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	app := &Application{
		ApplicationName: name,
		ApplicationID:   appID,
		ComputePlatform: computePlatform,
		AccountID:       b.accountID,
		Region:          b.region,
		CreationTime:    time.Now().UTC(),
		Tags:            t,
	}
	b.applications.Put(app)

	cp := *app

	return &cp, nil
}

// GetApplication returns an application by name.
func (b *InMemoryBackend) GetApplication(name string) (*Application, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	app, ok := b.applications.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, name)
	}

	cp := *app

	return &cp, nil
}

// ListApplications returns all application names in sorted order.
func (b *InMemoryBackend) ListApplications() []string {
	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	// Table.Snapshot returns entries sorted by primary key, which for
	// applications is ApplicationName, so this is already alphabetical.
	items := b.applications.Snapshot()
	names := make([]string, len(items))
	for i, app := range items {
		names[i] = app.ApplicationName
	}

	return names
}

// ListApplicationDetails returns all applications as structs.
func (b *InMemoryBackend) ListApplicationDetails() []*Application {
	b.mu.RLock("ListApplicationDetails")
	defer b.mu.RUnlock()

	all := b.applications.All()
	list := make([]*Application, 0, len(all))
	for _, app := range all {
		cp := *app
		list = append(list, &cp)
	}

	return list
}

// DeleteApplication deletes an application and all its deployment groups.
func (b *InMemoryBackend) DeleteApplication(name string) error {
	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	app, ok := b.applications.Get(name)
	if !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, name)
	}

	app.Tags.Close()

	// slices.Clone before the delete loop: deleting from b.deploymentGroups
	// mutates the same byApplication index bucket this slice is a view of.
	for _, dg := range slices.Clone(b.deploymentGroupsByApp.Get(name)) {
		dg.Tags.Close()
		b.deploymentGroups.Delete(dgKey(dg.ApplicationName, dg.DeploymentGroupName))
	}

	b.applications.Delete(name)

	return nil
}

// DeploymentGroupInput holds all the optional rich fields for CreateDeploymentGroup and UpdateDeploymentGroup.
type DeploymentGroupInput struct {
	BlueGreenDeploymentConfiguration *BlueGreenDeploymentConfiguration
	AlarmConfiguration               *AlarmConfiguration
	AutoRollbackConfiguration        *AutoRollbackConfiguration
	LoadBalancerInfo                 *LoadBalancerInfo
	DeploymentStyle                  *DeploymentStyle
	Ec2TagSet                        *Ec2TagSet
	OnPremisesTagSet                 *TagSet
	ServiceRoleArn                   string
	DeploymentConfigName             string
	OutdatedInstancesStrategy        string
	OnPremisesInstanceTagFilters     []TagFilter
	AutoScalingGroups                []AutoScalingGroup
	TriggerConfigurations            []TriggerConfiguration
	ECSServices                      []ECSService
	Ec2TagFilters                    []TagFilter
	TerminationHookEnabled           bool
}

// CreateDeploymentGroup creates a deployment group for an application.
func (b *InMemoryBackend) CreateDeploymentGroup(
	appName, dgName string,
	input DeploymentGroupInput,
	kv map[string]string,
) (*DeploymentGroup, error) {
	b.mu.Lock("CreateDeploymentGroup")
	defer b.mu.Unlock()

	app, ok := b.applications.Get(appName)
	if !ok {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	if b.deploymentGroups.Has(dgKey(appName, dgName)) {
		return nil, fmt.Errorf("%w: deployment group %s already exists", ErrDeploymentGroupAlreadyExists, dgName)
	}

	dgID := uuid.NewString()
	t := tags.New("codedeploy.dg." + appName + "." + dgName + ".tags")
	if len(kv) > 0 {
		t.Merge(kv)
	}

	if input.DeploymentConfigName == "" {
		input.DeploymentConfigName = "CodeDeployDefault.AllAtOnce"
	}

	dg := &DeploymentGroup{
		ApplicationName:                  appName,
		DeploymentGroupName:              dgName,
		DeploymentGroupID:                dgID,
		ServiceRoleArn:                   input.ServiceRoleArn,
		DeploymentConfigName:             input.DeploymentConfigName,
		ComputePlatform:                  app.ComputePlatform,
		AccountID:                        b.accountID,
		Region:                           b.region,
		Tags:                             t,
		Ec2TagFilters:                    input.Ec2TagFilters,
		OnPremisesInstanceTagFilters:     input.OnPremisesInstanceTagFilters,
		AutoScalingGroups:                input.AutoScalingGroups,
		LoadBalancerInfo:                 input.LoadBalancerInfo,
		DeploymentStyle:                  input.DeploymentStyle,
		Ec2TagSet:                        input.Ec2TagSet,
		OnPremisesTagSet:                 input.OnPremisesTagSet,
		BlueGreenDeploymentConfiguration: input.BlueGreenDeploymentConfiguration,
		AlarmConfiguration:               input.AlarmConfiguration,
		AutoRollbackConfiguration:        input.AutoRollbackConfiguration,
		TriggerConfigurations:            input.TriggerConfigurations,
		ECSServices:                      input.ECSServices,
		OutdatedInstancesStrategy:        input.OutdatedInstancesStrategy,
		TerminationHookEnabled:           input.TerminationHookEnabled,
	}

	b.deploymentGroups.Put(dg)

	cp := *dg

	return &cp, nil
}

// GetDeploymentGroup returns a deployment group by application and group name.
func (b *InMemoryBackend) GetDeploymentGroup(appName, dgName string) (*DeploymentGroup, error) {
	b.mu.RLock("GetDeploymentGroup")
	defer b.mu.RUnlock()

	dg, ok := b.deploymentGroups.Get(dgKey(appName, dgName))
	if !ok {
		return nil, fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
	}

	cp := *dg

	return &cp, nil
}

// UpdateDeploymentGroup updates a deployment group, optionally renaming it.
// Returns true if alarms or triggers were removed (hooksNotCleanedUp).
func (b *InMemoryBackend) UpdateDeploymentGroup(
	appName, currentDGName, newDGName string,
	input DeploymentGroupInput,
) (bool, error) {
	b.mu.Lock("UpdateDeploymentGroup")
	defer b.mu.Unlock()

	if !b.applications.Has(appName) {
		return false, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	oldKey := dgKey(appName, currentDGName)

	dg, ok := b.deploymentGroups.Get(oldKey)
	if !ok {
		return false, fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, currentDGName)
	}

	// Track whether hooks/alarms were previously configured and are now being removed.
	hooksNotCleanedUp := dg.AlarmConfiguration != nil && dg.AlarmConfiguration.Enabled &&
		(input.AlarmConfiguration == nil || !input.AlarmConfiguration.Enabled)

	if len(dg.TriggerConfigurations) > 0 && len(input.TriggerConfigurations) == 0 {
		hooksNotCleanedUp = true
	}

	if input.ServiceRoleArn != "" {
		dg.ServiceRoleArn = input.ServiceRoleArn
	}
	if input.DeploymentConfigName != "" {
		dg.DeploymentConfigName = input.DeploymentConfigName
	}
	if input.OutdatedInstancesStrategy != "" {
		dg.OutdatedInstancesStrategy = input.OutdatedInstancesStrategy
	}

	dg.Ec2TagFilters = input.Ec2TagFilters
	dg.OnPremisesInstanceTagFilters = input.OnPremisesInstanceTagFilters
	dg.AutoScalingGroups = input.AutoScalingGroups
	dg.LoadBalancerInfo = input.LoadBalancerInfo
	dg.DeploymentStyle = input.DeploymentStyle
	dg.Ec2TagSet = input.Ec2TagSet
	dg.OnPremisesTagSet = input.OnPremisesTagSet
	dg.BlueGreenDeploymentConfiguration = input.BlueGreenDeploymentConfiguration
	dg.AlarmConfiguration = input.AlarmConfiguration
	dg.AutoRollbackConfiguration = input.AutoRollbackConfiguration
	dg.TriggerConfigurations = input.TriggerConfigurations
	dg.ECSServices = input.ECSServices
	dg.TerminationHookEnabled = input.TerminationHookEnabled

	if newDGName != "" && newDGName != currentDGName {
		// DeploymentGroupName is part of the store.Table primary key (see
		// dgKey), so Put-after-in-place-mutate would leave a stale entry at
		// oldKey: delete first, then mutate, then Put under the new key.
		b.deploymentGroups.Delete(oldKey)
		dg.DeploymentGroupName = newDGName
		b.deploymentGroups.Put(dg)
	}

	return hooksNotCleanedUp, nil
}

// ListDeploymentGroups returns all deployment group names for an application in sorted order.
func (b *InMemoryBackend) ListDeploymentGroups(appName string) ([]string, error) {
	b.mu.RLock("ListDeploymentGroups")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	entries := b.deploymentGroupsByApp.Get(appName)
	names := make([]string, 0, len(entries))
	for _, dg := range entries {
		names = append(names, dg.DeploymentGroupName)
	}

	sort.Strings(names)

	return names, nil
}

// ListDeploymentGroupDetails returns all deployment groups for an application.
func (b *InMemoryBackend) ListDeploymentGroupDetails(appName string) ([]*DeploymentGroup, error) {
	b.mu.RLock("ListDeploymentGroupDetails")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	entries := b.deploymentGroupsByApp.Get(appName)
	list := make([]*DeploymentGroup, 0, len(entries))
	for _, dg := range entries {
		cp := *dg
		list = append(list, &cp)
	}

	return list, nil
}

// DeleteDeploymentGroup deletes a deployment group.
func (b *InMemoryBackend) DeleteDeploymentGroup(appName, dgName string) error {
	b.mu.Lock("DeleteDeploymentGroup")
	defer b.mu.Unlock()

	if !b.applications.Has(appName) {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	key := dgKey(appName, dgName)

	dg, ok := b.deploymentGroups.Get(key)
	if !ok {
		return fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
	}

	dg.Tags.Close()
	b.deploymentGroups.Delete(key)

	return nil
}

// DeploymentOptions holds optional per-deployment settings.
type DeploymentOptions struct {
	Revision                      *RevisionLocation
	FileExistsBehavior            string
	Description                   string
	Creator                       string
	UpdateOutdatedInstancesOnly   bool
	IgnoreApplicationStopFailures bool
}

// CreateDeployment creates a new deployment.
func (b *InMemoryBackend) CreateDeployment(appName, dgName string, opts DeploymentOptions) (*Deployment, error) {
	b.mu.Lock("CreateDeployment")
	defer b.mu.Unlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	dg, ok := b.deploymentGroups.Get(dgKey(appName, dgName))
	if !ok {
		return nil, fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
	}

	if opts.Creator == "" {
		opts.Creator = "user"
	}

	deployID := generateDeploymentID()
	now := time.Now().UTC()
	completed := now.Add(simulatedDeployDuration)

	d := &Deployment{
		DeploymentID:                  deployID,
		ApplicationName:               appName,
		DeploymentGroupName:           dgName,
		DeploymentConfigName:          dg.DeploymentConfigName,
		Status:                        statusSucceeded,
		Creator:                       opts.Creator,
		Description:                   opts.Description,
		FileExistsBehavior:            opts.FileExistsBehavior,
		UpdateOutdatedInstancesOnly:   opts.UpdateOutdatedInstancesOnly,
		IgnoreApplicationStopFailures: opts.IgnoreApplicationStopFailures,
		Revision:                      opts.Revision,
		CreateTime:                    now,
		CompleteTime:                  &completed,
		AccountID:                     b.accountID,
		Region:                        b.region,
	}
	b.deployments.Put(d)

	cp := *d

	return &cp, nil
}

// GetDeployment returns a deployment by ID.
func (b *InMemoryBackend) GetDeployment(deploymentID string) (*Deployment, error) {
	b.mu.RLock("GetDeployment")
	defer b.mu.RUnlock()

	d, ok := b.deployments.Get(deploymentID)
	if !ok {
		return nil, fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	cp := *d

	return &cp, nil
}

// DeploymentFilter holds optional filters for ListDeployments.
type DeploymentFilter struct {
	CreateTimeStart     *time.Time
	CreateTimeEnd       *time.Time
	ApplicationName     string
	DeploymentGroupName string
	Statuses            []string
}

// ListDeployments returns deployment IDs in sorted order, filtered by the provided criteria.
func (b *InMemoryBackend) ListDeployments(filter DeploymentFilter) []string {
	b.mu.RLock("ListDeployments")
	defer b.mu.RUnlock()

	statusSet := make(map[string]struct{}, len(filter.Statuses))
	for _, s := range filter.Statuses {
		statusSet[s] = struct{}{}
	}

	all := b.deployments.All()
	ids := make([]string, 0, len(all))

	for _, d := range all {
		if filter.ApplicationName != "" && d.ApplicationName != filter.ApplicationName {
			continue
		}

		if filter.DeploymentGroupName != "" && d.DeploymentGroupName != filter.DeploymentGroupName {
			continue
		}

		if len(statusSet) > 0 {
			if _, ok := statusSet[d.Status]; !ok {
				continue
			}
		}

		if filter.CreateTimeStart != nil && d.CreateTime.Before(*filter.CreateTimeStart) {
			continue
		}

		if filter.CreateTimeEnd != nil && d.CreateTime.After(*filter.CreateTimeEnd) {
			continue
		}

		ids = append(ids, d.DeploymentID)
	}

	sort.Strings(ids)

	return ids
}

// validateTagUpdate checks AWS tag limit rules.
func validateTagUpdate(existing map[string]string, additions map[string]string) error {
	for k, v := range additions {
		if strings.HasPrefix(k, tagReservedPrefix) {
			return fmt.Errorf("%w: tag key %q uses reserved prefix %q", ErrValidation, k, tagReservedPrefix)
		}
		if len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key exceeds maximum length of %d", ErrValidation, maxTagKeyLen)
		}
		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value for key %q exceeds maximum length of %d", ErrValidation, k, maxTagValueLen)
		}
	}

	projected := len(existing) + len(additions)
	// Overwriting existing keys does not increase count.
	for k := range additions {
		if _, alreadyExists := existing[k]; alreadyExists {
			projected--
		}
	}

	if projected > maxTagsPerResource {
		return fmt.Errorf("%w: resource would have %d tags, exceeding the maximum of %d",
			ErrTagLimitExceeded, projected, maxTagsPerResource)
	}

	return nil
}

// TagResource adds tags to a resource (application or deployment group) by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	t, err := b.findResourceTagsLocked(resourceARN)
	if err != nil {
		return err
	}

	existing := t.Clone()
	if valErr := validateTagUpdate(existing, kv); valErr != nil {
		return valErr
	}

	t.Merge(kv)

	return nil
}

// UntagResource removes tags from a resource (application or deployment group) by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	t, err := b.findResourceTagsLocked(resourceARN)
	if err != nil {
		return err
	}

	t.DeleteKeys(keys)

	return nil
}

// ListTagsForResource returns the tags for a resource (application or deployment group) by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t, err := b.findResourceTagsLocked(resourceARN)
	if err != nil {
		return nil, err
	}

	return t.Clone(), nil
}

// findResourceTagsLocked looks up the tags.Tags for a resource ARN.
// Supports application ARNs (arn:…:application:{name}) and deployment group ARNs
// (arn:…:deploymentgroup:{appName}/{groupName}).
// The caller must hold at least a read lock on b.mu before calling this method.
func (b *InMemoryBackend) findResourceTagsLocked(resourceARN string) (*tags.Tags, error) {
	parsed := parseARN(resourceARN)
	if parsed == nil {
		return nil, fmt.Errorf("%w: invalid ARN %s", ErrNotFound, resourceARN)
	}

	resourceType := parsed.resourceType
	resourceID := parsed.resourceID

	switch resourceType {
	case "application":
		app, ok := b.applications.Get(resourceID)
		if !ok {
			return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, resourceID)
		}

		return app.Tags, nil

	case "deploymentgroup":
		// deploymentgroup resource ID is "{appName}/{groupName}"
		appName, dgName, ok := strings.Cut(resourceID, "/")
		if !ok {
			return nil, fmt.Errorf("%w: invalid deployment group ARN %s", ErrNotFound, resourceARN)
		}

		dg, ok := b.deploymentGroups.Get(dgKey(appName, dgName))
		if !ok {
			return nil, fmt.Errorf("%w: deployment group %s not found", ErrDeploymentGroupNotFound, dgName)
		}

		return dg.Tags, nil

	default:
		return nil, fmt.Errorf("%w: unsupported resource type %s", ErrNotFound, resourceType)
	}
}

// parsedARN holds the parsed components of an AWS ARN relevant to CodeDeploy lookups.
type parsedARN struct {
	resourceType string
	resourceID   string
}

// parseARN parses an AWS ARN into resource type and resource ID.
// Format: arn:{partition}:{service}:{region}:{account}:{resourceType}:{resourceID}
// Handles non-standard partitions (aws-cn, aws-us-gov, etc.) correctly.
func parseARN(arnStr string) *parsedARN {
	if !strings.HasPrefix(arnStr, "arn:") {
		return nil
	}

	// Fixed-position split: arn : partition : service : region : account : resourceType : resourceID
	// Use SplitN with limit 7 to correctly handle resource IDs containing colons.
	parts := strings.SplitN(arnStr, ":", arnSegmentCount)
	if len(parts) != arnSegmentCount {
		return nil
	}

	return &parsedARN{
		resourceType: parts[5],
		resourceID:   parts[6],
	}
}

// ApplicationARN builds an ARN for a CodeDeploy application.
func (b *InMemoryBackend) ApplicationARN(name string) string {
	return arn.Build("codedeploy", b.region, b.accountID, "application:"+name)
}

// DeploymentGroupARN builds an ARN for a CodeDeploy deployment group.
func (b *InMemoryBackend) DeploymentGroupARN(appName, dgName string) string {
	return arn.Build("codedeploy", b.region, b.accountID, "deploymentgroup:"+appName+"/"+dgName)
}

// DeploymentConfigARN builds an ARN for a CodeDeploy deployment configuration.
func (b *InMemoryBackend) DeploymentConfigARN(name string) string {
	return arn.Build("codedeploy", b.region, b.accountID, "deploymentconfig:"+name)
}

// validComputePlatforms lists the accepted CodeDeploy compute platforms.
func validComputePlatforms() map[string]struct{} {
	return map[string]struct{}{
		computePlatformServer: {},
		computePlatformLambda: {},
		computePlatformECS:    {},
	}
}

// AddTagsToOnPremisesInstances adds tags to on-premises instances, registering them if needed.
func (b *InMemoryBackend) AddTagsToOnPremisesInstances(instanceNames []string, kv map[string]string) error {
	b.mu.Lock("AddTagsToOnPremisesInstances")
	defer b.mu.Unlock()

	for _, name := range instanceNames {
		inst, ok := b.onPremisesInstances.Get(name)
		if !ok {
			t := tags.New("codedeploy.onprem." + name + ".tags")
			inst = &OnPremisesInstance{
				InstanceName: name,
				RegisterTime: time.Now().UTC(),
				Tags:         t,
			}
			b.onPremisesInstances.Put(inst)
		}

		inst.Tags.Merge(kv)
	}

	return nil
}

// BatchGetApplicationRevisions validates that the application exists.
// It accepts up to maxBatchRevisions revisions per AWS spec.
func (b *InMemoryBackend) BatchGetApplicationRevisions(appName string, count int) (string, error) {
	b.mu.RLock("BatchGetApplicationRevisions")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return "", fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	if count > maxBatchRevisions {
		return "", fmt.Errorf("%w: at most %d revisions can be requested at once, got %d",
			ErrValidation, maxBatchRevisions, count)
	}

	return appName, nil
}

// BatchGetApplications returns application structs for the given names.
// Names that do not exist are silently omitted (AWS behavior).
func (b *InMemoryBackend) BatchGetApplications(names []string) []*Application {
	b.mu.RLock("BatchGetApplications")
	defer b.mu.RUnlock()

	result := make([]*Application, 0, len(names))

	for _, name := range names {
		app, ok := b.applications.Get(name)
		if !ok {
			continue
		}

		cp := *app
		result = append(result, &cp)
	}

	return result
}

// BatchGetDeploymentGroups returns deployment group info for the given names under an app.
// Groups that do not exist are silently omitted.
func (b *InMemoryBackend) BatchGetDeploymentGroups(appName string, dgNames []string) ([]*DeploymentGroup, error) {
	b.mu.RLock("BatchGetDeploymentGroups")
	defer b.mu.RUnlock()

	if !b.applications.Has(appName) {
		return nil, fmt.Errorf("%w: application %s not found", ErrNotFound, appName)
	}

	result := make([]*DeploymentGroup, 0, len(dgNames))

	for _, name := range dgNames {
		dg, ok := b.deploymentGroups.Get(dgKey(appName, name))
		if !ok {
			continue
		}

		cp := *dg
		result = append(result, &cp)
	}

	return result, nil
}

// BatchGetDeploymentInstances returns stub instance summaries for the given instance IDs.
// Missing deployment returns an error per AWS behavior.
func (b *InMemoryBackend) BatchGetDeploymentInstances(
	deploymentID string,
	instanceIDs []string,
) ([]InstanceSummaryItem, error) {
	b.mu.RLock("BatchGetDeploymentInstances")
	defer b.mu.RUnlock()

	d, ok := b.deployments.Get(deploymentID)
	if !ok {
		return nil, fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	result := make([]InstanceSummaryItem, 0, len(instanceIDs))

	for _, id := range instanceIDs {
		result = append(result, InstanceSummaryItem{
			DeploymentID: d.DeploymentID,
			InstanceID:   id,
			Status:       statusSucceeded,
		})
	}

	return result, nil
}

// BatchGetDeploymentTargets returns stub deployment targets for the given target IDs.
func (b *InMemoryBackend) BatchGetDeploymentTargets(
	deploymentID string,
	targetIDs []string,
) ([]*DeploymentTargetItem, error) {
	b.mu.RLock("BatchGetDeploymentTargets")
	defer b.mu.RUnlock()

	if !b.deployments.Has(deploymentID) {
		return nil, fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	result := make([]*DeploymentTargetItem, 0, len(targetIDs))

	for _, id := range targetIDs {
		result = append(result, &DeploymentTargetItem{
			DeploymentID: deploymentID,
			TargetID:     id,
			Status:       statusSucceeded,
			TargetType:   "instanceTarget",
		})
	}

	return result, nil
}

// BatchGetDeployments returns deployment structs for the given IDs.
// Deployment IDs that do not exist are silently omitted.
func (b *InMemoryBackend) BatchGetDeployments(deploymentIDs []string) []*Deployment {
	b.mu.RLock("BatchGetDeployments")
	defer b.mu.RUnlock()

	result := make([]*Deployment, 0, len(deploymentIDs))

	for _, id := range deploymentIDs {
		d, ok := b.deployments.Get(id)
		if !ok {
			continue
		}

		cp := *d
		result = append(result, &cp)
	}

	return result
}

// BatchGetOnPremisesInstances returns on-premises instance info for the given names.
// Names that do not exist are silently omitted.
func (b *InMemoryBackend) BatchGetOnPremisesInstances(instanceNames []string) []*OnPremisesInstance {
	b.mu.RLock("BatchGetOnPremisesInstances")
	defer b.mu.RUnlock()

	result := make([]*OnPremisesInstance, 0, len(instanceNames))

	for _, name := range instanceNames {
		inst, ok := b.onPremisesInstances.Get(name)
		if !ok {
			continue
		}

		cp := *inst
		result = append(result, &cp)
	}

	return result
}

// ContinueDeployment marks a blue/green deployment as continuing past the wait point.
func (b *InMemoryBackend) ContinueDeployment(deploymentID string) error {
	b.mu.Lock("ContinueDeployment")
	defer b.mu.Unlock()

	if !b.deployments.Has(deploymentID) {
		return fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	return nil
}

// CreateDeploymentConfig creates a named deployment configuration.
func (b *InMemoryBackend) CreateDeploymentConfig(
	name, computePlatform string,
	minHealthyHosts *MinimumHealthyHosts,
	trafficRouting *TrafficRoutingConfig,
	zonalConfig *ZonalConfig,
) (*DeploymentConfig, error) {
	b.mu.Lock("CreateDeploymentConfig")
	defer b.mu.Unlock()

	if b.deploymentConfigs.Has(name) {
		return nil, fmt.Errorf("%w: deployment config %s already exists", ErrDeploymentConfigAlreadyExists, name)
	}

	if computePlatform == "" {
		computePlatform = computePlatformServer
	}

	if _, ok := validComputePlatforms()[computePlatform]; !ok {
		return nil, fmt.Errorf("%w: invalid computePlatform %q, must be Server, Lambda, or ECS",
			ErrValidation, computePlatform)
	}

	cfg := &DeploymentConfig{
		DeploymentConfigName: name,
		DeploymentConfigID:   uuid.NewString(),
		ComputePlatform:      computePlatform,
		CreateTime:           time.Now().UTC(),
		MinimumHealthyHosts:  minHealthyHosts,
		TrafficRoutingConfig: trafficRouting,
		ZonalConfig:          zonalConfig,
	}
	b.deploymentConfigs.Put(cfg)

	cp := *cfg

	return &cp, nil
}

// InstanceSummaryItem is a simplified deployment instance summary used by BatchGetDeploymentInstances.
type InstanceSummaryItem struct {
	DeploymentID string `json:"deploymentId"`
	InstanceID   string `json:"instanceId"`
	Status       string `json:"status"`
}

// DeploymentTargetItem is a simplified deployment target used by BatchGetDeploymentTargets.
type DeploymentTargetItem struct {
	DeploymentID string `json:"deploymentId"`
	TargetID     string `json:"targetId"`
	Status       string `json:"status"`
	TargetType   string `json:"targetType"`
}

// AddApplicationInternal adds an application directly to the backend without validation.
// Used for test seeding only.
func (b *InMemoryBackend) AddApplicationInternal(app *Application) {
	b.mu.Lock("AddApplicationInternal")
	defer b.mu.Unlock()

	app.Tags = ensureTags(app.Tags, "codedeploy.application."+app.ApplicationName+".tags")

	if app.ApplicationID == "" {
		app.ApplicationID = uuid.NewString()
	}

	if app.CreationTime.IsZero() {
		app.CreationTime = time.Now().UTC()
	}

	b.applications.Put(app)
}

// AddDeploymentGroupInternal adds a deployment group directly to the backend without validation.
// Used for test seeding only.
func (b *InMemoryBackend) AddDeploymentGroupInternal(dg *DeploymentGroup) {
	b.mu.Lock("AddDeploymentGroupInternal")
	defer b.mu.Unlock()

	dg.Tags = ensureTags(dg.Tags, "codedeploy.dg."+dg.ApplicationName+"."+dg.DeploymentGroupName+".tags")

	if dg.DeploymentGroupID == "" {
		dg.DeploymentGroupID = uuid.NewString()
	}

	b.deploymentGroups.Put(dg)
}

// AddDeploymentInternal adds a deployment directly to the backend without validation.
// Used for test seeding only.
func (b *InMemoryBackend) AddDeploymentInternal(d *Deployment) {
	b.mu.Lock("AddDeploymentInternal")
	defer b.mu.Unlock()

	if d.DeploymentID == "" {
		d.DeploymentID = generateDeploymentID()
	}

	if d.CreateTime.IsZero() {
		d.CreateTime = time.Now().UTC()
	}

	b.deployments.Put(d)
}

// AddOnPremisesInstanceInternal adds an on-premises instance directly to the backend.
// Used for test seeding only.
func (b *InMemoryBackend) AddOnPremisesInstanceInternal(inst *OnPremisesInstance) {
	b.mu.Lock("AddOnPremisesInstanceInternal")
	defer b.mu.Unlock()

	inst.Tags = ensureTags(inst.Tags, "codedeploy.onprem."+inst.InstanceName+".tags")

	if inst.RegisterTime.IsZero() {
		inst.RegisterTime = time.Now().UTC()
	}

	b.onPremisesInstances.Put(inst)
}

// UpdateApplication renames a CodeDeploy application, updating all referencing deployment
// groups and deployments.
func (b *InMemoryBackend) UpdateApplication(name, newName string) error {
	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	app, ok := b.applications.Get(name)
	if !ok {
		return fmt.Errorf("%w: application %s not found", ErrNotFound, name)
	}

	if newName == "" || newName == name {
		return nil
	}

	if b.applications.Has(newName) {
		return fmt.Errorf("%w: application %s already exists", ErrAlreadyExists, newName)
	}

	// ApplicationName is the store.Table primary key, so Put-after-in-place-
	// mutate would leave a stale entry at the old key: delete, mutate, Put.
	b.applications.Delete(name)
	app.ApplicationName = newName
	b.applications.Put(app)

	// Move all deployment groups for this application to the new app name.
	// DeploymentGroup.ApplicationName is both part of the deploymentGroups
	// primary key (see dgKey) and the deploymentGroupsByApp index key, so
	// each affected group needs the same delete/mutate/Put treatment.
	// slices.Clone before the delete loop: deleting mutates the same
	// byApplication index bucket this slice is a view of.
	for _, dg := range slices.Clone(b.deploymentGroupsByApp.Get(name)) {
		b.deploymentGroups.Delete(dgKey(dg.ApplicationName, dg.DeploymentGroupName))
		dg.ApplicationName = newName
		b.deploymentGroups.Put(dg)
	}

	// Update all existing deployments that reference the old application
	// name. ApplicationName is not part of Deployment's primary key
	// (DeploymentID is), so in-place mutation is safe here.
	for _, d := range b.deployments.All() {
		if d.ApplicationName == name {
			d.ApplicationName = newName
		}
	}

	return nil
}

// StopDeployment marks a deployment as Stopped.
func (b *InMemoryBackend) StopDeployment(deploymentID string) error {
	b.mu.Lock("StopDeployment")
	defer b.mu.Unlock()

	d, ok := b.deployments.Get(deploymentID)
	if !ok {
		return fmt.Errorf("%w: deployment %s not found", ErrDeploymentNotFound, deploymentID)
	}

	d.Status = statusStopped

	return nil
}

// GetDeploymentConfig returns a deployment configuration by name.
func (b *InMemoryBackend) GetDeploymentConfig(name string) (*DeploymentConfig, error) {
	b.mu.RLock("GetDeploymentConfig")
	defer b.mu.RUnlock()

	cfg, ok := b.deploymentConfigs.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: deployment config %s not found", ErrDeploymentConfigNotFound, name)
	}

	cp := *cfg

	return &cp, nil
}

// ListDeploymentConfigs returns all deployment config names in sorted order.
func (b *InMemoryBackend) ListDeploymentConfigs() []string {
	b.mu.RLock("ListDeploymentConfigs")
	defer b.mu.RUnlock()

	// Table.Snapshot returns entries sorted by primary key, which for
	// deploymentConfigs is DeploymentConfigName, so this is already alphabetical.
	items := b.deploymentConfigs.Snapshot()
	names := make([]string, len(items))
	for i, cfg := range items {
		names[i] = cfg.DeploymentConfigName
	}

	return names
}

// DeleteDeploymentConfig deletes a deployment configuration by name.
// AWS-default configs (IsDefault=true) cannot be deleted.
func (b *InMemoryBackend) DeleteDeploymentConfig(name string) error {
	b.mu.Lock("DeleteDeploymentConfig")
	defer b.mu.Unlock()

	cfg, ok := b.deploymentConfigs.Get(name)
	if !ok {
		return fmt.Errorf("%w: deployment config %s not found", ErrDeploymentConfigNotFound, name)
	}

	if cfg.IsDefault {
		return fmt.Errorf("%w: cannot delete built-in deployment config %s", ErrDeploymentConfigInUse, name)
	}

	b.deploymentConfigs.Delete(name)

	return nil
}

// RemoveTagsFromOnPremisesInstances removes tag keys from the given on-premises instances.
func (b *InMemoryBackend) RemoveTagsFromOnPremisesInstances(instanceNames []string, keys []string) error {
	b.mu.Lock("RemoveTagsFromOnPremisesInstances")
	defer b.mu.Unlock()

	for _, name := range instanceNames {
		inst, ok := b.onPremisesInstances.Get(name)
		if !ok {
			continue
		}

		inst.Tags.DeleteKeys(keys)
	}

	return nil
}

// RegisterOnPremisesInstance registers a new on-premises instance.
// Exactly one of iamSessionArn or iamUserArn must be set.
func (b *InMemoryBackend) RegisterOnPremisesInstance(name, iamSessionArn, iamUserArn string) error {
	b.mu.Lock("RegisterOnPremisesInstance")
	defer b.mu.Unlock()

	if !onPremInstanceNameRe.MatchString(name) {
		return fmt.Errorf("%w: instance name %q does not match pattern [A-Za-z0-9._-]{1,100}", ErrValidation, name)
	}

	if iamSessionArn != "" && iamUserArn != "" {
		return fmt.Errorf("%w: only one of iamSessionArn or iamUserArn may be set", ErrMultipleIamArns)
	}

	if iamSessionArn == "" && iamUserArn == "" {
		return fmt.Errorf("%w: one of iamSessionArn or iamUserArn must be set", ErrIamArnRequired)
	}

	if b.onPremisesInstances.Has(name) {
		return nil
	}

	t := tags.New("codedeploy.onprem." + name + ".tags")
	b.onPremisesInstances.Put(&OnPremisesInstance{
		InstanceName:  name,
		IamSessionArn: iamSessionArn,
		IamUserArn:    iamUserArn,
		RegisterTime:  time.Now().UTC(),
		Tags:          t,
	})

	return nil
}

// DeregisterOnPremisesInstance marks an on-premises instance as deregistered.
func (b *InMemoryBackend) DeregisterOnPremisesInstance(name string) error {
	b.mu.Lock("DeregisterOnPremisesInstance")
	defer b.mu.Unlock()

	inst, ok := b.onPremisesInstances.Get(name)
	if !ok {
		return fmt.Errorf("%w: instance %s not found", ErrOnPremisesInstanceNotFound, name)
	}

	now := time.Now().UTC()
	inst.DeregisterTime = &now

	return nil
}

// GetOnPremisesInstance returns an on-premises instance by name.
func (b *InMemoryBackend) GetOnPremisesInstance(name string) (*OnPremisesInstance, error) {
	b.mu.RLock("GetOnPremisesInstance")
	defer b.mu.RUnlock()

	inst, ok := b.onPremisesInstances.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: instance %s not found", ErrOnPremisesInstanceNotFound, name)
	}

	cp := *inst

	return &cp, nil
}

// ListOnPremisesInstances returns instance names, optionally filtered by registration status and tag filters.
func (b *InMemoryBackend) ListOnPremisesInstances(registrationStatus string, tagFilters []TagFilter) []string {
	b.mu.RLock("ListOnPremisesInstances")
	defer b.mu.RUnlock()

	all := b.onPremisesInstances.All()
	names := make([]string, 0, len(all))
	for _, inst := range all {
		if registrationStatus == "Deregistered" && inst.DeregisterTime == nil {
			continue
		}
		if registrationStatus == "Registered" && inst.DeregisterTime != nil {
			continue
		}
		if len(tagFilters) > 0 && !matchesTagFilters(inst.Tags, tagFilters) {
			continue
		}
		names = append(names, inst.InstanceName)
	}

	sort.Strings(names)

	return names
}

// matchesTagFilters returns true if the tags satisfy all the given filters.
//
//nolint:gocognit // tag filter matching requires nested condition evaluation
func matchesTagFilters(t *tags.Tags, filters []TagFilter) bool {
	if t == nil {
		return len(filters) == 0
	}

	kv := t.Clone()

	for _, f := range filters {
		switch f.Type {
		case "KEY_ONLY":
			if _, ok := kv[f.Key]; !ok {
				return false
			}
		case "VALUE_ONLY":
			found := false
			for _, v := range kv {
				if v == f.Value {
					found = true

					break
				}
			}
			if !found {
				return false
			}
		default: // EQUALS or empty
			if v, ok := kv[f.Key]; !ok || v != f.Value {
				return false
			}
		}
	}

	return true
}

// AddDeploymentConfigInternal adds a deployment config directly to the backend without validation.
// Used for test seeding only.
func (b *InMemoryBackend) AddDeploymentConfigInternal(cfg *DeploymentConfig) {
	b.mu.Lock("AddDeploymentConfigInternal")
	defer b.mu.Unlock()

	if cfg.DeploymentConfigID == "" {
		cfg.DeploymentConfigID = uuid.NewString()
	}

	if cfg.CreateTime.IsZero() {
		cfg.CreateTime = time.Now().UTC()
	}

	b.deploymentConfigs.Put(cfg)
}

// ListGitHubAccountTokenNames returns all stored GitHub account token names.
func (b *InMemoryBackend) ListGitHubAccountTokenNames() []string {
	b.mu.RLock("ListGitHubAccountTokenNames")
	defer b.mu.RUnlock()

	names := collections.SortedKeys(b.githubTokens)

	return names
}

// DeleteGitHubAccountToken removes a stored GitHub account token name.
func (b *InMemoryBackend) DeleteGitHubAccountToken(name string) error {
	b.mu.Lock("DeleteGitHubAccountToken")
	defer b.mu.Unlock()

	if _, ok := b.githubTokens[name]; !ok {
		return fmt.Errorf("%w: GitHub account token %s not found", ErrGitHubAccountTokenNotFound, name)
	}

	delete(b.githubTokens, name)

	return nil
}

// AddGitHubAccountTokenInternal adds a GitHub token name directly (for test seeding and internal use).
func (b *InMemoryBackend) AddGitHubAccountTokenInternal(name string) {
	b.mu.Lock("AddGitHubAccountTokenInternal")
	defer b.mu.Unlock()

	b.githubTokens[name] = struct{}{}
}
