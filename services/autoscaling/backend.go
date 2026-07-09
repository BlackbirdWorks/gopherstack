package autoscaling

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// completedProgress is the progress value for a successfully completed scaling activity.
const completedProgress = int32(100)

// maxDesiredCapacity is the upper bound on DesiredCapacity for any ASG, used to
// cap user-supplied values and prevent excessive slice allocations
// (go/slice-memory-allocation-excessive-size).
const maxDesiredCapacity = 100

const (
	lifecycleActionContinue = "CONTINUE"
	lifecycleActionAbandon  = "ABANDON"
	// healthStatusHealthy is the health status for a healthy instance.
	healthStatusHealthy = "Healthy"
	// lifecycleStateInService is the lifecycle state for a running, healthy instance.
	lifecycleStateInService = "InService"
	// lifecycleStatePendingWait is the lifecycle state for an instance launching but
	// paused for an EC2_INSTANCE_LAUNCHING lifecycle hook to complete.
	lifecycleStatePendingWait = "Pending:Wait"
	// lifecycleStateTerminatingWait is the lifecycle state for an instance being
	// terminated but paused for an EC2_INSTANCE_TERMINATING lifecycle hook to complete.
	lifecycleStateTerminatingWait = "Terminating:Wait"
	// transitionLaunching and transitionTerminating are the two AWS-defined lifecycle
	// hook transition points.
	transitionLaunching   = "autoscaling:EC2_INSTANCE_LAUNCHING"
	transitionTerminating = "autoscaling:EC2_INSTANCE_TERMINATING"
	// statusCodeSuccessful is the status code for a successfully completed scaling activity.
	statusCodeSuccessful = "Successful"
	// statusInProgress is the status for an in-progress instance refresh or scaling activity.
	statusInProgress = "InProgress"
	// granularity1Minute is the only supported CloudWatch metric granularity.
	granularity1Minute = "1Minute"
	// lbStateAdded is the state for a load balancer that has been attached to the ASG.
	lbStateAdded = "InService"
	// maxAccountASGs is the simulated account limit for Auto Scaling groups.
	maxAccountASGs = int32(200)
	// maxAccountLaunchConfigs is the simulated account limit for launch configurations.
	maxAccountLaunchConfigs = int32(200)
	// percentDivisor is used when computing PercentChangeInCapacity adjustments.
	percentDivisor = 100.0
)

// defaultAvailabilityZone is the fallback AZ used when none is specified.
const defaultAvailabilityZone = "us-east-1a"

var (
	// ErrGroupNotFound is returned when the requested Auto Scaling group does not exist.
	ErrGroupNotFound = errors.New("AutoScalingGroupNotFound")
	// ErrGroupAlreadyExists is returned when an Auto Scaling group with that name already exists.
	ErrGroupAlreadyExists = errors.New("AlreadyExists")
	// ErrLaunchConfigurationNotFound is returned when a launch configuration does not exist.
	ErrLaunchConfigurationNotFound = errors.New("LaunchConfigurationNotFound")
	// ErrLaunchConfigurationAlreadyExists is returned when a launch configuration already exists.
	ErrLaunchConfigurationAlreadyExists = errors.New("AlreadyExists")
	// ErrUnknownAction is returned when the requested action is not recognized.
	ErrUnknownAction = errors.New("InvalidAction")
	// ErrInvalidParameter is returned when a request parameter is invalid.
	ErrInvalidParameter = errors.New("ValidationError")
	// ErrActiveInstanceRefreshNotFound is returned when no active instance refresh exists.
	ErrActiveInstanceRefreshNotFound = errors.New("ActiveInstanceRefreshNotFound")
	// ErrLifecycleHookNotFound is returned when the specified lifecycle hook does not exist.
	ErrLifecycleHookNotFound = errors.New("ValidationError")
	// ErrScalingActivityInProgress is returned when a delete is attempted on a group with active instances
	// and ForceDelete is not set.
	ErrScalingActivityInProgress = errors.New("ScalingActivityInProgress")
	// ErrInstanceNotFound is returned when a specific instance ID is not found in an ASG.
	ErrInstanceNotFound = errors.New("ValidationError")
	// ErrWarmPoolNotFound is returned when no warm pool exists for the group.
	ErrWarmPoolNotFound = errors.New("ValidationError")
	// ErrPolicyNotFound is returned when the specified scaling policy does not exist.
	ErrPolicyNotFound = errors.New("ValidationError")
)

// StorageBackend is the interface for the Autoscaling in-memory store.
type StorageBackend interface {
	CreateAutoScalingGroup(input CreateAutoScalingGroupInput) (*AutoScalingGroup, error)
	DescribeAutoScalingGroups(names []string) ([]AutoScalingGroup, error)
	UpdateAutoScalingGroup(input UpdateAutoScalingGroupInput) (*AutoScalingGroup, error)
	DeleteAutoScalingGroup(name string, forceDelete bool) error

	CreateLaunchConfiguration(input CreateLaunchConfigurationInput) (*LaunchConfiguration, error)
	DescribeLaunchConfigurations(names []string) ([]LaunchConfiguration, error)
	DeleteLaunchConfiguration(name string) error

	DescribeScalingActivities(groupName string) ([]ScalingActivity, error)

	AttachInstances(groupName string, instanceIDs []string) error
	AttachLoadBalancerTargetGroups(groupName string, targetGroupARNs []string) error
	AttachLoadBalancers(groupName string, loadBalancerNames []string) error
	AttachTrafficSources(groupName string, trafficSources []TrafficSource) error

	BatchDeleteScheduledAction(
		groupName string,
		scheduledActionNames []string,
	) ([]FailedScheduledAction, error)
	BatchPutScheduledUpdateGroupAction(
		groupName string,
		actions []ScheduledUpdateGroupAction,
	) ([]FailedScheduledAction, error)

	CancelInstanceRefresh(groupName string) (string, error)
	CompleteLifecycleAction(input CompleteLifecycleActionInput) error
	CreateOrUpdateTags(tags []ResourceTag) error
	DeleteLifecycleHook(groupName, hookName string) error

	SetDesiredCapacity(groupName string, desiredCapacity int32) error
	TerminateInstanceInAutoScalingGroup(instanceID string, shouldDecrement bool) (*ScalingActivity, error)
	PutLifecycleHook(hook LifecycleHook) error
	DescribeLifecycleHooks(groupName string, hookNames []string) ([]LifecycleHook, error)
	DescribeScheduledActions(groupName string, actionNames []string) ([]ScheduledAction, error)
	DeleteTags(tags []ResourceTag) error
	DescribeTags(filters []TagFilter) ([]ResourceTag, error)
	DescribeAutoScalingInstances(instanceIDs []string) ([]InstanceDetails, error)

	// Static describe methods
	DescribeAccountLimits() (*AccountLimits, error)
	DescribeAdjustmentTypes() ([]string, error)
	DescribeAutoScalingNotificationTypes() ([]string, error)
	DescribeLifecycleHookTypes() ([]string, error)
	DescribeMetricCollectionTypes() ([]MetricCollectionType, error)
	DescribeScalingProcessTypes() ([]string, error)
	DescribeTerminationPolicyTypes() ([]string, error)

	// Instance refresh
	StartInstanceRefresh(groupName string) (*InstanceRefresh, error)
	StartInstanceRefreshWithInput(input StartInstanceRefreshInput) (*InstanceRefresh, error)
	RollbackInstanceRefresh(groupName string) (string, error)
	DescribeInstanceRefreshes(groupName string, refreshIDs []string) ([]InstanceRefresh, error)

	// LB/TG/Traffic describe
	DescribeLoadBalancers(groupName string) ([]LoadBalancerState, error)
	DescribeLoadBalancerTargetGroups(groupName string) ([]LoadBalancerTargetGroupState, error)
	DescribeTrafficSources(groupName string) ([]TrafficSourceState, error)

	// Detach operations
	DetachInstances(groupName string, instanceIDs []string, shouldDecrement bool) ([]ScalingActivity, error)
	DetachLoadBalancerTargetGroups(groupName string, targetGroupARNs []string) error
	DetachLoadBalancers(groupName string, lbNames []string) error
	DetachTrafficSources(groupName string, trafficSources []TrafficSource) error

	// Metrics
	EnableMetricsCollection(groupName string, metrics []string, granularity string) error
	DisableMetricsCollection(groupName string, metrics []string) error

	// Process ops
	SuspendProcesses(groupName string, processes []string) error
	ResumeProcesses(groupName string, processes []string) error

	// Standby
	EnterStandby(groupName string, instanceIDs []string, decrementCapacity bool) ([]ScalingActivity, error)
	ExitStandby(groupName string, instanceIDs []string) ([]ScalingActivity, error)

	// Instance state
	SetInstanceHealth(instanceID string, healthStatus string, shouldRespectGracePeriod bool) error
	SetInstanceProtection(groupName string, instanceIDs []string, protectedFromScaleIn bool) error

	// Lifecycle heartbeat
	RecordLifecycleActionHeartbeat(input RecordLifecycleActionHeartbeatInput) error

	// Execute policy
	ExecutePolicy(input ExecutePolicyInput) error

	// Launch instances
	LaunchInstances(groupName string, count int32) ([]Instance, error)

	// Predictive scaling
	GetPredictiveScalingForecast(groupName string) error

	// Notification configs
	PutNotificationConfiguration(groupName, topicARN string, types []string) error
	DeleteNotificationConfiguration(groupName, topicARN string) error
	DescribeNotificationConfigurations(groupNames []string) ([]NotificationConfiguration, error)

	// Scaling policies
	PutScalingPolicy(input ScalingPolicyInput) (*ScalingPolicy, error)
	DeletePolicy(groupName, policyNameOrARN string) error
	DescribePolicies(groupName string, policyNames []string) ([]ScalingPolicy, error)

	// Scheduled actions (single)
	PutScheduledUpdateGroupAction(groupName string, action ScheduledUpdateGroupAction) error
	DeleteScheduledAction(groupName, scheduledActionName string) error

	// Warm pool
	PutWarmPool(input WarmPoolInput) error
	DeleteWarmPool(groupName string) error
	DescribeWarmPool(groupName string) (*WarmPool, error)
}

// CreateAutoScalingGroupInput holds the input for CreateAutoScalingGroup.
type CreateAutoScalingGroupInput struct {
	MixedInstancesPolicy             *MixedInstancesPolicy
	LaunchTemplate                   *LaunchTemplateSpecification
	AutoScalingGroupName             string
	LaunchConfigurationName          string
	HealthCheckType                  string
	VPCZoneIdentifier                string
	PlacementGroup                   string
	Context                          string
	DesiredCapacityType              string
	LoadBalancerNames                []string
	TargetGroupARNs                  []string
	TerminationPolicies              []string
	Tags                             []Tag
	AvailabilityZones                []string
	TrafficSources                   []TrafficSource
	LifecycleHookSpecificationList   []LifecycleHook
	MinSize                          int32
	DefaultCooldown                  int32
	HealthCheckGracePeriod           int32
	MaxInstanceLifetime              int32
	DefaultInstanceWarmup            int32
	MaxSize                          int32
	DesiredCapacity                  int32
	NewInstancesProtectedFromScaleIn bool
	CapacityRebalance                bool
}

// UpdateAutoScalingGroupInput holds the input for UpdateAutoScalingGroup.
type UpdateAutoScalingGroupInput struct {
	LaunchTemplate                   *LaunchTemplateSpecification
	MaxSize                          *int32
	DesiredCapacity                  *int32
	DefaultCooldown                  *int32
	HealthCheckGracePeriod           *int32
	MaxInstanceLifetime              *int32
	DefaultInstanceWarmup            *int32
	NewInstancesProtectedFromScaleIn *bool
	CapacityRebalance                *bool
	MixedInstancesPolicy             *MixedInstancesPolicy
	MinSize                          *int32
	LaunchConfigurationName          string
	VPCZoneIdentifier                string
	PlacementGroup                   string
	Context                          string
	DesiredCapacityType              string
	HealthCheckType                  string
	AutoScalingGroupName             string
	AvailabilityZones                []string
	TerminationPolicies              []string
}

// CreateLaunchConfigurationInput holds the input for CreateLaunchConfiguration.
type CreateLaunchConfigurationInput struct {
	LaunchConfigurationName      string
	ImageID                      string
	InstanceType                 string
	KeyName                      string
	IAMInstanceProfile           string
	UserData                     string
	KernelID                     string
	RamdiskID                    string
	SpotPrice                    string
	PlacementTenancy             string
	ClassicLinkVPCID             string
	SecurityGroups               []string
	ClassicLinkVPCSecurityGroups []string
	BlockDeviceMappings          []BlockDeviceMapping
	AssociatePublicIPAddress     bool
	EbsOptimized                 bool
	InstanceMonitoring           bool
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	groups                  *store.Table[AutoScalingGroup]
	launchConfigurations    *store.Table[LaunchConfiguration]
	activities              map[string][]ScalingActivity
	scheduledActions        *store.Table[ScheduledAction]
	scheduledActionsByGroup *store.Index[ScheduledAction]
	instanceRefreshes       map[string][]*InstanceRefresh
	lifecycleHooks          *store.Table[LifecycleHook]
	lifecycleHooksByGroup   *store.Index[LifecycleHook]
	scalingPolicies         *store.Table[ScalingPolicy]
	scalingPoliciesByGroup  *store.Index[ScalingPolicy]
	notificationConfigs     map[string][]*NotificationConfiguration
	warmPools               *store.Table[WarmPool]
	// pendingHookTokens is a *store.Table for Get/Put/Delete/Range convenience
	// but is deliberately NOT registered on registry — see registerAllTables.
	pendingHookTokens *store.Table[pendingHookAction]
	registry          *store.Registry
	// instanceIndex maps instanceID → groupName for O(1) lookup.
	instanceIndex map[string]string
	mu            *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		activities:          make(map[string][]ScalingActivity),
		instanceRefreshes:   make(map[string][]*InstanceRefresh),
		notificationConfigs: make(map[string][]*NotificationConfiguration),
		instanceIndex:       make(map[string]string),
		registry:            store.NewRegistry(),
		mu:                  lockmetrics.New("autoscaling"),
	}
	registerAllTables(b)

	return b
}

// Close stops any in-flight lifecycle-hook expiry timers so their goroutines do
// not outlive the backend. It is safe to call multiple times.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	defer b.mu.Unlock()

	b.pendingHookTokens.Range(func(action *pendingHookAction) bool {
		action.timer.Stop()

		return true
	})
	b.pendingHookTokens.Reset()
}

// lcInstanceType returns the InstanceType from the named launch configuration, or
// "t2.micro" if the launch configuration is not found (preserving previous default).
func lcInstanceType(lcs *store.Table[LaunchConfiguration], lcName string) string {
	if lc, ok := lcs.Get(lcName); ok && lc.InstanceType != "" {
		return lc.InstanceType
	}

	return "t2.micro"
}

// makeInstances creates the desired number of healthy InService instances for an ASG.
// The fake service immediately puts instances in InService/Healthy state so that
// Terraform provider capacity checks do not time out.
func makeInstances(count int32, azs []string, launchConfigName, instanceType string) []Instance {
	// Clamp to valid range before use to avoid CodeQL
	// go/slice-memory-allocation-excessive-size on the capacity hint.
	n := max(0, min(maxDesiredCapacity, int(count)))
	if n == 0 {
		return []Instance{}
	}

	az := defaultAvailabilityZone
	if len(azs) > 0 {
		az = azs[0]
	}

	// No capacity hint — append grows naturally; any user-derived value in
	// the capacity position would trigger CodeQL go/slice-memory-allocation-excessive-size.
	// nolint:prealloc,nolintlint // satisfies CodeQL by removing tainted capacity hint
	instances := make([]Instance, 0)

	now := time.Now()
	for range n {
		// Use full UUID (stripped of dashes) to generate a unique, collision-free instance ID.
		id := "i-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:17]
		instances = append(instances, Instance{
			InstanceID:              id,
			AvailabilityZone:        az,
			LifecycleState:          lifecycleStateInService,
			HealthStatus:            healthStatusHealthy,
			LaunchConfigurationName: launchConfigName,
			InstanceType:            instanceType,
			LaunchTime:              now,
		})
	}

	return instances
}

// adjustInstances adjusts the instances slice to match the new desired count.
// It adds or removes instances from the end, preserving existing instance IDs.
func adjustInstances(
	existing []Instance, desired int32, azs []string, launchConfigName, instanceType string,
) []Instance {
	current := len(existing)
	want := int(desired)

	if want == current {
		return existing
	}

	if want < current {
		return existing[:want]
	}

	// Add new instances for the delta.
	delta := desired - int32(current) //nolint:gosec // current <= math.MaxInt32 (bounded by desired which is int32)

	return append(existing, makeInstances(delta, azs, launchConfigName, instanceType)...)
}

// CreateAutoScalingGroup creates a new Auto Scaling group.
//
//nolint:funlen,cyclop // Too complex to refactor given time constraints
func (b *InMemoryBackend) CreateAutoScalingGroup(input CreateAutoScalingGroupInput) (*AutoScalingGroup, error) {
	b.mu.Lock("CreateAutoScalingGroup")
	defer b.mu.Unlock()

	if input.AutoScalingGroupName == "" {
		return nil, fmt.Errorf("%w: AutoScalingGroupName is required", ErrInvalidParameter)
	}

	if b.groups.Has(input.AutoScalingGroupName) {
		return nil, fmt.Errorf("%w: group %q already exists", ErrGroupAlreadyExists, input.AutoScalingGroupName)
	}

	// Validate capacity constraints: MinSize ≤ DesiredCapacity ≤ MaxSize.
	desired := input.DesiredCapacity
	if desired == 0 {
		desired = input.MinSize
	}

	if err := validateCapacity(input.MinSize, input.MaxSize, desired); err != nil {
		return nil, err
	}

	healthCheckType := input.HealthCheckType
	if healthCheckType == "" {
		healthCheckType = "EC2"
	}

	if err := validateHealthCheckType(healthCheckType); err != nil {
		return nil, err
	}

	if err := validateTerminationPolicies(input.TerminationPolicies); err != nil {
		return nil, err
	}

	azs := input.AvailabilityZones
	if len(azs) == 0 {
		azs = []string{defaultAvailabilityZone}
	}

	normalizedHooks := make([]LifecycleHook, 0, len(input.LifecycleHookSpecificationList))

	for _, hook := range input.LifecycleHookSpecificationList {
		if hook.LifecycleHookName == "" {
			return nil, fmt.Errorf("%w: LifecycleHookName is required", ErrInvalidParameter)
		}

		hook.AutoScalingGroupName = input.AutoScalingGroupName
		if err := normalizeLifecycleHook(&hook); err != nil {
			return nil, err
		}

		normalizedHooks = append(normalizedHooks, hook)
	}

	// Use the shared makeInstances helper so all instance IDs use the same format.
	instances := makeInstances(
		desired, azs, input.LaunchConfigurationName,
		lcInstanceType(b.launchConfigurations, input.LaunchConfigurationName),
	)

	group := &AutoScalingGroup{
		AutoScalingGroupName: input.AutoScalingGroupName,
		AutoScalingGroupARN: fmt.Sprintf(
			"arn:aws:autoscaling:%s:%s:autoScalingGroup:%s:autoScalingGroupName/%s",
			config.DefaultRegion, config.DefaultAccountID, uuid.NewString(), input.AutoScalingGroupName,
		),
		LaunchConfigurationName:          input.LaunchConfigurationName,
		LaunchTemplate:                   input.LaunchTemplate,
		MixedInstancesPolicy:             input.MixedInstancesPolicy,
		VPCZoneIdentifier:                input.VPCZoneIdentifier,
		PlacementGroup:                   input.PlacementGroup,
		Context:                          input.Context,
		DesiredCapacityType:              input.DesiredCapacityType,
		MinSize:                          input.MinSize,
		MaxSize:                          input.MaxSize,
		DesiredCapacity:                  desired,
		DefaultCooldown:                  input.DefaultCooldown,
		HealthCheckType:                  healthCheckType,
		HealthCheckGracePeriod:           input.HealthCheckGracePeriod,
		MaxInstanceLifetime:              input.MaxInstanceLifetime,
		DefaultInstanceWarmup:            input.DefaultInstanceWarmup,
		NewInstancesProtectedFromScaleIn: input.NewInstancesProtectedFromScaleIn,
		CapacityRebalance:                input.CapacityRebalance,
		AvailabilityZones:                azs,
		LoadBalancerNames:                input.LoadBalancerNames,
		TargetGroupARNs:                  input.TargetGroupARNs,
		TrafficSources:                   input.TrafficSources,
		Tags:                             input.Tags,
		TerminationPolicies:              input.TerminationPolicies,
		Instances:                        instances,
		CreatedTime:                      time.Now(),
	}

	if input.NewInstancesProtectedFromScaleIn {
		for i := range group.Instances {
			group.Instances[i].ProtectedFromScaleIn = true
		}
	}

	b.groups.Put(group)

	// Register any initial lifecycle hooks BEFORE gating instances so a launch hook
	// specified at creation time applies to the group's own initial instances too.
	for _, hook := range normalizedHooks {
		cp := hook
		b.lifecycleHooks.Put(&cp)
	}

	b.gateNewLaunchInstances(group, 0)

	for _, inst := range group.Instances {
		b.instanceIndex[inst.InstanceID] = input.AutoScalingGroupName
	}

	b.activities[input.AutoScalingGroupName] = append(
		b.activities[input.AutoScalingGroupName],
		ScalingActivity{
			ActivityID:           uuid.NewString(),
			AutoScalingGroupName: input.AutoScalingGroupName,
			Description:          "Launching a new EC2 instance",
			StatusCode:           statusCodeSuccessful,
			StatusMessage:        "",
			Progress:             completedProgress,
			StartTime:            time.Now(),
			EndTime:              time.Now(),
		},
	)

	cp := *group

	return &cp, nil
}

// DescribeAutoScalingGroups returns Auto Scaling groups, optionally filtered by name.
func (b *InMemoryBackend) DescribeAutoScalingGroups(names []string) ([]AutoScalingGroup, error) {
	b.mu.RLock("DescribeAutoScalingGroups")
	defer b.mu.RUnlock()

	return describeByNames(b.groups, names, ErrGroupNotFound, func(a, c *AutoScalingGroup) bool {
		return a.AutoScalingGroupName < c.AutoScalingGroupName
	})
}

// isValidHealthCheckType checks if the type is AWS-supported.
func isValidHealthCheckType(hct string) bool {
	return hct == "EC2" || hct == "ELB" || hct == "VPC_LATTICE"
}

// isValidTerminationPolicy checks if the termination policy is AWS-supported.
func isValidTerminationPolicy(p string) bool {
	switch p {
	case "Default", "AllocationStrategy", "ClosestToNextInstanceHour",
		"NewestInstance", "OldestInstance", "OldestLaunchConfiguration", "OldestLaunchTemplate":
		return true
	default:
		return false
	}
}

// isValidScalingProcessType checks if the process name is AWS-supported.
func isValidScalingProcessType(p string) bool {
	switch p {
	case "Launch", "Terminate", "HealthCheck", "ReplaceUnhealthy",
		"AZRebalance", "AlarmNotification", "ScheduledActions",
		"AddToLoadBalancer", "InstanceRefresh":
		return true
	default:
		return false
	}
}

// validateHealthCheckType returns an error for unsupported HealthCheckType values.
func validateHealthCheckType(hct string) error {
	if hct == "" || isValidHealthCheckType(hct) {
		return nil
	}

	return fmt.Errorf("%w: HealthCheckType must be EC2, ELB, or VPC_LATTICE, got %q", ErrInvalidParameter, hct)
}

// validateTerminationPolicies returns an error if any policy name is unknown.
func validateTerminationPolicies(policies []string) error {
	for _, p := range policies {
		if !isValidTerminationPolicy(p) {
			return fmt.Errorf("%w: unknown TerminationPolicy %q", ErrInvalidParameter, p)
		}
	}

	return nil
}

// validateCapacity checks that min ≤ desired ≤ max (when max > 0).
func validateCapacity(minSize, maxSize, desired int32) error {
	if minSize > desired {
		return fmt.Errorf("%w: DesiredCapacity must be >= MinSize", ErrInvalidParameter)
	}

	if maxSize > 0 && desired > maxSize {
		return fmt.Errorf("%w: DesiredCapacity must be <= MaxSize", ErrInvalidParameter)
	}

	if maxSize > 0 && minSize > maxSize {
		return fmt.Errorf("%w: MinSize must be <= MaxSize", ErrInvalidParameter)
	}

	return nil
}

// UpdateAutoScalingGroup updates an existing Auto Scaling group.
//
//nolint:gocognit,cyclop,funlen // Too complex to refactor given time constraints
func (b *InMemoryBackend) UpdateAutoScalingGroup(input UpdateAutoScalingGroupInput) (*AutoScalingGroup, error) {
	b.mu.Lock("UpdateAutoScalingGroup")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(input.AutoScalingGroupName)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	newMin := g.MinSize
	newMax := g.MaxSize
	newDesired := g.DesiredCapacity

	if input.MinSize != nil {
		newMin = *input.MinSize
	}

	if input.MaxSize != nil {
		newMax = *input.MaxSize
	}

	if input.DesiredCapacity != nil {
		newDesired = min(*input.DesiredCapacity, maxDesiredCapacity)
	}

	if err := validateCapacity(newMin, newMax, newDesired); err != nil {
		return nil, err
	}

	g.MinSize = newMin
	g.MaxSize = newMax

	if g.DesiredCapacity != newDesired {
		b.applyDesiredCapacityChange(g, newDesired)
	}

	if input.DefaultCooldown != nil {
		g.DefaultCooldown = *input.DefaultCooldown
	}

	if input.HealthCheckGracePeriod != nil {
		g.HealthCheckGracePeriod = *input.HealthCheckGracePeriod
	}

	if input.LaunchConfigurationName != "" {
		g.LaunchConfigurationName = input.LaunchConfigurationName
		g.LaunchTemplate = nil
	}

	if input.LaunchTemplate != nil {
		g.LaunchTemplate = input.LaunchTemplate
		g.LaunchConfigurationName = ""
	}

	if input.MixedInstancesPolicy != nil {
		g.MixedInstancesPolicy = input.MixedInstancesPolicy
	}

	if input.HealthCheckType != "" {
		if err := validateHealthCheckType(input.HealthCheckType); err != nil {
			return nil, err
		}

		g.HealthCheckType = input.HealthCheckType
	}

	if len(input.AvailabilityZones) > 0 {
		g.AvailabilityZones = input.AvailabilityZones
	}

	if input.VPCZoneIdentifier != "" {
		g.VPCZoneIdentifier = input.VPCZoneIdentifier
	}

	if input.PlacementGroup != "" {
		g.PlacementGroup = input.PlacementGroup
	}

	if input.Context != "" {
		g.Context = input.Context
	}

	if input.DesiredCapacityType != "" {
		g.DesiredCapacityType = input.DesiredCapacityType
	}

	if len(input.TerminationPolicies) > 0 {
		if err := validateTerminationPolicies(input.TerminationPolicies); err != nil {
			return nil, err
		}

		g.TerminationPolicies = input.TerminationPolicies
	}

	if input.MaxInstanceLifetime != nil {
		g.MaxInstanceLifetime = *input.MaxInstanceLifetime
	}

	if input.DefaultInstanceWarmup != nil {
		g.DefaultInstanceWarmup = *input.DefaultInstanceWarmup
	}

	if input.NewInstancesProtectedFromScaleIn != nil {
		g.NewInstancesProtectedFromScaleIn = *input.NewInstancesProtectedFromScaleIn
	}

	if input.CapacityRebalance != nil {
		g.CapacityRebalance = *input.CapacityRebalance
	}

	cp := *g

	return &cp, nil
}

// DeleteAutoScalingGroup removes an Auto Scaling group by name.
// When forceDelete is false, AWS rejects the delete if the group has active instances.
func (b *InMemoryBackend) DeleteAutoScalingGroup(name string, forceDelete bool) error {
	b.mu.Lock("DeleteAutoScalingGroup")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(name)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, name)
	}

	if !forceDelete && len(g.Instances) > 0 {
		return fmt.Errorf("%w: group %q has %d active instance(s); use ForceDelete=true to override",
			ErrScalingActivityInProgress, name, len(g.Instances))
	}

	b.cleanupHookTimers(name, "")

	for _, inst := range g.Instances {
		delete(b.instanceIndex, inst.InstanceID)
	}

	b.groups.Delete(name)
	delete(b.activities, name)
	b.deleteScheduledActionsForGroupLocked(name)
	delete(b.instanceRefreshes, name)
	b.deleteLifecycleHooksForGroupLocked(name)
	b.deleteScalingPoliciesForGroupLocked(name)
	delete(b.notificationConfigs, name)
	b.warmPools.Delete(name)

	return nil
}

// CreateLaunchConfiguration creates a new launch configuration.
func (b *InMemoryBackend) CreateLaunchConfiguration(
	input CreateLaunchConfigurationInput,
) (*LaunchConfiguration, error) {
	b.mu.Lock("CreateLaunchConfiguration")
	defer b.mu.Unlock()

	if b.launchConfigurations.Has(input.LaunchConfigurationName) {
		return nil, fmt.Errorf(
			"%w: launch configuration %q already exists",
			ErrLaunchConfigurationAlreadyExists,
			input.LaunchConfigurationName,
		)
	}

	if input.LaunchConfigurationName == "" {
		return nil, fmt.Errorf("%w: LaunchConfigurationName is required", ErrInvalidParameter)
	}

	lc := &LaunchConfiguration{
		LaunchConfigurationName: input.LaunchConfigurationName,
		LaunchConfigurationARN: fmt.Sprintf(
			"arn:aws:autoscaling:%s:%s:launchConfiguration:%s:launchConfigurationName/%s",
			config.DefaultRegion, config.DefaultAccountID, uuid.NewString(), input.LaunchConfigurationName,
		),
		ImageID:                      input.ImageID,
		InstanceType:                 input.InstanceType,
		KeyName:                      input.KeyName,
		IAMInstanceProfile:           input.IAMInstanceProfile,
		UserData:                     input.UserData,
		KernelID:                     input.KernelID,
		RamdiskID:                    input.RamdiskID,
		SpotPrice:                    input.SpotPrice,
		PlacementTenancy:             input.PlacementTenancy,
		ClassicLinkVPCID:             input.ClassicLinkVPCID,
		SecurityGroups:               input.SecurityGroups,
		ClassicLinkVPCSecurityGroups: input.ClassicLinkVPCSecurityGroups,
		BlockDeviceMappings:          input.BlockDeviceMappings,
		AssociatePublicIPAddress:     input.AssociatePublicIPAddress,
		EbsOptimized:                 input.EbsOptimized,
		InstanceMonitoring:           input.InstanceMonitoring,
		CreatedTime:                  time.Now(),
	}

	b.launchConfigurations.Put(lc)

	cp := *lc

	return &cp, nil
}

// DescribeLaunchConfigurations returns launch configurations, optionally filtered by name.
func (b *InMemoryBackend) DescribeLaunchConfigurations(names []string) ([]LaunchConfiguration, error) {
	b.mu.RLock("DescribeLaunchConfigurations")
	defer b.mu.RUnlock()

	return describeByNames(b.launchConfigurations, names, ErrLaunchConfigurationNotFound,
		func(a, c *LaunchConfiguration) bool {
			return a.LaunchConfigurationName < c.LaunchConfigurationName
		})
}

// DeleteLaunchConfiguration removes a launch configuration by name.
func (b *InMemoryBackend) DeleteLaunchConfiguration(name string) error {
	b.mu.Lock("DeleteLaunchConfiguration")
	defer b.mu.Unlock()

	if !b.launchConfigurations.Has(name) {
		return fmt.Errorf("%w: %q", ErrLaunchConfigurationNotFound, name)
	}

	b.launchConfigurations.Delete(name)

	return nil
}

// DescribeScalingActivities returns scaling activities for the given group.
func (b *InMemoryBackend) DescribeScalingActivities(groupName string) ([]ScalingActivity, error) {
	b.mu.RLock("DescribeScalingActivities")
	defer b.mu.RUnlock()

	if groupName != "" {
		if !b.groups.Has(groupName) {
			return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
		}

		acts := b.activities[groupName]
		result := make([]ScalingActivity, len(acts))
		copy(result, acts)

		return result, nil
	}

	result := make([]ScalingActivity, 0, len(b.activities))
	for _, acts := range b.activities {
		result = append(result, acts...)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ActivityID < result[j].ActivityID
	})

	return result, nil
}

// Purge removes all AutoScaling groups and launch configurations created before the cutoff time.
func (b *InMemoryBackend) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}

	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	// 1. Purge groups
	for _, g := range b.groups.All() {
		if ctx.Err() != nil {
			return
		}
		if g.CreatedTime.Before(cutoff) {
			name := g.AutoScalingGroupName
			b.cleanupHookTimers(name, "")
			b.groups.Delete(name)
			delete(b.activities, name)
			b.deleteScheduledActionsForGroupLocked(name)
			delete(b.instanceRefreshes, name)
			b.deleteLifecycleHooksForGroupLocked(name)
			b.deleteScalingPoliciesForGroupLocked(name)
			delete(b.notificationConfigs, name)
			b.warmPools.Delete(name)
		}
	}

	// 2. Purge launch configurations
	for _, lc := range b.launchConfigurations.All() {
		if ctx.Err() != nil {
			return
		}
		if lc.CreatedTime.Before(cutoff) {
			b.launchConfigurations.Delete(lc.LaunchConfigurationName)
		}
	}
}

// AttachInstances adds the given instance IDs to the specified Auto Scaling group.
func (b *InMemoryBackend) AttachInstances(groupName string, instanceIDs []string) error {
	b.mu.Lock("AttachInstances")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	existing := make(map[string]bool, len(g.Instances))
	for _, inst := range g.Instances {
		existing[inst.InstanceID] = true
	}

	az := defaultAvailabilityZone
	if len(g.AvailabilityZones) > 0 {
		az = g.AvailabilityZones[0]
	}

	for _, id := range instanceIDs {
		if existing[id] {
			continue
		}

		g.Instances = append(g.Instances, Instance{
			InstanceID:              id,
			AvailabilityZone:        az,
			LifecycleState:          lifecycleStateInService,
			HealthStatus:            healthStatusHealthy,
			LaunchConfigurationName: g.LaunchConfigurationName,
			InstanceType:            "t2.micro",
		})
	}

	return nil
}

// AttachLoadBalancerTargetGroups adds target group ARNs to the specified Auto Scaling group.
func (b *InMemoryBackend) AttachLoadBalancerTargetGroups(groupName string, targetGroupARNs []string) error {
	b.mu.Lock("AttachLoadBalancerTargetGroups")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	existing := make(map[string]bool, len(g.TargetGroupARNs))
	for _, arn := range g.TargetGroupARNs {
		existing[arn] = true
	}

	for _, arn := range targetGroupARNs {
		if !existing[arn] {
			g.TargetGroupARNs = append(g.TargetGroupARNs, arn)
		}
	}

	return nil
}

// AttachLoadBalancers adds load balancer names to the specified Auto Scaling group.
func (b *InMemoryBackend) AttachLoadBalancers(groupName string, loadBalancerNames []string) error {
	b.mu.Lock("AttachLoadBalancers")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	existing := make(map[string]bool, len(g.LoadBalancerNames))
	for _, lb := range g.LoadBalancerNames {
		existing[lb] = true
	}

	for _, lb := range loadBalancerNames {
		if !existing[lb] {
			g.LoadBalancerNames = append(g.LoadBalancerNames, lb)
		}
	}

	return nil
}

// AttachTrafficSources adds traffic sources to the specified Auto Scaling group.
func (b *InMemoryBackend) AttachTrafficSources(groupName string, trafficSources []TrafficSource) error {
	b.mu.Lock("AttachTrafficSources")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	type tsKey struct{ Identifier, Type string }

	existing := make(map[tsKey]bool, len(g.TrafficSources))
	for _, ts := range g.TrafficSources {
		existing[tsKey(ts)] = true
	}

	for _, ts := range trafficSources {
		k := tsKey(ts)
		if !existing[k] {
			g.TrafficSources = append(g.TrafficSources, ts)
		}
	}

	return nil
}

// BatchDeleteScheduledAction removes the named scheduled actions from the group.
// Actions that cannot be found are returned as failures.
func (b *InMemoryBackend) BatchDeleteScheduledAction(
	groupName string,
	scheduledActionNames []string,
) ([]FailedScheduledAction, error) {
	b.mu.Lock("BatchDeleteScheduledAction")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	failed := make([]FailedScheduledAction, 0, len(scheduledActionNames))

	for _, name := range scheduledActionNames {
		key := scopedKey(groupName, name)
		if !b.scheduledActions.Has(key) {
			failed = append(failed, FailedScheduledAction{
				ScheduledActionName: name,
				ErrorCode:           errValidationError,
				ErrorMessage:        fmt.Sprintf("scheduled action %q not found", name),
			})

			continue
		}

		b.scheduledActions.Delete(key)
	}

	return failed, nil
}

// BatchPutScheduledUpdateGroupAction creates or updates scheduled actions for the group.
func (b *InMemoryBackend) BatchPutScheduledUpdateGroupAction(
	groupName string,
	actions []ScheduledUpdateGroupAction,
) ([]FailedScheduledAction, error) {
	b.mu.Lock("BatchPutScheduledUpdateGroupAction")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	failed := make([]FailedScheduledAction, 0, len(actions))

	for _, a := range actions {
		if a.ScheduledActionName == "" {
			failed = append(failed, FailedScheduledAction{
				ScheduledActionName: a.ScheduledActionName,
				ErrorCode:           errValidationError,
				ErrorMessage:        "ScheduledActionName is required",
			})

			continue
		}

		b.scheduledActions.Put(&ScheduledAction{
			ScheduledActionName:  a.ScheduledActionName,
			AutoScalingGroupName: groupName,
			Recurrence:           a.Recurrence,
			TimeZone:             a.TimeZone,
			StartTime:            a.StartTime,
			EndTime:              a.EndTime,
			DesiredCapacity:      a.DesiredCapacity,
			MinSize:              a.MinSize,
			MaxSize:              a.MaxSize,
		})
	}

	return failed, nil
}

// CancelInstanceRefresh cancels an active instance refresh for the group.
// It returns the ID of the cancelled refresh.
func (b *InMemoryBackend) CancelInstanceRefresh(groupName string) (string, error) {
	b.mu.Lock("CancelInstanceRefresh")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return "", fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	for _, r := range b.instanceRefreshes[groupName] {
		if r.Status == statusInProgress || r.Status == "Pending" {
			r.Status = "Cancelling"

			return r.InstanceRefreshID, nil
		}
	}

	return "", fmt.Errorf("%w: no active instance refresh for group %q",
		ErrActiveInstanceRefreshNotFound, groupName)
}

// CompleteLifecycleAction completes a lifecycle action for the given group and hook.
func (b *InMemoryBackend) CompleteLifecycleAction(input CompleteLifecycleActionInput) error {
	b.mu.Lock("CompleteLifecycleAction")
	defer b.mu.Unlock()

	if !b.groups.Has(input.AutoScalingGroupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	if input.LifecycleHookName == "" {
		return fmt.Errorf("%w: LifecycleHookName is required", ErrInvalidParameter)
	}

	if input.LifecycleActionResult == "" {
		return fmt.Errorf("%w: LifecycleActionResult is required", ErrInvalidParameter)
	}

	// Validate LifecycleActionResult is CONTINUE or ABANDON (case-insensitive)
	upper := strings.ToUpper(input.LifecycleActionResult)
	if upper != lifecycleActionContinue && upper != lifecycleActionAbandon {
		return fmt.Errorf("%w: LifecycleActionResult must be CONTINUE or ABANDON, got %q",
			ErrInvalidParameter, input.LifecycleActionResult)
	}

	action := b.findPendingHookAction(
		input.LifecycleActionToken, input.AutoScalingGroupName, input.LifecycleHookName, input.InstanceID,
	)
	if action != nil {
		action.timer.Stop()
		b.pendingHookTokens.Delete(action.Token)
		b.applyLifecycleResult(action, upper)
	}

	return nil
}

// CreateOrUpdateTags creates or updates tags on Auto Scaling resources.
// Only group (auto-scaling-group) resource tags are currently supported.
func (b *InMemoryBackend) CreateOrUpdateTags(tags []ResourceTag) error {
	b.mu.Lock("CreateOrUpdateTags")
	defer b.mu.Unlock()

	for _, tag := range tags {
		if tag.ResourceType != resourceTypeAutoScalingGroup {
			continue
		}

		g, ok := b.groups.Get(tag.ResourceID)
		if !ok {
			return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, tag.ResourceID)
		}

		updated := false

		for i, t := range g.Tags {
			if t.Key == tag.Key {
				g.Tags[i].Value = tag.Value
				updated = true

				break
			}
		}

		if !updated {
			g.Tags = append(g.Tags, Tag{Key: tag.Key, Value: tag.Value})
		}
	}

	return nil
}

// DeleteLifecycleHook removes a lifecycle hook from the specified Auto Scaling group.
func (b *InMemoryBackend) DeleteLifecycleHook(groupName, hookName string) error {
	b.mu.Lock("DeleteLifecycleHook")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	key := scopedKey(groupName, hookName)
	if !b.lifecycleHooks.Has(key) {
		return fmt.Errorf("%w: lifecycle hook %q not found", ErrLifecycleHookNotFound, hookName)
	}

	b.cleanupHookTimers(groupName, hookName)
	b.lifecycleHooks.Delete(key)

	return nil
}

// AddLifecycleHook stores a lifecycle hook for the given group.
// This is the backend helper used by PutLifecycleHook and by tests.
func (b *InMemoryBackend) AddLifecycleHook(hook LifecycleHook) error {
	b.mu.Lock("AddLifecycleHook")
	defer b.mu.Unlock()

	if !b.groups.Has(hook.AutoScalingGroupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, hook.AutoScalingGroupName)
	}

	cp := hook
	b.lifecycleHooks.Put(&cp)

	return nil
}

// AddInstanceRefresh stores an instance refresh for the given group (used for testing CancelInstanceRefresh).
func (b *InMemoryBackend) AddInstanceRefresh(refresh InstanceRefresh) error {
	b.mu.Lock("AddInstanceRefresh")
	defer b.mu.Unlock()

	if !b.groups.Has(refresh.AutoScalingGroupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, refresh.AutoScalingGroupName)
	}

	cp := refresh
	b.instanceRefreshes[refresh.AutoScalingGroupName] = append(
		b.instanceRefreshes[refresh.AutoScalingGroupName],
		&cp,
	)

	return nil
}

// applyDesiredCapacityChange updates the group's desired capacity and adjusts instances,
// respecting suspended processes (Launch/Terminate) and scale-in protection.
// Must be called with b.mu held (write lock).
func (b *InMemoryBackend) applyDesiredCapacityChange(g *AutoScalingGroup, newDesired int32) {
	suspendedSet := make(map[string]bool, len(g.SuspendedProcesses))
	for _, p := range g.SuspendedProcesses {
		suspendedSet[p] = true
	}

	current := int32(len(g.Instances)) //nolint:gosec // bounded by maxDesiredCapacity
	g.DesiredCapacity = newDesired

	switch {
	case newDesired < current:
		if !suspendedSet["Terminate"] {
			oldInstances := g.Instances
			g.Instances = removeUnprotectedInstances(g.Instances, int(newDesired))
			// Remove terminated instances from index.
			surviving := make(map[string]bool, len(g.Instances))
			for _, inst := range g.Instances {
				surviving[inst.InstanceID] = true
			}

			for _, inst := range oldInstances {
				if !surviving[inst.InstanceID] {
					delete(b.instanceIndex, inst.InstanceID)
				}
			}
		}
	case newDesired > current:
		if !suspendedSet["Launch"] {
			oldLen := len(g.Instances)
			g.Instances = adjustInstances(
				g.Instances, g.DesiredCapacity, g.AvailabilityZones, g.LaunchConfigurationName,
				lcInstanceType(b.launchConfigurations, g.LaunchConfigurationName),
			)
			// Add newly launched instances to index.
			for _, inst := range g.Instances[oldLen:] {
				b.instanceIndex[inst.InstanceID] = g.AutoScalingGroupName
			}

			b.gateNewLaunchInstances(g, oldLen)
		}
	}

	g.LastScalingActivity = time.Now()
}

// removeUnprotectedInstances removes instances from the end, skipping protected ones,
// until the slice reaches targetCount. If all remaining instances are protected,
// it stops short of the target. O(N) via two-index compaction.
func removeUnprotectedInstances(instances []Instance, targetCount int) []Instance {
	toRemove := len(instances) - targetCount
	if toRemove <= 0 {
		return instances
	}

	// Collect indices of unprotected instances from the end.
	removeSet := make(map[int]bool, toRemove)

	for i := len(instances) - 1; i >= 0 && len(removeSet) < toRemove; i-- {
		if !instances[i].ProtectedFromScaleIn {
			removeSet[i] = true
		}
	}

	if len(removeSet) == 0 {
		return instances
	}

	// Compact: keep instances not in removeSet.
	result := instances[:0]

	for i, inst := range instances {
		if !removeSet[i] {
			result = append(result, inst)
		}
	}

	return result
}

// SetDesiredCapacity adjusts the DesiredCapacity of an Auto Scaling group immediately.
func (b *InMemoryBackend) SetDesiredCapacity(groupName string, desiredCapacity int32) error {
	b.mu.Lock("SetDesiredCapacity")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	desired := min(desiredCapacity, maxDesiredCapacity)

	if desired < g.MinSize {
		return fmt.Errorf("%w: DesiredCapacity %d is less than MinSize %d", ErrInvalidParameter, desired, g.MinSize)
	}

	if g.MaxSize > 0 && desired > g.MaxSize {
		return fmt.Errorf("%w: DesiredCapacity %d exceeds MaxSize %d", ErrInvalidParameter, desired, g.MaxSize)
	}

	b.applyDesiredCapacityChange(g, desired)

	return nil
}

// TerminateInstanceInAutoScalingGroup terminates a specific instance in an ASG.
// When shouldDecrement is true, MinSize is decremented (capped at 0) and DesiredCapacity is
// decreased by 1 without a replacement. Otherwise a replacement is launched.
func (b *InMemoryBackend) TerminateInstanceInAutoScalingGroup(
	instanceID string,
	shouldDecrement bool,
) (*ScalingActivity, error) {
	b.mu.Lock("TerminateInstanceInAutoScalingGroup")
	defer b.mu.Unlock()

	groupName, ok := b.instanceIndex[instanceID]
	if !ok {
		return nil, fmt.Errorf("%w: instance %q not found in any auto scaling group", ErrInstanceNotFound, instanceID)
	}

	targetGroup, _ := b.groups.Get(groupName)
	if targetGroup == nil {
		return nil, fmt.Errorf("%w: instance %q not found in any auto scaling group", ErrInstanceNotFound, instanceID)
	}

	// When a terminating lifecycle hook is registered, the instance is paused in
	// Terminating:Wait until CompleteLifecycleAction is called or the hook's
	// HeartbeatTimeout elapses; the actual removal/replacement is deferred to
	// finishTermination. This mirrors real AWS behavior instead of terminating
	// instantly regardless of configured hooks.
	if hook := findHookForTransition(b.lifecycleHooksByGroup.Get(groupName), transitionTerminating); hook != nil {
		for i := range targetGroup.Instances {
			if targetGroup.Instances[i].InstanceID == instanceID {
				b.armLifecycleWait(targetGroup, hook, &targetGroup.Instances[i], transitionTerminating, shouldDecrement)

				break
			}
		}

		activity := ScalingActivity{
			ActivityID:           uuid.NewString(),
			AutoScalingGroupName: targetGroup.AutoScalingGroupName,
			Description: "Terminating EC2 instance: " + instanceID +
				" (waiting for lifecycle hook '" + hook.LifecycleHookName + "')",
			StatusCode: statusInProgress,
			Progress:   50, //nolint:mnd // AWS reports partial progress mid-wait; no finer granularity to model
			StartTime:  time.Now(),
		}
		b.activities[groupName] = append(b.activities[groupName], activity)

		return &activity, nil
	}

	// Remove the instance from the group.
	newInstances := make([]Instance, 0, len(targetGroup.Instances)-1)

	for _, inst := range targetGroup.Instances {
		if inst.InstanceID != instanceID {
			newInstances = append(newInstances, inst)
		}
	}

	targetGroup.Instances = newInstances
	delete(b.instanceIndex, instanceID)

	if shouldDecrement {
		if targetGroup.DesiredCapacity > 0 {
			targetGroup.DesiredCapacity--
		}

		if targetGroup.MinSize > 0 {
			targetGroup.MinSize--
		}
	} else {
		// Launch a replacement to maintain DesiredCapacity.
		oldLen := len(targetGroup.Instances)
		targetGroup.Instances = adjustInstances(
			targetGroup.Instances,
			targetGroup.DesiredCapacity,
			targetGroup.AvailabilityZones,
			targetGroup.LaunchConfigurationName,
			lcInstanceType(b.launchConfigurations, targetGroup.LaunchConfigurationName),
		)

		for _, inst := range targetGroup.Instances[oldLen:] {
			b.instanceIndex[inst.InstanceID] = targetGroup.AutoScalingGroupName
		}

		b.gateNewLaunchInstances(targetGroup, oldLen)
	}

	activity := ScalingActivity{
		ActivityID:           uuid.NewString(),
		AutoScalingGroupName: targetGroup.AutoScalingGroupName,
		Description:          "Terminating EC2 instance: " + instanceID,
		StatusCode:           statusCodeSuccessful,
		Progress:             completedProgress,
		StartTime:            time.Now(),
		EndTime:              time.Now(),
	}

	b.activities[targetGroup.AutoScalingGroupName] = append(
		b.activities[targetGroup.AutoScalingGroupName],
		activity,
	)

	return &activity, nil
}

// defaultHeartbeatTimeout is the default HeartbeatTimeout for lifecycle hooks (1 hour), matching AWS.
const defaultHeartbeatTimeout = int32(3600)

// PutLifecycleHook creates or updates a lifecycle hook on an Auto Scaling group.
func (b *InMemoryBackend) PutLifecycleHook(hook LifecycleHook) error {
	b.mu.Lock("PutLifecycleHook")
	defer b.mu.Unlock()

	if hook.LifecycleHookName == "" {
		return fmt.Errorf("%w: LifecycleHookName is required", ErrInvalidParameter)
	}

	if !b.groups.Has(hook.AutoScalingGroupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, hook.AutoScalingGroupName)
	}

	if err := normalizeLifecycleHook(&hook); err != nil {
		return err
	}

	cp := hook
	b.lifecycleHooks.Put(&cp)

	return nil
}

// normalizeLifecycleHook validates a LifecycleHook's fields and fills in AWS
// defaults (HeartbeatTimeout=3600, DefaultResult=ABANDON, GlobalTimeout=HeartbeatTimeout).
// Shared by PutLifecycleHook and CreateAutoScalingGroup's LifecycleHookSpecificationList.
func normalizeLifecycleHook(hook *LifecycleHook) error {
	// Validate LifecycleTransition if provided
	if hook.LifecycleTransition != "" &&
		hook.LifecycleTransition != transitionLaunching &&
		hook.LifecycleTransition != transitionTerminating {
		return fmt.Errorf(
			"%w: LifecycleTransition must be %s or %s",
			ErrInvalidParameter, transitionLaunching, transitionTerminating,
		)
	}

	// Default HeartbeatTimeout to 3600 if not provided (matching AWS behavior).
	if hook.HeartbeatTimeout == 0 {
		hook.HeartbeatTimeout = defaultHeartbeatTimeout
	}

	// AWS: HeartbeatTimeout must be 30..172800 (48 h).
	const minHeartbeat = int32(30)
	const maxHeartbeat = int32(172800)

	if hook.HeartbeatTimeout < minHeartbeat || hook.HeartbeatTimeout > maxHeartbeat {
		return fmt.Errorf(
			"%w: HeartbeatTimeout must be between %d and %d seconds, got %d",
			ErrInvalidParameter, minHeartbeat, maxHeartbeat, hook.HeartbeatTimeout,
		)
	}

	// DefaultResult must be CONTINUE or ABANDON.
	if hook.DefaultResult != "" && hook.DefaultResult != lifecycleActionContinue &&
		hook.DefaultResult != lifecycleActionAbandon {
		return fmt.Errorf(
			"%w: DefaultResult must be CONTINUE or ABANDON, got %q",
			ErrInvalidParameter, hook.DefaultResult,
		)
	}

	// Default DefaultResult to ABANDON if not provided.
	if hook.DefaultResult == "" {
		hook.DefaultResult = lifecycleActionAbandon
	}

	// GlobalTimeout = HeartbeatTimeout * numberOfRetries; AWS uses numberOfRetries=1 by default.
	hook.GlobalTimeout = hook.HeartbeatTimeout

	return nil
}

// DescribeLifecycleHooks returns lifecycle hooks for the given group, optionally filtered by name.
func (b *InMemoryBackend) DescribeLifecycleHooks(groupName string, hookNames []string) ([]LifecycleHook, error) {
	b.mu.RLock("DescribeLifecycleHooks")
	defer b.mu.RUnlock()

	if !b.groups.Has(groupName) {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	hooks := b.lifecycleHooksByGroup.Get(groupName)

	if len(hookNames) > 0 {
		result := make([]LifecycleHook, 0, len(hookNames))

		for _, name := range hookNames {
			h, ok := b.lifecycleHooks.Get(scopedKey(groupName, name))
			if !ok {
				return nil, fmt.Errorf("%w: lifecycle hook %q not found", ErrLifecycleHookNotFound, name)
			}

			result = append(result, *h)
		}

		return result, nil
	}

	result := make([]LifecycleHook, 0, len(hooks))

	for _, h := range hooks {
		result = append(result, *h)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].LifecycleHookName < result[j].LifecycleHookName
	})

	return result, nil
}

// DescribeScheduledActions returns scheduled actions for the given group, optionally filtered by name.
func (b *InMemoryBackend) DescribeScheduledActions(
	groupName string,
	actionNames []string,
) ([]ScheduledAction, error) {
	b.mu.RLock("DescribeScheduledActions")
	defer b.mu.RUnlock()

	if groupName != "" {
		if !b.groups.Has(groupName) {
			return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
		}
	}

	if len(actionNames) > 0 && groupName != "" {
		result := make([]ScheduledAction, 0, len(actionNames))

		for _, name := range actionNames {
			a, exists := b.scheduledActions.Get(scopedKey(groupName, name))
			if !exists {
				continue
			}

			result = append(result, *a)
		}

		return result, nil
	}

	var result []ScheduledAction

	if groupName != "" {
		for _, a := range b.scheduledActionsByGroup.Get(groupName) {
			result = append(result, *a)
		}
	} else {
		for _, a := range b.scheduledActions.All() {
			result = append(result, *a)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ScheduledActionName < result[j].ScheduledActionName
	})

	return result, nil
}

// DeleteTags removes tags from Auto Scaling resources.
// Only auto-scaling-group resource tags are supported.
func (b *InMemoryBackend) DeleteTags(tags []ResourceTag) error {
	b.mu.Lock("DeleteTags")
	defer b.mu.Unlock()

	for _, tag := range tags {
		if tag.ResourceType != resourceTypeAutoScalingGroup {
			continue
		}

		g, ok := b.groups.Get(tag.ResourceID)
		if !ok {
			return fmt.Errorf("%w: group %q not found", ErrGroupNotFound, tag.ResourceID)
		}

		newTags := make([]Tag, 0, len(g.Tags))

		for _, t := range g.Tags {
			if t.Key != tag.Key {
				newTags = append(newTags, t)
			}
		}

		g.Tags = newTags
	}

	return nil
}

// buildTagFilterMap converts a slice of TagFilters into a nested map for O(1) lookups.
func buildTagFilterMap(filters []TagFilter) map[string]map[string]bool {
	m := make(map[string]map[string]bool, len(filters))

	for _, f := range filters {
		vals := make(map[string]bool, len(f.Values))

		for _, v := range f.Values {
			vals[v] = true
		}

		m[f.Name] = vals
	}

	return m
}

// tagMatchesFilters reports whether the tag identified by (resourceID, key, value) passes all filters.
func tagMatchesFilters(filterMap map[string]map[string]bool, resourceID, key, value string) bool {
	if len(filterMap) == 0 {
		return true
	}

	if ids, ok := filterMap[resourceTypeAutoScalingGroup]; ok && !ids[resourceID] {
		return false
	}

	if keys, ok := filterMap["key"]; ok && !keys[key] {
		return false
	}

	if vals, ok := filterMap["value"]; ok && !vals[value] {
		return false
	}

	return true
}

// DescribeTags returns tags for Auto Scaling resources, with optional filtering.
func (b *InMemoryBackend) DescribeTags(filters []TagFilter) ([]ResourceTag, error) {
	b.mu.RLock("DescribeTags")
	defer b.mu.RUnlock()

	filterMap := buildTagFilterMap(filters)

	var result []ResourceTag

	for _, g := range b.groups.All() {
		for _, t := range g.Tags {
			if tagMatchesFilters(filterMap, g.AutoScalingGroupName, t.Key, t.Value) {
				result = append(result, ResourceTag{
					ResourceID:   g.AutoScalingGroupName,
					ResourceType: resourceTypeAutoScalingGroup,
					Key:          t.Key,
					Value:        t.Value,
				})
			}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].ResourceID != result[j].ResourceID {
			return result[i].ResourceID < result[j].ResourceID
		}

		return result[i].Key < result[j].Key
	})

	return result, nil
}

// DescribeAutoScalingInstances returns instance details across all ASGs, optionally filtered by instance ID.
func (b *InMemoryBackend) DescribeAutoScalingInstances(instanceIDs []string) ([]InstanceDetails, error) {
	b.mu.RLock("DescribeAutoScalingInstances")
	defer b.mu.RUnlock()

	idFilter := make(map[string]bool, len(instanceIDs))

	for _, id := range instanceIDs {
		idFilter[id] = true
	}

	var result []InstanceDetails

	for _, g := range b.groups.All() {
		for _, inst := range g.Instances {
			if len(idFilter) > 0 && !idFilter[inst.InstanceID] {
				continue
			}

			result = append(result, InstanceDetails{
				AutoScalingGroupName:    g.AutoScalingGroupName,
				InstanceID:              inst.InstanceID,
				AvailabilityZone:        inst.AvailabilityZone,
				LifecycleState:          inst.LifecycleState,
				HealthStatus:            inst.HealthStatus,
				LaunchConfigurationName: inst.LaunchConfigurationName,
				InstanceType:            inst.InstanceType,
				ProtectedFromScaleIn:    inst.ProtectedFromScaleIn,
			})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].InstanceID < result[j].InstanceID
	})

	return result, nil
}

// cleanupHookTimers cancels and removes pending hook actions for a group/hook.
// If hookName is empty, all pending actions for the group are cancelled.
// Must be called with b.mu held (write lock).
func (b *InMemoryBackend) cleanupHookTimers(groupName, hookName string) {
	var toRemove []string

	b.pendingHookTokens.Range(func(action *pendingHookAction) bool {
		if action.GroupName == groupName && (hookName == "" || action.HookName == hookName) {
			action.timer.Stop()
			toRemove = append(toRemove, action.Token)
		}

		return true
	})

	for _, token := range toRemove {
		b.pendingHookTokens.Delete(token)
	}
}

// findHookForTransition returns the first lifecycle hook registered for the given
// group whose LifecycleTransition matches transition, or nil if none is registered.
// AWS allows multiple hooks per transition (each must complete independently); this
// simulation supports one active hook per transition per group, which covers the
// overwhelming majority of real configurations. Must be called with b.mu held.
func findHookForTransition(hooks []*LifecycleHook, transition string) *LifecycleHook {
	var found *LifecycleHook

	for _, h := range hooks {
		if h.LifecycleTransition == transition {
			if found == nil || h.LifecycleHookName < found.LifecycleHookName {
				found = h
			}
		}
	}

	return found
}

// gateNewLaunchInstances transitions newly-appended instances (g.Instances[startIdx:])
// to Pending:Wait and arms a heartbeat timer when the group has an active
// EC2_INSTANCE_LAUNCHING lifecycle hook. It is a no-op when no such hook exists, so
// callers can invoke it unconditionally after adding instances. Must be called with
// b.mu held (write lock).
func (b *InMemoryBackend) gateNewLaunchInstances(g *AutoScalingGroup, startIdx int) {
	hook := findHookForTransition(b.lifecycleHooksByGroup.Get(g.AutoScalingGroupName), transitionLaunching)
	if hook == nil {
		return
	}

	for i := startIdx; i < len(g.Instances); i++ {
		b.armLifecycleWait(g, hook, &g.Instances[i], transitionLaunching, false)
	}
}

// armLifecycleWait puts inst into the appropriate "Wait" lifecycle state and starts a
// heartbeat timer that resolves the action with the hook's DefaultResult if
// CompleteLifecycleAction/RecordLifecycleActionHeartbeat don't intervene first. Must
// be called with b.mu held (write lock).
func (b *InMemoryBackend) armLifecycleWait(
	g *AutoScalingGroup, hook *LifecycleHook, inst *Instance, transition string, shouldDecrement bool,
) {
	if transition == transitionLaunching {
		inst.LifecycleState = lifecycleStatePendingWait
	} else {
		inst.LifecycleState = lifecycleStateTerminatingWait
	}

	token := uuid.NewString()
	timeout := time.Duration(hook.HeartbeatTimeout) * time.Second

	action := &pendingHookAction{
		Token:           token,
		GroupName:       g.AutoScalingGroupName,
		HookName:        hook.LifecycleHookName,
		InstanceID:      inst.InstanceID,
		Transition:      transition,
		DefaultResult:   hook.DefaultResult,
		timeout:         timeout,
		ShouldDecrement: shouldDecrement,
	}
	action.timer = time.AfterFunc(timeout, func() {
		b.resolveLifecycleWait(token, action.DefaultResult)
	})
	b.pendingHookTokens.Put(action)
}

// resolveLifecycleWait applies result (CONTINUE/ABANDON) to the pending action
// identified by token, if it is still pending. Called both from expired timers (its
// own goroutine, hence it takes the lock itself) and, indirectly, from explicit
// CompleteLifecycleAction calls.
func (b *InMemoryBackend) resolveLifecycleWait(token, result string) {
	b.mu.Lock("resolveLifecycleWait")
	defer b.mu.Unlock()

	action, ok := b.pendingHookTokens.Get(token)
	if !ok {
		return // already resolved (race between timer and an explicit Complete call)
	}

	b.pendingHookTokens.Delete(token)
	b.applyLifecycleResult(action, result)
}

// applyLifecycleResult performs the actual state transition once a lifecycle wait
// resolves (either explicitly via CompleteLifecycleAction or via heartbeat timeout).
// Must be called with b.mu held (write lock).
func (b *InMemoryBackend) applyLifecycleResult(action *pendingHookAction, result string) {
	g, ok := b.groups.Get(action.GroupName)
	if !ok {
		return // group was deleted while the action was pending
	}

	switch action.Transition {
	case transitionLaunching:
		if strings.EqualFold(result, lifecycleActionContinue) {
			for i := range g.Instances {
				if g.Instances[i].InstanceID == action.InstanceID {
					g.Instances[i].LifecycleState = lifecycleStateInService

					break
				}
			}
		} else {
			// ABANDON: AWS terminates the instance that failed to launch.
			b.removeInstanceByID(g, action.InstanceID)
		}
	case transitionTerminating:
		// Both CONTINUE and ABANDON allow termination to proceed for a terminating
		// hook (the result only affects any downstream hook chaining, which this
		// simulation does not model).
		b.finishTermination(g, action)
	}
}

// removeInstanceByID removes the named instance from the group and its index, if present.
// Must be called with b.mu held (write lock).
func (b *InMemoryBackend) removeInstanceByID(g *AutoScalingGroup, instanceID string) {
	for i, inst := range g.Instances {
		if inst.InstanceID == instanceID {
			g.Instances = append(g.Instances[:i], g.Instances[i+1:]...)

			break
		}
	}

	delete(b.instanceIndex, instanceID)
}

// finishTermination completes a deferred termination once its Terminating:Wait hook
// resolves: removes the instance, applies the originally-requested capacity
// adjustment (decrement vs. replacement launch), and records the completion
// activity. Must be called with b.mu held (write lock).
func (b *InMemoryBackend) finishTermination(g *AutoScalingGroup, action *pendingHookAction) {
	found := false

	for _, inst := range g.Instances {
		if inst.InstanceID == action.InstanceID {
			found = true

			break
		}
	}

	if !found {
		return
	}

	b.removeInstanceByID(g, action.InstanceID)

	if action.ShouldDecrement {
		if g.DesiredCapacity > 0 {
			g.DesiredCapacity--
		}

		if g.MinSize > 0 {
			g.MinSize--
		}
	} else {
		oldLen := len(g.Instances)
		g.Instances = adjustInstances(
			g.Instances, g.DesiredCapacity, g.AvailabilityZones, g.LaunchConfigurationName,
			lcInstanceType(b.launchConfigurations, g.LaunchConfigurationName),
		)

		for _, inst := range g.Instances[oldLen:] {
			b.instanceIndex[inst.InstanceID] = g.AutoScalingGroupName
		}

		b.gateNewLaunchInstances(g, oldLen)
	}

	b.activities[g.AutoScalingGroupName] = append(b.activities[g.AutoScalingGroupName], ScalingActivity{
		ActivityID:           uuid.NewString(),
		AutoScalingGroupName: g.AutoScalingGroupName,
		Description:          "Terminating EC2 instance: " + action.InstanceID,
		StatusCode:           statusCodeSuccessful,
		Progress:             completedProgress,
		StartTime:            time.Now(),
		EndTime:              time.Now(),
	})
}

// rearmPendingWaits re-arms heartbeat timers for any instances left in a lifecycle
// "Wait" state by a restored snapshot. In-flight timers are never persisted (see
// pendingHookTokens/backendSnapshot), so without this an instance restored mid-wait
// would be stuck in Pending:Wait/Terminating:Wait forever. Must be called with b.mu
// held (write lock); intended to run once, right after Restore repopulates b.groups.
func (b *InMemoryBackend) rearmPendingWaits() {
	for _, g := range b.groups.All() {
		for i := range g.Instances {
			inst := &g.Instances[i]

			var transition string

			switch inst.LifecycleState {
			case lifecycleStatePendingWait:
				transition = transitionLaunching
			case lifecycleStateTerminatingWait:
				transition = transitionTerminating
			default:
				continue
			}

			hook := findHookForTransition(b.lifecycleHooksByGroup.Get(g.AutoScalingGroupName), transition)

			heartbeat := defaultHeartbeatTimeout
			defaultResult := lifecycleActionAbandon
			hookName := ""

			if hook != nil {
				heartbeat = hook.HeartbeatTimeout
				defaultResult = hook.DefaultResult
				hookName = hook.LifecycleHookName
			}

			token := uuid.NewString()
			action := &pendingHookAction{
				Token:         token,
				GroupName:     g.AutoScalingGroupName,
				HookName:      hookName,
				InstanceID:    inst.InstanceID,
				Transition:    transition,
				DefaultResult: defaultResult,
				timeout:       time.Duration(heartbeat) * time.Second,
			}
			action.timer = time.AfterFunc(action.timeout, func() {
				b.resolveLifecycleWait(token, action.DefaultResult)
			})
			b.pendingHookTokens.Put(action)
		}
	}
}

// findPendingHookAction looks up a pending lifecycle action, first by explicit token
// (as AWS does) and, when the token is empty, by (groupName, hookName, instanceID) —
// AWS's CompleteLifecycleAction and RecordLifecycleActionHeartbeat both accept either
// a token or an instance ID. Must be called with b.mu held.
func (b *InMemoryBackend) findPendingHookAction(token, groupName, hookName, instanceID string) *pendingHookAction {
	if token != "" {
		action, _ := b.pendingHookTokens.Get(token)

		return action
	}

	var found *pendingHookAction

	b.pendingHookTokens.Range(func(action *pendingHookAction) bool {
		if action.GroupName == groupName && action.HookName == hookName && action.InstanceID == instanceID {
			found = action

			return false
		}

		return true
	})

	return found
}

// DescribeAccountLimits returns account limits for Auto Scaling.
func (b *InMemoryBackend) DescribeAccountLimits() (*AccountLimits, error) {
	b.mu.RLock("DescribeAccountLimits")
	defer b.mu.RUnlock()

	return &AccountLimits{
		MaxNumberOfAutoScalingGroups:    maxAccountASGs,
		MaxNumberOfLaunchConfigurations: maxAccountLaunchConfigs,
		//nolint:gosec,G115 // bounded by maxAccountASGs
		NumberOfAutoScalingGroups: int32(b.groups.Len()),
		//nolint:gosec,G115 // bounded by maxAccountLaunchConfigs
		NumberOfLaunchConfigurations: int32(b.launchConfigurations.Len()),
	}, nil
}

// DescribeAdjustmentTypes returns the supported scaling adjustment types.
func (b *InMemoryBackend) DescribeAdjustmentTypes() ([]string, error) {
	return []string{"ChangeInCapacity", "ExactCapacity", "PercentChangeInCapacity"}, nil
}

// DescribeAutoScalingNotificationTypes returns the supported notification types.
func (b *InMemoryBackend) DescribeAutoScalingNotificationTypes() ([]string, error) {
	return []string{
		"autoscaling:EC2_INSTANCE_LAUNCH",
		"autoscaling:EC2_INSTANCE_LAUNCH_ERROR",
		"autoscaling:EC2_INSTANCE_TERMINATE",
		"autoscaling:EC2_INSTANCE_TERMINATE_ERROR",
		"autoscaling:TEST_NOTIFICATION",
		"autoscaling:EC2_INSTANCE_IN_STANDBY",
	}, nil
}

// DescribeLifecycleHookTypes returns the supported lifecycle hook transition types.
func (b *InMemoryBackend) DescribeLifecycleHookTypes() ([]string, error) {
	return []string{
		"autoscaling:EC2_INSTANCE_LAUNCHING",
		"autoscaling:EC2_INSTANCE_TERMINATING",
	}, nil
}

// DescribeMetricCollectionTypes returns the supported metric collection types.
func (b *InMemoryBackend) DescribeMetricCollectionTypes() ([]MetricCollectionType, error) {
	return []MetricCollectionType{
		{Metric: "GroupMinSize", Granularity: granularity1Minute},
		{Metric: "GroupMaxSize", Granularity: granularity1Minute},
		{Metric: "GroupDesiredCapacity", Granularity: granularity1Minute},
		{Metric: "GroupInServiceInstances", Granularity: granularity1Minute},
		{Metric: "GroupPendingInstances", Granularity: granularity1Minute},
		{Metric: "GroupTerminatingInstances", Granularity: granularity1Minute},
	}, nil
}

// DescribeScalingProcessTypes returns the supported scaling process types.
func (b *InMemoryBackend) DescribeScalingProcessTypes() ([]string, error) {
	return []string{
		"Launch",
		"Terminate",
		"HealthCheck",
		"ReplaceUnhealthy",
		"AZRebalance",
		"AlarmNotification",
		"ScheduledActions",
		"AddToLoadBalancer",
		"InstanceRefresh",
	}, nil
}

// DescribeTerminationPolicyTypes returns the supported termination policy types.
func (b *InMemoryBackend) DescribeTerminationPolicyTypes() ([]string, error) {
	return []string{
		"Default",
		"AllocationStrategy",
		"ClosestToNextInstanceHour",
		"NewestInstance",
		"OldestInstance",
		"OldestLaunchConfiguration",
		"OldestLaunchTemplate",
	}, nil
}

// StartInstanceRefreshInput holds the input for StartInstanceRefresh.
type StartInstanceRefreshInput struct {
	AutoScalingGroupName string
	Strategy             string
	Preferences          InstanceRefreshPreferences
}

// StartInstanceRefresh creates a new instance refresh for the group.
func (b *InMemoryBackend) StartInstanceRefresh(groupName string) (*InstanceRefresh, error) {
	return b.StartInstanceRefreshWithInput(StartInstanceRefreshInput{AutoScalingGroupName: groupName})
}

// StartInstanceRefreshWithInput creates a new instance refresh for the group with full input.
func (b *InMemoryBackend) StartInstanceRefreshWithInput(input StartInstanceRefreshInput) (*InstanceRefresh, error) {
	b.mu.Lock("StartInstanceRefresh")
	defer b.mu.Unlock()

	if !b.groups.Has(input.AutoScalingGroupName) {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	strategy := input.Strategy
	if strategy == "" {
		strategy = "Rolling"
	}

	prefs := input.Preferences
	if prefs.MinHealthyPercentage == 0 {
		prefs.MinHealthyPercentage = 90
	}

	refresh := &InstanceRefresh{
		InstanceRefreshID:    uuid.NewString(),
		AutoScalingGroupName: input.AutoScalingGroupName,
		Status:               statusInProgress,
		StartTime:            time.Now(),
		Strategy:             strategy,
		Preferences:          prefs,
	}

	b.instanceRefreshes[input.AutoScalingGroupName] = append(b.instanceRefreshes[input.AutoScalingGroupName], refresh)

	cp := *refresh

	return &cp, nil
}

// RollbackInstanceRefresh rolls back an in-progress instance refresh for the group.
func (b *InMemoryBackend) RollbackInstanceRefresh(groupName string) (string, error) {
	b.mu.Lock("RollbackInstanceRefresh")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return "", fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	for _, r := range b.instanceRefreshes[groupName] {
		if r.Status == statusInProgress || r.Status == "Pending" {
			r.Status = "RollbackInProgress"

			return r.InstanceRefreshID, nil
		}
	}

	return "", fmt.Errorf("%w: no active instance refresh for group %q",
		ErrActiveInstanceRefreshNotFound, groupName)
}

// DescribeInstanceRefreshes returns instance refreshes for the group, optionally filtered by ID.
func (b *InMemoryBackend) DescribeInstanceRefreshes(groupName string, refreshIDs []string) ([]InstanceRefresh, error) {
	b.mu.RLock("DescribeInstanceRefreshes")
	defer b.mu.RUnlock()

	if groupName != "" {
		if !b.groups.Has(groupName) {
			return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
		}
	}

	idFilter := make(map[string]bool, len(refreshIDs))
	for _, id := range refreshIDs {
		idFilter[id] = true
	}

	var result []InstanceRefresh

	groups := b.instanceRefreshes
	if groupName != "" {
		groups = map[string][]*InstanceRefresh{groupName: b.instanceRefreshes[groupName]}
	}

	for _, refreshes := range groups {
		for _, r := range refreshes {
			if len(idFilter) > 0 && !idFilter[r.InstanceRefreshID] {
				continue
			}
			result = append(result, *r)
		}
	}

	return result, nil
}

// DescribeLoadBalancers returns the load balancers attached to the group.
func (b *InMemoryBackend) DescribeLoadBalancers(groupName string) ([]LoadBalancerState, error) {
	b.mu.RLock("DescribeLoadBalancers")
	defer b.mu.RUnlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	result := make([]LoadBalancerState, 0, len(g.LoadBalancerNames))
	for _, lb := range g.LoadBalancerNames {
		result = append(result, LoadBalancerState{LoadBalancerName: lb, State: lbStateAdded})
	}

	return result, nil
}

// DescribeLoadBalancerTargetGroups returns the target groups attached to the group.
func (b *InMemoryBackend) DescribeLoadBalancerTargetGroups(groupName string) ([]LoadBalancerTargetGroupState, error) {
	b.mu.RLock("DescribeLoadBalancerTargetGroups")
	defer b.mu.RUnlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	result := make([]LoadBalancerTargetGroupState, 0, len(g.TargetGroupARNs))
	for _, arn := range g.TargetGroupARNs {
		result = append(result, LoadBalancerTargetGroupState{LoadBalancerTargetGroupARN: arn, State: lbStateAdded})
	}

	return result, nil
}

// DescribeTrafficSources returns the traffic sources attached to the group.
func (b *InMemoryBackend) DescribeTrafficSources(groupName string) ([]TrafficSourceState, error) {
	b.mu.RLock("DescribeTrafficSources")
	defer b.mu.RUnlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	result := make([]TrafficSourceState, 0, len(g.TrafficSources))
	for _, ts := range g.TrafficSources {
		result = append(result, TrafficSourceState{Identifier: ts.Identifier, Type: ts.Type, State: lbStateAdded})
	}

	return result, nil
}

// DetachInstances removes instances from the ASG, optionally decrementing desired capacity.
func (b *InMemoryBackend) DetachInstances(
	groupName string, instanceIDs []string, shouldDecrement bool,
) ([]ScalingActivity, error) {
	b.mu.Lock("DetachInstances")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	idSet := make(map[string]bool, len(instanceIDs))
	for _, id := range instanceIDs {
		idSet[id] = true
	}

	newInstances := make([]Instance, 0, len(g.Instances))
	for _, inst := range g.Instances {
		if !idSet[inst.InstanceID] {
			newInstances = append(newInstances, inst)
		}
	}

	detached := len(g.Instances) - len(newInstances)
	g.Instances = newInstances

	if shouldDecrement && detached > 0 {
		delta := int32(detached) //nolint:gosec // detached <= len(g.Instances), bounded by maxDesiredCapacity
		if g.DesiredCapacity >= delta {
			g.DesiredCapacity -= delta
		} else {
			g.DesiredCapacity = 0
		}
	}

	activities := make([]ScalingActivity, 0, len(instanceIDs))
	for _, id := range instanceIDs {
		act := ScalingActivity{
			ActivityID:           uuid.NewString(),
			AutoScalingGroupName: groupName,
			Description:          "Detaching EC2 instance: " + id,
			StatusCode:           statusCodeSuccessful,
			Progress:             completedProgress,
			StartTime:            time.Now(),
			EndTime:              time.Now(),
		}
		b.activities[groupName] = append(b.activities[groupName], act)
		activities = append(activities, act)
	}

	return activities, nil
}

// DetachLoadBalancerTargetGroups removes target group ARNs from the ASG.
func (b *InMemoryBackend) DetachLoadBalancerTargetGroups(groupName string, targetGroupARNs []string) error {
	b.mu.Lock("DetachLoadBalancerTargetGroups")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	removeSet := make(map[string]bool, len(targetGroupARNs))
	for _, arn := range targetGroupARNs {
		removeSet[arn] = true
	}

	newARNs := make([]string, 0, len(g.TargetGroupARNs))
	for _, arn := range g.TargetGroupARNs {
		if !removeSet[arn] {
			newARNs = append(newARNs, arn)
		}
	}

	g.TargetGroupARNs = newARNs

	return nil
}

// DetachLoadBalancers removes load balancer names from the ASG.
func (b *InMemoryBackend) DetachLoadBalancers(groupName string, lbNames []string) error {
	b.mu.Lock("DetachLoadBalancers")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	removeSet := make(map[string]bool, len(lbNames))
	for _, lb := range lbNames {
		removeSet[lb] = true
	}

	newLBs := make([]string, 0, len(g.LoadBalancerNames))
	for _, lb := range g.LoadBalancerNames {
		if !removeSet[lb] {
			newLBs = append(newLBs, lb)
		}
	}

	g.LoadBalancerNames = newLBs

	return nil
}

// DetachTrafficSources removes traffic sources from the ASG.
func (b *InMemoryBackend) DetachTrafficSources(groupName string, trafficSources []TrafficSource) error {
	b.mu.Lock("DetachTrafficSources")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	type tsKey struct{ Identifier, Type string }
	removeSet := make(map[tsKey]bool, len(trafficSources))
	for _, ts := range trafficSources {
		removeSet[tsKey(ts)] = true
	}

	newTS := make([]TrafficSource, 0, len(g.TrafficSources))
	for _, ts := range g.TrafficSources {
		if !removeSet[tsKey(ts)] {
			newTS = append(newTS, ts)
		}
	}

	g.TrafficSources = newTS

	return nil
}

// isKnownMetric reports whether metric is a valid Auto Scaling metric name.
func isKnownMetric(metric string) bool {
	switch metric {
	case "GroupMinSize", "GroupMaxSize", "GroupDesiredCapacity",
		"GroupInServiceInstances", "GroupPendingInstances", "GroupTerminatingInstances":
		return true
	}

	return false
}

// EnableMetricsCollection adds metrics to the ASG's enabled metrics list.
func (b *InMemoryBackend) EnableMetricsCollection(groupName string, metrics []string, _ string) error {
	b.mu.Lock("EnableMetricsCollection")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	// Validate provided metrics against known metrics
	if len(metrics) > 0 {
		for _, m := range metrics {
			if !isKnownMetric(m) {
				return fmt.Errorf("%w: unknown metric %q", ErrInvalidParameter, m)
			}
		}
	}

	existing := make(map[string]bool, len(g.EnabledMetrics))
	for _, m := range g.EnabledMetrics {
		existing[m] = true
	}

	for _, m := range metrics {
		if !existing[m] {
			g.EnabledMetrics = append(g.EnabledMetrics, m)
		}
	}

	return nil
}

// DisableMetricsCollection removes metrics from the ASG's enabled metrics list.
func (b *InMemoryBackend) DisableMetricsCollection(groupName string, metrics []string) error {
	b.mu.Lock("DisableMetricsCollection")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	if len(metrics) == 0 {
		g.EnabledMetrics = nil

		return nil
	}

	removeSet := make(map[string]bool, len(metrics))
	for _, m := range metrics {
		removeSet[m] = true
	}

	newMetrics := make([]string, 0, len(g.EnabledMetrics))
	for _, m := range g.EnabledMetrics {
		if !removeSet[m] {
			newMetrics = append(newMetrics, m)
		}
	}

	g.EnabledMetrics = newMetrics

	return nil
}

// SuspendProcesses adds processes to the ASG's suspended processes list.
func (b *InMemoryBackend) SuspendProcesses(groupName string, processes []string) error {
	b.mu.Lock("SuspendProcesses")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	for _, p := range processes {
		if !isValidScalingProcessType(p) {
			return fmt.Errorf("%w: unknown process %q", ErrInvalidParameter, p)
		}
	}

	existing := make(map[string]bool, len(g.SuspendedProcesses))
	for _, p := range g.SuspendedProcesses {
		existing[p] = true
	}

	for _, p := range processes {
		if !existing[p] {
			g.SuspendedProcesses = append(g.SuspendedProcesses, p)
		}
	}

	return nil
}

// ResumeProcesses removes processes from the ASG's suspended processes list.
func (b *InMemoryBackend) ResumeProcesses(groupName string, processes []string) error {
	b.mu.Lock("ResumeProcesses")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	if len(processes) == 0 {
		g.SuspendedProcesses = nil

		return nil
	}

	removeSet := make(map[string]bool, len(processes))
	for _, p := range processes {
		removeSet[p] = true
	}

	newProcs := make([]string, 0, len(g.SuspendedProcesses))
	for _, p := range g.SuspendedProcesses {
		if !removeSet[p] {
			newProcs = append(newProcs, p)
		}
	}

	g.SuspendedProcesses = newProcs

	return nil
}

// EnterStandby puts instances into standby mode.
func (b *InMemoryBackend) EnterStandby(
	groupName string,
	instanceIDs []string,
	decrementCapacity bool,
) ([]ScalingActivity, error) {
	b.mu.Lock("EnterStandby")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	idSet := make(map[string]bool, len(instanceIDs))
	for _, id := range instanceIDs {
		idSet[id] = true
	}

	count := 0
	for i := range g.Instances {
		if idSet[g.Instances[i].InstanceID] {
			g.Instances[i].LifecycleState = "Standby"
			count++
		}
	}

	if decrementCapacity && count > 0 {
		delta := int32(count)
		if g.DesiredCapacity >= delta {
			g.DesiredCapacity -= delta
		} else {
			g.DesiredCapacity = 0
		}
	}

	activities := make([]ScalingActivity, 0, len(instanceIDs))
	for _, id := range instanceIDs {
		act := ScalingActivity{
			ActivityID:           uuid.NewString(),
			AutoScalingGroupName: groupName,
			Description:          "Moving EC2 instance to Standby: " + id,
			StatusCode:           statusCodeSuccessful,
			Progress:             completedProgress,
			StartTime:            time.Now(),
			EndTime:              time.Now(),
		}
		b.activities[groupName] = append(b.activities[groupName], act)
		activities = append(activities, act)
	}

	return activities, nil
}

// ExitStandby moves instances from standby back to InService.
func (b *InMemoryBackend) ExitStandby(groupName string, instanceIDs []string) ([]ScalingActivity, error) {
	b.mu.Lock("ExitStandby")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	idSet := make(map[string]bool, len(instanceIDs))
	for _, id := range instanceIDs {
		idSet[id] = true
	}

	for i := range g.Instances {
		if idSet[g.Instances[i].InstanceID] {
			g.Instances[i].LifecycleState = lifecycleStateInService
		}
	}

	activities := make([]ScalingActivity, 0, len(instanceIDs))
	for _, id := range instanceIDs {
		act := ScalingActivity{
			ActivityID:           uuid.NewString(),
			AutoScalingGroupName: groupName,
			Description:          "Moving EC2 instance out of Standby: " + id,
			StatusCode:           statusCodeSuccessful,
			Progress:             completedProgress,
			StartTime:            time.Now(),
			EndTime:              time.Now(),
		}
		b.activities[groupName] = append(b.activities[groupName], act)
		activities = append(activities, act)
	}

	return activities, nil
}

// SetInstanceHealth sets the health status of an instance across all ASGs.
// When shouldRespectGracePeriod is true and the instance launched within the group's
// HealthCheckGracePeriod, the unhealthy mark is silently ignored (AWS behavior).
//
//nolint:gocognit
func (b *InMemoryBackend) SetInstanceHealth(
	instanceID string,
	healthStatus string,
	shouldRespectGracePeriod bool,
) error {
	b.mu.Lock("SetInstanceHealth")
	defer b.mu.Unlock()

	if healthStatus != healthStatusHealthy && healthStatus != "Unhealthy" {
		return fmt.Errorf("%w: HealthStatus must be Healthy or Unhealthy, got %q",
			ErrInvalidParameter, healthStatus)
	}

	for _, g := range b.groups.All() {
		for i := range g.Instances {
			if g.Instances[i].InstanceID != instanceID {
				continue
			}

			if shouldRespectGracePeriod && healthStatus == "Unhealthy" && g.HealthCheckGracePeriod > 0 {
				launchTime := g.Instances[i].LaunchTime
				if !launchTime.IsZero() {
					grace := time.Duration(g.HealthCheckGracePeriod) * time.Second
					if time.Since(launchTime) < grace {
						return nil
					}
				}
			}

			g.Instances[i].HealthStatus = healthStatus

			return nil
		}
	}

	return fmt.Errorf("%w: instance %q not found in any auto scaling group", ErrInstanceNotFound, instanceID)
}

// SetInstanceProtection sets the protected-from-scale-in flag on instances.
func (b *InMemoryBackend) SetInstanceProtection(
	groupName string,
	instanceIDs []string,
	protectedFromScaleIn bool,
) error {
	b.mu.Lock("SetInstanceProtection")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	idSet := make(map[string]bool, len(instanceIDs))
	for _, id := range instanceIDs {
		idSet[id] = true
	}

	for i := range g.Instances {
		if idSet[g.Instances[i].InstanceID] {
			g.Instances[i].ProtectedFromScaleIn = protectedFromScaleIn
		}
	}

	return nil
}

// RecordLifecycleActionHeartbeat resets or validates a lifecycle action heartbeat.
func (b *InMemoryBackend) RecordLifecycleActionHeartbeat(input RecordLifecycleActionHeartbeatInput) error {
	b.mu.Lock("RecordLifecycleActionHeartbeat")
	defer b.mu.Unlock()

	if !b.groups.Has(input.AutoScalingGroupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	if action := b.findPendingHookAction(
		input.LifecycleActionToken, input.AutoScalingGroupName, input.LifecycleHookName, input.InstanceID,
	); action != nil {
		action.timer.Stop()
		token := action.Token
		defaultResult := action.DefaultResult
		action.timer = time.AfterFunc(action.timeout, func() {
			b.resolveLifecycleWait(token, defaultResult)
		})

		return nil
	}

	// Validate hook exists
	if !b.lifecycleHooks.Has(scopedKey(input.AutoScalingGroupName, input.LifecycleHookName)) {
		return fmt.Errorf("%w: lifecycle hook %q not found", ErrLifecycleHookNotFound, input.LifecycleHookName)
	}

	return nil
}

// ExecutePolicy executes a scaling policy on the ASG.
func (b *InMemoryBackend) ExecutePolicy(input ExecutePolicyInput) error {
	b.mu.Lock("ExecutePolicy")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(input.AutoScalingGroupName)
	if !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	policy, ok := b.scalingPolicies.Get(scopedKey(input.AutoScalingGroupName, input.PolicyName))
	if !ok {
		return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, input.PolicyName)
	}

	// Check cooldown if HonorCooldown is requested
	if input.HonorCooldown && policy.Cooldown > 0 && !g.LastScalingActivity.IsZero() {
		cooldownDur := time.Duration(policy.Cooldown) * time.Second
		if time.Since(g.LastScalingActivity) < cooldownDur {
			return fmt.Errorf("%w: scaling activity in progress (cooldown)", ErrScalingActivityInProgress)
		}
	}

	// StepScaling policies select their ScalingAdjustment from StepAdjustments based
	// on where (MetricValue - BreachThreshold) falls; both are required in that case
	// and unsupported otherwise (matches AWS ExecutePolicy validation).
	scalingAdjustment := policy.ScalingAdjustment

	if policy.PolicyType == "StepScaling" {
		if input.MetricValue == nil || input.BreachThreshold == nil {
			return fmt.Errorf(
				"%w: MetricValue and BreachThreshold are required to execute a StepScaling policy",
				ErrInvalidParameter,
			)
		}

		diff := *input.MetricValue - *input.BreachThreshold

		step, found := findStepAdjustment(policy.StepAdjustments, diff)
		if !found {
			return fmt.Errorf(
				"%w: no step adjustment matches MetricValue %v with BreachThreshold %v",
				ErrInvalidParameter, *input.MetricValue, *input.BreachThreshold,
			)
		}

		scalingAdjustment = step.ScalingAdjustment
	}

	var newDesired int32

	switch policy.AdjustmentType {
	case "ExactCapacity":
		newDesired = scalingAdjustment
	case "PercentChangeInCapacity":
		pct := float64(g.DesiredCapacity) * float64(scalingAdjustment) / percentDivisor
		delta := int32(pct)
		newDesired = g.DesiredCapacity + delta
	default: // ChangeInCapacity
		newDesired = g.DesiredCapacity + scalingAdjustment
	}

	newDesired = max(g.MinSize, min(g.MaxSize, newDesired))
	newDesired = min(newDesired, maxDesiredCapacity)

	if g.DesiredCapacity != newDesired {
		// Route through applyDesiredCapacityChange (shared with SetDesiredCapacity)
		// so ExecutePolicy also respects SuspendedProcesses, scale-in protection,
		// instanceIndex bookkeeping, and launch-hook gating instead of duplicating
		// (and diverging from) that logic.
		b.applyDesiredCapacityChange(g, newDesired)
	}

	return nil
}

// findStepAdjustment returns the StepAdjustment whose [MetricIntervalLowerBound,
// MetricIntervalUpperBound) interval contains diff (MetricValue-BreachThreshold), and
// whether one was found. A nil bound means unbounded in that direction, matching AWS.
func findStepAdjustment(steps []StepAdjustment, diff float64) (StepAdjustment, bool) {
	for _, s := range steps {
		lowerOK := s.MetricIntervalLowerBound == nil || diff >= *s.MetricIntervalLowerBound
		upperOK := s.MetricIntervalUpperBound == nil || diff < *s.MetricIntervalUpperBound

		if lowerOK && upperOK {
			return s, true
		}
	}

	return StepAdjustment{}, false
}

// LaunchInstances adds new instances to the ASG.
func (b *InMemoryBackend) LaunchInstances(groupName string, count int32) ([]Instance, error) {
	b.mu.Lock("LaunchInstances")
	defer b.mu.Unlock()

	g, ok := b.groups.Get(groupName)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	oldLen := len(g.Instances)
	newInstances := makeInstances(
		count, g.AvailabilityZones, g.LaunchConfigurationName,
		lcInstanceType(b.launchConfigurations, g.LaunchConfigurationName),
	)
	g.Instances = append(g.Instances, newInstances...)
	g.DesiredCapacity = int32(len(g.Instances)) //nolint:gosec // bounded by maxDesiredCapacity

	for _, inst := range g.Instances[oldLen:] {
		b.instanceIndex[inst.InstanceID] = groupName
	}

	b.gateNewLaunchInstances(g, oldLen)

	// Return a copy reflecting any lifecycle-hook gating just applied (e.g.
	// Pending:Wait), not the pre-gating snapshot.
	result := make([]Instance, len(g.Instances)-oldLen)
	copy(result, g.Instances[oldLen:])

	return result, nil
}

// GetPredictiveScalingForecast validates the group exists (stub implementation).
func (b *InMemoryBackend) GetPredictiveScalingForecast(groupName string) error {
	b.mu.RLock("GetPredictiveScalingForecast")
	defer b.mu.RUnlock()

	if !b.groups.Has(groupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	return nil
}

// PutNotificationConfiguration stores or updates a notification configuration.
func (b *InMemoryBackend) PutNotificationConfiguration(groupName, topicARN string, types []string) error {
	b.mu.Lock("PutNotificationConfiguration")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	// Validate SNS ARN format
	if topicARN != "" &&
		!strings.HasPrefix(topicARN, "arn:aws:sns:") &&
		!strings.HasPrefix(topicARN, "arn:aws-cn:sns:") &&
		!strings.HasPrefix(topicARN, "arn:aws-us-gov:sns:") {
		return fmt.Errorf("%w: TopicARN must be a valid SNS ARN", ErrInvalidParameter)
	}

	// Remove existing configs for this group+topic
	existing := b.notificationConfigs[groupName]
	newConfigs := make([]*NotificationConfiguration, 0, len(existing)+len(types))

	for _, c := range existing {
		if c.TopicARN != topicARN {
			newConfigs = append(newConfigs, c)
		}
	}

	for _, t := range types {
		nc := &NotificationConfiguration{
			AutoScalingGroupName: groupName,
			TopicARN:             topicARN,
			NotificationType:     t,
		}
		newConfigs = append(newConfigs, nc)
	}

	b.notificationConfigs[groupName] = newConfigs

	return nil
}

// DeleteNotificationConfiguration removes notification configs for a group+topic.
func (b *InMemoryBackend) DeleteNotificationConfiguration(groupName, topicARN string) error {
	b.mu.Lock("DeleteNotificationConfiguration")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	existing := b.notificationConfigs[groupName]
	newConfigs := make([]*NotificationConfiguration, 0, len(existing))

	for _, c := range existing {
		if c.TopicARN != topicARN {
			newConfigs = append(newConfigs, c)
		}
	}

	b.notificationConfigs[groupName] = newConfigs

	return nil
}

// DescribeNotificationConfigurations returns notification configs for the given groups.
func (b *InMemoryBackend) DescribeNotificationConfigurations(groupNames []string) ([]NotificationConfiguration, error) {
	b.mu.RLock("DescribeNotificationConfigurations")
	defer b.mu.RUnlock()

	var result []NotificationConfiguration

	if len(groupNames) > 0 {
		for _, name := range groupNames {
			for _, c := range b.notificationConfigs[name] {
				result = append(result, *c)
			}
		}
	} else {
		for _, configs := range b.notificationConfigs {
			for _, c := range configs {
				result = append(result, *c)
			}
		}
	}

	return result, nil
}

// PutScalingPolicy creates or updates a scaling policy.
func (b *InMemoryBackend) PutScalingPolicy(input ScalingPolicyInput) (*ScalingPolicy, error) {
	b.mu.Lock("PutScalingPolicy")
	defer b.mu.Unlock()

	if !b.groups.Has(input.AutoScalingGroupName) {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	arn := "arn:aws:autoscaling:" + config.DefaultRegion + ":" + config.DefaultAccountID + ":scalingPolicy:" +
		uuid.NewString() + ":autoScalingGroupName/" + input.AutoScalingGroupName + ":policyName/" + input.PolicyName

	// Preserve ARN if policy already exists
	if existing, ok := b.scalingPolicies.Get(scopedKey(input.AutoScalingGroupName, input.PolicyName)); ok {
		arn = existing.PolicyARN
	}

	policy := &ScalingPolicy{
		PolicyName:                    input.PolicyName,
		PolicyARN:                     arn,
		AutoScalingGroupName:          input.AutoScalingGroupName,
		PolicyType:                    input.PolicyType,
		AdjustmentType:                input.AdjustmentType,
		MetricAggregationType:         input.MetricAggregationType,
		CustomizedMetricSpecification: input.CustomizedMetricSpecification,
		ScalingAdjustment:             input.ScalingAdjustment,
		MinAdjustmentStep:             input.MinAdjustmentStep,
		MinAdjustmentMagnitude:        input.MinAdjustmentMagnitude,
		Cooldown:                      input.Cooldown,
		TargetValue:                   input.TargetValue,
		MetricType:                    input.MetricType,
		DisableScaleIn:                input.DisableScaleIn,
		EstimatedWarmup:               input.EstimatedWarmup,
		StepAdjustments:               input.StepAdjustments,
	}

	b.scalingPolicies.Put(policy)

	cp := *policy

	return &cp, nil
}

// DeletePolicy removes a scaling policy from the ASG.
func (b *InMemoryBackend) DeletePolicy(groupName, policyNameOrARN string) error {
	b.mu.Lock("DeletePolicy")
	defer b.mu.Unlock()

	if groupName != "" {
		// Try by name first
		key := scopedKey(groupName, policyNameOrARN)
		if b.scalingPolicies.Has(key) {
			b.scalingPolicies.Delete(key)

			return nil
		}

		// Try by ARN
		for _, p := range b.scalingPoliciesInGroupLocked(groupName) {
			if p.PolicyARN == policyNameOrARN {
				b.scalingPolicies.Delete(scalingPoliciesKeyFn(p))

				return nil
			}
		}

		return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyNameOrARN)
	}

	// Check across all groups if groupName is empty
	for _, p := range b.scalingPolicies.All() {
		if p.PolicyName == policyNameOrARN || p.PolicyARN == policyNameOrARN {
			b.scalingPolicies.Delete(scalingPoliciesKeyFn(p))

			return nil
		}
	}

	return fmt.Errorf("%w: policy %q not found", ErrPolicyNotFound, policyNameOrARN)
}

// DescribePolicies returns scaling policies for the given group, optionally filtered by name.
func (b *InMemoryBackend) DescribePolicies(groupName string, policyNames []string) ([]ScalingPolicy, error) {
	b.mu.RLock("DescribePolicies")
	defer b.mu.RUnlock()

	nameFilter := make(map[string]bool, len(policyNames))
	for _, n := range policyNames {
		nameFilter[n] = true
	}

	var result []ScalingPolicy

	if groupName != "" {
		for _, p := range b.scalingPoliciesByGroup.Get(groupName) {
			if len(nameFilter) == 0 || nameFilter[p.PolicyName] {
				result = append(result, *p)
			}
		}
	} else {
		for _, p := range b.scalingPolicies.All() {
			if len(nameFilter) == 0 || nameFilter[p.PolicyName] {
				result = append(result, *p)
			}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].PolicyName < result[j].PolicyName
	})

	return result, nil
}

// PutScheduledUpdateGroupAction creates or updates a single scheduled action.
func (b *InMemoryBackend) PutScheduledUpdateGroupAction(groupName string, action ScheduledUpdateGroupAction) error {
	b.mu.Lock("PutScheduledUpdateGroupAction")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	scheduledARN := fmt.Sprintf(
		"arn:aws:autoscaling:%s:%s:scheduledUpdateGroupAction:%s:autoScalingGroupName/%s:scheduledActionName/%s",
		config.DefaultRegion, config.DefaultAccountID, uuid.NewString(), groupName, action.ScheduledActionName,
	)

	b.scheduledActions.Put(&ScheduledAction{
		ScheduledActionName:  action.ScheduledActionName,
		ScheduledActionARN:   scheduledARN,
		AutoScalingGroupName: groupName,
		Recurrence:           action.Recurrence,
		TimeZone:             action.TimeZone,
		StartTime:            action.StartTime,
		EndTime:              action.EndTime,
		DesiredCapacity:      action.DesiredCapacity,
		MinSize:              action.MinSize,
		MaxSize:              action.MaxSize,
	})

	return nil
}

// DeleteScheduledAction removes a single scheduled action from the ASG.
func (b *InMemoryBackend) DeleteScheduledAction(groupName, scheduledActionName string) error {
	b.mu.Lock("DeleteScheduledAction")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	key := scopedKey(groupName, scheduledActionName)
	if !b.scheduledActions.Has(key) {
		return fmt.Errorf("%w: scheduled action %q not found", ErrInvalidParameter, scheduledActionName)
	}

	b.scheduledActions.Delete(key)

	return nil
}

// PutWarmPool creates or updates a warm pool configuration for the ASG.
func (b *InMemoryBackend) PutWarmPool(input WarmPoolInput) error {
	b.mu.Lock("PutWarmPool")
	defer b.mu.Unlock()

	if !b.groups.Has(input.AutoScalingGroupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	poolState := input.PoolState
	if poolState == "" {
		poolState = "Stopped"
	} else if poolState != "Stopped" && poolState != "Running" && poolState != "Hibernated" {
		return fmt.Errorf("%w: PoolState must be one of Stopped, Running, Hibernated; got %q",
			ErrInvalidParameter, poolState)
	}

	b.warmPools.Put(&WarmPool{
		AutoScalingGroupName:     input.AutoScalingGroupName,
		PoolState:                poolState,
		MinSize:                  input.MinSize,
		MaxGroupPreparedCapacity: input.MaxGroupPreparedCapacity,
		InstanceReusePolicy:      input.InstanceReusePolicy,
	})

	return nil
}

// DeleteWarmPool removes the warm pool for the ASG.
func (b *InMemoryBackend) DeleteWarmPool(groupName string) error {
	b.mu.Lock("DeleteWarmPool")
	defer b.mu.Unlock()

	if !b.groups.Has(groupName) {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	b.warmPools.Delete(groupName)

	return nil
}

// DescribeWarmPool returns the warm pool configuration for the ASG.
func (b *InMemoryBackend) DescribeWarmPool(groupName string) (*WarmPool, error) {
	b.mu.RLock("DescribeWarmPool")
	defer b.mu.RUnlock()

	if !b.groups.Has(groupName) {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	wp, ok := b.warmPools.Get(groupName)
	if !ok {
		return nil, fmt.Errorf("%w: no warm pool found for group %q", ErrWarmPoolNotFound, groupName)
	}

	cp := *wp

	return &cp, nil
}
