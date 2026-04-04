package autoscaling

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// completedProgress is the progress value for a successfully completed scaling activity.
const completedProgress = int32(100)

// maxDesiredCapacity is the upper bound on DesiredCapacity for any ASG, used to
// cap user-supplied values and prevent excessive slice allocations
// (go/slice-memory-allocation-excessive-size).
const maxDesiredCapacity = 100

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
)

// StorageBackend is the interface for the Autoscaling in-memory store.
type StorageBackend interface {
	CreateAutoScalingGroup(input CreateAutoScalingGroupInput) (*AutoScalingGroup, error)
	DescribeAutoScalingGroups(names []string) ([]AutoScalingGroup, error)
	UpdateAutoScalingGroup(input UpdateAutoScalingGroupInput) (*AutoScalingGroup, error)
	DeleteAutoScalingGroup(name string) error

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
}

// CreateAutoScalingGroupInput holds the input for CreateAutoScalingGroup.
type CreateAutoScalingGroupInput struct {
	AutoScalingGroupName    string
	LaunchConfigurationName string
	HealthCheckType         string
	AvailabilityZones       []string
	LoadBalancerNames       []string
	TargetGroupARNs         []string
	Tags                    []Tag
	MinSize                 int32
	MaxSize                 int32
	DesiredCapacity         int32
	DefaultCooldown         int32
	HealthCheckGracePeriod  int32
}

// UpdateAutoScalingGroupInput holds the input for UpdateAutoScalingGroup.
type UpdateAutoScalingGroupInput struct {
	MinSize                 *int32
	MaxSize                 *int32
	DesiredCapacity         *int32
	DefaultCooldown         *int32
	HealthCheckGracePeriod  *int32
	AutoScalingGroupName    string
	LaunchConfigurationName string
	HealthCheckType         string
	AvailabilityZones       []string
}

// CreateLaunchConfigurationInput holds the input for CreateLaunchConfiguration.
type CreateLaunchConfigurationInput struct {
	LaunchConfigurationName string
	ImageID                 string
	InstanceType            string
	KeyName                 string
	IAMInstanceProfile      string
	UserData                string
	KernelID                string
	RamdiskID               string
	SecurityGroups          []string
	BlockDeviceMappings     []BlockDeviceMapping
}

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	groups               map[string]*AutoScalingGroup
	launchConfigurations map[string]*LaunchConfiguration
	activities           map[string][]ScalingActivity
	scheduledActions     map[string]map[string]*ScheduledAction
	instanceRefreshes    map[string][]*InstanceRefresh
	lifecycleHooks       map[string]map[string]*LifecycleHook
	mu                   *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		groups:               make(map[string]*AutoScalingGroup),
		launchConfigurations: make(map[string]*LaunchConfiguration),
		activities:           make(map[string][]ScalingActivity),
		scheduledActions:     make(map[string]map[string]*ScheduledAction),
		instanceRefreshes:    make(map[string][]*InstanceRefresh),
		lifecycleHooks:       make(map[string]map[string]*LifecycleHook),
		mu:                   lockmetrics.New("autoscaling"),
	}
}

// makeInstances creates the desired number of healthy InService instances for an ASG.
// The fake service immediately puts instances in InService/Healthy state so that
// Terraform provider capacity checks do not time out.
func makeInstances(count int32, azs []string, launchConfigName string) []Instance {
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

	for range n {
		// Use full UUID (stripped of dashes) to generate a unique, collision-free instance ID.
		id := "i-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:17]
		instances = append(instances, Instance{
			InstanceID:              id,
			AvailabilityZone:        az,
			LifecycleState:          "InService",
			HealthStatus:            "Healthy",
			LaunchConfigurationName: launchConfigName,
			InstanceType:            "t2.micro",
		})
	}

	return instances
}

// adjustInstances adjusts the instances slice to match the new desired count.
// It adds or removes instances from the end, preserving existing instance IDs.
func adjustInstances(existing []Instance, desired int32, azs []string, launchConfigName string) []Instance {
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

	return append(existing, makeInstances(delta, azs, launchConfigName)...)
}

// CreateAutoScalingGroup creates a new Auto Scaling group.
func (b *InMemoryBackend) CreateAutoScalingGroup(input CreateAutoScalingGroupInput) (*AutoScalingGroup, error) {
	b.mu.Lock("CreateAutoScalingGroup")
	defer b.mu.Unlock()

	if _, exists := b.groups[input.AutoScalingGroupName]; exists {
		return nil, fmt.Errorf("%w: group %q already exists", ErrGroupAlreadyExists, input.AutoScalingGroupName)
	}

	if input.AutoScalingGroupName == "" {
		return nil, fmt.Errorf("%w: AutoScalingGroupName is required", ErrInvalidParameter)
	}

	desired := input.DesiredCapacity
	if desired == 0 {
		desired = input.MinSize
	}

	// Clamp desired to [0, maxDesiredCapacity] so that CodeQL can verify
	// the make capacity hint is bounded (go/slice-memory-allocation-excessive-size).
	desiredN := max(0, min(maxDesiredCapacity, int(desired)))

	healthCheckType := input.HealthCheckType
	if healthCheckType == "" {
		healthCheckType = "EC2"
	}

	az := defaultAvailabilityZone
	if len(input.AvailabilityZones) > 0 {
		az = input.AvailabilityZones[0]
	}

	// No capacity hint — user-derived values in make capacity trigger CodeQL
	// go/slice-memory-allocation-excessive-size even after clamping.
	// nolint:prealloc,nolintlint // satisfies CodeQL by removing tainted capacity hint
	instances := make([]Instance, 0)
	for range desiredN {
		instances = append(instances, Instance{
			InstanceID:              "i-" + uuid.NewString()[:8],
			AvailabilityZone:        az,
			LifecycleState:          "InService",
			HealthStatus:            "Healthy",
			LaunchConfigurationName: input.LaunchConfigurationName,
			InstanceType:            "t2.micro",
		})
	}

	group := &AutoScalingGroup{
		AutoScalingGroupName: input.AutoScalingGroupName,
		AutoScalingGroupARN: "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:" +
			uuid.NewString() + ":autoScalingGroupName/" + input.AutoScalingGroupName,
		LaunchConfigurationName: input.LaunchConfigurationName,
		MinSize:                 input.MinSize,
		MaxSize:                 input.MaxSize,
		DesiredCapacity:         desired,
		DefaultCooldown:         input.DefaultCooldown,
		HealthCheckType:         healthCheckType,
		HealthCheckGracePeriod:  input.HealthCheckGracePeriod,
		AvailabilityZones:       input.AvailabilityZones,
		LoadBalancerNames:       input.LoadBalancerNames,
		TargetGroupARNs:         input.TargetGroupARNs,
		Tags:                    input.Tags,
		Instances:               instances,
		CreatedTime:             time.Now(),
		Status:                  "Active",
	}

	b.groups[input.AutoScalingGroupName] = group

	b.activities[input.AutoScalingGroupName] = append(
		b.activities[input.AutoScalingGroupName],
		ScalingActivity{
			ActivityID:           uuid.NewString(),
			AutoScalingGroupName: input.AutoScalingGroupName,
			Description:          "Launching a new EC2 instance",
			StatusCode:           "Successful",
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

	if len(names) > 0 {
		result := make([]AutoScalingGroup, 0, len(names))

		for _, name := range names {
			g, ok := b.groups[name]
			if !ok {
				return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, name)
			}

			cp := *g
			result = append(result, cp)
		}

		return result, nil
	}

	result := make([]AutoScalingGroup, 0, len(b.groups))
	for _, g := range b.groups {
		result = append(result, *g)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AutoScalingGroupName < result[j].AutoScalingGroupName
	})

	return result, nil
}

// UpdateAutoScalingGroup updates an existing Auto Scaling group.
func (b *InMemoryBackend) UpdateAutoScalingGroup(input UpdateAutoScalingGroupInput) (*AutoScalingGroup, error) {
	b.mu.Lock("UpdateAutoScalingGroup")
	defer b.mu.Unlock()

	g, ok := b.groups[input.AutoScalingGroupName]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	if input.MinSize != nil {
		g.MinSize = *input.MinSize
	}

	if input.MaxSize != nil {
		g.MaxSize = *input.MaxSize
	}

	if input.DesiredCapacity != nil {
		desired := min(*input.DesiredCapacity, maxDesiredCapacity)
		g.DesiredCapacity = desired
		g.Instances = adjustInstances(g.Instances, g.DesiredCapacity, g.AvailabilityZones, g.LaunchConfigurationName)
	}

	if input.DefaultCooldown != nil {
		g.DefaultCooldown = *input.DefaultCooldown
	}

	if input.HealthCheckGracePeriod != nil {
		g.HealthCheckGracePeriod = *input.HealthCheckGracePeriod
	}

	if input.LaunchConfigurationName != "" {
		g.LaunchConfigurationName = input.LaunchConfigurationName
	}

	if input.HealthCheckType != "" {
		g.HealthCheckType = input.HealthCheckType
	}

	if len(input.AvailabilityZones) > 0 {
		g.AvailabilityZones = input.AvailabilityZones
	}

	cp := *g

	return &cp, nil
}

// DeleteAutoScalingGroup removes an Auto Scaling group by name.
func (b *InMemoryBackend) DeleteAutoScalingGroup(name string) error {
	b.mu.Lock("DeleteAutoScalingGroup")
	defer b.mu.Unlock()

	if _, ok := b.groups[name]; !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, name)
	}

	delete(b.groups, name)
	delete(b.activities, name)

	return nil
}

// CreateLaunchConfiguration creates a new launch configuration.
func (b *InMemoryBackend) CreateLaunchConfiguration(
	input CreateLaunchConfigurationInput,
) (*LaunchConfiguration, error) {
	b.mu.Lock("CreateLaunchConfiguration")
	defer b.mu.Unlock()

	if _, exists := b.launchConfigurations[input.LaunchConfigurationName]; exists {
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
		LaunchConfigurationARN: "arn:aws:autoscaling:us-east-1:000000000000:launchConfiguration:" +
			uuid.NewString() + ":launchConfigurationName/" + input.LaunchConfigurationName,
		ImageID:             input.ImageID,
		InstanceType:        input.InstanceType,
		KeyName:             input.KeyName,
		IAMInstanceProfile:  input.IAMInstanceProfile,
		UserData:            input.UserData,
		KernelID:            input.KernelID,
		RamdiskID:           input.RamdiskID,
		SecurityGroups:      input.SecurityGroups,
		BlockDeviceMappings: input.BlockDeviceMappings,
		CreatedTime:         time.Now(),
	}

	b.launchConfigurations[input.LaunchConfigurationName] = lc

	cp := *lc

	return &cp, nil
}

// DescribeLaunchConfigurations returns launch configurations, optionally filtered by name.
func (b *InMemoryBackend) DescribeLaunchConfigurations(names []string) ([]LaunchConfiguration, error) {
	b.mu.RLock("DescribeLaunchConfigurations")
	defer b.mu.RUnlock()

	if len(names) > 0 {
		result := make([]LaunchConfiguration, 0, len(names))

		for _, name := range names {
			lc, ok := b.launchConfigurations[name]
			if !ok {
				return nil, fmt.Errorf("%w: %q", ErrLaunchConfigurationNotFound, name)
			}

			cp := *lc
			result = append(result, cp)
		}

		return result, nil
	}

	result := make([]LaunchConfiguration, 0, len(b.launchConfigurations))
	for _, lc := range b.launchConfigurations {
		result = append(result, *lc)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].LaunchConfigurationName < result[j].LaunchConfigurationName
	})

	return result, nil
}

// DeleteLaunchConfiguration removes a launch configuration by name.
func (b *InMemoryBackend) DeleteLaunchConfiguration(name string) error {
	b.mu.Lock("DeleteLaunchConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.launchConfigurations[name]; !ok {
		return fmt.Errorf("%w: %q", ErrLaunchConfigurationNotFound, name)
	}

	delete(b.launchConfigurations, name)

	return nil
}

// DescribeScalingActivities returns scaling activities for the given group.
func (b *InMemoryBackend) DescribeScalingActivities(groupName string) ([]ScalingActivity, error) {
	b.mu.RLock("DescribeScalingActivities")
	defer b.mu.RUnlock()

	if groupName != "" {
		if _, ok := b.groups[groupName]; !ok {
			return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
		}

		acts := b.activities[groupName]
		result := make([]ScalingActivity, len(acts))
		copy(result, acts)

		return result, nil
	}

	result := make([]ScalingActivity, 0)
	for _, acts := range b.activities {
		result = append(result, acts...)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ActivityID < result[j].ActivityID
	})

	return result, nil
}

// Purge removes all AutoScaling groups and launch configurations created before the cutoff time.
func (b *InMemoryBackend) Purge(cutoff time.Time) {
	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	// 1. Purge groups
	for name, g := range b.groups {
		if g.CreatedTime.Before(cutoff) {
			delete(b.groups, name)
			delete(b.activities, name)
			delete(b.scheduledActions, name)
			delete(b.instanceRefreshes, name)
			delete(b.lifecycleHooks, name)
		}
	}

	// 2. Purge launch configurations
	for name, lc := range b.launchConfigurations {
		if lc.CreatedTime.Before(cutoff) {
			delete(b.launchConfigurations, name)
		}
	}
}

// AttachInstances adds the given instance IDs to the specified Auto Scaling group.
func (b *InMemoryBackend) AttachInstances(groupName string, instanceIDs []string) error {
	b.mu.Lock("AttachInstances")
	defer b.mu.Unlock()

	g, ok := b.groups[groupName]
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
			LifecycleState:          "InService",
			HealthStatus:            "Healthy",
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

	g, ok := b.groups[groupName]
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

	g, ok := b.groups[groupName]
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

	g, ok := b.groups[groupName]
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

	if _, ok := b.groups[groupName]; !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	actions := b.scheduledActions[groupName]

	var failed []FailedScheduledAction

	for _, name := range scheduledActionNames {
		if actions == nil {
			failed = append(failed, FailedScheduledAction{
				ScheduledActionName: name,
				ErrorCode:           "ValidationError",
				ErrorMessage:        fmt.Sprintf("scheduled action %q not found", name),
			})

			continue
		}

		if _, exists := actions[name]; !exists {
			failed = append(failed, FailedScheduledAction{
				ScheduledActionName: name,
				ErrorCode:           "ValidationError",
				ErrorMessage:        fmt.Sprintf("scheduled action %q not found", name),
			})

			continue
		}

		delete(actions, name)
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

	if _, ok := b.groups[groupName]; !ok {
		return nil, fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	if b.scheduledActions[groupName] == nil {
		b.scheduledActions[groupName] = make(map[string]*ScheduledAction)
	}

	var failed []FailedScheduledAction

	for _, a := range actions {
		if a.ScheduledActionName == "" {
			failed = append(failed, FailedScheduledAction{
				ScheduledActionName: a.ScheduledActionName,
				ErrorCode:           "ValidationError",
				ErrorMessage:        "ScheduledActionName is required",
			})

			continue
		}

		b.scheduledActions[groupName][a.ScheduledActionName] = &ScheduledAction{
			ScheduledActionName:  a.ScheduledActionName,
			AutoScalingGroupName: groupName,
			Recurrence:           a.Recurrence,
			TimeZone:             a.TimeZone,
			StartTime:            a.StartTime,
			EndTime:              a.EndTime,
			DesiredCapacity:      a.DesiredCapacity,
			MinSize:              a.MinSize,
			MaxSize:              a.MaxSize,
		}
	}

	return failed, nil
}

// CancelInstanceRefresh cancels an active instance refresh for the group.
// It returns the ID of the cancelled refresh.
func (b *InMemoryBackend) CancelInstanceRefresh(groupName string) (string, error) {
	b.mu.Lock("CancelInstanceRefresh")
	defer b.mu.Unlock()

	if _, ok := b.groups[groupName]; !ok {
		return "", fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	for _, r := range b.instanceRefreshes[groupName] {
		if r.Status == "InProgress" || r.Status == "Pending" {
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

	if _, ok := b.groups[input.AutoScalingGroupName]; !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, input.AutoScalingGroupName)
	}

	if input.LifecycleHookName == "" {
		return fmt.Errorf("%w: LifecycleHookName is required", ErrInvalidParameter)
	}

	if input.LifecycleActionResult == "" {
		return fmt.Errorf("%w: LifecycleActionResult is required", ErrInvalidParameter)
	}

	return nil
}

// CreateOrUpdateTags creates or updates tags on Auto Scaling resources.
// Only group (auto-scaling-group) resource tags are currently supported.
func (b *InMemoryBackend) CreateOrUpdateTags(tags []ResourceTag) error {
	b.mu.Lock("CreateOrUpdateTags")
	defer b.mu.Unlock()

	for _, tag := range tags {
		if tag.ResourceType != "auto-scaling-group" {
			continue
		}

		g, ok := b.groups[tag.ResourceID]
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

	if _, ok := b.groups[groupName]; !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, groupName)
	}

	hooks := b.lifecycleHooks[groupName]
	if hooks == nil {
		return fmt.Errorf("%w: lifecycle hook %q not found", ErrLifecycleHookNotFound, hookName)
	}

	if _, exists := hooks[hookName]; !exists {
		return fmt.Errorf("%w: lifecycle hook %q not found", ErrLifecycleHookNotFound, hookName)
	}

	delete(hooks, hookName)

	return nil
}

// AddLifecycleHook stores a lifecycle hook for the given group (used internally and by PutLifecycleHook).
func (b *InMemoryBackend) AddLifecycleHook(hook LifecycleHook) error {
	b.mu.Lock("AddLifecycleHook")
	defer b.mu.Unlock()

	if _, ok := b.groups[hook.AutoScalingGroupName]; !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, hook.AutoScalingGroupName)
	}

	if b.lifecycleHooks[hook.AutoScalingGroupName] == nil {
		b.lifecycleHooks[hook.AutoScalingGroupName] = make(map[string]*LifecycleHook)
	}

	cp := hook
	b.lifecycleHooks[hook.AutoScalingGroupName][hook.LifecycleHookName] = &cp

	return nil
}

// AddInstanceRefresh stores an instance refresh for the given group (used for testing CancelInstanceRefresh).
func (b *InMemoryBackend) AddInstanceRefresh(refresh InstanceRefresh) error {
	b.mu.Lock("AddInstanceRefresh")
	defer b.mu.Unlock()

	if _, ok := b.groups[refresh.AutoScalingGroupName]; !ok {
		return fmt.Errorf("%w: %q", ErrGroupNotFound, refresh.AutoScalingGroupName)
	}

	cp := refresh
	b.instanceRefreshes[refresh.AutoScalingGroupName] = append(
		b.instanceRefreshes[refresh.AutoScalingGroupName],
		&cp,
	)

	return nil
}
