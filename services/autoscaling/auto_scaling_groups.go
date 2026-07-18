package autoscaling

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// lcInstanceType returns the InstanceType from the named launch configuration, or
// "t2.micro" if the launch configuration is not found (preserving previous default).
func lcInstanceType(lcs *store.Table[LaunchConfiguration], lcName string) string {
	if lc, ok := lcs.Get(lcName); ok && lc.InstanceType != "" {
		return lc.InstanceType
	}

	return "t2.micro"
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
		CreatedTime:                      time.Now(),
	}

	// Use the shared makeInstances helper (real EC2 instances when an
	// EC2Launcher is wired, fabricated IDs otherwise) so all initial
	// instances use the same launch path as later scale-out.
	group.Instances = b.makeInstances(group, desired)

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
			b.applyScaleIn(g, int(newDesired))
		}
	case newDesired > current:
		if !suspendedSet["Launch"] {
			oldLen := len(g.Instances)
			g.Instances = b.adjustInstances(g, g.Instances, g.DesiredCapacity)
			// Add newly launched instances to index.
			for _, inst := range g.Instances[oldLen:] {
				b.instanceIndex[inst.InstanceID] = g.AutoScalingGroupName
			}

			b.gateNewLaunchInstances(g, oldLen)
		}
	}

	g.LastScalingActivity = time.Now()
}

// applyScaleIn reduces g toward targetCount, skipping instances that are
// ProtectedFromScaleIn or already mid-termination (Terminating:Wait). If all
// remaining eligible instances are protected/already-waiting, it stops short of
// the target, matching real AWS ("scale-in protection" semantics). O(N) via
// two-index compaction.
//
// When the group has an active EC2_INSTANCE_TERMINATING lifecycle hook, selected
// instances are NOT removed immediately: they are transitioned to
// Terminating:Wait (remaining in g.Instances, per the LifecycleState state
// machine) and a heartbeat timer is armed, mirroring
// TerminateInstanceInAutoScalingGroup's hook-gating instead of always removing
// instances instantly regardless of configured hooks. DesiredCapacity/MinSize
// have already been set to their target by the caller
// (applyDesiredCapacityChange), so the pending action uses
// terminationCapacityPreset: once the wait resolves, only the instance removal
// itself is applied, with no further capacity bookkeeping or replacement launch.
// Must be called with b.mu held (write lock).
func (b *InMemoryBackend) applyScaleIn(g *AutoScalingGroup, targetCount int) {
	toRemove := len(g.Instances) - targetCount
	if toRemove <= 0 {
		return
	}

	// Collect indices of eligible instances from the end.
	removeSet := make(map[int]bool, toRemove)

	for i := len(g.Instances) - 1; i >= 0 && len(removeSet) < toRemove; i-- {
		inst := &g.Instances[i]
		if !inst.ProtectedFromScaleIn && inst.LifecycleState != lifecycleStateTerminatingWait {
			removeSet[i] = true
		}
	}

	if len(removeSet) == 0 {
		return
	}

	hook := findHookForTransition(b.lifecycleHooksByGroup.Get(g.AutoScalingGroupName), transitionTerminating)
	if hook != nil {
		for i := range g.Instances {
			if removeSet[i] {
				b.armLifecycleWait(g, hook, &g.Instances[i], transitionTerminating, terminationCapacityPreset)
			}
		}

		return
	}

	// No hook registered: remove immediately, as before.
	removedIDs := make([]string, 0, len(removeSet))
	result := g.Instances[:0]

	for i, inst := range g.Instances {
		if removeSet[i] {
			removedIDs = append(removedIDs, inst.InstanceID)

			continue
		}

		result = append(result, inst)
	}

	g.Instances = result

	for _, id := range removedIDs {
		delete(b.instanceIndex, id)
	}

	b.terminateInEC2(removedIDs)
	b.deregisterELBTargets(removedIDs, g.TargetGroupARNs)
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
