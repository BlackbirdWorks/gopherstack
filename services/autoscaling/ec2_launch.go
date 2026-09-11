package autoscaling

import (
	"context"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// InstanceLaunchSpec carries the EC2 launch parameters an Auto Scaling group
// derives from its LaunchConfiguration (LaunchTemplate/MixedInstancesPolicy
// support is a follow-up — see EC2Launcher doc comment) for a single scale-out
// batch, passed to an EC2Launcher so it can create real backing EC2 instances.
type InstanceLaunchSpec struct {
	Tags             map[string]string
	ImageID          string
	InstanceType     string
	SubnetID         string
	AvailabilityZone string
	KeyName          string
	SecurityGroups   []string
}

// EC2Launcher lets the Auto Scaling backend create and destroy real (mock) EC2
// instances backing an Auto Scaling group's members, so instances an ASG
// scales out are visible to EC2 DescribeInstances and instances it scales in
// are actually terminated there too. When no EC2Launcher is configured (the
// default), the backend falls back to its historical behavior of fabricating
// instance IDs with no EC2-side record — see SetEC2Launcher.
type EC2Launcher interface {
	// LaunchInstances launches count instances per spec and returns the real
	// EC2 instance IDs created. A partial success (fewer IDs than count) is
	// treated as the actual outcome, not an error.
	LaunchInstances(ctx context.Context, spec InstanceLaunchSpec, count int) ([]string, error)
	// TerminateInstances terminates the given real EC2 instance IDs.
	TerminateInstances(ctx context.Context, ids []string) error
	// ResolveLaunchTemplate resolves a LaunchTemplate ID or name (and version) to its ImageID and InstanceType.
	ResolveLaunchTemplate(ctx context.Context, id, name, version string) (imageID, instanceType string, err error)
}

// SetEC2Launcher wires an EC2Launcher so subsequent scale-out/scale-in
// operations launch and terminate real (mock) EC2 instances instead of
// fabricating instance IDs. Passing nil restores the historical,
// fabrication-only behavior. Intended to be called once during service
// wiring, before the backend serves traffic.
func (b *InMemoryBackend) SetEC2Launcher(l EC2Launcher) {
	b.mu.Lock("SetEC2Launcher")
	defer b.mu.Unlock()
	b.ec2Launcher = l
}

// makeInstances creates count new Instance records belonging to g: real (mock)
// EC2 instances launched via b.ec2Launcher when one is configured and g's
// launch configuration resolves to a usable spec, or synthetic fabricated
// instance IDs otherwise (the historical, EC2-less behavior). It does not
// mutate g or b.instanceIndex; callers remain responsible for both, exactly as
// they were with the pre-existing free-function makeInstances/adjustInstances
// helpers this replaces. Must be called with b.mu held (write lock).
func (b *InMemoryBackend) makeInstances(g *AutoScalingGroup, count int32) []Instance {
	n := max(0, min(maxDesiredCapacity, int(count)))
	if n == 0 {
		return []Instance{}
	}

	az := defaultAvailabilityZone
	if len(g.AvailabilityZones) > 0 {
		az = g.AvailabilityZones[0]
	}

	instanceType := lcInstanceType(b.launchConfigurations, g.LaunchConfigurationName)

	if instances, ok := b.launchInEC2(g, az, n, instanceType); ok {
		return instances
	}

	instances := fabricateInstances(n, az, g.LaunchConfigurationName, instanceType)
	b.registerELBTargets(instanceIDsOf(instances), g.TargetGroupARNs)
	b.registerELBInstances(instanceIDsOf(instances), g.LoadBalancerNames)

	return instances
}

// launchInEC2 attempts to launch n real instances in the wired EC2 launcher.
// Reports ok=false on any error or missing spec so the caller can fall back to
// fabrication. A MixedInstancesPolicy with multiple Overrides resolves to
// multiple specs, launched round-robin (overrides[i%len(overrides)] for
// instance i) mirroring EC2 Fleet's own round-robin fulfillment
// (services/ec2/fleet.go's launchFleetInstancesLocked/growFleetLocked) so the
// launched fleet's instance-type mix matches the overrides list instead of
// pinning every instance to the first override.
func (b *InMemoryBackend) launchInEC2(
	g *AutoScalingGroup, az string, n int, instanceType string,
) ([]Instance, bool) {
	if b.ec2Launcher == nil {
		return nil, false
	}

	specs, ok := b.launchSpecsForGroup(g, az)
	if !ok {
		return nil, false
	}

	ctx := context.Background()

	instances, allIDs, ok := b.launchRoundRobin(ctx, specs, n, az, g, instanceType)
	if !ok {
		if len(allIDs) > 0 {
			if err := b.ec2Launcher.TerminateInstances(ctx, allIDs); err != nil {
				logger.Load(ctx).ErrorContext(ctx,
					"autoscaling: EC2 rollback-terminate failed after partial launch",
					"error", err, "group", g.AutoScalingGroupName, "instanceIDs", allIDs)
			}
		}

		return nil, false
	}

	b.registerELBTargets(allIDs, g.TargetGroupARNs)
	b.registerELBInstances(allIDs, g.LoadBalancerNames)

	return instances, true
}

// launchRoundRobin launches n instances across specs round-robin (spec index
// i%len(specs) for instance i), batching the LaunchInstances call per spec.
// Reports ok=false on the first launcher error, in which case allIDs holds
// whatever was launched before the failure so the caller can roll it back.
func (b *InMemoryBackend) launchRoundRobin(
	ctx context.Context, specs []InstanceLaunchSpec, n int, az string, g *AutoScalingGroup, fallbackType string,
) ([]Instance, []string, bool) {
	var instances []Instance

	var allIDs []string

	for i, count := range distributeRoundRobin(n, len(specs)) {
		if count == 0 {
			continue
		}

		spec := specs[i]

		ids, err := b.ec2Launcher.LaunchInstances(ctx, spec, count)
		allIDs = append(allIDs, ids...)

		if err != nil {
			logger.Load(ctx).ErrorContext(ctx,
				"autoscaling: EC2 launch failed, falling back to synthetic instances",
				"error", err, "group", g.AutoScalingGroupName)

			return nil, allIDs, false
		}

		specType := spec.InstanceType
		if specType == "" {
			specType = fallbackType
		}

		instances = append(instances, instancesFromIDs(ids, az, g.LaunchConfigurationName, specType)...)
	}

	return instances, allIDs, true
}

// distributeRoundRobin splits n units round-robin across k buckets --
// bucket[i%k] for unit i -- returning each bucket's count. Matches EC2
// Fleet's overrides[i%len(overrides)] fulfillment loop (fleet.go).
func distributeRoundRobin(n, k int) []int {
	counts := make([]int, k)
	for i := range n {
		counts[i%k]++
	}

	return counts
}

// adjustInstances adjusts g's existing instance slice to match the new desired
// count, adding (real or fabricated — see makeInstances) or removing
// instances from the end while preserving existing instance IDs. Must be
// called with b.mu held (write lock).
func (b *InMemoryBackend) adjustInstances(g *AutoScalingGroup, existing []Instance, desired int32) []Instance {
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

	return append(existing, b.makeInstances(g, delta)...)
}

// terminateInEC2 best-effort terminates ids in the wired EC2 backend so EC2
// DescribeInstances stops listing instances this ASG has removed. No-op when
// no EC2Launcher is configured (the historical, EC2-less behavior). Errors
// are logged, not propagated: by the time this runs the instance has already
// been removed from the group's own bookkeeping (mirroring how AWS itself
// terminates the backing EC2 instance asynchronously, after the ASG-side
// state change), so there is nothing left to roll back. Must be called with
// b.mu held (write lock) — matches every existing call site.
func (b *InMemoryBackend) terminateInEC2(ids []string) {
	if b.ec2Launcher == nil || len(ids) == 0 {
		return
	}

	if err := b.ec2Launcher.TerminateInstances(context.Background(), ids); err != nil {
		logger.Load(context.Background()).Error(
			"autoscaling: EC2 terminate failed", "error", err, "instanceIDs", ids)
	}
}

// launchSpecsForGroup derives the InstanceLaunchSpec(s) to round-robin launch
// instances from for g: a single spec from its LaunchConfiguration or plain
// LaunchTemplate, or one spec per MixedInstancesPolicy.LaunchTemplate.Overrides
// entry when overrides are present (each override may set its own
// LaunchTemplateSpecification and/or InstanceType -- types.LaunchTemplateOverride,
// aws-sdk-go-v2/service/autoscaling/types/types.go, mirrored by this package's
// own LaunchTemplateOverride in models.go). Reports ok=false when g cannot be
// resolved to at least one usable ImageId, in which case the caller falls back
// to fabricating instances.
func (b *InMemoryBackend) launchSpecsForGroup(g *AutoScalingGroup, az string) ([]InstanceLaunchSpec, bool) {
	if g.LaunchConfigurationName != "" {
		lc, ok := b.launchConfigurations.Get(g.LaunchConfigurationName)
		if !ok || lc.ImageID == "" {
			return nil, false
		}

		return []InstanceLaunchSpec{{
			ImageID:          lc.ImageID,
			InstanceType:     lc.InstanceType,
			SubnetID:         firstSubnetID(g.VPCZoneIdentifier),
			AvailabilityZone: az,
			KeyName:          lc.KeyName,
			SecurityGroups:   lc.SecurityGroups,
			Tags:             launchTagsForGroup(g),
		}}, true
	}

	if b.ec2Launcher == nil {
		return nil, false
	}

	if g.MixedInstancesPolicy != nil && len(g.MixedInstancesPolicy.LaunchTemplate.Overrides) > 0 {
		return b.launchSpecsForOverrides(g, az)
	}

	ltSpec := extractLaunchTemplateSpec(g)
	if ltSpec == nil || (ltSpec.LaunchTemplateID == "" && ltSpec.LaunchTemplateName == "") {
		return nil, false
	}

	spec, ok := b.resolveLaunchTemplateSpec(g, az, ltSpec, "")
	if !ok {
		return nil, false
	}

	return []InstanceLaunchSpec{spec}, true
}

// launchSpecsForOverrides resolves one InstanceLaunchSpec per
// MixedInstancesPolicy.LaunchTemplate.Overrides entry: each override's own
// LaunchTemplateSpecification when it sets one, otherwise the policy's base
// LaunchTemplateSpecification, with the override's InstanceType taking
// precedence over the resolved template's when set. Skips overrides that fail
// to resolve; reports ok=false only when none resolve at all.
func (b *InMemoryBackend) launchSpecsForOverrides(g *AutoScalingGroup, az string) ([]InstanceLaunchSpec, bool) {
	baseSpec := g.MixedInstancesPolicy.LaunchTemplate.LaunchTemplateSpecification

	specs := make([]InstanceLaunchSpec, 0, len(g.MixedInstancesPolicy.LaunchTemplate.Overrides))

	for _, ov := range g.MixedInstancesPolicy.LaunchTemplate.Overrides {
		ltSpec := &baseSpec
		if ov.LaunchTemplateSpecification != nil {
			ltSpec = ov.LaunchTemplateSpecification
		}

		spec, ok := b.resolveLaunchTemplateSpec(g, az, ltSpec, ov.InstanceType)
		if !ok {
			continue
		}

		specs = append(specs, spec)
	}

	if len(specs) == 0 {
		return nil, false
	}

	return specs, true
}

// resolveLaunchTemplateSpec resolves one LaunchTemplateSpecification (id/name
// + version) against the wired EC2Launcher, applying instanceTypeOverride in
// place of the resolved template's own InstanceType when non-empty.
func (b *InMemoryBackend) resolveLaunchTemplateSpec(
	g *AutoScalingGroup, az string, ltSpec *LaunchTemplateSpecification, instanceTypeOverride string,
) (InstanceLaunchSpec, bool) {
	if ltSpec == nil || (ltSpec.LaunchTemplateID == "" && ltSpec.LaunchTemplateName == "") {
		return InstanceLaunchSpec{}, false
	}

	imageID, instanceType, err := b.ec2Launcher.ResolveLaunchTemplate(
		context.Background(),
		ltSpec.LaunchTemplateID,
		ltSpec.LaunchTemplateName,
		ltSpec.Version,
	)
	if err != nil || imageID == "" {
		return InstanceLaunchSpec{}, false
	}

	if instanceTypeOverride != "" {
		instanceType = instanceTypeOverride
	}

	return InstanceLaunchSpec{
		ImageID:          imageID,
		InstanceType:     instanceType,
		SubnetID:         firstSubnetID(g.VPCZoneIdentifier),
		AvailabilityZone: az,
		Tags:             launchTagsForGroup(g),
	}, true
}

func extractLaunchTemplateSpec(g *AutoScalingGroup) *LaunchTemplateSpecification {
	if g.LaunchTemplate != nil {
		return g.LaunchTemplate
	}
	if g.MixedInstancesPolicy != nil {
		return &g.MixedInstancesPolicy.LaunchTemplate.LaunchTemplateSpecification
	}

	return nil
}

// firstSubnetID returns the first subnet ID from a VPCZoneIdentifier
// (AWS represents this as a comma-separated list), or "" if unset.
func firstSubnetID(vpcZoneIdentifier string) string {
	if vpcZoneIdentifier == "" {
		return ""
	}

	first, _, _ := strings.Cut(vpcZoneIdentifier, ",")

	return strings.TrimSpace(first)
}

// launchTagsForGroup builds the tag set applied to instances g launches:
// every group tag with PropagateAtLaunch set, plus the synthetic
// "aws:autoscaling:groupName" tag AWS itself attaches to ASG-managed instances.
func launchTagsForGroup(g *AutoScalingGroup) map[string]string {
	tags := make(map[string]string, len(g.Tags)+1)
	tags["aws:autoscaling:groupName"] = g.AutoScalingGroupName

	for _, t := range g.Tags {
		if t.PropagateAtLaunch {
			tags[t.Key] = t.Value
		}
	}

	return tags
}

// fabricateInstances creates n Instance records with synthetic, EC2-less
// instance IDs. This is the pre-EC2Launcher behavior, preserved verbatim as
// the fallback used when no launcher is configured or a group's launch
// configuration can't be resolved to a usable EC2 launch spec.
func fabricateInstances(n int, az, launchConfigName, instanceType string) []Instance {
	// No capacity hint — append grows naturally; any user-derived value in
	// the capacity position would trigger CodeQL go/slice-memory-allocation-excessive-size.
	//nolint:prealloc,nolintlint // satisfies CodeQL by removing tainted capacity hint
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

// instancesFromIDs creates one Instance record per real EC2 instance ID
// returned by an EC2Launcher, in the same InService/Healthy shape
// fabricateInstances produces (any lifecycle-hook gating is applied afterward
// by the caller, exactly as it is for fabricated instances).
func instancesFromIDs(ids []string, az, launchConfigName, instanceType string) []Instance {
	now := time.Now()
	instances := make([]Instance, 0, len(ids))

	for _, id := range ids {
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
