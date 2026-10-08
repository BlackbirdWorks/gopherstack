package autoscaling

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
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
	// statusPending is the status for an instance refresh that has not yet started.
	statusPending = "Pending"
	// Instance refresh terminal/transitional statuses (autoscaling@v1.70.4
	// types/enums.go:289-303, InstanceRefreshStatus).
	statusSuccessful         = "Successful"
	statusCancelling         = "Cancelling"
	statusCancelled          = "Cancelled"
	statusRollbackInProgress = "RollbackInProgress"
	statusRollbackSuccessful = "RollbackSuccessful"
	// instanceRefreshTransitionDelay is the simulated async delay before an
	// instance refresh (or its cancel/rollback) reaches a terminal status,
	// matching the time.AfterFunc pattern used by lifecycle hooks and eks's
	// clusterTransitionDelay.
	instanceRefreshTransitionDelay = 100 * time.Millisecond
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

// InMemoryBackend implements StorageBackend using in-memory maps.
type InMemoryBackend struct {
	ec2Launcher             EC2Launcher
	instanceTypeResolver    InstanceTypeResolver
	elbv2Registrar          ELBv2TargetRegistrar
	elbRegistrar            ELBInstanceRegistrar
	ec2Lookup               EC2Lookup
	lifecycleHooks          *store.Table[LifecycleHook]
	notificationConfigs     map[string][]*NotificationConfiguration
	activities              map[string][]ScalingActivity
	mu                      *lockmetrics.RWMutex
	scheduledActions        *store.Table[ScheduledAction]
	scheduledActionsByGroup *store.Index[ScheduledAction]
	instanceRefreshes       map[string][]*InstanceRefresh
	groups                  *store.Table[AutoScalingGroup]
	lifecycleHooksByGroup   *store.Index[LifecycleHook]
	scalingPolicies         *store.Table[ScalingPolicy]
	scalingPoliciesByGroup  *store.Index[ScalingPolicy]
	launchConfigurations    *store.Table[LaunchConfiguration]
	warmPools               *store.Table[WarmPool]
	pendingHookTokens       *store.Table[pendingHookAction]
	pendingRefreshActions   *store.Table[pendingRefreshAction]
	registry                *store.Registry
	instanceIndex           map[string]string
	accountID               string
	region                  string
	deletedActivities       []ScalingActivity
	nextHookSeq             int64
}

// NewInMemoryBackend creates a new InMemoryBackend for the default account and region.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithConfig(config.DefaultAccountID, config.DefaultRegion)
}

// NewInMemoryBackendWithConfig creates an InMemoryBackend that builds ARNs for accountID and region.
func NewInMemoryBackendWithConfig(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		accountID:           accountID,
		region:              region,
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

// Close stops any in-flight lifecycle-hook and instance-refresh timers so their goroutines do
// not outlive the backend. It is safe to call multiple times.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")
	defer b.mu.Unlock()

	b.pendingHookTokens.Range(func(action *pendingHookAction) bool {
		action.timer.Stop()

		return true
	})
	b.pendingHookTokens.Reset()

	b.pendingRefreshActions.Range(func(action *pendingRefreshAction) bool {
		action.timer.Stop()

		return true
	})
	b.pendingRefreshActions.Reset()
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
			b.cleanupRefreshTimers(name)
			b.groups.Delete(name)
			b.retireActivities(name)
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

// defaultAvailabilityZone is the fallback AZ used when none is specified.
func (b *InMemoryBackend) defaultAvailabilityZone() string { return b.region + "a" }

// crossServiceContext carries the group's region to the EC2 and ELB backends it calls.
func (b *InMemoryBackend) crossServiceContext() context.Context {
	return awsmeta.Set(context.Background(), &awsmeta.Metadata{Region: b.region, Account: b.accountID})
}
