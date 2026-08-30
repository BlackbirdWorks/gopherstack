package autoscaling

import "time"

// StorageBackend is the interface for the Autoscaling in-memory store.
type StorageBackend interface {
	CreateAutoScalingGroup(input CreateAutoScalingGroupInput) (*AutoScalingGroup, error)
	DescribeAutoScalingGroups(names []string, filters []TagFilter) ([]AutoScalingGroup, error)
	UpdateAutoScalingGroup(input UpdateAutoScalingGroupInput) (*AutoScalingGroup, error)
	DeleteAutoScalingGroup(name string, forceDelete bool) error

	CreateLaunchConfiguration(input CreateLaunchConfigurationInput) (*LaunchConfiguration, error)
	DescribeLaunchConfigurations(names []string) ([]LaunchConfiguration, error)
	DeleteLaunchConfiguration(name string) error

	DescribeScalingActivities(groupName string, statuses []string) ([]ScalingActivity, error)

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
	DescribeScheduledActions(
		groupName string, actionNames []string, startTime, endTime time.Time,
	) ([]ScheduledAction, error)
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
	DescribeTrafficSources(groupName, trafficSourceType string) ([]TrafficSourceState, error)

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
	DescribePolicies(groupName string, policyNames, policyTypes []string) ([]ScalingPolicy, error)

	// Scheduled actions (single)
	PutScheduledUpdateGroupAction(groupName string, action ScheduledUpdateGroupAction) error
	DeleteScheduledAction(groupName, scheduledActionName string) error

	// Warm pool
	PutWarmPool(input WarmPoolInput) error
	DeleteWarmPool(groupName string) error
	DescribeWarmPool(groupName string) (*WarmPool, error)
}
