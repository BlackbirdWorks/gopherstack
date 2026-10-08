package ecs

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	deploymentStrategyRolling   = "ROLLING"
	deploymentStrategyBlueGreen = "BLUE_GREEN"
	deploymentStrategyLinear    = "LINEAR"
	deploymentStrategyCanary    = "CANARY"

	hookTargetLambda = "AWS_LAMBDA"
	hookTargetPause  = "PAUSE"

	hookStatusAwaiting  = "AWAITING_ACTION"
	hookStatusSucceeded = "SUCCEEDED"
	hookStatusTimedOut  = "TIMED_OUT"

	serviceDeploymentStatusInProgress = "IN_PROGRESS"
	serviceDeploymentStatusSuccessful = "SUCCESSFUL"
	serviceDeploymentRollbackFailed   = "ROLLBACK_FAILED"

	alarmsStatusDisabled           = "DISABLED"
	alarmsStatusMonitoring         = "MONITORING"
	alarmsStatusMonitoringComplete = "MONITORING_COMPLETE"
	alarmsStatusTriggered          = "TRIGGERED"

	stageReconcileService       = "RECONCILE_SERVICE"
	stageScaleUp                = "SCALE_UP"
	stageTestTrafficShift       = "TEST_TRAFFIC_SHIFT"
	stagePreProductionShift     = "PRE_PRODUCTION_TRAFFIC_SHIFT"
	stageProductionTrafficShift = "PRODUCTION_TRAFFIC_SHIFT"
	stageBakeTime               = "BAKE_TIME"
	stageCleanUp                = "CLEAN_UP"

	defaultHookTimeoutMinutes = 1440
	maxBakeTimeMinutes        = 1440
	minLinearStepPercent      = 3.0
	minCanaryPercent          = 0.1
	maxTrafficPercent         = 100.0
	trafficPercentGranularity = 10
)

// lifecycleStageOrder is the internal stage sequence; PRE_PRODUCTION_TRAFFIC_SHIFT is hook-only
// (types.ServiceDeploymentLifecycleStage has no such value) and is reported as PRODUCTION_TRAFFIC_SHIFT.
func lifecycleStageOrder() []string {
	return []string{
		stageReconcileService, "PRE_SCALE_UP", stageScaleUp, "POST_SCALE_UP",
		stageTestTrafficShift, "POST_TEST_TRAFFIC_SHIFT", stagePreProductionShift,
		stageProductionTrafficShift, "POST_PRODUCTION_TRAFFIC_SHIFT", stageBakeTime, stageCleanUp,
	}
}

func hookableStages() []string {
	return []string{
		stageReconcileService, "PRE_SCALE_UP", "POST_SCALE_UP", stageTestTrafficShift, "POST_TEST_TRAFFIC_SHIFT",
		stagePreProductionShift, stageProductionTrafficShift, "POST_PRODUCTION_TRAFFIC_SHIFT",
	}
}

// AlarmStateProvider reports which of the named CloudWatch alarms are in ALARM state.
type AlarmStateProvider interface {
	TriggeredAlarms(alarmNames []string) []string
}

// SetAlarmStateProvider wires the alarm source used by deployment alarm monitoring.
func (b *InMemoryBackend) SetAlarmStateProvider(p AlarmStateProvider) {
	b.mu.Lock("SetAlarmStateProvider")
	defer b.mu.Unlock()
	b.alarmStates = p
}

func lifecycleStrategy(dc *DeploymentConfiguration) bool {
	if dc == nil {
		return false
	}

	switch dc.Strategy {
	case deploymentStrategyBlueGreen, deploymentStrategyLinear, deploymentStrategyCanary:
		return true
	}

	return false
}

func validateDeploymentConfiguration(dc *DeploymentConfiguration) error {
	if dc == nil {
		return nil
	}

	switch dc.Strategy {
	case "", deploymentStrategyRolling, deploymentStrategyBlueGreen, deploymentStrategyLinear, deploymentStrategyCanary:
	default:
		return fmt.Errorf("%w: invalid deployment strategy %q", ErrInvalidParameter, dc.Strategy)
	}

	if dc.BakeTimeInMinutes != nil && (*dc.BakeTimeInMinutes < 0 || *dc.BakeTimeInMinutes > maxBakeTimeMinutes) {
		return fmt.Errorf("%w: bakeTimeInMinutes must be between 0 and %d", ErrInvalidParameter, maxBakeTimeMinutes)
	}

	if err := validateTrafficShiftConfigs(dc); err != nil {
		return err
	}

	for i := range dc.LifecycleHooks {
		if err := validateLifecycleHook(&dc.LifecycleHooks[i]); err != nil {
			return err
		}
	}

	return nil
}

func validPercentStep(p, lo float64) bool {
	tenths := p * trafficPercentGranularity

	return p >= lo && p <= maxTrafficPercent && math.Abs(tenths-math.Round(tenths)) < 1e-6
}

func validateTrafficShiftConfigs(dc *DeploymentConfiguration) error {
	if c := dc.CanaryConfiguration; c != nil {
		if dc.Strategy != deploymentStrategyCanary {
			return fmt.Errorf("%w: canaryConfiguration requires the CANARY strategy", ErrInvalidParameter)
		}

		if c.CanaryPercent != nil && !validPercentStep(*c.CanaryPercent, minCanaryPercent) {
			return fmt.Errorf("%w: canaryPercent must be a multiple of 0.1 from 0.1 to 100.0", ErrInvalidParameter)
		}

		if c.CanaryBakeTimeInMinutes != nil &&
			(*c.CanaryBakeTimeInMinutes < 0 || *c.CanaryBakeTimeInMinutes > maxBakeTimeMinutes) {
			return fmt.Errorf(
				"%w: canaryBakeTimeInMinutes must be between 0 and %d",
				ErrInvalidParameter,
				maxBakeTimeMinutes,
			)
		}
	}

	if l := dc.LinearConfiguration; l != nil {
		if dc.Strategy != deploymentStrategyLinear {
			return fmt.Errorf("%w: linearConfiguration requires the LINEAR strategy", ErrInvalidParameter)
		}

		if l.StepPercent != nil && !validPercentStep(*l.StepPercent, minLinearStepPercent) {
			return fmt.Errorf("%w: stepPercent must be a multiple of 0.1 from 3.0 to 100.0", ErrInvalidParameter)
		}

		if l.StepBakeTimeInMinutes != nil &&
			(*l.StepBakeTimeInMinutes < 0 || *l.StepBakeTimeInMinutes > maxBakeTimeMinutes) {
			return fmt.Errorf(
				"%w: stepBakeTimeInMinutes must be between 0 and %d",
				ErrInvalidParameter,
				maxBakeTimeMinutes,
			)
		}
	}

	return nil
}

func validateLifecycleHook(h *DeploymentLifecycleHook) error {
	if len(h.LifecycleStages) == 0 {
		return fmt.Errorf("%w: lifecycleStages is required for a lifecycle hook", ErrInvalidParameter)
	}

	for _, st := range h.LifecycleStages {
		if !slices.Contains(hookableStages(), st) {
			return fmt.Errorf("%w: invalid lifecycle stage %q", ErrInvalidParameter, st)
		}
	}

	if err := validateHookTarget(h); err != nil {
		return err
	}

	return validateHookTimeout(h.TimeoutConfiguration)
}

func validateHookTarget(h *DeploymentLifecycleHook) error {
	switch h.TargetType {
	case "", hookTargetLambda:
		if h.HookTargetArn == "" {
			return fmt.Errorf("%w: hookTargetArn is required for an AWS_LAMBDA lifecycle hook", ErrInvalidParameter)
		}
	case hookTargetPause:
		for _, st := range h.LifecycleStages {
			if st == stageTestTrafficShift || st == stageProductionTrafficShift {
				return fmt.Errorf("%w: PAUSE hooks cannot be configured at %s", ErrInvalidParameter, st)
			}
		}
	default:
		return fmt.Errorf("%w: invalid lifecycle hook targetType %q", ErrInvalidParameter, h.TargetType)
	}

	return nil
}

func validateHookTimeout(t *DeploymentLifecycleHookTimeout) error {
	if t == nil {
		return nil
	}

	switch t.Action {
	case "", deploymentLifecycleActionContinue, deploymentLifecycleActionRollback:
	default:
		return fmt.Errorf("%w: invalid timeout action %q", ErrInvalidParameter, t.Action)
	}

	if t.TimeoutInMinutes != nil && *t.TimeoutInMinutes <= 0 {
		return fmt.Errorf("%w: timeoutInMinutes must be positive", ErrInvalidParameter)
	}

	return nil
}

func reportedLifecycleStage(internal string) string {
	if internal == stagePreProductionShift {
		return stageProductionTrafficShift
	}

	return internal
}

func isPauseHook(h *DeploymentLifecycleHook) bool { return h.TargetType == hookTargetPause }

func hookTimeout(h *DeploymentLifecycleHook) (time.Duration, string) {
	minutes, action := defaultHookTimeoutMinutes, deploymentLifecycleActionRollback

	if t := h.TimeoutConfiguration; t != nil {
		if t.TimeoutInMinutes != nil {
			minutes = *t.TimeoutInMinutes
		}

		if t.Action != "" {
			action = t.Action
		}
	}

	return time.Duration(minutes) * time.Minute, action
}

func bakeDuration(dc *DeploymentConfiguration) time.Duration {
	if dc == nil || dc.BakeTimeInMinutes == nil {
		return 0
	}

	return time.Duration(*dc.BakeTimeInMinutes) * time.Minute
}

// initDeploymentLifecycleLocked starts the lifecycle of a freshly created ServiceDeployment.
func (b *InMemoryBackend) initDeploymentLifecycleLocked(svc *Service, sd *ServiceDeployment, now time.Time) {
	active := 0

	for i := range svc.Deployments {
		d := svc.Deployments[i]
		if d.Status == statusActive && d.RolloutState != deploymentRolloutStateFailed {
			active++
		}
	}

	if active <= 1 {
		sd.LifecycleCursor = 1
	}

	sd.LifecycleStage = reportedLifecycleStage(lifecycleStageOrder()[sd.LifecycleCursor])

	if al := svc.DeploymentConfiguration.Alarms; al != nil {
		sd.Alarms = &ServiceDeploymentAlarms{Status: alarmsStatusDisabled, AlarmNames: slices.Clone(al.AlarmNames)}
		if al.Enable {
			sd.Alarms.Status = alarmsStatusMonitoring
		}
	}

	b.advanceDeploymentLifecycleLocked(svc, sd, now)
}

// advanceAllDeploymentLifecyclesLocked moves every in-flight lifecycle forward as far as its state allows.
func (b *InMemoryBackend) advanceAllDeploymentLifecyclesLocked(now time.Time) {
	for _, sd := range b.serviceDeployments.All() {
		if sd.LifecycleStage == "" || isTerminalServiceDeploymentStatus(sd.Status) {
			continue
		}

		if svc := b.serviceByArnLocked(sd.ServiceArn); svc != nil {
			b.advanceDeploymentLifecycleLocked(svc, sd, now)
		}
	}
}

func (b *InMemoryBackend) advanceDeploymentLifecycleLocked(svc *Service, sd *ServiceDeployment, now time.Time) {
	if b.checkAlarmsLocked(svc, sd, now) {
		return
	}

	for !isTerminalServiceDeploymentStatus(sd.Status) {
		stage := lifecycleStageOrder()[sd.LifecycleCursor]
		sd.LifecycleStage = reportedLifecycleStage(stage)

		if b.resolveHooksLocked(svc, sd, stage, now) {
			return
		}

		if !b.stageCompleteLocked(svc, sd, stage, now) {
			return
		}

		sd.LifecycleCursor++
		sd.LifecycleEntered = false

		if sd.LifecycleCursor >= len(lifecycleStageOrder()) {
			b.completeDeploymentLifecycleLocked(svc, sd, now)

			return
		}
	}
}

// resolveHooksLocked fires the stage's PAUSE hooks on entry and reports whether the
// deployment is held (a hook is awaiting action) or was just rolled back.
func (b *InMemoryBackend) resolveHooksLocked(svc *Service, sd *ServiceDeployment, stage string, now time.Time) bool {
	if !sd.LifecycleEntered {
		sd.LifecycleEntered = true

		for i := range svc.DeploymentConfiguration.LifecycleHooks {
			h := &svc.DeploymentConfiguration.LifecycleHooks[i]
			if !isPauseHook(h) || !slices.Contains(h.LifecycleStages, stage) {
				continue
			}

			timeout, action := hookTimeout(h)
			expires := now.Add(timeout)
			sd.LifecycleHookDetails = append(sd.LifecycleHookDetails, LifecycleHookDetail{
				HookID: uuid.NewString(), Status: hookStatusAwaiting, TargetType: hookTargetPause,
				TimeoutAction: action, ExpiresAt: &expires, Stage: stage,
			})
		}
	}

	for i := range sd.LifecycleHookDetails {
		d := &sd.LifecycleHookDetails[i]
		if d.Stage != stage || d.Status != hookStatusAwaiting {
			continue
		}

		if d.ExpiresAt == nil || now.Before(*d.ExpiresAt) {
			return true
		}

		d.Status = hookStatusTimedOut

		if d.TimeoutAction == deploymentLifecycleActionRollback {
			b.failDeploymentLifecycleLocked(svc, sd, "Lifecycle hook timed out.", now)

			return true
		}
	}

	return false
}

func (b *InMemoryBackend) stageCompleteLocked(svc *Service, sd *ServiceDeployment, stage string, now time.Time) bool {
	switch stage {
	case stageScaleUp:
		return b.targetScaledLocked(svc, sd)
	case stageBakeTime:
		if sd.BakeStartedAt == nil {
			sd.BakeStartedAt = &now
		}

		return !now.Before(sd.BakeStartedAt.Add(bakeDuration(svc.DeploymentConfiguration)))
	}

	return true
}

func (b *InMemoryBackend) targetScaledLocked(svc *Service, sd *ServiceDeployment) bool {
	enriched := b.enrichService(svc, clusterKey(svc.ClusterArn))

	for i := range enriched.Deployments {
		d := enriched.Deployments[i]
		if serviceDeploymentArnFor(svc, d.ID) == sd.ServiceDeploymentArn {
			return d.DesiredCount <= 0 || d.RunningCount >= d.DesiredCount
		}
	}

	return true
}

func (b *InMemoryBackend) completeDeploymentLifecycleLocked(svc *Service, sd *ServiceDeployment, now time.Time) {
	sd.LifecycleStage = stageCleanUp
	sd.Status = serviceDeploymentStatusSuccessful
	sd.UpdatedAt, sd.FinishedAt = &now, &now

	if sd.Alarms != nil && sd.Alarms.Status == alarmsStatusMonitoring {
		sd.Alarms.Status = alarmsStatusMonitoringComplete
	}

	for i := range svc.Deployments {
		d := &svc.Deployments[i]
		if serviceDeploymentArnFor(svc, d.ID) == sd.ServiceDeploymentArn &&
			d.RolloutState == deploymentRolloutStateInProgress {
			d.RolloutState = deploymentRolloutStateCompleted
			d.RolloutStateReason = "ECS deployment ecs-svc completed."
		}
	}
}

// failDeploymentLifecycleLocked rolls the deployment back, or marks it ROLLBACK_FAILED when no earlier revision exists.
func (b *InMemoryBackend) failDeploymentLifecycleLocked(
	svc *Service,
	sd *ServiceDeployment,
	reason string,
	now time.Time,
) {
	if err := b.rollbackServiceDeploymentLocked(sd, now); err == nil {
		sd.StatusReason = reason

		return
	}

	sd.Status, sd.StatusReason = serviceDeploymentRollbackFailed, reason
	sd.UpdatedAt, sd.FinishedAt = &now, &now

	for i := range svc.Deployments {
		if serviceDeploymentArnFor(svc, svc.Deployments[i].ID) == sd.ServiceDeploymentArn {
			svc.Deployments[i].RolloutState = deploymentRolloutStateFailed
			svc.Deployments[i].RolloutStateReason = reason
		}
	}
}

// checkAlarmsLocked evaluates deployment alarms and reports whether the deployment failed.
func (b *InMemoryBackend) checkAlarmsLocked(svc *Service, sd *ServiceDeployment, now time.Time) bool {
	al := svc.DeploymentConfiguration.Alarms
	if al == nil || !al.Enable || b.alarmStates == nil || sd.Alarms == nil {
		return false
	}

	triggered := b.alarmStates.TriggeredAlarms(al.AlarmNames)
	if len(triggered) == 0 {
		return false
	}

	sd.Alarms.Status = alarmsStatusTriggered
	sd.Alarms.TriggeredAlarmNames = slices.Clone(triggered)
	reason := "CloudWatch alarms triggered: " + strings.Join(triggered, ",")

	if al.Rollback {
		b.failDeploymentLifecycleLocked(svc, sd, reason, now)
		sd.Alarms.Status = alarmsStatusTriggered

		return true
	}

	sd.Status, sd.StatusReason = statusStopped, reason
	sd.UpdatedAt, sd.StoppedAt, sd.FinishedAt = &now, &now, &now

	for i := range svc.Deployments {
		if serviceDeploymentArnFor(svc, svc.Deployments[i].ID) == sd.ServiceDeploymentArn {
			svc.Deployments[i].RolloutState = deploymentRolloutStateFailed
			svc.Deployments[i].RolloutStateReason = reason
		}
	}

	return true
}

// carryLifecycle copies lifecycle state from the previous record onto a re-synced one.
func carryLifecycle(dst, prev *ServiceDeployment) {
	dst.LifecycleStage = prev.LifecycleStage
	dst.LifecycleHookDetails = prev.LifecycleHookDetails
	dst.Alarms = prev.Alarms
	dst.BakeStartedAt = prev.BakeStartedAt
	dst.LifecycleCursor = prev.LifecycleCursor
	dst.LifecycleEntered = prev.LifecycleEntered
	dst.Status, dst.StatusReason = prev.Status, prev.StatusReason
	dst.StartedAt, dst.CreatedAt = prev.StartedAt, prev.CreatedAt
	dst.UpdatedAt, dst.StoppedAt, dst.FinishedAt = prev.UpdatedAt, prev.StoppedAt, prev.FinishedAt
	dst.Rollback = prev.Rollback
}

// hookInProgress reports whether the service deployment is still held by lifecycle processing.
func lifecycleActive(sd *ServiceDeployment) bool {
	return sd != nil && sd.LifecycleStage != "" && !isTerminalServiceDeploymentStatus(sd.Status)
}

// continueLifecycleHookLocked applies a ContinueServiceDeployment action to a paused hook.
func (b *InMemoryBackend) continueLifecycleHookLocked(
	sd *ServiceDeployment,
	hookID, action string,
	now time.Time,
) error {
	idx := slices.IndexFunc(sd.LifecycleHookDetails, func(d LifecycleHookDetail) bool {
		return d.HookID == hookID && d.Status == hookStatusAwaiting
	})
	if idx < 0 || isTerminalServiceDeploymentStatus(sd.Status) {
		return fmt.Errorf("%w: no paused lifecycle hook %q found for service deployment %s",
			errNoLifecycleHook, hookID, sd.ServiceDeploymentArn)
	}

	svc := b.serviceByArnLocked(sd.ServiceArn)
	if svc == nil {
		return fmt.Errorf("%w: service %s", ErrServiceNotFound, sd.ServiceArn)
	}

	sd.LifecycleHookDetails[idx].Status = hookStatusSucceeded

	if action == deploymentLifecycleActionRollback {
		if err := b.rollbackServiceDeploymentLocked(sd, now); err != nil {
			sd.LifecycleHookDetails[idx].Status = hookStatusAwaiting

			return err
		}

		return nil
	}

	sd.UpdatedAt = &now
	b.advanceDeploymentLifecycleLocked(svc, sd, now)

	return nil
}

// lifecycleHoldsLocked reports whether d's ServiceDeployment is still inside its lifecycle.
func (b *InMemoryBackend) lifecycleHoldsLocked(svc *Service, d Deployment) bool {
	sd, ok := b.serviceDeployments.Get(serviceDeploymentArnFor(svc, d.ID))

	return ok && lifecycleActive(sd)
}

func validateServiceConfig(platformVersion string, dc *DeploymentConfiguration) error {
	if err := validatePlatformVersion(platformVersion); err != nil {
		return err
	}

	return validateDeploymentConfiguration(dc)
}
