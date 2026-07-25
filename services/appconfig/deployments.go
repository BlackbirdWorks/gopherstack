package appconfig

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Deployment progression is simulated on a compressed timescale: real AWS
// AppConfig deployments unfold over the strategy's configured
// DeploymentDurationInMinutes/FinalBakeTimeInMinutes (which can be hours),
// which is obviously impractical to actually wait out in an in-memory
// backend or its test suite. Instead, every in-flight deployment advances
// one growth step every deploymentStepDelay of *wall-clock* time via
// reconcileDeploymentsLocked, regardless of the strategy's real-world
// duration -- only the PRESENCE of a growth phase / bake phase (duration >
// 0 / bake time > 0) and the strategy's GrowthType/GrowthFactor shape the
// simulated PercentageComplete curve, not the actual configured minutes.
// minEffectiveGrowthFactor clamps how small a per-step increment can be so
// that even a GrowthFactor of 1 completes in a bounded number of steps
// (100/20 = 5) rather than 100, keeping worst-case test runtime small. This
// mirrors the compressed-time pattern already used by services/rds
// (instanceTransitionDelay/reconcilerDivisor) and services/acm
// (autoValidateDelayMS) for the same reason: real AWS timings are
// impractical to emulate literally.
const (
	deploymentReconcileInterval = 4 * time.Millisecond
	deploymentStepDelay         = 8 * time.Millisecond
	deploymentBakeDelay         = 8 * time.Millisecond
	minEffectiveGrowthFactor    = 20
	fullPercentage              = 100.0
	exponentialGrowthBase       = 2
)

// Deployment.State values, matching real AWS AppConfig's DeploymentState enum.
const (
	deploymentStateDeploying  = "DEPLOYING"
	deploymentStateBaking     = "BAKING"
	deploymentStateValidating = "VALIDATING"
	deploymentStateComplete   = "COMPLETE"
	deploymentStateRolledBack = "ROLLED_BACK"
	deploymentStateReverted   = "REVERTED"
)

// deploymentTimer tracks when an in-flight deployment's next progression
// step is due. See the deploymentTimers doc comment on InMemoryBackend
// (store.go) for why this is not persisted.
type deploymentTimer struct {
	nextAt time.Time
	step   int32
}

// StartDeployment starts a deployment.
func (b *InMemoryBackend) StartDeployment(
	applicationID, environmentID, configProfileID, strategyID, configVersion, description string,
) (*Deployment, error) {
	b.mu.Lock("StartDeployment")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationID) {
		return nil, fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
	}

	env, ok := b.environments.Get(environmentID)
	if !ok || env.ApplicationID != applicationID {
		return nil, fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	profile, ok := b.configProfiles.Get(configProfileID)
	if !ok || profile.ApplicationID != applicationID {
		return nil, fmt.Errorf(
			"%w: configuration profile %s",
			ErrConfigurationProfileNotFound,
			configProfileID,
		)
	}

	strategy, ok := b.deploymentStrategies.Get(strategyID)
	if !ok {
		return nil, fmt.Errorf(
			"%w: deployment strategy %s",
			ErrDeploymentStrategyNotFound,
			strategyID,
		)
	}

	// Real StartDeployment validates ConfigurationVersion against the
	// backing store for AppConfig-hosted profiles (version number or
	// label); for any other location this backend has no way to validate
	// against the external system, so it is accepted as-is, same as
	// before.
	var versionLabel string
	if profile.LocationURI == contentTypeHostedLocation {
		hcv, found := b.resolveHostedConfigVersion(applicationID, configProfileID, configVersion)
		if !found {
			return nil, fmt.Errorf(
				"%w: configuration version %s for profile %s",
				ErrHostedConfigVersionNotFound,
				configVersion,
				configProfileID,
			)
		}

		versionLabel = hcv.VersionLabel
	}

	if b.deploymentCounters[applicationID] == nil {
		b.deploymentCounters[applicationID] = make(map[string]int32)
	}

	b.deploymentCounters[applicationID][environmentID]++
	deploymentNumber := b.deploymentCounters[applicationID][environmentID]

	now := time.Now()
	deployment := &Deployment{
		ApplicationID:               applicationID,
		EnvironmentID:               environmentID,
		ConfigurationProfileID:      configProfileID,
		DeploymentStrategyID:        strategyID,
		ConfigurationVersion:        configVersion,
		Description:                 description,
		ConfigurationName:           profile.Name,
		ConfigurationLocationURI:    profile.LocationURI,
		GrowthType:                  strategy.GrowthType,
		GrowthFactor:                strategy.GrowthFactor,
		VersionLabel:                versionLabel,
		DeploymentDurationInMinutes: strategy.DeploymentDurationInMinutes,
		FinalBakeTimeInMinutes:      strategy.FinalBakeTimeInMinutes,
		TriggeredBy:                 "USER",
		DeploymentNumber:            deploymentNumber,
		StartedAt:                   now,
		AppliedExtensions:           b.appliedExtensionsLocked(applicationID, environmentID, configProfileID),
	}
	appendDeploymentEvent(deployment, "DEPLOYMENT_STARTED", "USER", "Deployment started", now)

	key := deploymentKey(applicationID, environmentID, deploymentNumber)

	switch {
	case strategy.DeploymentDurationInMinutes <= 0 && strategy.FinalBakeTimeInMinutes <= 0:
		// Zero-duration, zero-bake strategies (e.g. AppConfig.AllAtOnce)
		// complete synchronously -- there is no growth curve to simulate.
		b.finalizeDeploymentLocked(deployment, now)
	case strategy.DeploymentDurationInMinutes <= 0:
		deployment.State = deploymentStateBaking
		deployment.PercentageComplete = fullPercentage
		appendDeploymentEvent(deployment, "BAKE_TIME_STARTED", "APPCONFIG", "Bake time started", now)
		b.deploymentTimers[key] = &deploymentTimer{nextAt: now.Add(deploymentBakeDelay)}
		b.scheduleDeploymentReconcilerLocked()
	default:
		deployment.State = deploymentStateDeploying
		deployment.PercentageComplete = 0
		b.deploymentTimers[key] = &deploymentTimer{nextAt: now.Add(deploymentStepDelay)}
		b.scheduleDeploymentReconcilerLocked()
	}

	b.deployments.Put(deployment)
	cp := *deployment

	return &cp, nil
}

// appliedExtensionsLocked gathers the extensions currently associated with
// the application, environment, or configuration profile being deployed --
// matching real AWS's "extensions that were previously associated ... when
// StartDeployment was called" semantics. Must be called under lock.
func (b *InMemoryBackend) appliedExtensionsLocked(
	applicationID, environmentID, profileID string,
) []AppliedExtension {
	targets := map[string]bool{
		b.appconfigARN("application/" + applicationID):                                        true,
		b.appconfigARN("application/" + applicationID + "/environment/" + environmentID):      true,
		b.appconfigARN("application/" + applicationID + "/configurationprofile/" + profileID): true,
	}

	var out []AppliedExtension

	for _, assoc := range b.extensionAssociations.All() {
		if !targets[assoc.ResourceArn] {
			continue
		}

		out = append(out, AppliedExtension{
			ExtensionAssociationID: assoc.ID,
			ExtensionID:            extensionIDFromArn(assoc.ExtensionArn),
			Parameters:             assoc.Parameters,
			VersionNumber:          assoc.ExtensionVersionNumber,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ExtensionAssociationID < out[j].ExtensionAssociationID })

	return out
}

// extensionIDFromArn extracts the trailing "extension/{id}" segment's id
// from an extension ARN built by appconfigARN.
func extensionIDFromArn(extensionArn string) string {
	i := strings.LastIndex(extensionArn, "/")
	if i < 0 {
		return ""
	}

	return extensionArn[i+1:]
}

// appendDeploymentEvent prepends a new event to the deployment's EventLog,
// matching real AWS's "the most recent events are displayed first" order.
func appendDeploymentEvent(d *Deployment, eventType, triggeredBy, description string, at time.Time) {
	event := DeploymentEvent{
		EventType:   eventType,
		TriggeredBy: triggeredBy,
		Description: description,
		OccurredAt:  at,
	}
	d.EventLog = append([]DeploymentEvent{event}, d.EventLog...)
}

// finalizeDeploymentLocked transitions d to its COMPLETE terminal state and
// records the deployed configuration version so GetConfiguration /
// CurrentDeployedConfiguration serve it. Must be called under lock.
func (b *InMemoryBackend) finalizeDeploymentLocked(d *Deployment, at time.Time) {
	d.State = deploymentStateComplete
	d.PercentageComplete = fullPercentage
	d.CompletedAt = at
	appendDeploymentEvent(d, "DEPLOYMENT_COMPLETED", "APPCONFIG", "Deployment completed", at)

	b.deployedConfigs[appEnvProfileKey(d.ApplicationID, d.EnvironmentID, d.ConfigurationProfileID)] =
		d.ConfigurationVersion
}

// effectiveGrowthFactor clamps a strategy's configured GrowthFactor into
// the range used to drive the compressed-time growth simulation -- see the
// package-level doc comment on minEffectiveGrowthFactor.
func effectiveGrowthFactor(growthFactor float32) float32 {
	switch {
	case growthFactor < minEffectiveGrowthFactor:
		return minEffectiveGrowthFactor
	case growthFactor > fullPercentage:
		return fullPercentage
	default:
		return growthFactor
	}
}

// nextDeploymentPercentage computes the next PercentageComplete for a
// deployment mid-growth, per its strategy's GrowthType.
func nextDeploymentPercentage(growthType string, current float32, step int32, growthFactor float32) float32 {
	gf := effectiveGrowthFactor(growthFactor)

	var next float32
	if growthType == "EXPONENTIAL" {
		next = gf * float32(math.Pow(exponentialGrowthBase, float64(step)))
	} else {
		next = current + gf
	}

	if next > fullPercentage {
		next = fullPercentage
	}

	return next
}

// scheduleDeploymentReconcilerLocked starts the background progression
// goroutine if it is not already running. The goroutine is ephemeral: it
// exits on its own once every in-flight deployment has reached a terminal
// state (see reconcileDeploymentsLocked), so there is nothing to
// context-parent or drain on shutdown -- the same self-terminating pattern
// services/rds uses for its instance/cluster lifecycle reconciler.
func (b *InMemoryBackend) scheduleDeploymentReconcilerLocked() {
	if b.deploymentReconcilerAlive {
		return
	}

	b.deploymentReconcilerAlive = true

	go func() {
		defer func() {
			b.mu.Lock("deploymentReconcilerExit")
			b.deploymentReconcilerAlive = false
			b.mu.Unlock()
		}()

		ticker := time.NewTicker(deploymentReconcileInterval)
		defer ticker.Stop()

		for {
			<-ticker.C

			b.mu.Lock("deploymentReconcile")
			b.reconcileDeploymentsLocked()

			done := len(b.deploymentTimers) == 0
			b.mu.Unlock()

			if done {
				return
			}
		}
	}()
}

// reconcileDeploymentsLocked advances every in-flight deployment whose
// timer has elapsed by one progression step. Must be called under lock.
func (b *InMemoryBackend) reconcileDeploymentsLocked() {
	now := time.Now()

	for key, timer := range b.deploymentTimers {
		if now.Before(timer.nextAt) {
			continue
		}

		d, ok := b.deployments.Get(key)
		if !ok {
			delete(b.deploymentTimers, key)

			continue
		}

		b.advanceDeploymentLocked(d, timer, now)
		b.deployments.Put(d)

		if d.State == deploymentStateComplete || d.State == deploymentStateRolledBack ||
			d.State == deploymentStateReverted {
			delete(b.deploymentTimers, key)
		}
	}
}

// advanceDeploymentLocked moves a single deployment forward by one
// progression step. Must be called under lock.
func (b *InMemoryBackend) advanceDeploymentLocked(d *Deployment, timer *deploymentTimer, now time.Time) {
	switch d.State {
	case deploymentStateDeploying:
		timer.step++
		d.PercentageComplete = nextDeploymentPercentage(d.GrowthType, d.PercentageComplete, timer.step, d.GrowthFactor)

		if d.PercentageComplete >= fullPercentage {
			if d.FinalBakeTimeInMinutes > 0 {
				d.State = deploymentStateBaking
				appendDeploymentEvent(d, "BAKE_TIME_STARTED", "APPCONFIG", "Bake time started", now)
				timer.nextAt = now.Add(deploymentBakeDelay)
			} else {
				b.finalizeDeploymentLocked(d, now)
			}

			return
		}

		appendDeploymentEvent(
			d, "PERCENTAGE_UPDATED", "APPCONFIG",
			fmt.Sprintf("Deployment is %.0f%% complete", d.PercentageComplete), now,
		)
		timer.nextAt = now.Add(deploymentStepDelay)
	case deploymentStateBaking:
		b.finalizeDeploymentLocked(d, now)
	}
}

// finalizeStaleDeploymentsLocked completes any deployment restored in a
// non-terminal state. deploymentTimers is not persisted (see its doc
// comment on InMemoryBackend), so a deployment snapshotted mid-flight would
// otherwise have no timer driving it after Restore and would sit stuck in
// DEPLOYING/BAKING forever -- the "stuck deployment, client polls forever"
// failure mode StartDeployment's design already avoids for the synchronous
// case. Must be called under lock.
func (b *InMemoryBackend) finalizeStaleDeploymentsLocked() {
	now := time.Now()

	for _, d := range b.deployments.All() {
		if d.State == deploymentStateDeploying || d.State == deploymentStateBaking {
			b.finalizeDeploymentLocked(d, now)
			b.deployments.Put(d)
		}
	}
}

// GetDeployment retrieves a deployment.
func (b *InMemoryBackend) GetDeployment(
	applicationID, environmentID string,
	deploymentNumber int32,
) (*Deployment, error) {
	b.mu.RLock("GetDeployment")
	defer b.mu.RUnlock()

	d, ok := b.deployments.Get(deploymentKey(applicationID, environmentID, deploymentNumber))
	if !ok {
		return nil, fmt.Errorf("%w: deployment %d", ErrDeploymentNotFound, deploymentNumber)
	}

	cp := *d

	return &cp, nil
}

// ListDeployments returns paginated deployments for an environment.
func (b *InMemoryBackend) ListDeployments(
	applicationID, environmentID, nextToken string,
	maxResults int,
) ([]Deployment, string, error) {
	b.mu.RLock("ListDeployments")
	defer b.mu.RUnlock()

	// Single lookup — returns a clear error for app-not-found or env-not-found.
	env, ok := b.environments.Get(environmentID)
	if !ok || env.ApplicationID != applicationID {
		if !b.applications.Has(applicationID) {
			return nil, "", fmt.Errorf("%w: application %s", ErrApplicationNotFound, applicationID)
		}

		return nil, "", fmt.Errorf("%w: environment %s", ErrEnvironmentNotFound, environmentID)
	}

	deploys := b.deploymentsByEnv.Get(appEnvKey(applicationID, environmentID))

	out := make([]Deployment, 0, len(deploys))
	for _, d := range deploys {
		out = append(out, *d)
	}

	sort.Slice(
		out,
		func(i, j int) bool { return out[i].DeploymentNumber < out[j].DeploymentNumber },
	)

	page, token := appConfigPaginate(out, nextToken, b.paginationSecret, maxResults)

	return page, token, nil
}

// stoppableDeploymentStates are the states from which a deployment can be
// stopped (moved to ROLLED_BACK).
var stoppableDeploymentStates = map[string]bool{ //nolint:gochecknoglobals // compile-time constant map
	deploymentStateBaking:     true,
	deploymentStateDeploying:  true,
	deploymentStateValidating: true,
}

// StopDeployment stops an in-progress deployment, moving it to
// ROLLED_BACK. When allowRevert is true and the deployment is already
// COMPLETE, it instead reverts the environment to the previous
// configuration version and moves the deployment to REVERTED -- matching
// real StopDeploymentInput.AllowRevert semantics.
func (b *InMemoryBackend) StopDeployment(
	applicationID, environmentID string,
	deploymentNumber int32,
	allowRevert bool,
) error {
	b.mu.Lock("StopDeployment")
	defer b.mu.Unlock()

	key := deploymentKey(applicationID, environmentID, deploymentNumber)

	d, ok := b.deployments.Get(key)
	if !ok {
		return fmt.Errorf("%w: deployment %d", ErrDeploymentNotFound, deploymentNumber)
	}

	now := time.Now()
	updated := *d

	switch {
	case allowRevert && d.State == deploymentStateComplete:
		updated.State = deploymentStateReverted
		updated.CompletedAt = now
		appendDeploymentEvent(&updated, "REVERT_COMPLETED", "USER", "Deployment reverted", now)
		b.revertDeployedConfigLocked(&updated)
	case stoppableDeploymentStates[d.State]:
		updated.State = deploymentStateRolledBack
		updated.CompletedAt = now
		appendDeploymentEvent(&updated, "ROLLBACK_COMPLETED", "USER", "Deployment rolled back", now)
	default:
		return fmt.Errorf("%w: cannot stop deployment in state %s", ErrBadRequest, d.State)
	}

	delete(b.deploymentTimers, key)
	b.deployments.Put(&updated)

	return nil
}

// revertDeployedConfigLocked restores deployedConfigs for the reverted
// deployment's environment/profile to the ConfigurationVersion of the last
// COMPLETE deployment before it (or clears the entry if there is none),
// matching AllowRevert's "roll back to the previous configuration version"
// contract. Must be called under lock.
func (b *InMemoryBackend) revertDeployedConfigLocked(reverted *Deployment) {
	envDeploys := b.deploymentsByEnv.Get(appEnvKey(reverted.ApplicationID, reverted.EnvironmentID))

	var previous *Deployment

	for _, d := range envDeploys {
		if d.ConfigurationProfileID != reverted.ConfigurationProfileID {
			continue
		}

		if d.DeploymentNumber >= reverted.DeploymentNumber || d.State != deploymentStateComplete {
			continue
		}

		if previous == nil || d.DeploymentNumber > previous.DeploymentNumber {
			previous = d
		}
	}

	key := appEnvProfileKey(reverted.ApplicationID, reverted.EnvironmentID, reverted.ConfigurationProfileID)
	if previous == nil {
		delete(b.deployedConfigs, key)

		return
	}

	b.deployedConfigs[key] = previous.ConfigurationVersion
}
