package fis

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ----------------------------------------
// Experiment lifecycle
// ----------------------------------------

// StartExperiment creates and starts a new experiment from a template.
func (b *InMemoryBackend) StartExperiment(
	_ context.Context,
	input *startExperimentRequest,
	accountID, region string,
) (*Experiment, error) {
	// Check clientToken idempotency first (read-only fast path).
	if input.ClientToken != "" {
		b.mu.RLock("StartExperiment-idempotency")
		existingID, ok := b.expClientTokens[input.ClientToken]
		b.mu.RUnlock()

		if ok {
			return b.GetExperiment(existingID)
		}
	}

	b.mu.RLock("StartExperiment")
	tpl, ok := b.templates.Get(input.ExperimentTemplateID)
	leverEngaged := b.safetyLever != nil && b.safetyLever.State.Status == "engaged"
	experimentCount := b.experiments.Len()
	tplAccountCount := len(b.targetAccountConfigsByTemplate.Get(input.ExperimentTemplateID))
	b.mu.RUnlock()

	if leverEngaged {
		return nil, fmt.Errorf("%w: safety lever is engaged", ErrSafetyLeverEngaged)
	}

	if experimentCount >= maxExperiments {
		return nil, fmt.Errorf(
			"%w: experiment count would exceed the limit of %d",
			ErrTooManyExperiments,
			maxExperiments,
		)
	}

	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrTemplateNotFound, input.ExperimentTemplateID)
	}

	id := generateID("EXP")
	arnStr := arn.Build("fis", region, accountID, "experiment/"+id)

	// expCtx derives from b.svcCtx — NOT the HTTP request context — so the experiment
	// goroutine is not cancelled when the HTTP response is sent, but IS cancelled on shutdown.
	expCtx, cancel := context.WithCancel(b.svcCtx)
	exp := buildExperimentFromTemplate(id, arnStr, tpl, input.Tags, cancel)
	exp.TargetAccountConfigurationsCount = tplAccountCount

	// Clone the template BEFORE passing to the goroutine so template updates don't race.
	tplForRun := cloneTemplate(tpl)

	b.mu.Lock("StartExperiment")
	b.experiments.Put(exp)

	if input.ClientToken != "" {
		b.expClientTokens[input.ClientToken] = id
	}

	// Take the snapshot while holding the lock, before launching the goroutine,
	// so the background goroutine cannot mutate exp while we're reading it.
	snapshot := cloneExperiment(exp)
	b.mu.Unlock()

	// Run the experiment lifecycle in the background.
	go b.runExperiment(expCtx, id, tplForRun)

	return snapshot, nil
}

// buildExperimentFromTemplate constructs a new Experiment from a template and input tags.
func buildExperimentFromTemplate(
	id, arnStr string,
	tpl *ExperimentTemplate,
	inputTags map[string]string,
	cancel context.CancelFunc,
) *Experiment {
	targets := make(map[string]ExperimentTarget, len(tpl.Targets))
	for name, t := range tpl.Targets {
		targets[name] = ExperimentTarget{
			ResourceType: t.ResourceType,
			ResourceArns: append([]string(nil), t.ResourceArns...),
			Parameters:   copyStringMap(t.Parameters),
		}
	}

	actions := make(map[string]ExperimentAction, len(tpl.Actions))
	for name, a := range tpl.Actions {
		actions[name] = ExperimentAction{
			ActionID:   a.ActionID,
			Parameters: copyStringMap(a.Parameters),
			Targets:    copyStringMap(a.Targets),
			Status:     ExperimentActionStatus{Status: actionStatusPending},
		}
	}

	stopConditions := make([]ExperimentStopCondition, len(tpl.StopConditions))
	for i, sc := range tpl.StopConditions {
		stopConditions[i] = ExperimentStopCondition(sc)
	}

	logConfig := copyLogConfiguration(tpl.LogConfiguration)

	var expOptions *ExperimentExperimentOptions
	if tpl.ExperimentOptions != nil {
		expOptions = &ExperimentExperimentOptions{
			AccountTargeting:          tpl.ExperimentOptions.AccountTargeting,
			EmptyTargetResolutionMode: tpl.ExperimentOptions.EmptyTargetResolutionMode,
		}
	}

	// expCtx derives from svcCtx — NOT the HTTP request context — so the experiment
	// goroutine is not cancelled when the HTTP response is sent.
	// cancel is passed in from StartExperiment and stored on the returned experiment.

	now := time.Now()

	return &Experiment{
		ID:                   id,
		Arn:                  arnStr,
		ExperimentTemplateID: tpl.ID,
		RoleArn:              tpl.RoleArn,
		Status:               ExperimentStatus{Status: statusPending},
		Targets:              targets,
		Actions:              actions,
		StopConditions:       stopConditions,
		LogConfiguration:     logConfig,
		ExperimentOptions:    expOptions,
		Tags:                 copyStringMap(inputTags),
		CreationTime:         now,
		StartTime:            now,
		cancel:               cancel,
	}
}

// copyLogConfiguration deep-copies a template log configuration into its experiment equivalent.
func copyLogConfiguration(tplLog *ExperimentTemplateLogConfiguration) *ExperimentLogConfiguration {
	if tplLog == nil {
		return nil
	}

	lc := &ExperimentLogConfiguration{LogSchemaVersion: tplLog.LogSchemaVersion}

	if tplLog.CloudWatchLogsConfiguration != nil {
		lc.CloudWatchLogsConfiguration = &ExperimentCloudWatchLogsConfiguration{
			LogGroupArn: tplLog.CloudWatchLogsConfiguration.LogGroupArn,
		}
	}

	if tplLog.S3Configuration != nil {
		lc.S3Configuration = &ExperimentS3Configuration{
			BucketName: tplLog.S3Configuration.BucketName,
			Prefix:     tplLog.S3Configuration.Prefix,
		}
	}

	return lc
}

// GetExperiment retrieves an experiment by ID.
func (b *InMemoryBackend) GetExperiment(id string) (*Experiment, error) {
	b.mu.RLock("GetExperiment")
	defer b.mu.RUnlock()

	exp, ok := b.experiments.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExperimentNotFound, id)
	}

	return cloneExperiment(exp), nil
}

// StopExperiment stops a running experiment.
func (b *InMemoryBackend) StopExperiment(id string) (*Experiment, error) {
	b.mu.Lock("StopExperiment")

	exp, ok := b.experiments.Get(id)
	if !ok {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrExperimentNotFound, id)
	}

	s := exp.Status.Status
	if s != statusPending && s != statusInitiating && s != statusRunning && s != statusCompleting {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: %s", ErrExperimentNotRunning, id)
	}

	// Signal the background goroutine to stop.
	if exp.cancel != nil {
		exp.cancel()
	}

	// Immediately reflect the transition to stopping in the response.
	exp.Status = ExperimentStatus{Status: statusStopping}

	snap := cloneExperiment(exp)
	b.mu.Unlock()

	return snap, nil
}

// ListExperiments returns all experiments sorted by ID.
func (b *InMemoryBackend) ListExperiments() ([]*Experiment, error) {
	b.mu.RLock("ListExperiments")
	defer b.mu.RUnlock()

	all := b.experiments.All()
	result := make([]*Experiment, 0, len(all))

	for _, exp := range all {
		result = append(result, cloneExperiment(exp))
	}

	slices.SortFunc(result, func(a, b *Experiment) int { return strings.Compare(a.ID, b.ID) })

	return result, nil
}

// StopAllExperiments cancels every running experiment goroutine.
// Called during graceful shutdown to prevent goroutine leaks.
func (b *InMemoryBackend) StopAllExperiments() {
	b.mu.Lock("StopAllExperiments")
	defer b.mu.Unlock()

	for _, exp := range b.experiments.All() {
		if exp.cancel != nil {
			exp.cancel()
		}
	}
}

// ----------------------------------------
// Phase 3 — Resolved Targets
// ----------------------------------------

// ListExperimentResolvedTargets returns the resolved resource ARN counts for each
// named target group in the given experiment.
func (b *InMemoryBackend) ListExperimentResolvedTargets(id string) ([]ExperimentResolvedTarget, error) {
	b.mu.RLock("ListExperimentResolvedTargets")
	defer b.mu.RUnlock()

	exp, ok := b.experiments.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrExperimentNotFound, id)
	}

	resolved := make([]ExperimentResolvedTarget, 0, len(exp.Targets))

	for name, tgt := range exp.Targets {
		arns := make([]string, len(tgt.ResourceArns))
		copy(arns, tgt.ResourceArns)
		resolved = append(resolved, ExperimentResolvedTarget{
			ResourceType:         tgt.ResourceType,
			TargetName:           name,
			ResolvedArns:         arns,
			TargetResourcesCount: len(tgt.ResourceArns),
		})
	}

	slices.SortFunc(
		resolved,
		func(a, b ExperimentResolvedTarget) int { return strings.Compare(a.TargetName, b.TargetName) },
	)

	return resolved, nil
}

// ----------------------------------------
// Deep copy helpers
// ----------------------------------------

// cloneExperiment returns a snapshot of an Experiment safe to return outside the lock.
// The cancel field is intentionally NOT copied.
func cloneExperiment(exp *Experiment) *Experiment {
	cp := *exp
	cp.cancel = nil
	cp.CreationTime = exp.CreationTime
	cp.Tags = copyStringMap(exp.Tags)

	if exp.Targets != nil {
		cp.Targets = make(map[string]ExperimentTarget, len(exp.Targets))

		for k, v := range exp.Targets {
			t := v
			t.ResourceArns = append([]string(nil), v.ResourceArns...)
			t.Parameters = copyStringMap(v.Parameters)
			cp.Targets[k] = t
		}
	}

	if exp.Actions != nil {
		cp.Actions = make(map[string]ExperimentAction, len(exp.Actions))

		for k, v := range exp.Actions {
			a := v
			a.Parameters = copyStringMap(v.Parameters)
			a.Targets = copyStringMap(v.Targets)

			if v.StartTime != nil {
				st := *v.StartTime
				a.StartTime = &st
			}

			if v.EndTime != nil {
				et := *v.EndTime
				a.EndTime = &et
			}

			cp.Actions[k] = a
		}
	}

	if exp.StopConditions != nil {
		cp.StopConditions = append([]ExperimentStopCondition(nil), exp.StopConditions...)
	}

	if exp.EndTime != nil {
		et := *exp.EndTime
		cp.EndTime = &et
	}

	if exp.LogConfiguration != nil {
		lc := *exp.LogConfiguration
		if exp.LogConfiguration.CloudWatchLogsConfiguration != nil {
			cwl := *exp.LogConfiguration.CloudWatchLogsConfiguration
			lc.CloudWatchLogsConfiguration = &cwl
		}

		if exp.LogConfiguration.S3Configuration != nil {
			s3 := *exp.LogConfiguration.S3Configuration
			lc.S3Configuration = &s3
		}

		cp.LogConfiguration = &lc
	}

	if exp.ExperimentOptions != nil {
		opt := *exp.ExperimentOptions
		cp.ExperimentOptions = &opt
	}

	return &cp
}

// ----------------------------------------
// Experiment goroutine
// ----------------------------------------

// lifecycleDelay is the short pause between lifecycle state transitions so that
// SDK polling can observe intermediate states (initiating, completing, stopping).
const lifecycleDelay = 10 * time.Millisecond

// runExperiment manages the full lifecycle of a single experiment.
func (b *InMemoryBackend) runExperiment(ctx context.Context, expID string, tpl *ExperimentTemplate) {
	// PENDING → INITIATING.
	b.setExperimentStatus(expID, statusInitiating)
	b.setAllActionStatuses(expID, actionStatusInitiating)

	initiatingTimer := time.NewTimer(lifecycleDelay)
	defer initiatingTimer.Stop()
	select {
	case <-ctx.Done():
		b.cleanupActions(nil, expID, statusStopped, actionStatusCancelled)

		return
	case <-initiatingTimer.C:
	}

	// INITIATING → RUNNING.
	b.setExperimentStatus(expID, statusRunning)
	b.setAllActionStatuses(expID, actionStatusRunning)

	// Build fault rules and run actions respecting startAfter dependencies.
	faultRules, maxDuration, failReason := b.executeActionsOrdered(ctx, expID, tpl)

	if failReason != "" {
		b.cleanupActions(faultRules, expID, statusFailed, actionStatusFailed)

		return
	}

	// Wait for duration, stop signal, or context cancellation.
	// If maxDuration is 0 (e.g. all actions are immediate/non-timed), complete right away.
	if maxDuration == 0 {
		b.setExperimentStatus(expID, statusCompleting)
		b.setAllActionStatuses(expID, actionStatusCompleting)

		completingTimer := time.NewTimer(lifecycleDelay)
		defer completingTimer.Stop()
		select {
		case <-ctx.Done():
			b.cleanupActions(faultRules, expID, statusStopped, actionStatusStopped)

			return
		case <-completingTimer.C:
		}

		b.cleanupActions(faultRules, expID, statusCompleted, actionStatusCompleted)

		return
	}

	timer := time.NewTimer(maxDuration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		// Manually stopped or context cancelled — transition through stopping.
		b.setExperimentStatus(expID, statusStopping)
		b.cleanupActions(faultRules, expID, statusStopped, actionStatusStopped)
	case <-timer.C:
		// All actions completed naturally — transition through completing.
		b.setExperimentStatus(expID, statusCompleting)
		b.setAllActionStatuses(expID, actionStatusCompleting)

		finalTimer := time.NewTimer(lifecycleDelay)
		defer finalTimer.Stop()
		select {
		case <-ctx.Done():
			b.cleanupActions(faultRules, expID, statusStopped, actionStatusStopped)
		case <-finalTimer.C:
			b.cleanupActions(faultRules, expID, statusCompleted, actionStatusCompleted)
		}
	}
}

// executeActionsOrdered executes template actions in startAfter dependency order.
// Chaos fault rules are applied first, then external actions run in topological order.
// Returns accumulated fault rules, the maximum action duration, and a non-empty failure reason on error.
func (b *InMemoryBackend) executeActionsOrdered(
	ctx context.Context,
	expID string,
	tpl *ExperimentTemplate,
) ([]chaos.FaultRule, time.Duration, string) {
	var faultRules []chaos.FaultRule

	var maxDuration time.Duration

	// Sort actions into topological order respecting startAfter.
	ordered := topoSortActions(tpl.Actions)

	// Track which action names have completed so downstream deps can be released.
	completed := make(map[string]bool, len(tpl.Actions))

	for _, name := range ordered {
		action := tpl.Actions[name]

		// Check context before each action.
		select {
		case <-ctx.Done():
			return faultRules, maxDuration, ""
		default:
		}

		// Wait for all startAfter dependencies.
		for _, dep := range action.StartAfter {
			if !completed[dep] {
				// Dep should already be done since we process in topo order,
				// but guard against topo sort edge cases.
				continue
			}
		}

		dur := parseISODuration(action.Parameters["duration"])
		if dur > maxDuration {
			maxDuration = dur
		}

		switch {
		case strings.HasPrefix(action.ActionID, "aws:fis:inject-api-"):
			faultRules = append(faultRules, buildFaultRules(action)...)
			// Apply immediately so faults are active as soon as possible.
			if len(faultRules) > 0 && b.getFaultStore() != nil {
				b.getFaultStore().AppendRules(buildFaultRules(action))
			}
		case action.ActionID == actionIDWait:
			// Wait action — duration already captured above.
		default:
			ea := externalAction{
				actionID:   action.ActionID,
				params:     copyStringMap(action.Parameters),
				targets:    action.Targets,
				duration:   dur,
				tplTargets: tpl.Targets,
			}

			b.setActionStatus(expID, name, actionStatusRunning)

			if err := b.executeExternalAction(ctx, ea); err != nil {
				b.markExperimentFailed(expID, name, err.Error())

				return faultRules, maxDuration, err.Error()
			}

			b.setActionStatus(expID, name, actionStatusCompleted)
		}

		completed[name] = true
	}

	return faultRules, maxDuration, ""
}

// topoSortActions returns action names in a topological order respecting startAfter.
// Actions with no dependencies come first; actions whose dependencies are all earlier come later.
// The result is deterministic: within the same dependency level, actions are sorted by name.
func topoSortActions(actions map[string]ExperimentTemplateAction) []string {
	inDegree := make(map[string]int, len(actions))
	dependents := make(map[string][]string, len(actions)) // name → names that depend on it

	for name := range actions {
		if _, ok := inDegree[name]; !ok {
			inDegree[name] = 0
		}
	}

	for name, action := range actions {
		for _, dep := range action.StartAfter {
			inDegree[name]++
			dependents[dep] = append(dependents[dep], name)
		}
	}

	// Collect zero-in-degree nodes, sorted for determinism.
	var queue []string
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	slices.Sort(queue)

	result := make([]string, 0, len(actions))

	for len(queue) > 0 {
		// Pop front.
		cur := queue[0]
		queue = queue[1:]
		result = append(result, cur)

		// Reduce in-degree for dependents.
		next := make([]string, 0)

		for _, dep := range dependents[cur] {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				next = append(next, dep)
			}
		}

		slices.Sort(next)
		queue = append(queue, next...)
	}

	return result
}

// setActionStatus atomically updates a single action's status.
func (b *InMemoryBackend) setActionStatus(expID, actionName, status string) {
	b.mu.Lock("setActionStatus")
	defer b.mu.Unlock()

	if exp, ok := b.experiments.Get(expID); ok {
		if action, ok2 := exp.Actions[actionName]; ok2 {
			action.Status = ExperimentActionStatus{Status: status}
			exp.Actions[actionName] = action
		}
	}
}

// externalAction carries the data needed to call an external FISActionProvider.
type externalAction struct {
	params     map[string]string
	targets    map[string]string
	tplTargets map[string]ExperimentTemplateTarget
	actionID   string
	duration   time.Duration
}

// executeExternalAction calls the appropriate FISActionProvider for a non-built-in action.
// Returns an error if the provider reports a failure.
func (b *InMemoryBackend) executeExternalAction(ctx context.Context, ea externalAction) error {
	b.mu.RLock("executeExternalAction")
	providers := b.actionProviders
	b.mu.RUnlock()

	// Resolve target ARNs.
	var targetARNs []string

	for targetKey, targetName := range ea.targets {
		if tgt, ok := ea.tplTargets[targetKey]; ok {
			targetARNs = append(targetARNs, tgt.ResourceArns...)
		} else if tgtByName, ok2 := ea.tplTargets[targetName]; ok2 {
			targetARNs = append(targetARNs, tgtByName.ResourceArns...)
		}
	}

	exec := service.FISActionExecution{
		ActionID:   ea.actionID,
		Parameters: ea.params,
		Targets:    targetARNs,
		Duration:   ea.duration,
	}

	for _, p := range providers {
		for _, def := range p.FISActions() {
			if def.ActionID == ea.actionID {
				return p.ExecuteFISAction(ctx, exec)
			}
		}
	}

	return nil
}

// cleanupActions removes fault rules and sets the final experiment status.
// It also calls exp.cancel() to release the context and prevent goroutine leaks.
func (b *InMemoryBackend) cleanupActions(faultRules []chaos.FaultRule, expID, expStatus, actionStatus string) {
	if len(faultRules) > 0 && b.getFaultStore() != nil {
		b.getFaultStore().DeleteRules(faultRules)
	}

	now := time.Now()
	b.mu.Lock("cleanupActions")

	if exp, ok := b.experiments.Get(expID); ok {
		exp.Status = ExperimentStatus{Status: expStatus}
		exp.EndTime = &now

		for name, action := range exp.Actions {
			action.Status = ExperimentActionStatus{Status: actionStatus}
			endTime := now
			action.EndTime = &endTime
			exp.Actions[name] = action
		}

		// Release context resources; safe to call multiple times.
		if exp.cancel != nil {
			exp.cancel()
		}
	}

	b.mu.Unlock()
}

// setExperimentStatus atomically updates an experiment's status.
func (b *InMemoryBackend) setExperimentStatus(id, status string) {
	b.mu.Lock("setExperimentStatus")
	defer b.mu.Unlock()

	if exp, ok := b.experiments.Get(id); ok {
		exp.Status = ExperimentStatus{Status: status}
	}
}

// setAllActionStatuses atomically sets all actions in an experiment to the given status.
func (b *InMemoryBackend) setAllActionStatuses(expID, status string) {
	b.mu.Lock("setAllActionStatuses")
	defer b.mu.Unlock()

	if exp, ok := b.experiments.Get(expID); ok {
		now := time.Now()

		for name, action := range exp.Actions {
			action.Status = ExperimentActionStatus{Status: status}
			action.StartTime = &now
			exp.Actions[name] = action
		}
	}
}

// getFaultStore safely returns the fault store (may be nil).
func (b *InMemoryBackend) getFaultStore() *chaos.FaultStore {
	b.mu.RLock("getFaultStore")
	defer b.mu.RUnlock()

	return b.faultStore
}

// actionExecutionFailedCode is the ExperimentStatusError.Code reported when an
// external action provider fails during experiment execution. AWS FIS does not
// publish a fixed enum for this field (it is a free-form string), so this mirrors
// the class of failure without inventing a fictitious modeled exception name.
const actionExecutionFailedCode = "ActionExecutionFailed"

// markExperimentFailed sets an experiment and all its actions to failed with a
// reason. actionName identifies the template action whose execution failed and is
// reported as the structured error's Location, matching the real AWS FIS
// ExperimentError.Location semantics ("context for the section of the experiment
// template that failed").
func (b *InMemoryBackend) markExperimentFailed(expID, actionName, reason string) {
	b.mu.Lock("markExperimentFailed")
	defer b.mu.Unlock()

	exp, ok := b.experiments.Get(expID)
	if !ok {
		return
	}

	now := time.Now()
	exp.Status = ExperimentStatus{
		Status: statusFailed,
		Reason: reason,
		Error: &ExperimentStatusError{
			Code:      actionExecutionFailedCode,
			Location:  actionName,
			AccountID: b.accountID,
		},
	}
	exp.EndTime = &now

	for name, action := range exp.Actions {
		if action.Status.Status == actionStatusRunning || action.Status.Status == actionStatusPending {
			action.Status = ExperimentActionStatus{Status: actionStatusFailed, Reason: reason}
			endTime := now
			action.EndTime = &endTime
			exp.Actions[name] = action
		}
	}
}
