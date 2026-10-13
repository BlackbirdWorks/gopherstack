package ecs

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"time"
)

const (
	hookStatusInProgress = "IN_PROGRESS"
	hookStatusFailed     = "FAILED"

	defaultHookCallbackDelay = 30 * time.Second
	fullTrafficWeight        = 100
)

// LambdaInvoker runs a function synchronously; a function error must be returned as an error.
type LambdaInvoker interface {
	Invoke(ctx context.Context, functionARN string, payload []byte) ([]byte, error)
}

// SetLambdaInvoker wires the invoker used by AWS_LAMBDA lifecycle hooks. Without one the hooks are not run.
func (b *InMemoryBackend) SetLambdaInvoker(l LambdaInvoker) {
	b.mu.Lock("SetLambdaInvoker")
	defer b.mu.Unlock()

	b.lambdaInvoker = l
}

type hookResponse struct {
	CallBackDelay *int           `json:"callBackDelay"`
	HookDetails   map[string]any `json:"hookDetails"`
	HookStatus    string         `json:"hookStatus"`
}

// parseHookResponse maps an invocation result to a hook status per the ECS developer guide ("Lambda hooks"):
// a missing or invalid hookStatus, an unparsable response or an invocation error fails the hook.
func parseHookResponse(resp []byte, invokeErr error) (string, time.Duration, map[string]any) {
	if invokeErr != nil {
		return hookStatusFailed, 0, nil
	}

	var r hookResponse
	if json.Unmarshal(resp, &r) != nil {
		return hookStatusFailed, 0, nil
	}

	switch r.HookStatus {
	case hookStatusSucceeded:
		return hookStatusSucceeded, 0, nil
	case hookStatusInProgress:
		delay := defaultHookCallbackDelay
		if r.CallBackDelay != nil {
			delay = time.Duration(*r.CallBackDelay) * time.Second
		}

		return hookStatusInProgress, delay, r.HookDetails
	}

	return hookStatusFailed, 0, nil
}

// mergeHookDetails folds runtime hookDetails from an IN_PROGRESS response into the next invocation.
func mergeHookDetails(current any, update map[string]any) any {
	if len(update) == 0 {
		return current
	}

	merged := map[string]any{}
	if cur, ok := current.(map[string]any); ok {
		maps.Copy(merged, cur)
	}

	maps.Copy(merged, update)

	return merged
}

func isLambdaHook(h *DeploymentLifecycleHook) bool { return h.TargetType == hookTargetLambda }

// lambdaHookDetails returns the configured hookDetails for the hook running at stage against arn.
func lambdaHookDetails(svc *Service, arn, stage string) any {
	for i := range svc.DeploymentConfiguration.LifecycleHooks {
		h := &svc.DeploymentConfiguration.LifecycleHooks[i]
		if isLambdaHook(h) && h.HookTargetArn == arn && slices.Contains(h.LifecycleStages, stage) {
			return h.HookDetails
		}
	}

	return nil
}

type hookInvocation struct {
	expires     time.Time
	hookDetails any
	hookID      string
	deployment  string
	targetArn   string
	serviceArn  string
	stage       string
	rawStage    string
	revisionArn string
	sourceArns  []string
}

// trafficWeights returns the test and production weight maps the guide shows for the stage.
func (inv *hookInvocation) trafficWeights() (map[string]int, map[string]int) {
	test, prod := map[string]int{}, map[string]int{}

	shift := func(m map[string]int, target int) {
		m[inv.revisionArn] = target
		for _, src := range inv.sourceArns {
			m[src] = fullTrafficWeight - target
		}
	}

	switch inv.rawStage {
	case stageTestTrafficShift:
		shift(test, fullTrafficWeight)
	case stagePreProductionShift:
		shift(prod, 0)
	case stageProductionTrafficShift:
		shift(prod, fullTrafficWeight)
	}

	return test, prod
}

func (inv *hookInvocation) payload() []byte {
	test, prod := inv.trafficWeights()
	event := map[string]any{
		"executionId":    inv.hookID,
		"lifecycleStage": inv.stage,
		"resourceArn":    inv.deployment,
		"executionDetails": map[string]any{
			"serviceArn":               inv.serviceArn,
			"targetServiceRevisionArn": inv.revisionArn,
			"testTrafficWeights":       test,
			"productionTrafficWeights": prod,
		},
	}

	if inv.hookDetails != nil {
		event["hookDetails"] = inv.hookDetails
	}

	raw, _ := json.Marshal(event)

	return raw
}

// ensureLambdaHookRunningLocked starts the invocation goroutine for an IN_PROGRESS AWS_LAMBDA hook once.
func (b *InMemoryBackend) ensureLambdaHookRunningLocked(svc *Service, sd *ServiceDeployment, d *LifecycleHookDetail) {
	if d.TargetType != hookTargetLambda || b.lambdaInvoker == nil || d.ExpiresAt == nil {
		return
	}

	if _, running := b.hooksInFlight[d.HookID]; running {
		return
	}

	if b.hooksInFlight == nil {
		b.hooksInFlight = make(map[string]struct{})
	}

	b.hooksInFlight[d.HookID] = struct{}{}

	inv := &hookInvocation{
		expires: *d.ExpiresAt, hookDetails: lambdaHookDetails(svc, d.TargetArn, d.Stage), hookID: d.HookID,
		deployment: sd.ServiceDeploymentArn, targetArn: d.TargetArn, serviceArn: sd.ServiceArn,
		stage: reportedLifecycleStage(d.Stage), rawStage: d.Stage, revisionArn: sd.TargetServiceRevisionArn,
	}
	for _, src := range sd.sourceRevisions {
		inv.sourceArns = append(inv.sourceArns, src.Arn)
	}

	go b.runLambdaHook(b.lambdaInvoker, inv)
}

func (b *InMemoryBackend) runLambdaHook(invoker LambdaInvoker, inv *hookInvocation) {
	defer func() {
		b.mu.Lock("runLambdaHook")
		delete(b.hooksInFlight, inv.hookID)
		b.mu.Unlock()
	}()

	ctx, cancel := context.WithDeadline(context.Background(), inv.expires)
	defer cancel()

	for {
		resp, err := invoker.Invoke(ctx, inv.targetArn, inv.payload())
		if ctx.Err() != nil {
			return
		}

		status, delay, details := parseHookResponse(resp, err)
		if status != hookStatusInProgress {
			b.finishLambdaHook(inv, status)

			return
		}

		inv.hookDetails = mergeHookDetails(inv.hookDetails, details)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
	}
}

func (b *InMemoryBackend) finishLambdaHook(inv *hookInvocation, status string) {
	b.mu.Lock("finishLambdaHook")
	defer b.mu.Unlock()

	sd, ok := b.serviceDeployments.Get(inv.deployment)
	if !ok || isTerminalServiceDeploymentStatus(sd.Status) {
		return
	}

	for i := range sd.LifecycleHookDetails {
		d := &sd.LifecycleHookDetails[i]
		if d.HookID == inv.hookID && d.Status == hookStatusInProgress {
			d.Status = status
		}
	}

	if svc := b.serviceByArnLocked(sd.ServiceArn); svc != nil {
		b.advanceDeploymentLifecycleLocked(svc, sd, time.Now())
	}
}
