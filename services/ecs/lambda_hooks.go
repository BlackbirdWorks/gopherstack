package ecs

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"
)

const (
	hookStatusInProgress = "IN_PROGRESS"
	hookStatusFailed     = "FAILED"

	defaultHookCallbackDelay = 30 * time.Second
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
	CallBackDelay *int   `json:"callBackDelay"`
	HookStatus    string `json:"hookStatus"`
	Reason        string `json:"reason"`
}

// parseHookResponse maps an invocation result to a hook status. The AWS developer guide has the function
// return {"hookStatus": SUCCEEDED|FAILED|IN_PROGRESS}; the SDK does not define it, so a successful invocation
// without a recognised hookStatus counts as SUCCEEDED and an invocation error as FAILED.
func parseHookResponse(resp []byte, invokeErr error) (string, time.Duration) {
	if invokeErr != nil {
		return hookStatusFailed, 0
	}

	var r hookResponse
	if json.Unmarshal(resp, &r) != nil {
		return hookStatusSucceeded, 0
	}

	switch strings.ToUpper(r.HookStatus) {
	case hookStatusFailed:
		return hookStatusFailed, 0
	case hookStatusInProgress:
		delay := defaultHookCallbackDelay
		if r.CallBackDelay != nil {
			delay = time.Duration(*r.CallBackDelay) * time.Second
		}

		return hookStatusInProgress, delay
	}

	return hookStatusSucceeded, 0
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
	revisionArn string
}

func (inv *hookInvocation) payload() []byte {
	details := map[string]any{
		"serviceArn": inv.serviceArn, "targetServiceRevisionArn": inv.revisionArn, "lifecycleStage": inv.stage,
	}
	if inv.hookDetails != nil {
		details["hookDetails"] = inv.hookDetails
	}

	raw, _ := json.Marshal(map[string]any{"executionDetails": details})

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
		stage: reportedLifecycleStage(d.Stage), revisionArn: sd.TargetServiceRevisionArn,
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

		status, delay := parseHookResponse(resp, err)
		if status != hookStatusInProgress {
			b.finishLambdaHook(inv, status)

			return
		}

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
