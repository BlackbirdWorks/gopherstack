package secretsmanager

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *Handler) smRotationActions() map[string]smActionFn {
	return map[string]smActionFn{
		"RotateSecret": func(ctx context.Context, region string, b []byte) (any, error) {
			var input RotateSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.rotateSecret(ctx, region, &input)
		},
		"CancelRotateSecret": func(ctx context.Context, _ string, b []byte) (any, error) {
			var input CancelRotateSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CancelRotateSecret(ctx, &input)
		},
	}
}

// rotationSteps is the ordered sequence of rotation steps sent to a rotation Lambda.
//
//nolint:gochecknoglobals // intentional package-level constant slice
var rotationSteps = []string{"createSecret", "setSecret", "testSecret", "finishSecret"}

// testSecretStep is the single step run by RotateSecret's RotateImmediately=false
// probe (see runRotationTestProbe): it validates the rotation configuration
// without creating a permanent new version.
const testSecretStep = "testSecret"

// Field names for the JSON event payload sent to a rotation Lambda invocation.
// Shared by every call site that builds a rotation step event, to avoid
// goconst-flagged literal duplication across this file, invokeLambdaRotationSteps,
// and the scheduler's rotation.go:runLambdaRotationSteps.
const (
	rotationEventSecretIDKey = "SecretId"
	rotationEventTokenKey    = "ClientRequestToken"
	rotationEventStepKey     = "Step"
)

// buildRotationStepEvent builds the JSON payload sent to a rotation Lambda for a
// single rotation step.
func buildRotationStepEvent(secretID, token, step string) ([]byte, error) {
	return json.Marshal(map[string]string{
		rotationEventSecretIDKey: secretID,
		rotationEventTokenKey:    token,
		rotationEventStepKey:     step,
	})
}

// extractFunctionNameFromARN extracts the bare function name from a Lambda ARN.
// Handles both unqualified ARNs (…:function:my-fn) and qualified ARNs (…:function:my-fn:alias).
func extractFunctionNameFromARN(arn string) string {
	const functionSegment = ":function:"

	if _, after, found := strings.Cut(arn, functionSegment); found {
		// Strip trailing qualifier (alias or version) if present.
		name, _, _ := strings.Cut(after, ":")

		return name
	}

	// Fallback: take the last colon-separated segment.
	if idx := strings.LastIndex(arn, ":"); idx >= 0 {
		return arn[idx+1:]
	}

	return arn
}

// rotateSecret performs RotateSecret, optionally invoking a rotation Lambda for each step.
// The backend creates a new AWSPENDING version; this function promotes it to AWSCURRENT
// after all Lambda steps succeed (or immediately if no Lambda ARN is configured).
func (h *Handler) rotateSecret(ctx context.Context, _ string, input *RotateSecretInput) (*RotateSecretOutput, error) {
	out, err := h.Backend.RotateSecret(ctx, input)
	if err != nil {
		return nil, err
	}

	rotateImmediately := true
	if input.RotateImmediately != nil {
		rotateImmediately = *input.RotateImmediately
	}

	if !rotateImmediately {
		// RotateSecret doesn't rotate now; it only validates the rotation
		// configuration by running the Lambda's testSecret step against a
		// transient AWSPENDING version, then removes that version.
		return h.runRotationTestProbe(ctx, input, out)
	}

	lambdaARN := h.resolveRotationLambdaARN(ctx, input)
	if lambdaARN == "" || h.lambdaInvoker == nil {
		// No Lambda ARN configured (on this request or already stored on the
		// secret), or no invoker wired — backend already promoted to AWSCURRENT.
		return out, nil
	}

	// Lambda ARN + invoker: backend created AWSPENDING; invoke steps and promote.
	if err = h.invokeLambdaRotationSteps(ctx, input, lambdaARN, out); err != nil {
		return nil, err
	}

	// Promote AWSPENDING → AWSCURRENT after all Lambda steps succeed.
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		if finishErr := b.FinishRotation(ctx, input.SecretID, out.VersionID); finishErr != nil {
			return nil, finishErr
		}
	}

	return out, nil
}

// resolveRotationLambdaARN returns the Lambda ARN that governs this rotation:
// the request's own RotationLambdaARN if set, otherwise the ARN already
// stored on the secret from a prior EnableRotation/RotateSecret call. Real
// AWS callers normally configure the ARN once and omit it on every
// subsequent RotateSecret call, so falling back to the stored value (rather
// than treating an omitted request field as "no Lambda configured") is
// required to ever actually invoke it.
func (h *Handler) resolveRotationLambdaARN(ctx context.Context, input *RotateSecretInput) string {
	if input.RotationLambdaARN != "" {
		return input.RotationLambdaARN
	}

	desc, err := h.Backend.DescribeSecret(ctx, &DescribeSecretInput{SecretID: input.SecretID})
	if err != nil {
		return ""
	}

	return desc.RotationLambdaARN
}

// runRotationTestProbe implements RotateSecret's RotateImmediately=false path: if a
// rotation Lambda is configured (either on the request or already stored on the
// secret) and an invoker is wired, it runs only the testSecret step against a
// transient AWSPENDING version and then removes that version, without promoting
// anything to AWSCURRENT. If no Lambda is configured, or no invoker is wired,
// this is a no-op: the backend has already stored any RotationRules/Lambda ARN
// supplied on the request.
func (h *Handler) runRotationTestProbe(
	ctx context.Context, input *RotateSecretInput, out *RotateSecretOutput,
) (*RotateSecretOutput, error) {
	if h.lambdaInvoker == nil {
		return out, nil
	}

	lambdaARN := h.resolveRotationLambdaARN(ctx, input)
	if lambdaARN == "" {
		return out, nil
	}

	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return out, nil
	}

	versionID, probeErr := b.BeginRotationTestProbe(ctx, input.SecretID)
	if probeErr != nil {
		return nil, probeErr
	}

	// The transient probe version must be removed no matter how the Lambda
	// invocation below turns out.
	defer func() { _ = b.AbortRotation(ctx, input.SecretID, versionID) }()

	token := input.ClientRequestToken
	if token == "" {
		token = versionID
	}

	functionName := extractFunctionNameFromARN(lambdaARN)

	event, marshalErr := buildRotationStepEvent(input.SecretID, token, testSecretStep)
	if marshalErr != nil {
		return nil, fmt.Errorf("rotation event marshal: %w", marshalErr)
	}

	_, _, invokeErr := h.lambdaInvoker.InvokeFunction(ctx, functionName, "RequestResponse", event)
	if invokeErr != nil {
		return nil, fmt.Errorf("rotation Lambda step %q failed: %w", testSecretStep, invokeErr)
	}

	return out, nil
}

// invokeLambdaRotationSteps calls each rotation step in order via the Lambda invoker.
func (h *Handler) invokeLambdaRotationSteps(
	ctx context.Context,
	input *RotateSecretInput,
	lambdaARN string,
	out *RotateSecretOutput,
) error {
	token := input.ClientRequestToken
	if token == "" {
		token = out.VersionID
	}

	functionName := extractFunctionNameFromARN(lambdaARN)

	for _, step := range rotationSteps {
		event, marshalErr := buildRotationStepEvent(input.SecretID, token, step)
		if marshalErr != nil {
			return fmt.Errorf("rotation event marshal: %w", marshalErr)
		}

		result, _, invokeErr := h.lambdaInvoker.InvokeFunction(
			ctx, functionName, "RequestResponse", event,
		)
		if invokeErr != nil {
			if b, ok := h.Backend.(*InMemoryBackend); ok {
				_ = b.AbortRotation(ctx, input.SecretID, out.VersionID)
			}

			return fmt.Errorf("rotation Lambda step %q failed: %w", step, invokeErr)
		}
		if len(result) > 0 && string(result) != "{}" && string(result) != "null" {
			// This is a function error or unexpected result
			logger.Load(ctx).DebugContext(ctx, "Lambda step returned",
				"step", step,
				"result", string(result),
			)
		}
	}

	return nil
}
