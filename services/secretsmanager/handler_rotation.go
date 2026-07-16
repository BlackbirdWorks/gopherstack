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

	if input.RotationLambdaARN == "" || h.lambdaInvoker == nil {
		// No Lambda ARN, or no invoker wired — backend already promoted to AWSCURRENT.
		return out, nil
	}

	// Lambda ARN + invoker: backend created AWSPENDING; invoke steps and promote.
	if err = h.invokeLambdaRotationSteps(ctx, input, out); err != nil {
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

// invokeLambdaRotationSteps calls each rotation step in order via the Lambda invoker.
func (h *Handler) invokeLambdaRotationSteps(
	ctx context.Context,
	input *RotateSecretInput,
	out *RotateSecretOutput,
) error {
	token := input.ClientRequestToken
	if token == "" {
		token = out.VersionID
	}

	functionName := extractFunctionNameFromARN(input.RotationLambdaARN)

	for _, step := range rotationSteps {
		event, marshalErr := json.Marshal(map[string]string{
			"SecretId":           input.SecretID,
			"ClientRequestToken": token,
			"Step":               step,
		})
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
