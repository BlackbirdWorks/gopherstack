package secretsmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrUnknownOperation is returned when an unsupported operation is requested.
var ErrUnknownOperation = errors.New("UnknownOperationException")

// LambdaInvoker can invoke a Lambda function synchronously.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name, invocationType string, payload []byte) ([]byte, int, error)
}

// Handler is the Echo HTTP handler for Secrets Manager operations.
type Handler struct {
	Backend       StorageBackend
	lambdaInvoker LambdaInvoker
	ops           map[string]smActionFn
	janitor       *Janitor
	janitorCancel context.CancelFunc
	janitorDone   chan struct{}
	DefaultRegion string
	janitorMu     sync.Mutex
}

// NewHandler creates a new Secrets Manager handler.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{
		Backend: backend,
	}
	h.buildOps()

	return h
}

// WithJanitor attaches a background janitor to the handler.
func (h *Handler) WithJanitor(interval ...time.Duration) *Handler {
	if memBackend, ok := h.Backend.(*InMemoryBackend); ok {
		var d time.Duration
		if len(interval) > 0 {
			d = interval[0]
		}
		h.janitor = NewJanitor(memBackend, d)
	}

	return h
}

// StartWorker starts the background janitor if it is configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		h.janitorMu.Lock()
		if h.janitorDone != nil {
			h.janitorMu.Unlock()

			return nil
		}

		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		h.janitorCancel = cancel
		h.janitorDone = done
		h.janitorMu.Unlock()

		go func() {
			defer close(done)
			h.janitor.Run(runCtx)
		}()
	}

	return nil
}

// Shutdown stops the janitor worker and waits for it to exit.
func (h *Handler) Shutdown(ctx context.Context) {
	h.janitorMu.Lock()
	cancel := h.janitorCancel
	done := h.janitorDone
	h.janitorCancel = nil
	h.janitorDone = nil
	h.janitorMu.Unlock()

	if cancel == nil || done == nil {
		return
	}

	cancel()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

var _ service.BackgroundWorker = (*Handler)(nil)
var _ service.Shutdowner = (*Handler)(nil)

// buildOps builds and caches the operation dispatch table.
func (h *Handler) buildOps() {
	table := make(map[string]smActionFn)
	maps.Copy(table, h.smCRUDActions())
	maps.Copy(table, h.smVersionActions())
	maps.Copy(table, h.smTagActions())
	maps.Copy(table, h.smExtendedActions())
	h.ops = table
}

// SetLambdaInvoker sets the Lambda invoker used for RotateSecret. It is stored
// on both the handler (for HTTP-triggered rotations) and the backend (for
// scheduled rotations triggered by the rotation scheduler goroutine).
func (h *Handler) SetLambdaInvoker(invoker LambdaInvoker) {
	h.lambdaInvoker = invoker

	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.SetLambdaInvoker(invoker)
	}
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "SecretsManager"
}

// GetSupportedOperations returns the list of supported Secrets Manager operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"BatchGetSecretValue",
		"CancelRotateSecret",
		"CreateSecret",
		"DeleteResourcePolicy",
		"DeleteSecret",
		"DescribeSecret",
		"GetRandomPassword",
		"GetResourcePolicy",
		"GetSecretValue",
		"ListSecretVersionIds",
		"ListSecrets",
		"PutResourcePolicy",
		"PutSecretValue",
		"RemoveRegionsFromReplication",
		"ReplicateSecretToRegions",
		"RestoreSecret",
		"RotateSecret",
		"StopReplicationToReplica",
		"TagResource",
		"UntagResource",
		"UpdateSecret",
		"UpdateSecretVersionStage",
		"ValidateResourcePolicy",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "secretsmanager" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this Secrets Manager instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Secrets Manager requests by X-Amz-Target header.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "secretsmanager")
	}
}

// MatchPriority returns the routing priority for the Secrets Manager handler.
func (h *Handler) MatchPriority() int {
	return service.PriorityHeaderPartial
}

// ExtractOperation extracts the Secrets Manager operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")

	const targetParts = 2
	if len(parts) == targetParts {
		return parts[1]
	}

	return "Unknown"
}

// ExtractResource returns the secret ID from the request body when present.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	if secretID, ok := data["SecretId"].(string); ok {
		return secretID
	}

	if name, ok := data["Name"].(string); ok {
		return name
	}

	return ""
}

// Handler returns the Echo handler function for Secrets Manager operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"SecretsManager", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(ctx, c.Request(), action, body)
			},
			h.handleError,
		)
	}
}

type smActionFn func(ctx context.Context, region string, body []byte) (any, error)

func (h *Handler) smExtendedActions() map[string]smActionFn {
	return map[string]smActionFn{
		"GetResourcePolicy": func(_ context.Context, _ string, b []byte) (any, error) {
			var input GetResourcePolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetResourcePolicy(&input)
		},
		"PutResourcePolicy": func(_ context.Context, _ string, b []byte) (any, error) {
			var input PutResourcePolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutResourcePolicy(&input)
		},
		"DeleteResourcePolicy": func(_ context.Context, _ string, b []byte) (any, error) {
			var input DeleteResourcePolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteResourcePolicy(&input)
		},
		"BatchGetSecretValue": func(_ context.Context, _ string, b []byte) (any, error) {
			var input BatchGetSecretValueInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.BatchGetSecretValue(&input)
		},
		"CancelRotateSecret": func(_ context.Context, _ string, b []byte) (any, error) {
			var input CancelRotateSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.CancelRotateSecret(&input)
		},
		"ReplicateSecretToRegions": func(_ context.Context, _ string, b []byte) (any, error) {
			var input ReplicateSecretToRegionsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ReplicateSecretToRegions(&input)
		},
		"RemoveRegionsFromReplication": func(_ context.Context, _ string, b []byte) (any, error) {
			var input RemoveRegionsFromReplicationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RemoveRegionsFromReplication(&input)
		},
		"StopReplicationToReplica": func(_ context.Context, _ string, b []byte) (any, error) {
			var input StopReplicationToReplicaInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.StopReplicationToReplica(&input)
		},
		"ValidateResourcePolicy": func(_ context.Context, _ string, b []byte) (any, error) {
			var input ValidateResourcePolicyInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ValidateResourcePolicy(&input)
		},
	}
}

func (h *Handler) smCRUDActions() map[string]smActionFn {
	return map[string]smActionFn{
		"CreateSecret": func(_ context.Context, region string, b []byte) (any, error) {
			var input CreateSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			input.Region = region

			return h.Backend.CreateSecret(&input)
		},
		"GetSecretValue": func(_ context.Context, _ string, b []byte) (any, error) {
			var input GetSecretValueInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetSecretValue(&input)
		},
		"PutSecretValue": func(_ context.Context, _ string, b []byte) (any, error) {
			var input PutSecretValueInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutSecretValue(&input)
		},
		"DeleteSecret": func(_ context.Context, _ string, b []byte) (any, error) {
			var input DeleteSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteSecret(&input)
		},
		"ListSecrets": func(_ context.Context, _ string, b []byte) (any, error) {
			var input ListSecretsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListSecrets(&input)
		},
		"DescribeSecret": func(_ context.Context, _ string, b []byte) (any, error) {
			var input DescribeSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeSecret(&input)
		},
		"UpdateSecret": func(_ context.Context, _ string, b []byte) (any, error) {
			var input UpdateSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateSecret(&input)
		},
		"RestoreSecret": func(_ context.Context, _ string, b []byte) (any, error) {
			var input RestoreSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.RestoreSecret(&input)
		},
		"RotateSecret": func(ctx context.Context, region string, b []byte) (any, error) {
			var input RotateSecretInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.rotateSecret(ctx, region, &input)
		},
		"GetRandomPassword": func(_ context.Context, _ string, b []byte) (any, error) {
			var input GetRandomPasswordInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetRandomPassword(&input)
		},
	}
}

func (h *Handler) smTagActions() map[string]smActionFn {
	return map[string]smActionFn{
		"TagResource": func(_ context.Context, _ string, b []byte) (any, error) {
			var input TagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.TagResource(&input)
		},
		"UntagResource": func(_ context.Context, _ string, b []byte) (any, error) {
			var input UntagResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.UntagResource(&input)
		},
	}
}

func (h *Handler) smVersionActions() map[string]smActionFn {
	return map[string]smActionFn{
		"ListSecretVersionIds": func(_ context.Context, _ string, b []byte) (any, error) {
			var input ListSecretVersionIDsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListSecretVersionIDs(&input)
		},
		"UpdateSecretVersionStage": func(_ context.Context, _ string, b []byte) (any, error) {
			var input UpdateSecretVersionStageInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.UpdateSecretVersionStage(&input)
		},
	}
}

// dispatch routes the operation to the appropriate backend method.
func (h *Handler) dispatch(ctx context.Context, r *http.Request, action string, body []byte) ([]byte, error) {
	region := httputils.ExtractRegionFromRequest(r, h.DefaultRegion)

	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownOperation, action)
	}

	response, err := fn(ctx, region, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(response)
}

// handleError writes a structured error response for a Secrets Manager operation failure.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	var errorType string

	statusCode := http.StatusBadRequest

	switch {
	case errors.Is(reqErr, ErrSecretNotFound), errors.Is(reqErr, ErrVersionNotFound):
		errorType = errResourceNotFoundException
	case errors.Is(reqErr, ErrSecretAlreadyExists):
		errorType = "ResourceExistsException"
	case errors.Is(reqErr, ErrSecretDeleted):
		errorType = "InvalidRequestException"
	case errors.Is(reqErr, ErrSecretValueTooLarge),
		errors.Is(reqErr, ErrInvalidPasswordParameters),
		errors.Is(reqErr, ErrInvalidParameter),
		errors.Is(reqErr, ErrInvalidSecretName):
		errorType = "InvalidParameterException"
	case errors.Is(reqErr, ErrUnknownOperation):
		errorType = "UnknownOperationException"
	default:
		errorType = "InternalServiceError"
		statusCode = http.StatusInternalServerError
	}

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "SecretsManager internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "SecretsManager request error", "error", reqErr, "action", action)
	}

	payload, _ := json.Marshal(ErrorResponse{
		Type:    errorType,
		Message: reqErr.Error(),
	})

	return c.JSONBlob(statusCode, payload)
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
	out, err := h.Backend.RotateSecret(input)
	if err != nil {
		return nil, err
	}

	if input.RotationLambdaARN == "" || h.lambdaInvoker == nil {
		// No Lambda ARN, or no invoker wired — backend already promoted to AWSCURRENT.
		return out, nil
	}

	// Lambda ARN + invoker: backend created AWSPENDING; invoke steps and promote.
	if err := h.invokeLambdaRotationSteps(ctx, input, out); err != nil {
		return nil, err
	}

	// Promote AWSPENDING → AWSCURRENT after all Lambda steps succeed.
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		if finishErr := b.FinishRotation(input.SecretID, out.VersionID); finishErr != nil {
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

		if _, _, invokeErr := h.lambdaInvoker.InvokeFunction(
			ctx, functionName, "RequestResponse", event,
		); invokeErr != nil {
			if b, ok := h.Backend.(*InMemoryBackend); ok {
				_ = b.AbortRotation(input.SecretID, out.VersionID)
			}

			return fmt.Errorf("rotation Lambda step %q failed: %w", step, invokeErr)
		}
	}

	return nil
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}
}
