package sagemakerruntime

import (
	"context"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// ContainerInvoker runs inference against the model containers behind an endpoint. It is satisfied
// by the wired SageMaker control plane (see lazyEndpointLookup) and implemented by its container
// engine; without one, invoke calls fail with ServiceUnavailable instead of returning made-up output.
type ContainerInvoker interface {
	InvokeEndpoint(ctx context.Context, req sagemaker.InvokeRequest) (*sagemaker.InvokeResult, error)
	InvokeEndpointStream(ctx context.Context, req sagemaker.InvokeRequest) (*sagemaker.InvokeStream, error)
	SubmitAsyncInference(ctx context.Context, req sagemaker.AsyncRequest) (*sagemaker.AsyncAccepted, error)
}

var errNoInvoker = errors.New("no model container backend is wired to SageMaker Runtime")

// SetInvoker wires the model containers invoke calls are forwarded to.
func (b *InMemoryBackend) SetInvoker(inv ContainerInvoker) {
	b.mu.Lock("SetInvoker")
	defer b.mu.Unlock()

	b.containerInvoker = inv
}

func (b *InMemoryBackend) invoker() (ContainerInvoker, error) {
	b.mu.RLock("invoker")
	inv := b.containerInvoker
	b.mu.RUnlock()

	if inv == nil {
		return nil, errNoInvoker
	}

	return inv, nil
}

// writeInvokeError maps container-engine failures onto the SageMaker Runtime error shapes.
func writeInvokeError(c *echo.Context, err error) error {
	var modelErr *sagemaker.ModelError

	switch {
	case errors.As(err, &modelErr):
		return c.JSON(http.StatusFailedDependency, map[string]any{
			"__type": "ModelError", "Message": modelErr.Error(),
			"OriginalStatusCode": modelErr.OriginalStatusCode, "OriginalMessage": modelErr.OriginalMessage,
			"LogStreamArn": modelErr.LogStreamArn,
		})
	case errors.Is(err, context.DeadlineExceeded):
		return c.JSON(http.StatusFailedDependency, map[string]any{
			"__type":             "ModelError",
			"Message":            "Your invocation timed out while waiting for a response from container.",
			"OriginalStatusCode": http.StatusGatewayTimeout,
			"OriginalMessage":    "invocation timed out",
		})
	case errors.Is(err, sagemaker.ErrEndpointNotFound), errors.Is(err, sagemaker.ErrNotAsyncEndpoint):
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationError", err.Error()))
	case errors.Is(err, sagemaker.ErrEngineDisabled), errors.Is(err, sagemaker.ErrEngineUnavailable),
		errors.Is(err, sagemaker.ErrEndpointNotRunning), errors.Is(err, errNoInvoker), errors.Is(err, errEndpointLookupUnavailable):
		return c.JSON(http.StatusServiceUnavailable, errorResponse("ServiceUnavailable", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}
}

func invokeRequest(c *echo.Context, endpointName string, body []byte, accept, contentType string) sagemaker.InvokeRequest {
	h := c.Request().Header

	return sagemaker.InvokeRequest{
		EndpointName: endpointName, Body: body, ContentType: contentType, Accept: accept,
		CustomAttributes:        h.Get(headerCustomAttributes),
		TargetVariant:           h.Get(headerTargetVariant),
		TargetContainerHostname: h.Get(headerTargetContainer),
		TargetModel:             h.Get(headerTargetModel),
		InferenceID:             h.Get(headerInferenceID),
	}
}
