package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// objectLambdaTimeout is the maximum time to wait for a WriteGetObjectResponse.
const objectLambdaTimeout = 30 * time.Second

// objectLambdaResponse carries the transformed response from a Lambda function.
type objectLambdaResponse struct {
	body       []byte
	err        error
	headers    http.Header
	statusCode int
}

// objectLambdaGetObjectContext is the getObjectContext JSON block sent to the lambda.
type objectLambdaGetObjectContext struct {
	InputS3URL  string `json:"inputS3Url"`
	OutputRoute string `json:"outputRoute"`
	OutputToken string `json:"outputToken"`
}

// objectLambdaEvent is the JSON payload sent when invoking a Lambda for GetObject.
type objectLambdaEvent struct {
	GetObjectContext objectLambdaGetObjectContext `json:"getObjectContext"`
}

// objectLambdaConfigs and pendingObjectLambdaRequests are kept on S3Handler via
// SetObjectLambdaConfig and the pending request map.

// SetObjectLambdaConfig registers a Lambda ARN to be invoked for GetObject requests
// on the given bucket.  When set, GetObject triggers the Lambda and waits for
// WriteGetObjectResponse before streaming the (transformed) body back to the caller.
func (h *S3Handler) SetObjectLambdaConfig(bucket, lambdaARN string) {
	h.objectLambdaMu.Lock()
	defer h.objectLambdaMu.Unlock()

	if h.objectLambdaConfigs == nil {
		h.objectLambdaConfigs = make(map[string]string)
	}

	h.objectLambdaConfigs[bucket] = lambdaARN
}

// objectLambdaARN returns the configured Lambda ARN for the bucket, or "".
func (h *S3Handler) objectLambdaARN(bucket string) string {
	h.objectLambdaMu.RLock()
	defer h.objectLambdaMu.RUnlock()

	return h.objectLambdaConfigs[bucket]
}

// registerObjectLambdaRequest adds a pending channel keyed by token and returns the channel.
func (h *S3Handler) registerObjectLambdaRequest(token string) chan objectLambdaResponse {
	ch := make(chan objectLambdaResponse, 1)
	h.pendingObjectLambdaRequests.Store(token, ch)

	return ch
}

// resolveObjectLambdaRequest looks up the pending channel for a token and removes it.
func (h *S3Handler) resolveObjectLambdaRequest(token string) (chan objectLambdaResponse, bool) {
	v, ok := h.pendingObjectLambdaRequests.LoadAndDelete(token)
	if !ok {
		return nil, false
	}

	ch, ok := v.(chan objectLambdaResponse)

	return ch, ok
}

// handleObjectLambdaGetObject invokes the configured Lambda for the given bucket/key
// and streams the transformed response back to the caller.
func (h *S3Handler) handleObjectLambdaGetObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key, lambdaARN string,
) {
	if h.notifier == nil {
		WriteError(ctx, w, r, ErrNoSuchKey)
		return
	}

	token := uuid.NewString()
	ch := h.registerObjectLambdaRequest(token)

	inputURL := fmt.Sprintf("%s/%s/%s", h.Endpoint, bucket, key)

	event := objectLambdaEvent{
		GetObjectContext: objectLambdaGetObjectContext{
			InputS3URL:  inputURL,
			OutputRoute: "gopherstack",
			OutputToken: token,
		},
	}
	payload, err := json.Marshal(event)
	if err != nil {
		h.pendingObjectLambdaRequests.Delete(token)
		WriteError(ctx, w, r, err)
		return
	}

	// Invoke the Lambda asynchronously so it can call back via WriteGetObjectResponse
	// without blocking the current goroutine.
	go func() {
		dispCtx := h.notificationDispatchContext()
		if inv, ok := h.notifier.(LambdaInvoker); ok {
			_, _, invErr := inv.InvokeFunction(dispCtx, lambdaARN, "RequestResponse", payload)
			if invErr != nil {
				logger.Load(dispCtx).WarnContext(dispCtx, "object lambda: invocation failed",
					"arn", lambdaARN, "error", invErr)
				h.pendingObjectLambdaRequests.LoadAndDelete(token)
				ch <- objectLambdaResponse{err: invErr}
			}
		} else {
			h.pendingObjectLambdaRequests.LoadAndDelete(token)
			ch <- objectLambdaResponse{err: ErrNoSuchKey}
		}
	}()

	select {
	case resp := <-ch:
		if resp.err != nil {
			WriteError(ctx, w, r, resp.err)
			return
		}
		for k, vals := range resp.headers {
			for _, v := range vals {
				w.Header().Set(k, v)
			}
		}
		if resp.statusCode != 0 {
			w.WriteHeader(resp.statusCode)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		_, _ = w.Write(resp.body)

	case <-time.After(objectLambdaTimeout):
		h.pendingObjectLambdaRequests.Delete(token)
		WriteError(ctx, w, r, ErrNoSuchKey)

	case <-ctx.Done():
		h.pendingObjectLambdaRequests.Delete(token)
	}
}

// handleWriteGetObjectResponse handles POST /?writeGetObjectResponse.
// It reads the transformed body and delivers it to the pending GetObject channel.
func (h *S3Handler) handleWriteGetObjectResponse(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "WriteGetObjectResponse")

	token := r.Header.Get("X-Amz-Request-Token")

	if token == "" {
		// No pending request token; return 200 as a no-op stub.
		w.WriteHeader(http.StatusOK)
		return
	}

	ch, ok := h.resolveObjectLambdaRequest(token)
	if !ok {
		w.WriteHeader(http.StatusOK)
		return
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		ch <- objectLambdaResponse{err: err}
		w.WriteHeader(http.StatusOK)
		return
	}

	// Propagate selected response headers from the Lambda's WriteGetObjectResponse call.
	fwdHeaders := http.Header{}
	for _, hdr := range []string{
		"Content-Type", "Content-Length", "Content-Encoding",
		"ETag", "Last-Modified", "x-amz-server-side-encryption",
	} {
		if v := r.Header.Get(hdr); v != "" {
			fwdHeaders.Set(hdr, v)
		}
	}

	ch <- objectLambdaResponse{
		statusCode: http.StatusOK,
		headers:    fwdHeaders,
		body:       body,
	}

	w.WriteHeader(http.StatusOK)
}

// objectLambdaMu, objectLambdaConfigs, pendingObjectLambdaRequests are added as fields
// on S3Handler (see handler.go additions below).

// objectLambdaHandlerFields holds the new fields required by object lambda support.
// They are embedded into S3Handler.
type objectLambdaHandlerFields struct {
	pendingObjectLambdaRequests sync.Map
	objectLambdaConfigs         map[string]string
	objectLambdaMu              sync.RWMutex
}

// InvokeFunction satisfies LambdaInvoker for inMemoryNotificationDispatcher.
// The existing dispatcher already exposes its LambdaInvoker. We add a small shim
// so callers don't need to type-assert into the private struct.
func (d *inMemoryNotificationDispatcher) InvokeFunction(
	ctx context.Context,
	name, invocationType string,
	payload []byte,
) ([]byte, int, error) {
	if d.targets == nil || d.targets.LambdaInvoker == nil {
		return nil, 0, io.EOF
	}

	return d.targets.LambdaInvoker.InvokeFunction(ctx, name, invocationType, payload)
}
