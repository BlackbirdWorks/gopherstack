package s3

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// objectLambdaTimeout is the maximum time to wait for a WriteGetObjectResponse.
const objectLambdaTimeout = 30 * time.Second

// objectLambdaResponse carries the transformed response from a Lambda function.
type objectLambdaResponse struct {
	err        error
	headers    http.Header
	body       []byte
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

// SetObjectLambdaConfig registers a Lambda ARN to be invoked for GetObject requests
// on the given bucket.  When set, GetObject triggers the Lambda and waits for
// WriteGetObjectResponse before streaming the (transformed) body back to the caller.
// Satisfies services/s3control's ObjectLambdaConfigSink interface.
func (h *S3Handler) SetObjectLambdaConfig(bucket, lambdaARN string) {
	h.Backend.SetObjectLambdaConfig(bucket, lambdaARN)
}

// objectLambdaARN returns the configured Lambda ARN for the bucket, or "".
func (h *S3Handler) objectLambdaARN(bucket string) string {
	return h.Backend.ObjectLambdaConfig(bucket)
}

// SetObjectLambdaConfig stores lambdaARN on bucket's own record under the bucket's
// coarse lock, mirroring how every other sub-resource config (CORS, policy,
// lifecycle, ...) is stored -- see cors.go. A no-op if the bucket doesn't exist
// (or is pending deletion): the config is cleared implicitly once the bucket
// record itself is removed, so a bucket recreated under the same name never
// inherits a prior incarnation's Lambda wiring.
func (b *InMemoryBackend) SetObjectLambdaConfig(bucketName, lambdaARN string) {
	b.mu.RLock("SetObjectLambdaConfig")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return
	}

	bucket.mu.Lock("SetObjectLambdaConfig")
	defer bucket.mu.Unlock()

	bucket.ObjectLambdaConfig = lambdaARN
}

// ObjectLambdaConfig returns the Lambda ARN configured for GetObject on
// bucketName, or "" if none is configured or the bucket doesn't exist.
func (b *InMemoryBackend) ObjectLambdaConfig(bucketName string) string {
	b.mu.RLock("ObjectLambdaConfig")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return ""
	}

	bucket.mu.RLock("ObjectLambdaConfig")
	defer bucket.mu.RUnlock()

	return bucket.ObjectLambdaConfig
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

// handleWriteGetObjectResponse handles POST /WriteGetObjectResponse.
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

	// X-Amz-Fwd-Status carries the Lambda's chosen response status (real SDK:
	// serializers.go's awsRestxml_serializeOpHttpBindingsWriteGetObjectResponseInput
	// binds WriteGetObjectResponseInput.StatusCode to this header) -- e.g. an
	// access-control Lambda returning 403, or a redirecting Lambda returning
	// 3xx. Previously hardcoded to 200 regardless of what the Lambda sent,
	// silently discarding the Lambda's real status in every case but success.
	statusCode := http.StatusOK
	if fwd := r.Header.Get("X-Amz-Fwd-Status"); fwd != "" {
		if n, convErr := strconv.Atoi(fwd); convErr == nil && n > 0 {
			statusCode = n
		}
	}

	ch <- objectLambdaResponse{
		statusCode: statusCode,
		headers:    fwdHeaders,
		body:       body,
	}

	w.WriteHeader(http.StatusOK)
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

// isWriteGetObjectResponseRequest returns true when the request targets
// WriteGetObjectResponse: POST to the literal path "/WriteGetObjectResponse"
// with no query string, per s3@v1.106.5 serializers.go:
// awsRestxml_serializeOpWriteGetObjectResponse (httpbinding.SplitURI("/WriteGetObjectResponse")).
// The real SDK never sends a ?writeGetObjectResponse query parameter.
func isWriteGetObjectResponseRequest(r *http.Request) bool {
	return r.Method == http.MethodPost &&
		strings.TrimPrefix(r.URL.Path, "/") == "WriteGetObjectResponse"
}
