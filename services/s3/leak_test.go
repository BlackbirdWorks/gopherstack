package s3_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

// serveWithContext is like serveS3Handler but uses an explicit context so tests
// can cancel the request context directly without going through the HTTP layer.
func serveWithContext(ctx context.Context, handler *s3.S3Handler, method, target string) {
	req := httptest.NewRequest(method, target, nil)
	reqCtx := logger.Save(ctx, slog.Default())
	req = req.WithContext(reqCtx)

	w := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, w)
	_ = handler.Handler()(c)
}

// TestObjectLambdaPendingRequestsClearedOnCtxCancel verifies that entries in
// pendingObjectLambdaRequests are removed when the caller's context is cancelled
// before WriteGetObjectResponse is received, preventing the sync.Map from
// growing without bound on client disconnects.
func TestObjectLambdaPendingRequestsClearedOnCtxCancel(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)

	const (
		bucket    = "lambda-leak-test"
		key       = "obj.txt"
		lambdaARN = "arn:aws:lambda:us-east-1:000000000000:function:slow-fn"
	)

	// Create bucket and put an object.
	_, err := backend.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)

	_, err = backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("hello")),
	})
	require.NoError(t, err)

	// invoked is closed once InvokeFunction is called.
	// release is closed to let InvokeFunction return (unblocking the goroutine).
	invoked := make(chan struct{})
	release := make(chan struct{})

	blockingLambda := &blockingLambdaInvoker{
		invoked: invoked,
		release: release,
	}
	targets := &s3.NotificationTargets{LambdaInvoker: blockingLambda}
	handler.SetNotificationDispatcher(s3.NewNotificationDispatcher(targets, "us-east-1"))
	handler.SetObjectLambdaConfig(bucket, lambdaARN)

	// Build a cancellable context for the request.
	reqCtx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Serve GetObject in a goroutine — the handler blocks in the select
	// waiting for the lambda to respond (which it never will).
	done := make(chan struct{})
	go func() {
		defer close(done)
		serveWithContext(reqCtx, handler, http.MethodGet, "/"+bucket+"/"+key)
	}()

	// Wait until the lambda has been invoked (token is now in the sync.Map).
	select {
	case <-invoked:
	case <-time.After(3 * time.Second):
		t.Fatal("lambda was not invoked within timeout")
	}

	assert.Positive(t, handler.PendingObjectLambdaRequestsCount(),
		"pending entry should exist while lambda is in-flight")

	// Cancel the request context — triggers ctx.Done() cleanup in the handler.
	cancel()

	// Release the blocking lambda goroutine so it can exit cleanly.
	close(release)

	// Wait for the handler goroutine to finish.
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler goroutine did not finish after context cancellation")
	}

	assert.Equal(t, 0, handler.PendingObjectLambdaRequestsCount(),
		"pending entry must be removed after context cancellation")
}

// blockingLambdaInvoker is a LambdaInvoker that signals when called and then
// blocks until release is closed. Used to hold the pending request in-flight.
type blockingLambdaInvoker struct {
	invoked chan struct{}
	release chan struct{}
}

func (b *blockingLambdaInvoker) InvokeFunction(_ context.Context, _, _ string, _ []byte) ([]byte, int, error) {
	// Signal that InvokeFunction has been called (once only).
	select {
	case <-b.invoked:
	default:
		close(b.invoked)
	}

	// Block until the test releases us.
	<-b.release

	return nil, 200, nil
}

// TestObjectTagsNotLeakedAfterDelete verifies that deleting an object removes
// its tag entry, bounding tag map growth to the number of live tagged objects.
func TestObjectTagsNotLeakedAfterDelete(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	mustCreateBucket(t, b, "bkt")

	_, err := b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket:  aws.String("bkt"),
		Key:     aws.String("k"),
		Body:    bytes.NewReader([]byte("data")),
		Tagging: aws.String("env=test"),
	})
	require.NoError(t, err)

	require.Positive(t, b.TagsForBucket("bkt"), "tag entry should exist before delete")

	_, err = b.DeleteObject(t.Context(), &sdk_s3.DeleteObjectInput{
		Bucket: aws.String("bkt"),
		Key:    aws.String("k"),
	})
	require.NoError(t, err)

	assert.Equal(t, 0, b.TagsForBucket("bkt"),
		"tag entry must be cleaned up after delete")
}

// TestObjectTagsNotLeakedOnOverwrite verifies that consecutive PutObject calls
// on the same null-version key do not accumulate tag entries.
func TestObjectTagsNotLeakedOnOverwrite(t *testing.T) {
	t.Parallel()

	b := newTestBackend(t)
	mustCreateBucket(t, b, "bkt")

	for range 5 {
		_, err := b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
			Bucket:  aws.String("bkt"),
			Key:     aws.String("k"),
			Body:    bytes.NewReader([]byte("data")),
			Tagging: aws.String("env=test"),
		})
		require.NoError(t, err)
	}

	// One key, one null version — should have exactly 1 tag entry.
	assert.Equal(t, 1, b.TagsForBucket("bkt"),
		"repeated overwrites of the same key must not accumulate tag entries")
}
