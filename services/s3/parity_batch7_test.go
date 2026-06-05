package s3_test

// parity_batch7_test.go — §3 Single-service B (#19–#23)

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// ---------------------------------------------------------------------------
// #19 — S3 Bucket Replication (cross-bucket async COPY on PutObject)
// ---------------------------------------------------------------------------

func TestS3BucketReplication_PutObjectReplicates(t *testing.T) {
	t.Parallel()

	handler, bk := newTestHandler(t)

	src := "repl-src-bucket"
	dst := "repl-dst-bucket"

	for _, b := range []string{src, dst} {
		req := httptest.NewRequest(http.MethodPut, "/"+b, nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code, "create bucket %s", b)
	}

	cfgXML := fmt.Sprintf(`<ReplicationConfiguration>
<Role>arn:aws:iam::123456789012:role/repl</Role>
<Rule>
  <ID>r1</ID>
  <Status>Enabled</Status>
  <Prefix></Prefix>
  <Destination><Bucket>arn:aws:s3:::%s</Bucket></Destination>
</Rule>
</ReplicationConfiguration>`, dst)

	req := httptest.NewRequest(http.MethodPut, "/"+src+"?replication", strings.NewReader(cfgXML))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// PUT an object into the source bucket.
	body := []byte("hello-replication")
	req = httptest.NewRequest(http.MethodPut, "/"+src+"/test.txt", bytes.NewReader(body))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Allow the async goroutine to run.
	require.Eventually(t, func() bool {
		_, err := bk.GetObject(t.Context(), &sdk_s3.GetObjectInput{
			Bucket: strPtr(dst),
			Key:    strPtr("test.txt"),
		})
		return err == nil
	}, 3*time.Second, 50*time.Millisecond, "replicated object should appear in destination bucket")
}

func TestS3BucketReplication_PrefixFilter(t *testing.T) {
	t.Parallel()

	handler, bk := newTestHandler(t)

	src := "repl-prefix-src"
	dst := "repl-prefix-dst"

	for _, b := range []string{src, dst} {
		req := httptest.NewRequest(http.MethodPut, "/"+b, nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	cfgXML := fmt.Sprintf(`<ReplicationConfiguration>
<Role>arn:aws:iam::123456789012:role/repl</Role>
<Rule>
  <ID>prefix-rule</ID>
  <Status>Enabled</Status>
  <Prefix>images/</Prefix>
  <Destination><Bucket>arn:aws:s3:::%s</Bucket></Destination>
</Rule>
</ReplicationConfiguration>`, dst)

	req := httptest.NewRequest(http.MethodPut, "/"+src+"?replication", strings.NewReader(cfgXML))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		key      string
		wantRepl bool
	}{
		{"images/photo.jpg", true},
		{"documents/report.pdf", false},
	}

	for _, tt := range tests {
		req = httptest.NewRequest(http.MethodPut, "/"+src+"/"+tt.key, strings.NewReader("data"))
		rec = httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	time.Sleep(200 * time.Millisecond) // let goroutines finish

	for _, tt := range tests {
		_, err := bk.GetObject(t.Context(), &sdk_s3.GetObjectInput{
			Bucket: strPtr(dst),
			Key:    strPtr(tt.key),
		})
		if tt.wantRepl {
			assert.NoError(t, err, "key %q should be replicated", tt.key)
		} else {
			assert.Error(t, err, "key %q should NOT be replicated", tt.key)
		}
	}
}

func TestS3BucketReplication_DeleteMarker(t *testing.T) {
	t.Parallel()

	handler, bk := newTestHandler(t)

	src := "repl-del-src"
	dst := "repl-del-dst"

	for _, b := range []string{src, dst} {
		req := httptest.NewRequest(http.MethodPut, "/"+b, nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Enable versioning on both buckets so delete creates a delete marker.
	for _, b := range []string{src, dst} {
		vxml := `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`
		req := httptest.NewRequest(http.MethodPut, "/"+b+"?versioning", strings.NewReader(vxml))
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	cfgXML := fmt.Sprintf(`<ReplicationConfiguration>
<Role>arn:aws:iam::123456789012:role/repl</Role>
<Rule>
  <ID>dmr-rule</ID>
  <Status>Enabled</Status>
  <Prefix></Prefix>
  <DeleteMarkerReplication><Status>Enabled</Status></DeleteMarkerReplication>
  <Destination><Bucket>arn:aws:s3:::%s</Bucket></Destination>
</Rule>
</ReplicationConfiguration>`, dst)

	req := httptest.NewRequest(http.MethodPut, "/"+src+"?replication", strings.NewReader(cfgXML))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Put object in both buckets directly.
	for _, b := range []string{src, dst} {
		req = httptest.NewRequest(http.MethodPut, "/"+b+"/note.txt", strings.NewReader("content"))
		rec = httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Delete from source — should trigger delete-marker replication to dest.
	req = httptest.NewRequest(http.MethodDelete, "/"+src+"/note.txt", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)

	require.Eventually(t, func() bool {
		out, err := bk.GetObject(t.Context(), &sdk_s3.GetObjectInput{
			Bucket: strPtr(dst),
			Key:    strPtr("note.txt"),
		})
		if err != nil {
			return true // key was deleted
		}
		_ = out.Body.Close()
		return false
	}, 3*time.Second, 50*time.Millisecond, "delete marker should propagate to destination")
}

// ---------------------------------------------------------------------------
// #20 — S3 Lambda notification filter (prefix/suffix on CloudFunctionConfiguration)
// ---------------------------------------------------------------------------

func TestLambdaNotificationFilter_PrefixMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		notifXML    string
		key         string
		wantInvoked bool
	}{
		{
			name: "prefix_match",
			notifXML: `<NotificationConfiguration>
<CloudFunctionConfiguration>
  <Id>fn1</Id>
  <CloudFunction>arn:aws:lambda:us-east-1:000000000000:function:my-fn</CloudFunction>
  <Event>s3:ObjectCreated:*</Event>
  <Filter><S3Key><FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule></S3Key></Filter>
</CloudFunctionConfiguration>
</NotificationConfiguration>`,
			key:         "images/photo.jpg",
			wantInvoked: true,
		},
		{
			name: "prefix_no_match",
			notifXML: `<NotificationConfiguration>
<CloudFunctionConfiguration>
  <Id>fn1</Id>
  <CloudFunction>arn:aws:lambda:us-east-1:000000000000:function:my-fn</CloudFunction>
  <Event>s3:ObjectCreated:*</Event>
  <Filter><S3Key><FilterRule><Name>prefix</Name><Value>images/</Value></FilterRule></S3Key></Filter>
</CloudFunctionConfiguration>
</NotificationConfiguration>`,
			key:         "docs/report.pdf",
			wantInvoked: false,
		},
		{
			name: "suffix_match",
			notifXML: `<NotificationConfiguration>
<CloudFunctionConfiguration>
  <Id>fn1</Id>
  <CloudFunction>arn:aws:lambda:us-east-1:000000000000:function:my-fn</CloudFunction>
  <Event>s3:ObjectCreated:*</Event>
  <Filter><S3Key><FilterRule><Name>suffix</Name><Value>.jpg</Value></FilterRule></S3Key></Filter>
</CloudFunctionConfiguration>
</NotificationConfiguration>`,
			key:         "photo.jpg",
			wantInvoked: true,
		},
		{
			name: "suffix_no_match",
			notifXML: `<NotificationConfiguration>
<CloudFunctionConfiguration>
  <Id>fn1</Id>
  <CloudFunction>arn:aws:lambda:us-east-1:000000000000:function:my-fn</CloudFunction>
  <Event>s3:ObjectCreated:*</Event>
  <Filter><S3Key><FilterRule><Name>suffix</Name><Value>.jpg</Value></FilterRule></S3Key></Filter>
</CloudFunctionConfiguration>
</NotificationConfiguration>`,
			key:         "photo.png",
			wantInvoked: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fn := &captureLambdaB7{}
			targets := &s3.NotificationTargets{LambdaInvoker: fn}
			d := s3.NewNotificationDispatcher(targets, "us-east-1")

			d.DispatchObjectCreated(t.Context(), "my-bucket", tt.key, "etag", 42, tt.notifXML)

			fn.mu.Lock()
			defer fn.mu.Unlock()

			if tt.wantInvoked {
				assert.Len(t, fn.calls, 1, "lambda should be invoked for key %q", tt.key)
			} else {
				assert.Empty(t, fn.calls, "lambda should NOT be invoked for key %q", tt.key)
			}
		})
	}
}

type captureLambdaB7 struct {
	calls []string
	mu    sync.Mutex
}

func (c *captureLambdaB7) InvokeFunction(_ context.Context, name, _ string, _ []byte) ([]byte, int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, name)
	return nil, 200, nil
}

// ---------------------------------------------------------------------------
// #23 — S3 Object Lambda WriteGetObjectResponse
// ---------------------------------------------------------------------------

const objectLambdaTransformedContent = "lambda-transformed-content"

func TestS3ObjectLambda_WriteGetObjectResponse(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	bucket := "object-lambda-test-bucket"
	key := "hello.txt"

	// Create bucket and put an object (content doesn't matter; lambda replaces it).
	req := httptest.NewRequest(http.MethodPut, "/"+bucket, nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodPut, "/"+bucket+"/"+key, strings.NewReader("original content"))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Start an HTTP server so the lambda can call WriteGetObjectResponse via HTTP.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveS3Handler(handler, w, r)
	}))
	defer srv.Close()

	handler.Endpoint = srv.URL

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:transformer"
	handler.SetObjectLambdaConfig(bucket, lambdaARN)

	// The lambda ignores the original object and writes back a hardcoded body.
	// This tests the full WriteGetObjectResponse pipeline without recursive GetObject.
	lambdaFn := &staticObjectLambda{serverURL: srv.URL, responseBody: objectLambdaTransformedContent}
	targets := &s3.NotificationTargets{LambdaInvoker: lambdaFn}
	handler.SetNotificationDispatcher(s3.NewNotificationDispatcher(targets, "us-east-1"))

	// GetObject → lambda invoked → WriteGetObjectResponse → response returned to caller.
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"/"+key, nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, objectLambdaTransformedContent, rec.Body.String())
}

// staticObjectLambda is a test LambdaInvoker that calls WriteGetObjectResponse
// with a hardcoded body, bypassing the original object entirely.
type staticObjectLambda struct {
	serverURL    string
	responseBody string
}

func (l *staticObjectLambda) InvokeFunction(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
	var event struct {
		GetObjectContext struct {
			OutputToken string `json:"outputToken"`
		} `json:"getObjectContext"`
	}
	if err := json.Unmarshal(payload, &event); err != nil {
		return nil, 0, err
	}

	wgorURL := l.serverURL + "/?writeGetObjectResponse"
	wgorReq, err := http.NewRequest(http.MethodPost, wgorURL, strings.NewReader(l.responseBody))
	if err != nil {
		return nil, 0, err
	}
	wgorReq.Header.Set("x-amz-request-token", event.GetObjectContext.OutputToken)
	wgorReq.Header.Set("Content-Type", "application/octet-stream")

	wgorResp, err := http.DefaultClient.Do(wgorReq)
	if err != nil {
		return nil, 0, err
	}
	wgorResp.Body.Close()

	return nil, 200, nil
}

// strPtr is a local helper (the test package may already have one; using a
// distinct name to avoid collision).
func strPtr(s string) *string {
	return &s
}
