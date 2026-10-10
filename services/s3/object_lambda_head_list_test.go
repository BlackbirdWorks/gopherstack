package s3_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

type recordingObjectLambda struct {
	response string
	events   []map[string]any
	mu       sync.Mutex
}

func (l *recordingObjectLambda) InvokeFunction(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
	var ev map[string]any
	_ = json.Unmarshal(payload, &ev)

	l.mu.Lock()
	l.events = append(l.events, ev)
	l.mu.Unlock()

	return []byte(l.response), http.StatusOK, nil
}

func TestS3ObjectLambda_HeadAndListRouting(t *testing.T) {
	t.Parallel()

	const (
		bucket  = "olap-head-list-bucket"
		account = "123456789012"
		alias   = "hl-abcd1234--ol-s3"
	)

	tests := []struct {
		wantHeader  [2]string
		name        string
		method      string
		path        string
		response    string
		wantContext string
		wantBody    string
		actions     []string
		wantCode    int
	}{
		{
			name: "head", method: http.MethodHead, path: "/" + alias + "/hello.txt",
			actions: []string{"HeadObject"},
			response: `{"statusCode":200,"headers":{"Content-Length":5,"x-amz-delete-marker":false,` +
				`"x-amz-meta-k":"v"}}`,
			wantContext: "headObjectContext", wantCode: http.StatusOK, wantHeader: [2]string{"X-Amz-Meta-K", "v"},
		},
		{
			name: "head error", method: http.MethodHead, path: "/" + alias + "/hello.txt",
			actions:     []string{"HeadObject"},
			response:    `{"statusCode":403,"errorCode":"Denied"}`,
			wantContext: "headObjectContext", wantCode: http.StatusForbidden,
		},
		{
			name: "list v1 xml", method: http.MethodGet, path: "/" + alias + "?prefix=a",
			actions:     []string{"ListObjects"},
			response:    `{"statusCode":200,"listResultXml":"<ListBucketResult><Name>x</Name></ListBucketResult>"}`,
			wantContext: "listObjectsContext", wantCode: http.StatusOK, wantBody: "<Name>x</Name>",
		},
		{
			name: "list v2 structured", method: http.MethodGet, path: "/" + alias + "?list-type=2",
			actions: []string{"ListObjectsV2"},
			response: `{"statusCode":200,"listBucketResult":{"name":"n","keyCount":1,"maxKeys":1000,` +
				`"contents":[{"key":"k1","size":3}]}}`,
			wantContext: "listObjectsV2Context", wantCode: http.StatusOK, wantBody: "<Key>k1</Key>",
		},
		{
			name: "list error", method: http.MethodGet, path: "/" + alias + "?list-type=2",
			actions:     []string{"ListObjectsV2"},
			response:    `{"statusCode":404,"errorCode":"NoSuchBucket","errorMessage":"gone"}`,
			wantContext: "listObjectsV2Context", wantCode: http.StatusNotFound, wantBody: "<Code>NoSuchBucket</Code>",
		},
		{
			name: "invalid lambda output", method: http.MethodGet, path: "/" + alias + "?list-type=2",
			actions:     []string{"ListObjectsV2"},
			response:    `not json`,
			wantContext: "listObjectsV2Context", wantCode: http.StatusInternalServerError,
		},
		{
			name: "action not configured passes through", method: http.MethodHead, path: "/" + alias + "/hello.txt",
			actions:  []string{"GetObject"},
			response: `{"statusCode":418}`, wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, _ := newTestHandler(t)

			for _, req := range []*http.Request{
				httptest.NewRequest(http.MethodPut, "/"+bucket, nil),
				httptest.NewRequest(http.MethodPut, "/"+bucket+"/hello.txt", strings.NewReader("hello")),
			} {
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			fn := &recordingObjectLambda{response: tt.response}
			handler.SetNotificationDispatcher(s3.NewNotificationDispatcher(
				&s3.NotificationTargets{LambdaInvoker: fn}, "us-east-1"))
			handler.Endpoint = "localhost"
			handler.SetObjectLambdaAccessPoint(bucket, s3.StoredObjectLambdaAccessPoint{
				Name: "hl", AccountID: account, Alias: alias, Actions: tt.actions,
				LambdaARN:                "arn:aws:lambda:us-east-1:000000000000:function:f",
				SupportingAccessPointARN: "arn:aws:s3:us-east-1:123456789012:accesspoint/base",
				Payload:                  `{"k":"v"}`,
			})

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)

			if tt.wantHeader[0] != "" {
				assert.Equal(t, tt.wantHeader[1], rec.Header().Get(tt.wantHeader[0]))
			}

			fn.mu.Lock()
			defer fn.mu.Unlock()

			if tt.wantContext == "" {
				assert.Empty(t, fn.events)

				return
			}

			require.Len(t, fn.events, 1)
			ev := fn.events[0]
			assert.Contains(t, ev, tt.wantContext)
			assert.Equal(t, "1.00", ev["protocolVersion"])
			assert.NotEmpty(t, ev["xAmzRequestId"])

			ctxBlock, ok := ev[tt.wantContext].(map[string]any)
			require.True(t, ok)
			assert.Contains(t, ctxBlock["inputS3Url"], bucket)

			cfg, ok := ev["configuration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "arn:aws:s3-object-lambda:us-east-1:123456789012:accesspoint/hl", cfg["accessPointArn"])
			assert.JSONEq(t, `{"k":"v"}`, cfg["payload"].(string))
			assert.Contains(t, ev, "userRequest")
		})
	}
}
