package s3tables_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_InputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		notContain string
		wantCode   int
	}{
		{
			name: "bucket policy not json", method: http.MethodPut, path: "/buckets/{b}/policy",
			body: map[string]any{"resourcePolicy": "notjson"}, wantCode: http.StatusBadRequest,
		},
		{
			name: "bucket encryption bad algorithm", method: http.MethodPut, path: "/buckets/{b}/encryption",
			body:     map[string]any{"encryptionConfiguration": map[string]any{"sseAlgorithm": "bogus"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "bucket encryption kms needs key", method: http.MethodPut, path: "/buckets/{b}/encryption",
			body:     map[string]any{"encryptionConfiguration": map[string]any{"sseAlgorithm": "aws:kms"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "bucket maintenance bad status", method: http.MethodPut,
			path:     "/buckets/{b}/maintenance/icebergUnreferencedFileRemoval",
			body:     map[string]any{"value": map[string]any{"status": "bogus"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "list buckets limit too big", method: http.MethodGet, path: "/buckets?maxBuckets=1001",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "list buckets limit zero", method: http.MethodGet, path: "/buckets?maxBuckets=0",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "list buckets bad type", method: http.MethodGet, path: "/buckets?type=bogus",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "duplicate bucket has no code prefix", method: http.MethodPut, path: "/buckets",
			body: map[string]any{"name": "val-bucket"}, wantCode: http.StatusConflict,
			notContain: "ConflictException:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arn := createBucketHelper(t, h, "val-bucket")
			path := strings.ReplaceAll(tt.path, "{b}", url.PathEscape(arn))

			rec := doS3TablesRequest(t, h, tt.method, path, tt.body)
			require.Equal(t, tt.wantCode, rec.Code, rec.Body.String())

			if tt.notContain != "" {
				assert.NotContains(t, rec.Body.String(), tt.notContain)
			}
		})
	}
}
