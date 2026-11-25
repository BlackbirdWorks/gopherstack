package s3_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestObjectReads_InvalidVersionID(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ver-bucket")

	_, err := backend.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
		Bucket:                  aws.String("ver-bucket"),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	require.NoError(t, err)
	mustPutObject(t, backend, "ver-bucket", "k", []byte("v"))

	tests := []struct {
		name       string
		method     string
		path       string
		versionID  string
		wantCode   string
		wantStatus int
	}{
		{"get malformed", http.MethodGet, "/ver-bucket/k", "garbage", "InvalidArgument", 400},
		{"head malformed", http.MethodHead, "/ver-bucket/k", "garbage", "", 400},
		{
			"get absent aws style",
			http.MethodGet,
			"/ver-bucket/k",
			"3HL4kqtJlcpXroDTDmJ.rSpS_GYIzbCs",
			"NoSuchVersion",
			404,
		},
		{"head absent aws style", http.MethodHead, "/ver-bucket/k", "3HL4kqtJlcpXroDTDmJ.rSpS_GYIzbCs", "", 404},
		{"get absent hex", http.MethodGet, "/ver-bucket/k", "0123456789abcdef0123456789abcdef", "NoSuchVersion", 404},
		{"get null absent", http.MethodGet, "/ver-bucket/k", "null", "NoSuchVersion", 404},
		{"get missing key", http.MethodGet, "/ver-bucket/nokey", "garbage", "NoSuchKey", 404},
		{"tagging malformed", http.MethodGet, "/ver-bucket/k?tagging", "garbage", "InvalidArgument", 400},
		{"acl absent", http.MethodGet, "/ver-bucket/k?acl", "0123456789abcdef0123456789abcdef", "NoSuchVersion", 404},
		{"attributes malformed", http.MethodGet, "/ver-bucket/k?attributes", "garbage", "InvalidArgument", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sep := "?"
			if len(tt.path) > 0 && containsQuery(tt.path) {
				sep = "&"
			}

			req := httptest.NewRequest(tt.method, tt.path+sep+"versionId="+tt.versionID, nil)
			if tt.path == "/ver-bucket/k?attributes" {
				req.Header.Set("X-Amz-Object-Attributes", "ETag")
			}

			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCode != "" {
				var errResp s3.ErrorResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantCode, errResp.Code)
			}
		})
	}
}

func containsQuery(p string) bool {
	for _, c := range p {
		if c == '?' {
			return true
		}
	}

	return false
}

func TestObjectReads_EmittedVersionIDsResolve(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "emit-bucket")

	_, err := backend.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
		Bucket:                  aws.String("emit-bucket"),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled},
	})
	require.NoError(t, err)

	out, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("emit-bucket"), Key: aws.String("k"), Body: strings.NewReader("v"),
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/emit-bucket/k?versionId="+aws.ToString(out.VersionId), nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
}
