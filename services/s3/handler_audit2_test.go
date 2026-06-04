package s3_test

// handler_audit2_test.go — AWS-accuracy fixes for S3 batch-2 ops audit (go-14l6):
//   - ListObjects V1: KeyCount must not appear in the response
//   - GetBucketVersioning: Status element omitted for new (never-configured) buckets
//   - ListMultipartUploads: Delimiter echoed in the response body

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// ListObjects V1 — KeyCount must not appear
// ---------------------------------------------------------------------------

// TestBatch2_ListObjectsV1_NoKeyCount verifies that the ListObjects (V1) response
// does not include a KeyCount element. KeyCount is a ListObjectsV2-only field.
func TestBatch2_ListObjectsV1_NoKeyCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		bucket  string
		objects []string
	}{
		{
			name:    "empty bucket: no KeyCount",
			bucket:  "b2kc-empty",
			objects: nil,
		},
		{
			name:    "non-empty bucket: no KeyCount",
			bucket:  "b2kc-full",
			objects: []string{"a/obj1", "b/obj2", "c/obj3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			for _, key := range tt.objects {
				mustPutObject(t, backend, tt.bucket, key, []byte("data"))
			}

			req := httptest.NewRequest(http.MethodGet, "/"+tt.bucket, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, http.StatusOK, rec.Code)
			assert.NotContains(t, rec.Body.String(), "KeyCount",
				"ListObjects V1 response must not contain KeyCount (V2-only field)")
		})
	}
}

// TestBatch2_ListObjectsV2_HasKeyCount verifies that the ListObjectsV2 response
// does include a KeyCount element (confirming the V2 path is unaffected).
func TestBatch2_ListObjectsV2_HasKeyCount(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "b2kc-v2-bkt")
	mustPutObject(t, backend, "b2kc-v2-bkt", "obj1", []byte("data"))

	req := httptest.NewRequest(http.MethodGet, "/b2kc-v2-bkt?list-type=2", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "KeyCount",
		"ListObjectsV2 response must contain KeyCount")
}

// ---------------------------------------------------------------------------
// GetBucketVersioning — Status element omitted for new buckets
// ---------------------------------------------------------------------------

// versioningConfigXML is a minimal XML struct for parsing GetBucketVersioning responses.
type versioningConfigXML struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Status  string   `xml:"Status"`
}

// TestBatch2_GetBucketVersioning_NewBucket_NoStatus verifies that GetBucketVersioning
// returns an empty VersioningConfiguration (no Status element) for buckets that have
// never had versioning configured, matching AWS S3 behaviour.
func TestBatch2_GetBucketVersioning_NewBucket_NoStatus(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "b2ver-new")

	req := httptest.NewRequest(http.MethodGet, "/b2ver-new?versioning", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var cfg versioningConfigXML
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &cfg))
	assert.Empty(t, cfg.Status,
		"new bucket VersioningConfiguration must have empty Status")
	assert.NotContains(t, rec.Body.String(), "<Status>",
		"new bucket VersioningConfiguration must not contain Status element")
}

// TestBatch2_GetBucketVersioning_Configured_HasStatus verifies that GetBucketVersioning
// returns the correct Status after versioning has been explicitly configured.
func TestBatch2_GetBucketVersioning_Configured_HasStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucket     string
		xmlBody    string
		wantStatus string
	}{
		{
			name:       "enabled versioning: Status=Enabled",
			bucket:     "b2ver-enabled",
			xmlBody:    `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`,
			wantStatus: "Enabled",
		},
		{
			name:       "suspended versioning: Status=Suspended",
			bucket:     "b2ver-suspended",
			xmlBody:    `<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`,
			wantStatus: "Suspended",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			putReq := httptest.NewRequest(
				http.MethodPut, "/"+tt.bucket+"?versioning",
				strings.NewReader(tt.xmlBody),
			)
			putRec := httptest.NewRecorder()
			serveS3Handler(handler, putRec, putReq)
			require.Equal(t, http.StatusOK, putRec.Code)

			getReq := httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"?versioning", nil)
			getRec := httptest.NewRecorder()
			serveS3Handler(handler, getRec, getReq)

			require.Equal(t, http.StatusOK, getRec.Code)

			var cfg versioningConfigXML
			require.NoError(t, xml.Unmarshal(getRec.Body.Bytes(), &cfg))
			assert.Equal(t, tt.wantStatus, cfg.Status)
		})
	}
}

// ---------------------------------------------------------------------------
// ListMultipartUploads — Delimiter echoed in response
// ---------------------------------------------------------------------------

// listMPUResultXML is a minimal struct for parsing ListMultipartUploads responses.
type listMPUResultXML struct {
	XMLName   xml.Name `xml:"ListMultipartUploadsResult"`
	Delimiter string   `xml:"Delimiter"`
}

// TestBatch2_ListMultipartUploads_DelimiterEchoed verifies that the Delimiter query
// parameter is echoed in the ListMultipartUploads response body, matching AWS S3.
func TestBatch2_ListMultipartUploads_DelimiterEchoed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		bucket        string
		delimiter     string
		wantDelimiter string
	}{
		{
			name:          "no delimiter: Delimiter element absent",
			bucket:        "b2mu-nodlm",
			delimiter:     "",
			wantDelimiter: "",
		},
		{
			name:          "slash delimiter: echoed in response",
			bucket:        "b2mu-slash",
			delimiter:     "/",
			wantDelimiter: "/",
		},
		{
			name:          "dash delimiter: echoed in response",
			bucket:        "b2mu-dash",
			delimiter:     "-",
			wantDelimiter: "-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			url := "/" + tt.bucket + "?uploads"
			if tt.delimiter != "" {
				url += "&delimiter=" + tt.delimiter
			}

			req := httptest.NewRequest(http.MethodGet, url, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, http.StatusOK, rec.Code)

			var result listMPUResultXML
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
			assert.Equal(t, tt.wantDelimiter, result.Delimiter,
				"ListMultipartUploads must echo the delimiter request parameter")
		})
	}
}
