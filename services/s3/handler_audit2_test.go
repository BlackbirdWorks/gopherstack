package s3_test

// handler_audit2_test.go — AWS-accuracy fixes for S3 batch-2 ops audit (go-14l6):
//   - ListObjects V1: KeyCount must not appear in the response
//   - GetBucketVersioning: Status element omitted for new (never-configured) buckets
//   - ListMultipartUploads: Delimiter echoed in the response body

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
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
		objects []string
	}{
		{
			name:    "empty bucket: no KeyCount",
			objects: nil,
		},
		{
			name:    "non-empty bucket: no KeyCount",
			objects: []string{"a/obj1", "b/obj2", "c/obj3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "b2kc-bkt")

			for _, key := range tt.objects {
				mustPutObject(t, backend, "b2kc-bkt", key, []byte("data"))
			}

			req := httptest.NewRequest(http.MethodGet, "/b2kc-bkt", nil)
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

// versioningConfigurationXML is a minimal XML struct for parsing GetBucketVersioning responses.
type versioningConfigurationXML struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Status  string   `xml:"Status"`
}

// TestBatch2_GetBucketVersioning_NewBucket_NoStatus verifies that GetBucketVersioning
// returns an empty VersioningConfiguration (no Status element) for buckets that have
// never had versioning configured, matching AWS S3 behaviour.
func TestBatch2_GetBucketVersioning_NewBucket_NoStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupExtra func(t *testing.T, handler interface{}, bucket string)
		wantStatus string
	}{
		{
			name:       "new bucket: Status element absent",
			setupExtra: nil,
			wantStatus: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "b2ver-new")

			req := httptest.NewRequest(http.MethodGet, "/b2ver-new?versioning", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, http.StatusOK, rec.Code)

			var cfg versioningConfigurationXML
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &cfg))
			assert.Equal(t, tt.wantStatus, cfg.Status,
				"new bucket VersioningConfiguration must have empty Status")
			assert.NotContains(t, rec.Body.String(), "<Status>",
				"new bucket VersioningConfiguration must not contain Status element")
		})
	}
}

// TestBatch2_GetBucketVersioning_Enabled_HasStatus verifies that GetBucketVersioning
// returns Status=Enabled after versioning is enabled.
func TestBatch2_GetBucketVersioning_Enabled_HasStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		xmlBody    string
		wantStatus string
	}{
		{
			name:       "enabled versioning: Status=Enabled",
			xmlBody:    `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`,
			wantStatus: "Enabled",
		},
		{
			name:       "suspended versioning: Status=Suspended",
			xmlBody:    `<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>`,
			wantStatus: "Suspended",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			bucket := "b2ver-cfg-" + tt.wantStatus
			mustCreateBucket(t, backend, bucket)

			// Set versioning status via the handler.
			putReq := httptest.NewRequest(
				http.MethodPut, "/"+bucket+"?versioning",
				stringReader(tt.xmlBody),
			)
			putRec := httptest.NewRecorder()
			serveS3Handler(handler, putRec, putReq)
			require.Equal(t, http.StatusOK, putRec.Code)

			getReq := httptest.NewRequest(http.MethodGet, "/"+bucket+"?versioning", nil)
			getRec := httptest.NewRecorder()
			serveS3Handler(handler, getRec, getReq)

			require.Equal(t, http.StatusOK, getRec.Code)

			var cfg versioningConfigurationXML
			require.NoError(t, xml.Unmarshal(getRec.Body.Bytes(), &cfg))
			assert.Equal(t, tt.wantStatus, cfg.Status)
		})
	}
}

// ---------------------------------------------------------------------------
// ListMultipartUploads — Delimiter echoed in response
// ---------------------------------------------------------------------------

// listMultipartUploadsResultXML is a minimal struct for parsing ListMultipartUploads responses.
type listMultipartUploadsResultXML struct {
	XMLName   xml.Name `xml:"ListMultipartUploadsResult"`
	Delimiter string   `xml:"Delimiter"`
}

// TestBatch2_ListMultipartUploads_DelimiterEchoed verifies that the Delimiter query
// parameter is echoed in the ListMultipartUploads response body, matching AWS S3.
func TestBatch2_ListMultipartUploads_DelimiterEchoed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		delimiter     string
		wantDelimiter string
	}{
		{
			name:          "no delimiter: Delimiter element absent",
			delimiter:     "",
			wantDelimiter: "",
		},
		{
			name:          "slash delimiter: echoed in response",
			delimiter:     "/",
			wantDelimiter: "/",
		},
		{
			name:          "dash delimiter: echoed in response",
			delimiter:     "-",
			wantDelimiter: "-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "b2mu-bkt")

			url := "/b2mu-bkt?uploads"
			if tt.delimiter != "" {
				url += "&delimiter=" + tt.delimiter
			}

			req := httptest.NewRequest(http.MethodGet, url, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, http.StatusOK, rec.Code)

			var result listMultipartUploadsResultXML
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
			assert.Equal(t, tt.wantDelimiter, result.Delimiter,
				"ListMultipartUploads must echo the delimiter request parameter")
		})
	}
}

// stringReader is a helper for creating an io.Reader from a string inline.
func stringReader(s string) *stringReaderImpl {
	return &stringReaderImpl{s: s}
}

type stringReaderImpl struct {
	s   string
	pos int
}

func (r *stringReaderImpl) Read(p []byte) (int, error) {
	if r.pos >= len(r.s) {
		return 0, nil
	}
	n := copy(p, r.s[r.pos:])
	r.pos += n

	return n, nil
}
