package s3_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// versioningConfigXML is a minimal XML struct for parsing GetBucketVersioning responses.
type versioningConfigXML struct {
	XMLName xml.Name `xml:"VersioningConfiguration"`
	Status  string   `xml:"Status"`
}

// TestGetBucketVersioning_NewBucket_NoStatus verifies that GetBucketVersioning
// returns an empty VersioningConfiguration (no Status element) for buckets that
// have never had versioning configured, matching AWS S3 behaviour.
func TestGetBucketVersioning_NewBucket_NoStatus(t *testing.T) {
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

// TestGetBucketVersioning_Configured_HasStatus verifies that GetBucketVersioning
// returns the correct Status after versioning has been explicitly configured.
func TestGetBucketVersioning_Configured_HasStatus(t *testing.T) {
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
