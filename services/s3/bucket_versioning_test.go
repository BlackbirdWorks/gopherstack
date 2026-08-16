package s3_test

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
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

// TestSuspendedVersioningDeletePreservesVersions verifies that an unversioned
// DELETE against a bucket whose versioning is Suspended only removes the "null"
// version and inserts a "null" delete marker, leaving non-null versions (created
// while versioning was Enabled) retrievable by version ID.
func TestSuspendedVersioningDeletePreservesVersions(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	mustCreateBucket(t, backend, "bkt")

	// Enable versioning and write a non-null version.
	_, err := backend.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
		Bucket: aws.String("bkt"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	putOut, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String("k"),
		Body: bytes.NewReader([]byte("enabled-data")),
	})
	require.NoError(t, err)
	keptVersion := aws.ToString(putOut.VersionId)
	require.NotEqual(t, s3.NullVersion, keptVersion)

	// Suspend versioning and overwrite with a "null" version.
	_, err = backend.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
		Bucket: aws.String("bkt"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusSuspended,
		},
	})
	require.NoError(t, err)
	mustPutObject(t, backend, "bkt", "k", []byte("null-data"))

	// Unversioned DELETE under suspended versioning.
	delOut, err := backend.DeleteObject(t.Context(), &sdk_s3.DeleteObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String("k"),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(delOut.DeleteMarker), "expected a delete marker")
	assert.Equal(t, s3.NullVersion, aws.ToString(delOut.VersionId))

	// The non-null version must still be retrievable by its version ID.
	got, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String("k"),
		VersionId: aws.String(keptVersion),
	})
	require.NoError(t, err)
	data, _ := io.ReadAll(got.Body)
	assert.Equal(t, "enabled-data", string(data))

	// An unversioned GET now sees the delete marker → 404 with x-amz-delete-marker
	// (ErrLatestDeleteMarker renders as NoSuchKey but carries the header).
	_, err = backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String("k"),
	})
	require.ErrorIs(t, err, s3.ErrLatestDeleteMarker)
}

func TestHandler_ObjectLifecycle_Versioned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "head and delete specific version"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			enableVersioning(t, handler, "bkt")
			mustPutObject(t, backend, "bkt", "key", []byte("data"))

			req := httptest.NewRequest(http.MethodGet, "/bkt?versions", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			var res s3.ListVersionsResult
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &res), "Body: %s", rec.Body.String())
			require.NotEmpty(t, res.Versions, "Body: %s", rec.Body.String())

			vid := res.Versions[0].VersionID
			require.NotEmpty(t, vid, "VersionID is empty. Body: %s", rec.Body.String())

			req = httptest.NewRequest(http.MethodHead, "/bkt/key?versionId="+vid, nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, vid, rec.Header().Get("X-Amz-Version-Id"))

			req = httptest.NewRequest(http.MethodDelete, "/bkt/key?versionId="+vid, nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNoContent, rec.Code)
			assert.Equal(t, vid, rec.Header().Get("X-Amz-Version-Id"))
		})
	}
}

func TestHandler_BucketVersioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantConf   *s3.VersioningConfiguration
		name       string
		bucket     string
		xmlBody    string
		wantStatus int
	}{
		{
			name:       "put and get versioning",
			bucket:     "bkt",
			xmlBody:    `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`,
			wantStatus: http.StatusOK,
			wantConf:   &s3.VersioningConfiguration{Status: "Enabled"},
		},
		{
			name:       "non-existent bucket returns 404",
			bucket:     "no-bucket",
			xmlBody:    `<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid XML returns 400",
			bucket:     "bkt",
			xmlBody:    "not xml",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.bucket == "bkt" {
				mustCreateBucket(t, backend, "bkt")
			}

			req := httptest.NewRequest(
				http.MethodPut,
				"/"+tt.bucket+"?versioning",
				strings.NewReader(tt.xmlBody),
			)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantConf != nil {
				req = httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"?versioning", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)

				var got s3.VersioningConfiguration
				require.NoError(t, xml.NewDecoder(rec.Body).Decode(&got))

				if diff := cmp.Diff(
					*tt.wantConf, got,
					cmpopts.IgnoreFields(s3.VersioningConfiguration{}, "XMLName"),
				); diff != "" {
					assert.Empty(t, diff, "VersioningConfiguration mismatch")
				}
			}
		})
	}
}

func TestHandler_VersionedObjectLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "versioned object full lifecycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			enableVersioning(t, handler, "bkt")

			req := httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader("v1 data"))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			v1ID := rec.Header().Get("X-Amz-Version-Id")
			assert.NotEmpty(t, v1ID)
			assert.NotEqual(t, s3.NullVersion, v1ID)

			req = httptest.NewRequest(http.MethodPut, "/bkt/key", strings.NewReader("v2 data"))
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			v2ID := rec.Header().Get("X-Amz-Version-Id")
			assert.NotEmpty(t, v2ID)
			assert.NotEqual(t, v1ID, v2ID)

			req = httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			body, _ := io.ReadAll(rec.Body)
			assert.Equal(t, "v2 data", string(body))

			req = httptest.NewRequest(http.MethodGet, "/bkt/key?versionId="+v1ID, nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			body, _ = io.ReadAll(rec.Body)
			assert.Equal(t, "v1 data", string(body))

			req = httptest.NewRequest(http.MethodDelete, "/bkt/key", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusNoContent, rec.Code)
			assert.Equal(t, "true", rec.Header().Get("X-Amz-Delete-Marker"))
			dmID := rec.Header().Get("X-Amz-Version-Id")
			assert.NotEmpty(t, dmID)

			req = httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusNotFound, rec.Code)

			req = httptest.NewRequest(http.MethodGet, "/bkt/key?versionId="+v1ID, nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
		})
	}
}
