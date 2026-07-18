package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestUpdateBucketMetadataTableConfig verifies that PUT
// ?metadataInventoryTableConfiguration and ?metadataJournalTableConfiguration
// persist the config (rather than being silently discarded as a no-op).
func TestUpdateBucketMetadataTableConfig(t *testing.T) {
	t.Parallel()

	t.Run("inventory", func(t *testing.T) {
		t.Parallel()

		const cfgXML = `<InventoryTableConfiguration><Status>Enabled</Status></InventoryTableConfiguration>`

		tests := []struct {
			name     string
			bucket   string
			wantCode int
		}{
			{name: "config_stored", bucket: "bkt", wantCode: http.StatusOK},
			{name: "missing_bucket_404", bucket: "nosuchbucket", wantCode: http.StatusNotFound},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				handler, backend := newTestHandler(t)
				if tt.wantCode != http.StatusNotFound {
					mustCreateBucket(t, backend, tt.bucket)
				}

				req := httptest.NewRequest(
					http.MethodPut,
					"/"+tt.bucket+"?metadataInventoryTableConfiguration",
					strings.NewReader(cfgXML),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, tt.wantCode, rec.Code)
			})
		}
	})

	t.Run("journal", func(t *testing.T) {
		t.Parallel()

		const cfgXML = `<JournalTableConfiguration><Status>Enabled</Status></JournalTableConfiguration>`

		tests := []struct {
			name     string
			bucket   string
			wantCode int
		}{
			{name: "config_stored", bucket: "bkt", wantCode: http.StatusOK},
			{name: "missing_bucket_404", bucket: "nosuchbucket", wantCode: http.StatusNotFound},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				handler, backend := newTestHandler(t)
				if tt.wantCode != http.StatusNotFound {
					mustCreateBucket(t, backend, tt.bucket)
				}

				req := httptest.NewRequest(
					http.MethodPut,
					"/"+tt.bucket+"?metadataJournalTableConfiguration",
					strings.NewReader(cfgXML),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, tt.wantCode, rec.Code)
			})
		}
	})
}

// TestBackendMetadataTableConfigs verifies inventory/journal table config
// storage at the backend level.
func TestBackendMetadataTableConfigs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configType string // "inventory" or "journal"
		putXML     string
		wantErr    bool
		missingBkt bool
	}{
		{
			name:       "inventory_stored",
			configType: "inventory",
			putXML:     `<InventoryTableConfiguration><Status>Enabled</Status></InventoryTableConfiguration>`,
		},
		{
			name:       "journal_stored",
			configType: "journal",
			putXML:     `<JournalTableConfiguration><Status>Enabled</Status></JournalTableConfiguration>`,
		},
		{
			name:       "inventory_missing_bucket_errors",
			configType: "inventory",
			putXML:     `<InventoryTableConfiguration/>`,
			wantErr:    true,
			missingBkt: true,
		},
		{
			name:       "journal_missing_bucket_errors",
			configType: "journal",
			putXML:     `<JournalTableConfiguration/>`,
			wantErr:    true,
			missingBkt: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
			bucketName := "bkt"

			if !tt.missingBkt {
				_, err := backend.CreateBucket(t.Context(),
					&sdk_s3.CreateBucketInput{Bucket: aws.String(bucketName)})
				require.NoError(t, err)
			} else {
				bucketName = "no-such-bucket"
			}

			var err error
			switch tt.configType {
			case "inventory":
				err = backend.UpdateBucketMetadataInventoryTableConfig(
					t.Context(), bucketName, tt.putXML)
			case "journal":
				err = backend.UpdateBucketMetadataJournalTableConfig(
					t.Context(), bucketName, tt.putXML)
			}

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestS3_BucketMetadataConfig(t *testing.T) {
	t.Parallel()

	metadataXML := `<MetadataConfiguration>` +
		`<DestinationBucket>arn:aws:s3:::my-dest</DestinationBucket>` +
		`</MetadataConfiguration>`

	tests := []struct {
		setup      func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend)
		name       string
		method     string
		path       string
		body       string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "CreateBucketMetadataConfiguration stores config",
			method: http.MethodPut,
			path:   "/metadata-bucket?metadataConfiguration",
			body:   metadataXML,
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "metadata-bucket")
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "GetBucketMetadataConfiguration returns stored config",
			method: http.MethodGet,
			path:   "/metadata-bucket?metadataConfiguration",
			setup: func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "metadata-bucket")
				req := httptest.NewRequest(
					http.MethodPut,
					"/metadata-bucket?metadataConfiguration",
					strings.NewReader(metadataXML),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusOK,
			wantBody:   "my-dest",
		},
		{
			name:   "GetBucketMetadataConfiguration returns 404 when not set",
			method: http.MethodGet,
			path:   "/metadata-bucket?metadataConfiguration",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "metadata-bucket")
			},
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchConfiguration",
		},
		{
			name:   "DeleteBucketMetadataConfiguration clears config",
			method: http.MethodDelete,
			path:   "/metadata-bucket?metadataConfiguration",
			setup: func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "metadata-bucket")
				req := httptest.NewRequest(
					http.MethodPut,
					"/metadata-bucket?metadataConfiguration",
					strings.NewReader(metadataXML),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "CreateBucketMetadataConfiguration on missing bucket returns 404",
			method:     http.MethodPut,
			path:       "/no-such-bucket?metadataConfiguration",
			body:       metadataXML,
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchBucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, handler, backend)
			}

			var body *strings.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			} else {
				body = strings.NewReader("")
			}

			req := httptest.NewRequest(tt.method, tt.path, body)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestS3_BucketMetadataTableConfig verifies create/get/delete bucket metadata table configuration.

func TestS3_BucketMetadataTableConfig(t *testing.T) {
	t.Parallel()

	metadataTableXML := `<MetadataTableConfiguration><S3TablesDestination>` +
		`<TableBucketArn>arn:aws:s3tables:::bucket/my-table</TableBucketArn>` +
		`</S3TablesDestination></MetadataTableConfiguration>`

	tests := []struct {
		setup      func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend)
		name       string
		method     string
		path       string
		body       string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "CreateBucketMetadataTableConfiguration stores config",
			method: http.MethodPut,
			path:   "/mt-bucket?metadataTableConfiguration",
			body:   metadataTableXML,
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "mt-bucket")
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "GetBucketMetadataTableConfiguration returns stored config",
			method: http.MethodGet,
			path:   "/mt-bucket?metadataTableConfiguration",
			setup: func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "mt-bucket")
				req := httptest.NewRequest(
					http.MethodPut,
					"/mt-bucket?metadataTableConfiguration",
					strings.NewReader(metadataTableXML),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusOK,
			wantBody:   "my-table",
		},
		{
			name:   "GetBucketMetadataTableConfiguration returns 404 when not set",
			method: http.MethodGet,
			path:   "/mt-bucket?metadataTableConfiguration",
			setup: func(t *testing.T, _ *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "mt-bucket")
			},
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchConfiguration",
		},
		{
			name:   "DeleteBucketMetadataTableConfiguration clears config",
			method: http.MethodDelete,
			path:   "/mt-bucket?metadataTableConfiguration",
			setup: func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "mt-bucket")
				req := httptest.NewRequest(
					http.MethodPut,
					"/mt-bucket?metadataTableConfiguration",
					strings.NewReader(metadataTableXML),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "CreateBucketMetadataTableConfiguration on missing bucket returns 404",
			method:     http.MethodPut,
			path:       "/no-such-bucket?metadataTableConfiguration",
			body:       metadataTableXML,
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchBucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, handler, backend)
			}

			var body *strings.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			} else {
				body = strings.NewReader("")
			}

			req := httptest.NewRequest(tt.method, tt.path, body)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestS3_BucketMetricsConfig verifies put/get/list/delete bucket metrics configuration.
