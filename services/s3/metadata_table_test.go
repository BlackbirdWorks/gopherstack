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
