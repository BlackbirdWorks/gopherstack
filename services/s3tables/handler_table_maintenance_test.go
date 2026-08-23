package s3tables_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_MaintenanceConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		method     string
		pathType   string // "bucket_get", "bucket_put", "table_get", "table_put"
		wantStatus int
	}{
		{
			name:       "get_bucket_maintenance",
			method:     http.MethodGet,
			pathType:   "bucket_get",
			wantStatus: http.StatusOK,
		},
		{
			name:     "put_bucket_maintenance",
			method:   http.MethodPut,
			pathType: "bucket_put",
			body: map[string]any{
				"value": map[string]any{"status": "enabled"},
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:     "reject_bucket_compaction_type",
			method:   http.MethodPut,
			pathType: "bucket_put_compaction",
			body: map[string]any{
				"value": map[string]any{"status": "enabled"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "reject_missing_bucket_value",
			method:     http.MethodPut,
			pathType:   "bucket_put",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "get_table_maintenance",
			method:     http.MethodGet,
			pathType:   "table_get",
			wantStatus: http.StatusOK,
		},
		{
			name:     "put_table_compaction",
			method:   http.MethodPut,
			pathType: "table_put_compaction",
			body: map[string]any{
				"value": map[string]any{"status": "enabled"},
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:     "put_table_snapshot_management",
			method:   http.MethodPut,
			pathType: "table_put_snapshot",
			body: map[string]any{
				"value": map[string]any{"status": "enabled"},
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:     "reject_unreferenced_file_removal_for_table",
			method:   http.MethodPut,
			pathType: "table_put_unreferenced",
			body: map[string]any{
				"value": map[string]any{"status": "enabled"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "reject_missing_table_value",
			method:     http.MethodPut,
			pathType:   "table_put_compaction",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "maint-bucket-"+bucketSuffix(tt.name))
			encodedARN := url.PathEscape(bucketARN)
			createNamespaceHelper(t, h, bucketARN, []string{"maint_ns"})
			_ = createTableHelper(t, h, bucketARN, "maint_ns", "maint_table")

			var path string

			switch tt.pathType {
			case "bucket_get":
				path = "/buckets/" + encodedARN + "/maintenance"
			case "bucket_put":
				path = "/buckets/" + encodedARN + "/maintenance/icebergUnreferencedFileRemoval"
			case "bucket_put_compaction":
				path = "/buckets/" + encodedARN + "/maintenance/icebergCompaction"
			case "table_get":
				path = "/tables/" + encodedARN + "/maint_ns/maint_table/maintenance"
			case "table_put_compaction":
				path = "/tables/" + encodedARN + "/maint_ns/maint_table/maintenance/icebergCompaction"
			case "table_put_snapshot":
				path = "/tables/" + encodedARN + "/maint_ns/maint_table/maintenance/icebergSnapshotManagement"
			case "table_put_unreferenced":
				path = "/tables/" + encodedARN + "/maint_ns/maint_table/maintenance/icebergUnreferencedFileRemoval"
			}

			rec := doS3TablesRequest(t, h, tt.method, path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_Encryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		pathType   string // "bucket" or "table"
		wantStatus int
	}{
		{
			name:       "get_bucket_encryption_returns_not_found",
			pathType:   "bucket",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "get_table_encryption_returns_aes256",
			pathType:   "table",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "enc-bucket-"+bucketSuffix(tt.name))
			encodedARN := url.PathEscape(bucketARN)

			var path string

			if tt.pathType == "bucket" {
				path = "/buckets/" + encodedARN + "/encryption"
			} else {
				createNamespaceHelper(t, h, bucketARN, []string{"enc_ns"})
				_ = createTableHelper(t, h, bucketARN, "enc_ns", "enc_table")
				path = "/tables/" + encodedARN + "/enc_ns/enc_table/encryption"
			}

			rec := doS3TablesRequest(t, h, http.MethodGet, path, nil)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				result := parseResponse(t, rec)
				encCfg, ok := result["encryptionConfiguration"].(map[string]any)
				require.True(t, ok, "expected encryptionConfiguration to be an object")
				assert.Equal(t, "AES256", encCfg["sseAlgorithm"])
			}
		})
	}
}

func TestHandler_GetTableMaintenanceJobStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setup    bool
		wantCode int
	}{
		{
			name:     "existing table",
			setup:    true,
			wantCode: http.StatusOK,
		},
		{
			name:     "missing table",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup {
				bucketARN := createBucketHelper(t, h, "maint-job-bucket")
				createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
				createTableHelper(t, h, bucketARN, "ns1", "t1")

				encodedARN := url.PathEscape(bucketARN)
				rec := doS3TablesRequest(t, h, http.MethodGet,
					"/tables/"+encodedARN+"/ns1/t1/maintenance-job-status", nil)
				assert.Equal(t, tt.wantCode, rec.Code)

				if tt.wantCode == http.StatusOK {
					result := parseResponse(t, rec)
					assert.NotEmpty(t, result["tableARN"])
				}
			} else {
				bucketARN := "arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent"
				encodedARN := url.PathEscape(bucketARN)
				rec := doS3TablesRequest(t, h, http.MethodGet,
					"/tables/"+encodedARN+"/ns1/t1/maintenance-job-status", nil)
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

func TestHandler_GetTableMetadataLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		setMeta  string
		wantMeta string
		wantCode int
		setup    bool
	}{
		{
			name:     "table without metadata location",
			setup:    true,
			wantCode: http.StatusOK,
			wantMeta: "",
		},
		{
			name:     "table with metadata location",
			setup:    true,
			setMeta:  "s3://meta-loc-bucket/ns1/t1/v1.metadata.json",
			wantCode: http.StatusOK,
			wantMeta: "s3://meta-loc-bucket/ns1/t1/v1.metadata.json",
		},
		{
			name:     "missing table",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup {
				bucketARN := createBucketHelper(t, h, "meta-loc-bucket")
				createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
				tableARN := createTableHelper(t, h, bucketARN, "ns1", "t1")

				if tt.setMeta != "" {
					table := getTableHelper(t, h, bucketARN, "ns1", "t1")
					_, err := h.Backend.UpdateTableMetadataLocation(
						bucketARN, []string{"ns1"}, "t1", tt.setMeta, table["versionToken"].(string),
					)
					require.NoError(t, err)
				}

				_ = tableARN
				encodedARN := url.PathEscape(bucketARN)
				rec := doS3TablesRequest(t, h, http.MethodGet,
					"/tables/"+encodedARN+"/ns1/t1/metadata-location", nil)
				assert.Equal(t, tt.wantCode, rec.Code)

				if tt.wantCode == http.StatusOK {
					result := parseResponse(t, rec)
					assert.Equal(t, tt.wantMeta, result["metadataLocation"])
					assert.NotEmpty(t, result["versionToken"])
				}
			} else {
				bucketARN := "arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent"
				encodedARN := url.PathEscape(bucketARN)
				rec := doS3TablesRequest(t, h, http.MethodGet,
					"/tables/"+encodedARN+"/ns1/t1/metadata-location", nil)
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

func TestHandler_MetadataLocationVsUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		metaLocation string
		expectInGet  string
	}{
		{
			name:         "update then get",
			metaLocation: "s3://ml-bucket/ns1/t1/v1.metadata.json",
			expectInGet:  "s3://ml-bucket/ns1/t1/v1.metadata.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, "ml-bucket")
			createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
			createTableHelper(t, h, bucketARN, "ns1", "t1")
			table := getTableHelper(t, h, bucketARN, "ns1", "t1")

			encodedARN := url.PathEscape(bucketARN)

			updateBody, err := json.Marshal(map[string]string{
				"metadataLocation": tt.metaLocation,
				"versionToken":     table["versionToken"].(string),
			})
			require.NoError(t, err)

			rec := doS3TablesRequest(t, h, http.MethodPut,
				"/tables/"+encodedARN+"/ns1/t1/metadata-location",
				json.RawMessage(updateBody))
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doS3TablesRequest(t, h, http.MethodGet,
				"/tables/"+encodedARN+"/ns1/t1/metadata-location", nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			result := parseResponse(t, rec2)
			assert.Equal(t, tt.expectInGet, result["metadataLocation"])
		})
	}
}
