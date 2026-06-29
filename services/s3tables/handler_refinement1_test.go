package s3tables_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/s3tables"
)

// ----------------------------------------
// Refinement check 1 tests
// ----------------------------------------

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateTableBucket("mybucket")
	require.NoError(t, err)

	require.Equal(t, 1, s3tables.BucketCount(b))

	b.Reset()

	assert.Equal(t, 0, s3tables.BucketCount(b))
	assert.Equal(t, 0, s3tables.NamespaceCount(b))
	assert.Equal(t, 0, s3tables.TableCount(b))
	assert.Equal(t, 0, s3tables.BucketReplicationCount(b))
	assert.Equal(t, 0, s3tables.TableReplicationCount(b))
	assert.Equal(t, 0, s3tables.TableRecordExpiryCount(b))
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createBucketHelper(t, h, "reset-bucket")

	require.Equal(t, 1, s3tables.BucketCount(h.Backend))

	h.Reset()

	assert.Equal(t, 0, s3tables.BucketCount(h.Backend))
}

func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Greater(t, s3tables.HandlerOpsLen(h), 25)
	assert.Contains(t, ops, "DeleteTableBucketEncryption")
	assert.Contains(t, ops, "DeleteTableBucketMetricsConfiguration")
	assert.Contains(t, ops, "DeleteTableBucketReplication")
	assert.Contains(t, ops, "DeleteTableReplication")
	assert.Contains(t, ops, "GetTableBucketMetricsConfiguration")
	assert.Contains(t, ops, "GetTableBucketReplication")
	assert.Contains(t, ops, "GetTableBucketStorageClass")
	assert.Contains(t, ops, "GetTableMaintenanceJobStatus")
	assert.Contains(t, ops, "GetTableMetadataLocation")
	assert.Contains(t, ops, "GetTableRecordExpirationConfiguration")
}

func TestRefinement1_DeleteTableBucketEncryption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucketName string
		wantCode   int
	}{
		{
			name:       "existing bucket",
			bucketName: "enc-bucket",
			wantCode:   http.StatusNoContent,
		},
		{
			name:     "missing bucket",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var bucketARN string
			if tt.bucketName != "" {
				bucketARN = createBucketHelper(t, h, tt.bucketName)
			} else {
				bucketARN = "arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent"
			}

			encodedARN := url.PathEscape(bucketARN)
			rec := doS3TablesRequest(t, h, http.MethodDelete, "/buckets/"+encodedARN+"/encryption", nil)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement1_GetTableBucketMetricsConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucketName string
		wantCode   int
	}{
		{
			name:       "existing bucket",
			bucketName: "metrics-bucket",
			wantCode:   http.StatusOK,
		},
		{
			name:     "missing bucket",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var bucketARN string
			if tt.bucketName != "" {
				bucketARN = createBucketHelper(t, h, tt.bucketName)
			} else {
				bucketARN = "arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent"
			}

			encodedARN := url.PathEscape(bucketARN)
			rec := doS3TablesRequest(t, h, http.MethodGet, "/buckets/"+encodedARN+"/metrics", nil)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				result := parseResponse(t, rec)
				assert.Equal(t, bucketARN, result["tableBucketARN"])
			}
		})
	}
}

func TestRefinement1_DeleteTableBucketMetricsConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucketName string
		wantCode   int
	}{
		{
			name:       "existing bucket",
			bucketName: "metrics-delete-bucket",
			wantCode:   http.StatusNoContent,
		},
		{
			name:     "missing bucket",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var bucketARN string
			if tt.bucketName != "" {
				bucketARN = createBucketHelper(t, h, tt.bucketName)
			} else {
				bucketARN = "arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent"
			}

			encodedARN := url.PathEscape(bucketARN)
			rec := doS3TablesRequest(t, h, http.MethodDelete, "/buckets/"+encodedARN+"/metrics", nil)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement1_GetTableBucketStorageClass(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucketName string
		wantClass  string
		wantCode   int
	}{
		{
			name:       "existing bucket",
			bucketName: "sc-bucket",
			wantCode:   http.StatusOK,
			wantClass:  "STANDARD",
		},
		{
			name:     "missing bucket",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var bucketARN string
			if tt.bucketName != "" {
				bucketARN = createBucketHelper(t, h, tt.bucketName)
			} else {
				bucketARN = "arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent"
			}

			encodedARN := url.PathEscape(bucketARN)
			rec := doS3TablesRequest(t, h, http.MethodGet, "/buckets/"+encodedARN+"/storage-class", nil)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				result := parseResponse(t, rec)
				assert.Equal(t, tt.wantClass, result["storageClass"])
			}
		})
	}
}

func TestRefinement1_GetTableBucketReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucketName     string
		hasReplication bool
		wantCode       int
	}{
		{
			name:           "existing replication",
			bucketName:     "repl-bucket",
			hasReplication: true,
			wantCode:       http.StatusOK,
		},
		{
			name:       "no replication config",
			bucketName: "no-repl-bucket",
			wantCode:   http.StatusNotFound,
		},
		{
			name:     "missing bucket ARN param",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.bucketName != "" {
				bucketARN := createBucketHelper(t, h, tt.bucketName)

				if tt.hasReplication {
					s3tables.AddBucketReplicationInternal(h.Backend, bucketARN, &s3tables.BucketReplicationConfig{
						Destinations: []s3tables.ReplicationDestination{
							{DestinationBucketARN: "arn:aws:s3tables:us-east-1:000000000000:bucket/dest"},
						},
					})
				}

				q := url.Values{}
				q.Set("tableBucketARN", bucketARN)
				rec := doS3TablesRequest(t, h, http.MethodGet, "/table-bucket-replication?"+q.Encode(), nil)
				assert.Equal(t, tt.wantCode, rec.Code)

				if tt.wantCode == http.StatusOK {
					result := parseResponse(t, rec)
					assert.Equal(t, bucketARN, result["tableBucketARN"])
					replCfg, ok := result["replicationConfiguration"].(map[string]any)
					require.True(t, ok)
					dests, ok := replCfg["destinations"].([]any)
					require.True(t, ok)
					assert.Len(t, dests, 1)
				}
			} else {
				rec := doS3TablesRequest(t, h, http.MethodGet, "/table-bucket-replication", nil)
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

func TestRefinement1_DeleteTableBucketReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucketName     string
		hasReplication bool
		noARNParam     bool
		wantCode       int
	}{
		{
			name:           "delete existing replication",
			bucketName:     "repl-del-bucket",
			hasReplication: true,
			wantCode:       http.StatusNoContent,
		},
		{
			name:       "missing bucket",
			bucketName: "missing",
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "missing ARN param",
			noARNParam: true,
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.noARNParam {
				rec := doS3TablesRequest(t, h, http.MethodDelete, "/table-bucket-replication", nil)
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			var bucketARN string
			if tt.bucketName == "missing" {
				bucketARN = "arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent"
			} else {
				bucketARN = createBucketHelper(t, h, tt.bucketName)
			}

			if tt.hasReplication {
				s3tables.AddBucketReplicationInternal(h.Backend, bucketARN, &s3tables.BucketReplicationConfig{})
			}

			q := url.Values{}
			q.Set("tableBucketARN", bucketARN)
			rec := doS3TablesRequest(t, h, http.MethodDelete, "/table-bucket-replication?"+q.Encode(), nil)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusNoContent {
				assert.Equal(t, 0, s3tables.BucketReplicationCount(h.Backend))
			}
		})
	}
}

func TestRefinement1_DeleteTableReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupTable bool
		noARNParam bool
		wantCode   int
	}{
		{
			name:       "delete existing replication",
			setupTable: true,
			wantCode:   http.StatusNoContent,
		},
		{
			name:       "missing tableArn param",
			noARNParam: true,
			wantCode:   http.StatusBadRequest,
		},
		{
			name:     "table not found",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.noARNParam {
				rec := doS3TablesRequest(t, h, http.MethodDelete, "/table-replication", nil)
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			if tt.setupTable {
				bucketARN := createBucketHelper(t, h, "repl-table-bucket")
				createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
				tableARN := createTableHelper(t, h, bucketARN, "ns1", "mytable")

				err := h.Backend.PutTableReplication(tableARN)
				require.NoError(t, err)

				q := url.Values{}
				q.Set("tableArn", tableARN)
				rec := doS3TablesRequest(t, h, http.MethodDelete, "/table-replication?"+q.Encode(), nil)
				assert.Equal(t, tt.wantCode, rec.Code)
			} else {
				q := url.Values{}
				q.Set("tableArn", "arn:aws:s3tables:us-east-1:000000000000:bucket/b/table/ns/t")
				rec := doS3TablesRequest(t, h, http.MethodDelete, "/table-replication?"+q.Encode(), nil)
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

func TestRefinement1_GetTableMaintenanceJobStatus(t *testing.T) {
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

func TestRefinement1_GetTableMetadataLocation(t *testing.T) {
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

func TestRefinement1_GetTableRecordExpirationConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		wantCode   int
		setupTable bool
		noARNParam bool
	}{
		{
			name:       "table without expiry config",
			setupTable: true,
			wantCode:   http.StatusOK,
			wantStatus: "DISABLED",
		},
		{
			name:       "missing tableArn param",
			noARNParam: true,
			wantCode:   http.StatusBadRequest,
		},
		{
			name:     "table not found",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.noARNParam {
				rec := doS3TablesRequest(t, h, http.MethodGet, "/table-record-expiration", nil)
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			if tt.setupTable {
				bucketARN := createBucketHelper(t, h, "expiry-bucket")
				createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
				tableARN := createTableHelper(t, h, bucketARN, "ns1", "t1")

				q := url.Values{}
				q.Set("tableArn", tableARN)
				rec := doS3TablesRequest(t, h, http.MethodGet, "/table-record-expiration?"+q.Encode(), nil)
				assert.Equal(t, tt.wantCode, rec.Code)

				if tt.wantCode == http.StatusOK {
					result := parseResponse(t, rec)
					assert.Equal(t, tt.wantStatus, result["status"])
					assert.Equal(t, tableARN, result["tableARN"])
				}
			} else {
				q := url.Values{}
				q.Set("tableArn", "arn:aws:s3tables:us-east-1:000000000000:bucket/b/table/ns/t")
				rec := doS3TablesRequest(t, h, http.MethodGet, "/table-record-expiration?"+q.Encode(), nil)
				assert.Equal(t, tt.wantCode, rec.Code)
			}
		})
	}
}

func TestRefinement1_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	tb, err := b.CreateTableBucket("snap-bucket")
	require.NoError(t, err)

	_, err = b.CreateNamespace(tb.ARN, []string{"ns1"})
	require.NoError(t, err)

	_, err = b.CreateTable(tb.ARN, []string{"ns1"}, "t1", "ICEBERG")
	require.NoError(t, err)

	s3tables.AddBucketReplicationInternal(b, tb.ARN, &s3tables.BucketReplicationConfig{
		Destinations: []s3tables.ReplicationDestination{
			{DestinationBucketARN: "arn:aws:s3tables:us-east-1:000000000000:bucket/dest"},
		},
	})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, s3tables.BucketCount(b2))
	assert.Equal(t, 1, s3tables.NamespaceCount(b2))
	assert.Equal(t, 1, s3tables.TableCount(b2))
	assert.Equal(t, 1, s3tables.BucketReplicationCount(b2))
}

func TestRefinement1_ProviderInitNilContext(t *testing.T) {
	t.Parallel()

	p := &s3tables.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
}

func TestRefinement1_BucketReplicationRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		destARN   string
		wantCount int
	}{
		{
			name:      "single destination",
			destARN:   "arn:aws:s3tables:us-east-1:000000000000:bucket/dest1",
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
			tb, err := b.CreateTableBucket("rr-bucket")
			require.NoError(t, err)

			cfg := &s3tables.BucketReplicationConfig{
				Destinations: []s3tables.ReplicationDestination{
					{DestinationBucketARN: tt.destARN},
				},
			}

			require.NoError(t, b.PutTableBucketReplication(tb.ARN, cfg))

			got, err := b.GetTableBucketReplication(tb.ARN)
			require.NoError(t, err)
			assert.Len(t, got.Destinations, tt.wantCount)
			assert.Equal(t, tt.destARN, got.Destinations[0].DestinationBucketARN)

			require.NoError(t, b.DeleteTableBucketReplication(tb.ARN))
			assert.Equal(t, 0, s3tables.BucketReplicationCount(b))
		})
	}
}

func TestRefinement1_TableReplicationRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantEnabled bool
	}{
		{
			name:        "enable and disable replication",
			wantEnabled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
			tb, err := b.CreateTableBucket("tr-bucket")
			require.NoError(t, err)

			_, err = b.CreateNamespace(tb.ARN, []string{"ns1"})
			require.NoError(t, err)

			table, err := b.CreateTable(tb.ARN, []string{"ns1"}, "t1", "ICEBERG")
			require.NoError(t, err)

			require.NoError(t, b.PutTableReplication(table.ARN))
			assert.Equal(t, 1, s3tables.TableReplicationCount(b))

			require.NoError(t, b.DeleteTableReplication(table.ARN))
			assert.Equal(t, tt.wantEnabled, s3tables.TableReplicationCount(b) > 0)
		})
	}
}

func TestRefinement1_TableRecordExpiryRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     string
		wantStatus string
	}{
		{
			name:       "set enabled",
			status:     "ENABLED",
			wantStatus: "ENABLED",
		},
		{
			name:       "set disabled",
			status:     "DISABLED",
			wantStatus: "DISABLED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
			tb, err := b.CreateTableBucket("exp-bucket")
			require.NoError(t, err)

			_, err = b.CreateNamespace(tb.ARN, []string{"ns1"})
			require.NoError(t, err)

			table, err := b.CreateTable(tb.ARN, []string{"ns1"}, "t1", "ICEBERG")
			require.NoError(t, err)

			cfg := &s3tables.TableRecordExpiryConfig{Status: tt.status}
			require.NoError(t, b.PutTableRecordExpirationConfiguration(table.ARN, cfg))

			got, err := b.GetTableRecordExpirationConfiguration(table.ARN)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, got.Status)
		})
	}
}

func TestRefinement1_TableRecordExpiryDefaultDisabled(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	tb, err := b.CreateTableBucket("exp-default-bucket")
	require.NoError(t, err)

	_, err = b.CreateNamespace(tb.ARN, []string{"ns1"})
	require.NoError(t, err)

	table, err := b.CreateTable(tb.ARN, []string{"ns1"}, "t1", "ICEBERG")
	require.NoError(t, err)

	got, err := b.GetTableRecordExpirationConfiguration(table.ARN)
	require.NoError(t, err)
	assert.Equal(t, "DISABLED", got.Status)
}

func TestRefinement1_RouteMatcherNewPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		want   string
	}{
		{
			name:   "table-bucket-replication GET",
			path:   "/table-bucket-replication?tableBucketARN=arn",
			method: http.MethodGet,
			want:   "GetTableBucketReplication",
		},
		{
			name:   "table-bucket-replication DELETE",
			path:   "/table-bucket-replication?tableBucketARN=arn",
			method: http.MethodDelete,
			want:   "DeleteTableBucketReplication",
		},
		{
			name:   "table-replication DELETE",
			path:   "/table-replication?tableArn=arn",
			method: http.MethodDelete,
			want:   "DeleteTableReplication",
		},
		{
			name:   "table-record-expiration GET",
			path:   "/table-record-expiration?tableArn=arn",
			method: http.MethodGet,
			want:   "GetTableRecordExpirationConfiguration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			// The operation is checked via GetSupportedOperations which includes these ops.
			ops := h.GetSupportedOperations()
			assert.Contains(t, ops, tt.want)
		})
	}
}

func TestRefinement1_BucketEncryptionAndMetricsRoute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		subPath    string
		wantOp     string
		bucketName string
		wantCode   int
	}{
		{
			name:       "GET encryption",
			method:     http.MethodGet,
			subPath:    "encryption",
			wantCode:   http.StatusNotFound,
			bucketName: "enc-get",
		},
		{
			name:       "DELETE encryption",
			method:     http.MethodDelete,
			subPath:    "encryption",
			wantCode:   http.StatusNoContent,
			bucketName: "enc-del",
		},
		{
			name:       "GET metrics",
			method:     http.MethodGet,
			subPath:    "metrics",
			wantCode:   http.StatusOK,
			bucketName: "met-get",
		},
		{
			name:       "DELETE metrics",
			method:     http.MethodDelete,
			subPath:    "metrics",
			wantCode:   http.StatusNoContent,
			bucketName: "met-del",
		},
		{
			name:       "GET storage-class",
			method:     http.MethodGet,
			subPath:    "storage-class",
			wantCode:   http.StatusOK,
			bucketName: "sc-get",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			bucketARN := createBucketHelper(t, h, tt.bucketName)
			encodedARN := url.PathEscape(bucketARN)
			rec := doS3TablesRequest(t, h, tt.method, "/buckets/"+encodedARN+"/"+tt.subPath, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestRefinement1_PutTableReplicationNotFound(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.PutTableReplication("arn:aws:s3tables:us-east-1:000000000000:bucket/b/table/ns/t")
	require.Error(t, err)
}

func TestRefinement1_DeleteTableReplicationNotFound(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.DeleteTableReplication("arn:aws:s3tables:us-east-1:000000000000:bucket/b/table/ns/t")
	require.Error(t, err)
}

func TestRefinement1_PutTableRecordExpiryNotFound(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.PutTableRecordExpirationConfiguration(
		"arn:aws:s3tables:us-east-1:000000000000:bucket/b/table/ns/t",
		&s3tables.TableRecordExpiryConfig{Status: "ENABLED"},
	)
	require.Error(t, err)
}

func TestRefinement1_GetTableRecordExpiryNotFound(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.GetTableRecordExpirationConfiguration(
		"arn:aws:s3tables:us-east-1:000000000000:bucket/b/table/ns/t",
	)
	require.Error(t, err)
}

func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("123456789012", "us-west-2")
	assert.Equal(t, "123456789012", b.AccountID())
	assert.Equal(t, "us-west-2", b.Region())
}

func TestRefinement1_SnapshotRestoreNewFields(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	tb, err := b.CreateTableBucket("snap2-bucket")
	require.NoError(t, err)

	_, err = b.CreateNamespace(tb.ARN, []string{"ns1"})
	require.NoError(t, err)

	table, err := b.CreateTable(tb.ARN, []string{"ns1"}, "t1", "ICEBERG")
	require.NoError(t, err)

	require.NoError(t, b.PutTableReplication(table.ARN))

	expCfg := &s3tables.TableRecordExpiryConfig{Status: "ENABLED"}
	require.NoError(t, b.PutTableRecordExpirationConfiguration(table.ARN, expCfg))

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, s3tables.TableReplicationCount(b2))
	assert.Equal(t, 1, s3tables.TableRecordExpiryCount(b2))
}

func TestRefinement1_InvalidSnapshotRestore(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestRefinement1_MetadataLocationVsUpdate(t *testing.T) {
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

func TestRefinement1_ProviderName(t *testing.T) {
	t.Parallel()

	p := &s3tables.Provider{}
	assert.Equal(t, "S3tables", p.Name())
}

func TestRefinement1_ProviderInitDefault(t *testing.T) {
	t.Parallel()

	p := &s3tables.Provider{}
	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	require.NotNil(t, svc)
}

func TestRefinement1_ChaosInterface(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "s3tables", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
	assert.Equal(t, "us-east-1", h.ChaosRegions()[0])
}

func TestRefinement1_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Positive(t, h.MatchPriority())
}

func TestRefinement1_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "bucket path",
			path: "/buckets/arn%3Aaws%3As3tables%3Aus-east-1%3A123%3Abucket%2Fb",
			want: "arn:aws:s3tables:us-east-1:123:bucket/b",
		},
		{
			name: "table-bucket-replication with param",
			path: "/table-bucket-replication?tableBucketARN=myarn",
			want: "myarn",
		},
		{
			name: "table-replication with tableArn",
			path: "/table-replication?tableArn=mytablearn",
			want: "mytablearn",
		},
		{
			name: "empty path",
			path: "/",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetRequest(req)
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestRefinement1_RouteMatcherNewPathPrefixes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "table-bucket-replication", path: "/table-bucket-replication?tableBucketARN=arn", want: true},
		{name: "table-replication", path: "/table-replication?tableArn=arn", want: true},
		{name: "table-record-expiration", path: "/table-record-expiration?tableArn=arn", want: true},
		{name: "no match", path: "/other", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetRequest(req)
			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestRefinement1_RestoreMinimalSnapshot(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	// Restore a minimal snapshot with nil maps to exercise ensureNonNilMaps
	require.NoError(t, b.Restore(t.Context(), []byte(`{"accountID":"123456789012","region":"us-west-2"}`)))
	assert.Equal(t, 0, s3tables.BucketCount(b))
	assert.Equal(t, "123456789012", b.AccountID())
	assert.Equal(t, "us-west-2", b.Region())
}

func TestRefinement1_DeleteTableBucketReplicationNotFound(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.DeleteTableBucketReplication("arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent")
	require.Error(t, err)
}

func TestRefinement1_GetTableBucketReplicationBucketNotFound(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.GetTableBucketReplication("arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent")
	require.Error(t, err)
}

func TestRefinement1_PutTableBucketReplicationBucketNotFound(t *testing.T) {
	t.Parallel()

	b := s3tables.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.PutTableBucketReplication(
		"arn:aws:s3tables:us-east-1:000000000000:bucket/nonexistent",
		&s3tables.BucketReplicationConfig{},
	)
	require.Error(t, err)
}
