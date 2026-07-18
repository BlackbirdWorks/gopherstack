package s3tables_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_PutTableReplicationReturns204(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "put-repl-204-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
	tableARN := createTableHelper(t, h, bucketARN, "ns1", "tbl")

	q := url.Values{}
	q.Set("tableArn", tableARN)
	rec := doS3TablesRequest(t, h, http.MethodPut, "/table-replication?"+q.Encode(),
		map[string]any{"replicationConfiguration": map[string]any{}})

	assert.Equal(t, http.StatusNoContent, rec.Code,
		"PutTableReplication must return 204 No Content per real AWS API")
	assert.Empty(t, rec.Body.String(), "PutTableReplication response body must be empty")
}

func TestHandler_DeleteTableReplication(t *testing.T) {
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

func TestHandler_GetTableRecordExpirationConfiguration(t *testing.T) {
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
