package s3tables_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tableReplicationConfigBody builds a PutTableReplication request body
// matching the real wire shape: configuration.role + configuration.rules,
// each rule a list of {destinationTableBucketARN} destinations -- see
// aws-sdk-go-v2/service/s3tables's TableReplicationConfiguration /
// TableReplicationRule / ReplicationDestination types.
func tableReplicationConfigBody(role, destARN string) map[string]any {
	return map[string]any{
		"configuration": map[string]any{
			"role": role,
			"rules": []any{
				map[string]any{
					"destinations": []any{
						map[string]any{"destinationTableBucketARN": destARN},
					},
				},
			},
		},
	}
}

func TestHandler_PutTableReplication(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "put-repl-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
	tableARN := createTableHelper(t, h, bucketARN, "ns1", "tbl")

	q := url.Values{}
	q.Set("tableArn", tableARN)
	rec := doS3TablesRequest(
		t,
		h,
		http.MethodPut,
		"/table-replication?"+q.Encode(),
		tableReplicationConfigBody(
			"arn:aws:iam::000000000000:role/repl",
			"arn:aws:s3tables:us-east-1:000000000000:bucket/dest",
		),
	)

	require.Equal(
		t,
		http.StatusOK,
		rec.Code,
		"PutTableReplicationOutput requires status and versionToken, so the response must be a 200 with a JSON body, not 204",
	)

	result := parseResponse(t, rec)
	assert.Equal(t, "COMPLETED", result["status"])
	assert.NotEmpty(t, result["versionToken"], "PutTableReplicationOutput.versionToken is a required response member")
}

func TestHandler_GetTableReplication(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "get-repl-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
	tableARN := createTableHelper(t, h, bucketARN, "ns1", "tbl")

	destARN := "arn:aws:s3tables:us-east-1:000000000000:bucket/dest"

	q := url.Values{}
	q.Set("tableArn", tableARN)
	putRec := doS3TablesRequest(t, h, http.MethodPut, "/table-replication?"+q.Encode(),
		tableReplicationConfigBody("arn:aws:iam::000000000000:role/repl", destARN))
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doS3TablesRequest(t, h, http.MethodGet, "/table-replication?"+q.Encode(), nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	result := parseResponse(t, getRec)
	assert.NotEmpty(t, result["versionToken"], "GetTableReplicationOutput.versionToken is a required response member")

	cfg, ok := result["configuration"].(map[string]any)
	require.True(t, ok, "GetTableReplicationOutput.configuration is a required response member")
	assert.Equal(t, "arn:aws:iam::000000000000:role/repl", cfg["role"])

	rules, ok := cfg["rules"].([]any)
	require.True(t, ok)
	require.Len(t, rules, 1)

	rule, ok := rules[0].(map[string]any)
	require.True(t, ok)

	dests, ok := rule["destinations"].([]any)
	require.True(t, ok)
	require.Len(t, dests, 1)

	dest, ok := dests[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, destARN, dest["destinationTableBucketARN"],
		"the wire field is destinationTableBucketARN, not destinationBucketARN")
}

func TestHandler_GetTableReplication_NoConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "get-repl-none-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
	tableARN := createTableHelper(t, h, bucketARN, "ns1", "tbl")

	q := url.Values{}
	q.Set("tableArn", tableARN)
	rec := doS3TablesRequest(t, h, http.MethodGet, "/table-replication?"+q.Encode(), nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_PutTableReplication_StaleVersionTokenConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	bucketARN := createBucketHelper(t, h, "put-repl-conflict-bucket")
	createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
	tableARN := createTableHelper(t, h, bucketARN, "ns1", "tbl")

	q := url.Values{}
	q.Set("tableArn", tableARN)
	firstPut := doS3TablesRequest(
		t,
		h,
		http.MethodPut,
		"/table-replication?"+q.Encode(),
		tableReplicationConfigBody(
			"arn:aws:iam::000000000000:role/repl",
			"arn:aws:s3tables:us-east-1:000000000000:bucket/dest",
		),
	)
	require.Equal(t, http.StatusOK, firstPut.Code)

	staleQ := url.Values{}
	staleQ.Set("tableArn", tableARN)
	staleQ.Set("versionToken", "stale-token")
	rec := doS3TablesRequest(
		t,
		h,
		http.MethodPut,
		"/table-replication?"+staleQ.Encode(),
		tableReplicationConfigBody(
			"arn:aws:iam::000000000000:role/repl2",
			"arn:aws:s3tables:us-east-1:000000000000:bucket/dest2",
		),
	)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_DeleteTableReplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		setupTable       bool
		noARNParam       bool
		omitVersionToken bool
		useStaleToken    bool
		wantCode         int
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
		{
			name:             "missing versionToken param",
			setupTable:       true,
			omitVersionToken: true,
			wantCode:         http.StatusBadRequest,
		},
		{
			name:          "stale versionToken",
			setupTable:    true,
			useStaleToken: true,
			wantCode:      http.StatusConflict,
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

			if !tt.setupTable {
				q := url.Values{}
				q.Set("tableArn", "arn:aws:s3tables:us-east-1:000000000000:bucket/b/table/ns/t")
				q.Set("versionToken", "whatever")
				rec := doS3TablesRequest(t, h, http.MethodDelete, "/table-replication?"+q.Encode(), nil)
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			bucketARN := createBucketHelper(t, h, "repl-table-bucket")
			createNamespaceHelper(t, h, bucketARN, []string{"ns1"})
			tableARN := createTableHelper(t, h, bucketARN, "ns1", "mytable")

			putQ := url.Values{}
			putQ.Set("tableArn", tableARN)
			putRec := doS3TablesRequest(
				t,
				h,
				http.MethodPut,
				"/table-replication?"+putQ.Encode(),
				tableReplicationConfigBody(
					"arn:aws:iam::000000000000:role/repl",
					"arn:aws:s3tables:us-east-1:000000000000:bucket/dest",
				),
			)
			require.Equal(t, http.StatusOK, putRec.Code)

			putResult := parseResponse(t, putRec)
			versionToken, _ := putResult["versionToken"].(string)
			require.NotEmpty(t, versionToken)

			q := url.Values{}
			q.Set("tableArn", tableARN)

			switch {
			case tt.omitVersionToken:
				// leave versionToken unset
			case tt.useStaleToken:
				q.Set("versionToken", "stale-token")
			default:
				q.Set("versionToken", versionToken)
			}

			rec := doS3TablesRequest(t, h, http.MethodDelete, "/table-replication?"+q.Encode(), nil)
			assert.Equal(t, tt.wantCode, rec.Code)
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
			wantStatus: "disabled",
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
					_, hasTopLevelTableARN := result["tableARN"]
					assert.False(t, hasTopLevelTableARN,
						"GetTableRecordExpirationConfigurationOutput has no top-level tableARN field")

					cfg, ok := result["configuration"].(map[string]any)
					require.True(t, ok, "response must include the real configuration field")
					assert.Equal(t, tt.wantStatus, cfg["status"])
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
