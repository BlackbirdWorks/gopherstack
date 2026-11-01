package awsconfig_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// TestListStoredQueriesMetadata verifies ListStoredQueries returns StoredQueryMetadata.
func TestListStoredQueriesMetadata(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	b := h.Backend
	_, err := b.PutStoredQuery("my-query", "", "", nil)
	require.NoError(t, err)

	rec := doAWSConfigRequest(t, h, "ListStoredQueries", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		StoredQueryMetadata []struct {
			QueryArn  string `json:"QueryArn"`
			QueryName string `json:"QueryName"`
		} `json:"StoredQueryMetadata"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1()))
	require.Len(t, out.StoredQueryMetadata, 1)
	assert.Equal(t, "my-query", out.StoredQueryMetadata[0].QueryName)
	assert.Contains(t, out.StoredQueryMetadata[0].QueryArn, "arn:aws:config:")
}
