package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfiguredTableHasIDKey verifies that ConfiguredTable responses
// include the canonical "id" key.
func TestConfiguredTableHasIDKey(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/configuredTables", map[string]any{
		"name":           "id-test-table",
		"allowedColumns": []string{"col1"},
		"analysisMethod": "DIRECT_QUERY",
		"tableReference": map[string]any{"glue": map[string]any{
			"databaseName": "db", "tableName": "tbl",
		}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ct := resp["configuredTable"].(map[string]any)

	id, hasID := ct["id"]
	legacyID, hasLegacy := ct["configuredTableIdentifier"]

	assert.True(t, hasID, "configuredTable must have 'id' key (AWS canonical)")
	assert.True(t, hasLegacy, "configuredTable must have 'configuredTableIdentifier' (backward compat)")
	assert.Equal(t, id, legacyID)
	assert.NotEmpty(t, id)
}

// TestConfiguredTableAnalysisRuleHasConfiguredTableID verifies the
// canonical "configuredTableId" key on ConfiguredTableAnalysisRule.
func TestConfiguredTableAnalysisRuleHasConfiguredTableID(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	rec := doRequest(t, e, "POST", "/configuredTables", map[string]any{
		"name": "ar-table", "allowedColumns": []string{"col1"},
		"analysisMethod": "DIRECT_QUERY",
		"tableReference": map[string]any{"glue": map[string]any{
			"databaseName": "db", "tableName": "tbl",
		}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var ctResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ctResp))
	ctID := ctResp["configuredTable"].(map[string]any)["id"].(string)

	// Create analysis rule
	rec2 := doRequest(t, e, "POST", "/configuredTables/"+ctID+"/analysisRule", map[string]any{
		"type":   "LIST",
		"policy": map[string]any{"v1": map[string]any{"list": map[string]any{}}},
	})
	require.Equal(t, http.StatusOK, rec2.Code, rec2.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))
	ar := resp["analysisRule"].(map[string]any)
	assert.Contains(t, ar, "configuredTableId",
		"analysisRule must have canonical 'configuredTableId' key")
	assert.Equal(t, ctID, ar["configuredTableId"])
}
