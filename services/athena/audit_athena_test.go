package athena_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAuditAthena_ResultReuseConfiguration_StoredAndReturned verifies that
// ResultReuseConfiguration is persisted on the query execution and returned.
func TestAuditAthena_ResultReuseConfiguration_StoredAndReturned(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // field order is chosen for readability, not alignment
		reuseCfg        string
		name            string
		wantEnabled     bool
		wantMaxAge      float64
		wantReuseInBody bool
	}{
		{
			name:     "no_reuse_config",
			reuseCfg: "",
		},
		{
			name: "reuse_disabled",
			reuseCfg: `"ResultReuseConfiguration":{` +
				`"ResultReuseByAgeConfiguration":{"Enabled":false,"MaxAgeInMinutes":60}}`,
			wantEnabled:     false,
			wantMaxAge:      60,
			wantReuseInBody: true,
		},
		{
			name: "reuse_enabled_60min",
			reuseCfg: `"ResultReuseConfiguration":{` +
				`"ResultReuseByAgeConfiguration":{"Enabled":true,"MaxAgeInMinutes":60}}`,
			wantEnabled:     true,
			wantMaxAge:      60,
			wantReuseInBody: true,
		},
		{
			name: "reuse_enabled_30min",
			reuseCfg: `"ResultReuseConfiguration":{` +
				`"ResultReuseByAgeConfiguration":{"Enabled":true,"MaxAgeInMinutes":30}}`,
			wantEnabled:     true,
			wantMaxAge:      30,
			wantReuseInBody: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)

			body := `{"QueryString":"SELECT 1","WorkGroup":"primary"`
			if tc.reuseCfg != "" {
				body += "," + tc.reuseCfg
			}
			body += "}"

			rec := a1Do(t, h, "StartQueryExecution", body)
			require.Equal(t, http.StatusOK, rec.Code)

			m := a1Unmarshal(t, rec)
			execID, _ := m["QueryExecutionId"].(string)
			require.NotEmpty(t, execID)

			rec2 := a1Do(t, h, "GetQueryExecution", fmt.Sprintf(`{"QueryExecutionId":%q}`, execID))
			require.Equal(t, http.StatusOK, rec2.Code)

			qe := a1Unmarshal(t, rec2)["QueryExecution"].(map[string]any)

			if !tc.wantReuseInBody {
				assert.Nil(t, qe["ResultReuseConfiguration"], "no ResultReuseConfiguration when not sent")

				return
			}

			reuseCfg, ok := qe["ResultReuseConfiguration"].(map[string]any)
			require.True(t, ok, "ResultReuseConfiguration should be present")

			byAge, ok := reuseCfg["ResultReuseByAgeConfiguration"].(map[string]any)
			require.True(t, ok, "ResultReuseByAgeConfiguration should be present")
			assert.Equal(t, tc.wantEnabled, byAge["Enabled"])
			assert.InDelta(t, tc.wantMaxAge, byAge["MaxAgeInMinutes"], 0)
		})
	}
}

// TestAuditAthena_ResultReuse_MarksReusedPreviousResult verifies that when a
// query matches a recent succeeded execution, ReusedPreviousResult is set to true.
func TestAuditAthena_ResultReuse_MarksReusedPreviousResult(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	const query = "SELECT 42"
	const reuseCfgJSON = `"ResultReuseConfiguration":{` +
		`"ResultReuseByAgeConfiguration":{"Enabled":true,"MaxAgeInMinutes":60}}`

	// First execution.
	rec1 := a1Do(t, h, "StartQueryExecution",
		`{"QueryString":"`+query+`","WorkGroup":"primary",`+reuseCfgJSON+`}`)
	require.Equal(t, http.StatusOK, rec1.Code)

	id1 := a1Unmarshal(t, rec1)["QueryExecutionId"].(string)

	qe1 := a1Unmarshal(t, a1Do(t, h, "GetQueryExecution",
		fmt.Sprintf(`{"QueryExecutionId":%q}`, id1)))["QueryExecution"].(map[string]any)
	stats1 := qe1["Statistics"].(map[string]any)
	assert.NotEqual(t, true, stats1["ReusedPreviousResult"],
		"first execution should NOT be marked as reused")

	// Second execution with same query and reuse enabled → should be reused.
	rec2 := a1Do(t, h, "StartQueryExecution",
		`{"QueryString":"`+query+`","WorkGroup":"primary",`+reuseCfgJSON+`}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	id2 := a1Unmarshal(t, rec2)["QueryExecutionId"].(string)
	assert.NotEqual(t, id1, id2, "each execution gets a new ID")

	qe2 := a1Unmarshal(t, a1Do(t, h, "GetQueryExecution",
		fmt.Sprintf(`{"QueryExecutionId":%q}`, id2)))["QueryExecution"].(map[string]any)
	stats2 := qe2["Statistics"].(map[string]any)
	assert.Equal(t, true, stats2["ReusedPreviousResult"],
		"second execution should be marked as reused")
}

// TestAuditAthena_ResultReuse_DifferentQuery tests that reuse is not triggered for different queries.
func TestAuditAthena_ResultReuse_DifferentQuery(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	const reuseCfgJSON = `"ResultReuseConfiguration":{` +
		`"ResultReuseByAgeConfiguration":{"Enabled":true,"MaxAgeInMinutes":60}}`

	// First execution.
	rec1 := a1Do(t, h, "StartQueryExecution",
		`{"QueryString":"SELECT 1","WorkGroup":"primary",`+reuseCfgJSON+`}`)
	require.Equal(t, http.StatusOK, rec1.Code)

	// Second execution with DIFFERENT query — should NOT be reused.
	rec2 := a1Do(t, h, "StartQueryExecution",
		`{"QueryString":"SELECT 2","WorkGroup":"primary",`+reuseCfgJSON+`}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	id2 := a1Unmarshal(t, rec2)["QueryExecutionId"].(string)

	qe2 := a1Unmarshal(t, a1Do(t, h, "GetQueryExecution",
		fmt.Sprintf(`{"QueryExecutionId":%q}`, id2)))["QueryExecution"].(map[string]any)
	stats2 := qe2["Statistics"].(map[string]any)
	assert.NotEqual(t, true, stats2["ReusedPreviousResult"],
		"different query must not be marked as reused")
}

// TestAuditAthena_ResultReuse_DisabledDoesNotReuse tests that disabled reuse never triggers.
func TestAuditAthena_ResultReuse_DisabledDoesNotReuse(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	const query = "SELECT 100"
	const disabledCfg = `"ResultReuseConfiguration":{` +
		`"ResultReuseByAgeConfiguration":{"Enabled":false,"MaxAgeInMinutes":60}}`

	// Two identical executions with reuse disabled.
	a1Do(t, h, "StartQueryExecution",
		`{"QueryString":"`+query+`","WorkGroup":"primary",`+disabledCfg+`}`)

	rec2 := a1Do(t, h, "StartQueryExecution",
		`{"QueryString":"`+query+`","WorkGroup":"primary",`+disabledCfg+`}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	id2 := a1Unmarshal(t, rec2)["QueryExecutionId"].(string)

	qe2 := a1Unmarshal(t, a1Do(t, h, "GetQueryExecution",
		fmt.Sprintf(`{"QueryExecutionId":%q}`, id2)))["QueryExecution"].(map[string]any)
	stats2 := qe2["Statistics"].(map[string]any)
	assert.NotEqual(t, true, stats2["ReusedPreviousResult"],
		"disabled reuse should never mark ReusedPreviousResult=true")
}

// TestAuditAthena_EnforceWorkGroupConfiguration_OverridesOutputLocation verifies
// that when EnforceWorkGroupConfiguration=true, the workgroup OutputLocation
// overrides the per-query ResultConfiguration.
func TestAuditAthena_EnforceWorkGroupConfiguration_OverridesOutputLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wgOutputLocation string
		perQueryLocation string
		expectedLocation string
		name             string
		enforceWGConfig  bool
	}{
		{
			name:             "enforce_true_wg_overrides_query",
			enforceWGConfig:  true,
			wgOutputLocation: "s3://wg-bucket/results/",
			perQueryLocation: "s3://query-bucket/results/",
			expectedLocation: "s3://wg-bucket/results/",
		},
		{
			name:             "enforce_false_query_location_used",
			enforceWGConfig:  false,
			wgOutputLocation: "s3://wg-bucket/results/",
			perQueryLocation: "s3://query-bucket/results/",
			expectedLocation: "s3://query-bucket/results/",
		},
		{
			name:             "enforce_true_no_wg_location_keeps_query",
			enforceWGConfig:  true,
			wgOutputLocation: "",
			perQueryLocation: "s3://query-bucket/results/",
			expectedLocation: "s3://query-bucket/results/",
		},
		{
			name:             "enforce_true_no_query_location_uses_wg",
			enforceWGConfig:  true,
			wgOutputLocation: "s3://wg-only/results/",
			perQueryLocation: "",
			expectedLocation: "s3://wg-only/results/",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := a1Handler(t)

			// Create workgroup with EnforceWorkGroupConfiguration.
			wgCfgJSON, _ := json.Marshal(map[string]any{
				"EnforceWorkGroupConfiguration": tc.enforceWGConfig,
				"ResultConfiguration":           map[string]any{"OutputLocation": tc.wgOutputLocation},
			})
			rec := a1Do(t, h, "CreateWorkGroup",
				fmt.Sprintf(`{"Name":"enforce-wg","Configuration":%s}`, string(wgCfgJSON)))
			require.Equal(t, http.StatusOK, rec.Code)

			// Start query with per-query location.
			startBody, _ := json.Marshal(map[string]any{
				"QueryString": "SELECT 1",
				"WorkGroup":   "enforce-wg",
				"ResultConfiguration": map[string]any{
					"OutputLocation": tc.perQueryLocation,
				},
			})
			rec2 := a1Do(t, h, "StartQueryExecution", string(startBody))
			require.Equal(t, http.StatusOK, rec2.Code)

			execID := a1Unmarshal(t, rec2)["QueryExecutionId"].(string)

			rec3 := a1Do(t, h, "GetQueryExecution",
				fmt.Sprintf(`{"QueryExecutionId":%q}`, execID))
			require.Equal(t, http.StatusOK, rec3.Code)

			qe := a1Unmarshal(t, rec3)["QueryExecution"].(map[string]any)
			resultCfg := qe["ResultConfiguration"].(map[string]any)
			assert.Equal(t, tc.expectedLocation, resultCfg["OutputLocation"])
		})
	}
}

// TestAuditAthena_ListPreparedStatements_Pagination verifies that ListPreparedStatements
// supports NextToken/MaxResults pagination.
func TestAuditAthena_ListPreparedStatements_Pagination(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	// Create 5 prepared statements.
	names := []string{"stmt_a", "stmt_b", "stmt_c", "stmt_d", "stmt_e"}
	for _, name := range names {
		rec := a1Do(t, h, "CreatePreparedStatement", fmt.Sprintf(
			`{"StatementName":%q,"WorkGroup":"primary","QueryStatement":"SELECT 1"}`, name))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Page 1: MaxResults=2 → expect 2 results + NextToken.
	rec := a1Do(t, h, "ListPreparedStatements", `{"WorkGroup":"primary","MaxResults":2}`)
	require.Equal(t, http.StatusOK, rec.Code)

	page1 := a1Unmarshal(t, rec)
	stmts1, _ := page1["PreparedStatements"].([]any)
	require.Len(t, stmts1, 2, "page 1 should have 2 statements")

	tok, _ := page1["NextToken"].(string)
	require.NotEmpty(t, tok, "page 1 should return NextToken")

	// Page 2: MaxResults=2, NextToken → expect 2 more.
	rec2 := a1Do(t, h, "ListPreparedStatements",
		fmt.Sprintf(`{"WorkGroup":"primary","MaxResults":2,"NextToken":%q}`, tok))
	require.Equal(t, http.StatusOK, rec2.Code)

	page2 := a1Unmarshal(t, rec2)
	stmts2, _ := page2["PreparedStatements"].([]any)
	require.Len(t, stmts2, 2, "page 2 should have 2 statements")

	tok2, _ := page2["NextToken"].(string)
	require.NotEmpty(t, tok2, "page 2 should return NextToken")

	// Page 3: last page → expect 1 result, no NextToken.
	rec3 := a1Do(t, h, "ListPreparedStatements",
		fmt.Sprintf(`{"WorkGroup":"primary","MaxResults":10,"NextToken":%q}`, tok2))
	require.Equal(t, http.StatusOK, rec3.Code)

	page3 := a1Unmarshal(t, rec3)
	stmts3, _ := page3["PreparedStatements"].([]any)
	require.Len(t, stmts3, 1, "page 3 should have 1 statement")
	assert.Empty(t, page3["NextToken"], "last page must not have NextToken")

	// Collect all names across pages.
	gotNames := make([]string, 0, len(stmts1)+len(stmts2)+len(stmts3))
	for _, s := range append(append(stmts1, stmts2...), stmts3...) {
		m := s.(map[string]any)
		gotNames = append(gotNames, m["StatementName"].(string))
	}
	assert.ElementsMatch(t, names, gotNames, "all names should be returned across pages")
}

// TestAuditAthena_ListPreparedStatements_NoMaxResults verifies that ListPreparedStatements
// without MaxResults returns all statements (up to default limit).
func TestAuditAthena_ListPreparedStatements_NoMaxResults(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	for i := range 5 {
		rec := a1Do(t, h, "CreatePreparedStatement", fmt.Sprintf(
			`{"StatementName":"s%d","WorkGroup":"primary","QueryStatement":"SELECT 1"}`, i))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := a1Do(t, h, "ListPreparedStatements", `{"WorkGroup":"primary"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	page := a1Unmarshal(t, rec)
	stmts, _ := page["PreparedStatements"].([]any)
	assert.Len(t, stmts, 5)
	assert.Empty(t, page["NextToken"], "no NextToken when all fit on one page")
}

// TestAuditAthena_BatchGetQueryExecution_Shape verifies shape of the batch response.
func TestAuditAthena_BatchGetQueryExecution_Shape(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	// Create one real execution.
	rec := a1Do(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1","WorkGroup":"primary"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	realID := a1Unmarshal(t, rec)["QueryExecutionId"].(string)

	tests := []struct { //nolint:govet // field order is chosen for readability, not alignment
		ids             []string
		name            string
		wantFound       int
		wantUnprocessed int
	}{
		{
			name:      "one_real_one_missing",
			ids:       []string{realID, "nonexistent-id"},
			wantFound: 1, wantUnprocessed: 1,
		},
		{
			name:      "only_real",
			ids:       []string{realID},
			wantFound: 1, wantUnprocessed: 0,
		},
		{
			name:      "only_missing",
			ids:       []string{"bad-id-1", "bad-id-2"},
			wantFound: 0, wantUnprocessed: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			idsJSON, _ := json.Marshal(tc.ids)
			batchRec := a1Do(t, h, "BatchGetQueryExecution",
				fmt.Sprintf(`{"QueryExecutionIds":%s}`, string(idsJSON)))
			require.Equal(t, http.StatusOK, batchRec.Code)

			m := a1Unmarshal(t, batchRec)
			found, _ := m["QueryExecutions"].([]any)
			unprocessed, _ := m["UnprocessedQueryExecutionIds"].([]any)

			assert.Len(t, found, tc.wantFound, "found count")
			assert.Len(t, unprocessed, tc.wantUnprocessed, "unprocessed count")
		})
	}
}

// TestAuditAthena_StopQueryExecution_TerminalRejected verifies that stopping
// an already-terminal execution returns an error.
func TestAuditAthena_StopQueryExecution_TerminalRejected(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	rec := a1Do(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1","WorkGroup":"primary"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	execID := a1Unmarshal(t, rec)["QueryExecutionId"].(string)

	// StartQueryExecution immediately puts it in SUCCEEDED (terminal).
	// A stop attempt must fail.
	rec2 := a1Do(t, h, "StopQueryExecution", fmt.Sprintf(`{"QueryExecutionId":%q}`, execID))
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestAuditAthena_QueryExecution_Statistics_Fields verifies statistics fields shape.
func TestAuditAthena_QueryExecution_Statistics_Fields(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	rec := a1Do(t, h, "StartQueryExecution", `{"QueryString":"SELECT 1","WorkGroup":"primary"}`)
	require.Equal(t, http.StatusOK, rec.Code)
	execID := a1Unmarshal(t, rec)["QueryExecutionId"].(string)

	rec2 := a1Do(t, h, "GetQueryExecution", fmt.Sprintf(`{"QueryExecutionId":%q}`, execID))
	require.Equal(t, http.StatusOK, rec2.Code)

	qe := a1Unmarshal(t, rec2)["QueryExecution"].(map[string]any)
	stats := qe["Statistics"].(map[string]any)

	assert.NotZero(t, stats["EngineExecutionTimeInMillis"], "EngineExecutionTimeInMillis should be set")
	assert.NotZero(t, stats["TotalExecutionTimeInMillis"], "TotalExecutionTimeInMillis should be set")

	status := qe["Status"].(map[string]any)
	assert.Equal(t, "SUCCEEDED", status["State"])
	assert.NotZero(t, status["SubmissionDateTime"])
	assert.NotZero(t, status["CompletionDateTime"])
}

// TestAuditAthena_WorkGroup_EnforceEncryption verifies that encryption configuration
// from the workgroup overrides the per-query setting when EnforceWorkGroupConfiguration=true.
func TestAuditAthena_WorkGroup_EnforceEncryption(t *testing.T) {
	t.Parallel()

	h := a1Handler(t)

	wgCfg, _ := json.Marshal(map[string]any{
		"EnforceWorkGroupConfiguration": true,
		"ResultConfiguration": map[string]any{
			"EncryptionConfiguration": map[string]any{
				"EncryptionOption": "SSE_S3",
			},
		},
	})

	rec := a1Do(t, h, "CreateWorkGroup",
		fmt.Sprintf(`{"Name":"enc-wg","Configuration":%s}`, string(wgCfg)))
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := a1Do(t, h, "StartQueryExecution",
		`{"QueryString":"SELECT 1","WorkGroup":"enc-wg"}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	execID := a1Unmarshal(t, rec2)["QueryExecutionId"].(string)

	rec3 := a1Do(t, h, "GetQueryExecution",
		fmt.Sprintf(`{"QueryExecutionId":%q}`, execID))
	require.Equal(t, http.StatusOK, rec3.Code)

	qe := a1Unmarshal(t, rec3)["QueryExecution"].(map[string]any)
	resultCfg := qe["ResultConfiguration"].(map[string]any)
	encCfg, ok := resultCfg["EncryptionConfiguration"].(map[string]any)
	require.True(t, ok, "EncryptionConfiguration should be present when enforced")
	assert.Equal(t, "SSE_S3", encCfg["EncryptionOption"])
}
