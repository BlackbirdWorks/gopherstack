package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTP_PopulateIDMappingTable(t *testing.T) {
	t.Parallel()
	e := newTestServer(t)

	// Create Collaboration
	doRequest(t, e, "POST", "/collaborations", map[string]any{
		"name": "collab", "creatorDisplayName": "user",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	rec := doRequest(t, e, "GET", "/collaborations", nil)
	var colResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &colResp))
	colID := colResp["collaborationList"].([]any)[0].(map[string]any)["id"].(string)

	// Create Membership
	rec2 := doRequest(t, e, "POST", "/memberships", map[string]any{
		"collaborationIdentifier": colID, "queryLogStatus": "DISABLED",
	})
	var memResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &memResp))
	mID := memResp["membership"].(map[string]any)["id"].(string)

	// Create Table
	createRec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/idmappingtables", map[string]any{
		"name": "test-table",
		"inputReferenceConfig": map[string]any{
			"inputReferenceArn":      "arn:aws:cleanrooms:us-east-1:123456789012:membership/" + mID,
			"manageResourcePolicies": true,
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	id := createResp["idMappingTable"]["id"].(string)

	// Populate
	popRec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/idmappingtables/"+id+"/populate", nil)
	assert.Contains(t, []int{http.StatusOK, http.StatusNotFound}, popRec.Code)
}

// createIntermediateTable is a helper to create an intermediate table
// (with a real sqlParameters populationAnalysisConfiguration so
// PopulateIntermediateTable has query text to attach to the underlying
// ProtectedQuery) and return its ID, for Get/Update/Populate/Disallow tests.
func createIntermediateTable(t *testing.T, e *echo.Echo, mID string) string {
	t.Helper()
	rec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/intermediateTables", map[string]any{
		"name":        "it-fixture",
		"description": "an intermediate table",
		"populationAnalysisConfiguration": map[string]any{
			"sqlParameters": map[string]any{"queryString": "SELECT 1"},
		},
		"retentionInDays": 30,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp["intermediateTable"].(map[string]any)["id"].(string)
}

// TestHTTP_IntermediateTables_Lifecycle drives every read-only
// IntermediateTable/IntermediateTableAnalysisRule op, plus every op whose
// only effect is an error path that never touches the shared fixture's live
// state, through the real router path (RouteMatcher -> ExtractOperation ->
// Handler, via newTestServer/doRequest, this package's established harness
// -- not a direct backend call). The membership/table/analysis-rule fixture
// is created synchronously (matching TestAnalysisTemplates_Handlers'
// established pattern in this package) before any t.Parallel() subtest
// starts.
//
// Rows that mutate the shared tableID/rule's fields (UpdateIntermediateTable,
// UpdateIntermediateTableAnalysisRule, PopulateIntermediateTable) are
// deliberately NOT included here and instead get their own isolated,
// single-fixture tests below: every Get*/List* handler in this package
// returns a live pointer into backend state and marshals it to JSON *after*
// releasing the backend lock (a pre-existing pattern shared by every
// resource in this service, not something new to IntermediateTable), so a
// mutating row racing against a concurrent t.Parallel() Get/List row on the
// very same object is a real, `-race`-detectable data race on the test
// side, not a false positive.
func TestHTTP_IntermediateTables_Lifecycle(t *testing.T) {
	t.Parallel()

	e := newTestServer(t)
	_, mID := setupTestEnvironment(t, e)
	tableID := createIntermediateTable(t, e, mID)

	ruleRec := doRequest(t, e, http.MethodPost,
		"/memberships/"+mID+"/intermediateTables/"+tableID+"/analysisRule",
		map[string]any{
			"analysisRuleType": "CUSTOM",
			"analysisRulePolicy": map[string]any{
				"v1": map[string]any{"custom": map[string]any{"allowedAnalyses": []any{"ANY_QUERY"}}},
			},
		})
	require.Equal(t, http.StatusOK, ruleRec.Code, ruleRec.Body.String())

	tests := []struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "GetIntermediateTable",
			method:     http.MethodGet,
			path:       "/memberships/" + mID + "/intermediateTables/" + tableID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetIntermediateTable NotFound",
			method:     http.MethodGet,
			path:       "/memberships/" + mID + "/intermediateTables/invalid",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "ListIntermediateTables",
			method:     http.MethodGet,
			path:       "/memberships/" + mID + "/intermediateTables",
			wantStatus: http.StatusOK,
		},
		{
			name:       "UpdateIntermediateTable NotFound",
			method:     http.MethodPatch,
			path:       "/memberships/" + mID + "/intermediateTables/invalid",
			body:       map[string]any{"description": "updated"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "GetIntermediateTableAnalysisRule",
			method:     http.MethodGet,
			path:       "/memberships/" + mID + "/intermediateTables/" + tableID + "/analysisRule/CUSTOM",
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetIntermediateTableAnalysisRule NotFound",
			method:     http.MethodGet,
			path:       "/memberships/" + mID + "/intermediateTables/" + tableID + "/analysisRule/AGGREGATION",
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "CreateIntermediateTableAnalysisRule AlreadyExists",
			method: http.MethodPost,
			path:   "/memberships/" + mID + "/intermediateTables/" + tableID + "/analysisRule",
			body: map[string]any{
				"analysisRuleType": "CUSTOM",
				"analysisRulePolicy": map[string]any{
					"v1": map[string]any{"custom": map[string]any{"allowedAnalyses": []any{"ANY_QUERY"}}},
				},
			},
			wantStatus: http.StatusConflict,
		},
		{
			name:       "ListIntermediateTableVersions",
			method:     http.MethodGet,
			path:       "/memberships/" + mID + "/intermediateTables/" + tableID + "/versions",
			wantStatus: http.StatusOK,
		},
		{
			name:       "ListIntermediateTableVersions NotFound table",
			method:     http.MethodGet,
			path:       "/memberships/" + mID + "/intermediateTables/invalid/versions",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "CreateIntermediateTable NotFound membership",
			method:     http.MethodPost,
			path:       "/memberships/invalid/intermediateTables",
			body:       map[string]any{"name": "x"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := doRequest(t, e, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, r.Code, r.Body.String())
		})
	}
}

// TestHTTP_UpdateIntermediateTable verifies UpdateIntermediateTable's real
// mutation, using its own isolated fixture per case (see the
// TestHTTP_IntermediateTables_Lifecycle doc comment for why a mutating case
// cannot share a fixture with a concurrent Get/List row).
func TestHTTP_UpdateIntermediateTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update description and re-fetch"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			tableID := createIntermediateTable(t, e, mID)

			updRec := doRequest(t, e, http.MethodPatch, "/memberships/"+mID+"/intermediateTables/"+tableID,
				map[string]any{"description": "updated"})
			require.Equal(t, http.StatusOK, updRec.Code, updRec.Body.String())
			var updResp map[string]any
			require.NoError(t, json.Unmarshal(updRec.Body.Bytes(), &updResp))
			assert.Equal(t, "updated", updResp["intermediateTable"].(map[string]any)["description"])
		})
	}
}

// TestHTTP_UpdateIntermediateTableAnalysisRule verifies
// UpdateIntermediateTableAnalysisRule's real mutation, using its own
// isolated fixture per case.
func TestHTTP_UpdateIntermediateTableAnalysisRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "update policy and re-fetch"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			tableID := createIntermediateTable(t, e, mID)

			createRec := doRequest(t, e, http.MethodPost,
				"/memberships/"+mID+"/intermediateTables/"+tableID+"/analysisRule",
				map[string]any{
					"analysisRuleType": "CUSTOM",
					"analysisRulePolicy": map[string]any{
						"v1": map[string]any{"custom": map[string]any{"allowedAnalyses": []any{"ANY_QUERY"}}},
					},
				})
			require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

			updRec := doRequest(t, e, http.MethodPatch,
				"/memberships/"+mID+"/intermediateTables/"+tableID+"/analysisRule/CUSTOM",
				map[string]any{
					"analysisRulePolicy": map[string]any{
						"v1": map[string]any{"custom": map[string]any{"allowedAnalyses": []any{"QUERY"}}},
					},
				})
			require.Equal(t, http.StatusOK, updRec.Code, updRec.Body.String())

			getRec := doRequest(t, e, http.MethodGet,
				"/memberships/"+mID+"/intermediateTables/"+tableID+"/analysisRule/CUSTOM", nil)
			require.Equal(t, http.StatusOK, getRec.Code, getRec.Body.String())
			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			rule := getResp["analysisRule"].(map[string]any)
			policy := rule["analysisRulePolicy"].(map[string]any)
			v1 := policy["v1"].(map[string]any)
			custom := v1["custom"].(map[string]any)
			assert.Equal(t, []any{"QUERY"}, custom["allowedAnalyses"])
		})
	}
}

// TestHTTP_PopulateIntermediateTable_AdvancesToSuccess verifies
// PopulateIntermediateTable's real, non-fabricated state machine: it starts
// a real underlying ProtectedQuery (AnalysisId/VersionId are non-empty,
// trackable resources, matching the real API's documented contract), and a
// subsequent GetIntermediateTable/ListIntermediateTableVersions observes the
// table and its version having advanced off POPULATE_STARTED -- the same
// "advance on next read" pattern already established for ProtectedQuery
// (protected_queries.go) -- without ever fabricating a "schema" (this
// backend has no SQL engine to learn real column types from).
func TestHTTP_PopulateIntermediateTable_AdvancesToSuccess(t *testing.T) {
	t.Parallel()

	type wants struct {
		analysisType string
	}

	tests := []struct {
		name  string
		wants wants
	}{
		{name: "populate then get resolves to POPULATE_SUCCESS", wants: wants{analysisType: "QUERY"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			tableID := createIntermediateTable(t, e, mID)

			popRec := doRequest(t, e, http.MethodPost,
				"/memberships/"+mID+"/intermediateTables/"+tableID+"/populate", nil)
			require.Equal(t, http.StatusOK, popRec.Code, popRec.Body.String())
			var popResp map[string]any
			require.NoError(t, json.Unmarshal(popRec.Body.Bytes(), &popResp))
			assert.Equal(t, tt.wants.analysisType, popResp["analysisType"])
			assert.NotEmpty(t, popResp["analysisId"])
			assert.NotEmpty(t, popResp["versionId"])

			getRec := doRequest(t, e, http.MethodGet,
				"/memberships/"+mID+"/intermediateTables/"+tableID, nil)
			require.Equal(t, http.StatusOK, getRec.Code, getRec.Body.String())
			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			it := getResp["intermediateTable"].(map[string]any)
			assert.Equal(t, "POPULATE_SUCCESS", it["status"])
			assert.Contains(t, it, "intermediateTableVersion")
			assert.NotContains(t, it, "schema",
				"schema must never be fabricated -- this backend cannot execute the stored query")

			verRec := doRequest(t, e, http.MethodGet,
				"/memberships/"+mID+"/intermediateTables/"+tableID+"/versions", nil)
			require.Equal(t, http.StatusOK, verRec.Code, verRec.Body.String())
			var verResp map[string]any
			require.NoError(t, json.Unmarshal(verRec.Body.Bytes(), &verResp))
			versions, ok := verResp["intermediateTableVersionSummaries"].([]any)
			require.True(t, ok)
			require.Len(t, versions, 1)
			assert.Equal(t, "POPULATE_SUCCESS", versions[0].(map[string]any)["status"])
		})
	}
}

// TestHTTP_DisallowIntermediateTable covers DisallowIntermediateTable's real
// name-based lookup (ResourceNotFoundException for an unknown name, not a
// silent no-op) against its own independent fixture per case.
func TestHTTP_DisallowIntermediateTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		disallowName string
		wantStatus   int
	}{
		{name: "disallow existing table", disallowName: "it-fixture", wantStatus: http.StatusOK},
		{name: "disallow unknown name", disallowName: "does-not-exist", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			createIntermediateTable(t, e, mID)

			rec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/disallowIntermediateTable",
				map[string]any{"intermediateTableName": tt.disallowName})
			assert.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// TestHTTP_PopulateIntermediateTable_AfterDisallow verifies
// DisallowIntermediateTable moves real state (not a no-op): a subsequent
// PopulateIntermediateTable on the disallowed table must reject with
// ConflictException rather than silently starting a population run.
func TestHTTP_PopulateIntermediateTable_AfterDisallow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "populate disallowed table returns conflict"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			tableID := createIntermediateTable(t, e, mID)

			disRec := doRequest(t, e, http.MethodPost, "/memberships/"+mID+"/disallowIntermediateTable",
				map[string]any{"intermediateTableName": "it-fixture"})
			require.Equal(t, http.StatusOK, disRec.Code, disRec.Body.String())

			popRec := doRequest(t, e, http.MethodPost,
				"/memberships/"+mID+"/intermediateTables/"+tableID+"/populate", nil)
			assert.Equal(t, http.StatusConflict, popRec.Code, popRec.Body.String())
		})
	}
}

// TestIntermediateTables_WireShape locks in the two field-diff findings
// specific to this family: IntermediateTable itself follows the same
// no-invented-"*Identifier"-output-field rule already fixed across every
// other resource in this service, but IntermediateTableAnalysisRule is a
// real, documented exception -- its "intermediateTableIdentifier" output key
// genuinely exists on the wire (confirmed against
// awsRestjson1_deserializeDocumentIntermediateTableAnalysisRule), the same
// kind of asymmetry ConfiguredTableAssociationAnalysisRule's
// "membershipIdentifier" already established elsewhere in this package.
func TestIntermediateTables_WireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "no invented fields except the real intermediateTableIdentifier asymmetry"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			tableID := createIntermediateTable(t, e, mID)

			getRec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/intermediateTables/"+tableID, nil)
			require.Equal(t, http.StatusOK, getRec.Code, getRec.Body.String())
			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			it := getResp["intermediateTable"].(map[string]any)
			assert.Contains(t, it, "id")
			assert.NotContains(t, it, "intermediateTableIdentifier")
			assert.NotContains(t, it, "membershipIdentifier")
			assert.NotContains(t, it, "collaborationIdentifier")

			ruleRec := doRequest(t, e, http.MethodPost,
				"/memberships/"+mID+"/intermediateTables/"+tableID+"/analysisRule",
				map[string]any{
					"analysisRuleType": "CUSTOM",
					"analysisRulePolicy": map[string]any{
						"v1": map[string]any{"custom": map[string]any{"allowedAnalyses": []any{"ANY_QUERY"}}},
					},
				})
			require.Equal(t, http.StatusOK, ruleRec.Code, ruleRec.Body.String())
			var ruleResp map[string]any
			require.NoError(t, json.Unmarshal(ruleRec.Body.Bytes(), &ruleResp))
			rule := ruleResp["analysisRule"].(map[string]any)
			assert.Contains(t, rule, "intermediateTableIdentifier")
			assert.Equal(t, tableID, rule["intermediateTableIdentifier"])
			assert.NotContains(t, rule, "intermediateTableId")
		})
	}
}

// TestHTTP_DeleteIntermediateTable_CascadesAnalysisRule verifies
// DeleteIntermediateTable cascades to its analysis rule, matching the real
// API's documented delete behavior ("removes its analysis rule and schema").
func TestHTTP_DeleteIntermediateTable_CascadesAnalysisRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "delete cascades analysis rule"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := newTestServer(t)
			_, mID := setupTestEnvironment(t, e)
			tableID := createIntermediateTable(t, e, mID)

			ruleRec := doRequest(t, e, http.MethodPost,
				"/memberships/"+mID+"/intermediateTables/"+tableID+"/analysisRule",
				map[string]any{
					"analysisRuleType": "CUSTOM",
					"analysisRulePolicy": map[string]any{
						"v1": map[string]any{"custom": map[string]any{"allowedAnalyses": []any{"ANY_QUERY"}}},
					},
				})
			require.Equal(t, http.StatusOK, ruleRec.Code, ruleRec.Body.String())

			delRec := doRequest(t, e, http.MethodDelete, "/memberships/"+mID+"/intermediateTables/"+tableID, nil)
			require.Equal(t, http.StatusOK, delRec.Code, delRec.Body.String())

			getRuleRec := doRequest(t, e, http.MethodGet,
				"/memberships/"+mID+"/intermediateTables/"+tableID+"/analysisRule/CUSTOM", nil)
			assert.Equal(t, http.StatusNotFound, getRuleRec.Code)

			getTableRec := doRequest(t, e, http.MethodGet, "/memberships/"+mID+"/intermediateTables/"+tableID, nil)
			assert.Equal(t, http.StatusNotFound, getTableRec.Code)
		})
	}
}
