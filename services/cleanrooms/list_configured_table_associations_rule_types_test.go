package cleanrooms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestListConfiguredTableAssociations_SummaryHasAnalysisRuleTypes proves
// ListConfiguredTableAssociations' item shape decodes the real, optional
// "analysisRuleTypes" key (types.ConfiguredTableAssociationSummary,
// confirmed against
// awsRestjson1_deserializeDocumentConfiguredTableAssociationSummary in
// cleanrooms@v1.49.4's deserializers.go). The backend already tracks
// AnalysisRuleTypes per association (appended by
// CreateConfiguredTableAssociationAnalysisRule and correctly surfaced by
// the singular GetConfiguredTableAssociation) but never copied it into the
// list summary struct at all -- a real client's list arrived with this
// field permanently absent regardless of how many analysis rules were
// attached.
func TestListConfiguredTableAssociations_SummaryHasAnalysisRuleTypes(t *testing.T) {
	t.Parallel()
	e := newTestServer(t)

	colRec := doRequest(t, e, http.MethodPost, "/collaborations", map[string]any{
		"name": "collab-cta-rt", "creatorDisplayName": "Me",
		"creatorMemberAbilities": []string{"CAN_QUERY"},
		"members":                []any{}, "queryLogStatus": "DISABLED",
	})
	var colResp map[string]any
	require.NoError(t, json.Unmarshal(colRec.Body.Bytes(), &colResp))
	collabID := colResp["collaboration"].(map[string]any)["id"].(string)

	memRec := doRequest(t, e, http.MethodPost, "/memberships", map[string]any{
		"collaborationIdentifier": collabID, "queryLogStatus": "DISABLED",
	})
	var memResp map[string]any
	require.NoError(t, json.Unmarshal(memRec.Body.Bytes(), &memResp))
	memID := memResp["membership"].(map[string]any)["id"].(string)

	ctRec := doRequest(t, e, http.MethodPost, "/configuredTables", map[string]any{
		"name": "ct-rt", "description": "desc",
		"tableReference": map[string]any{"glue": map[string]any{"databaseName": "db", "tableName": "t"}},
		"allowedColumns": []string{"id"}, "analysisMethod": "DIRECT_QUERY",
	})
	var ctResp map[string]any
	require.NoError(t, json.Unmarshal(ctRec.Body.Bytes(), &ctResp))
	ctID := ctResp["configuredTable"].(map[string]any)["id"].(string)

	ctaRec := doRequest(t, e, http.MethodPost, "/memberships/"+memID+"/configuredTableAssociations", map[string]any{
		"name":                      "cta-rt",
		"configuredTableIdentifier": ctID,
		"roleArn":                   "arn:aws:iam::123:role/foo",
	})
	require.Equal(t, http.StatusOK, ctaRec.Code)
	var ctaResp map[string]any
	require.NoError(t, json.Unmarshal(ctaRec.Body.Bytes(), &ctaResp))
	ctaID := ctaResp["configuredTableAssociation"].(map[string]any)["id"].(string)

	ruleRec := doRequest(
		t, e, http.MethodPost,
		"/memberships/"+memID+"/configuredTableAssociations/"+ctaID+"/analysisRule",
		map[string]any{
			"analysisRuleType":   "AGGREGATION",
			"analysisRulePolicy": map[string]any{"v1": map[string]any{}},
		},
	)
	require.Equal(t, http.StatusOK, ruleRec.Code)

	listRec := doRequest(t, e, http.MethodGet, "/memberships/"+memID+"/configuredTableAssociations", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp struct {
		Summaries []map[string]any `json:"configuredTableAssociationSummaries"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Summaries, 1)

	ruleTypes, ok := listResp.Summaries[0]["analysisRuleTypes"].([]any)
	require.True(t, ok, "analysisRuleTypes must decode as a present list, not be absent from the summary")
	require.Equal(t, []any{"AGGREGATION"}, ruleTypes)
}
