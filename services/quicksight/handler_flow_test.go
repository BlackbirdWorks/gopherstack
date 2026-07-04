package quicksight_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

func seedTestFlow(b *quicksight.InMemoryBackend, flowID, name string) {
	quicksight.SeedFlow(b, testAccountID, &quicksight.Flow{
		CreatedTime:     time.Now().UTC(),
		LastUpdatedTime: time.Now().UTC(),
		FlowID:          flowID,
		Name:            name,
		PublishState:    "PUBLISHED",
	})
}

// ---- GetFlowMetadata: found and not-found ----

func TestQuickSight_GetFlowMetadata(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	seedTestFlow(backend, "flow1", "My Flow")
	h := quicksight.NewHandler(backend)

	rec := doRequest(t, h, http.MethodGet, accountPath("/flows/flow1/metadata"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	assert.Equal(t, "flow1", body["FlowId"])
	assert.Equal(t, "My Flow", body["Name"])
	assert.Equal(t, "PUBLISHED", body["PublishState"])
	assert.Contains(t, body["Arn"], "arn:aws:quicksight:us-east-1:000000000000:flow/flow1")

	missingRec := doRequest(t, h, http.MethodGet, accountPath("/flows/notexist/metadata"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
	assert.Equal(t, "ResourceNotFoundException", parseBody(t, missingRec)["Code"])
}

// ---- ListFlows pagination ----

func TestQuickSight_ListFlows_Pagination(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		seedTestFlow(backend, id, id)
	}
	h := quicksight.NewHandler(backend)

	rec := doRequest(t, h, http.MethodGet, accountPath("/flows?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["FlowSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	next, ok := body["NextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, next)

	seen := map[string]bool{}
	for _, it := range items {
		m := it.(map[string]any)
		seen[m["FlowId"].(string)] = true
	}

	page2 := doRequest(t, h, http.MethodGet, accountPath("/flows?max-results=2&next-token="+next), nil)
	require.Equal(t, http.StatusOK, page2.Code)
	items2 := parseBody(t, page2)["FlowSummaryList"].([]any)
	assert.Len(t, items2, 2)
	for _, it := range items2 {
		m := it.(map[string]any)
		assert.False(t, seen[m["FlowId"].(string)], "page 2 must not repeat page 1 items")
	}
}

// ---- SearchFlows filters by assetName ----

func TestQuickSight_SearchFlows(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	seedTestFlow(backend, "f1", "Onboarding Flow")
	seedTestFlow(backend, "f2", "Onboarding Follow-up")
	seedTestFlow(backend, "f3", "Billing Flow")
	h := quicksight.NewHandler(backend)

	rec := doRequest(t, h, http.MethodPost, accountPath("/flows/searchFlows"), map[string]any{
		"Filters": []any{
			map[string]any{"Name": "assetName", "Operator": "StringLike", "Value": "Onboarding"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	items := parseBody(t, rec)["FlowSummaryList"].([]any)
	assert.Len(t, items, 2)
}

// ---- Flow permissions grant/revoke ----

func TestQuickSight_FlowPermissions(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	seedTestFlow(backend, "flow1", "My Flow")
	h := quicksight.NewHandler(backend)

	updateRec := doRequest(t, h, http.MethodPut, accountPath("/flows/flow1/permissions"), map[string]any{
		"GrantPermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
				"Actions":   []any{"quicksight:GetFlow"},
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)
	perms := parseBody(t, updateRec)["Permissions"].([]any)
	require.Len(t, perms, 1)

	describeRec := doRequest(t, h, http.MethodGet, accountPath("/flows/flow1/permissions"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	describePerms := parseBody(t, describeRec)["Permissions"].([]any)
	require.Len(t, describePerms, 1)

	revokeRec := doRequest(t, h, http.MethodPut, accountPath("/flows/flow1/permissions"), map[string]any{
		"RevokePermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
				"Actions":   []any{"quicksight:GetFlow"},
			},
		},
	})
	require.Equal(t, http.StatusOK, revokeRec.Code)
	assert.Empty(t, parseBody(t, revokeRec)["Permissions"])

	missingRec := doRequest(t, h, http.MethodGet, accountPath("/flows/notexist/permissions"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
}
