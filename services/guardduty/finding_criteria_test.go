package guardduty_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// listFindings drives POST /detector/{id}/findings with an optional request
// body and returns the decoded response.
func listFindings(t *testing.T, h *guardduty.Handler, detectorID string, body any) map[string]any {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/findings", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

// createSample creates one sample finding of the given type and returns its
// ID, found by set difference against the finding IDs that existed before
// the call -- ListFindings' default order is ID-ascending, not insertion
// order, so "the last ID returned" is not reliably "the ID just created".
func createSample(t *testing.T, h *guardduty.Handler, detectorID, findingType string) string {
	t.Helper()

	before := listFindingIDSet(t, h, detectorID)

	doRequest(t, h, http.MethodPost, "/detector/"+detectorID+"/findings/create", map[string]any{
		"findingTypes": []string{findingType},
	})

	after := listFindings(t, h, detectorID, nil)
	ids, _ := after["findingIds"].([]any)
	require.NotEmpty(t, ids)

	for _, v := range ids {
		id, _ := v.(string)
		if !before[id] {
			return id
		}
	}

	t.Fatalf("createSample: no new finding ID found after creating %q", findingType)

	return ""
}

func listFindingIDSet(t *testing.T, h *guardduty.Handler, detectorID string) map[string]bool {
	t.Helper()

	resp := listFindings(t, h, detectorID, nil)
	ids, _ := resp["findingIds"].([]any)

	set := make(map[string]bool, len(ids))
	for _, v := range ids {
		if id, ok := v.(string); ok {
			set[id] = true
		}
	}

	return set
}

// TestListFindings_FindingCriteria locks the previously-missing
// FindingCriteria filtering gap: ListFindings ignored findingCriteria
// entirely and always returned every finding on the detector.
func TestListFindings_FindingCriteria(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	detID := createTestDetector(t, h)

	createSample(t, h, detID, "Backdoor:EC2/DenialOfService.Tcp")
	wantID := createSample(t, h, detID, "UnauthorizedAccess:IAMUser/ConsoleLoginSuccess.B")

	resp := listFindings(t, h, detID, map[string]any{
		"findingCriteria": map[string]any{
			"criterion": map[string]any{
				"type": map[string]any{
					"equals": []string{"UnauthorizedAccess:IAMUser/ConsoleLoginSuccess.B"},
				},
			},
		},
	})

	ids, ok := resp["findingIds"].([]any)
	require.True(t, ok)
	require.Len(t, ids, 1, "equals criterion must filter out the non-matching finding")
	assert.Equal(t, wantID, ids[0])
}

// TestListFindings_FindingCriteria_NestedAttribute locks that dot-path
// attributes (e.g. service.archived) resolve against nested finding fields,
// not just top-level ones.
func TestListFindings_FindingCriteria_NestedAttribute(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	detID := createTestDetector(t, h)

	fid := createSample(t, h, detID, "Recon:IAMUser/TorIPCaller")

	doRequest(t, h, http.MethodPost, "/detector/"+detID+"/findings/archive", map[string]any{
		"findingIds": []string{fid},
	})

	archived := listFindings(t, h, detID, map[string]any{
		"findingCriteria": map[string]any{
			"criterion": map[string]any{
				"service.archived": map[string]any{"equals": []string{"true"}},
			},
		},
	})
	archivedIDs, _ := archived["findingIds"].([]any)
	require.Len(t, archivedIDs, 1)
	assert.Equal(t, fid, archivedIDs[0])

	unarchived := listFindings(t, h, detID, map[string]any{
		"findingCriteria": map[string]any{
			"criterion": map[string]any{
				"service.archived": map[string]any{"equals": []string{"false"}},
			},
		},
	})
	unarchivedIDs, _ := unarchived["findingIds"].([]any)
	assert.Empty(t, unarchivedIDs)
}

// TestListFindings_SortCriteria locks the previously-missing SortCriteria
// support: results were always ID-ascending regardless of request.
func TestListFindings_SortCriteria(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	detID := createTestDetector(t, h)

	doRequest(t, h, http.MethodPost, "/detector/"+detID+"/findings/create", map[string]any{
		"findingTypes": []string{"Backdoor:EC2/DenialOfService.Tcp", "Recon:IAMUser/TorIPCaller"},
	})

	asc := listFindings(t, h, detID, map[string]any{
		"sortCriteria": map[string]any{"attributeName": "type", "orderBy": "ASC"},
	})
	desc := listFindings(t, h, detID, map[string]any{
		"sortCriteria": map[string]any{"attributeName": "type", "orderBy": "DESC"},
	})

	ascIDs, _ := asc["findingIds"].([]any)
	descIDs, _ := desc["findingIds"].([]any)
	require.Len(t, ascIDs, 2)
	require.Len(t, descIDs, 2)
	assert.Equal(t, ascIDs[0], descIDs[1], "ASC/DESC must be reverse orderings of each other")
	assert.Equal(t, ascIDs[1], descIDs[0])
}

// TestListFindings_Pagination locks the previously-missing MaxResults/
// NextToken support: ListFindings always returned the full result set in
// one page and never emitted nextToken.
func TestListFindings_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	detID := createTestDetector(t, h)

	doRequest(t, h, http.MethodPost, "/detector/"+detID+"/findings/create", map[string]any{
		"findingTypes": []string{
			"Backdoor:EC2/DenialOfService.Tcp",
			"Recon:IAMUser/TorIPCaller",
			"UnauthorizedAccess:IAMUser/ConsoleLoginSuccess.B",
		},
	})

	page1 := listFindings(t, h, detID, map[string]any{"maxResults": 2})
	ids1, _ := page1["findingIds"].([]any)
	require.Len(t, ids1, 2)
	nextToken, ok := page1["nextToken"].(string)
	require.True(t, ok, "a partial page must include nextToken")
	require.NotEmpty(t, nextToken)

	page2 := listFindings(t, h, detID, map[string]any{"maxResults": 2, "nextToken": nextToken})
	ids2, _ := page2["findingIds"].([]any)
	require.Len(t, ids2, 1, "the final page must contain the remaining item")
	_, hasNext := page2["nextToken"]
	assert.False(t, hasNext, "the exhausted final page must not include nextToken")

	assert.NotEqual(t, ids1[0], ids2[0])
	assert.NotEqual(t, ids1[1], ids2[0])
}

// TestListFindings_Empty_State_StillReturnsEmptyArray guards that the
// FindingCriteria/pagination rework preserves the pre-existing behavior of
// returning [] (not null) for findingIds on an empty detector.
func TestListFindings_Empty_State_StillReturnsEmptyArray(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	detID := createTestDetector(t, h)

	resp := listFindings(t, h, detID, nil)
	ids, ok := resp["findingIds"].([]any)
	require.True(t, ok, "findingIds must be an array, got %T", resp["findingIds"])
	assert.Empty(t, ids)
}
