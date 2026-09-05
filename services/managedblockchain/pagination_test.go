package managedblockchain_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_ListNetworks_Pagination verifies ListNetworks honors
// maxResults/nextToken (see serializers.go's SetQuery("maxResults") /
// SetQuery("nextToken") bindings, identical across every List* op in this
// service) instead of always returning the entire result set in one page,
// which PARITY.md previously flagged as a gap.
func TestHandler_ListNetworks_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		rec := doRequest(t, h, http.MethodPost, "/networks", map[string]any{
			"Name":                fmt.Sprintf("net-%d", i),
			"ClientRequestToken":  fmt.Sprintf("tok-net-%d", i),
			"MemberConfiguration": testMemberConfiguration("m1"),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	var page1 listNetworksPage

	rec := doRequestWithQuery(t, h, "/networks", map[string]string{"maxResults": "2"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	assert.Len(t, page1.Networks, 2)
	require.NotEmpty(t, page1.NextToken, "a 5-item list capped at maxResults=2 must return a NextToken")

	var page2 listNetworksPage

	rec = doRequestWithQuery(t, h, "/networks", map[string]string{"maxResults": "2", "nextToken": page1.NextToken})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
	assert.Len(t, page2.Networks, 2)
	require.NotEmpty(t, page2.NextToken)

	var page3 listNetworksPage

	rec = doRequestWithQuery(t, h, "/networks", map[string]string{"maxResults": "2", "nextToken": page2.NextToken})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page3))
	assert.Len(t, page3.Networks, 1)
	assert.Empty(t, page3.NextToken, "the final page must omit NextToken")

	// The three pages together must cover every network exactly once, with
	// no duplicates and no omissions, and a request with no maxResults must
	// still return everything in one page (the pre-pagination behavior).
	seen := make(map[string]bool)
	for _, batch := range []listNetworksPage{page1, page2, page3} {
		for _, n := range batch.Networks {
			assert.False(t, seen[n.ID], "network %s returned more than once across pages", n.ID)
			seen[n.ID] = true
		}
	}
	assert.Len(t, seen, 5)

	var unpaged listNetworksPage

	rec = doRequest(t, h, http.MethodGet, "/networks", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &unpaged))
	assert.Len(t, unpaged.Networks, 5)
	assert.Empty(t, unpaged.NextToken)
}

type listNetworksPage struct {
	NextToken string `json:"NextToken"`
	Networks  []struct {
		ID string `json:"Id"`
	} `json:"Networks"`
}

// TestHandler_ListMembers_Pagination verifies ListMembers' pagination
// independently of ListNetworks', since each List* handler wires the shared
// paginate() helper separately.
func TestHandler_ListMembers_Pagination(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	networkID, _ := createTestNetwork(t, h)

	for i := range 3 {
		invitationID := createTestInvitation(t, b, networkID, "test-net")
		rec := doRequest(t, h, http.MethodPost, "/networks/"+networkID+"/members", map[string]any{
			"InvitationId":        invitationID,
			"ClientRequestToken":  fmt.Sprintf("tok-mem-%d", i),
			"MemberConfiguration": testMemberConfiguration(fmt.Sprintf("member-%d", i)),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// 1 founding member (from createTestNetwork) + 3 more = 4 total.
	var page1 struct {
		NextToken string `json:"NextToken"`
		Members   []any  `json:"Members"`
	}

	rec := doRequestWithQuery(t, h, "/networks/"+networkID+"/members", map[string]string{"maxResults": "3"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	assert.Len(t, page1.Members, 3)
	require.NotEmpty(t, page1.NextToken)

	var page2 struct {
		NextToken string `json:"NextToken"`
		Members   []any  `json:"Members"`
	}

	rec = doRequestWithQuery(t, h, "/networks/"+networkID+"/members",
		map[string]string{"maxResults": "3", "nextToken": page1.NextToken})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
	assert.Len(t, page2.Members, 1)
	assert.Empty(t, page2.NextToken)
}
