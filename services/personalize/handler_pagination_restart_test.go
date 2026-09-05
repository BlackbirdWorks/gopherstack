package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPersonalize_ListCampaigns_Pagination_DeletedMidPage proves that
// deleting the campaign a cursor names does not restart pagination at page
// one.
func TestPersonalize_ListCampaigns_Pagination_DeletedMidPage(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	names := []string{"camp-a", "camp-b", "camp-c", "camp-d", "camp-e"}
	for _, name := range names {
		svArn := personalizeCreateSolutionVersion(t, h, "sol-"+name)
		rec := personalizeDo(t, h, "CreateCampaign", map[string]any{
			"name":               name,
			"solutionVersionArn": svArn,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := personalizeDo(t, h, "ListCampaigns", map[string]any{"maxResults": float64(2)})
	require.Equal(t, http.StatusOK, rec.Code)

	resp1 := personalizeUnmarshal(t, rec)
	nextToken, ok := resp1["nextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, nextToken)

	rec = personalizeDo(t, h, "DeleteCampaign", map[string]any{"campaignArn": nextToken})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = personalizeDo(t, h, "ListCampaigns", map[string]any{
		"maxResults": float64(2),
		"nextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp2 := personalizeUnmarshal(t, rec)
	page2, _ := resp2["campaigns"].([]any)

	restarted := false

	for _, item := range page2 {
		entry, _ := item.(map[string]any)
		if entry["name"] == "camp-a" || entry["name"] == "camp-b" {
			restarted = true
		}
	}

	assert.False(t, restarted, "cursor must not restart pagination at page one after its item is deleted")
}

// TestPersonalize_ListRecipes_Pagination_StaleTokenDoesNotRestart proves that
// a forged/unresolvable nextToken does not restart ListRecipes at page one.
// The built-in recipe catalog can't be mutated, so the hostile scenario is a
// forged token rather than deletion.
func TestPersonalize_ListRecipes_Pagination_StaleTokenDoesNotRestart(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "ListRecipes", map[string]any{"maxResults": float64(3)})
	require.Equal(t, http.StatusOK, rec.Code)

	resp1 := personalizeUnmarshal(t, rec)
	page1, _ := resp1["recipes"].([]any)
	require.Len(t, page1, 3)

	page1ARNs := map[string]bool{}
	for _, item := range page1 {
		entry, _ := item.(map[string]any)
		page1ARNs[entry["recipeArn"].(string)] = true
	}

	rec = personalizeDo(t, h, "ListRecipes", map[string]any{
		"maxResults": float64(3),
		"nextToken":  "arn:aws:personalize:::recipe/does-not-exist",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp2 := personalizeUnmarshal(t, rec)
	page2, _ := resp2["recipes"].([]any)

	for _, item := range page2 {
		entry, _ := item.(map[string]any)
		arn, _ := entry["recipeArn"].(string)
		assert.False(t, page1ARNs[arn], "a forged nextToken must not restart pagination at page one")
	}
}
