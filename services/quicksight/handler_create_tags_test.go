package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateOps_TagsRoundTrip drives every QuickSight Create op whose real
// Input struct accepts Tags (quicksight@v1.123.1) through the handler and
// asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl). CreateBrand, CreateNamespace, and RegisterUser accepted
// a Tags field in the request body but never applied it -- the backend
// Create methods didn't even take a tags parameter, a decode-drop identical
// in shape to the forecast/rds bug this issue tracks. CreateOAuthClientApplication
// was suspected broken by an initial partial grep but a full read showed it
// already threads tagsFromBody through correctly -- kept here as regression
// coverage, matching the route53resolver near-miss this issue's notes warn
// about (do not trust a partial grep).
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tagBody := []any{map[string]any{"Key": "env", "Value": "prod"}}

	t.Run("createbrand", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		rec := doRequest(t, h, http.MethodPost, accountPath("/brands/tagged-brand"), map[string]any{
			"BrandDefinition": map[string]any{"BrandName": "Tagged"},
			"Tags":            tagBody,
		})
		require.Equal(t, http.StatusOK, rec.Code)
		detail := parseBody(t, rec)["BrandDetail"].(map[string]any)
		arn := detail["Arn"].(string)

		tagsRec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/resources/%s/tags", arn), nil)
		require.Equal(t, http.StatusOK, tagsRec.Code)
		assertHasEnvProdTag(t, parseBody(t, tagsRec))
	})

	t.Run("createnamespace", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		rec := doRequest(t, h, http.MethodPost, accountPath(""), map[string]any{
			"Namespace": "tagged-ns",
			"Tags":      tagBody,
		})
		require.Equal(t, http.StatusOK, rec.Code)
		arn := parseBody(t, rec)["Arn"].(string)

		tagsRec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/resources/%s/tags", arn), nil)
		require.Equal(t, http.StatusOK, tagsRec.Code)
		assertHasEnvProdTag(t, parseBody(t, tagsRec))
	})

	t.Run("registeruser", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		rec := doRequest(t, h, http.MethodPost, nsPath("/users"), map[string]any{
			"UserName": "tagged-user",
			"Email":    "tagged-user@example.com",
			"Tags":     tagBody,
		})
		require.Equal(t, http.StatusOK, rec.Code)
		user := parseBody(t, rec)["User"].(map[string]any)
		arn := user["Arn"].(string)

		tagsRec := doRequest(t, h, http.MethodGet, fmt.Sprintf("/resources/%s/tags", arn), nil)
		require.Equal(t, http.StatusOK, tagsRec.Code)
		assertHasEnvProdTag(t, parseBody(t, tagsRec))
	})

	// createoauthclientapplication is not covered here: it is already
	// wired correctly (tagsFromBody threaded through in handler_oauth.go)
	// and verified clean by TestQuickSight_OAuthClientApp_CreateTags in
	// handler_oauth_test.go.
}

func assertHasEnvProdTag(t *testing.T, body map[string]any) {
	t.Helper()

	items, ok := body["Tags"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	item, ok := items[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "env", item["Key"])
	assert.Equal(t, "prod", item["Value"])
}
