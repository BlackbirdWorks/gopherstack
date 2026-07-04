package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Theme CRUD round-trip, versioning, not-found, and duplicate errors ----

func TestQuickSight_ThemeCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create.
	createRec := doRequest(t, h, http.MethodPost, accountPath("/themes/thm1"), map[string]any{
		"Name": "Theme1", "BaseThemeId": "SEASIDE",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := parseBody(t, createRec)
	assert.Equal(t, "thm1", createBody["ThemeId"])
	assert.Contains(t, createBody["Arn"], "arn:aws:quicksight:us-east-1:000000000000:theme/thm1")
	assert.Equal(t, "CREATION_SUCCESSFUL", createBody["CreationStatus"])

	// Duplicate create -> ResourceExistsException.
	dupRec := doRequest(t, h, http.MethodPost, accountPath("/themes/thm1"), map[string]any{"Name": "x"})
	assert.Equal(t, http.StatusConflict, dupRec.Code)
	assert.Equal(t, "ResourceExistsException", parseBody(t, dupRec)["Code"])

	// Describe -> version 1.
	describeRec := doRequest(t, h, http.MethodGet, accountPath("/themes/thm1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	describeBody := parseBody(t, describeRec)
	theme, ok := describeBody["Theme"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Theme1", theme["Name"])
	assert.Equal(t, "CUSTOM", theme["Type"])
	version, ok := theme["Version"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 1, version["VersionNumber"], 0)
	assert.Equal(t, "SEASIDE", version["BaseThemeId"])

	// Describe missing -> 404.
	missingRec := doRequest(t, h, http.MethodGet, accountPath("/themes/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
	assert.Equal(t, "ResourceNotFoundException", parseBody(t, missingRec)["Code"])

	// Update -> creates version 2.
	updateRec := doRequest(t, h, http.MethodPut, accountPath("/themes/thm1"), map[string]any{"Name": "Theme1Renamed"})
	require.Equal(t, http.StatusOK, updateRec.Code)
	updateBody := parseBody(t, updateRec)
	assert.Equal(t, "thm1", updateBody["ThemeId"])
	assert.Contains(t, updateBody["VersionArn"], "/version/2")

	describeAfterUpdate := doRequest(t, h, http.MethodGet, accountPath("/themes/thm1"), nil)
	afterBody := parseBody(t, describeAfterUpdate)
	afterTheme, ok := afterBody["Theme"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Theme1Renamed", afterTheme["Name"])
	afterVersion, ok := afterTheme["Version"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 2, afterVersion["VersionNumber"], 0)
	// BaseThemeId carries forward from the previous version when not supplied.
	assert.Equal(t, "SEASIDE", afterVersion["BaseThemeId"])

	// Update missing -> 404.
	updateMissingRec := doRequest(t, h, http.MethodPut, accountPath("/themes/notexist"), map[string]any{"Name": "x"})
	assert.Equal(t, http.StatusNotFound, updateMissingRec.Code)

	// Delete.
	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/themes/thm1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Equal(t, "thm1", parseBody(t, deleteRec)["ThemeId"])

	// Delete missing -> 404.
	deleteMissingRec := doRequest(t, h, http.MethodDelete, accountPath("/themes/thm1"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- Theme missing Name -> validation error ----

func TestQuickSight_CreateTheme_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/themes/noname"), map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "InvalidParameterValueException", parseBody(t, rec)["Code"])
}

// ---- ListThemeVersions after multiple updates ----

func TestQuickSight_ListThemeVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/themes/vthm"), map[string]any{"Name": "V1"})
	doRequest(t, h, http.MethodPut, accountPath("/themes/vthm"), map[string]any{"Name": "V2"})
	doRequest(t, h, http.MethodPut, accountPath("/themes/vthm"), map[string]any{"Name": "V3"})

	rec := doRequest(t, h, http.MethodGet, accountPath("/themes/vthm/versions"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	versions, ok := body["ThemeVersionSummaryList"].([]any)
	require.True(t, ok)
	require.Len(t, versions, 3)

	seen := make([]float64, 0, len(versions))
	for _, v := range versions {
		m, isMap := v.(map[string]any)
		require.True(t, isMap)
		seen = append(seen, m["VersionNumber"].(float64))
	}
	assert.Equal(t, []float64{1, 2, 3}, seen)

	missingRec := doRequest(t, h, http.MethodGet, accountPath("/themes/notexist/versions"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
}

// ---- Theme alias CRUD, not-found, and duplicate errors ----

func TestQuickSight_ThemeAliasCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/themes/athm"), map[string]any{"Name": "A1"})
	doRequest(t, h, http.MethodPut, accountPath("/themes/athm"), map[string]any{"Name": "A2"})

	createRec := doRequest(t, h, http.MethodPost, accountPath("/themes/athm/aliases/PROD"), map[string]any{
		"ThemeVersionNumber": float64(1),
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	alias, ok := parseBody(t, createRec)["ThemeAlias"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "PROD", alias["AliasName"])
	assert.InDelta(t, 1, alias["ThemeVersionNumber"], 0)

	dupRec := doRequest(t, h, http.MethodPost, accountPath("/themes/athm/aliases/PROD"), map[string]any{
		"ThemeVersionNumber": float64(1),
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)
	assert.Equal(t, "ResourceExistsException", parseBody(t, dupRec)["Code"])

	badVersionRec := doRequest(t, h, http.MethodPost, accountPath("/themes/athm/aliases/BAD"), map[string]any{
		"ThemeVersionNumber": float64(99),
	})
	assert.Equal(t, http.StatusNotFound, badVersionRec.Code)

	describeRec := doRequest(t, h, http.MethodGet, accountPath("/themes/athm/aliases/PROD"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	describeAlias, ok := parseBody(t, describeRec)["ThemeAlias"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 1, describeAlias["ThemeVersionNumber"], 0)

	missingAliasRec := doRequest(t, h, http.MethodGet, accountPath("/themes/athm/aliases/NOPE"), nil)
	assert.Equal(t, http.StatusNotFound, missingAliasRec.Code)

	// List aliases: PROD + auto-managed $LATEST.
	listRec := doRequest(t, h, http.MethodGet, accountPath("/themes/athm/aliases"), nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	aliases, ok := parseBody(t, listRec)["ThemeAliasList"].([]any)
	require.True(t, ok)
	assert.Len(t, aliases, 2)

	updateRec := doRequest(t, h, http.MethodPut, accountPath("/themes/athm/aliases/PROD"), map[string]any{
		"ThemeVersionNumber": float64(2),
	})
	require.Equal(t, http.StatusOK, updateRec.Code)
	updatedAlias, ok := parseBody(t, updateRec)["ThemeAlias"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 2, updatedAlias["ThemeVersionNumber"], 0)

	updateMissingRec := doRequest(t, h, http.MethodPut, accountPath("/themes/athm/aliases/NOPE"), map[string]any{
		"ThemeVersionNumber": float64(1),
	})
	assert.Equal(t, http.StatusNotFound, updateMissingRec.Code)

	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/themes/athm/aliases/PROD"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Equal(t, "athm", parseBody(t, deleteRec)["ThemeId"])

	deleteMissingRec := doRequest(t, h, http.MethodDelete, accountPath("/themes/athm/aliases/PROD"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- Theme permissions: grant, revoke, describe ----

func TestQuickSight_ThemePermissions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/themes/pthm"), map[string]any{"Name": "P1"})

	describeEmpty := doRequest(t, h, http.MethodGet, accountPath("/themes/pthm/permissions"), nil)
	require.Equal(t, http.StatusOK, describeEmpty.Code)
	emptyBody := parseBody(t, describeEmpty)
	assert.Equal(t, "pthm", emptyBody["ThemeId"])
	emptyPerms, ok := emptyBody["Permissions"].([]any)
	require.True(t, ok)
	assert.Empty(t, emptyPerms)

	grantRec := doRequest(t, h, http.MethodPut, accountPath("/themes/pthm/permissions"), map[string]any{
		"GrantPermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
				"Actions":   []any{"quicksight:DescribeTheme", "quicksight:UpdateTheme"},
			},
		},
	})
	require.Equal(t, http.StatusOK, grantRec.Code)
	perms, ok := parseBody(t, grantRec)["Permissions"].([]any)
	require.True(t, ok)
	require.Len(t, perms, 1)

	revokeRec := doRequest(t, h, http.MethodPut, accountPath("/themes/pthm/permissions"), map[string]any{
		"RevokePermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
				"Actions":   []any{"quicksight:UpdateTheme"},
			},
		},
	})
	require.Equal(t, http.StatusOK, revokeRec.Code)
	revokedPerms, ok := parseBody(t, revokeRec)["Permissions"].([]any)
	require.True(t, ok)
	require.Len(t, revokedPerms, 1)
	rp0, ok := revokedPerms[0].(map[string]any)
	require.True(t, ok)
	rActions, ok := rp0["Actions"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"quicksight:DescribeTheme"}, rActions)

	permsMissing := doRequest(t, h, http.MethodGet, accountPath("/themes/notexist/permissions"), nil)
	assert.Equal(t, http.StatusNotFound, permsMissing.Code)
}

// ---- ListThemes pagination ----

func TestQuickSight_ListThemes_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		doRequest(t, h, http.MethodPost, accountPath("/themes/"+id), map[string]any{"Name": id})
	}

	rec := doRequest(t, h, http.MethodGet, accountPath("/themes?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["ThemeSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	next, ok := body["NextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, next)

	seen := map[string]bool{}
	for _, it := range items {
		m1, isMap := it.(map[string]any)
		require.True(t, isMap)
		seen[m1["ThemeId"].(string)] = true
	}

	rec2 := doRequest(t, h, http.MethodGet, accountPath(fmt.Sprintf("/themes?max-results=2&next-token=%s", next)), nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	body2 := parseBody(t, rec2)
	items2, ok := body2["ThemeSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, items2, 2)
	for _, it := range items2 {
		m2, isMap := it.(map[string]any)
		require.True(t, isMap)
		assert.False(t, seen[m2["ThemeId"].(string)], "page 2 must not repeat page 1 items")
	}
}

// ---- DeleteTheme with a specific VersionNumber only removes that version ----

func TestQuickSight_DeleteTheme_SpecificVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/themes/dvthm"), map[string]any{"Name": "D1"})
	doRequest(t, h, http.MethodPut, accountPath("/themes/dvthm"), map[string]any{"Name": "D2"})

	delRec := doRequest(t, h, http.MethodDelete, accountPath("/themes/dvthm?version-number=1"), nil)
	require.Equal(t, http.StatusOK, delRec.Code)

	describeRec := doRequest(t, h, http.MethodGet, accountPath("/themes/dvthm"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)

	describeV1Rec := doRequest(t, h, http.MethodGet, accountPath("/themes/dvthm?version-number=1"), nil)
	assert.Equal(t, http.StatusNotFound, describeV1Rec.Code)
}
