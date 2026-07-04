package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Template CRUD round-trip, versioning, not-found, and duplicate errors ----

func TestQuickSight_TemplateCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create.
	createRec := doRequest(t, h, http.MethodPost, accountPath("/templates/tpl1"), map[string]any{"Name": "Tpl1"})
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := parseBody(t, createRec)
	assert.Equal(t, "tpl1", createBody["TemplateId"])
	assert.Contains(t, createBody["Arn"], "arn:aws:quicksight:us-east-1:000000000000:template/tpl1")
	assert.Equal(t, "CREATION_SUCCESSFUL", createBody["CreationStatus"])

	// Duplicate create -> ResourceExistsException.
	dupRec := doRequest(t, h, http.MethodPost, accountPath("/templates/tpl1"), map[string]any{"Name": "x"})
	assert.Equal(t, http.StatusConflict, dupRec.Code)
	dupBody := parseBody(t, dupRec)
	assert.Equal(t, "ResourceExistsException", dupBody["Code"])

	// Describe -> version 1.
	describeRec := doRequest(t, h, http.MethodGet, accountPath("/templates/tpl1"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	describeBody := parseBody(t, describeRec)
	tpl, ok := describeBody["Template"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Tpl1", tpl["Name"])
	version, ok := tpl["Version"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 1, version["VersionNumber"], 0)

	// Describe missing -> 404.
	missingRec := doRequest(t, h, http.MethodGet, accountPath("/templates/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
	assert.Equal(t, "ResourceNotFoundException", parseBody(t, missingRec)["Code"])

	// Update -> creates version 2.
	updateRec := doRequest(t, h, http.MethodPut, accountPath("/templates/tpl1"), map[string]any{"Name": "Tpl1Renamed"})
	require.Equal(t, http.StatusOK, updateRec.Code)
	updateBody := parseBody(t, updateRec)
	assert.Equal(t, "tpl1", updateBody["TemplateId"])
	assert.Contains(t, updateBody["VersionArn"], "/version/2")

	describeAfterUpdate := doRequest(t, h, http.MethodGet, accountPath("/templates/tpl1"), nil)
	afterBody := parseBody(t, describeAfterUpdate)
	afterTpl, ok := afterBody["Template"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Tpl1Renamed", afterTpl["Name"])
	afterVersion, ok := afterTpl["Version"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 2, afterVersion["VersionNumber"], 0)

	// Update missing -> 404.
	updateMissingRec := doRequest(t, h, http.MethodPut, accountPath("/templates/notexist"), map[string]any{"Name": "x"})
	assert.Equal(t, http.StatusNotFound, updateMissingRec.Code)

	// Delete.
	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/templates/tpl1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Equal(t, "tpl1", parseBody(t, deleteRec)["TemplateId"])

	// Delete missing -> 404.
	deleteMissingRec := doRequest(t, h, http.MethodDelete, accountPath("/templates/tpl1"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- Template missing Name/TemplateId -> validation error ----

func TestQuickSight_CreateTemplate_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, accountPath("/templates/noname"), map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "InvalidParameterValueException", parseBody(t, rec)["Code"])
}

// ---- ListTemplateVersions after multiple updates ----

func TestQuickSight_ListTemplateVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/templates/vtpl"), map[string]any{"Name": "V1"})
	doRequest(t, h, http.MethodPut, accountPath("/templates/vtpl"), map[string]any{"Name": "V2"})
	doRequest(t, h, http.MethodPut, accountPath("/templates/vtpl"), map[string]any{"Name": "V3"})

	rec := doRequest(t, h, http.MethodGet, accountPath("/templates/vtpl/versions"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	versions, ok := body["TemplateVersionSummaryList"].([]any)
	require.True(t, ok)
	require.Len(t, versions, 3)

	seen := make([]float64, 0, len(versions))
	for _, v := range versions {
		m, isMap := v.(map[string]any)
		require.True(t, isMap)
		seen = append(seen, m["VersionNumber"].(float64))
	}
	assert.Equal(t, []float64{1, 2, 3}, seen)

	// ListTemplateVersions on missing template -> 404.
	missingRec := doRequest(t, h, http.MethodGet, accountPath("/templates/notexist/versions"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
}

// ---- Template alias CRUD, not-found, and duplicate errors ----

func TestQuickSight_TemplateAliasCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/templates/atpl"), map[string]any{"Name": "A1"})
	doRequest(t, h, http.MethodPut, accountPath("/templates/atpl"), map[string]any{"Name": "A2"})

	// Create alias pointing at version 1.
	createRec := doRequest(t, h, http.MethodPost, accountPath("/templates/atpl/aliases/PROD"), map[string]any{
		"TemplateVersionNumber": float64(1),
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := parseBody(t, createRec)
	alias, ok := createBody["TemplateAlias"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "PROD", alias["AliasName"])
	assert.InDelta(t, 1, alias["TemplateVersionNumber"], 0)

	// Duplicate alias create -> 409.
	dupRec := doRequest(t, h, http.MethodPost, accountPath("/templates/atpl/aliases/PROD"), map[string]any{
		"TemplateVersionNumber": float64(1),
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)
	assert.Equal(t, "ResourceExistsException", parseBody(t, dupRec)["Code"])

	// Create alias with nonexistent version -> 404.
	badVersionRec := doRequest(t, h, http.MethodPost, accountPath("/templates/atpl/aliases/BAD"), map[string]any{
		"TemplateVersionNumber": float64(99),
	})
	assert.Equal(t, http.StatusNotFound, badVersionRec.Code)

	// Describe alias.
	describeRec := doRequest(t, h, http.MethodGet, accountPath("/templates/atpl/aliases/PROD"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	describeAlias, ok := parseBody(t, describeRec)["TemplateAlias"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 1, describeAlias["TemplateVersionNumber"], 0)

	// Describe missing alias -> 404.
	missingAliasRec := doRequest(t, h, http.MethodGet, accountPath("/templates/atpl/aliases/NOPE"), nil)
	assert.Equal(t, http.StatusNotFound, missingAliasRec.Code)

	// List aliases: PROD + auto-managed $LATEST.
	listRec := doRequest(t, h, http.MethodGet, accountPath("/templates/atpl/aliases"), nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	listBody := parseBody(t, listRec)
	aliases, ok := listBody["TemplateAliasList"].([]any)
	require.True(t, ok)
	assert.Len(t, aliases, 2)

	// Update alias to point at version 2.
	updateRec := doRequest(t, h, http.MethodPut, accountPath("/templates/atpl/aliases/PROD"), map[string]any{
		"TemplateVersionNumber": float64(2),
	})
	require.Equal(t, http.StatusOK, updateRec.Code)
	updatedAlias, ok := parseBody(t, updateRec)["TemplateAlias"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 2, updatedAlias["TemplateVersionNumber"], 0)

	// Update missing alias -> 404.
	updateMissingRec := doRequest(t, h, http.MethodPut, accountPath("/templates/atpl/aliases/NOPE"), map[string]any{
		"TemplateVersionNumber": float64(1),
	})
	assert.Equal(t, http.StatusNotFound, updateMissingRec.Code)

	// Delete alias.
	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/templates/atpl/aliases/PROD"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Equal(t, "atpl", parseBody(t, deleteRec)["TemplateId"])

	// Delete missing alias -> 404.
	deleteMissingRec := doRequest(t, h, http.MethodDelete, accountPath("/templates/atpl/aliases/PROD"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- Template permissions: grant, revoke, describe ----

func TestQuickSight_TemplatePermissions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/templates/ptpl"), map[string]any{"Name": "P1"})

	describeEmpty := doRequest(t, h, http.MethodGet, accountPath("/templates/ptpl/permissions"), nil)
	require.Equal(t, http.StatusOK, describeEmpty.Code)
	emptyBody := parseBody(t, describeEmpty)
	assert.Equal(t, "ptpl", emptyBody["TemplateId"])
	emptyPerms, ok := emptyBody["Permissions"].([]any)
	require.True(t, ok)
	assert.Empty(t, emptyPerms)

	grantRec := doRequest(t, h, http.MethodPut, accountPath("/templates/ptpl/permissions"), map[string]any{
		"GrantPermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
				"Actions":   []any{"quicksight:DescribeTemplate", "quicksight:UpdateTemplate"},
			},
		},
	})
	require.Equal(t, http.StatusOK, grantRec.Code)
	grantBody := parseBody(t, grantRec)
	perms, ok := grantBody["Permissions"].([]any)
	require.True(t, ok)
	require.Len(t, perms, 1)

	revokeRec := doRequest(t, h, http.MethodPut, accountPath("/templates/ptpl/permissions"), map[string]any{
		"RevokePermissions": []any{
			map[string]any{
				"Principal": "arn:aws:quicksight:us-east-1:000000000000:user/default/alice",
				"Actions":   []any{"quicksight:UpdateTemplate"},
			},
		},
	})
	require.Equal(t, http.StatusOK, revokeRec.Code)
	revokeBody := parseBody(t, revokeRec)
	revokedPerms, ok := revokeBody["Permissions"].([]any)
	require.True(t, ok)
	require.Len(t, revokedPerms, 1)
	rp0, ok := revokedPerms[0].(map[string]any)
	require.True(t, ok)
	rActions, ok := rp0["Actions"].([]any)
	require.True(t, ok)
	assert.Equal(t, []any{"quicksight:DescribeTemplate"}, rActions)

	permsMissing := doRequest(t, h, http.MethodGet, accountPath("/templates/notexist/permissions"), nil)
	assert.Equal(t, http.StatusNotFound, permsMissing.Code)
}

// ---- ListTemplates pagination ----

func TestQuickSight_ListTemplates_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		doRequest(t, h, http.MethodPost, accountPath("/templates/"+id), map[string]any{"Name": id})
	}

	rec := doRequest(t, h, http.MethodGet, accountPath("/templates?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["TemplateSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	next, ok := body["NextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, next)

	seen := map[string]bool{}
	for _, it := range items {
		m1, isMap := it.(map[string]any)
		require.True(t, isMap)
		seen[m1["TemplateId"].(string)] = true
	}

	page2Path := accountPath(fmt.Sprintf("/templates?max-results=2&next-token=%s", next))
	rec2 := doRequest(t, h, http.MethodGet, page2Path, nil)
	require.Equal(t, http.StatusOK, rec2.Code)
	body2 := parseBody(t, rec2)
	items2, ok := body2["TemplateSummaryList"].([]any)
	require.True(t, ok)
	assert.Len(t, items2, 2)
	for _, it := range items2 {
		m2, isMap := it.(map[string]any)
		require.True(t, isMap)
		assert.False(t, seen[m2["TemplateId"].(string)], "page 2 must not repeat page 1 items")
	}
}

// ---- DeleteTemplate with a specific VersionNumber only removes that version ----

func TestQuickSight_DeleteTemplate_SpecificVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/templates/dvtpl"), map[string]any{"Name": "D1"})
	doRequest(t, h, http.MethodPut, accountPath("/templates/dvtpl"), map[string]any{"Name": "D2"})

	delRec := doRequest(t, h, http.MethodDelete, accountPath("/templates/dvtpl?version-number=1"), nil)
	require.Equal(t, http.StatusOK, delRec.Code)

	// Template itself still exists (version 2 remains).
	describeRec := doRequest(t, h, http.MethodGet, accountPath("/templates/dvtpl"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)

	// Version 1 itself is gone.
	describeV1Rec := doRequest(t, h, http.MethodGet, accountPath("/templates/dvtpl?version-number=1"), nil)
	assert.Equal(t, http.StatusNotFound, describeV1Rec.Code)
}
