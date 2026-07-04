package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Brand CRUD round-trip, versioning, and errors ----

func TestQuickSight_BrandCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, accountPath("/brands/b1"), map[string]any{
		"BrandDefinition": map[string]any{"BrandName": "Brand One"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := parseBody(t, createRec)
	detail, ok := createBody["BrandDetail"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "b1", detail["BrandId"])
	assert.Equal(t, "1", detail["VersionId"])
	def, ok := createBody["BrandDefinition"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Brand One", def["BrandName"])

	// Duplicate create -> ConflictException.
	dupRec := doRequest(t, h, http.MethodPost, accountPath("/brands/b1"), map[string]any{
		"BrandDefinition": map[string]any{"BrandName": "x"},
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)
	assert.Equal(t, "ResourceExistsException", parseBody(t, dupRec)["Code"])

	// Describe missing -> 404.
	missingRec := doRequest(t, h, http.MethodGet, accountPath("/brands/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
	assert.Equal(t, "ResourceNotFoundException", parseBody(t, missingRec)["Code"])

	// Update creates a new version.
	updateRec := doRequest(t, h, http.MethodPut, accountPath("/brands/b1"), map[string]any{
		"BrandDefinition": map[string]any{"BrandName": "Brand One Renamed"},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)
	updateDetail := parseBody(t, updateRec)["BrandDetail"].(map[string]any)
	assert.Equal(t, "2", updateDetail["VersionId"])

	// Describe a specific (now non-current) version still works.
	v1Rec := doRequest(t, h, http.MethodGet, accountPath("/brands/b1?version-id=1"), nil)
	require.Equal(t, http.StatusOK, v1Rec.Code)
	v1Def := parseBody(t, v1Rec)["BrandDefinition"].(map[string]any)
	assert.Equal(t, "Brand One", v1Def["BrandName"])

	// Describe an unknown version -> 404.
	badVersionRec := doRequest(t, h, http.MethodGet, accountPath("/brands/b1?version-id=99"), nil)
	assert.Equal(t, http.StatusNotFound, badVersionRec.Code)

	// No published version yet -> 404.
	noPubRec := doRequest(t, h, http.MethodGet, accountPath("/brands/b1/publishedversion"), nil)
	assert.Equal(t, http.StatusNotFound, noPubRec.Code)

	// Publish version 2.
	pubRec := doRequest(t, h, http.MethodPut, accountPath("/brands/b1/publishedversion"), map[string]any{
		"VersionId": "2",
	})
	require.Equal(t, http.StatusOK, pubRec.Code)
	assert.Equal(t, "2", parseBody(t, pubRec)["VersionId"])

	// Publishing an unknown version -> 404.
	badPubRec := doRequest(t, h, http.MethodPut, accountPath("/brands/b1/publishedversion"), map[string]any{
		"VersionId": "99",
	})
	assert.Equal(t, http.StatusNotFound, badPubRec.Code)

	pubDescRec := doRequest(t, h, http.MethodGet, accountPath("/brands/b1/publishedversion"), nil)
	require.Equal(t, http.StatusOK, pubDescRec.Code)
	pubDef := parseBody(t, pubDescRec)["BrandDefinition"].(map[string]any)
	assert.Equal(t, "Brand One Renamed", pubDef["BrandName"])

	// Assign the brand, then deleting it should conflict.
	assignRec := doRequest(t, h, http.MethodPut, accountPath("/brandassignments"), map[string]any{
		"BrandArn": detail["Arn"],
	})
	require.Equal(t, http.StatusOK, assignRec.Code)
	assert.Equal(t, detail["Arn"], parseBody(t, assignRec)["BrandArn"])

	inUseRec := doRequest(t, h, http.MethodDelete, accountPath("/brands/b1"), nil)
	assert.Equal(t, http.StatusConflict, inUseRec.Code)

	// Unassign, then delete succeeds.
	unassignRec := doRequest(t, h, http.MethodDelete, accountPath("/brandassignments"), nil)
	require.Equal(t, http.StatusOK, unassignRec.Code)

	describeAssignmentRec := doRequest(t, h, http.MethodGet, accountPath("/brandassignments"), nil)
	require.Equal(t, http.StatusOK, describeAssignmentRec.Code)
	assert.Empty(t, parseBody(t, describeAssignmentRec)["BrandArn"])

	deleteRec := doRequest(t, h, http.MethodDelete, accountPath("/brands/b1"), nil)
	require.Equal(t, http.StatusOK, deleteRec.Code)

	deleteMissingRec := doRequest(t, h, http.MethodDelete, accountPath("/brands/b1"), nil)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- UpdateBrandAssignment with an unknown brand ARN ----

func TestQuickSight_BrandAssignment_UnknownBrand(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPut, accountPath("/brandassignments"), map[string]any{
		"BrandArn": "arn:aws:quicksight:us-east-1:000000000000:brand/doesnotexist",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- ListBrands pagination ----

func TestQuickSight_ListBrands_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		doRequest(t, h, http.MethodPost, accountPath("/brands/"+id), map[string]any{
			"BrandDefinition": map[string]any{"BrandName": id},
		})
	}

	rec := doRequest(t, h, http.MethodGet, accountPath("/brands?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["Brands"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	next, ok := body["NextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, next)

	seen := map[string]bool{}
	for _, it := range items {
		m := it.(map[string]any)
		seen[m["BrandId"].(string)] = true
	}

	page2 := doRequest(
		t, h, http.MethodGet, accountPath(fmt.Sprintf("/brands?max-results=2&next-token=%s", next)), nil,
	)
	require.Equal(t, http.StatusOK, page2.Code)
	items2 := parseBody(t, page2)["Brands"].([]any)
	assert.Len(t, items2, 2)
	for _, it := range items2 {
		m := it.(map[string]any)
		assert.False(t, seen[m["BrandId"].(string)], "page 2 must not repeat page 1 items")
	}
}
