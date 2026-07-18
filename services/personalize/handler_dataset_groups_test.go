package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersonalize_DatasetGroup_CRUD(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	// Create
	rec := personalizeDo(t, h, "CreateDatasetGroup", map[string]any{
		"name":   "my-group",
		"domain": "ECOMMERCE",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	created := personalizeUnmarshal(t, rec)
	dgArn, _ := created["datasetGroupArn"].(string)
	assert.NotEmpty(t, dgArn)
	assert.Equal(t, "ECOMMERCE", created["domain"])

	// Describe
	rec = personalizeDo(t, h, "DescribeDatasetGroup", map[string]any{"datasetGroupArn": dgArn})
	require.Equal(t, http.StatusOK, rec.Code)
	described := personalizeUnmarshal(t, rec)
	dg := described["datasetGroup"].(map[string]any)
	assert.Equal(t, "my-group", dg["name"])
	assert.Equal(t, "ECOMMERCE", dg["domain"])
	assert.Equal(t, "ACTIVE", dg["status"])
	assert.NotEmpty(t, dg["creationDateTime"])
	assert.NotEmpty(t, dg["lastUpdatedDateTime"])

	// List
	rec = personalizeDo(t, h, "ListDatasetGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	listed := personalizeUnmarshal(t, rec)
	groups := listed["datasetGroups"].([]any)
	assert.Len(t, groups, 1)

	// Delete
	rec = personalizeDo(t, h, "DeleteDatasetGroup", map[string]any{"datasetGroupArn": dgArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted
	rec = personalizeDo(t, h, "DescribeDatasetGroup", map[string]any{"datasetGroupArn": dgArn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	errBody := personalizeUnmarshal(t, rec)
	assert.Equal(t, "ResourceNotFoundException", errBody["__type"])
}

func TestPersonalize_DatasetGroup_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	personalizeDo(t, h, "CreateDatasetGroup", map[string]any{"name": "dup-group"})
	rec := personalizeDo(t, h, "CreateDatasetGroup", map[string]any{"name": "dup-group"})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	m := personalizeUnmarshal(t, rec)
	assert.Equal(t, "ResourceAlreadyExistsException", m["__type"])
}

// --- Dataset ---
