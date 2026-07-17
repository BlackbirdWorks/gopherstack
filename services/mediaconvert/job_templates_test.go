package mediaconvert_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

func TestMediaConvert_JobTemplate_FullLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	templateName := "test-template"

	// Create
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobTemplates", map[string]any{
		"name":        templateName,
		"description": "a test template",
		"category":    "Standard",
		"priority":    0,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	jtData, ok := createResp["jobTemplate"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, templateName, jtData["name"])
	assert.Equal(t, "CUSTOM", jtData["type"])
	assert.NotEmpty(t, jtData["arn"])

	// Get
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/jobTemplates/"+templateName, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), templateName)

	// List
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/jobTemplates", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), templateName)

	// Update
	priority := 5
	rec = doRequest(t, h, http.MethodPut, "/2017-08-29/jobTemplates/"+templateName, map[string]any{
		"description": "updated description",
		"priority":    &priority,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&updateResp))
	updatedJT, ok := updateResp["jobTemplate"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "updated description", updatedJT["description"])

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/2017-08-29/jobTemplates/"+templateName, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify deleted
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/jobTemplates/"+templateName, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMediaConvert_JobTemplate_DuplicateCreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, "/2017-08-29/jobTemplates", map[string]any{"name": "dup-tpl"})

	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobTemplates", map[string]any{"name": "dup-tpl"})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestMediaConvert_JobTemplate_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobTemplates", map[string]any{
		"description": "no name",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestMediaConvert_TagLeakOnDeleteJobTemplate(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

	jt, err := b.CreateJobTemplate("my-template", "desc", "", "", 0, nil, nil)
	require.NoError(t, err)

	b.TagResource(jt.Arn, map[string]string{"team": "infra"})
	assert.Equal(t, "infra", b.GetTags(jt.Arn)["team"])

	require.NoError(t, b.DeleteJobTemplate("my-template"))

	// Tags must be gone after deletion.
	assert.Empty(t, b.GetTags(jt.Arn))
}

// TestCreateJobTemplate_WithTags verifies tags are stored at creation time.
func TestCreateJobTemplate_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobTemplates", map[string]any{
		"name": "tagged-template",
		"tags": map[string]string{"cost-center": "12345"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	jt := resp["jobTemplate"].(map[string]any)
	tags := jt["tags"].(map[string]any)
	assert.Equal(t, "12345", tags["cost-center"])
}

// TestUpdateJobTemplate_SettingsDeepCopy verifies settings deep copy on update.
func TestUpdateJobTemplate_SettingsDeepCopy(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateJobTemplate("jt-settings", "", "", "", 0, nil, nil)
	require.NoError(t, err)

	newSettings := map[string]any{"key": "val1"}

	jt, err := b.UpdateJobTemplate("jt-settings", "", "", "", nil, newSettings)
	require.NoError(t, err)

	// Mutate the returned copy.
	jt.Settings["key"] = "MUTATED"

	// Backend should be unaffected.
	jt2, err := b.GetJobTemplate("jt-settings")
	require.NoError(t, err)
	assert.Equal(t, "val1", jt2.Settings["key"])
}

// TestListJobTemplates_SortedByName verifies sort order.
func TestListJobTemplates_SortedByName(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

	for _, name := range []string{"z-tmpl", "a-tmpl", "m-tmpl"} {
		_, err := b.CreateJobTemplate(name, "", "", "", 0, nil, nil)
		require.NoError(t, err)
	}

	templates := b.ListJobTemplates()
	require.Len(t, templates, 3)
	assert.Equal(t, "a-tmpl", templates[0].Name)
	assert.Equal(t, "m-tmpl", templates[1].Name)
	assert.Equal(t, "z-tmpl", templates[2].Name)
}
