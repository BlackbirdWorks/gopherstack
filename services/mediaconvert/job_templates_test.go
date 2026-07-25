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

// TestCreateJobTemplate_AccelerationHopDestinationsStatusUpdateInterval verifies
// the AccelerationSettings/HopDestinations/StatusUpdateInterval fields that the
// real CreateJobTemplateInput wire shape accepts (previously silently dropped
// since JobTemplate had no such fields).
func TestCreateJobTemplate_AccelerationHopDestinationsStatusUpdateInterval(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobTemplates", map[string]any{
		"name":                 "accel-tpl",
		"accelerationSettings": map[string]any{"mode": "PREFERRED"},
		"statusUpdateInterval": "SECONDS_30",
		"hopDestinations": []any{
			map[string]any{"queue": "backup-q", "waitMinutes": 15},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	jt := resp["jobTemplate"].(map[string]any)

	accel := jt["accelerationSettings"].(map[string]any)
	assert.Equal(t, "PREFERRED", accel["mode"])
	assert.Equal(t, "SECONDS_30", jt["statusUpdateInterval"])

	hops, ok := jt["hopDestinations"].([]any)
	require.True(t, ok)
	require.Len(t, hops, 1)
	assert.Equal(t, "backup-q", hops[0].(map[string]any)["queue"])
}

// TestCreateJobTemplate_StatusUpdateIntervalDefault verifies the real API's
// documented default (SECONDS_60) is applied when unspecified.
func TestCreateJobTemplate_StatusUpdateIntervalDefault(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	jt, err := b.CreateJobTemplate("default-interval-tpl", "", "", "", 0, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "SECONDS_60", jt.StatusUpdateInterval)
}

// TestUpdateJobTemplate_AccelerationHopDestinationsStatusUpdateInterval verifies
// UpdateJobTemplateFull applies the newer fields instead of silently dropping
// them (real UpdateJobTemplateInput accepts them).
func TestUpdateJobTemplate_AccelerationHopDestinationsStatusUpdateInterval(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateJobTemplate("update-accel-tpl", "", "", "", 0, nil, nil)
	require.NoError(t, err)

	hops := []mediaconvert.HopDestination{{Queue: "q2", WaitMinutes: 30}}
	jt, err := b.UpdateJobTemplateFull(
		"update-accel-tpl", "", "", "", nil, nil,
		&mediaconvert.AccelerationSettings{Mode: "ENABLED"},
		"SECONDS_120",
		hops,
	)
	require.NoError(t, err)
	require.NotNil(t, jt.AccelerationSettings)
	assert.Equal(t, "ENABLED", jt.AccelerationSettings.Mode)
	assert.Equal(t, "SECONDS_120", jt.StatusUpdateInterval)
	require.Len(t, jt.HopDestinations, 1)
	assert.Equal(t, "q2", jt.HopDestinations[0].Queue)

	// Fetching again confirms persistence in the backend, not just the returned copy.
	fetched, err := b.GetJobTemplate("update-accel-tpl")
	require.NoError(t, err)
	assert.Equal(t, "SECONDS_120", fetched.StatusUpdateInterval)
	require.Len(t, fetched.HopDestinations, 1)
}

// TestUpdateJobTemplate_ViaHTTP verifies JSON parsing of the newer fields
// end-to-end through the HTTP handler.
func TestUpdateJobTemplate_ViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/2017-08-29/jobTemplates", map[string]any{"name": "http-update-tpl"})

	rec := doRequest(t, h, http.MethodPut, "/2017-08-29/jobTemplates/http-update-tpl", map[string]any{
		"accelerationSettings": map[string]any{"mode": "DISABLED"},
		"statusUpdateInterval": "SECONDS_15",
		"hopDestinations": []any{
			map[string]any{"queue": "hop-q", "waitMinutes": 60, "priority": 1},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	jt := resp["jobTemplate"].(map[string]any)

	accel := jt["accelerationSettings"].(map[string]any)
	assert.Equal(t, "DISABLED", accel["mode"])
	assert.Equal(t, "SECONDS_15", jt["statusUpdateInterval"])

	hops, ok := jt["hopDestinations"].([]any)
	require.True(t, ok)
	require.Len(t, hops, 1)
	assert.Equal(t, "hop-q", hops[0].(map[string]any)["queue"])
}

// TestCloneJobTemplate_AccelerationHopDestinationsDeepCopy verifies mutating
// a returned JobTemplate's AccelerationSettings/HopDestinations does not leak
// back into backend state.
func TestCloneJobTemplate_AccelerationHopDestinationsDeepCopy(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	jt, err := b.CreateJobTemplateFull(
		"clone-tpl", "", "", "", 0, nil, nil,
		"ENABLED", "SECONDS_60",
		[]mediaconvert.HopDestination{{Queue: "orig-q"}},
	)
	require.NoError(t, err)

	jt.AccelerationSettings.Mode = "MUTATED"
	jt.HopDestinations[0].Queue = "MUTATED"

	fetched, err := b.GetJobTemplate("clone-tpl")
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", fetched.AccelerationSettings.Mode)
	assert.Equal(t, "orig-q", fetched.HopDestinations[0].Queue)
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
