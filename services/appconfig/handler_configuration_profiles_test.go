package appconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestHandler_ConfigurationProfile_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"prof-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	// Create profile.
	profileBody := []byte(`{"name":"my-config","locationUri":"hosted","type":"AWS.Freeform"}`)
	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles",
		profileBody,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))
	assert.Equal(t, "my-config", profile.Name)

	// Get profile.
	rec = doRequest(
		t,
		h,
		http.MethodGet,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID,
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete profile.
	rec = doRequest(
		t,
		h,
		http.MethodDelete,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID,
		nil,
	)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_ListConfigurationProfiles_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"list-prof-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	// Create two profiles.
	for _, name := range []string{"prof-1", "prof-2"} {
		body := []byte(`{"name":"` + name + `","locationUri":"hosted","type":"AWS.Freeform"}`)
		rec = doRequest(
			t,
			h,
			http.MethodPost,
			"/applications/"+app.ID+"/configurationprofiles",
			body,
		)
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	// List.
	rec = doRequest(t, h, http.MethodGet, "/applications/"+app.ID+"/configurationprofiles", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_UpdateConfigurationProfile_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"upd-prof-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	profileBody := []byte(`{"name":"old-name","locationUri":"hosted","type":"AWS.Freeform"}`)
	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles",
		profileBody,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))

	// Update.
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID,
		[]byte(`{"name":"new-name","description":"updated"}`),
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "new-name", updated.Name)
}

// TestHandler_UpdateConfigurationProfile_RetrievalRoleArnAndValidators verifies
// that UpdateConfigurationProfile applies RetrievalRoleArn and Validators
// when present (previously silently dropped -- the backend only accepted
// Name/Description) and preserves Description when it is omitted, matching
// real UpdateConfigurationProfileInput's optional members.
func TestHandler_UpdateConfigurationProfile_RetrievalRoleArnAndValidators(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"upd-prof-app2"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	profileBody := []byte(
		`{"Name":"old-name","Description":"keep-me","LocationUri":"hosted","Type":"AWS.Freeform"}`,
	)
	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles", profileBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))

	rec = doRequest(t, h, http.MethodPatch,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID,
		[]byte(`{"RetrievalRoleArn":"arn:aws:iam::123456789012:role/retrieval",`+
			`"Validators":[{"Type":"JSON_SCHEMA","Content":"{}"}]}`))
	assert.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.Equal(t, "arn:aws:iam::123456789012:role/retrieval", updated.RetrievalRoleArn,
		"RetrievalRoleArn must be applied, not silently dropped")
	require.Len(t, updated.Validators, 1, "Validators must be applied, not silently dropped")
	assert.Equal(t, "keep-me", updated.Description, "omitted Description must be preserved")
}

// TestHandler_CreateConfigurationProfile_TagsAppliedInline verifies that
// Tags sent inline on CreateConfigurationProfileInput are visible via
// ListTagsForResource immediately after creation -- previously
// CreateConfigurationProfile's handler never bound or forwarded the Tags
// field at all, so tags set at create time silently vanished (bd
// gopherstack-lcan).
func TestHandler_CreateConfigurationProfile_TagsAppliedInline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{
			name: "tags_applied_at_create",
			tags: map[string]string{"env": "prod", "team": "platform"},
		},
		{
			name: "no_tags_is_not_an_error",
			tags: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			appRec := doRequest(t, h, http.MethodPost, "/applications",
				[]byte(`{"Name":"tagged-prof-app-`+tt.name+`"}`))
			require.Equal(t, http.StatusCreated, appRec.Code)

			var app appconfig.Application
			require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

			body, err := json.Marshal(map[string]any{
				"Name":        "tagged-prof-" + tt.name,
				"LocationUri": "hosted",
				"Type":        "AWS.Freeform",
				"Tags":        tt.tags,
			})
			require.NoError(t, err)

			rec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var profile appconfig.ConfigurationProfile
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))

			resourceArn := "arn:aws:appconfig:us-east-1:123456789012:application/" +
				app.ID + "/configurationprofile/" + profile.ID
			tagsRec := doRequest(t, h, http.MethodGet, "/tags/"+resourceArn, nil)
			require.Equal(t, http.StatusOK, tagsRec.Code)

			var tagsResp struct {
				Tags map[string]string `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(tagsRec.Body.Bytes(), &tagsResp))

			if len(tt.tags) == 0 {
				assert.Empty(t, tagsResp.Tags)
			} else {
				assert.Equal(t, tt.tags, tagsResp.Tags)
			}
		})
	}
}

func TestHandler_ConfigProfile_HTTP_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		pathSuffix string
		wantStatus int
	}{
		{
			name:       "get profile not found",
			method:     http.MethodGet,
			pathSuffix: "/applications/nonexistent/configurationprofiles/prof-1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list profiles app not found",
			method:     http.MethodGet,
			pathSuffix: "/applications/nonexistent/configurationprofiles",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete profile not found",
			method:     http.MethodDelete,
			pathSuffix: "/applications/nonexistent/configurationprofiles/prof-1",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.pathSuffix, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// seedExperimentDefinitionHTTP drives CreateApplication/CreateEnvironment/
// CreateConfigurationProfile through the real router path (doRequest, not
// the backend directly) so experiment definition handler tests exercise
// full wire-shape parsing end to end, returning the application/
// environment/configuration-profile IDs.
func seedExperimentDefinitionHTTP(t *testing.T, h *appconfig.Handler) (string, string, string) {
	t.Helper()

	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"exp-http-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

	envRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/environments",
		[]byte(`{"Name":"exp-http-env"}`))
	require.Equal(t, http.StatusCreated, envRec.Code)

	var env struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(envRec.Body.Bytes(), &env))

	profRec := doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"exp-http-profile","LocationUri":"hosted","Type":"AWS.AppConfig.FeatureFlags"}`))
	require.Equal(t, http.StatusCreated, profRec.Code)

	var prof struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(profRec.Body.Bytes(), &prof))

	return app.ID, env.ID, prof.ID
}

// TestHandler_ExperimentDefinition_CRUD drives CreateExperimentDefinition/
// GetExperimentDefinition/ListExperimentDefinitions/
// UpdateExperimentDefinition/DeleteExperimentDefinition through the real
// router path, asserting the REST-JSON wire shapes field-diffed against
// aws-sdk-go-v2/service/appconfig@v1.48.0's Create/Get/List/Update/
// DeleteExperimentDefinitionOutput.
func TestHandler_ExperimentDefinition_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID, envID, profID := seedExperimentDefinitionHTTP(t, h)

	createBody := `{
		"Name": "http-def",
		"AudienceRule": "true",
		"FlagKey": "flag1",
		"EnvironmentIdentifier": "` + envID + `",
		"ConfigurationProfileIdentifier": "` + profID + `",
		"Control": {"FlagValue": {"Enabled": false}, "Weight": 50},
		"Treatments": [{"FlagValue": {"Enabled": true}, "Weight": 50}]
	}`
	createRec := doRequest(t, h, http.MethodPost, "/applications/"+appID+"/experimentdefinitions", []byte(createBody))
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created appconfig.ExperimentDefinition
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &created))
	assert.NotEmpty(t, created.ID)
	assert.Equal(t, appID, created.ApplicationID)
	assert.Equal(t, envID, created.EnvironmentID)
	assert.Equal(t, profID, created.ConfigurationProfileID)
	assert.Equal(t, "IDLE", created.Status)
	assert.Equal(t, "Control", created.Control.Key)
	require.Len(t, created.Treatments, 1)
	assert.Equal(t, "Treatment1", created.Treatments[0].Key)

	getRec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/experimentdefinitions/"+created.ID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var got appconfig.ExperimentDefinition
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &got))
	assert.Equal(t, created.ID, got.ID)

	listRec := doRequest(t, h, http.MethodGet, "/experimentdefinitions?application_identifier="+appID, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		Items []appconfig.ExperimentDefinition `json:"Items"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Items, 1)
	assert.Equal(t, created.ID, listResp.Items[0].ID)

	updateRec := doRequest(t, h, http.MethodPatch, "/applications/"+appID+"/experimentdefinitions/"+created.ID,
		[]byte(`{"AudienceRule":"country == 'US'"}`))
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updated appconfig.ExperimentDefinition
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updated))
	assert.Equal(t, "country == 'US'", updated.AudienceRule)

	deleteRec := doRequest(t, h, http.MethodDelete, "/applications/"+appID+"/experimentdefinitions/"+created.ID, nil)
	require.Equal(t, http.StatusNoContent, deleteRec.Code)

	afterDeleteRec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/experimentdefinitions/"+created.ID, nil)
	require.Equal(t, http.StatusOK, afterDeleteRec.Code, "default delete_type archives rather than destroying")

	var afterDelete appconfig.ExperimentDefinition
	require.NoError(t, json.Unmarshal(afterDeleteRec.Body.Bytes(), &afterDelete))
	assert.Equal(t, "ARCHIVED", afterDelete.Status)
}

// TestHandler_ExperimentDefinition_HTTP_Errors covers not-found and
// validation error status codes across the experiment-definition routes.
func TestHandler_ExperimentDefinition_HTTP_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		pathSuffix string
		body       []byte
		wantStatus int
	}{
		{
			name:       "get definition not found",
			method:     http.MethodGet,
			pathSuffix: "/applications/nonexistent/experimentdefinitions/def-1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete definition not found",
			method:     http.MethodDelete,
			pathSuffix: "/applications/nonexistent/experimentdefinitions/def-1",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "create missing required fields",
			method:     http.MethodPost,
			pathSuffix: "/applications/nonexistent/experimentdefinitions",
			body:       []byte(`{}`),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "list account-wide with no filters",
			method:     http.MethodGet,
			pathSuffix: "/experimentdefinitions",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.pathSuffix, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
