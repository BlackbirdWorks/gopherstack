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
