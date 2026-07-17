package appconfig_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestHandler_GetConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Setup: create application, environment, config profile, and hosted config version.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"conf-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/environments",
		[]byte(`{"name":"conf-env"}`),
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var env appconfig.Environment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))

	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"conf-profile","LocationUri":"hosted","Type":"AWS.Freeform"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "no version returns 204",
			path:       "/applications/" + app.ID + "/environments/" + env.ID + "/configurations/" + profile.ID,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "app not found",
			path:       "/applications/nonexistent/environments/" + env.ID + "/configurations/" + profile.ID,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "env not found",
			path:       "/applications/" + app.ID + "/environments/nonexistent/configurations/" + profile.ID,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "profile not found",
			path:       "/applications/" + app.ID + "/environments/" + env.ID + "/configurations/nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, http.MethodGet, tt.path, nil)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}

func TestHandler_GetConfiguration_WithContent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Setup.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"content-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	rec = doRequest(
		t,
		h,
		http.MethodPost,
		"/applications/"+app.ID+"/environments",
		[]byte(`{"name":"content-env"}`),
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var env appconfig.Environment
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &env))

	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"content-profile","LocationUri":"hosted","Type":"AWS.Freeform"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))

	// Create a hosted configuration version.
	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID+"/hostedconfigurationversions",
		bytes.NewReader([]byte(`{"feature":"enabled"}`)),
	)
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(logger.Save(t.Context(), slog.Default()))
	recVer := httptest.NewRecorder()
	c := e.NewContext(req, recVer)
	err := h.Handler()(c)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, recVer.Code)

	// GetConfiguration should now return 200 with content.
	rec = doRequest(t, h, http.MethodGet,
		"/applications/"+app.ID+"/environments/"+env.ID+"/configurations/"+profile.ID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, rec.Body.Bytes())
	assert.Equal(t, "1", rec.Header().Get("Configuration-Version"))
}

func TestHandler_ValidateConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create application and profile.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"validate-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	rec = doRequest(t, h, http.MethodPost, "/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"validate-profile","LocationUri":"hosted","Type":"AWS.Freeform"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var profile appconfig.ConfigurationProfile
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &profile))

	validatorBase := "/applications/" + app.ID + "/configurationprofiles/" + profile.ID + "/validators"

	tests := []struct {
		name       string
		path       string
		wantStatus int
	}{
		{
			name:       "valid configuration",
			path:       validatorBase + "?configuration_version=1",
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "app not found",
			path:       "/applications/nonexistent/configurationprofiles/" + profile.ID + "/validators",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "profile not found",
			path:       "/applications/" + app.ID + "/configurationprofiles/nonexistent/validators",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := doRequest(t, h, http.MethodPost, tt.path, nil)
			assert.Equal(t, tt.wantStatus, result.Code)
		})
	}
}
