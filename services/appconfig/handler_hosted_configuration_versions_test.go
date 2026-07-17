package appconfig_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestHandler_HostedConfigurationVersion_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create app and profile first.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"hcv-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	profileBody := []byte(`{"name":"hcv-profile","locationUri":"hosted","type":"AWS.Freeform"}`)
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

	// Create hosted configuration version with raw body.
	e := echo.New()
	content := []byte(`{"feature":"enabled"}`)
	req := httptest.NewRequest(
		http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID+"/hostedconfigurationversions",
		bytes.NewReader(content),
	)
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(logger.Save(t.Context(), slog.Default()))
	recHcv := httptest.NewRecorder()
	c := e.NewContext(req, recHcv)
	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, recHcv.Code)

	// List versions.
	rec = doRequest(
		t,
		h,
		http.MethodGet,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID+"/hostedconfigurationversions",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get version 1.
	req2 := httptest.NewRequest(
		http.MethodGet,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID+"/hostedconfigurationversions/1",
		nil,
	)
	req2 = req2.WithContext(logger.Save(t.Context(), slog.Default()))
	rec2 := httptest.NewRecorder()
	c2 := e.NewContext(req2, rec2)
	err = h.Handler()(c2)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Equal(t, content, rec2.Body.Bytes())

	// Delete version 1.
	rec = doRequest(
		t,
		h,
		http.MethodDelete,
		"/applications/"+app.ID+"/configurationprofiles/"+profile.ID+"/hostedconfigurationversions/1",
		nil,
	)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_HostedConfigVersion_HTTP_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		pathSuffix string
		wantStatus int
	}{
		{
			name:       "get version not found",
			method:     http.MethodGet,
			pathSuffix: "/applications/app-1/configurationprofiles/prof-1/hostedconfigurationversions/99",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete version not found",
			method:     http.MethodDelete,
			pathSuffix: "/applications/app-1/configurationprofiles/prof-1/hostedconfigurationversions/99",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list versions unknown app",
			method:     http.MethodGet,
			pathSuffix: "/applications/app-1/configurationprofiles/prof-1/hostedconfigurationversions",
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

func TestHandler_HostedConfigVersion_HTTP_ListEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Pre-create app and profile so listing returns an empty list (200).
	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"my-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var appOut struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &appOut))

	profRec := doRequest(
		t, h, http.MethodPost,
		"/applications/"+appOut.ID+"/configurationprofiles",
		[]byte(`{"Name":"my-profile","LocationUri":"hosted"}`),
	)
	require.Equal(t, http.StatusCreated, profRec.Code)

	var profOut struct {
		ID string `json:"Id"`
	}

	require.NoError(t, json.Unmarshal(profRec.Body.Bytes(), &profOut))

	listRec := doRequest(
		t,
		h,
		http.MethodGet,
		"/applications/"+appOut.ID+"/configurationprofiles/"+profOut.ID+"/hostedconfigurationversions",
		nil,
	)
	assert.Equal(t, http.StatusOK, listRec.Code)
}

// TestParity_HostedConfigVersionResponseHeaders verifies that
// CreateHostedConfigurationVersion and GetHostedConfigurationVersion return
// the real AWS AppConfig wire shape: the configuration content as the raw
// response body (not a JSON envelope), with every other field -- including
// the version number -- bound to a response header. Real aws-sdk-go-v2
// reads Version-Number (not a fabricated "Appconfig-Configuration-Version"),
// plus Application-Id, Configuration-Profile-Id, Description, and
// VersionLabel; see
// awsRestjson1_deserializeOpHttpBindingsCreateHostedConfigurationVersionOutput
// / ...GetHostedConfigurationVersionOutput in
// aws-sdk-go-v2/service/appconfig/deserializers.go.
func TestHandler_HostedConfigVersionResponseHeaders(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	appRec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"Name":"hcv-hdr-app"}`))
	require.Equal(t, http.StatusCreated, appRec.Code)

	var app struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(appRec.Body.Bytes(), &app))

	profRec := doRequest(t, h, http.MethodPost,
		"/applications/"+app.ID+"/configurationprofiles",
		[]byte(`{"Name":"hcv-hdr-profile","LocationUri":"hosted","Type":"AWS.Freeform"}`))
	require.Equal(t, http.StatusCreated, profRec.Code)

	var prof struct {
		ID string `json:"Id"`
	}
	require.NoError(t, json.Unmarshal(profRec.Body.Bytes(), &prof))

	base := "/applications/" + app.ID + "/configurationprofiles/" + prof.ID +
		"/hostedconfigurationversions"

	contents := [][]byte{
		[]byte(`{"value":"first"}`),
		[]byte(`{"value":"second"}`),
		[]byte(`{"value":"third"}`),
	}

	for i, content := range contents {
		wantVer := strconv.Itoa(i + 1)

		rec := doRequest(t, h, http.MethodPost, base, content)
		require.Equal(t, http.StatusCreated, rec.Code)

		assert.Equal(t, wantVer, rec.Header().Get("Version-Number"),
			"Version-Number header must increment with each version (real SDK deserializer key)")
		assert.Equal(t, app.ID, rec.Header().Get("Application-Id"))
		assert.Equal(t, prof.ID, rec.Header().Get("Configuration-Profile-Id"))
		assert.Equal(t, content, rec.Body.Bytes(),
			"response body must be the raw configuration content, not a JSON envelope")

		getRec := doRequest(t, h, http.MethodGet, base+"/"+wantVer, nil)
		require.Equal(t, http.StatusOK, getRec.Code)
		assert.Equal(t, wantVer, getRec.Header().Get("Version-Number"))
		assert.Equal(t, app.ID, getRec.Header().Get("Application-Id"))
		assert.Equal(t, prof.ID, getRec.Header().Get("Configuration-Profile-Id"))
		assert.Equal(t, content, getRec.Body.Bytes())
	}
}
