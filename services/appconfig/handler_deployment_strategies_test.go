package appconfig_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestHandler_DeploymentStrategy_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := []byte(`{"name":"my-strategy","deploymentDurationInMinutes":0,` +
		`"finalBakeTimeInMinutes":0,"growthFactor":100,"growthType":"LINEAR","replicateTo":"NONE"}`)
	rec := doRequest(t, h, http.MethodPost, "/deploymentstrategies", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var strategy appconfig.DeploymentStrategy
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &strategy))
	assert.Equal(t, "my-strategy", strategy.Name)

	// Get.
	rec = doRequest(t, h, http.MethodGet, "/deploymentstrategies/"+strategy.ID, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// List.
	rec = doRequest(t, h, http.MethodGet, "/deploymentstrategies", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Update.
	rec = doRequest(
		t,
		h,
		http.MethodPatch,
		"/deploymentstrategies/"+strategy.ID,
		[]byte(`{"name":"updated-strategy"}`),
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete.
	rec = doRequest(t, h, http.MethodDelete, "/deploymentstrategies/"+strategy.ID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestHandler_DeploymentStrategy_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/deploymentstrategies/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListDeploymentStrategies_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := []byte(`{"name":"list-strat","deploymentDurationInMinutes":0,` +
		`"finalBakeTimeInMinutes":0,"growthFactor":100,"growthType":"LINEAR","replicateTo":"NONE"}`)
	rec := doRequest(t, h, http.MethodPost, "/deploymentstrategies", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/deploymentstrategies", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_UpdateDeploymentStrategy_OmittedDescriptionPreserved verifies
// that updating only GrowthFactor leaves Description unchanged when it is
// omitted, matching real UpdateDeploymentStrategyInput's optional *string
// Description member.
func TestHandler_UpdateDeploymentStrategy_OmittedDescriptionPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := []byte(`{"Name":"upd-strat","Description":"keep-me",` +
		`"DeploymentDurationInMinutes":0,"FinalBakeTimeInMinutes":0,` +
		`"GrowthFactor":10,"GrowthType":"LINEAR","ReplicateTo":"NONE"}`)
	rec := doRequest(t, h, http.MethodPost, "/deploymentstrategies", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var strat appconfig.DeploymentStrategy
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &strat))

	rec = doRequest(t, h, http.MethodPatch, "/deploymentstrategies/"+strat.ID,
		[]byte(`{"GrowthFactor":50}`))
	assert.Equal(t, http.StatusOK, rec.Code)

	var updated appconfig.DeploymentStrategy
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updated))
	assert.InDelta(t, float32(50), updated.GrowthFactor, 0.001)
	assert.Equal(t, "keep-me", updated.Description, "omitted Description must be preserved")
}

func TestHandler_ExtractResource_Strategy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/deploymentstrategies/strat-xyz", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "strat-xyz", h.ExtractResource(c))
}
