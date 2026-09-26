package apigateway

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandleProxyRequest_StageWithNoDeployment defends the stage.DeploymentID
// == "" branch in handleProxyRequest. Every client-reachable way to create a
// stage (CreateStage, CreateDeployment's inline stage) requires a
// deploymentId, so this constructs the state directly to prove the fallback
// still returns AWS's real 403 rather than a panic or 500 if that invariant
// is ever broken by a future change (e.g. a restored snapshot from an older,
// buggier version).
func TestHandleProxyRequest_StageWithNoDeployment(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend()
	h := NewHandler(backend)
	e := echo.New()

	api, err := backend.CreateRestAPI(CreateRestAPIInput{Name: "orphan-stage-api"})
	require.NoError(t, err)

	backend.stages.Put(&Stage{
		RestAPIID: api.ID,
		StageName: "orphan",
		Variables: map[string]string{},
	})

	req := httptest.NewRequest(http.MethodGet, "/restapis/"+api.ID+"/orphan/_user_request_/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "Missing Authentication Token")
}
