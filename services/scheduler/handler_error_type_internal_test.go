package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// errBoomTest is a static stand-in for an unexpected internal error, used to
// drive handleError's default branch directly in a whitebox test.
var errBoomTest = errors.New("boom")

// TestHandleError_GenericErrorSurfacesInternalServerException proves
// handleError's default branch now sets the __type field restjson.GetErrorInfo
// reads. This branch has no organic trigger reachable through a real SDK
// client in the current codebase: every backend op error wraps
// ErrNotFound/ErrAlreadyExists/ErrValidation, and a malformed request (bad
// JSON, unknown action) is caught one case earlier -- so this whitebox test
// exercises the line directly (gopherstack-he80; scheduler@v1.20.4 models
// InternalServerException on all 12 operations).
func TestHandleError_GenericErrorSurfacesInternalServerException(t *testing.T) {
	t.Parallel()

	h := NewHandler(NewInMemoryBackend("000000000000", "us-east-1"))

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.handleError(context.Background(), c, "GetSchedule", errBoomTest))

	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	var payload service.JSONErrorResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &payload))
	assert.Equal(t, "InternalServerException", payload.Type)
}
