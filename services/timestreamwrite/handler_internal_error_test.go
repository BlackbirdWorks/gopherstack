package timestreamwrite //nolint:testpackage // needs access to the unexported handleError method.

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errUnmatchedForTest = errors.New("boom: matches none of handleError's sentinel checks")

// TestHandleError_DefaultBranchEmitsInternalServerException is a white-box
// test of handleError's default branch: timestreamwrite@v1.38.4
// types/errors.go models "InternalServerException" (ErrorFault:
// FaultServer) as the service's dominant 5xx fault (wired into 16 of its 19
// operation deserializers -- confirmed via deserializers.go's error-code
// switches), so any backend error not classified as NotFound/Conflict/
// RejectedRecords/ValidationException must surface that code, not the
// unmodeled "InternalServerError" this branch wrote before the fix
// (gopherstack-o7gx).
//
// handleError's default is reachable only when a backend error doesn't
// match any of its sentinel checks; every currently-wired dispatch path
// wraps errors into one of those sentinels (or into a JSON syntax/type
// error, itself mapped to ValidationException here), so there is no
// legitimately-constructed real SDK client request that reaches this branch
// today. This test drives handleError directly with a synthetic unmatched
// error to pin the wire-level contract regardless.
func TestHandleError_DefaultBranchEmitsInternalServerException(t *testing.T) {
	t.Parallel()

	h := NewHandler(NewInMemoryBackend())

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.handleError(t.Context(), c, "SomeAction", errUnmatchedForTest))

	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "InternalServerException", body[keyTypeField])
}
