package codeartifact //nolint:testpackage // needs access to the unexported handleError method.

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
// test of handleError's default branch: codeartifact@v1.41.4 types/errors.go
// models "InternalServerException" (ErrorFault: FaultServer) as the service's
// sole 5xx fault, wired into 45 of codeartifact's 48 operation deserializers
// (confirmed via deserializers.go's error-code switches; the exceptions are
// ListTagsForResource/TagResource/UntagResource, which model no server fault
// at all), so any backend error not classified under one of the enumerated
// sentinels must surface that code, not the unmodeled "InternalFailure" this
// branch returned before the fix.
//
// The default branch is reachable only when a backend error doesn't match
// ErrNotFound/ErrAlreadyExists/ErrValidation/errInvalidRequest. This
// package's readRequestBody swallows JSON decode/read errors by returning a
// nil body rather than propagating them (unlike most other services here,
// codeartifact places no cap on request body size before decoding, so an
// oversized-body test cannot trigger this path either), and every backend
// call site returns one of the four enumerated sentinels. There is currently
// no legitimately-constructed real SDK client request that reaches this
// branch. This test drives handleError directly with a synthetic unmatched
// error to pin the wire-level contract regardless.
func TestHandleError_DefaultBranchEmitsInternalServerException(t *testing.T) {
	t.Parallel()

	h := &Handler{}

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	c := echo.New().NewContext(req, rec)

	require.NoError(t, h.handleError(c, errUnmatchedForTest))

	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "InternalServerException", body["code"])
	assert.NotEqual(t, "InternalFailure", body["code"])
}
