package ecr //nolint:testpackage // needs access to the unexported classifyError method.

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

var errUnmatchedForTest = errors.New("boom: matches none of classifyError's sentinel checks")

// TestClassifyError_DefaultBranchEmitsServerException is a white-box test
// of classifyError's default branch: ecr@v1.60.4 types/errors.go models
// "ServerException" (ErrorFault: FaultServer, doc comment "These errors are
// usually caused by a server-side issue") as the service's 5xx fault, wired
// into all 58 of ecr's operation deserializers (confirmed via
// deserializers.go's error-code switches), so any backend error not
// classified under one of the enumerated sentinel/not-found groups must
// surface that code, not the unmodeled "InternalServerError" this branch
// returned before the fix (gopherstack-o7gx).
//
// classifyError's default is reachable only when a backend error doesn't
// match any of the enumerated sentinels, errUnknownAction, or a JSON
// syntax/type error (itself mapped to InvalidParameterException here); no
// currently-wired dispatch path leaves an error unclassified, so there is
// no legitimately-constructed real SDK client request that reaches this
// branch today. This test drives classifyError directly with a synthetic
// unmatched error to pin the wire-level contract regardless.
func TestClassifyError_DefaultBranchEmitsServerException(t *testing.T) {
	t.Parallel()

	h := &Handler{}

	status, code := h.classifyError(errUnmatchedForTest)

	assert.Equal(t, http.StatusInternalServerError, status)
	assert.Equal(t, "ServerException", code)
}
