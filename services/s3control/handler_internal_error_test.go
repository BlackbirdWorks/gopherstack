package s3control //nolint:testpackage // needs access to the unexported handleBackendError function.

import (
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errUnmatchedForTest = errors.New("boom: matches none of handleBackendError's sentinel checks")

// TestHandleBackendError_DefaultBranchEmitsInternalServiceException is a
// white-box test of handleBackendError's default branch: s3control@v1.73.4
// types/errors.go models "InternalServiceException" (ErrorFault:
// FaultServer) as the service's 5xx fault. It is wired into only 8 of
// s3control's 97 operation deserializers -- no single code is dominant for
// this service (most operations model no 5xx exception of their own at all,
// so any string here falls through to the same GenericAPIError for them
// regardless), making InternalServiceException the closest fit rather than
// the unmodeled "InternalError" this branch returned before the fix
// (gopherstack-o7gx).
//
// handleBackendError's default is reachable only when a backend error is
// not classified as NotFound/InvalidParameter/AlreadyExists; no
// currently-wired dispatch path leaves an error unclassified this way, so
// there is no legitimately-constructed real SDK client request that reaches
// this branch today. This test drives handleBackendError directly with a
// synthetic unmatched error to pin the wire-level contract regardless.
func TestHandleBackendError_DefaultBranchEmitsInternalServiceException(t *testing.T) {
	t.Parallel()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, handleBackendError(c, errUnmatchedForTest))

	assert.Equal(t, http.StatusInternalServerError, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InternalServiceException", resp.Error.Code)
}
