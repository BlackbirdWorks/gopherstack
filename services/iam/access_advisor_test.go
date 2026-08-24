package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestIAM_ServiceLastAccessedDetails(t *testing.T) {
	t.Parallel()

	_, be := newTestHandler(t)

	// GetServiceLastAccessedDetails — exercises the backend function
	_, _, err := be.GetServiceLastAccessedDetails("arn:aws:iam::123456789012:user/test-user")
	// May return empty or error — just exercises the code path
	assert.NoError(t, err)
}

// TestIAMHandler_ServiceLastAccessedDetails_LiveHandlersWinOverShadowedStubs
// confirms buildDispatchTable's documented merge order (handler.go) actually
// resolves "GenerateServiceLastAccessedDetails" and
// "GetServiceLastAccessedDetails" to their real, state-reading
// implementations (handler_access_advisor.go), not the dead stubs of the
// same name in iamOrgsReportDispatch/iamMiscDispatchTable (handler_account.go,
// handler_providers.go) that ignore Arn/JobId entirely. A pre-recorded
// service access is only visible through the round trip below if the real
// handlers ran end to end.
func TestIAMHandler_ServiceLastAccessedDetails_LiveHandlersWinOverShadowedStubs(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)

	const entityARN = "arn:aws:iam::123456789012:user/alice"
	b.RecordServiceAccess(entityARN, "s3", "Amazon S3")

	genReq := iamRequest("GenerateServiceLastAccessedDetails", map[string]string{"Arn": entityARN})
	genRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(genReq, genRec)))
	assert.Equal(t, http.StatusOK, genRec.Code)

	var genResp struct {
		Result struct {
			JobID string `xml:"JobId"`
		} `xml:"GenerateServiceLastAccessedDetailsResult"`
	}
	require.NoError(t, xml.Unmarshal(genRec.Body.Bytes(), &genResp))
	require.NotEmpty(t, genResp.Result.JobID)

	getReq := iamRequest("GetServiceLastAccessedDetails", map[string]string{"JobId": genResp.Result.JobID})
	getRec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(getReq, getRec)))
	assert.Equal(t, http.StatusOK, getRec.Code)

	var getResp iam.GetServiceLastAccessedDetailsResponse
	require.NoError(t, xml.Unmarshal(getRec.Body.Bytes(), &getResp))
	require.Len(t, getResp.GetServiceLastAccessedDetailsResult.ServicesLastAccessed, 1)
	assert.Equal(t, "Amazon S3", getResp.GetServiceLastAccessedDetailsResult.ServicesLastAccessed[0].ServiceName)
}
