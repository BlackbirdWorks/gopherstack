package s3control_test

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/s3control"
)

func newTestS3ControlHandler(t *testing.T) *s3control.Handler {
	t.Helper()

	return s3control.NewHandler(s3control.NewInMemoryBackend(), slog.Default())
}

const publicAccessBlockPath = "/v20180820/configuration/publicAccessBlock"

func doS3ControlRequest(
	t *testing.T,
	h *s3control.Handler,
	method, accountID, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, publicAccessBlockPath, strings.NewReader(body))
	if accountID != "" {
		req.Header.Set("X-Amz-Account-Id", accountID)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestS3Control_Handler_PutAndGetPublicAccessBlock(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)

	putBody := `<PublicAccessBlockConfiguration>
		<BlockPublicAcls>true</BlockPublicAcls>
		<IgnorePublicAcls>true</IgnorePublicAcls>
		<BlockPublicPolicy>false</BlockPublicPolicy>
		<RestrictPublicBuckets>false</RestrictPublicBuckets>
	</PublicAccessBlockConfiguration>`

	putRec := doS3ControlRequest(
		t,
		h,
		http.MethodPut,
		"000000000000",
		putBody,
	)
	assert.Equal(t, http.StatusCreated, putRec.Code)

	getRec := doS3ControlRequest(t, h, http.MethodGet, "000000000000", "")
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "BlockPublicAcls")
}

func TestS3Control_Handler_DeletePublicAccessBlock(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)

	putBody := `<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlockConfiguration>`
	doS3ControlRequest(t, h, http.MethodPut, "000000000000", putBody)

	delRec := doS3ControlRequest(
		t,
		h,
		http.MethodDelete,
		"000000000000",
		"",
	)
	assert.Equal(t, http.StatusNoContent, delRec.Code)

	getRec := doS3ControlRequest(t, h, http.MethodGet, "000000000000", "")
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

func TestS3Control_Handler_GetPublicAccessBlock_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)

	rec := doS3ControlRequest(t, h, http.MethodGet, "999999999999", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestS3Control_Handler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, publicAccessBlockPath, nil)
	c := e.NewContext(req, httptest.NewRecorder())

	assert.True(t, matcher(c))
}

func TestS3Control_Handler_RouteMatcher_NoMatch(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/s3/bucket", nil)
	c := e.NewContext(req, httptest.NewRecorder())

	assert.False(t, matcher(c))
}

func TestS3Control_Handler_XMLResponse(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)

	putBody := `<PublicAccessBlockConfiguration>
		<BlockPublicAcls>true</BlockPublicAcls>
		<IgnorePublicAcls>false</IgnorePublicAcls>
		<BlockPublicPolicy>true</BlockPublicPolicy>
		<RestrictPublicBuckets>true</RestrictPublicBuckets>
	</PublicAccessBlockConfiguration>`
	doS3ControlRequest(t, h, http.MethodPut, "test-account", putBody)

	rec := doS3ControlRequest(t, h, http.MethodGet, "test-account", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		XMLName xml.Name `xml:"GetPublicAccessBlockOutput"`
		Config  struct {
			BlockPublicAcls   bool `xml:"BlockPublicAcls"`
			BlockPublicPolicy bool `xml:"BlockPublicPolicy"`
		} `xml:"PublicAccessBlockConfiguration"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes()[len(xml.Header):], &out))
	assert.True(t, out.Config.BlockPublicAcls)
	assert.True(t, out.Config.BlockPublicPolicy)
}

func TestS3Control_Provider(t *testing.T) {
	t.Parallel()

	p := &s3control.Provider{}
	assert.Equal(t, "S3Control", p.Name())
}
