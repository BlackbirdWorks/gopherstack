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

	"github.com/blackbirdworks/gopherstack/pkgs/service"
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
		XMLName           xml.Name `xml:"PublicAccessBlockConfiguration"`
		BlockPublicAcls   bool     `xml:"BlockPublicAcls"`
		BlockPublicPolicy bool     `xml:"BlockPublicPolicy"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes()[len(xml.Header):], &out))
	assert.True(t, out.BlockPublicAcls)
	assert.True(t, out.BlockPublicPolicy)
}

func TestS3Control_Provider(t *testing.T) {
	t.Parallel()

	p := &s3control.Provider{}
	assert.Equal(t, "S3Control", p.Name())
}

func TestS3Control_Handler_Name(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	assert.Equal(t, "S3Control", h.Name())
}

func TestS3Control_Handler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "GetPublicAccessBlock")
	assert.Contains(t, ops, "PutPublicAccessBlock")
	assert.Contains(t, ops, "DeletePublicAccessBlock")
}

func TestS3Control_Handler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	assert.Equal(t, 85, h.MatchPriority())
}

func TestS3Control_Handler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	e := echo.New()

	for _, tc := range []struct {
		method string
		want   string
	}{
		{http.MethodGet, "GetPublicAccessBlock"},
		{http.MethodPut, "PutPublicAccessBlock"},
		{http.MethodDelete, "DeletePublicAccessBlock"},
	} {
		req := httptest.NewRequest(tc.method, publicAccessBlockPath, nil)
		c := e.NewContext(req, httptest.NewRecorder())
		assert.Equal(t, tc.want, h.ExtractOperation(c))
	}

	// Unknown path
	req := httptest.NewRequest(http.MethodGet, "/other/path", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "Unknown", h.ExtractOperation(c))
}

func TestS3Control_Handler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodGet, publicAccessBlockPath, nil)
	req.Header.Set("X-Amz-Account-Id", "123456789012")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "123456789012", h.ExtractResource(c))
}

func TestS3Control_Handler_DeletePublicAccessBlock_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)

	rec := doS3ControlRequest(t, h, http.MethodDelete, "nonexistent-account", "")
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestS3Control_Handler_GetWithDefaultAccount(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)

	// Put a config using empty account (uses "default")
	putBody := `<PublicAccessBlockConfiguration><BlockPublicAcls>true</BlockPublicAcls></PublicAccessBlockConfiguration>`
	putRec := doS3ControlRequest(t, h, http.MethodPut, "", putBody)
	assert.Equal(t, http.StatusCreated, putRec.Code)

	// Get it back using empty account ID (uses "default")
	getRec := doS3ControlRequest(t, h, http.MethodGet, "", "")
	assert.Equal(t, http.StatusOK, getRec.Code)

	// Delete using empty account ID
	delRec := doS3ControlRequest(t, h, http.MethodDelete, "", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)
}

func TestS3Control_Handler_InvalidMethod(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, publicAccessBlockPath, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestS3Control_Backend_ListAll(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	assert.Empty(t, b.ListAll())

	b.PutPublicAccessBlock(s3control.PublicAccessBlock{AccountID: "acc1", BlockPublicAcls: true})
	b.PutPublicAccessBlock(s3control.PublicAccessBlock{AccountID: "acc2", BlockPublicPolicy: true})

	all := b.ListAll()
	assert.Len(t, all, 2)
}

func TestS3Control_Handler_PutInvalidXML(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPut, publicAccessBlockPath, strings.NewReader("not-xml"))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestS3Control_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &s3control.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "S3Control", svc.Name())
}
