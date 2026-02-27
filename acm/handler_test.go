package acm_test

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

	"github.com/blackbirdworks/gopherstack/acm"
)

func newACMHandler() *acm.Handler {
	return acm.NewHandler(acm.NewInMemoryBackend("000000000000", "us-east-1"), slog.Default())
}

func postACMForm(t *testing.T, h *acm.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestACMHandler_RequestCertificate(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	rec := postACMForm(t, h, "Action=RequestCertificate&Version=2015-12-08&DomainName=example.com")

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RequestCertificateResponse")
	assert.Contains(t, rec.Body.String(), "arn:aws:acm:")
}

func TestACMHandler_RequestCertificate_EmptyDomain(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	rec := postACMForm(t, h, "Action=RequestCertificate&Version=2015-12-08&DomainName=")

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ErrorResponse")
}

func TestACMHandler_DescribeCertificate(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	rec := postACMForm(t, h, "Action=RequestCertificate&Version=2015-12-08&DomainName=describe-test.com")
	require.Equal(t, http.StatusOK, rec.Code)

	// Extract ARN from response
	type reqResp struct {
		XMLName xml.Name `xml:"RequestCertificateResponse"`
		ARN     string   `xml:"RequestCertificateResult>CertificateArn"`
	}
	var result reqResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))

	rec2 := postACMForm(t, h, "Action=DescribeCertificate&Version=2015-12-08&CertificateArn="+result.ARN)
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "DescribeCertificateResponse")
	assert.Contains(t, rec2.Body.String(), "describe-test.com")
}

func TestACMHandler_ListCertificates(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	postACMForm(t, h, "Action=RequestCertificate&Version=2015-12-08&DomainName=list1.com")
	postACMForm(t, h, "Action=RequestCertificate&Version=2015-12-08&DomainName=list2.com")

	rec := postACMForm(t, h, "Action=ListCertificates&Version=2015-12-08")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListCertificatesResponse")
	assert.Contains(t, rec.Body.String(), "list1.com")
	assert.Contains(t, rec.Body.String(), "list2.com")
}

func TestACMHandler_DeleteCertificate(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	rec := postACMForm(t, h, "Action=RequestCertificate&Version=2015-12-08&DomainName=delete-cert.com")
	require.Equal(t, http.StatusOK, rec.Code)

	type reqResp struct {
		XMLName xml.Name `xml:"RequestCertificateResponse"`
		ARN     string   `xml:"RequestCertificateResult>CertificateArn"`
	}
	var result reqResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))

	rec2 := postACMForm(t, h, "Action=DeleteCertificate&Version=2015-12-08&CertificateArn="+result.ARN)
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "DeleteCertificateResponse")
}

func TestACMHandler_DescribeCertificate_NotFound(t *testing.T) {
	t.Parallel()

	const notFoundForm = "Action=DescribeCertificate&Version=2015-12-08" +
		"&CertificateArn=arn:aws:acm:us-east-1:000000000000:certificate/nonexistent"

	h := newACMHandler()
	rec := postACMForm(t, h, notFoundForm)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ErrorResponse")
}

func TestACMHandler_InvalidAction(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	rec := postACMForm(t, h, "Action=InvalidAction&Version=2015-12-08")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestACMHandler_MissingAction(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	rec := postACMForm(t, h, "Version=2015-12-08")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
