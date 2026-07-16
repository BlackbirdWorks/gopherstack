package iam_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ServerCertificate_UploadAndGet(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	certBody := "-----BEGIN CERTIFICATE-----\nMIIBxxx\n-----END CERTIFICATE-----"

	req := iamRequest("UploadServerCertificate", map[string]string{
		"ServerCertificateName": "my-tls-cert",
		"CertificateBody":       certBody,
		"Path":                  "/",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	uploadBody := rec.Body.String()
	assert.Contains(t, uploadBody, "my-tls-cert")
	assert.Contains(t, uploadBody, "arn:aws:iam::")

	// Get.
	req2 := iamRequest("GetServerCertificate", map[string]string{
		"ServerCertificateName": "my-tls-cert",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "my-tls-cert")
}

func TestHandler_ServerCertificate_CRUD(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)
	const certBody = "-----BEGIN CERTIFICATE-----\nfakecert\n-----END CERTIFICATE-----"

	// UploadServerCertificate.
	req := iamRequest("UploadServerCertificate", map[string]string{
		"ServerCertificateName": "MyCert",
		"CertificateBody":       certBody,
		"PrivateKey":            "-----BEGIN RSA PRIVATE KEY-----\nfakekey\n-----END RSA PRIVATE KEY-----",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code, "UploadServerCertificate must succeed")
	assert.Contains(t, rec.Body.String(), "MyCert")

	// GetServerCertificate.
	req2 := iamRequest("GetServerCertificate", map[string]string{
		"ServerCertificateName": "MyCert",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "MyCert")

	// ListServerCertificates.
	req3 := iamRequest("ListServerCertificates", map[string]string{})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
	assert.Contains(t, rec3.Body.String(), "MyCert")

	// UpdateServerCertificate.
	req4 := iamRequest("UpdateServerCertificate", map[string]string{
		"ServerCertificateName":    "MyCert",
		"NewServerCertificateName": "RenamedCert",
	})
	rec4 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req4, rec4)))
	assert.Equal(t, http.StatusOK, rec4.Code)

	// DeleteServerCertificate.
	req5 := iamRequest("DeleteServerCertificate", map[string]string{
		"ServerCertificateName": "RenamedCert",
	})
	rec5 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req5, rec5)))
	assert.Equal(t, http.StatusOK, rec5.Code)
}
