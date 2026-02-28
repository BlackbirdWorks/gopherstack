package acm_test

import (
	"encoding/json"
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

// postACMJSON sends an ACM JSON-protocol request with the given target and body.
func postACMJSON(t *testing.T, h *acm.Handler, target, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "CertificateManager."+target)

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestACMHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "RequestCertificate",
			run: func(t *testing.T) {
				h := newACMHandler()
				rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"example.com"}`)

				assert.Equal(t, http.StatusOK, rec.Code)

				var out map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Contains(t, out["CertificateArn"], "arn:aws:acm:")
			},
		},
		{
			name: "RequestCertificate_EmptyDomain",
			run: func(t *testing.T) {
				h := newACMHandler()
				rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":""}`)

				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "DescribeCertificate",
			run: func(t *testing.T) {
				h := newACMHandler()
				rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"describe-test.com"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				var created map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				arn := created["CertificateArn"]

				body, _ := json.Marshal(map[string]string{"CertificateArn": arn})
				rec2 := postACMJSON(t, h, "DescribeCertificate", string(body))
				assert.Equal(t, http.StatusOK, rec2.Code)
				assert.Contains(t, rec2.Body.String(), "describe-test.com")
			},
		},
		{
			name: "ListCertificates",
			run: func(t *testing.T) {
				h := newACMHandler()
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"list1.com"}`)
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"list2.com"}`)

				rec := postACMJSON(t, h, "ListCertificates", `{}`)
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "list1.com")
				assert.Contains(t, rec.Body.String(), "list2.com")
			},
		},
		{
			name: "DeleteCertificate",
			run: func(t *testing.T) {
				h := newACMHandler()
				rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"delete-cert.com"}`)
				require.Equal(t, http.StatusOK, rec.Code)

				var created map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
				arn := created["CertificateArn"]

				body, _ := json.Marshal(map[string]string{"CertificateArn": arn})
				rec2 := postACMJSON(t, h, "DeleteCertificate", string(body))
				assert.Equal(t, http.StatusOK, rec2.Code)
			},
		},
		{
			name: "DescribeCertificate_NotFound",
			run: func(t *testing.T) {
				const arn = "arn:aws:acm:us-east-1:000000000000:certificate/nonexistent"
				h := newACMHandler()
				body, _ := json.Marshal(map[string]string{"CertificateArn": arn})
				rec := postACMJSON(t, h, "DescribeCertificate", string(body))
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "InvalidAction",
			run: func(t *testing.T) {
				h := newACMHandler()
				rec := postACMJSON(t, h, "InvalidAction", `{}`)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "MissingAction",
			run: func(t *testing.T) {
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
				req.Header.Set("Content-Type", "application/x-amz-json-1.1")
				// No X-Amz-Target header
				rec := httptest.NewRecorder()

				h := newACMHandler()
				e := echo.New()
				c := e.NewContext(req, rec)

				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
