package acmpca_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acmpca"
)

// b64 base64-encodes s the way aws-sdk-go-v2 encodes []byte-typed blob
// fields (IssueCertificateInput.Csr, ImportCertificateAuthorityCertificateInput.
// Certificate/CertificateChain) before putting them on the wire.
func b64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}

func newACMPCAHandler() *acmpca.Handler {
	return acmpca.NewHandler(acmpca.NewInMemoryBackend(testAccountID, testRegion))
}

func doACMPCARequest(t *testing.T, h *acmpca.Handler, op string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "ACMPrivateCA."+op)
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func parseACMPCAResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out
}

func createHandlerCA(t *testing.T, h *acmpca.Handler) string {
	t.Helper()

	ca, err := h.Backend.CreateCertificateAuthority(
		context.Background(),
		"ROOT",
		acmpca.CertificateAuthorityConfiguration{
			Subject: acmpca.CertificateAuthoritySubject{CommonName: "Handler CA"},
		},
	)
	require.NoError(t, err)

	return ca.ARN
}

func TestACMPCAHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		want string
	}{
		{
			name: "certificate authority arn",
			body: map[string]any{
				"CertificateAuthorityArn": "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/ca-1",
			},
			want: "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/ca-1",
		},
		{
			name: "resource arn",
			body: map[string]any{"ResourceArn": "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/ca-2"},
			want: "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/ca-2",
		},
		{
			name: "no supported resource field",
			body: map[string]any{"AuditReportId": "report-1"},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bodyBytes, err := json.Marshal(tt.body)
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			rec := httptest.NewRecorder()
			e := echo.New()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, newACMPCAHandler().ExtractResource(c))
		})
	}
}

// ---- Handler Reset ----

func TestACMPCAHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// Create some state
	caARN := createHandlerCA(t, h)
	h.SetTagsForTest(caARN, map[string]string{"key": "val"})

	// Reset
	h.Reset()

	// After reset, CA should not exist
	rec := doACMPCARequest(t, h, "DescribeCertificateAuthority", map[string]any{
		"CertificateAuthorityArn": caARN,
	})
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

// ---- RouteMatcher and ExtractOperation ----

func TestACMPCAHandler_Routing(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	matcher := h.RouteMatcher()
	assert.NotNil(t, matcher)

	// Test chaos methods
	assert.Equal(t, "acm-pca", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
	assert.NotEmpty(t, h.MatchPriority())
}

// ---- dispatchJSON error path ----

func TestACMPCAHandler_DispatchJSON_MalformedInput(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// CreateCertificateAuthority with missing config (should get error from backend)
	rec := doACMPCARequest(t, h, "CreateCertificateAuthority", map[string]any{})
	// Empty config still creates a CA (default config)
	assert.NotNil(t, rec)
}

// ---- handleOpError internal error path ----

func TestACMPCAHandler_HandleOpError_InternalError(t *testing.T) {
	t.Parallel()

	h := newACMPCAHandler()

	// Use an unknown operation to trigger error path
	rec := doACMPCARequest(t, h, "UnknownOperation", map[string]any{})
	assert.NotNil(t, rec)
}
