package acm_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acm"
)

func newACMHandler() *acm.Handler {
	return acm.NewHandler(acm.NewInMemoryBackend("000000000000", "us-east-1"))
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
		setup        func(t *testing.T, h *acm.Handler)
		name         string
		target       string
		body         string
		wantContains []string
		wantCode     int
		omitTarget   bool
	}{
		{
			name:         "RequestCertificate",
			target:       "RequestCertificate",
			body:         `{"DomainName":"example.com"}`,
			wantCode:     http.StatusOK,
			wantContains: []string{"arn:aws:acm:"},
		},
		{
			name:     "RequestCertificate_EmptyDomain",
			target:   "RequestCertificate",
			body:     `{"DomainName":""}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "DescribeCertificate_NotFound",
			target:   "DescribeCertificate",
			body:     `{"CertificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/nonexistent"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "DescribeCertificate_AfterCreate",
			target: "DescribeCertificate",
			setup: func(t *testing.T, h *acm.Handler) {
				t.Helper()
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"describe-test.com"}`)
			},
			body:         "", // filled dynamically below won't work; use setup to get ARN
			wantCode:     http.StatusOK,
			wantContains: []string{"describe-test.com"},
		},
		{
			name:   "ListCertificates",
			target: "ListCertificates",
			setup: func(t *testing.T, h *acm.Handler) {
				t.Helper()
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"list1.com"}`)
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"list2.com"}`)
			},
			body:         `{}`,
			wantCode:     http.StatusOK,
			wantContains: []string{"list1.com", "list2.com"},
		},
		{
			name:   "DeleteCertificate",
			target: "DeleteCertificate",
			setup: func(t *testing.T, h *acm.Handler) {
				t.Helper()
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"delete-test.com"}`)
			},
			body:     "",
			wantCode: http.StatusOK,
		},
		{
			name:         "DeleteCertificate_NotFound",
			target:       "DeleteCertificate",
			body:         `{"CertificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/nonexistent"}`,
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ResourceNotFoundException"},
		},
		{
			name:   "AddTagsToCertificate",
			target: "AddTagsToCertificate",
			body: `{"CertificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/t1",` +
				`"Tags":[{"Key":"Env","Value":"prod"}]}`,
			wantCode: http.StatusOK,
		},
		{
			name:   "ListTagsForCertificate",
			target: "ListTagsForCertificate",
			setup: func(t *testing.T, h *acm.Handler) {
				t.Helper()
				addBody := `{"CertificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/t2",` +
					`"Tags":[{"Key":"Env","Value":"staging"}]}`
				postACMJSON(t, h, "AddTagsToCertificate", addBody)
			},
			body: `{"CertificateArn":` +
				`"arn:aws:acm:us-east-1:000000000000:certificate/t2"}`,
			wantCode:     http.StatusOK,
			wantContains: []string{"Env", "staging"},
		},
		{
			name:   "RemoveTagsFromCertificate",
			target: "RemoveTagsFromCertificate",
			setup: func(t *testing.T, h *acm.Handler) {
				t.Helper()
				addBody := `{"CertificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/t3",` +
					`"Tags":[{"Key":"Env","Value":"dev"}]}`
				postACMJSON(t, h, "AddTagsToCertificate", addBody)
			},
			body: `{"CertificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/t3",` +
				`"Tags":[{"Key":"Env"}]}`,
			wantCode: http.StatusOK,
		},
		{
			name:         "UnknownAction",
			target:       "BogusAction",
			body:         `{}`,
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidAction"},
		},
		{
			name:       "MissingAction",
			body:       `{}`,
			omitTarget: true,
			wantCode:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			var rec *httptest.ResponseRecorder
			if tt.omitTarget {
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
				req.Header.Set("Content-Type", "application/x-amz-json-1.1")
				rec = httptest.NewRecorder()
				e := echo.New()
				c := e.NewContext(req, rec)
				err := h.Handler()(c)
				require.NoError(t, err)
			} else {
				body := tt.body
				if body == "" {
					// For DescribeCertificate_AfterCreate, list certs and use first ARN
					listRec := postACMJSON(t, h, "ListCertificates", `{}`)
					var listResp struct {
						CertificateSummaryList []struct {
							CertificateArn string `json:"CertificateArn"`
						} `json:"CertificateSummaryList"`
					}
					require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
					require.NotEmpty(t, listResp.CertificateSummaryList)
					b, _ := json.Marshal(
						map[string]string{"CertificateArn": listResp.CertificateSummaryList[0].CertificateArn},
					)
					body = string(b)
				}
				rec = postACMJSON(t, h, tt.target, body)
			}

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}
