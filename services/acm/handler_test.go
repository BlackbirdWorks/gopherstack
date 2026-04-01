package acm_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
			name:         "RequestCertificate_DNS_validation",
			target:       "RequestCertificate",
			body:         `{"DomainName":"dns.example.com","ValidationMethod":"DNS"}`,
			wantCode:     http.StatusOK,
			wantContains: []string{"arn:aws:acm:"},
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
			name:   "DescribeCertificate_DNS_CNAME_records",
			target: "DescribeCertificate",
			setup: func(t *testing.T, h *acm.Handler) {
				t.Helper()
				postACMJSON(t, h, "RequestCertificate",
					`{"DomainName":"cname-test.com","ValidationMethod":"DNS"}`)
			},
			body:         "",
			wantCode:     http.StatusOK,
			wantContains: []string{"CNAME", "acm-validations.aws", "cname-test.com"},
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
					// For tests that need to reuse an ARN: list certs and use first ARN
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

func TestACMHandler_ImportCertificate(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	// Request cert to get a PEM body and key
	cert, err := b.RequestCertificate("import-test.example.com", "", "", nil)
	require.NoError(t, err)

	certPEM := cert.CertificateBody
	keyPEM := cert.PrivateKey

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: func() string {
				in, _ := json.Marshal(map[string]string{
					"Certificate": certPEM,
					"PrivateKey":  keyPEM,
				})

				return string(in)
			}(),
			wantCode:     http.StatusOK,
			wantContains: []string{"CertificateArn", "arn:aws:acm:"},
		},
		{
			name:     "missing_cert",
			body:     `{"PrivateKey":"dummy"}`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_key",
			body: func() string {
				in, _ := json.Marshal(map[string]string{"Certificate": certPEM})

				return string(in)
			}(),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			rec := postACMJSON(t, h, "ImportCertificate", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestACMHandler_RenewCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "not_found",
			body:     `{"CertificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/none"}`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			rec := postACMJSON(t, h, "RenewCertificate", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestACMHandler_ExportCertificate(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	cert, err := b.RequestCertificate("export-test.example.com", "", "", nil)
	require.NoError(t, err)

	importedCert, err := b.ImportCertificate(cert.CertificateBody, cert.PrivateKey, "")
	require.NoError(t, err)

	tests := []struct {
		name         string
		certARN      string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success_imported",
			certARN:      importedCert.ARN,
			wantCode:     http.StatusOK,
			wantContains: []string{"Certificate", "PrivateKey"},
		},
		{
			name:     "fails_amazon_issued",
			certARN:  cert.ARN,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "not_found",
			certARN:  "arn:aws:acm:us-east-1:000000000000:certificate/none",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := acm.NewHandler(b)
			body, _ := json.Marshal(map[string]string{
				"CertificateArn": tt.certARN,
				"Passphrase":     "dGVzdA==",
			})
			rec := postACMJSON(t, h, "ExportCertificate", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestACMHandler_GetCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			wantCode:     http.StatusOK,
			wantContains: []string{"Certificate", "BEGIN CERTIFICATE"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"get-cert.example.com"}`)
			require.Equal(t, http.StatusOK, reqRec.Code)

			var reqOut struct {
				CertificateArn string `json:"CertificateArn"`
			}
			require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

			body, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
			rec := postACMJSON(t, h, "GetCertificate", string(body))
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestACMHandler_DNSValidationWorkflow verifies the full PENDING_VALIDATION → ISSUED flow.
func TestACMHandler_DNSValidationWorkflow(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	// Request with DNS validation
	reqRec := postACMJSON(t, h, "RequestCertificate",
		`{"DomainName":"workflow.example.com","ValidationMethod":"DNS"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))
	require.NotEmpty(t, reqOut.CertificateArn)

	// Describe should show PENDING_VALIDATION with CNAME records
	descBody, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			Status                  string `json:"Status"`
			DomainValidationOptions []struct {
				ResourceRecord *struct {
					Type string `json:"Type"`
				} `json:"ResourceRecord"`
				ValidationStatus string `json:"ValidationStatus"`
			} `json:"DomainValidationOptions"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	// Initial describe may already show ISSUED (auto-validate is quick), so accept either.
	assert.Contains(t, []string{"PENDING_VALIDATION", "ISSUED"}, descOut.Certificate.Status)
	require.NotEmpty(t, descOut.Certificate.DomainValidationOptions)
	assert.NotNil(t, descOut.Certificate.DomainValidationOptions[0].ResourceRecord)
	assert.Equal(t, "CNAME", descOut.Certificate.DomainValidationOptions[0].ResourceRecord.Type)

	// Wait for auto-transition to ISSUED
	require.Eventually(t, func() bool {
		rec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
		var out struct {
			Certificate struct {
				Status string `json:"Status"`
			} `json:"Certificate"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &out)

		return out.Certificate.Status == "ISSUED"
	}, 2*time.Second, 50*time.Millisecond, "cert should transition to ISSUED")
}

func TestACMHandler_GetAccountConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		wantDaysContains string
		wantCode         int
	}{
		{
			name:             "returns_default_45_days",
			wantCode:         http.StatusOK,
			wantDaysContains: "45",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			rec := postACMJSON(t, h, "GetAccountConfiguration", `{}`)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantDaysContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantDaysContains)
			}
		})
	}
}

func TestACMHandler_PutAccountConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:     "set_30_days",
			body:     `{"IdempotencyToken":"tok1","ExpiryEvents":{"DaysBeforeExpiry":30}}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "idempotent_second_call_same_token",
			body:     `{"IdempotencyToken":"tok1","ExpiryEvents":{"DaysBeforeExpiry":30}}`,
			wantCode: http.StatusOK,
		},
		{
			name:         "missing_idempotency_token",
			body:         `{"ExpiryEvents":{"DaysBeforeExpiry":30}}`,
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException"},
		},
		{
			name:         "negative_days_before_expiry",
			body:         `{"IdempotencyToken":"tok2","ExpiryEvents":{"DaysBeforeExpiry":-1}}`,
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			rec := postACMJSON(t, h, "PutAccountConfiguration", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestACMHandler_PutAndGetAccountConfiguration(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	// Set to 30 days
	putRec := postACMJSON(t, h, "PutAccountConfiguration",
		`{"IdempotencyToken":"abc","ExpiryEvents":{"DaysBeforeExpiry":30}}`)
	require.Equal(t, http.StatusOK, putRec.Code)

	// Get should reflect new value
	getRec := postACMJSON(t, h, "GetAccountConfiguration", `{}`)
	require.Equal(t, http.StatusOK, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "30")
	assert.NotContains(t, getRec.Body.String(), "45")
}

func TestACMHandler_ResendValidationEmail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *acm.Handler) string
		buildBody    func(certARN string) string
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success_pending_email",
			setup: func(t *testing.T, h *acm.Handler) string {
				t.Helper()
				rec := postACMJSON(t, h, "RequestCertificate",
					`{"DomainName":"email-resend.example.com","ValidationMethod":"EMAIL"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					CertificateArn string `json:"CertificateArn"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out.CertificateArn
			},
			buildBody: func(certARN string) string {
				b, _ := json.Marshal(map[string]string{
					"CertificateArn":   certARN,
					"Domain":           "email-resend.example.com",
					"ValidationDomain": "email-resend.example.com",
				})

				return string(b)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "cert_not_found",
			setup: func(_ *testing.T, _ *acm.Handler) string {
				return "arn:aws:acm:us-east-1:000000000000:certificate/none"
			},
			buildBody: func(certARN string) string {
				b, _ := json.Marshal(map[string]string{
					"CertificateArn":   certARN,
					"Domain":           "example.com",
					"ValidationDomain": "example.com",
				})

				return string(b)
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ResourceNotFoundException"},
		},
		{
			name: "cert_already_issued",
			setup: func(t *testing.T, h *acm.Handler) string {
				t.Helper()
				rec := postACMJSON(t, h, "RequestCertificate",
					`{"DomainName":"issued.example.com"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					CertificateArn string `json:"CertificateArn"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out.CertificateArn
			},
			buildBody: func(certARN string) string {
				b, _ := json.Marshal(map[string]string{
					"CertificateArn":   certARN,
					"Domain":           "issued.example.com",
					"ValidationDomain": "issued.example.com",
				})

				return string(b)
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException"},
		},
		{
			name: "dns_cert_rejects_resend",
			setup: func(t *testing.T, h *acm.Handler) string {
				t.Helper()
				rec := postACMJSON(t, h, "RequestCertificate",
					`{"DomainName":"dns.example.com","ValidationMethod":"DNS"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					CertificateArn string `json:"CertificateArn"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out.CertificateArn
			},
			buildBody: func(certARN string) string {
				b, _ := json.Marshal(map[string]string{
					"CertificateArn":   certARN,
					"Domain":           "dns.example.com",
					"ValidationDomain": "dns.example.com",
				})

				return string(b)
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException"},
		},
		{
			name:  "missing_domain",
			setup: func(_ *testing.T, _ *acm.Handler) string { return "" },
			buildBody: func(_ string) string {
				return `{"CertificateArn":"arn:x","ValidationDomain":"x.com"}`
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			certARN := ""
			if tt.setup != nil {
				certARN = tt.setup(t, h)
			}
			body := tt.buildBody(certARN)
			rec := postACMJSON(t, h, "ResendValidationEmail", body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestACMHandler_RevokeCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *acm.Handler) string
		buildBody    func(certARN string) string
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success_unspecified",
			setup: func(t *testing.T, h *acm.Handler) string {
				t.Helper()
				rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"revoke.example.com"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					CertificateArn string `json:"CertificateArn"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out.CertificateArn
			},
			buildBody: func(certARN string) string {
				b, _ := json.Marshal(map[string]string{
					"CertificateArn":   certARN,
					"RevocationReason": "UNSPECIFIED",
				})

				return string(b)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "success_key_compromise",
			setup: func(t *testing.T, h *acm.Handler) string {
				t.Helper()
				rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"revoke2.example.com"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					CertificateArn string `json:"CertificateArn"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out.CertificateArn
			},
			buildBody: func(certARN string) string {
				b, _ := json.Marshal(map[string]string{
					"CertificateArn":   certARN,
					"RevocationReason": "KEY_COMPROMISE",
				})

				return string(b)
			},
			wantCode: http.StatusOK,
		},
		{
			name:  "cert_not_found",
			setup: func(_ *testing.T, _ *acm.Handler) string { return "" },
			buildBody: func(_ string) string {
				return `{"CertificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/none","RevocationReason":"UNSPECIFIED"}`
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ResourceNotFoundException"},
		},
		{
			name:  "invalid_reason",
			setup: func(_ *testing.T, _ *acm.Handler) string { return "" },
			buildBody: func(_ string) string {
				return `{"CertificateArn":"arn:x","RevocationReason":"BOGUS_REASON"}`
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException"},
		},
		{
			name: "already_revoked",
			setup: func(t *testing.T, h *acm.Handler) string {
				t.Helper()
				rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"revoke3.example.com"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					CertificateArn string `json:"CertificateArn"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				// Revoke once
				b, _ := json.Marshal(map[string]string{
					"CertificateArn":   out.CertificateArn,
					"RevocationReason": "UNSPECIFIED",
				})
				postACMJSON(t, h, "RevokeCertificate", string(b))

				return out.CertificateArn
			},
			buildBody: func(certARN string) string {
				b, _ := json.Marshal(map[string]string{
					"CertificateArn":   certARN,
					"RevocationReason": "UNSPECIFIED",
				})

				return string(b)
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidStateException"},
		},
		{
			name:  "missing_revocation_reason",
			setup: func(_ *testing.T, _ *acm.Handler) string { return "" },
			buildBody: func(_ string) string {
				return `{"CertificateArn":"arn:x"}`
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			certARN := ""
			if tt.setup != nil {
				certARN = tt.setup(t, h)
			}
			body := tt.buildBody(certARN)
			rec := postACMJSON(t, h, "RevokeCertificate", body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestACMHandler_RevokeCertificate_DescribeShowsRevoked(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	// Create a cert
	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"revoke-describe.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	// Revoke it
	revokeBody, _ := json.Marshal(map[string]string{
		"CertificateArn":   reqOut.CertificateArn,
		"RevocationReason": "KEY_COMPROMISE",
	})
	revokeRec := postACMJSON(t, h, "RevokeCertificate", string(revokeBody))
	require.Equal(t, http.StatusOK, revokeRec.Code)

	// Describe should show REVOKED with RevocationReason
	descBody, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			RevokedAt        *int64 `json:"RevokedAt"`
			Status           string `json:"Status"`
			RevocationReason string `json:"RevocationReason"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, "REVOKED", descOut.Certificate.Status)
	assert.Equal(t, "KEY_COMPROMISE", descOut.Certificate.RevocationReason)
	assert.NotNil(t, descOut.Certificate.RevokedAt)
}

func TestACMHandler_UpdateCertificateOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *acm.Handler) string
		buildBody    func(certARN string) string
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "enable_transparency_logging",
			setup: func(t *testing.T, h *acm.Handler) string {
				t.Helper()
				rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"opts.example.com"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					CertificateArn string `json:"CertificateArn"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out.CertificateArn
			},
			buildBody: func(certARN string) string {
				b, _ := json.Marshal(map[string]any{
					"CertificateArn": certARN,
					"Options": map[string]string{
						"CertificateTransparencyLoggingPreference": "ENABLED",
					},
				})

				return string(b)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "disable_transparency_logging",
			setup: func(t *testing.T, h *acm.Handler) string {
				t.Helper()
				rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"opts2.example.com"}`)
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					CertificateArn string `json:"CertificateArn"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out.CertificateArn
			},
			buildBody: func(certARN string) string {
				b, _ := json.Marshal(map[string]any{
					"CertificateArn": certARN,
					"Options": map[string]string{
						"CertificateTransparencyLoggingPreference": "DISABLED",
					},
				})

				return string(b)
			},
			wantCode: http.StatusOK,
		},
		{
			name:  "cert_not_found",
			setup: func(_ *testing.T, _ *acm.Handler) string { return "" },
			buildBody: func(_ string) string {
				const certARN = "arn:aws:acm:us-east-1:000000000000:certificate/none"
				b, _ := json.Marshal(map[string]any{
					"CertificateArn": certARN,
					"Options": map[string]string{
						"CertificateTransparencyLoggingPreference": "ENABLED",
					},
				})

				return string(b)
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ResourceNotFoundException"},
		},
		{
			name:  "invalid_preference",
			setup: func(_ *testing.T, _ *acm.Handler) string { return "" },
			buildBody: func(_ string) string {
				return `{"CertificateArn":"arn:x","Options":{"CertificateTransparencyLoggingPreference":"BOGUS"}}`
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException"},
		},
		{
			name:  "missing_preference",
			setup: func(_ *testing.T, _ *acm.Handler) string { return "" },
			buildBody: func(_ string) string {
				return `{"CertificateArn":"arn:x","Options":{}}`
			},
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			certARN := ""
			if tt.setup != nil {
				certARN = tt.setup(t, h)
			}
			body := tt.buildBody(certARN)
			rec := postACMJSON(t, h, "UpdateCertificateOptions", body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestACMHandler_UpdateCertificateOptions_DescribeShowsOptions(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	// Create a cert
	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"opts-describe.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	// Update options
	optsBody, _ := json.Marshal(map[string]any{
		"CertificateArn": reqOut.CertificateArn,
		"Options": map[string]string{
			"CertificateTransparencyLoggingPreference": "DISABLED",
		},
	})
	optsRec := postACMJSON(t, h, "UpdateCertificateOptions", string(optsBody))
	require.Equal(t, http.StatusOK, optsRec.Code)

	// Describe should reflect options
	descBody, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			Options *struct {
				CertificateTransparencyLoggingPreference string `json:"CertificateTransparencyLoggingPreference"`
			} `json:"Options"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	require.NotNil(t, descOut.Certificate.Options)
	assert.Equal(t, "DISABLED", descOut.Certificate.Options.CertificateTransparencyLoggingPreference)
}
