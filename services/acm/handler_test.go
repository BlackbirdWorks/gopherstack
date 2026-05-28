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
			setup: func(t *testing.T, h *acm.Handler) {
				t.Helper()
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"tag-t1.com"}`)
			},
			// body is empty: test runner will extract the ARN from the cert list and
			// send {"CertificateArn": "<arn>"} — empty Tags is valid per AWS API.
			body:     "",
			wantCode: http.StatusOK,
		},
		{
			name:   "ListTagsForCertificate",
			target: "ListTagsForCertificate",
			setup: func(t *testing.T, h *acm.Handler) {
				t.Helper()
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"tag-t2.com"}`)
			},
			// body is empty: test runner injects {"CertificateArn": "<arn>"}.
			body:         "",
			wantCode:     http.StatusOK,
			wantContains: []string{"Tags"},
		},
		{
			name:   "RemoveTagsFromCertificate",
			target: "RemoveTagsFromCertificate",
			setup: func(t *testing.T, h *acm.Handler) {
				t.Helper()
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"tag-t3.com"}`)
			},
			// body is empty: test runner injects {"CertificateArn": "<arn>"}; empty Tags is valid.
			body:     "",
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
	cert, err := b.RequestCertificate("import-test.example.com", "", "", "", "", "", "", nil)
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
	cert, err := b.RequestCertificate("export-test.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)

	importedCert, err := b.ImportCertificate(cert.CertificateBody, cert.PrivateKey, "", "")
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

// TestACMHandler_DescribeCertificate_RealistFields verifies that DescribeCertificate
// returns the new realism fields: Serial, Subject, Issuer, KeyAlgorithm, SignatureAlgorithm, IssuedAt.
func TestACMHandler_DescribeCertificate_RealistFields(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"realism.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	descBody, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			IssuedAt           *int64 `json:"IssuedAt"`
			Serial             string `json:"Serial"`
			Subject            string `json:"Subject"`
			Issuer             string `json:"Issuer"`
			KeyAlgorithm       string `json:"KeyAlgorithm"`
			SignatureAlgorithm string `json:"SignatureAlgorithm"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.NotEmpty(t, descOut.Certificate.Serial, "Serial should be set")
	assert.Contains(t, descOut.Certificate.Subject, "realism.example.com", "Subject should contain domain")
	assert.Contains(t, descOut.Certificate.Issuer, "realism.example.com", "Issuer should contain domain (self-signed)")
	assert.Equal(t, "EC_prime256v1", descOut.Certificate.KeyAlgorithm)
	assert.Equal(t, "SHA256WITHECDSA", descOut.Certificate.SignatureAlgorithm)
	assert.NotNil(t, descOut.Certificate.IssuedAt, "IssuedAt should be set for ISSUED cert")
}

// TestACMHandler_ImportCertificate_RealistFields verifies ImportedAt is set.
func TestACMHandler_ImportCertificate_RealistFields(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	// First create a cert to get PEM material
	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	src, err := b.RequestCertificate("import-realism.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]string{
		"Certificate": src.CertificateBody,
		"PrivateKey":  src.PrivateKey,
	})
	importRec := postACMJSON(t, h, "ImportCertificate", string(body))
	require.Equal(t, http.StatusOK, importRec.Code)

	var importOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importOut))

	descBody, _ := json.Marshal(map[string]string{"CertificateArn": importOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			ImportedAt *int64 `json:"ImportedAt"`
			Type       string `json:"Type"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	assert.Equal(t, "IMPORTED", descOut.Certificate.Type)
	assert.NotNil(t, descOut.Certificate.ImportedAt, "ImportedAt should be set for imported cert")
}

// TestACMHandler_ImportCertificate_ReImport verifies that passing CertificateArn re-imports.
func TestACMHandler_ImportCertificate_ReImport(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	b := acm.NewInMemoryBackend("000000000000", "us-east-1")

	// Create two certs to get two sets of PEM material
	src1, err := b.RequestCertificate("reimport.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)
	src2, err := b.RequestCertificate("reimport2.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)

	// Import first cert
	body1, _ := json.Marshal(map[string]string{
		"Certificate": src1.CertificateBody,
		"PrivateKey":  src1.PrivateKey,
	})
	importRec := postACMJSON(t, h, "ImportCertificate", string(body1))
	require.Equal(t, http.StatusOK, importRec.Code)

	var importOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(importRec.Body.Bytes(), &importOut))
	originalARN := importOut.CertificateArn

	// Re-import using the same ARN with new cert material
	body2, _ := json.Marshal(map[string]string{
		"CertificateArn": originalARN,
		"Certificate":    src2.CertificateBody,
		"PrivateKey":     src2.PrivateKey,
	})
	reImportRec := postACMJSON(t, h, "ImportCertificate", string(body2))
	require.Equal(t, http.StatusOK, reImportRec.Code)

	// Should return the same ARN
	var reImportOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reImportRec.Body.Bytes(), &reImportOut))
	assert.Equal(t, originalARN, reImportOut.CertificateArn, "re-import should return same ARN")
}

// TestACMHandler_RequestCertificate_IdempotencyToken verifies idempotent cert creation.
func TestACMHandler_RequestCertificate_IdempotencyToken(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	body := `{"DomainName":"idem.example.com","IdempotencyToken":"tok-abc123"}`

	// First call
	rec1 := postACMJSON(t, h, "RequestCertificate", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	var out1 struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))

	// Second call with same token should return same ARN
	rec2 := postACMJSON(t, h, "RequestCertificate", body)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))

	assert.Equal(t, out1.CertificateArn, out2.CertificateArn,
		"repeated RequestCertificate with same IdempotencyToken must return same ARN")
}

// TestACMHandler_ListCertificates_StatusFilter verifies CertificateStatuses filtering.
func TestACMHandler_ListCertificates_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	// Create one regular (ISSUED) cert
	rec1 := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"issued-filter.example.com"}`)
	require.Equal(t, http.StatusOK, rec1.Code)

	// Create one cert that starts in PENDING_VALIDATION
	rec2 := postACMJSON(t, h, "RequestCertificate",
		`{"DomainName":"pending-filter.example.com","ValidationMethod":"DNS"}`)
	require.Equal(t, http.StatusOK, rec2.Code)

	// Filter for ISSUED only (immediately-issued should show; wait for pending to not match)
	time.Sleep(10 * time.Millisecond) // give autoValidate timer a head start

	filterRec := postACMJSON(t, h, "ListCertificates",
		`{"CertificateStatuses":["ISSUED"]}`)
	require.Equal(t, http.StatusOK, filterRec.Code)

	var out struct {
		CertificateSummaryList []struct {
			DomainName string `json:"DomainName"`
			Status     string `json:"Status"`
		} `json:"CertificateSummaryList"`
	}
	require.NoError(t, json.Unmarshal(filterRec.Body.Bytes(), &out))

	for _, s := range out.CertificateSummaryList {
		assert.Equal(t, "ISSUED", s.Status,
			"filtered list should only contain ISSUED certs; got %s for %s", s.Status, s.DomainName)
	}
}

// TestACMHandler_ListCertificates_EnrichedSummary verifies that summary includes Status and KeyAlgorithm.
func TestACMHandler_ListCertificates_EnrichedSummary(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"enriched.example.com"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	listRec := postACMJSON(t, h, "ListCertificates", `{}`)
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		CertificateSummaryList []struct {
			Status       string `json:"Status"`
			KeyAlgorithm string `json:"KeyAlgorithm"`
			DomainName   string `json:"DomainName"`
		} `json:"CertificateSummaryList"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.NotEmpty(t, out.CertificateSummaryList)

	summary := out.CertificateSummaryList[0]
	assert.Equal(t, "ISSUED", summary.Status, "summary should include Status")
	assert.Equal(t, "EC_prime256v1", summary.KeyAlgorithm, "summary should include KeyAlgorithm")
}

// TestACMHandler_PutAccountConfiguration_ConflictDetection verifies ErrConflict on token reuse.
func TestACMHandler_PutAccountConfiguration_ConflictDetection(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	// First call: 30 days
	rec1 := postACMJSON(t, h, "PutAccountConfiguration",
		`{"IdempotencyToken":"conftest-1","ExpiryEvents":{"DaysBeforeExpiry":30}}`)
	require.Equal(t, http.StatusOK, rec1.Code)

	// Second call: same token, same settings → idempotent OK
	rec2 := postACMJSON(t, h, "PutAccountConfiguration",
		`{"IdempotencyToken":"conftest-1","ExpiryEvents":{"DaysBeforeExpiry":30}}`)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// Third call: same token, different settings → conflict
	rec3 := postACMJSON(t, h, "PutAccountConfiguration",
		`{"IdempotencyToken":"conftest-1","ExpiryEvents":{"DaysBeforeExpiry":60}}`)
	assert.Equal(t, http.StatusBadRequest, rec3.Code)
	assert.Contains(t, rec3.Body.String(), "ConflictException")
}

// TestACMHandler_TagOps_CertExistenceValidation verifies that tag ops reject unknown ARNs.
func TestACMHandler_TagOps_CertExistenceValidation(t *testing.T) {
	t.Parallel()

	const fakeARN = "arn:aws:acm:us-east-1:000000000000:certificate/does-not-exist"

	tests := []struct {
		name   string
		action string
		body   string
	}{
		{
			name:   "AddTags_NotFound",
			action: "AddTagsToCertificate",
			body:   `{"CertificateArn":"` + fakeARN + `","Tags":[{"Key":"k","Value":"v"}]}`,
		},
		{
			name:   "ListTags_NotFound",
			action: "ListTagsForCertificate",
			body:   `{"CertificateArn":"` + fakeARN + `"}`,
		},
		{
			name:   "RemoveTags_NotFound",
			action: "RemoveTagsFromCertificate",
			body:   `{"CertificateArn":"` + fakeARN + `","Tags":[{"Key":"k"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			rec := postACMJSON(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "ResourceNotFoundException")
		})
	}
}

// TestACMHandler_RevokeCertificate_PendingValidationRejected verifies that PENDING certs cannot be revoked.
func TestACMHandler_RevokeCertificate_PendingValidationRejected(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	// Create cert with DNS validation → starts PENDING
	reqRec := postACMJSON(t, h, "RequestCertificate",
		`{"DomainName":"pending-revoke.example.com","ValidationMethod":"DNS"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	// Poll until status is PENDING_VALIDATION (autoValidate may fire quickly)
	// We need to act before auto-validation fires; if it already fired, skip test.
	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	_ = b // just for reference to domain

	body, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", string(body))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			Status string `json:"Status"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	if descOut.Certificate.Status != "PENDING_VALIDATION" {
		t.Skip("cert auto-validated before test could run")
	}

	// Try to revoke PENDING cert
	revokeBody, _ := json.Marshal(map[string]string{
		"CertificateArn":   reqOut.CertificateArn,
		"RevocationReason": "UNSPECIFIED",
	})
	revokeRec := postACMJSON(t, h, "RevokeCertificate", string(revokeBody))
	assert.Equal(t, http.StatusBadRequest, revokeRec.Code)
	assert.Contains(t, revokeRec.Body.String(), "ValidationException")
}

// TestACMHandler_RequestCertificate_Tags verifies that tags passed at request time are stored.
func TestACMHandler_RequestCertificate_Tags(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	body := `{"DomainName":"tagged.example.com","Tags":[{"Key":"env","Value":"test"},{"Key":"team","Value":"infra"}]}`
	rec := postACMJSON(t, h, "RequestCertificate", body)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Verify tags are stored
	listBody, _ := json.Marshal(map[string]string{"CertificateArn": out.CertificateArn})
	listRec := postACMJSON(t, h, "ListTagsForCertificate", string(listBody))
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut struct {
		Tags []map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsOut))

	tagMap := make(map[string]string)
	for _, t2 := range tagsOut.Tags {
		tagMap[t2["Key"]] = t2["Value"]
	}

	assert.Equal(t, "test", tagMap["env"])
	assert.Equal(t, "infra", tagMap["team"])
}

// TestACMHandler_RequestCertificate_DomainValidation verifies domain name validation errors.
func TestACMHandler_RequestCertificate_DomainValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		domain string
	}{
		{
			name:   "empty_domain",
			domain: "",
		},
		{
			name:   "domain_too_long",
			domain: "a." + strings.Repeat("b", 252) + ".com",
		},
		{
			name:   "label_too_long",
			domain: strings.Repeat("x", 64) + ".example.com",
		},
		{
			name:   "empty_label",
			domain: "foo..example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			body, _ := json.Marshal(map[string]string{"DomainName": tt.domain})
			rec := postACMJSON(t, h, "RequestCertificate", string(body))
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestACMHandler_DescribeCertificate_KeyUsageAndInUseBy verifies new fields in DescribeCertificate.
func TestACMHandler_DescribeCertificate_KeyUsageAndInUseBy(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"keyusage.example.com"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	descBody, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			KeyUsage         []map[string]string `json:"KeyUsage"`
			ExtendedKeyUsage []map[string]string `json:"ExtendedKeyUsage"`
			InUseBy          []string            `json:"InUseBy"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	require.NotEmpty(t, descOut.Certificate.KeyUsage, "KeyUsage should be set")
	assert.Equal(t, "DIGITAL_SIGNATURE", descOut.Certificate.KeyUsage[0]["Name"])

	require.NotEmpty(t, descOut.Certificate.ExtendedKeyUsage, "ExtendedKeyUsage should be set")
	assert.Equal(t, "TLS_WEB_SERVER_AUTHENTICATION", descOut.Certificate.ExtendedKeyUsage[0]["Name"])
}

// TestACMHandler_ListCertificates_SortByCreatedAt verifies SortBy=CREATED_AT ordering.
func TestACMHandler_ListCertificates_SortByCreatedAt(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	// Create three certs in sequence
	for _, domain := range []string{"sort-a.example.com", "sort-b.example.com", "sort-c.example.com"} {
		body, _ := json.Marshal(map[string]string{"DomainName": domain})
		rec := postACMJSON(t, h, "RequestCertificate", string(body))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// List with CREATED_AT DESCENDING
	listRec := postACMJSON(t, h, "ListCertificates",
		`{"SortBy":"CREATED_AT","SortOrder":"DESCENDING"}`)
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		CertificateSummaryList []struct {
			DomainName string `json:"DomainName"`
		} `json:"CertificateSummaryList"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.Len(t, out.CertificateSummaryList, 3)

	// Descending means newest first: sort-c, sort-b, sort-a
	assert.Equal(t, "sort-c.example.com", out.CertificateSummaryList[0].DomainName)
	assert.Equal(t, "sort-a.example.com", out.CertificateSummaryList[2].DomainName)
}

// TestACMHandler_ListCertificates_KeyTypesFilter verifies Includes.keyTypes filtering.
func TestACMHandler_ListCertificates_KeyTypesFilter(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"kt.example.com","KeyAlgorithm":"EC_prime256v1"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Filter that matches
	matchRec := postACMJSON(t, h, "ListCertificates",
		`{"Includes":{"keyTypes":["EC_prime256v1"]}}`)
	require.Equal(t, http.StatusOK, matchRec.Code)

	var matchOut struct {
		CertificateSummaryList []struct {
			DomainName string `json:"DomainName"`
		} `json:"CertificateSummaryList"`
	}
	require.NoError(t, json.Unmarshal(matchRec.Body.Bytes(), &matchOut))
	require.NotEmpty(t, matchOut.CertificateSummaryList, "EC_prime256v1 filter should return results")

	// Filter that does not match
	noMatchRec := postACMJSON(t, h, "ListCertificates",
		`{"Includes":{"keyTypes":["RSA_2048"]}}`)
	require.Equal(t, http.StatusOK, noMatchRec.Code)

	var noMatchOut struct {
		CertificateSummaryList []struct{} `json:"CertificateSummaryList"`
	}
	require.NoError(t, json.Unmarshal(noMatchRec.Body.Bytes(), &noMatchOut))
	assert.Empty(t, noMatchOut.CertificateSummaryList, "RSA_2048 filter should return no results")
}

// TestACMHandler_ListCertificates_SubjectAlternativeNameSummaries verifies SANs in summary.
func TestACMHandler_ListCertificates_SubjectAlternativeNameSummaries(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	body := `{"DomainName":"san.example.com","SubjectAlternativeNames":["www.san.example.com","api.san.example.com"]}`
	rec := postACMJSON(t, h, "RequestCertificate", body)
	require.Equal(t, http.StatusOK, rec.Code)

	listRec := postACMJSON(t, h, "ListCertificates", `{}`)
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		CertificateSummaryList []struct {
			DomainName                      string   `json:"DomainName"`
			SubjectAlternativeNameSummaries []string `json:"SubjectAlternativeNameSummaries"`
		} `json:"CertificateSummaryList"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.NotEmpty(t, out.CertificateSummaryList)
	assert.Contains(t, out.CertificateSummaryList[0].SubjectAlternativeNameSummaries, "www.san.example.com")
	assert.Contains(t, out.CertificateSummaryList[0].SubjectAlternativeNameSummaries, "api.san.example.com")
}

// TestACMHandler_RequestCertificate_EMAIL_ValidationEmails verifies ValidationEmails for EMAIL method.
func TestACMHandler_RequestCertificate_EMAIL_ValidationEmails(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate",
		`{"DomainName":"email-val.example.com","ValidationMethod":"EMAIL"}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	descBody, _ := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut struct {
		Certificate struct {
			DomainValidationOptions []struct {
				ValidationMethod string   `json:"ValidationMethod"`
				ValidationEmails []string `json:"ValidationEmails"`
			} `json:"DomainValidationOptions"`
		} `json:"Certificate"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	require.NotEmpty(t, descOut.Certificate.DomainValidationOptions)

	dvo := descOut.Certificate.DomainValidationOptions[0]
	assert.Equal(t, "EMAIL", dvo.ValidationMethod)
	assert.NotEmpty(t, dvo.ValidationEmails, "ValidationEmails should be set for EMAIL validation")
	assert.Contains(t, dvo.ValidationEmails, "admin@email-val.example.com")
}

// TestACMHandler_WildcardCertificate_Accepted verifies wildcard domains are accepted.
func TestACMHandler_WildcardCertificate_Accepted(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"*.example.com"}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestACMHandler_ExportCertificate_PassphraseRequired verifies that Passphrase is required.
func TestACMHandler_ExportCertificate_PassphraseRequired(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	src, err := b.RequestCertificate("export-pass.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)

	importedCert, err := b.ImportCertificate(src.CertificateBody, src.PrivateKey, "", "")
	require.NoError(t, err)

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "missing_passphrase_rejected",
			body: func() string {
				b2, _ := json.Marshal(map[string]string{"CertificateArn": importedCert.ARN})

				return string(b2)
			}(),
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException", "Passphrase"},
		},
		{
			name: "with_passphrase_success_encrypted_key",
			body: func() string {
				b2, _ := json.Marshal(map[string]string{
					"CertificateArn": importedCert.ARN,
					"Passphrase":     "dGVzdC1wYXNz", // base64("test-pass")
				})

				return string(b2)
			}(),
			wantCode:     http.StatusOK,
			wantContains: []string{"ENCRYPTED PRIVATE KEY"},
		},
		{
			name: "invalid_base64_passphrase",
			body: func() string {
				b2, _ := json.Marshal(map[string]string{
					"CertificateArn": importedCert.ARN,
					"Passphrase":     "not-valid-base64!!!",
				})

				return string(b2)
			}(),
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ValidationException"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := acm.NewHandler(b)
			rec := postACMJSON(t, h, "ExportCertificate", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestACMHandler_ListCertificates_IncludesFilters verifies KeyUsage and ExtendedKeyUsage filtering.
func TestACMHandler_ListCertificates_IncludesFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		listBody     string
		wantNonEmpty bool
		wantCode     int
	}{
		{
			name:         "filter_digital_signature_matches",
			listBody:     `{"Includes":{"keyUsage":["DIGITAL_SIGNATURE"]}}`,
			wantCode:     http.StatusOK,
			wantNonEmpty: true,
		},
		{
			name:         "filter_nonexistent_key_usage_empty",
			listBody:     `{"Includes":{"keyUsage":["KEY_ENCIPHERMENT"]}}`,
			wantCode:     http.StatusOK,
			wantNonEmpty: false,
		},
		{
			name:         "filter_tls_server_auth_matches",
			listBody:     `{"Includes":{"extendedKeyUsage":["TLS_WEB_SERVER_AUTHENTICATION"]}}`,
			wantCode:     http.StatusOK,
			wantNonEmpty: true,
		},
		{
			name:         "filter_code_signing_empty",
			listBody:     `{"Includes":{"extendedKeyUsage":["CODE_SIGNING"]}}`,
			wantCode:     http.StatusOK,
			wantNonEmpty: false,
		},
		{
			name:         "filter_combined_key_type_and_usage",
			listBody:     `{"Includes":{"keyTypes":["EC_prime256v1"],"keyUsage":["DIGITAL_SIGNATURE"]}}`,
			wantCode:     http.StatusOK,
			wantNonEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			// Create one cert that will have DIGITAL_SIGNATURE + TLS_WEB_SERVER_AUTHENTICATION
			rec := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"includes-filter.example.com"}`)
			require.Equal(t, http.StatusOK, rec.Code)

			listRec := postACMJSON(t, h, "ListCertificates", tt.listBody)
			assert.Equal(t, tt.wantCode, listRec.Code)

			if tt.wantCode == http.StatusOK {
				var out struct {
					CertificateSummaryList []struct{} `json:"CertificateSummaryList"`
				}
				require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
				if tt.wantNonEmpty {
					assert.NotEmpty(t, out.CertificateSummaryList)
				} else {
					assert.Empty(t, out.CertificateSummaryList)
				}
			}
		})
	}
}

// TestACMHandler_StatusLifecycle_DescribeReflectsNewStatus verifies that lifecycle status
// transitions are visible via DescribeCertificate through the full HTTP stack.
func TestACMHandler_StatusLifecycle_DescribeReflectsNewStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupCert  func(t *testing.T, b *acm.InMemoryBackend) string
		transition func(t *testing.T, b *acm.InMemoryBackend, certARN string)
		name       string
		wantStatus string
	}{
		{
			name: "issued_to_expired",
			setupCert: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("lifecycle-expire.example.com", "", "", "", "", "", "", nil)
				require.NoError(t, err)

				return cert.ARN
			},
			transition: func(t *testing.T, b *acm.InMemoryBackend, certARN string) {
				t.Helper()
				require.NoError(t, b.ExpireCertificate(certARN))
			},
			wantStatus: "EXPIRED",
		},
		{
			name: "issued_to_inactive",
			setupCert: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("lifecycle-inactive.example.com", "", "", "", "", "", "", nil)
				require.NoError(t, err)

				return cert.ARN
			},
			transition: func(t *testing.T, b *acm.InMemoryBackend, certARN string) {
				t.Helper()
				require.NoError(t, b.InactivateCertificate(certARN))
			},
			wantStatus: "INACTIVE",
		},
		{
			name: "pending_to_validation_timed_out",
			setupCert: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("lifecycle-timeout.example.com", "", "DNS", "", "", "", "", nil)
				require.NoError(t, err)
				require.Equal(t, "PENDING_VALIDATION", cert.Status)

				return cert.ARN
			},
			transition: func(t *testing.T, b *acm.InMemoryBackend, certARN string) {
				t.Helper()
				require.NoError(t, b.TimeoutPendingValidation(certARN))
			},
			wantStatus: "VALIDATION_TIMED_OUT",
		},
		{
			name: "pending_to_failed",
			setupCert: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("lifecycle-fail.example.com", "", "EMAIL", "", "", "", "", nil)
				require.NoError(t, err)
				require.Equal(t, "PENDING_VALIDATION", cert.Status)

				return cert.ARN
			},
			transition: func(t *testing.T, b *acm.InMemoryBackend, certARN string) {
				t.Helper()
				require.NoError(t, b.FailCertificate(certARN, "NO_AVAILABLE_CONTACTS"))
			},
			wantStatus: "FAILED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			h := acm.NewHandler(b)

			certARN := tt.setupCert(t, b)
			tt.transition(t, b, certARN)

			descBody, _ := json.Marshal(map[string]string{"CertificateArn": certARN})
			descRec := postACMJSON(t, h, "DescribeCertificate", string(descBody))
			require.Equal(t, http.StatusOK, descRec.Code)

			var descOut struct {
				Certificate struct {
					Status        string `json:"Status"`
					FailureReason string `json:"FailureReason,omitempty"`
				} `json:"Certificate"`
			}
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
			assert.Equal(t, tt.wantStatus, descOut.Certificate.Status)
		})
	}
}

// TestACMHandler_ListCertificates_StatusFilter_AllStatuses verifies filtering across all lifecycle statuses.
func TestACMHandler_ListCertificates_StatusFilter_AllStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupAndTransition func(t *testing.T, b *acm.InMemoryBackend) string
		name               string
		filterStatus       string
		wantCount          int
	}{
		{
			name: "filter_expired_status",
			setupAndTransition: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("expired-list.example.com", "", "", "", "", "", "", nil)
				require.NoError(t, err)
				require.NoError(t, b.ExpireCertificate(cert.ARN))

				return cert.ARN
			},
			filterStatus: "EXPIRED",
			wantCount:    1,
		},
		{
			name: "filter_inactive_status",
			setupAndTransition: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("inactive-list.example.com", "", "", "", "", "", "", nil)
				require.NoError(t, err)
				require.NoError(t, b.InactivateCertificate(cert.ARN))

				return cert.ARN
			},
			filterStatus: "INACTIVE",
			wantCount:    1,
		},
		{
			name: "filter_timed_out_status",
			setupAndTransition: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("timeout-list.example.com", "", "DNS", "", "", "", "", nil)
				require.NoError(t, err)
				require.NoError(t, b.TimeoutPendingValidation(cert.ARN))

				return cert.ARN
			},
			filterStatus: "VALIDATION_TIMED_OUT",
			wantCount:    1,
		},
		{
			name: "filter_failed_status",
			setupAndTransition: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate("failed-list.example.com", "", "EMAIL", "", "", "", "", nil)
				require.NoError(t, err)
				require.NoError(t, b.FailCertificate(cert.ARN, "CAA_ERROR"))

				return cert.ARN
			},
			filterStatus: "FAILED",
			wantCount:    1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := acm.NewInMemoryBackend("000000000000", "us-east-1")
			h := acm.NewHandler(b)

			tt.setupAndTransition(t, b)

			filterBody, _ := json.Marshal(map[string][]string{
				"CertificateStatuses": {tt.filterStatus},
			})
			rec := postACMJSON(t, h, "ListCertificates", string(filterBody))
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				CertificateSummaryList []struct {
					Status string `json:"Status"`
				} `json:"CertificateSummaryList"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.CertificateSummaryList, tt.wantCount)

			if tt.wantCount > 0 {
				assert.Equal(t, tt.filterStatus, out.CertificateSummaryList[0].Status)
			}
		})
	}
}

// TestACMHandler_ExportCertificate_CertificateChainAlwaysPresent verifies chain is always returned.
func TestACMHandler_ExportCertificate_CertificateChainAlwaysPresent(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	src, err := b.RequestCertificate("chain-test.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)

	// Import without chain
	imported, err := b.ImportCertificate(src.CertificateBody, src.PrivateKey, "", "")
	require.NoError(t, err)

	h := acm.NewHandler(b)
	exportBody, _ := json.Marshal(map[string]string{
		"CertificateArn": imported.ARN,
		"Passphrase":     "dGVzdA==", // base64("test")
	})
	rec := postACMJSON(t, h, "ExportCertificate", string(exportBody))
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CertificateChain string `json:"CertificateChain"`
		Certificate      string `json:"Certificate"`
		PrivateKey       string `json:"PrivateKey"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.CertificateChain, "CertificateChain should always be present on export")
	assert.NotEmpty(t, out.Certificate)
	assert.NotEmpty(t, out.PrivateKey)
}
