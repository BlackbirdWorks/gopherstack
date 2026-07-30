package acm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/acm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
				cert, err := b.RequestCertificate(
					context.Background(),
					"expired-list.example.com",
					"",
					"",
					"",
					"",
					"",
					"",
					nil,
				)
				require.NoError(t, err)
				require.NoError(t, b.ExpireCertificate(context.Background(), cert.ARN))

				return cert.ARN
			},
			filterStatus: "EXPIRED",
			wantCount:    1,
		},
		{
			name: "filter_inactive_status",
			setupAndTransition: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate(
					context.Background(),
					"inactive-list.example.com",
					"",
					"",
					"",
					"",
					"",
					"",
					nil,
				)
				require.NoError(t, err)
				require.NoError(t, b.InactivateCertificate(context.Background(), cert.ARN))

				return cert.ARN
			},
			filterStatus: "INACTIVE",
			wantCount:    1,
		},
		{
			name: "filter_timed_out_status",
			setupAndTransition: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate(
					context.Background(),
					"timeout-list.example.com",
					"",
					"DNS",
					"",
					"",
					"",
					"",
					nil,
				)
				require.NoError(t, err)
				require.NoError(t, b.TimeoutPendingValidation(context.Background(), cert.ARN))

				return cert.ARN
			},
			filterStatus: "VALIDATION_TIMED_OUT",
			wantCount:    1,
		},
		{
			name: "filter_failed_status",
			setupAndTransition: func(t *testing.T, b *acm.InMemoryBackend) string {
				t.Helper()
				cert, err := b.RequestCertificate(
					context.Background(),
					"failed-list.example.com",
					"",
					"EMAIL",
					"",
					"",
					"",
					"",
					nil,
				)
				require.NoError(t, err)
				require.NoError(t, b.FailCertificate(context.Background(), cert.ARN, "CAA_ERROR"))

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

// TestACMHandler_ListCertificates_SubjectAlternativeNameSummaries_IncludesPrimary verifies that
// ListCertificates SubjectAlternativeNameSummaries includes the primary domain.
func TestACMHandler_ListCertificates_SubjectAlternativeNameSummaries_IncludesPrimary(t *testing.T) {
	t.Parallel()

	h := newACMHandler()
	reqBody := mustMarshal(t, map[string]any{
		"DomainName":              "primary.example.com",
		"SubjectAlternativeNames": []string{"www.primary.example.com"},
	})
	reqRec := postACMJSON(t, h, "RequestCertificate", reqBody)
	require.Equal(t, http.StatusOK, reqRec.Code)

	listRec := postACMJSON(t, h, "ListCertificates", "{}")
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		CertificateSummaryList []struct {
			DomainName                      string   `json:"DomainName"`
			SubjectAlternativeNameSummaries []string `json:"SubjectAlternativeNameSummaries"`
		} `json:"CertificateSummaryList"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.NotEmpty(t, listOut.CertificateSummaryList)

	summaries := listOut.CertificateSummaryList[0].SubjectAlternativeNameSummaries
	assert.Contains(t, summaries, "primary.example.com",
		"SubjectAlternativeNameSummaries must include the primary domain")
	assert.Contains(t, summaries, "www.primary.example.com",
		"SubjectAlternativeNameSummaries must include extra SANs")
}

// TestACMHandler_SearchCertificates covers SearchCertificates, field-diffed
// against aws-sdk-go-v2/service/acm's SearchCertificatesInput/Output and the
// CertificateFilterStatement/CertificateFilter/AcmCertificateMetadataFilter/
// X509AttributeFilter union wire shapes (each union member serializes as a
// single-key wrapper object, e.g. {"Filter":{"AcmCertificateMetadataFilter":
// {"Status":"ISSUED"}}} -- verified against serializers.go).
func TestACMHandler_SearchCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *acm.Handler)
		name string
	}{
		{
			name: "NoFilter_ReturnsAll",
			run: func(t *testing.T, h *acm.Handler) {
				t.Helper()

				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"search-all-1.example.com"}`)
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"search-all-2.example.com"}`)

				rec := postACMJSON(t, h, "SearchCertificates", `{}`)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "search-all-1.example.com")
				assert.Contains(t, rec.Body.String(), "search-all-2.example.com")
			},
		},
		{
			name: "AcmCertificateMetadataFilter_Status",
			run: func(t *testing.T, h *acm.Handler) {
				t.Helper()

				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"search-issued.example.com"}`)
				importCertRec := postACMJSON(t, h, "RequestCertificate",
					`{"DomainName":"search-pending.example.com","ValidationMethod":"DNS"}`)
				require.Equal(t, http.StatusOK, importCertRec.Code)

				body := `{"FilterStatement":{"Filter":{"AcmCertificateMetadataFilter":{"Status":"ISSUED"}}}}`
				rec := postACMJSON(t, h, "SearchCertificates", body)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "search-issued.example.com")
				assert.NotContains(t, rec.Body.String(), "search-pending.example.com")
			},
		},
		{
			name: "AcmCertificateMetadataFilter_ManagedBy",
			run: func(t *testing.T, h *acm.Handler) {
				t.Helper()

				managedRec := postACMJSON(t, h, "RequestCertificate",
					`{"DomainName":"search-managed.example.com","ManagedBy":"CLOUDFRONT"}`)
				require.Equal(t, http.StatusOK, managedRec.Code)
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"search-unmanaged.example.com"}`)

				body := `{"FilterStatement":{"Filter":{"AcmCertificateMetadataFilter":{"ManagedBy":"CLOUDFRONT"}}}}`
				rec := postACMJSON(t, h, "SearchCertificates", body)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "search-managed.example.com")
				assert.NotContains(t, rec.Body.String(), "search-unmanaged.example.com")
			},
		},
		{
			name: "CertificateArn_Filter",
			run: func(t *testing.T, h *acm.Handler) {
				t.Helper()

				rec1 := postACMJSON(t, h, "RequestCertificate", `{"DomainName":"search-arn-1.example.com"}`)
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"search-arn-2.example.com"}`)

				var out struct {
					CertificateArn string `json:"CertificateArn"`
				}
				require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out))

				body, _ := json.Marshal(map[string]any{
					"FilterStatement": map[string]any{
						"Filter": map[string]any{"CertificateArn": out.CertificateArn},
					},
				})
				rec := postACMJSON(t, h, "SearchCertificates", string(body))
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "search-arn-1.example.com")
				assert.NotContains(t, rec.Body.String(), "search-arn-2.example.com")
			},
		},
		{
			name: "AndOrNot_Composition",
			run: func(t *testing.T, h *acm.Handler) {
				t.Helper()

				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"search-composed-keep.example.com"}`)
				postACMJSON(t, h, "RequestCertificate",
					`{"DomainName":"search-composed-drop.example.com","ValidationMethod":"DNS"}`)

				// AND(Status=ISSUED, NOT(ValidationMethod=DNS)) -- both certs
				// are default (non-DNS) validation-method-eligible for the
				// first, but only the ISSUED one should match here since the
				// DNS one is PENDING_VALIDATION, not ISSUED.
				body := `{"FilterStatement":{"And":[
					{"Filter":{"AcmCertificateMetadataFilter":{"Status":"ISSUED"}}},
					{"Not":{"Filter":{"AcmCertificateMetadataFilter":{"ValidationMethod":"DNS"}}}}
				]}}`
				rec := postACMJSON(t, h, "SearchCertificates", body)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "search-composed-keep.example.com")
				assert.NotContains(t, rec.Body.String(), "search-composed-drop.example.com")
			},
		},
		{
			name: "X509AttributeFilter_SubjectAlternativeNameDnsName",
			run: func(t *testing.T, h *acm.Handler) {
				t.Helper()

				postACMJSON(t, h, "RequestCertificate",
					`{"DomainName":"sandns.example.com","SubjectAlternativeNames":["alt.sandns.example.com"]}`)
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"other.example.com"}`)

				body := `{"FilterStatement":{"Filter":{"X509AttributeFilter":{"SubjectAlternativeName":` +
					`{"DnsName":{"ComparisonOperator":"EQUALS","Value":"alt.sandns.example.com"}}}}}}`
				rec := postACMJSON(t, h, "SearchCertificates", body)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "sandns.example.com")
				assert.NotContains(t, rec.Body.String(), "other.example.com")
			},
		},
		{
			// X509AttributeFilter.Subject: verified against
			// aws-sdk-go-v2/service/acm@v1.43.0/types/types.go that the real
			// SubjectFilter union currently defines only CommonName -- so this
			// case covers the entirety of what the real API supports filtering
			// on, not a partial implementation of a wider "structured DN
			// filter" that AWS itself hasn't shipped yet. Also locks in the
			// X509Attributes.Subject.CommonName wire-shape fix: it must be
			// just the CN ("commonname.example.com"), not the whole rendered
			// DN string ("CN=commonname.example.com,OU=...,O=Amazon,C=US").
			name: "X509AttributeFilter_SubjectCommonName",
			run: func(t *testing.T, h *acm.Handler) {
				t.Helper()

				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"commonname.example.com"}`)
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"other-cn.example.com"}`)

				body := `{"FilterStatement":{"Filter":{"X509AttributeFilter":{"Subject":` +
					`{"CommonName":{"ComparisonOperator":"EQUALS","Value":"commonname.example.com"}}}}}}`
				rec := postACMJSON(t, h, "SearchCertificates", body)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "commonname.example.com")
				assert.NotContains(t, rec.Body.String(), "other-cn.example.com")

				var out struct {
					Results []struct {
						X509Attributes struct {
							Subject struct {
								CommonName string `json:"CommonName"`
							} `json:"Subject"`
						} `json:"X509Attributes"`
					} `json:"Results"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.Len(t, out.Results, 1)
				assert.Equal(t, "commonname.example.com", out.Results[0].X509Attributes.Subject.CommonName)
			},
		},
		{
			name: "SortBy_CreatedAt_Descending",
			run: func(t *testing.T, h *acm.Handler) {
				t.Helper()

				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"sort-first.example.com"}`)
				time.Sleep(2 * time.Millisecond)
				postACMJSON(t, h, "RequestCertificate", `{"DomainName":"sort-second.example.com"}`)

				body := `{"SortBy":"CREATED_AT","SortOrder":"DESCENDING"}`
				rec := postACMJSON(t, h, "SearchCertificates", body)
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					Results []struct {
						CertificateMetadata struct {
							AcmCertificateMetadata struct {
								CreatedAt int64 `json:"CreatedAt"`
							} `json:"AcmCertificateMetadata"`
						} `json:"CertificateMetadata"`
					} `json:"Results"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.Len(t, out.Results, 2)
				assert.GreaterOrEqual(t,
					out.Results[0].CertificateMetadata.AcmCertificateMetadata.CreatedAt,
					out.Results[1].CertificateMetadata.AcmCertificateMetadata.CreatedAt,
					"DESCENDING sort must put the newer cert first",
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newACMHandler()
			tt.run(t, h)
		})
	}
}

// TestACMBackend_SearchCertificates covers SearchCertificates at the backend
// level, including NextToken validation.
func TestACMBackend_SearchCertificates(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	ctx := context.Background()

	_, err := b.RequestCertificate(ctx, "backend-search.example.com", "", "", "", "", "", "", nil)
	require.NoError(t, err)

	pg, err := b.SearchCertificates(ctx, acm.SearchCertificatesParams{})
	require.NoError(t, err)
	assert.Len(t, pg.Data, 1)

	_, err = b.SearchCertificates(ctx, acm.SearchCertificatesParams{NextToken: "not-valid-base64!!"})
	assert.ErrorIs(t, err, acm.ErrInvalidParameter)
}
