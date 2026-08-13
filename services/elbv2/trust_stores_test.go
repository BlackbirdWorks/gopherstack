package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestELBv2_TrustStoreLifecycle validates trust store create and delete operation error paths.
func TestELBv2_TrustStoreLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		checkResp  func(t *testing.T, rec *httptest.ResponseRecorder)
		name       string
		wantStatus int
	}{
		{
			name: "create_trust_store",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"my-trust-store"},
				}
			},
			wantStatus: http.StatusOK,
			checkResp: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var resp struct {
					Result struct {
						TrustStores struct {
							Members []struct {
								TrustStoreArn string `xml:"TrustStoreArn"`
								Name          string `xml:"Name"`
								Status        string `xml:"Status"`
							} `xml:"member"`
						} `xml:"TrustStores"`
					} `xml:"CreateTrustStoreResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				require.Len(t, resp.Result.TrustStores.Members, 1)
				assert.NotEmpty(t, resp.Result.TrustStores.Members[0].TrustStoreArn)
				assert.Equal(t, "my-trust-store", resp.Result.TrustStores.Members[0].Name)
				assert.Equal(t, "ACTIVE", resp.Result.TrustStores.Members[0].Status)
			},
		},
		{
			name: "create_trust_store_missing_name",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "delete_trust_store_not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"DeleteTrustStore"},
					"Version": {"2015-12-01"},
					"TrustStoreArn": {
						"arn:aws:elasticloadbalancing:us-east-1:123456789012:truststore/nonexistent/abc123",
					},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "delete_trust_store_missing_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"DeleteTrustStore"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := tt.setup(t, h)

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkResp != nil {
				tt.checkResp(t, rec)
			}
		})
	}
}

// TestELBv2_TrustStoreFullLifecycle tests the complete lifecycle in sequence.
func TestELBv2_TrustStoreFullLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create a trust store.
	createRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"my-ts"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))
	require.Len(t, createResp.Result.TrustStores.Members, 1)

	tsArn := createResp.Result.TrustStores.Members[0].TrustStoreArn
	assert.NotEmpty(t, tsArn)

	// Add revocations. Real AWS's RevocationContent request shape is always
	// S3Bucket/S3Key/(S3ObjectVersion)/RevocationType (verified against
	// aws-sdk-go-v2 types.RevocationContent) -- there is no plain/inline form.
	revRec := doELBv2(t, h, url.Values{
		"Action":                               {"AddTrustStoreRevocations"},
		"Version":                              {"2015-12-01"},
		"TrustStoreArn":                        {tsArn},
		"RevocationContents.member.1.S3Bucket": {"my-bucket"},
		"RevocationContents.member.1.S3Key":    {"revocations.crl"},
		"RevocationContents.member.1.RevocationType": {"CRL"},
	})
	assert.Equal(t, http.StatusOK, revRec.Code)

	// AddTrustStoreRevocations must echo back the added revocation info in its
	// AddTrustStoreRevocationsResult.TrustStoreRevocations list (AWS behaviour) —
	// this used to be silently dropped (an empty response body). RevocationId is
	// int64 and server-assigned (real AWS never accepts a client-supplied one).
	var addResp struct {
		Result struct {
			TrustStoreRevocations struct {
				Members []struct {
					RevocationType string `xml:"RevocationType"`
					TrustStoreArn  string `xml:"TrustStoreArn"`
					RevocationID   int64  `xml:"RevocationId"`
				} `xml:"member"`
			} `xml:"TrustStoreRevocations"`
		} `xml:"AddTrustStoreRevocationsResult"`
	}
	require.NoError(t, xml.Unmarshal(revRec.Body.Bytes(), &addResp))
	require.Len(t, addResp.Result.TrustStoreRevocations.Members, 1)
	assert.NotZero(t, addResp.Result.TrustStoreRevocations.Members[0].RevocationID)
	assert.Equal(t, "CRL", addResp.Result.TrustStoreRevocations.Members[0].RevocationType)
	assert.Equal(t, tsArn, addResp.Result.TrustStoreRevocations.Members[0].TrustStoreArn)

	addedRevocationID := addResp.Result.TrustStoreRevocations.Members[0].RevocationID

	// Describe trust store associations (expect empty).
	assocRec := doELBv2(t, h, url.Values{
		"Action":        {"DescribeTrustStoreAssociations"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	var assocResp struct {
		Result struct {
			TrustStoreAssociations struct {
				Members []struct {
					ResourceArn string `xml:"ResourceArn"`
				} `xml:"member"`
			} `xml:"TrustStoreAssociations"`
		} `xml:"DescribeTrustStoreAssociationsResult"`
	}
	require.NoError(t, xml.Unmarshal(assocRec.Body.Bytes(), &assocResp))
	assert.Empty(t, assocResp.Result.TrustStoreAssociations.Members)

	// DescribeTrustStoreRevocations — the one we added should be visible.
	revDescRec := doELBv2(t, h, url.Values{
		"Action":        {"DescribeTrustStoreRevocations"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	require.Equal(t, http.StatusOK, revDescRec.Code)

	var revDescResp struct {
		Result struct {
			TrustStoreRevocations struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
					RevocationID  int64  `xml:"RevocationId"`
				} `xml:"member"`
			} `xml:"TrustStoreRevocations"`
		} `xml:"DescribeTrustStoreRevocationsResult"`
	}
	require.NoError(t, xml.Unmarshal(revDescRec.Body.Bytes(), &revDescResp))
	require.Len(t, revDescResp.Result.TrustStoreRevocations.Members, 1)
	assert.Equal(t, addedRevocationID, revDescResp.Result.TrustStoreRevocations.Members[0].RevocationID)
	assert.Equal(t, tsArn, revDescResp.Result.TrustStoreRevocations.Members[0].TrustStoreArn)

	// RemoveTrustStoreRevocations — remove the entry we added.
	revRmRec := doELBv2(t, h, url.Values{
		"Action":                 {"RemoveTrustStoreRevocations"},
		"Version":                {"2015-12-01"},
		"TrustStoreArn":          {tsArn},
		"RevocationIds.member.1": {strconv.FormatInt(addedRevocationID, 10)},
	})
	require.Equal(t, http.StatusOK, revRmRec.Code)

	// Verify revocations are now empty.
	revDescRec2 := doELBv2(t, h, url.Values{
		"Action":        {"DescribeTrustStoreRevocations"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	require.Equal(t, http.StatusOK, revDescRec2.Code)

	var revDescResp2 struct {
		Result struct {
			TrustStoreRevocations struct {
				Members []struct {
					RevocationID string `xml:"RevocationId"`
				} `xml:"member"`
			} `xml:"TrustStoreRevocations"`
		} `xml:"DescribeTrustStoreRevocationsResult"`
	}
	require.NoError(t, xml.Unmarshal(revDescRec2.Body.Bytes(), &revDescResp2))
	assert.Empty(t, revDescResp2.Result.TrustStoreRevocations.Members)

	// DescribeTrustStores — should return our trust store.
	descTSRec := doELBv2(t, h, url.Values{
		"Action":                  {"DescribeTrustStores"},
		"Version":                 {"2015-12-01"},
		"TrustStoreArns.member.1": {tsArn},
	})
	require.Equal(t, http.StatusOK, descTSRec.Code)

	var descTSResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
					Name          string `xml:"Name"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"DescribeTrustStoresResult"`
	}
	require.NoError(t, xml.Unmarshal(descTSRec.Body.Bytes(), &descTSResp))
	require.Len(t, descTSResp.Result.TrustStores.Members, 1)
	assert.Equal(t, "my-ts", descTSResp.Result.TrustStores.Members[0].Name)

	// ModifyTrustStore — Name is not a real ModifyTrustStoreInput field (verified
	// against elasticloadbalancingv2@v1.58.5 api_op_ModifyTrustStore.go); a real
	// client never sends it. Sending it anyway must NOT rename the trust store.
	modTSRec := doELBv2(t, h, url.Values{
		"Action":        {"ModifyTrustStore"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
		"Name":          {"my-ts-renamed"},
	})
	require.Equal(t, http.StatusOK, modTSRec.Code)

	var modTSResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					Name string `xml:"Name"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"ModifyTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(modTSRec.Body.Bytes(), &modTSResp))
	require.Len(t, modTSResp.Result.TrustStores.Members, 1)
	assert.Equal(t, "my-ts", modTSResp.Result.TrustStores.Members[0].Name)

	// DeleteSharedTrustStoreAssociation with no existing association returns
	// AssociationNotFound (HTTP 400, AWS query-protocol status), matching AWS behavior.
	delAssocRec := doELBv2(t, h, url.Values{
		"Action":        {"DeleteSharedTrustStoreAssociation"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
		"ResourceArn":   {"arn:aws:elasticloadbalancing:us-east-1:000000000000:listener/app/x/y/z"},
	})
	assert.Equal(t, http.StatusBadRequest, delAssocRec.Code)

	// Delete trust store.
	delRec := doELBv2(t, h, url.Values{
		"Action":        {"DeleteTrustStore"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	// Deleting again must return NotFound.
	delRec2 := doELBv2(t, h, url.Values{
		"Action":        {"DeleteTrustStore"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusBadRequest, delRec2.Code)
}

// TestELBv2_DescribeTrustStores validates the DescribeTrustStores operation.
func TestELBv2_DescribeTrustStores(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		checkResp  func(t *testing.T, rec *httptest.ResponseRecorder)
		name       string
		wantStatus int
	}{
		{
			name: "describe_all",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				doELBv2(t, h, url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"ts-a"},
				})
				doELBv2(t, h, url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"ts-b"},
				})

				return url.Values{
					"Action":  {"DescribeTrustStores"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusOK,
			checkResp: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var resp struct {
					Result struct {
						TrustStores struct {
							Members []struct {
								Name string `xml:"Name"`
							} `xml:"member"`
						} `xml:"TrustStores"`
					} `xml:"DescribeTrustStoresResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp.Result.TrustStores.Members, 2)
			},
		},
		{
			name: "describe_by_name",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				doELBv2(t, h, url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"ts-named"},
				})
				doELBv2(t, h, url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"ts-other"},
				})

				return url.Values{
					"Action":         {"DescribeTrustStores"},
					"Version":        {"2015-12-01"},
					"Names.member.1": {"ts-named"},
				}
			},
			wantStatus: http.StatusOK,
			checkResp: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()
				var resp struct {
					Result struct {
						TrustStores struct {
							Members []struct {
								Name string `xml:"Name"`
							} `xml:"member"`
						} `xml:"TrustStores"`
					} `xml:"DescribeTrustStoresResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				require.Len(t, resp.Result.TrustStores.Members, 1)
				assert.Equal(t, "ts-named", resp.Result.TrustStores.Members[0].Name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := tt.setup(t, h)

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkResp != nil {
				tt.checkResp(t, rec)
			}
		})
	}
}

// TestELBv2_ModifyTrustStore validates ModifyTrustStore against the real
// ModifyTrustStoreInput shape: TrustStoreArn is the only field this handler
// reads. "Name" is not a real field (verified against
// elasticloadbalancingv2@v1.58.5 api_op_ModifyTrustStore.go:33-49) and must
// have no effect even if a caller sends it.
func TestELBv2_ModifyTrustStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		checkResp  func(t *testing.T, rec *httptest.ResponseRecorder)
		name       string
		wantStatus int
	}{
		{
			name: "name_param_has_no_effect",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()

				createRec := doELBv2(t, h, url.Values{
					"Action":  {"CreateTrustStore"},
					"Version": {"2015-12-01"},
					"Name":    {"orig-name"},
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var resp struct {
					Result struct {
						TrustStores struct {
							Members []struct {
								TrustStoreArn string `xml:"TrustStoreArn"`
							} `xml:"member"`
						} `xml:"TrustStores"`
					} `xml:"CreateTrustStoreResult"`
				}
				require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &resp))
				require.Len(t, resp.Result.TrustStores.Members, 1)

				return url.Values{
					"Action":        {"ModifyTrustStore"},
					"Version":       {"2015-12-01"},
					"TrustStoreArn": {resp.Result.TrustStores.Members[0].TrustStoreArn},
					"Name":          {"new-name"},
				}
			},
			wantStatus: http.StatusOK,
			checkResp: func(t *testing.T, rec *httptest.ResponseRecorder) {
				t.Helper()

				var resp struct {
					Result struct {
						TrustStores struct {
							Members []struct {
								Name string `xml:"Name"`
							} `xml:"member"`
						} `xml:"TrustStores"`
					} `xml:"ModifyTrustStoreResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				require.Len(t, resp.Result.TrustStores.Members, 1)
				assert.Equal(t, "orig-name", resp.Result.TrustStores.Members[0].Name)
			},
		},
		{
			name: "not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":        {"ModifyTrustStore"},
					"Version":       {"2015-12-01"},
					"TrustStoreArn": {"arn:aws:elasticloadbalancing:us-east-1:123:truststore/nonexistent/abc"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"ModifyTrustStore"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			vals := tt.setup(t, h)

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkResp != nil {
				tt.checkResp(t, rec)
			}
		})
	}
}

func TestTrustStore_FullLifecycle(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()

	// Create
	cRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-full-lifecycle"},
	})
	require.Equal(t, http.StatusOK, cRec.Code)
	var cResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
					Name          string `xml:"Name"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(cRec.Body.Bytes(), &cResp))
	tsArn := cResp.Result.TrustStores.Members[0].TrustStoreArn
	assert.NotEmpty(t, tsArn)

	// Describe
	dRec := doELBv2(t, h, url.Values{
		"Action":                  {"DescribeTrustStores"},
		"Version":                 {"2015-12-01"},
		"TrustStoreArns.member.1": {tsArn},
	})
	require.Equal(t, http.StatusOK, dRec.Code)
	assert.Contains(t, dRec.Body.String(), "ts-full-lifecycle")

	// Modify
	mRec := doELBv2(t, h, url.Values{
		"Action":        {"ModifyTrustStore"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusOK, mRec.Code)

	// Delete
	delRec := doELBv2(t, h, url.Values{
		"Action":        {"DeleteTrustStore"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusOK, delRec.Code)
}

func TestTrustStore_Revocations(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-revocations"},
	})
	require.Equal(t, http.StatusOK, tsRec.Code)
	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	// Add revocations
	addRec := doELBv2(t, h, url.Values{
		"Action":                               {"AddTrustStoreRevocations"},
		"Version":                              {"2015-12-01"},
		"TrustStoreArn":                        {tsArn},
		"RevocationContents.member.1.S3Bucket": {"my-bucket"},
		"RevocationContents.member.1.S3Key":    {"revocations.crl"},
		"RevocationContents.member.1.RevocationType": {"CRL"},
	})
	assert.Equal(t, http.StatusOK, addRec.Code)

	// Describe revocations
	descRec := doELBv2(t, h, url.Values{
		"Action":        {"DescribeTrustStoreRevocations"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusOK, descRec.Code)
}

func TestGetTrustStoreCaCertificatesBundle(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-ca-bundle"},
	})
	require.Equal(t, http.StatusOK, tsRec.Code)
	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))

	rec := doELBv2(t, h, url.Values{
		"Action":        {"GetTrustStoreCaCertificatesBundle"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsResp.Result.TrustStores.Members[0].TrustStoreArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestDescribeTrustStoreAssociations(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-associations"},
	})
	require.Equal(t, http.StatusOK, tsRec.Code)
	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	rec := doELBv2(t, h, url.Values{
		"Action":        {"DescribeTrustStoreAssociations"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestDeleteSharedTrustStoreAssociation(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-shared-assoc"},
	})
	require.Equal(t, http.StatusOK, tsRec.Code)
	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	lbArn := b1CreateLB(t, h, "shared-assoc-lb")
	tgArn := b1CreateTG(t, h, "shared-assoc-tg")

	// Create an HTTPS listener with mutual authentication referencing the trust store.
	listRec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:000000000000:certificate/ccc"},
		"MutualAuthentication.Mode":              {"verify"},
		"MutualAuthentication.TrustStoreArn":     {tsArn},
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(listRec.Body.Bytes(), &listResp))
	listenerArn := listResp.Result.Listeners.Members[0].ListenerArn

	// Missing ResourceArn is a validation error.
	missingRec := doELBv2(t, h, url.Values{
		"Action":        {"DeleteSharedTrustStoreAssociation"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
	})
	assert.Equal(t, http.StatusBadRequest, missingRec.Code)

	// Deleting the existing association succeeds.
	rec := doELBv2(t, h, url.Values{
		"Action":        {"DeleteSharedTrustStoreAssociation"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
		"ResourceArn":   {listenerArn},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Deleting again returns AssociationNotFound (HTTP 400, AWS query-protocol status).
	rec2 := doELBv2(t, h, url.Values{
		"Action":        {"DeleteSharedTrustStoreAssociation"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
		"ResourceArn":   {listenerArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestRemoveTrustStoreRevocations(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-rm-rev"},
	})
	require.Equal(t, http.StatusOK, tsRec.Code)
	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	rec := doELBv2(t, h, url.Values{
		"Action":                 {"RemoveTrustStoreRevocations"},
		"Version":                {"2015-12-01"},
		"TrustStoreArn":          {tsArn},
		"RevocationIds.member.1": {"1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestGetTrustStoreRevocationContent(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"ts-rev-content"},
	})
	require.Equal(t, http.StatusOK, tsRec.Code)
	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	rec := doELBv2(t, h, url.Values{
		"Action":        {"GetTrustStoreRevocationContent"},
		"Version":       {"2015-12-01"},
		"TrustStoreArn": {tsArn},
		"RevocationId":  {"1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
