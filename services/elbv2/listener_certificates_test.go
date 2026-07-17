package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestELBv2_ListenerCertificates validates error handling for add and describe listener certificate operations.
func TestELBv2_ListenerCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elbv2.Handler) url.Values
		checkResp  func(t *testing.T, rec *httptest.ResponseRecorder)
		name       string
		wantStatus int
	}{
		{
			name: "add_certificates_listener_not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"AddListenerCertificates"},
					"Version": {"2015-12-01"},
					"ListenerArn": {
						"arn:aws:elasticloadbalancing:us-east-1:123:listener/nonexistent",
					},
					"Certificates.member.1.CertificateArn": {"arn:aws:acm:us-east-1:123:certificate/abc"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe_certificates_listener_not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":      {"DescribeListenerCertificates"},
					"Version":     {"2015-12-01"},
					"ListenerArn": {"arn:aws:elasticloadbalancing:us-east-1:123:listener/nonexistent"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "add_certificates_missing_listener_arn",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"AddListenerCertificates"},
					"Version": {"2015-12-01"},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "add_certificates_no_certs_provided",
			setup: func(t *testing.T, h *elbv2.Handler) url.Values {
				t.Helper()
				lbArn := mustCreateLB(t, h, "nocert-lb")
				tgArn := mustCreateTG(t, h, "nocert-tg")
				listenerArn := mustCreateListener(t, h, lbArn, tgArn)

				return url.Values{
					"Action":      {"AddListenerCertificates"},
					"Version":     {"2015-12-01"},
					"ListenerArn": {listenerArn},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "remove_certificates_listener_not_found",
			setup: func(t *testing.T, _ *elbv2.Handler) url.Values {
				t.Helper()

				return url.Values{
					"Action":  {"RemoveListenerCertificates"},
					"Version": {"2015-12-01"},
					"ListenerArn": {
						"arn:aws:elasticloadbalancing:us-east-1:123:listener/nonexistent",
					},
					"Certificates.member.1.CertificateArn": {"arn:aws:acm:us-east-1:123:certificate/abc"},
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

// TestELBv2_ListenerCertificatesFullLifecycle tests add, describe, and removal in sequence.
func TestELBv2_ListenerCertificatesFullLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "cert-lb")
	tgArn := mustCreateTG(t, h, "cert-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	cert1 := "arn:aws:acm:us-east-1:123456789012:certificate/cert-1"
	cert2 := "arn:aws:acm:us-east-1:123456789012:certificate/cert-2"

	// Add two certificates.
	addRec := doELBv2(t, h, url.Values{
		"Action":                               {"AddListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {listenerArn},
		"Certificates.member.1.CertificateArn": {cert1},
		"Certificates.member.2.CertificateArn": {cert2},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	var addResp struct {
		Result struct {
			Certificates struct {
				Members []struct {
					CertificateArn string `xml:"CertificateArn"`
				} `xml:"member"`
			} `xml:"Certificates"`
		} `xml:"AddListenerCertificatesResult"`
	}
	require.NoError(t, xml.Unmarshal(addRec.Body.Bytes(), &addResp))
	assert.Len(t, addResp.Result.Certificates.Members, 2)

	// Describe certificates — both should appear.
	descRec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerCertificates"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp struct {
		Result struct {
			Certificates struct {
				Members []struct {
					CertificateArn string `xml:"CertificateArn"`
				} `xml:"member"`
			} `xml:"Certificates"`
		} `xml:"DescribeListenerCertificatesResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.Len(t, descResp.Result.Certificates.Members, 2)

	// Adding same certs again is idempotent.
	addRec2 := doELBv2(t, h, url.Values{
		"Action":                               {"AddListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {listenerArn},
		"Certificates.member.1.CertificateArn": {cert1},
	})
	assert.Equal(t, http.StatusOK, addRec2.Code)

	descRec2 := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerCertificates"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, descRec2.Code)

	var descResp2 struct {
		Result struct {
			Certificates struct {
				Members []struct {
					CertificateArn string `xml:"CertificateArn"`
				} `xml:"member"`
			} `xml:"Certificates"`
		} `xml:"DescribeListenerCertificatesResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec2.Body.Bytes(), &descResp2))
	assert.Len(t, descResp2.Result.Certificates.Members, 2, "duplicate add should be idempotent")

	// Remove cert1 — only cert2 should remain.
	rmRec := doELBv2(t, h, url.Values{
		"Action":                               {"RemoveListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {listenerArn},
		"Certificates.member.1.CertificateArn": {cert1},
	})
	require.Equal(t, http.StatusOK, rmRec.Code)

	descRec3 := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerCertificates"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, descRec3.Code)

	var descResp3 struct {
		Result struct {
			Certificates struct {
				Members []struct {
					CertificateArn string `xml:"CertificateArn"`
				} `xml:"member"`
			} `xml:"Certificates"`
		} `xml:"DescribeListenerCertificatesResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec3.Body.Bytes(), &descResp3))
	require.Len(t, descResp3.Result.Certificates.Members, 1)
	assert.Equal(t, cert2, descResp3.Result.Certificates.Members[0].CertificateArn)
}

// TestRemoveLastCertHTTPAllowed tests that removing all certs from HTTP listener is allowed.
func TestRemoveLastCertHTTPAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "http-cert-lb")
	tgArn := mustCreateTG(t, h, "http-cert-tg")

	// Create HTTP listener.
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	// Add a cert.
	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/http-cert"
	addRec := doELBv2(t, h, url.Values{
		"Action":                               {"AddListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {listenerArn},
		"Certificates.member.1.CertificateArn": {certArn},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	// Remove the cert - should be allowed for HTTP.
	removeRec := doELBv2(t, h, url.Values{
		"Action":                               {"RemoveListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {listenerArn},
		"Certificates.member.1.CertificateArn": {certArn},
	})
	assert.Equal(t, http.StatusOK, removeRec.Code)
}

func TestListenerCertificates_AddAndDescribe(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "cert-lb")
	tgArn := b1CreateTG(t, h, "cert-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn) // HTTP, no cert needed

	addRec := doELBv2(t, h, url.Values{
		"Action":                               {"AddListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {lArn},
		"Certificates.member.1.CertificateArn": {"arn:aws:acm:us-east-1:000000000000:certificate/extra1"},
	})
	assert.Equal(t, http.StatusOK, addRec.Code)

	descRec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerCertificates"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {lArn},
	})
	assert.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), "extra1")
}

func TestListenerCertificates_Remove(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rm-cert-lb")
	tgArn := b1CreateTG(t, h, "rm-cert-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	certArn := "arn:aws:acm:us-east-1:000000000000:certificate/to-remove"
	doELBv2(t, h, url.Values{
		"Action":                               {"AddListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {lArn},
		"Certificates.member.1.CertificateArn": {certArn},
	})

	rmRec := doELBv2(t, h, url.Values{
		"Action":                               {"RemoveListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {lArn},
		"Certificates.member.1.CertificateArn": {certArn},
	})
	assert.Equal(t, http.StatusOK, rmRec.Code)
}
