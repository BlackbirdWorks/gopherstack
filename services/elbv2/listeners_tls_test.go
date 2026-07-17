package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHTTPSListenerCertificateEnforcement verifies that HTTPS listeners require ≥1 cert.
func TestHTTPSListenerCertificateEnforcement(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "https-cert-lb")
	tgArn := mustCreateTG(t, h, "https-cert-tg")

	// Should fail: no certificate provided.
	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Should succeed: certificate provided.
	rec2 := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:123:certificate/my-cert"},
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestHTTPSListenerDefaultCertMarked verifies the first cert is marked IsDefault.
func TestHTTPSListenerDefaultCertMarked(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "https-default-cert-lb")
	tgArn := mustCreateTG(t, h, "https-default-cert-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:123:certificate/cert1"},
		"Certificates.member.2.CertificateArn":   {"arn:aws:acm:us-east-1:123:certificate/cert2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	listenerArn := resp.Result.Listeners.Members[0].ListenerArn

	// DescribeListenerCertificates should show the first cert as default.
	rec2 := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerCertificates"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var certResp struct {
		Result struct {
			Certificates struct {
				Members []struct {
					CertificateArn string `xml:"CertificateArn"`
					IsDefault      bool   `xml:"IsDefault"`
				} `xml:"member"`
			} `xml:"Certificates"`
		} `xml:"DescribeListenerCertificatesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &certResp))
	require.Len(t, certResp.Result.Certificates.Members, 2)
	// First cert should be marked as default.
	assert.True(t, certResp.Result.Certificates.Members[0].IsDefault)
}

// TestMutualAuthenticationOnListener verifies mTLS mode is stored and returned.
func TestMutualAuthenticationOnListener(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "mtls-lb")
	tgArn := mustCreateTG(t, h, "mtls-tg")

	// Create trust store first.
	tsr := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"mtls-ts"},
	})
	require.Equal(t, http.StatusOK, tsr.Code)

	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsr.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:123:certificate/cert"},
		"MutualAuthentication.Mode":              {"verify"},
		"MutualAuthentication.TrustStoreArn":     {tsArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn          string `xml:"ListenerArn"`
					MutualAuthentication struct {
						Mode          string `xml:"Mode"`
						TrustStoreArn string `xml:"TrustStoreArn"`
					} `xml:"MutualAuthentication"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Listeners.Members, 1)
	assert.Equal(t, "verify", resp.Result.Listeners.Members[0].MutualAuthentication.Mode)
	assert.Equal(t, tsArn, resp.Result.Listeners.Members[0].MutualAuthentication.TrustStoreArn)
}

// TestRemoveLastCertificateHTTPS tests that removing the last cert from HTTPS listener fails.
func TestRemoveLastCertificateHTTPS(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "cert-lb")
	tgArn := mustCreateTG(t, h, "cert-tg")

	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/test-cert-1"

	// Create HTTPS listener with one certificate.
	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"SslPolicy":                              {"ELBSecurityPolicy-2016-08"},
		"Certificates.member.1.CertificateArn":   {certArn},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var lResp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &lResp))
	listenerArn := lResp.Result.Listeners.Members[0].ListenerArn

	// Removing the only certificate should fail.
	removeRec := doELBv2(t, h, url.Values{
		"Action":                               {"RemoveListenerCertificates"},
		"Version":                              {"2015-12-01"},
		"ListenerArn":                          {listenerArn},
		"Certificates.member.1.CertificateArn": {certArn},
	})
	assert.Equal(t, http.StatusBadRequest, removeRec.Code)
}

// TestCertificatesInDescribeListeners tests that certificates appear in DescribeListeners output.
func TestCertificatesInDescribeListeners(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "certs-describe-lb")
	tgArn := mustCreateTG(t, h, "certs-describe-tg")

	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/desc-cert"

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"SslPolicy":                              {"ELBSecurityPolicy-2016-08"},
		"Certificates.member.1.CertificateArn":   {certArn},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doELBv2(t, h, url.Values{
		"Action":          {"DescribeListeners"},
		"Version":         {"2015-12-01"},
		"LoadBalancerArn": {lbArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					Certificates struct {
						Members []struct {
							CertificateArn string `xml:"CertificateArn"`
						} `xml:"member"`
					} `xml:"Certificates"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"DescribeListenersResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Listeners.Members, 1)
	require.Len(t, resp.Result.Listeners.Members[0].Certificates.Members, 1)
	assert.Equal(t, certArn, resp.Result.Listeners.Members[0].Certificates.Members[0].CertificateArn)
}

// TestHTTPSListenerRequiresCert tests that HTTPS listener requires at least one certificate.
func TestHTTPSListenerRequiresCert(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "https-nocert-lb")
	tgArn := mustCreateTG(t, h, "https-nocert-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHTTPSListenerDefaultSSLPolicy verifies that HTTPS listeners get a default SSL policy.
func TestHTTPSListenerDefaultSSLPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "https-ssl-policy-lb")
	tgArn := mustCreateTG(t, h, "https-ssl-policy-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:123456789012:certificate/test"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					SslPolicy string `xml:"SslPolicy"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Listeners.Members, 1)
	assert.Equal(t, "ELBSecurityPolicy-2016-08", resp.Result.Listeners.Members[0].SslPolicy)
}

func TestCreateListener_HTTPS_RequiresCert(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "listener-https-no-cert")
	tgArn := b1CreateTG(t, h, "listener-https-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestCreateListener_HTTPS_WithCert(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "listener-https-cert")
	tgArn := b1CreateTG(t, h, "listener-https-cert-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:000000000000:certificate/aaa"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "HTTPS")
}

func TestCreateListener_DefaultSSLPolicy(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "listener-default-ssl")
	tgArn := b1CreateTG(t, h, "listener-default-ssl-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTPS"},
		"Port":                                   {"443"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
		"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:000000000000:certificate/bbb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ELBSecurityPolicy")
}

func TestMutualAuth_OnListener(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "mtls-lb")
	tgArn := b1CreateTG(t, h, "mtls-tg")

	// Create trust store first
	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"mtls-ts"},
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
		"MutualAuthentication.IgnoreClientCertificateExpiry": {"false"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "verify")
	assert.Contains(t, body, tsArn)
}
