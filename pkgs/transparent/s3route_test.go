package transparent_test

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/pkgs/transparent"
)

const testAcct = "123456789012"

type hostCase struct {
	host string
	want transparent.S3Host
	ok   bool
}

func apHost(name string) transparent.S3Host {
	return transparent.S3Host{Kind: transparent.S3HostAccessPoint, Name: name, Account: testAcct}
}

func webHost(name string) transparent.S3Host {
	return transparent.S3Host{Kind: transparent.S3HostWebsite, Name: name}
}

func ctlHost() transparent.S3Host {
	return transparent.S3Host{Kind: transparent.S3HostControl, Account: testAcct}
}

func s3HostCases() []hostCase {
	const z = ".amazonaws.com"

	return []hostCase{
		{"ap1-" + testAcct + ".s3-accesspoint.us-east-1" + z, apHost("ap1"), true},
		{"my-ap-" + testAcct + ".s3-accesspoint.dualstack.eu-west-1" + z + ":443", apHost("my-ap"), true},
		{"ap-" + testAcct + ".s3-accesspoint-fips.us-gov-west-1" + z, apHost("ap"), true},
		{"ap-short.s3-accesspoint.us-east-1" + z, transparent.S3Host{}, false},
		{"bkt.s3-website-us-east-1" + z, webHost("bkt"), true},
		{"my.bkt.s3-website.eu-central-1" + z, webHost("my.bkt"), true},
		{"s3-website.us-east-1" + z, transparent.S3Host{}, false},
		{testAcct + ".s3-control.us-east-1" + z, ctlHost(), true},
		{testAcct + ".s3-control.dualstack.us-east-1" + z, ctlHost(), true},
		{testAcct + ".s3-control-fips.us-east-1" + z, ctlHost(), true},
		{"s3.dualstack.us-east-1" + z, transparent.S3Host{}, false},
		{"bkt.s3-fips.us-east-1" + z, transparent.S3Host{}, false},
		{"localhost:4566", transparent.S3Host{}, false},
	}
}

func TestParseS3Host(t *testing.T) {
	t.Parallel()

	for _, tt := range s3HostCases() {
		t.Run(tt.host, func(t *testing.T) {
			t.Parallel()

			got, ok := transparent.ParseS3Host(tt.host)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func fakeRouter() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if name, ok := strings.CutPrefix(r.URL.Path, "/v20180820/accesspoint/"); ok {
			if name != "ap1" || r.Header.Get("X-Amz-Account-Id") != "123456789012" {
				w.WriteHeader(http.StatusNotFound)

				return
			}

			_, _ = io.WriteString(w,
				`<GetAccessPointResult><Name>ap1</Name><Bucket>real-bkt</Bucket></GetAccessPointResult>`)

			return
		}

		_, _ = io.WriteString(w, r.Method+" "+r.URL.Path+" acct="+r.Header.Get("X-Amz-Account-Id"))
	})
}

func TestS3HostRouter(t *testing.T) {
	t.Parallel()

	const (
		ap   = "-" + testAcct + ".s3-accesspoint.us-east-1.amazonaws.com"
		web  = ".s3-website-us-east-1.amazonaws.com"
		ctl  = testAcct + ".s3-control.us-east-1.amazonaws.com"
		wsp  = "GET /_gopherstack/website/bkt/"
		acct = "acct=" + testAcct
	)

	tests := []struct {
		name, host, path, wantBody string
		wantCode                   int
	}{
		{"access point", "ap1" + ap, "/k/1", "GET /real-bkt/k/1 acct=", 200},
		{"unknown access point", "nope" + ap, "/k", "NoSuchAccessPoint", 404},
		{"website", "bkt" + web, "/index.html", wsp + "index.html", 200},
		{"website root", "bkt.s3-website.us-east-1.amazonaws.com", "/", wsp, 200},
		{"control", ctl, "/v20180820/bucket", acct, 200},
		{"plain", "s3.dualstack.us-east-1.amazonaws.com", "/bkt/k", "GET /bkt/k acct=", 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, "http://"+tt.host+tt.path, nil)
			rec := httptest.NewRecorder()
			transparent.S3HostRouter(fakeRouter()).ServeHTTP(rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestCertCoversS3Hosts(t *testing.T) {
	t.Parallel()

	ca, err := devtls.NewCA()
	require.NoError(t, err)

	src := transparent.NewCertSource(ca)
	t.Cleanup(src.Close)

	for _, tt := range s3HostCases() {
		host, _, _ := strings.Cut(tt.host, ":")

		t.Run(host, func(t *testing.T) {
			t.Parallel()

			cert, gErr := src.GetCertificate(&tls.ClientHelloInfo{ServerName: host})
			require.NoError(t, gErr)

			leaf, pErr := x509.ParseCertificate(cert.Certificate[0])
			require.NoError(t, pErr)

			if strings.HasSuffix(host, "amazonaws.com") {
				_, vErr := leaf.Verify(x509.VerifyOptions{Roots: ca.Pool(), DNSName: host})
				assert.NoError(t, vErr)
			}
		})
	}
}

func TestStartHTTP(t *testing.T) {
	t.Parallel()

	srv, err := transparent.StartHTTP(t.Context(), "127.0.0.1:0", fakeRouter())
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Close() })

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+srv.Addr().String()+"/x", nil)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	assert.Contains(t, string(b), "GET /x")
}
