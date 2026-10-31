package service

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAltGateAdmits(t *testing.T) {
	t.Parallel()

	gate := AltGate{
		Target:      []string{"Svc."},
		Path:        []string{"/svc/"},
		PathSuffix:  []string{"/jwks.json"},
		Host:        []string{".svc-url."},
		UserAgent:   []string{"mediastore"},
		ContentType: []string{"application/x-www-form-urlencoded"},
		Query:       []string{"Action="},
		Scope:       []string{"svc"},
	}

	signedFor := func(svc string) map[string]string {
		return map[string]string{
			"Authorization": "AWS4-HMAC-SHA256 Credential=k/20260101/us-east-1/" + svc +
				"/aws4_request, SignedHeaders=host, Signature=x",
		}
	}
	form := map[string]string{"Content-Type": "Application/X-WWW-Form-Urlencoded"}

	tests := []struct {
		headers map[string]string
		name    string
		uri     string
		host    string
		want    bool
	}{
		{name: "none", uri: "/bucket/key"},
		{name: "target", uri: "/", headers: map[string]string{"X-Amz-Target": "Svc.Op"}, want: true},
		{name: "target_other", uri: "/", headers: map[string]string{"X-Amz-Target": "Other.Op"}},
		{name: "path", uri: "/svc/x", want: true},
		{name: "path_double_slash", uri: "//svc/x", want: true},
		{name: "path_suffix", uri: "/pool/jwks.json", want: true},
		{name: "host_fold", uri: "/", host: "ABC.SVC-URL.us-east-1.on.aws", want: true},
		{name: "host_other", uri: "/", host: "abc.example.com"},
		{name: "ua_fold", uri: "/", headers: map[string]string{"User-Agent": "x MediaStoreData"}, want: true},
		{name: "ua_non_ascii", uri: "/", headers: map[string]string{"User-Agent": "xİy"}, want: true},
		{name: "amz_ua", uri: "/", headers: map[string]string{"X-Amz-User-Agent": "js mediastore-data"}, want: true},
		{name: "ctype", uri: "/", headers: form, want: true},
		{name: "ctype_json", uri: "/", headers: map[string]string{"Content-Type": "application/json"}},
		{name: "query", uri: "/?Action=Foo", want: true},
		{name: "query_other", uri: "/?x=1"},
		{name: "scope_header", uri: "/", headers: signedFor("svc"), want: true},
		{name: "scope_other", uri: "/", headers: signedFor("s3")},
		{
			name: "scope_presigned", want: true,
			uri: "/?X-Amz-Credential=k%2F20260101%2Fus-east-1%2Fsvc%2Faws4_request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, "http://localhost:4566"+tt.uri, nil)
			if tt.host != "" {
				req.Host = tt.host
			}

			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			q := lookupCtx{req: req, target: req.Header.Get(amzTargetHeader), rawPath: req.URL.Path}
			q.path = gatePath(q.rawPath)

			assert.Equal(t, tt.want, gate.admits(&q))
		})
	}
}

func TestAltGateValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		gate AltGate
		want bool
	}{
		{name: "empty", gate: AltGate{}},
		{name: "empty_string", gate: AltGate{Target: []string{""}}},
		{name: "ok", gate: AltGate{Scope: []string{"s3"}}, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.gate.valid())
		})
	}
}
