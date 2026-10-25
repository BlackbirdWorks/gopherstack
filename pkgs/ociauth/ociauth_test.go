package ociauth_test

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
)

func TestParseKeyID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want ociauth.KeyID
		ok   bool
	}{
		{in: "ocid1.tenancy.oc1..t/ocid1.user.oc1..u/aa:bb", want: ociauth.KeyID{Tenancy: "ocid1.tenancy.oc1..t", User: "ocid1.user.oc1..u", Fingerprint: "aa:bb"}, ok: true},
		{in: "ST$token"},
		{in: "a/b"},
		{in: "a//c"},
		{in: "a/b/c/d"},
	}

	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			t.Parallel()

			got, ok := ociauth.ParseKeyID(tt.in)
			assert.Equal(t, tt.ok, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseAuthorization(t *testing.T) {
	t.Parallel()

	good := `Signature version="1",headers="(request-target) date host",keyId="t/u/f",algorithm="rsa-sha256",signature="` +
		base64.StdEncoding.EncodeToString([]byte("sig")) + `"`

	sig, err := ociauth.ParseAuthorization(good)
	require.NoError(t, err)
	assert.Equal(t, "t/u/f", sig.KeyID)
	assert.Equal(t, []string{"(request-target)", "date", "host"}, sig.Headers)
	assert.Equal(t, []byte("sig"), sig.Signature)

	for _, bad := range []string{"", "Bearer abc", `Signature keyId="k"`, `Signature keyId="k",signature="!!"`, `Signature junk`} {
		_, err = ociauth.ParseAuthorization(bad)
		require.ErrorIs(t, err, ociauth.ErrNotAuthenticated, bad)
	}
}

type signer struct {
	key   *rsa.PrivateKey
	pem   string
	keyID string
}

func newSigner(t *testing.T) signer {
	t.Helper()

	k, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	der, err := x509.MarshalPKIXPublicKey(&k.PublicKey)
	require.NoError(t, err)

	fp, err := ociauth.Fingerprint(&k.PublicKey)
	require.NoError(t, err)

	return signer{key: k, pem: string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})), keyID: "t/u/" + fp}
}

func (s signer) sign(t *testing.T, r *http.Request, headers []string) {
	t.Helper()

	lines := make([]string, len(headers))

	for i, h := range headers {
		switch h {
		case "(request-target)":
			lines[i] = h + ": " + strings.ToLower(r.Method) + " " + r.URL.RequestURI()
		case "host":
			lines[i] = h + ": " + r.Host
		default:
			lines[i] = h + ": " + r.Header.Get(h)
		}
	}

	digest := sha256.Sum256([]byte(strings.Join(lines, "\n")))
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.key, crypto.SHA256, digest[:])
	require.NoError(t, err)

	r.Header.Set("Authorization", `Signature version="1",headers="`+strings.Join(headers, " ")+`",keyId="`+s.keyID+
		`",algorithm="rsa-sha256",signature="`+base64.StdEncoding.EncodeToString(sig)+`"`)
}

func TestVerify(t *testing.T) {
	t.Parallel()

	s := newSigner(t)
	other := newSigner(t)
	now := time.Now()
	reg := ociauth.NewRegistry()
	_, err := reg.Register("t", "u", s.pem)
	require.NoError(t, err)

	mk := func(date time.Time, body string) *http.Request {
		r, _ := http.NewRequestWithContext(t.Context(), http.MethodPut, "http://h.example/n/ns/b/bk/o/x", strings.NewReader(body))
		r.Header.Set("Date", date.UTC().Format(http.TimeFormat))

		return r
	}

	std := []string{"(request-target)", "date", "host"}

	tests := []struct {
		build   func() *http.Request
		name    string
		opts    ociauth.Options
		wantErr bool
		wantVer bool
		wantAnn bool
	}{
		{name: "valid", wantVer: true, opts: ociauth.Options{Registry: reg, Now: func() time.Time { return now }}, build: func() *http.Request {
			r := mk(now, ""); s.sign(t, r, std); return r
		}},
		{name: "tampered target", wantErr: true, opts: ociauth.Options{Registry: reg}, build: func() *http.Request {
			r := mk(now, ""); s.sign(t, r, std); r.URL.Path = "/n/ns/b/bk/o/y"; return r
		}},
		{name: "wrong key same id", wantErr: true, opts: ociauth.Options{Registry: reg}, build: func() *http.Request {
			o := other; o.keyID = s.keyID; r := mk(now, ""); o.sign(t, r, std); return r
		}},
		{name: "skew", wantErr: true, opts: ociauth.Options{Registry: reg, Now: func() time.Time { return now.Add(time.Hour) }}, build: func() *http.Request {
			r := mk(now, ""); s.sign(t, r, std); return r
		}},
		{name: "host not signed", wantErr: true, opts: ociauth.Options{Registry: reg}, build: func() *http.Request {
			r := mk(now, ""); s.sign(t, r, []string{"(request-target)", "date"}); return r
		}},
		{name: "body hash mismatch", wantErr: true, opts: ociauth.Options{Registry: reg}, build: func() *http.Request {
			r := mk(now, "body"); r.Header.Set("x-content-sha256", "bogus")
			s.sign(t, r, append([]string{"x-content-sha256"}, std...)); return r
		}},
		{name: "unknown key permissive", opts: ociauth.Options{Registry: reg}, build: func() *http.Request {
			o := other; r := mk(now, ""); o.sign(t, r, std); return r
		}},
		{name: "unknown key enforced", wantErr: true, opts: ociauth.Options{Registry: reg, Enforce: true}, build: func() *http.Request {
			o := other; r := mk(now, ""); o.sign(t, r, std); return r
		}},
		{name: "anonymous permissive", wantAnn: true, opts: ociauth.Options{Registry: reg}, build: func() *http.Request { return mk(now, "") }},
		{name: "anonymous enforced", wantErr: true, opts: ociauth.Options{Registry: reg, Enforce: true}, build: func() *http.Request { return mk(now, "") }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := tt.build()

			var body []byte
			if r.Header.Get("x-content-sha256") != "" {
				body = []byte("body")
			}

			p, verr := ociauth.Verify(r, body, tt.opts)
			if tt.wantErr {
				require.ErrorIs(t, verr, ociauth.ErrNotAuthenticated)

				return
			}

			require.NoError(t, verr)
			assert.Equal(t, tt.wantVer, p.Verified)
			assert.Equal(t, tt.wantAnn, p.Anonymous)
		})
	}
}

func TestRegistry(t *testing.T) {
	t.Parallel()

	s := newSigner(t)
	reg := ociauth.NewRegistry()

	_, err := reg.Register("t", "u", "junk")
	require.ErrorIs(t, err, ociauth.ErrBadKey)

	fp, err := reg.Register("t", "u", s.pem)
	require.NoError(t, err)
	assert.Len(t, reg.Records(), 1)

	other := ociauth.NewRegistry()
	other.Load(reg.Records())
	assert.Len(t, other.Records(), 1)
	assert.True(t, other.Remove("t/u/"+fp))
	assert.False(t, other.Remove("t/u/"+fp))
}
