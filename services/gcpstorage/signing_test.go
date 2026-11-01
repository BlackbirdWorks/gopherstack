package gcpstorage_test

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // V2 HMAC signing is defined with HMAC-SHA1
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

const signerEmail = "signer@test-project.iam.gserviceaccount.com"

var signerKey = sync.OnceValue(func() *rsa.PrivateKey { //nolint:gochecknoglobals // expensive shared test key
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	return k
})

type signEnv struct {
	backend *gcpstorage.InMemoryBackend
	handler *gcpstorage.Handler
	srv     *httptest.Server
	host    string
}

func newSignEnv(t *testing.T) *signEnv {
	t.Helper()

	backend := gcpstorage.NewInMemoryBackend()
	backend.SetKeyResolver(func(email string) []*rsa.PublicKey {
		if email == signerEmail {
			return []*rsa.PublicKey{&signerKey().PublicKey}
		}

		return nil
	})

	h := gcpstorage.NewHandler(backend)
	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	_, err := backend.CreateBucket(sdkProject, gcpstorage.Patch{"name": []byte(`"bkt"`)})
	require.NoError(t, err)

	_, err = backend.PutObject("bkt", "dir/a b.txt", []byte("secret"), gcpstorage.Patch{}, gcpstorage.Preconds{})
	require.NoError(t, err)

	return &signEnv{backend: backend, handler: h, srv: srv, host: strings.TrimPrefix(srv.URL, "http://")}
}

func (s *signEnv) do(t *testing.T, method, rawURL string, hdr map[string]string) (int, string, http.Header) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, rawURL, strings.NewReader(""))
	require.NoError(t, err)

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(body), resp.Header
}

func (s *signEnv) rsaURL(t *testing.T, scheme storage.SigningScheme, method string, exp time.Duration) string {
	t.Helper()

	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(signerKey())})
	u, err := storage.SignedURL("bkt", "dir/a b.txt", &storage.SignedURLOptions{
		GoogleAccessID: signerEmail, PrivateKey: pemKey, Method: method, Expires: time.Now().Add(exp),
		Scheme: scheme, Hostname: s.host, Insecure: true,
	})
	require.NoError(t, err)

	return strings.Replace(u, "https://", "http://", 1)
}

func withQuery(t *testing.T, raw, key, val string) string {
	t.Helper()

	u, err := url.Parse(raw)
	require.NoError(t, err)

	q := u.Query()
	q.Set(key, val)
	u.RawQuery = q.Encode()

	return u.String()
}

func hmacBytes(key []byte, msg string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(msg))

	return m.Sum(nil)
}

// signV4HMAC builds a GOOG4-HMAC-SHA256 signed URL following the documented algorithm.
func (s *signEnv) signV4HMAC(method, accessID, secret string, at time.Time, expires int) string {
	const path = "/bkt/dir/a%20b.txt"

	date, ts := at.Format("20060102"), at.Format("20060102T150405Z")
	scope := date + "/auto/storage/goog4_request"
	q := url.Values{
		"X-Goog-Algorithm": {"GOOG4-HMAC-SHA256"}, "X-Goog-Credential": {accessID + "/" + scope},
		"X-Goog-Date": {ts}, "X-Goog-Expires": {fmt.Sprint(expires)}, "X-Goog-SignedHeaders": {"host"},
	}
	canon := strings.Join([]string{
		method, path, strings.ReplaceAll(q.Encode(), "+", "%20"), "host:" + s.host + "\n", "host", "UNSIGNED-PAYLOAD",
	}, "\n")
	sum := sha256.Sum256([]byte(canon))
	toSign := "GOOG4-HMAC-SHA256\n" + ts + "\n" + scope + "\n" + hex.EncodeToString(sum[:])

	k := hmacBytes([]byte("GOOG4"+secret), date)
	k = hmacBytes(k, "auto")
	k = hmacBytes(k, "storage")
	k = hmacBytes(k, "goog4_request")
	q.Set("X-Goog-Signature", hex.EncodeToString(hmacBytes(k, toSign)))

	return "http://" + s.host + path + "?" + q.Encode()
}

func TestSignedURLVerification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build    func(t *testing.T, s *signEnv) (method, u string)
		clock    time.Duration
		name     string
		wantCode string
		wantBody string
		want     int
	}{
		{
			name: "v4 rsa valid",
			build: func(t *testing.T, s *signEnv) (string, string) {
				return http.MethodGet, s.rsaURL(t, storage.SigningSchemeV4, http.MethodGet, time.Hour)
			},
			want: http.StatusOK, wantBody: "secret",
		},
		{
			name: "v4 rsa expired",
			build: func(t *testing.T, s *signEnv) (string, string) {
				return http.MethodGet, s.rsaURL(t, storage.SigningSchemeV4, http.MethodGet, time.Hour)
			},
			clock: 2 * time.Hour, want: http.StatusBadRequest, wantCode: "ExpiredToken",
		},
		{
			name: "v4 rsa tampered path",
			build: func(t *testing.T, s *signEnv) (string, string) {
				return http.MethodGet, strings.Replace(
					s.rsaURL(t, storage.SigningSchemeV4, http.MethodGet, time.Hour), "a%20b.txt", "other.txt", 1)
			},
			want: http.StatusForbidden, wantCode: "SignatureDoesNotMatch",
		},
		{
			name: "v4 rsa tampered expiry",
			build: func(t *testing.T, s *signEnv) (string, string) {
				u := s.rsaURL(t, storage.SigningSchemeV4, http.MethodGet, time.Hour)

				return http.MethodGet, withQuery(t, u, "X-Goog-Expires", "604800")
			},
			want: http.StatusForbidden, wantCode: "SignatureDoesNotMatch",
		},
		{
			name: "v4 rsa wrong method",
			build: func(t *testing.T, s *signEnv) (string, string) {
				return http.MethodDelete, s.rsaURL(t, storage.SigningSchemeV4, http.MethodGet, time.Hour)
			},
			want: http.StatusForbidden, wantCode: "SignatureDoesNotMatch",
		},
		{
			name: "v4 rsa unknown account",
			build: func(t *testing.T, s *signEnv) (string, string) {
				u := s.rsaURL(t, storage.SigningSchemeV4, http.MethodGet, time.Hour)

				return http.MethodGet, strings.Replace(u, "signer%40", "ghost%40", 1)
			},
			want: http.StatusForbidden, wantCode: "InvalidSecurity",
		},
		{
			name: "v4 expires over a week",
			build: func(t *testing.T, s *signEnv) (string, string) {
				u := s.rsaURL(t, storage.SigningSchemeV4, http.MethodGet, time.Hour)

				return http.MethodGet, withQuery(t, u, "X-Goog-Expires", "999999")
			},
			want: http.StatusBadRequest, wantCode: "InvalidArgument",
		},
		{
			name: "v2 rsa valid",
			build: func(t *testing.T, s *signEnv) (string, string) {
				return http.MethodGet, s.rsaURL(t, storage.SigningSchemeV2, http.MethodGet, time.Hour)
			},
			want: http.StatusOK, wantBody: "secret",
		},
		{
			name: "v2 rsa expired",
			build: func(t *testing.T, s *signEnv) (string, string) {
				return http.MethodGet, s.rsaURL(t, storage.SigningSchemeV2, http.MethodGet, time.Hour)
			},
			clock: 2 * time.Hour, want: http.StatusBadRequest, wantCode: "ExpiredToken",
		},
		{
			name: "v2 rsa tampered",
			build: func(t *testing.T, s *signEnv) (string, string) {
				return http.MethodGet, strings.Replace(
					s.rsaURL(t, storage.SigningSchemeV2, http.MethodGet, time.Hour), "a%20b.txt", "other.txt", 1)
			},
			want: http.StatusForbidden, wantCode: "SignatureDoesNotMatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSignEnv(t)
			method, u := tt.build(t, s)

			if tt.clock != 0 {
				s.backend.SetClock(func() time.Time { return time.Now().Add(tt.clock) })
			}

			code, body, _ := s.do(t, method, u, nil)
			assert.Equal(t, tt.want, code, body)

			if tt.wantCode != "" {
				assert.Contains(t, body, "<Code>"+tt.wantCode+"</Code>")
			}

			if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, body)
			}
		})
	}
}

func TestSignedURLHMAC(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(s *signEnv, key *gcpstorage.HMACKey) (accessID, secret string)
		name     string
		wantCode string
		expires  int
		want     int
		age      time.Duration
	}{
		{
			name: "valid",
			mutate: func(_ *signEnv, k *gcpstorage.HMACKey) (string, string) {
				return k.AccessID, k.Secret
			},
			expires: 600, want: http.StatusOK,
		},
		{
			name: "expired",
			mutate: func(_ *signEnv, k *gcpstorage.HMACKey) (string, string) {
				return k.AccessID, k.Secret
			},
			expires: 600, age: time.Hour, want: http.StatusBadRequest, wantCode: "ExpiredToken",
		},
		{
			name: "wrong secret",
			mutate: func(_ *signEnv, k *gcpstorage.HMACKey) (string, string) {
				return k.AccessID, k.Secret + "x"
			},
			expires: 600, want: http.StatusForbidden, wantCode: "SignatureDoesNotMatch",
		},
		{
			name: "unknown key",
			mutate: func(_ *signEnv, k *gcpstorage.HMACKey) (string, string) {
				return "GOOG1Eghost", k.Secret
			},
			expires: 600, want: http.StatusForbidden, wantCode: "InvalidSecurity",
		},
		{
			name: "inactive key",
			mutate: func(s *signEnv, k *gcpstorage.HMACKey) (string, string) {
				_, err := s.backend.UpdateHMACKey(sdkProject, k.AccessID, "INACTIVE")
				require.NoError(t, err)

				return k.AccessID, k.Secret
			},
			expires: 600, want: http.StatusForbidden, wantCode: "InvalidSecurity",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSignEnv(t)
			key, err := s.backend.CreateHMACKey(sdkProject, saA)
			require.NoError(t, err)

			id, secret := tt.mutate(s, key)
			u := s.signV4HMAC(http.MethodGet, id, secret, time.Now().UTC(), tt.expires)

			if tt.age != 0 {
				s.backend.SetClock(func() time.Time { return time.Now().Add(tt.age) })
			}

			code, body, _ := s.do(t, http.MethodGet, u, nil)
			assert.Equal(t, tt.want, code, body)

			if tt.wantCode != "" {
				assert.Contains(t, body, "<Code>"+tt.wantCode+"</Code>")
			}
		})
	}
}

func TestSignedURLV2HMAC(t *testing.T) {
	t.Parallel()

	s := newSignEnv(t)
	key, err := s.backend.CreateHMACKey(sdkProject, saA)
	require.NoError(t, err)

	exp := time.Now().Add(time.Hour).Unix()
	path := "/bkt/dir/a%20b.txt"
	m := hmac.New(sha1.New, []byte(key.Secret)) //nolint:gosec // V2 HMAC signing is defined with HMAC-SHA1
	m.Write([]byte(fmt.Sprintf("GET\n\n\n%d\n%s", exp, path)))

	q := url.Values{
		"GoogleAccessId": {key.AccessID}, "Expires": {fmt.Sprint(exp)},
		"Signature": {base64.StdEncoding.EncodeToString(m.Sum(nil))},
	}

	code, body, _ := s.do(t, http.MethodGet, "http://"+s.host+path+"?"+q.Encode(), nil)
	assert.Equal(t, http.StatusOK, code, body)

	q.Set("Signature", base64.StdEncoding.EncodeToString([]byte("nope")))
	code, body, _ = s.do(t, http.MethodGet, "http://"+s.host+path+"?"+q.Encode(), nil)
	assert.Equal(t, http.StatusForbidden, code, body)
}

func TestRequireAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hdr     map[string]string
		name    string
		signed  bool
		require bool
		want    int
	}{
		{name: "anonymous allowed by default", want: http.StatusOK},
		{name: "anonymous rejected when required", require: true, want: http.StatusForbidden},
		{name: "authorization header accepted", require: true, hdr: map[string]string{"Authorization": "Bearer x"}, want: http.StatusOK},
		{name: "signed url accepted", require: true, signed: true, want: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSignEnv(t)
			s.handler.RequireAuth = tt.require

			u := "http://" + s.host + "/bkt/dir/a%20b.txt"
			if tt.signed {
				u = s.rsaURL(t, storage.SigningSchemeV4, http.MethodGet, time.Hour)
			}

			code, body, _ := s.do(t, http.MethodGet, u, tt.hdr)
			assert.Equal(t, tt.want, code, body)

			if tt.want == http.StatusForbidden {
				assert.Contains(t, body, "<Code>AccessDenied</Code>")
			}
		})
	}
}
