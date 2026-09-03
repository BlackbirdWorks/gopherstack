package azureauth_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/azureauth"
)

func TestParseAuthorizationHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		header      string
		wantAccount string
		wantSig     string
		wantScheme  azureauth.Scheme
		wantOK      bool
	}{
		{
			name:        "valid SharedKey",
			header:      "SharedKey devstoreaccount1:abcdefg123==",
			wantOK:      true,
			wantScheme:  azureauth.SchemeSharedKey,
			wantAccount: "devstoreaccount1",
			wantSig:     "abcdefg123==",
		},
		{
			name:        "valid SharedKeyLite",
			header:      "SharedKeyLite devstoreaccount1:xyz789==",
			wantOK:      true,
			wantScheme:  azureauth.SchemeSharedKeyLite,
			wantAccount: "devstoreaccount1",
			wantSig:     "xyz789==",
		},
		{
			name:   "empty header",
			header: "",
			wantOK: false,
		},
		{
			name:   "wrong scheme",
			header: "Bearer sometoken",
			wantOK: false,
		},
		{
			name:   "missing colon",
			header: "SharedKey devstoreaccount1nosig",
			wantOK: false,
		},
		{
			name:   "empty account",
			header: "SharedKey :abcdef==",
			wantOK: false,
		},
		{
			name:   "empty signature",
			header: "SharedKey devstoreaccount1:",
			wantOK: false,
		},
		{
			name:   "extra junk after signature",
			header: "SharedKey devstoreaccount1:abcdef== extra junk",
			wantOK: false,
		},
		{
			name:   "scheme with no space",
			header: "SharedKeydevstoreaccount1:abcdef==",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := azureauth.ParseAuthorizationHeader(tt.header)
			assert.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				return
			}
			assert.Equal(t, tt.wantScheme, got.Scheme)
			assert.Equal(t, tt.wantAccount, got.Account)
			assert.Equal(t, tt.wantSig, got.Signature)
		})
	}
}

func TestVerifySharedKey(t *testing.T) {
	t.Parallel()

	newSignedRequest := func(t *testing.T) *http.Request {
		t.Helper()

		r := httptest.NewRequest(
			http.MethodGet,
			"http://127.0.0.1:10000/devstoreaccount1/mycontainer?restype=container&comp=list",
			nil,
		)
		r.Header.Set("x-ms-date", "Tue, 27 Aug 2024 12:00:00 GMT")
		r.Header.Set("x-ms-version", "2021-08-06")

		sig, err := azureauth.SignSharedKey(r, azureauth.DefaultAccountName, azureauth.DefaultAccountKey)
		require.NoError(t, err)
		r.Header.Set("Authorization", "SharedKey "+azureauth.DefaultAccountName+":"+sig)

		return r
	}

	tests := []struct {
		mutate    func(r *http.Request)
		name      string
		wantOK    bool
		wantValid bool
	}{
		{
			name:      "valid round-trip",
			mutate:    func(*http.Request) {},
			wantOK:    true,
			wantValid: true,
		},
		{
			name: "tampered signature",
			mutate: func(r *http.Request) {
				r.Header.Set("Authorization", "SharedKey "+azureauth.DefaultAccountName+":dGFtcGVyZWQ=")
			},
			wantOK:    true,
			wantValid: false,
		},
		{
			name: "tampered method",
			mutate: func(r *http.Request) {
				r.Method = http.MethodDelete
			},
			wantOK:    true,
			wantValid: false,
		},
		{
			name: "malformed authorization header",
			mutate: func(r *http.Request) {
				r.Header.Set("Authorization", "Bearer sometoken")
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := newSignedRequest(t)
			tt.mutate(r)

			valid, err := azureauth.VerifySharedKey(azureauth.DefaultAccountKey, r)
			if !tt.wantOK {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantValid, valid)
		})
	}
}

func TestVerifySharedKeyLite(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(
		http.MethodGet,
		"http://127.0.0.1:10002/devstoreaccount1/Tables",
		nil,
	)
	r.Header.Set("x-ms-date", "Tue, 27 Aug 2024 12:00:00 GMT")
	r.Header.Set("x-ms-version", "2021-08-06")
	r.Header.Set("Content-Type", "application/json")

	sig, err := azureauth.SignSharedKeyLite(r, azureauth.DefaultAccountName, azureauth.DefaultAccountKey)
	require.NoError(t, err)
	r.Header.Set("Authorization", "SharedKeyLite "+azureauth.DefaultAccountName+":"+sig)

	valid, err := azureauth.VerifySharedKey(azureauth.DefaultAccountKey, r)
	require.NoError(t, err)
	assert.True(t, valid)
}

func TestStringToSign(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(
		http.MethodPut,
		"http://127.0.0.1:10000/devstoreaccount1/c/blob.txt?comp=block&blockid=AAAA",
		nil,
	)
	r.Header.Set("Content-Type", "text/plain")
	r.Header.Set("x-ms-date", "Tue, 27 Aug 2024 12:00:00 GMT")
	r.Header.Set("x-ms-version", "2021-08-06")
	r.Header.Set("x-ms-blob-type", "BlockBlob")
	r.ContentLength = 11

	want := "PUT\n" + // verb
		"\n" + // content-encoding
		"\n" + // content-language
		"11\n" + // content-length
		"\n" + // content-md5
		"text/plain\n" + // content-type
		"\n" + // date
		"\n" + // if-modified-since
		"\n" + // if-match
		"\n" + // if-none-match
		"\n" + // if-unmodified-since
		"\n" + // range
		"x-ms-blob-type:BlockBlob\n" +
		"x-ms-date:Tue, 27 Aug 2024 12:00:00 GMT\n" +
		"x-ms-version:2021-08-06\n" +
		"/devstoreaccount1/c/blob.txt\n" +
		// (no doubled account segment: the request path already carries
		// devstoreaccount1, matching Azurite's path-style addressing)
		"blockid:AAAA\n" +
		"comp:block"

	assert.Equal(t, want, azureauth.StringToSign(r, azureauth.DefaultAccountName))
}

func TestStringToSignLite(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:10002/devstoreaccount1/Tables", nil)
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("x-ms-date", "Tue, 27 Aug 2024 12:00:00 GMT")

	want := "GET\n" +
		"\n" + // content-md5
		"application/json\n" +
		"\n" + // date
		"x-ms-date:Tue, 27 Aug 2024 12:00:00 GMT\n" +
		"/devstoreaccount1/Tables"

	assert.Equal(t, want, azureauth.StringToSignLite(r, azureauth.DefaultAccountName))
}

func TestCanonicalizedResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		rawURL  string
		account string
		want    string
	}{
		{
			name:    "no query",
			rawURL:  "http://host/devstoreaccount1/container/blob",
			account: "devstoreaccount1",
			want:    "/devstoreaccount1/container/blob",
		},
		{
			name:    "single query param",
			rawURL:  "http://host/acct/c?restype=container",
			account: "acct",
			want:    "/acct/c\nrestype:container",
		},
		{
			name:    "multiple params sorted and lowercased",
			rawURL:  "http://host/acct/c?comp=list&RESTYPE=container",
			account: "acct",
			want:    "/acct/c\ncomp:list\nrestype:container",
		},
		{
			name:    "repeated param comma-joined sorted",
			rawURL:  "http://host/acct/c?comp=b&comp=a",
			account: "acct",
			want:    "/acct/c\ncomp:a,b",
		},
		{
			name:    "host-style path with no account segment",
			rawURL:  "http://acct.blob.core.windows.net/c/blob",
			account: "acct",
			want:    "/acct/c/blob",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, tt.rawURL, nil)
			assert.Equal(t, tt.want, azureauth.CanonicalizedResource(tt.account, r.URL))
		})
	}
}

func TestCanonicalizedHeaders(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodGet, "http://host/a/b", nil)
	r.Header.Set("x-ms-version", "2021-08-06")
	r.Header.Set("x-ms-date", "Tue, 27 Aug 2024 12:00:00 GMT")
	r.Header.Set("Content-Type", "text/plain") // not x-ms-*, must be excluded
	r.Header.Set("x-ms-meta-foo", "  a   b  ") // whitespace collapsed/trimmed

	want := "x-ms-date:Tue, 27 Aug 2024 12:00:00 GMT\n" +
		"x-ms-meta-foo:a b\n" +
		"x-ms-version:2021-08-06\n"

	assert.Equal(t, want, azureauth.CanonicalizedHeaders(r))
}
