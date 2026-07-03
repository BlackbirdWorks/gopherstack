package s3_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	testAKID   = "AKIDEXAMPLE"
	testSecret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

// signRequestV4 signs r with the real aws-sdk-go-v2 SigV4 header signer so the
// verification path is exercised against authoritative signer semantics.
func signRequestV4(t *testing.T, r *http.Request, payloadHash string) {
	t.Helper()

	r.Header.Set(s3.ContentSHA256HeaderName, payloadHash)
	signer := v4.NewSigner()
	creds := aws.Credentials{AccessKeyID: testAKID, SecretAccessKey: testSecret}
	err := signer.SignHTTP(
		context.Background(), creds, r, payloadHash, "s3", "us-east-1", time.Now().UTC(),
	)
	require.NoError(t, err)
}

func TestParseAuthorizationHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		header     string
		wantAKID   string
		wantRegion string
		wantOK     bool
		wantSigV2  bool
	}{
		{
			name: "valid sigv4",
			header: "AWS4-HMAC-SHA256 Credential=AKID/20240101/eu-west-1/s3/aws4_request, " +
				"SignedHeaders=host;x-amz-date, Signature=abcdef",
			wantOK:     true,
			wantAKID:   "AKID",
			wantRegion: "eu-west-1",
		},
		{
			name:       "sigv4 credential only",
			header:     "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request",
			wantOK:     true,
			wantAKID:   "AKID",
			wantRegion: "us-east-1",
		},
		{
			name:      "sigv2 header",
			header:    "AWS AKIDEXAMPLE:aGVsbG8signature",
			wantOK:    true,
			wantSigV2: true,
			wantAKID:  "AKIDEXAMPLE",
		},
		{
			name:   "unknown scheme",
			header: "Bearer sometoken",
			wantOK: false,
		},
		{
			name:   "sigv4 short credential",
			header: "AWS4-HMAC-SHA256 Credential=AKID/20240101, Signature=x",
			wantOK: false,
		},
		{
			name:   "sigv2 missing colon",
			header: "AWS AKIDEXAMPLEnosig",
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := s3.ParseAuthorizationHeaderForTest(tt.header)
			assert.Equal(t, tt.wantOK, got.OK)
			if !tt.wantOK {
				return
			}
			assert.Equal(t, tt.wantSigV2, got.IsSigV2)
			assert.Equal(t, tt.wantAKID, got.AccessKeyID)
			if tt.wantRegion != "" {
				assert.Equal(t, tt.wantRegion, got.Region)
			}
		})
	}
}

func TestVerifyHeaderAuth_EnforcementMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, r *http.Request)
		name      string
		wantCode  string
		wantAllow bool
	}{
		{
			name: "valid sdk-signed request accepted",
			setup: func(t *testing.T, r *http.Request) {
				t.Helper()
				signRequestV4(t, r, s3.EmptyStringSHA256)
			},
			wantAllow: true,
		},
		{
			name: "tampered signature rejected",
			setup: func(t *testing.T, r *http.Request) {
				t.Helper()
				signRequestV4(t, r, s3.EmptyStringSHA256)
				auth := r.Header.Get("Authorization")
				r.Header.Set("Authorization", auth[:len(auth)-4]+"dead")
			},
			wantAllow: false,
			wantCode:  s3.ErrSignatureMismatch,
		},
		{
			name: "tampered method rejected",
			setup: func(t *testing.T, r *http.Request) {
				t.Helper()
				signRequestV4(t, r, s3.EmptyStringSHA256)
				r.Method = http.MethodDelete
			},
			wantAllow: false,
			wantCode:  s3.ErrSignatureMismatch,
		},
		{
			name: "no authorization header treated as anonymous",
			setup: func(t *testing.T, _ *http.Request) {
				t.Helper()
			},
			wantAllow: true,
		},
		{
			name: "credential-only header missing signature rejected in enforcement mode",
			setup: func(t *testing.T, r *http.Request) {
				t.Helper()
				r.Header.Set("Authorization",
					"AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request")
			},
			wantAllow: false,
			wantCode:  s3.ErrSignatureMismatch,
		},
		{
			name: "wrong service scope rejected",
			setup: func(t *testing.T, r *http.Request) {
				t.Helper()
				r.Header.Set("Authorization",
					"AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/dynamodb/aws4_request, "+
						"SignedHeaders=host, Signature=abc")
			},
			wantAllow: false,
			wantCode:  s3.ErrSignatureMismatch,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := s3.NewHandler(s3.NewInMemoryBackend(&s3.GzipCompressor{})).
				WithPresignValidation(testSecret)
			r := httptest.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil)
			tt.setup(t, r)

			rec := httptest.NewRecorder()
			allow := h.VerifyHeaderAuth(context.Background(), rec, r)

			assert.Equal(t, tt.wantAllow, allow)
			if !tt.wantAllow {
				assert.Contains(t, rec.Body.String(), tt.wantCode)
			}
		})
	}
}

func TestVerifyHeaderAuth_DisabledByDefault(t *testing.T) {
	t.Parallel()

	// With no PresignSecret configured, even a bogus signature is accepted (the
	// credential scope is only used for region routing).
	h := s3.NewHandler(s3.NewInMemoryBackend(&s3.GzipCompressor{}))
	r := httptest.NewRequest(http.MethodGet, "http://example.com/bucket/key", nil)
	r.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, "+
			"SignedHeaders=host, Signature=deadbeef")

	rec := httptest.NewRecorder()
	assert.True(t, h.VerifyHeaderAuth(context.Background(), rec, r))
}

func TestVerifyRequestDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		amzDate  string
		wantPass bool
	}{
		{name: "now", amzDate: time.Now().UTC().Format(s3.PresignedDateFormat), wantPass: true},
		{name: "no date", amzDate: "", wantPass: true},
		{
			name:     "far in past",
			amzDate:  time.Now().UTC().Add(-2 * time.Hour).Format(s3.PresignedDateFormat),
			wantPass: false,
		},
		{
			name:     "far in future",
			amzDate:  time.Now().UTC().Add(2 * time.Hour).Format(s3.PresignedDateFormat),
			wantPass: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodGet, "/b/k", nil)
			if tt.amzDate != "" {
				r.Header.Set("X-Amz-Date", tt.amzDate)
			}
			assert.Equal(t, tt.wantPass, s3.VerifyRequestDate(r))
		})
	}
}

func TestSignatureMatches_QueryStringSigned(t *testing.T) {
	t.Parallel()

	// A signed request that carries a canonical query string must still verify;
	// this guards the canonical-query construction.
	h := s3.NewHandler(s3.NewInMemoryBackend(&s3.GzipCompressor{})).
		WithPresignValidation(testSecret)
	r := httptest.NewRequest(http.MethodGet,
		"http://example.com/bucket/key?versionId=abc&partNumber=2", nil)
	signRequestV4(t, r, s3.UnsignedPayloadHash)

	rec := httptest.NewRecorder()
	assert.True(t, h.VerifyHeaderAuth(context.Background(), rec, r),
		"query-bearing signed request should verify; body=%s", rec.Body.String())
}

func TestGlobMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern string
		input   string
		want    bool
	}{
		{"s3:*", s3.ActionGetObjectLower, true},
		{"s3:get*", s3.ActionGetObjectLower, true},
		{"s3:put*", s3.ActionGetObjectLower, false},
		{"*", "anything", true},
		{"arn:aws:s3:::bucket/*", "arn:aws:s3:::bucket/key", true},
		{"arn:aws:s3:::bucket/*", "arn:aws:s3:::other/key", false},
		{"arn:aws:s3:::bucket", "arn:aws:s3:::bucket", true},
		{"a?c", "abc", true},
		{"a?c", "ac", false},
	}

	for _, tt := range tests {
		t.Run(tt.pattern+"_"+tt.input, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, s3.WildcardMatch(tt.pattern, tt.input))
		})
	}
}
