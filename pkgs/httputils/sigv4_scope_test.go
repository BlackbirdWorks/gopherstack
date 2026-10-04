package httputils_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func TestSigV4ScopeExtraction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		auth        string
		query       string
		wantAK      string
		wantRegion  string
		wantService string
	}{
		{
			name:        "header scope",
			auth:        "AWS4-HMAC-SHA256 Credential=AKID/20260101/eu-west-1/sqs/aws4_request, SignedHeaders=host",
			wantAK:      "AKID",
			wantRegion:  "eu-west-1",
			wantService: "sqs",
		},
		{
			name:        "query scope",
			query:       "X-Amz-Credential=AKQ%2F20260101%2Fus-west-2%2Fs3%2Faws4_request",
			wantAK:      "AKQ",
			wantRegion:  "us-west-2",
			wantService: "s3",
		},
		{
			name:       "too few parts falls back",
			auth:       "AWS4-HMAC-SHA256 Credential=AKID/20260101/eu-west-1/sqs, SignedHeaders=host",
			wantRegion: "us-east-1",
			wantAK:     "",
		},
		{
			name:       "too many parts falls back",
			auth:       "AWS4-HMAC-SHA256 Credential=AKID/20260101/eu-west-1/sqs/aws4_request/x, SignedHeaders=host",
			wantRegion: "us-east-1",
		},
		{
			name:       "wrong terminal falls back",
			auth:       "AWS4-HMAC-SHA256 Credential=AKID/20260101/eu-west-1/sqs/other, SignedHeaders=host",
			wantRegion: "us-east-1",
		},
		{
			name:       "empty part falls back",
			auth:       "AWS4-HMAC-SHA256 Credential=AKID/20260101//sqs/aws4_request, SignedHeaders=host",
			wantRegion: "us-east-1",
		},
		{
			name:       "blank part falls back",
			auth:       "AWS4-HMAC-SHA256 Credential=AKID/ /eu-west-1/sqs/aws4_request",
			wantRegion: "us-east-1",
		},
		{
			name:       "no auth",
			wantRegion: "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			target := "http://x/"
			if tt.query != "" {
				target += "?" + tt.query
			}

			r := httptest.NewRequest(http.MethodGet, target, nil)
			if tt.auth != "" {
				r.Header.Set("Authorization", tt.auth)
			}

			assert.Equal(t, tt.wantAK, httputils.ExtractAccessKeyFromRequest(r))
			assert.Equal(t, tt.wantRegion, httputils.ExtractRegionFromRequest(r, "us-east-1"))
			assert.Equal(t, tt.wantService, httputils.ExtractServiceFromRequest(r))

			ak, region, svc := httputils.SigV4RequestFields(r, "us-east-1")
			assert.Equal(t, tt.wantAK, ak)
			assert.Equal(t, tt.wantRegion, region)
			assert.Equal(t, tt.wantService, svc)
		})
	}
}

func TestSanitizeHeaderString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"clean", "us-east-1", "us-east-1"},
		{"empty", "", ""},
		{"arn chars", "a+b/c=d:e~f_g.h", "a+b/c=d:e~f_g.h"},
		{"spaces dropped", "a b\tc\n", "abc"},
		{"control dropped", "ab\x00\x7fc", "abc"},
		{"unicode dropped", "aéb世", "ab"},
		{"invalid utf8 dropped", "a\xffb", "ab"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, httputils.SanitizeHeaderString(tt.in))
		})
	}
}
