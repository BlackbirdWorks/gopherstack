package httputils

import (
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func signV4aManually(t *testing.T, signed []string, regionSet string) *http.Request {
	t.Helper()

	const (
		access = "AKIDEXAMPLE"
		secret = "secret1"
		date   = "20260101T000000Z"
	)

	r := httptest.NewRequest(http.MethodPost, "http://example.com/", strings.NewReader("{}"))
	r.Header.Set("X-Amz-Date", date)
	r.Header.Set("X-Amz-Content-Sha256", hashHex([]byte("{}")))

	if regionSet != "" {
		r.Header.Set("X-Amz-Region-Set", regionSet)
	}

	canonical := buildCanonicalRequest(r, signed, hashHex([]byte("{}")))
	sts := strings.Join([]string{
		SigV4aAlgorithm, date, "20260101/lambda/aws4_request", hashHex([]byte(canonical)),
	}, "\n")
	digest := sha256.Sum256([]byte(sts))

	key := deriveSigV4aKey(access, secret)
	require.NotNil(t, key)

	sig, err := ecdsa.SignASN1(rand.Reader, key, digest[:])
	require.NoError(t, err)

	r.Header.Set("Authorization", SigV4aAlgorithm+" Credential="+access+"/20260101/lambda/aws4_request, SignedHeaders="+
		strings.Join(signed, ";")+", Signature="+hex.EncodeToString(sig))

	return r
}

func TestVerifyV4aRegionSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		regionSet string
		wantCode  string
		signed    []string
	}{
		{name: "signed region set", signed: []string{"host", "x-amz-date", "x-amz-region-set"}, regionSet: "*"},
		{
			name: "region set unsigned", signed: []string{"host", "x-amz-date"}, regionSet: "*",
			wantCode: codeIncompleteSignature,
		},
		{
			name: "region set absent", signed: []string{"host", "x-amz-date", "x-amz-region-set"},
			wantCode: codeIncompleteSignature,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := signV4aManually(t, tt.signed, tt.regionSet)
			err := NewSigV4Validator("secret1").VerifyV4a(r)

			if tt.wantCode == "" {
				assert.Nil(t, err)

				return
			}

			require.NotNil(t, err)
			assert.Equal(t, tt.wantCode, err.Code)
		})
	}
}

func TestVerifyV4aTrailingDER(t *testing.T) {
	t.Parallel()

	r := signV4aManually(t, []string{"host", "x-amz-date", "x-amz-region-set"}, "*")
	r.Header.Set("Authorization", r.Header.Get("Authorization")+"00")

	err := NewSigV4Validator("secret1").VerifyV4a(r)
	require.NotNil(t, err)
	assert.Equal(t, "InvalidSignatureException", err.Code)
}
