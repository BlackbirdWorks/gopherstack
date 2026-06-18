package httputils_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	testSecret = "test-secret"
	testAKID   = "AKIDEXAMPLE"
)

// signRequest signs req with the AWS SDK v4 signer using testSecret, returning
// the request with the Authorization header populated.
func signRequest(t *testing.T, req *http.Request, body string, secret string) {
	t.Helper()

	sum := sha256.Sum256([]byte(body))
	payloadHash := hex.EncodeToString(sum[:])

	signer := v4.NewSigner()

	creds := aws.Credentials{AccessKeyID: testAKID, SecretAccessKey: secret}
	if err := signer.SignHTTP(
		context.Background(),
		creds,
		req,
		payloadHash,
		"dynamodb",
		"us-east-1",
		time.Now(),
	); err != nil {
		t.Fatalf("sign request: %v", err)
	}
}

func TestSigV4Validator_Verify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		body      string
		secret    string // secret the client signs with
		tamper    func(*http.Request)
		wantCode  string // "" => expect success
		validator string // secret the validator uses
	}{
		{
			name:      "valid signature accepted",
			body:      `{"TableName":"t"}`,
			secret:    testSecret,
			validator: testSecret,
			wantCode:  "",
		},
		{
			name:      "wrong secret rejected",
			body:      `{"TableName":"t"}`,
			secret:    "different-secret",
			validator: testSecret,
			wantCode:  "InvalidSignatureException",
		},
		{
			name:      "tampered body rejected",
			body:      `{"TableName":"t"}`,
			secret:    testSecret,
			validator: testSecret,
			tamper: func(r *http.Request) {
				r.Header.Set("X-Amz-Content-Sha256", "deadbeef")
			},
			wantCode: "InvalidSignatureException",
		},
		{
			name:      "tampered signature rejected",
			body:      `{"TableName":"t"}`,
			secret:    testSecret,
			validator: testSecret,
			tamper: func(r *http.Request) {
				auth := r.Header.Get("Authorization")
				r.Header.Set("Authorization", flipLastHexNibble(auth))
			},
			wantCode: "InvalidSignatureException",
		},
		{
			name:      "missing amz-date rejected",
			body:      `{}`,
			secret:    testSecret,
			validator: testSecret,
			tamper: func(r *http.Request) {
				r.Header.Del("X-Amz-Date")
			},
			wantCode: "IncompleteSignatureException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "http://localhost:8000/", strings.NewReader(tc.body))
			req.Header.Set("X-Amz-Target", "DynamoDB_20120810.CreateTable")
			signRequest(t, req, tc.body, tc.secret)

			if tc.tamper != nil {
				tc.tamper(req)
			}

			v := httputils.NewSigV4Validator(tc.validator)
			err := v.Verify(req)

			if tc.wantCode == "" {
				if err != nil {
					t.Fatalf("expected valid signature, got error: %+v", err)
				}

				return
			}

			if err == nil {
				t.Fatalf("expected error %s, got nil", tc.wantCode)
			}

			if err.Code != tc.wantCode {
				t.Fatalf("expected code %s, got %s (%s)", tc.wantCode, err.Code, err.Message)
			}
		})
	}
}

func TestSigV4Validator_EchoMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		secret     string
		wantStatus int
		signed     bool
	}{
		{
			name:       "valid signed request passes through",
			signed:     true,
			secret:     testSecret,
			wantStatus: http.StatusOK,
		},
		{
			name:       "bad signature returns 403",
			signed:     true,
			secret:     "wrong",
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "unsigned request passes through",
			signed:     false,
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			v := httputils.NewSigV4Validator(testSecret)
			e.Use(v.EchoMiddleware())
			e.POST("/", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})

			body := `{"TableName":"t"}`
			req := httptest.NewRequest(http.MethodPost, "http://localhost:8000/", strings.NewReader(body))
			if tc.signed {
				signRequest(t, req, body, tc.secret)
			}

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d (body=%s)", tc.wantStatus, rec.Code, rec.Body.String())
			}
		})
	}
}

// flipLastHexNibble flips the final hex character of the Signature= value so the
// signature no longer matches while remaining well-formed.
func flipLastHexNibble(auth string) string {
	idx := strings.LastIndex(auth, "Signature=")
	if idx < 0 {
		return auth
	}

	b := []byte(auth)
	last := len(b) - 1
	if b[last] == '0' {
		b[last] = '1'
	} else {
		b[last] = '0'
	}

	return string(b)
}
