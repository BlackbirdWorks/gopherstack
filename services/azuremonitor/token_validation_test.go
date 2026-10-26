package azuremonitor_test

import (
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/services/azuremonitor"
)

func forgedToken(t *testing.T, aud string) string {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"aud": aud, "exp": time.Now().Add(time.Hour).Unix(),
	})

	signed, err := tok.SignedString(key)
	require.NoError(t, err)

	return signed
}

func TestValidateTokensVerifiesSignature(t *testing.T) {
	t.Parallel()

	const monitorAud = "https://api.loganalytics.io"

	tests := []struct {
		token func(t *testing.T, iss *aadauth.Issuer) string
		name  string
		wait  time.Duration
		want  int
	}{
		{
			name: "valid", want: http.StatusOK,
			token: func(t *testing.T, iss *aadauth.Issuer) string {
				t.Helper()

				return issue(t, iss, monitorAud, 3600)
			},
		},
		{
			name: "unsigned by issuer", want: http.StatusUnauthorized,
			token: func(t *testing.T, _ *aadauth.Issuer) string {
				t.Helper()

				return forgedToken(t, monitorAud)
			},
		},
		{
			name: "expired", want: http.StatusUnauthorized, wait: 5 * time.Second,
			token: func(t *testing.T, iss *aadauth.Issuer) string {
				t.Helper()

				return issue(t, iss, monitorAud, 1)
			},
		},
		{
			name: "wrong audience", want: http.StatusUnauthorized,
			token: func(t *testing.T, iss *aadauth.Issuer) string {
				t.Helper()

				return issue(t, iss, "https://management.azure.com", 3600)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				settings := azuremonitor.Settings{ValidateTokens: true}
				h := azuremonitor.NewHandler(azuremonitor.NewBackend(settings), settings)
				require.NotNil(t, h.Issuer)

				token := tt.token(t, h.Issuer)
				time.Sleep(tt.wait)

				e := echo.New()
				e.Any("/*", h.Handler())

				req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/v1/workspaces/x/query",
					strings.NewReader(`{"query":"Heartbeat"}`))
				req.Header.Set("Authorization", "Bearer "+token)

				rec := httptest.NewRecorder()
				e.ServeHTTP(rec, req)

				if tt.want == http.StatusUnauthorized {
					assert.Equal(t, http.StatusUnauthorized, rec.Code)
				} else {
					assert.NotEqual(t, http.StatusUnauthorized, rec.Code)
				}
			})
		})
	}
}

func issue(t *testing.T, iss *aadauth.Issuer, aud string, expiry int) string {
	t.Helper()

	tok, err := iss.IssueClientCredentialsToken(aadauth.ClientCredentialsClaims{
		Issuer: "https://login/t/v2.0", Audience: aud, TenantID: "t", ClientID: "c",
	}, expiry)
	require.NoError(t, err)

	return tok
}
