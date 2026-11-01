package azuregraph

import (
	"crypto/rand"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
)

const (
	graphResource    = "https://graph.microsoft.com"
	inProcessTTL     = 5 * time.Minute
	inProcessKeySize = 32
)

// InProcessToken mints a short-lived bearer token this handler accepts, for callers in the same process that
// never see the listener. The token has no oid, so objects created with it get no owner.
func (h *Handler) InProcessToken() (string, error) {
	tenant := h.Backend.TenantID()

	if h.Settings.ValidateTokens && h.Issuer != nil {
		tok, err := h.Issuer.IssueClientCredentialsToken(aadauth.ClientCredentialsClaims{
			Audience: graphResource, TenantID: tenant, ClientID: h.Backend.cfg.DevClientID,
		}, int(inProcessTTL.Seconds()))
		if err != nil {
			return "", fmt.Errorf("azuregraph: in-process token: %w", err)
		}

		return tok, nil
	}

	key := make([]byte, inProcessKeySize)
	if _, err := rand.Read(key); err != nil {
		return "", fmt.Errorf("azuregraph: in-process token key: %w", err)
	}

	now := time.Now()

	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"aud": graphResource, "tid": tenant, "iat": now.Unix(), "exp": now.Add(inProcessTTL).Unix(),
	}).SignedString(key)
	if err != nil {
		return "", fmt.Errorf("azuregraph: in-process token: %w", err)
	}

	return tok, nil
}
