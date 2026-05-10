package cognitoidp

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// rsaKeyBits is the size of the RSA key generated for JWT signing.
const rsaKeyBits = 2048

// keyIDLen is the byte length of the random key ID.
const keyIDLen = 8

// refreshTokenLen is the byte length of the random refresh token.
const refreshTokenLen = 32

// confirmCodeLen is the length of the random confirmation code generated on SignUp.
const confirmCodeLen = 6

// tokenExpirySeconds is the lifetime in seconds for ID and access tokens.
const tokenExpirySeconds = 3600

// tokenIssuer generates and signs JWTs for a user pool.
type tokenIssuer struct {
	privateKey *rsa.PrivateKey
	keyID      string
	issuerURL  string
}

// newTokenIssuer generates a stable RSA-2048 keypair for this user pool.
func newTokenIssuer(issuerURL string) (*tokenIssuer, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		return nil, fmt.Errorf("generating RSA key: %w", err)
	}

	kidBytes := make([]byte, keyIDLen)
	if _, err = rand.Read(kidBytes); err != nil {
		return nil, fmt.Errorf("generating key ID: %w", err)
	}

	return &tokenIssuer{
		privateKey: privateKey,
		keyID:      base64.RawURLEncoding.EncodeToString(kidBytes),
		issuerURL:  issuerURL,
	}, nil
}

// newTokenIssuerFromKey reconstructs a tokenIssuer from an existing key, keyID and issuerURL.
// This is used to restore persisted user pool state.
func newTokenIssuerFromKey(privateKey *rsa.PrivateKey, keyID, issuerURL string) *tokenIssuer {
	return &tokenIssuer{
		privateKey: privateKey,
		keyID:      keyID,
		issuerURL:  issuerURL,
	}
}

type JWKSResponse struct {
	Keys []JWK `json:"keys"`
}

// JWK represents a JSON Web Key.
type JWK struct {
	Kty string `json:"kty"`
	N   string `json:"n"`
	E   string `json:"e"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
}

// JWKS returns the JSON Web Key Set for this token issuer.
func (t *tokenIssuer) JWKS() JWKSResponse {
	pub := &t.privateKey.PublicKey
	n := base64.RawURLEncoding.EncodeToString(pub.N.Bytes())
	eBytes := big.NewInt(int64(pub.E)).Bytes()
	e := base64.RawURLEncoding.EncodeToString(eBytes)

	return JWKSResponse{
		Keys: []JWK{{
			Kty: "RSA",
			N:   n,
			E:   e,
			Kid: t.keyID,
			Use: "sig",
			Alg: "RS256",
		}},
	}
}

// TokenResult contains the three tokens returned on successful authentication.
type TokenResult struct {
	IDToken      string
	AccessToken  string
	RefreshToken string
	ExpiresIn    int32
}

// TokenParams holds the inputs for token issuance.
type TokenParams struct {
	// Scopes overrides the default access-token scope when non-empty.
	Scopes []string
	// Groups is included as cognito:groups in the ID token when non-empty.
	Groups   []string
	ClientID string
	Username string
	UserSub  string
	// AuthTime is the Unix timestamp when the authentication event occurred.
	// If zero, the current time is used.
	AuthTime int64
}

// defaultAccessScope is the default scope on access tokens when the client has no configured scopes.
const defaultAccessScope = "aws.cognito.signin.user.admin"

// Issue generates ID, Access, and Refresh tokens for the given user.
func (t *tokenIssuer) Issue(p TokenParams) (*TokenResult, error) {
	now := time.Now()
	if p.AuthTime == 0 {
		p.AuthTime = now.Unix()
	}
	exp := now.Add(time.Duration(tokenExpirySeconds) * time.Second)

	idClaims := jwt.MapClaims{
		"sub":              p.UserSub,
		"iss":              t.issuerURL,
		"aud":              p.ClientID,
		"token_use":        "id",
		"cognito:username": p.Username,
		"iat":              now.Unix(),
		"exp":              exp.Unix(),
		"auth_time":        p.AuthTime,
	}
	if len(p.Groups) > 0 {
		idClaims["cognito:groups"] = p.Groups
	}

	idToken := jwt.NewWithClaims(jwt.SigningMethodRS256, idClaims)
	idToken.Header["kid"] = t.keyID

	idTokenString, err := idToken.SignedString(t.privateKey)
	if err != nil {
		return nil, fmt.Errorf("signing ID token: %w", err)
	}

	scope := defaultAccessScope
	if len(p.Scopes) > 0 {
		scope = strings.Join(p.Scopes, " ")
	}

	accessClaims := jwt.MapClaims{
		"sub":       p.UserSub,
		"iss":       t.issuerURL,
		"client_id": p.ClientID,
		"token_use": "access",
		"username":  p.Username,
		"scope":     scope,
		"iat":       now.Unix(),
		"exp":       exp.Unix(),
		"auth_time": p.AuthTime,
	}

	accessToken := jwt.NewWithClaims(jwt.SigningMethodRS256, accessClaims)
	accessToken.Header["kid"] = t.keyID

	accessTokenString, err := accessToken.SignedString(t.privateKey)
	if err != nil {
		return nil, fmt.Errorf("signing access token: %w", err)
	}

	refreshBytes := make([]byte, refreshTokenLen)
	if _, err = rand.Read(refreshBytes); err != nil {
		return nil, fmt.Errorf("generating refresh token: %w", err)
	}

	refreshTokenString := base64.RawURLEncoding.EncodeToString(refreshBytes)

	return &TokenResult{
		IDToken:      idTokenString,
		AccessToken:  accessTokenString,
		RefreshToken: refreshTokenString,
		ExpiresIn:    tokenExpirySeconds,
	}, nil
}

// ParseAccessToken validates and parses an access token, returning its claims.
func (t *tokenIssuer) ParseAccessToken(tokenString string) (jwt.MapClaims, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("%w: unexpected signing method", ErrInvalidToken)
		}

		return &t.privateKey.PublicKey, nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		return nil, fmt.Errorf("%w: token claims are not valid", ErrInvalidToken)
	}

	return claims, nil
}

// jwksResponseJSON serializes JWKSResponse as JSON bytes.
func jwksResponseJSON(r JWKSResponse) ([]byte, error) {
	return json.Marshal(r)
}
