package cognitoidp

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// rsaKeyBits is the size of the RSA key generated for JWT signing.
const rsaKeyBits = 2048

// keyIDLen is the byte length of the random key ID.
const keyIDLen = 8

// refreshTokenLen is the byte length of the random refresh token.
const refreshTokenLen = 32

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

// JWKSResponse is the JSON Web Key Set response.
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

// Issue generates ID, Access, and Refresh tokens for the given user.
func (t *tokenIssuer) Issue(clientID, username, userSub string) (*TokenResult, error) {
	now := time.Now()
	exp := now.Add(time.Hour)

	idClaims := jwt.MapClaims{
		"sub":              userSub,
		"iss":              t.issuerURL,
		"aud":              clientID,
		"token_use":        "id",
		"cognito:username": username,
		"iat":              now.Unix(),
		"exp":              exp.Unix(),
	}
	idToken := jwt.NewWithClaims(jwt.SigningMethodRS256, idClaims)
	idToken.Header["kid"] = t.keyID
	idTokenString, err := idToken.SignedString(t.privateKey)
	if err != nil {
		return nil, fmt.Errorf("signing ID token: %w", err)
	}

	accessClaims := jwt.MapClaims{
		"sub":       userSub,
		"iss":       t.issuerURL,
		"client_id": clientID,
		"token_use": "access",
		"username":  username,
		"scope":     "aws.cognito.signin.user.admin",
		"iat":       now.Unix(),
		"exp":       exp.Unix(),
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

// jwksResponseJSON serializes JWKSResponse as JSON bytes.
func jwksResponseJSON(r JWKSResponse) ([]byte, error) {
	return json.Marshal(r)
}
