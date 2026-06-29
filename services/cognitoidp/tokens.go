package cognitoidp

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"sync"
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

// signingCertValidityYears is the validity window of the generated signing certificate.
const signingCertValidityYears = 10

// tokenIssuer generates and signs JWTs for a user pool.
type tokenIssuer struct {
	certErr    error
	privateKey *rsa.PrivateKey
	keyID      string
	issuerURL  string
	certPEM    string
	certOnce   sync.Once
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
	Keys []JWK `json:"keys,omitempty"`
}

// JWK represents a JSON Web Key.
type JWK struct {
	Kty string `json:"kty,omitempty"`
	N   string `json:"n,omitempty"`
	E   string `json:"e,omitempty"`
	Kid string `json:"kid,omitempty"`
	Use string `json:"use,omitempty"`
	Alg string `json:"alg,omitempty"`
}

// PublicKeyForKID returns the RSA public key if kid matches this issuer's key ID.
func (t *tokenIssuer) PublicKeyForKID(kid string) (*rsa.PublicKey, bool) {
	if t.keyID != kid {
		return nil, false
	}

	return &t.privateKey.PublicKey, true
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

// SigningCertificatePEM returns a deterministic, PEM-encoded, self-signed X.509
// certificate that wraps this issuer's RSA public key. AWS Cognito returns such a
// certificate from GetSigningCertificate for clients that validate JWT signatures
// against an X.509 chain rather than the JWKS endpoint.
//
// The certificate is generated once and cached, so repeated calls for the same pool
// return the identical PEM. The template fields (serial number, validity window) are
// derived deterministically from the issuer key ID so the result is stable for the
// pool's lifetime and across snapshot restore (which preserves the RSA key + key ID).
func (t *tokenIssuer) SigningCertificatePEM() (string, error) {
	t.certOnce.Do(func() {
		t.certPEM, t.certErr = t.buildSigningCertificate()
	})

	return t.certPEM, t.certErr
}

func (t *tokenIssuer) buildSigningCertificate() (string, error) {
	// Derive a deterministic serial number from the key ID so the certificate is
	// stable for a given pool.
	digest := sha256.Sum256([]byte(t.keyID + "|" + t.issuerURL))
	serial := new(big.Int).SetBytes(digest[:])

	// Use a fixed validity window so the certificate bytes do not change between calls.
	notBefore := time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	notAfter := notBefore.AddDate(signingCertValidityYears, 0, 0)

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   t.issuerURL,
			Organization: []string{"gopherstack-cognito-idp"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &t.privateKey.PublicKey, t.privateKey)
	if err != nil {
		return "", fmt.Errorf("creating signing certificate: %w", err)
	}

	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	return string(pemBytes), nil
}

// TokenResult contains the three tokens returned on successful authentication.
type TokenResult struct {
	IDToken      string `json:"idToken,omitempty"`
	AccessToken  string `json:"accessToken,omitempty"`
	RefreshToken string `json:"refreshToken,omitempty"`
	ExpiresIn    int32  `json:"expiresIn,omitempty"`
}

// TokenParams holds the inputs for token issuance.
type TokenParams struct {
	Attributes        map[string]string `json:"attributes,omitempty"`
	ClientID          string            `json:"clientID,omitempty"`
	Username          string            `json:"username,omitempty"`
	UserSub           string            `json:"userSub,omitempty"`
	Scopes            []string          `json:"scopes,omitempty"`
	Groups            []string          `json:"groups,omitempty"`
	AuthTime          int64             `json:"authTime,omitempty"`
	AccessTokenExpiry time.Duration     `json:"accessTokenExpiry,omitempty"`
	IDTokenExpiry     time.Duration     `json:"idTokenExpiry,omitempty"`
}

// defaultAccessScope is the default scope on access tokens when the client has no configured scopes.
const defaultAccessScope = "aws.cognito.signin.user.admin"

// Issue generates ID, Access, and Refresh tokens for the given user.
//
//nolint:gocognit,cyclop,funlen // complexity matches JWT issuance contract with per-client expiry
func (t *tokenIssuer) Issue(p TokenParams) (*TokenResult, error) {
	now := time.Now()
	if p.AuthTime == 0 {
		p.AuthTime = now.Unix()
	}

	defaultExpiry := time.Duration(tokenExpirySeconds) * time.Second
	accessExpiry := defaultExpiry
	if p.AccessTokenExpiry > 0 {
		accessExpiry = p.AccessTokenExpiry
	}
	idExpiry := defaultExpiry
	if p.IDTokenExpiry > 0 {
		idExpiry = p.IDTokenExpiry
	}

	exp := now.Add(idExpiry)

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

	// Include standard user attributes in the ID token (email, phone_number, name, etc.)
	standardAttrs := []string{
		"email", "email_verified", "phone_number", "phone_number_verified",
		"name", "given_name", "family_name", "middle_name", "nickname",
		"preferred_username", "website", "zoneinfo", "locale", "birthdate",
		"gender", "address", "updated_at",
	}
	for _, attr := range standardAttrs {
		if val, ok := p.Attributes[attr]; ok {
			idClaims[attr] = val
		}
	}

	idToken := jwt.NewWithClaims(jwt.SigningMethodRS256, idClaims)
	idToken.Header["kid"] = t.keyID

	idTokenString, err := idToken.SignedString(t.privateKey)
	if err != nil {
		return nil, fmt.Errorf("signing ID token: %w", err)
	}

	scope := defaultAccessScope
	if len(p.Scopes) > 0 {
		deduped := make([]string, 0, len(p.Scopes))
		seen := make(map[string]struct{}, len(p.Scopes))

		for _, s := range p.Scopes {
			if s == "" {
				continue
			}

			if _, dup := seen[s]; dup {
				continue
			}

			seen[s] = struct{}{}
			deduped = append(deduped, s)
		}

		if len(deduped) > 0 {
			sort.Strings(deduped)
			scope = strings.Join(deduped, " ")
		}
	}

	accessClaims := jwt.MapClaims{
		"sub":       p.UserSub,
		"iss":       t.issuerURL,
		"client_id": p.ClientID,
		"token_use": "access",
		"username":  p.Username,
		"scope":     scope,
		"iat":       now.Unix(),
		"exp":       now.Add(accessExpiry).Unix(),
		"auth_time": p.AuthTime,
	}
	if len(p.Groups) > 0 {
		accessClaims["cognito:groups"] = p.Groups
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
		ExpiresIn:    int32(accessExpiry.Seconds()),
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

	// AWS Cognito stamps every token with a "token_use" claim ("access" or
	// "id"). Access-token operations (GetUser, GlobalSignOut, etc.) must reject
	// an ID token presented in place of an access token, otherwise an ID token
	// is silently accepted where an access token is required.
	if tu, _ := claims["token_use"].(string); tu != "access" {
		return nil, fmt.Errorf("%w: token is not an access token", ErrInvalidToken)
	}

	return claims, nil
}

// jwksResponseJSON serializes JWKSResponse as JSON bytes.
func jwksResponseJSON(r JWKSResponse) ([]byte, error) {
	return json.Marshal(r)
}
