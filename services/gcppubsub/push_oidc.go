package gcppubsub

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	oidcIssuer    = "https://accounts.google.com"
	oidcTokenTTL  = time.Hour
	oidcKeyBits   = 2048
	oidcIDDigits  = 21
	decimalRadix  = 10
	oidcKeyIDSize = 8
)

// ServiceAccountResolver maps a service account email to its numeric unique id.
type ServiceAccountResolver func(email string) (uniqueID string, ok bool)

type oidcSigner struct {
	key  *rsa.PrivateKey
	err  error
	kid  string
	once sync.Once
}

func (s *oidcSigner) init() {
	s.once.Do(func() {
		s.key, s.err = rsa.GenerateKey(rand.Reader, oidcKeyBits)
		if s.err != nil {
			return
		}

		sum := sha256.Sum256(s.key.N.Bytes())
		s.kid = hex.EncodeToString(sum[:oidcKeyIDSize])
	})
}

// SetServiceAccountResolver sets the lookup used for the sub and azp claims of push OIDC tokens.
func (b *InMemoryBackend) SetServiceAccountResolver(r ServiceAccountResolver) {
	b.mu.Lock("SetServiceAccountResolver")
	defer b.mu.Unlock()

	b.saResolver = r
}

func (b *InMemoryBackend) serviceAccountID(email string) string {
	b.mu.RLock("ServiceAccountID")
	resolve := b.saResolver
	b.mu.RUnlock()

	if resolve != nil {
		if id, ok := resolve(email); ok {
			return id
		}
	}

	sum := sha256.Sum256([]byte(email))
	n := new(big.Int).SetUint64(binary.BigEndian.Uint64(sum[:8]))

	return fmt.Sprintf("%0*s", oidcIDDigits, n.Text(decimalRadix))
}

// pushToken mints the RS256 JWT sent in the Authorization header of an authenticated push request.
func (b *InMemoryBackend) pushToken(email, audience, endpoint string, now time.Time) (string, error) {
	b.signer.init()

	if b.signer.err != nil {
		return "", b.signer.err
	}

	if audience == "" {
		audience = endpoint
	}

	id := b.serviceAccountID(email)
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss": oidcIssuer, "aud": audience, "sub": id, "azp": id, "email": email,
		"email_verified": true, "iat": now.Unix(), "exp": now.Add(oidcTokenTTL).Unix(),
	})
	tok.Header["kid"] = b.signer.kid

	return tok.SignedString(b.signer.key)
}

type jwk struct {
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// PushTokenKeySet returns the JWKS document holding the key that signs push OIDC tokens.
func (b *InMemoryBackend) PushTokenKeySet() ([]byte, error) {
	b.signer.init()

	if b.signer.err != nil {
		return nil, b.signer.err
	}

	pub := b.signer.key.PublicKey
	e := big.NewInt(int64(pub.E)).Bytes()

	return json.Marshal(map[string][]jwk{"keys": {{
		Kty: "RSA", Alg: "RS256", Use: "sig", Kid: b.signer.kid,
		N: base64.RawURLEncoding.EncodeToString(pub.N.Bytes()), E: base64.RawURLEncoding.EncodeToString(e),
	}}}, jsonx.V1())
}
