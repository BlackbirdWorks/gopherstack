package ssooidc

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	grantDeviceCode   = "urn:ietf:params:oauth:grant-type:device_code"
	grantAuthCode     = "authorization_code"
	grantRefreshToken = "refresh_token"

	clientTypePublic = "public"
	schemeHTTPS      = "https"

	clientSecretTTL   = 90 * 24 * time.Hour
	deviceTTL         = 600 * time.Second
	authCodeTTL       = 10 * time.Minute
	accessTokenTTL    = 8 * time.Hour
	refreshTokenTTL   = 90 * 24 * time.Hour
	deviceInterval    = 1
	slowDownIncrement = 5
	tokenTypeBearer   = "Bearer"

	clientIDBytes     = 16
	clientSecretBytes = 48
	deviceCodeBytes   = 24
	tokenBytes        = 48
	sessionIDBytes    = 12
	userCodeHalf      = 4
	userCodeLen       = 2 * userCodeHalf
	userCodeAlphabet  = "BCDFGHJKLMNPQRSTVWXZ"
)

// InMemoryBackend holds every OIDC client, in-flight authorization and issued token.
type InMemoryBackend struct {
	clients  *store.Table[Client]
	devices  *store.Table[Device]
	codes    *store.Table[AuthCode]
	tokens   *store.Table[Token]
	registry *store.Registry
	mu       *lockmetrics.RWMutex
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		registry: store.NewRegistry(),
		mu:       lockmetrics.New("ssooidc"),
	}
	registerAllTables(b)

	return b
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
}

func randomString(n int) string {
	buf := make([]byte, n)
	_, _ = rand.Read(buf)

	return base64.RawURLEncoding.EncodeToString(buf)
}

func newUserCode() string {
	buf := make([]byte, userCodeLen)
	_, _ = rand.Read(buf)

	var sb strings.Builder

	for i, v := range buf {
		if i == userCodeHalf {
			sb.WriteByte('-')
		}

		sb.WriteByte(userCodeAlphabet[int(v)%len(userCodeAlphabet)])
	}

	return sb.String()
}

func normalizeUserCode(code string) string {
	code = strings.ToUpper(strings.TrimSpace(code))
	if len(code) == userCodeLen && !strings.Contains(code, "-") {
		code = code[:userCodeHalf] + "-" + code[userCodeHalf:]
	}

	return code
}

// pruneLocked drops expired devices, codes and tokens.
func (b *InMemoryBackend) pruneLocked(now time.Time) {
	for _, d := range b.devices.All() {
		if now.After(d.ExpiresAt) {
			b.devices.Delete(d.DeviceCode)
		}
	}

	for _, c := range b.codes.All() {
		if now.After(c.ExpiresAt) {
			b.codes.Delete(c.Code)
		}
	}

	for _, t := range b.tokens.All() {
		if now.After(t.ExpiresAt) {
			b.tokens.Delete(t.Token)
		}
	}
}
