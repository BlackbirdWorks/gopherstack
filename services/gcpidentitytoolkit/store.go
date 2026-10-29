package gcpidentitytoolkit

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"sort"
	"strings"
	"time"

	idtkv2 "google.golang.org/api/identitytoolkit/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	idTokenTTL     = time.Hour
	rsaBits        = 2048
	keyValidity    = 20 * 365 * 24 * time.Hour
	maxUIDLen      = 128
	defaultPage    = 20
	maxPage        = 1000
	secretBytes    = 32
	oobIDBytes     = 16
	anonProvider   = "anonymous"
	passwordProv   = "password"
	phoneProv      = "phone"
	customProv     = "custom"
	rootTenantKey  = ""
	fakeAPIKey     = "fake-api-key" //nolint:gosec // placeholder shown in config, not a credential
	timeLayoutRFC  = time.RFC3339Nano
	redactedHash   = "UkVEQUNURUQ="
	maxClaimsBytes = 1000
	uidLen         = 28
)

type providerRec struct {
	ProviderID  string `json:"providerId"`
	RawID       string `json:"rawId"`
	Email       string `json:"email,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
	PhotoURL    string `json:"photoUrl,omitempty"`
	Phone       string `json:"phoneNumber,omitempty"`
	FederatedID string `json:"federatedId,omitempty"`
}

type userRec struct {
	Hash          *hashSpec     `json:"hash,omitempty"`
	UID           string        `json:"uid"`
	Email         string        `json:"email,omitempty"`
	DisplayName   string        `json:"displayName,omitempty"`
	PhotoURL      string        `json:"photoUrl,omitempty"`
	Phone         string        `json:"phoneNumber,omitempty"`
	PasswordHash  []byte        `json:"passwordHash,omitempty"`
	Salt          []byte        `json:"salt,omitempty"`
	CustomAttrs   string        `json:"customAttributes,omitempty"`
	Providers     []providerRec `json:"providers,omitempty"`
	CreatedAt     int64         `json:"createdAt"`
	LastLoginAt   int64         `json:"lastLoginAt,omitempty"`
	LastRefreshAt int64         `json:"lastRefreshAt,omitempty"`
	PasswordAt    int64         `json:"passwordUpdatedAt,omitempty"`
	ValidSince    int64         `json:"validSince,omitempty"`
	EmailVerified bool          `json:"emailVerified,omitempty"`
	Disabled      bool          `json:"disabled,omitempty"`
	Anonymous     bool          `json:"anonymous,omitempty"`
}

type oobRec struct {
	Email       string `json:"email"`
	NewEmail    string `json:"newEmail,omitempty"`
	RequestType string `json:"requestType"`
	TenantID    string `json:"tenantId,omitempty"`
	ContinueURL string `json:"continueUrl,omitempty"`
	Link        string `json:"link"`
	Created     int64  `json:"created"`
	Expires     int64  `json:"expires"`
}

type projectRec struct {
	Config  *idtkv2.GoogleCloudIdentitytoolkitAdminV2Config            `json:"config"`
	Tenants map[string]*idtkv2.GoogleCloudIdentitytoolkitAdminV2Tenant `json:"tenants"`
	Users   map[string]map[string]*userRec                             `json:"users"`
	Codes   map[string]*oobRec                                         `json:"codes"`
	Hash    hashSpec                                                   `json:"hash"`
}

type keyRec struct {
	KID    string `json:"kid"`
	Cert   string `json:"cert"`
	Priv   []byte `json:"priv"`
	Secret []byte `json:"secret"`
}

// CustomTokenKeys resolves the public keys that may verify a custom token issued by a service account email.
type CustomTokenKeys func(email string) []*rsa.PublicKey

// InMemoryBackend holds all Identity Platform state under one lock.
type InMemoryBackend struct {
	now            func() time.Time
	mu             *lockmetrics.RWMutex
	projects       map[string]*projectRec
	keys           *keyRec
	priv           *rsa.PrivateKey
	customKeys     CustomTokenKeys
	defaultProject string
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{mu: lockmetrics.New("gcpidentitytoolkit"), now: time.Now, defaultProject: DefaultProject}
	b.projects = map[string]*projectRec{}

	return b
}

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// SetCustomTokenKeys installs the resolver used to verify RS256 custom tokens.
func (b *InMemoryBackend) SetCustomTokenKeys(f CustomTokenKeys) {
	b.mu.Lock("SetCustomTokenKeys")
	defer b.mu.Unlock()

	b.customKeys = f
}

// Reset removes all state, including signing keys.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.projects = map[string]*projectRec{}
	b.keys, b.priv = nil, nil
}

func (b *InMemoryBackend) nowMillis() int64 { return b.now().UnixMilli() }

func defaultConfig(project string) *idtkv2.GoogleCloudIdentitytoolkitAdminV2Config {
	return &idtkv2.GoogleCloudIdentitytoolkitAdminV2Config{
		Name:              "projects/" + project + "/config",
		AuthorizedDomains: []string{"localhost"},
		SignIn: &idtkv2.GoogleCloudIdentitytoolkitAdminV2SignInConfig{
			Email:     &idtkv2.GoogleCloudIdentitytoolkitAdminV2Email{Enabled: true},
			Anonymous: &idtkv2.GoogleCloudIdentitytoolkitAdminV2Anonymous{Enabled: true},
		},
		EmailPrivacyConfig: &idtkv2.GoogleCloudIdentitytoolkitAdminV2EmailPrivacyConfig{
			EnableImprovedEmailPrivacy: true,
		},
		Mfa:    &idtkv2.GoogleCloudIdentitytoolkitAdminV2MultiFactorAuthConfig{State: "DISABLED"},
		Client: &idtkv2.GoogleCloudIdentitytoolkitAdminV2ClientConfig{ApiKey: fakeAPIKey},
		PasswordPolicyConfig: &idtkv2.GoogleCloudIdentitytoolkitAdminV2PasswordPolicyConfig{
			PasswordPolicyEnforcementState: "OFF",
		},
	}
}

// projectLocked returns the project record, creating it with defaults on first reference.
func (b *InMemoryBackend) projectLocked(project string) *projectRec {
	if p, ok := b.projects[project]; ok {
		return p
	}

	p := &projectRec{
		Config:  defaultConfig(project),
		Tenants: map[string]*idtkv2.GoogleCloudIdentitytoolkitAdminV2Tenant{},
		Users:   map[string]map[string]*userRec{rootTenantKey: {}},
		Codes:   map[string]*oobRec{},
		Hash:    newNativeSpec(),
	}
	b.projects[project] = p

	return p
}

// scope bundles what an operation needs about one project or tenant.
type scope struct {
	proj     *projectRec
	users    map[string]*userRec
	policy   *idtkv2.GoogleCloudIdentitytoolkitAdminV2PasswordPolicyConfig
	project  string
	tenant   string
	password bool
	link     bool
	anon     bool
	privacy  bool
}

func (b *InMemoryBackend) scopeLocked(project, tenant string) (*scope, error) {
	p := b.projectLocked(project)
	s := &scope{proj: p, project: project, tenant: tenant}

	if tenant == rootTenantKey {
		s.users = p.Users[rootTenantKey]
		in := p.Config.SignIn

		if in != nil && in.Email != nil {
			s.password = in.Email.Enabled
			s.link = in.Email.Enabled && !in.Email.PasswordRequired
		}

		s.anon = in != nil && in.Anonymous != nil && in.Anonymous.Enabled
		s.privacy = p.Config.EmailPrivacyConfig != nil && p.Config.EmailPrivacyConfig.EnableImprovedEmailPrivacy
		s.policy = p.Config.PasswordPolicyConfig

		return s, nil
	}

	t, ok := p.Tenants[tenant]
	if !ok {
		return nil, errNotFound("TENANT_NOT_FOUND")
	}

	if p.Users[tenant] == nil {
		p.Users[tenant] = map[string]*userRec{}
	}

	s.users = p.Users[tenant]
	s.password, s.link, s.anon = t.AllowPasswordSignup, t.EnableEmailLinkSignin, t.EnableAnonymousUser
	s.privacy = t.EmailPrivacyConfig != nil && t.EmailPrivacyConfig.EnableImprovedEmailPrivacy
	s.policy = t.PasswordPolicyConfig

	return s, nil
}

func (s *scope) byEmail(email string) *userRec {
	for _, u := range s.users {
		if u.Email != "" && strs.Equal(u.Email, email) {
			return u
		}
	}

	return nil
}

func (s *scope) byPhone(phone string) *userRec {
	for _, u := range s.users {
		if u.Phone != "" && u.Phone == phone {
			return u
		}
	}

	return nil
}

func (s *scope) byProvider(providerID, rawID string) *userRec {
	for _, u := range s.users {
		for _, p := range u.Providers {
			if p.ProviderID == providerID && p.RawID == rawID {
				return u
			}
		}
	}

	return nil
}

func (s *scope) sorted() []*userRec {
	out := make([]*userRec, 0, len(s.users))
	for _, u := range s.users {
		out = append(out, u)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt < out[j].CreatedAt
		}

		return out[i].UID < out[j].UID
	})

	return out
}

func newUID() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	raw := make([]byte, uidLen)
	_, _ = rand.Read(raw)

	for i, c := range raw {
		raw[i] = alphabet[int(c)%len(alphabet)]
	}

	return string(raw)
}

func randomToken(n int) string {
	raw := make([]byte, n)
	_, _ = rand.Read(raw)

	return strings.TrimRight(b64url(raw), "=")
}

// ensureKeysLocked creates the signing key pair and token secret on first use.
func (b *InMemoryBackend) ensureKeysLocked() (*rsa.PrivateKey, *keyRec, error) {
	if b.priv != nil && b.keys != nil {
		return b.priv, b.keys, nil
	}

	if b.keys != nil {
		parsed, err := x509.ParsePKCS8PrivateKey(b.keys.Priv)
		if err != nil {
			return nil, nil, err //nolint:wrapcheck // corrupt snapshot
		}

		priv, _ := parsed.(*rsa.PrivateKey)
		b.priv = priv

		return b.priv, b.keys, nil
	}

	priv, err := rsa.GenerateKey(rand.Reader, rsaBits)
	if err != nil {
		return nil, nil, err //nolint:wrapcheck // entropy failure
	}

	now := b.now()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "securetoken@system.gserviceaccount.com"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(keyValidity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err //nolint:wrapcheck // entropy failure
	}

	pkcs8, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, err //nolint:wrapcheck // unreachable for RSA
	}

	sum := sha256.Sum256(x509.MarshalPKCS1PublicKey(&priv.PublicKey))
	secret := make([]byte, secretBytes)
	_, _ = rand.Read(secret)

	b.priv = priv
	b.keys = &keyRec{
		KID:    hex.EncodeToString(sum[:20]),
		Cert:   string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		Priv:   pkcs8,
		Secret: secret,
	}

	return b.priv, b.keys, nil
}

func cloneJSON[T any](v *T) *T {
	if v == nil {
		return nil
	}

	data, err := json.Marshal(v)
	if err != nil {
		return v
	}

	out := new(T)
	if json.Unmarshal(data, out) != nil {
		return v
	}

	return out
}

// mergePatch applies a JSON merge patch (RFC 7386) to dst.
func mergePatch(dst, patch map[string]any) {
	for k, v := range patch {
		if v == nil {
			delete(dst, k)

			continue
		}

		pm, isMap := v.(map[string]any)
		dm, dIsMap := dst[k].(map[string]any)

		if isMap && dIsMap {
			mergePatch(dm, pm)

			continue
		}

		dst[k] = v
	}
}

func patched[T any](cur *T, patch map[string]any) (*T, error) {
	data, err := json.Marshal(cur)
	if err != nil {
		return nil, errInvalidArg("Invalid JSON payload received.")
	}

	var m map[string]any
	if err = json.Unmarshal(data, &m); err != nil {
		return nil, errInvalidArg("Invalid JSON payload received.")
	}

	mergePatch(m, patch)

	data, err = json.Marshal(m)
	if err != nil {
		return nil, errInvalidArg("Invalid JSON payload received.")
	}

	out := new(T)
	if err = json.Unmarshal(data, out); err != nil {
		return nil, errInvalidArg("Invalid JSON payload received: %v", err)
	}

	return out, nil
}
