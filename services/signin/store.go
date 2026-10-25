package signin

import (
	"context"
	"crypto/rand"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const signingKeyBytes = 32

// InMemoryBackend is the in-memory Sign-In backend. One coarse lock guards all tables.
type InMemoryBackend struct {
	mu             *lockmetrics.RWMutex
	registry       *store.Registry
	authCodes      *store.Table[AuthCode]
	sessions       *store.Table[Session]
	accessTokens   *store.Table[AccessToken]
	consoleConfigs *store.Table[ConsoleConfig]
	statements     *store.Table[PermissionStatement]
	now            func() time.Time
	accountID      string
	region         string
	signingKey     []byte
}

// NewInMemoryBackend creates an empty backend with a fresh token signing key.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		mu:         lockmetrics.New("signin"),
		registry:   store.NewRegistry(),
		now:        time.Now,
		accountID:  accountID,
		region:     region,
		signingKey: make([]byte, signingKeyBytes),
	}

	_, _ = rand.Read(b.signingKey)

	b.authCodes = store.Register(b.registry, "authCodes", store.New(func(v *AuthCode) string { return v.Code }))
	b.sessions = store.Register(b.registry, "sessions", store.New(func(v *Session) string { return v.RefreshToken }))
	b.accessTokens = store.Register(b.registry, "accessTokens", store.New(func(v *AccessToken) string { return v.JTI }))
	b.consoleConfigs = store.Register(
		b.registry, "consoleConfigs", store.New(func(v *ConsoleConfig) string { return v.TargetID }),
	)
	b.statements = store.Register(
		b.registry, "statements", store.New(func(v *PermissionStatement) string { return v.Key }),
	)

	return b
}

// Region returns the default region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all state; the signing key is kept so outstanding JWTs stay verifiable.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()
	b.registry.ResetAll()
}

// SetClock replaces the time source (tests).
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()
	b.now = now
}

// caller is the resolved identity behind a request.
type caller struct {
	account string
	region  string
	arn     string
	userID  string
	anon    bool
}

func (b *InMemoryBackend) callerOf(ctx context.Context) caller {
	m := awsmeta.Get(ctx)

	account := m.Account
	if account == "" {
		account = b.accountID
	}

	region := m.Region
	if region == "" {
		region = b.region
	}

	c := caller{account: account, region: region}

	switch p := m.Principal; {
	case p == nil:
		c.arn = "arn:" + awsmeta.DefaultPartition + ":iam::" + account + ":root"
		c.userID = account
	case p.Kind == awsmeta.PrincipalKindAnonymous:
		c.anon = true
	default:
		c.arn = p.Arn
		c.userID = p.UserID

		if p.AccountID != "" {
			c.account = p.AccountID
		}

		if c.userID == "" {
			c.userID = c.account
		}
	}

	return c
}

// ResolvePrincipal resolves credentials issued by CreateOAuth2Token to the principal that signed in.
func (b *InMemoryBackend) ResolvePrincipal(_ context.Context, accessKeyID, sessionToken string) (*awsmeta.Principal, bool) {
	if accessKeyID == "" {
		return nil, false
	}

	b.mu.RLock("ResolvePrincipal")
	defer b.mu.RUnlock()

	now := b.now()

	for _, s := range b.sessions.All() {
		if s.AccessKeyID != accessKeyID || s.SessionToken != sessionToken || s.Revoked ||
			!now.Before(s.CredsExpireAt) {
			continue
		}

		return &awsmeta.Principal{
			Kind: awsmeta.PrincipalKindUser, Arn: s.PrincipalArn, AccountID: s.Account, UserID: s.UserID,
		}, true
	}

	return nil, false
}
