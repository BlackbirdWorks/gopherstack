package bedrockagentcorecontrol

import (
	"context"
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName   = "bedrockagentcorecontrol"
	signingName   = "bedrock-agentcore"
	arnService    = "bedrock-agentcore"
	defaultRegion = "us-east-1"
	vaultKey      = "default"

	managedSecretPrefix = "bedrock-agentcore-identity!default/"
	// managedSecretName is the Secrets Manager name prefix of stored secrets; "!" is not a legal name character.
	managedSecretName = "bedrock-agentcore-identity-default/"
)

// References resolves resources owned by other emulated services; nil members skip the check.
type References interface {
	ECRRepositoryExists(account, region, repository string) bool
	LambdaFunctionExists(arn string) bool
	SecretExists(arn string) bool
	ReadObject(bucket, key string) ([]byte, bool)
}

// SecretStore is the optional References extension that persists credential provider secrets in Secrets Manager.
type SecretStore interface {
	PutSecret(account, region, name, value string) (string, error)
	GetSecret(arn string) (string, bool)
	DeleteSecret(arn string)
}

// InMemoryBackend is the AgentCore control-plane state; one coarse lock guards every table.
type InMemoryBackend struct {
	resources *store.Table[Resource]
	policies  *store.Table[ResourcePolicy]
	vaults    *store.Table[TokenVault]
	registry  *store.Registry
	mu        *lockmetrics.RWMutex
	idem      *idempotency.Memo
	now       func() time.Time
	refs      References
	jwks      JWKSProvider
	outbound  *http.Client
	kinds     map[string]*kindSpec
	accountID string
	region    string
	sigSecret string
	dwell     time.Duration
	external  bool
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if region == "" {
		region = defaultRegion
	}

	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		idem:      idempotency.New(serviceName),
		now:       func() time.Time { return time.Now().UTC() },
		kinds:     map[string]*kindSpec{},
		accountID: accountID,
		region:    region,
	}

	b.outbound = newOutboundClient(b.externalAllowed, nil)

	for _, k := range kindTable() {
		b.kinds[k.Name] = k
	}

	b.resources = store.Register(b.registry, "resources", store.New(resourceKey))
	b.policies = store.Register(
		b.registry,
		"resourcePolicies",
		store.New(func(v *ResourcePolicy) string { return v.ARN }),
	)
	b.vaults = store.Register(b.registry, "tokenVaults", store.New(func(v *TokenVault) string { return v.Key }))

	return b
}

// Region returns the default Region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the default account.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// SetReferences wires lookups of resources owned by other services.
func (b *InMemoryBackend) SetReferences(r References) {
	b.mu.Lock("SetReferences")
	defer b.mu.Unlock()

	b.refs = r
}

// SetJWKSProvider resolves signing keys of issuers hosted by the emulator (Cognito user pools).
func (b *InMemoryBackend) SetJWKSProvider(p JWKSProvider) {
	b.mu.Lock("SetJWKSProvider")
	defer b.mu.Unlock()

	b.jwks = p
}

// SetSigV4Secret sets the secret AWS_IAM gateways verify request signatures against.
func (b *InMemoryBackend) SetSigV4Secret(secret string) {
	b.mu.Lock("SetSigV4Secret")
	defer b.mu.Unlock()

	b.sigSecret = secret
}

// SetAllowExternalTargets lets gateway targets and JWKS fetches reach hosts outside loopback.
func (b *InMemoryBackend) SetAllowExternalTargets(allow bool) {
	b.mu.Lock("SetAllowExternalTargets")
	defer b.mu.Unlock()

	b.external = allow
}

func (b *InMemoryBackend) externalAllowed() bool {
	b.mu.RLock("externalAllowed")
	defer b.mu.RUnlock()

	return b.external
}

// SetLifecycleDelay keeps resources in CREATING, UPDATING and DELETING for d; zero settles immediately.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.dwell = d
}

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.idem.Clear()
}

func (b *InMemoryBackend) callerAccount(ctx context.Context) string {
	if a := awsmeta.Account(ctx); a != "" {
		return a
	}

	return b.accountID
}

func (b *InMemoryBackend) callerRegion(ctx context.Context) string {
	if r := awsmeta.Region(ctx); r != "" {
		return r
	}

	return b.region
}

// scope identifies the account and Region a request operates in.
type scope struct{ account, region string }

func (b *InMemoryBackend) scopeOf(ctx context.Context) scope {
	return scope{account: b.callerAccount(ctx), region: b.callerRegion(ctx)}
}
