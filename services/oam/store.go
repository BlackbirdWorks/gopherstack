package oam

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName       = "oam"
	defaultPageSize   = 50
	maxPageSize       = 50
	maxSinksPerRegion = 1
)

// AccountInfo describes an account for link labels and sink policy conditions.
type AccountInfo struct {
	Name  string
	Email string
	OrgID string
}

// AccountDirectory resolves account details from the organization, when one is wired.
type AccountDirectory interface {
	AccountInfo(accountID string) (AccountInfo, bool)
}

// InMemoryBackend is the OAM state; one coarse lock guards both tables.
type InMemoryBackend struct {
	sinks     *store.Table[Sink]
	links     *store.Table[Link]
	registry  *store.Registry
	mu        *lockmetrics.RWMutex
	accounts  AccountDirectory
	accountID string
	region    string
	seq       int64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:  store.NewRegistry(),
		mu:        lockmetrics.New(serviceName),
		accountID: accountID,
		region:    region,
	}

	b.sinks = store.Register(b.registry, "sinks", store.New(func(v *Sink) string { return v.ARN }))
	b.links = store.Register(b.registry, "links", store.New(func(v *Link) string { return v.ARN }))

	return b
}

// Region returns the default region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the default account.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// SetAccountDirectory wires the Organizations lookup.
func (b *InMemoryBackend) SetAccountDirectory(d AccountDirectory) {
	b.mu.Lock("SetAccountDirectory")
	defer b.mu.Unlock()

	b.accounts = d
}

// Reset clears every table.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.seq = 0
}

func (b *InMemoryBackend) nextSeqLocked() int64 {
	b.seq++

	return b.seq
}

type scope struct{ account, region string }

func (b *InMemoryBackend) scopeOf(ctx context.Context) scope {
	s := scope{account: awsmeta.Account(ctx), region: awsmeta.Region(ctx)}
	if s.account == "" {
		s.account = b.accountID
	}

	if s.region == "" {
		s.region = b.region
	}

	return s
}
