package bedrockagentcore

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	serviceName   = "bedrockagentcore"
	signingName   = "bedrock-agentcore"
	arnService    = "bedrock-agentcore"
	defaultRegion = "us-east-1"
)

// InMemoryBackend is the AgentCore data-plane state; one coarse lock guards every table.
type InMemoryBackend struct {
	registry *store.Registry
	events   *store.Table[Event]
	records  *store.Table[MemoryRecord]
	sessions *store.Table[ToolSession]
	tokens   *store.Table[AccessToken]
	mu       *lockmetrics.RWMutex
	control  atomic.Pointer[controlBox]
	clock    atomic.Pointer[clockBox]
	engine   *engine
	account  string
	region   string
	seq      uint64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	if region == "" {
		region = defaultRegion
	}

	b := &InMemoryBackend{
		registry: store.NewRegistry(),
		mu:       lockmetrics.New(serviceName),
		account:  accountID,
		region:   region,
	}

	b.engine = newEngine(b.nowT)
	b.SetClock(func() time.Time { return time.Now().UTC() })

	b.events = store.Register(b.registry, "events", store.New(func(e *Event) string {
		return eventKey(e.Account, e.Region, e.MemoryID, e.EventID)
	}))
	b.records = store.Register(b.registry, "memoryRecords", store.New(func(r *MemoryRecord) string {
		return recordKey(r.Account, r.Region, r.MemoryID, r.ID)
	}))
	b.sessions = store.Register(b.registry, "toolSessions", store.New(func(s *ToolSession) string {
		return sessionKey(s.Account, s.Region, s.Kind, s.ToolID, s.ID)
	}))
	b.tokens = store.Register(b.registry, "accessTokens", store.New(func(t *AccessToken) string { return t.Token }))

	return b
}

func eventKey(account, region, memory, id string) string {
	return account + "|" + region + "|" + memory + "|" + id
}

func recordKey(account, region, memory, id string) string {
	return account + "|" + region + "|" + memory + "|" + id
}

func sessionKey(account, region string, kind ToolKind, tool, id string) string {
	return account + "|" + region + "|" + string(kind) + "|" + tool + "|" + id
}

// SetControl wires the control plane the data plane resolves resources from.
func (b *InMemoryBackend) SetControl(c Control) { b.control.Store(&controlBox{c: c}) }

type controlBox struct{ c Control }

// SetClock replaces the time source, which drives idle expiry and token lifetimes.
func (b *InMemoryBackend) SetClock(now func() time.Time) { b.clock.Store(&clockBox{now: now}) }

type clockBox struct{ now func() time.Time }

func (b *InMemoryBackend) nowT() time.Time { return b.clock.Load().now() }

// Reset clears all state and stops every running runtime session and sandbox.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	b.registry.ResetAll()
	b.seq = 0
	b.mu.Unlock()

	b.engine.stopAll()
}

func (b *InMemoryBackend) scopeOf(ctx context.Context) Scope {
	sc := Scope{Account: awsmeta.Account(ctx), Region: awsmeta.Region(ctx)}
	if sc.Account == "" {
		sc.Account = b.account
	}

	if sc.Region == "" {
		sc.Region = b.region
	}

	return sc
}

func (b *InMemoryBackend) ctl() Control {
	if box := b.control.Load(); box != nil {
		return box.c
	}

	return nil
}
