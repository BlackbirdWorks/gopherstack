package gcpapigateway

import (
	"net/http"
	"sync/atomic"
	"time"

	"cloud.google.com/go/apigateway/apiv1/apigatewaypb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Resource states shared by Api, ApiConfig and Gateway (the enums use the same numbers).
const (
	stateCreating int32 = 1
	stateActive   int32 = 2
	stateFailed   int32 = 3
	stateDeleting int32 = 4
	stateUpdating int32 = 5
)

const defaultCreate = 2 * time.Second

// Timing controls how long create, update and delete operations take on the backend clock.
type Timing struct {
	Create time.Duration
	Update time.Duration
	Delete time.Duration
}

// DefaultTiming returns the operation durations used unless a test overrides them.
func DefaultTiming() Timing {
	return Timing{Create: defaultCreate, Update: time.Second, Delete: time.Second}
}

// life tracks the transient state of a resource on the backend clock.
type life struct {
	Until     time.Time `json:"until"`
	DeleteAt  time.Time `json:"deleteAt"`
	DeleteOp  string    `json:"deleteOp,omitempty"`
	Transient int32     `json:"transient"`
}

func (l *life) gone(now time.Time) bool { return !l.DeleteAt.IsZero() && !now.Before(l.DeleteAt) }

func (l *life) deleting() bool { return !l.DeleteAt.IsZero() }

func (l *life) state(now time.Time, final int32) int32 {
	switch {
	case l.deleting():
		return stateDeleting
	case now.Before(l.Until):
		return l.Transient
	default:
		return final
	}
}

type apiRec struct {
	pb     *apigatewaypb.Api
	policy *iampb.Policy
	life   life
}

type configRec struct {
	pb     *apigatewaypb.ApiConfig
	policy *iampb.Policy
	spec   *compiledSpec
	life   life
}

type gatewayRec struct {
	pb     *apigatewaypb.Gateway
	policy *iampb.Policy
	life   life
}

// InMemoryBackend holds all API Gateway state under one lock.
type InMemoryBackend struct {
	clock         atomic.Pointer[func() time.Time]
	client        *http.Client
	mu            *lockmetrics.RWMutex
	apis          map[string]*apiRec
	configs       map[string]*configRec
	gateways      map[string]*gatewayRec
	ops           map[string]*opRec
	quota         map[string]*quotaCounter
	jwks          *jwksCache
	timing        Timing
	allowExternal bool
	seq           uint64
}

// NewInMemoryBackend creates an empty backend on the wall clock.
func NewInMemoryBackend() *InMemoryBackend {
	b := &InMemoryBackend{
		mu:       lockmetrics.New("gcpapigateway"),
		apis:     map[string]*apiRec{},
		configs:  map[string]*configRec{},
		gateways: map[string]*gatewayRec{},
		ops:      map[string]*opRec{},
		quota:    map[string]*quotaCounter{},
		jwks:     newJWKSCache(),
		timing:   DefaultTiming(),
	}
	b.SetClock(time.Now)
	b.client = b.newBackendClient()

	return b
}

// SetClock replaces the clock lifecycle states, quotas and token validation derive from.
func (b *InMemoryBackend) SetClock(now func() time.Time) { b.clock.Store(&now) }

func (b *InMemoryBackend) now() time.Time { return (*b.clock.Load())().UTC() }

// SetTiming replaces the operation durations.
func (b *InMemoryBackend) SetTiming(t Timing) {
	b.mu.Lock("SetTiming")
	defer b.mu.Unlock()

	b.timing = t
}

// SetAllowExternal lets gateway backends and remote JWKS URIs reach hosts outside loopback.
func (b *InMemoryBackend) SetAllowExternal(allow bool) {
	b.mu.Lock("SetAllowExternal")
	defer b.mu.Unlock()

	b.allowExternal = allow
}

func (b *InMemoryBackend) externalAllowed() bool {
	b.mu.RLock("externalAllowed")
	defer b.mu.RUnlock()

	return b.allowExternal
}

func (b *InMemoryBackend) ts() *timestamppb.Timestamp { return timestamppb.New(b.now()) }

func clone[T proto.Message](m T) T {
	out, _ := proto.Clone(m).(T)

	return out
}

// sweepLocked drops resources whose delete operation has run its course.
func (b *InMemoryBackend) sweepLocked() {
	now := b.now()

	for k, r := range b.apis {
		if r.life.gone(now) {
			delete(b.apis, k)
		}
	}

	for k, r := range b.configs {
		if r.life.gone(now) {
			delete(b.configs, k)
		}
	}

	for k, r := range b.gateways {
		if r.life.gone(now) {
			delete(b.gateways, k)
		}
	}
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.apis, b.configs, b.gateways = map[string]*apiRec{}, map[string]*configRec{}, map[string]*gatewayRec{}
	b.ops, b.quota = map[string]*opRec{}, map[string]*quotaCounter{}
	b.jwks.reset()
}

// CloseIdleConnections releases pooled backend connections.
func (b *InMemoryBackend) CloseIdleConnections() { b.client.CloseIdleConnections() }
