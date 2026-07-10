package sagemakerruntime

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// maxInvocationHistory is the maximum number of invocations retained in memory.
const maxInvocationHistory = 1000

// MaxInvocationHistory is the exported value for testing.
const MaxInvocationHistory = maxInvocationHistory

// maxSessions bounds the number of stateful endpoint sessions retained in memory.
// InvokeEndpoint with a NEW session header is a per-request hot path; without a
// cap the sessions map would grow without bound in a long-running emulator. When
// the cap is exceeded the oldest session (by CreatedAt) is evicted FIFO-style.
const maxSessions = 1000

// MaxSessions is the exported value for testing.
const MaxSessions = maxSessions

// maxAsyncInvocations bounds the number of accepted async inference records
// retained in memory. RecordAsyncInvocation runs on every InvokeEndpointAsync
// request; without a cap the map would grow without bound. When the cap is
// exceeded the oldest record (by CreatedAt) is evicted FIFO-style.
const maxAsyncInvocations = 1000

// MaxAsyncInvocations is the exported value for testing.
const MaxAsyncInvocations = maxAsyncInvocations

const sessionDuration = 5 * time.Minute

// Invocation records a single SageMaker Runtime endpoint invocation.
type Invocation struct {
	CreatedAt    time.Time `json:"createdAt"`
	EndpointName string    `json:"endpointName"`
	Operation    string    `json:"operation"`
	Input        string    `json:"input"`
	Output       string    `json:"output"`
}

// Session records a stateful endpoint session established by InvokeEndpoint.
type Session struct {
	CreatedAt     time.Time `json:"createdAt"`
	LastInvokedAt time.Time `json:"lastInvokedAt"`
	ExpiresAt     time.Time `json:"expiresAt"`
	ID            string    `json:"id"`
	EndpointName  string    `json:"endpointName"`
}

// AsyncInvocation records accepted asynchronous inference work.
type AsyncInvocation struct {
	CreatedAt      time.Time `json:"createdAt"`
	InferenceID    string    `json:"inferenceId"`
	EndpointName   string    `json:"endpointName"`
	Input          string    `json:"input"`
	OutputLocation string    `json:"outputLocation"`
}

// InMemoryBackend stores SageMaker Runtime state in memory.
type InMemoryBackend struct {
	// registry holds every store.Table-backed resource field so their
	// Reset/Snapshot/Restore collapse to one call each -- see
	// store_setup.go's file doc comment for why sessions and
	// asyncInvocations are both "clean" tables (registered directly, no
	// DTO-registry needed).
	registry         *store.Registry
	mu               *lockmetrics.RWMutex
	sessions         *store.Table[Session]
	asyncInvocations *store.Table[AsyncInvocation]
	accountID        string
	region           string
	invocations      []*Invocation
	nextID           uint64
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:    store.NewRegistry(),
		invocations: make([]*Invocation, 0),
		accountID:   accountID,
		region:      region,
		mu:          lockmetrics.New("sagemakerruntime"),
	}
	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// RecordInvocation stores a completed invocation in memory.
func (b *InMemoryBackend) RecordInvocation(operation, endpointName, input, output string) *Invocation {
	b.mu.Lock("RecordInvocation")
	defer b.mu.Unlock()

	inv := &Invocation{
		Operation:    operation,
		EndpointName: endpointName,
		Input:        input,
		Output:       output,
		CreatedAt:    time.Now().UTC(),
	}
	b.invocations = append(b.invocations, inv)

	if len(b.invocations) > maxInvocationHistory {
		b.invocations = b.invocations[len(b.invocations)-maxInvocationHistory:]
	}

	cp := *inv

	return &cp
}

// ListInvocations returns all recorded invocations.
func (b *InMemoryBackend) ListInvocations() []*Invocation {
	b.mu.RLock("ListInvocations")
	defer b.mu.RUnlock()

	out := make([]*Invocation, 0, len(b.invocations))

	for _, inv := range b.invocations {
		cp := *inv
		out = append(out, &cp)
	}

	return out
}

// StartSession creates stateful invocation session metadata.
func (b *InMemoryBackend) StartSession(endpointName string) *Session {
	b.mu.Lock("StartSession")
	defer b.mu.Unlock()

	b.nextID++
	now := time.Now().UTC()
	session := &Session{
		ID:            fmt.Sprintf("gopherstack-session-%d", b.nextID),
		EndpointName:  endpointName,
		CreatedAt:     now,
		LastInvokedAt: now,
		ExpiresAt:     now.Add(sessionDuration),
	}
	b.sessions.Put(session)
	evictOldest(b.sessions, maxSessions, sessionKeyFn, func(s *Session) time.Time { return s.CreatedAt })

	return cloneSession(session)
}

// TouchSession marks an existing stateful session as invoked.
func (b *InMemoryBackend) TouchSession(sessionID string) {
	b.mu.Lock("TouchSession")
	defer b.mu.Unlock()

	if session, ok := b.sessions.Get(sessionID); ok {
		session.LastInvokedAt = time.Now().UTC()
	}
}

// ListSessions returns all active endpoint sessions.
func (b *InMemoryBackend) ListSessions() []*Session {
	b.mu.RLock("ListSessions")
	defer b.mu.RUnlock()

	all := b.sessions.All()
	out := make([]*Session, 0, len(all))

	for _, session := range all {
		out = append(out, cloneSession(session))
	}

	return out
}

// RecordAsyncInvocation stores accepted asynchronous inference work.
// If outputLocation is empty a fake S3 location is synthesised.
func (b *InMemoryBackend) RecordAsyncInvocation(
	endpointName, requestedID, input, outputLocation string,
) *AsyncInvocation {
	b.mu.Lock("RecordAsyncInvocation")
	defer b.mu.Unlock()

	inferenceID := requestedID
	if inferenceID == "" {
		b.nextID++
		inferenceID = fmt.Sprintf("gopherstack-inference-%d", b.nextID)
	}

	loc := outputLocation
	if loc == "" {
		loc = fmt.Sprintf("s3://sagemaker-runtime-mock/%s/%s/output", endpointName, inferenceID)
	}

	invocation := &AsyncInvocation{
		InferenceID:    inferenceID,
		EndpointName:   endpointName,
		Input:          input,
		OutputLocation: loc,
		CreatedAt:      time.Now().UTC(),
	}
	b.asyncInvocations.Put(invocation)
	evictOldest(
		b.asyncInvocations, maxAsyncInvocations, asyncInvocationKeyFn,
		func(a *AsyncInvocation) time.Time { return a.CreatedAt },
	)

	return cloneAsyncInvocation(invocation)
}

// ListAsyncInvocations returns accepted asynchronous inference work.
func (b *InMemoryBackend) ListAsyncInvocations() []*AsyncInvocation {
	b.mu.RLock("ListAsyncInvocations")
	defer b.mu.RUnlock()

	all := b.asyncInvocations.All()
	out := make([]*AsyncInvocation, 0, len(all))

	for _, invocation := range all {
		out = append(out, cloneAsyncInvocation(invocation))
	}

	return out
}

// evictOldest enforces a FIFO size bound on t: while the table exceeds
// maxSize, it repeatedly removes the entry with the oldest timestamp (as
// reported by createdAt), identified via idFn. This keeps long-running
// sessions/async-invocation tables from growing without bound.
func evictOldest[V any](t *store.Table[V], maxSize int, idFn func(*V) string, createdAt func(*V) time.Time) {
	for t.Len() > maxSize {
		var (
			oldestID   string
			oldestTime time.Time
			found      bool
		)

		for _, v := range t.All() {
			ts := createdAt(v)
			if !found || ts.Before(oldestTime) {
				oldestID = idFn(v)
				oldestTime = ts
				found = true
			}
		}

		if !found {
			return
		}

		t.Delete(oldestID)
	}
}

func cloneSession(session *Session) *Session {
	cp := *session

	return &cp
}

func cloneAsyncInvocation(invocation *AsyncInvocation) *AsyncInvocation {
	cp := *invocation

	return &cp
}
