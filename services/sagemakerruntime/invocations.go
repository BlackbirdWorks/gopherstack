package sagemakerruntime

import (
	"fmt"
	"time"
)

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

func cloneSession(session *Session) *Session {
	cp := *session

	return &cp
}
