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

// SessionTouchOutcome reports what TouchSession did to the named session.
type SessionTouchOutcome struct {
	// ClosedSessionID is set to the session's ID when TouchSession found the
	// session expired and evicted it, mirroring the real AWS
	// InvokeEndpointOutput.ClosedSessionId behaviour. It is empty when the
	// session was touched normally or was not found at all.
	ClosedSessionID string
}

// TouchSession marks an existing stateful session as invoked, or -- if the
// session has passed its ExpiresAt -- evicts it and reports the closure via
// SessionTouchOutcome.ClosedSessionID (see setSessionResponseHeader, which
// surfaces this as the X-Amzn-SageMaker-Closed-Session-Id response header).
// A sessionID that does not match any tracked session is a silent no-op,
// matching this backend's pre-existing behaviour for unrecognised session
// IDs.
func (b *InMemoryBackend) TouchSession(sessionID string) SessionTouchOutcome {
	b.mu.Lock("TouchSession")
	defer b.mu.Unlock()

	session, ok := b.sessions.Get(sessionID)
	if !ok {
		return SessionTouchOutcome{}
	}

	now := time.Now().UTC()
	if now.After(session.ExpiresAt) {
		b.sessions.Delete(sessionID)

		return SessionTouchOutcome{ClosedSessionID: sessionID}
	}

	session.LastInvokedAt = now

	return SessionTouchOutcome{}
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
