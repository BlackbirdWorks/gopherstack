package glue

import (
	"fmt"
	"maps"
	"sort"
	"time"
)

func cloneSession(s *Session) *Session {
	cp := *s
	if s.DefaultArguments != nil {
		cp.DefaultArguments = make(map[string]string, len(s.DefaultArguments))
		maps.Copy(cp.DefaultArguments, s.DefaultArguments)
	}

	return &cp
}

func cloneStatement(st *Statement) *Statement {
	cp := *st

	return &cp
}

func (b *InMemoryBackend) CreateSession(
	id, role string,
	cmd SessionCommand,
	opts Session,
) (*Session, error) {
	b.mu.Lock("CreateSession")
	defer b.mu.Unlock()

	if b.sessions.Has(id) {
		return nil, fmt.Errorf("session %q already exists: %w", id, ErrAlreadyExists)
	}
	s := &Session{
		SessionID:        id,
		Role:             role,
		Command:          cmd,
		Status:           "PROVISIONING",
		CreatedOn:        float64(time.Now().Unix()),
		Timeout:          opts.Timeout,
		MaxCapacity:      opts.MaxCapacity,
		Description:      opts.Description,
		DefaultArguments: opts.DefaultArguments,
	}
	b.sessions.Put(s)
	b.sessionStatements[id] = nil

	return cloneSession(s), nil
}

func (b *InMemoryBackend) GetSession(id string) (*Session, error) {
	b.mu.RLock("GetSession")
	defer b.mu.RUnlock()

	s, ok := b.sessions.Get(id)
	if !ok {
		return nil, fmt.Errorf("session %q not found: %w", id, ErrNotFound)
	}

	return cloneSession(s), nil
}

func (b *InMemoryBackend) ListSessions() []*Session {
	b.mu.RLock("ListSessions")
	defer b.mu.RUnlock()

	src := b.sessions.All()
	out := make([]*Session, 0, len(src))
	for _, s := range src {
		out = append(out, cloneSession(s))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SessionID < out[j].SessionID })

	return out
}

// DeleteSession deletes a session. Its error switch (glue's deserializers.go)
// has no EntityNotFoundException case, unlike GetSession's, so an unknown Id
// surfaces as InvalidInputException.
func (b *InMemoryBackend) DeleteSession(id string) error {
	b.mu.Lock("DeleteSession")
	defer b.mu.Unlock()

	if !b.sessions.Has(id) {
		return fmt.Errorf("session %q not found: %w", id, ErrValidation)
	}
	b.sessions.Delete(id)
	delete(b.sessionStatements, id)

	return nil
}

// StopSession stops a session. Its error switch also has no
// EntityNotFoundException case.
func (b *InMemoryBackend) StopSession(id string) error {
	b.mu.Lock("StopSession")
	defer b.mu.Unlock()

	s, ok := b.sessions.Get(id)
	if !ok {
		return fmt.Errorf("session %q not found: %w", id, ErrValidation)
	}
	s.Status = stateStopping

	return nil
}

func (b *InMemoryBackend) RunStatement(sessionID, code string) (*Statement, error) {
	b.mu.Lock("RunStatement")
	defer b.mu.Unlock()

	if !b.sessions.Has(sessionID) {
		return nil, fmt.Errorf("session %q not found: %w", sessionID, ErrNotFound)
	}
	stmts := b.sessionStatements[sessionID]
	st := &Statement{
		Id:        int32(len(stmts) + 1), //nolint:gosec // statement count always small
		SessionId: sessionID,
		Code:      code,
		State:     "WAITING",
		StartedOn: float64(time.Now().Unix()),
	}
	b.sessionStatements[sessionID] = append(stmts, st)

	return cloneStatement(st), nil
}

func (b *InMemoryBackend) GetStatement(sessionID string, statementID int32) (*Statement, error) {
	b.mu.RLock("GetStatement")
	defer b.mu.RUnlock()

	stmts, ok := b.sessionStatements[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found: %w", sessionID, ErrNotFound)
	}
	for _, st := range stmts {
		if st.Id == statementID {
			return cloneStatement(st), nil
		}
	}

	return nil, fmt.Errorf(
		"statement %d not found in session %q: %w",
		statementID,
		sessionID,
		ErrNotFound,
	)
}

func (b *InMemoryBackend) GetStatements(sessionID string) ([]*Statement, error) {
	b.mu.RLock("GetStatements")
	defer b.mu.RUnlock()

	if !b.sessions.Has(sessionID) {
		return nil, fmt.Errorf("session %q not found: %w", sessionID, ErrNotFound)
	}
	stmts := b.sessionStatements[sessionID]
	out := make([]*Statement, len(stmts))
	for i, st := range stmts {
		out[i] = cloneStatement(st)
	}

	return out, nil
}

func (b *InMemoryBackend) CancelStatement(sessionID string, statementID int32) error {
	b.mu.Lock("CancelStatement")
	defer b.mu.Unlock()

	stmts, ok := b.sessionStatements[sessionID]
	if !ok {
		return fmt.Errorf("session %q not found: %w", sessionID, ErrNotFound)
	}
	for _, st := range stmts {
		if st.Id == statementID {
			st.State = "CANCELLING"

			return nil
		}
	}

	return fmt.Errorf(
		"statement %d not found in session %q: %w",
		statementID,
		sessionID,
		ErrNotFound,
	)
}
