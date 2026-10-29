package glue

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"time"
)

// sessionTransitionDelay is how long a session stays in PROVISIONING/STOPPING
// before the reconciler advances it, on the same scale as crawlerTransitionDelay.
const sessionTransitionDelay = 200 * time.Millisecond

func cloneSession(s *Session) *Session {
	cp := *s
	if s.DefaultArguments != nil {
		cp.DefaultArguments = make(map[string]string, len(s.DefaultArguments))
		maps.Copy(cp.DefaultArguments, s.DefaultArguments)
	}

	cp.Connections.Connections = slices.Clone(s.Connections.Connections)

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
	now := b.clockNow()

	b.mu.Lock("CreateSession")
	defer b.mu.Unlock()

	if b.sessions.Has(id) {
		return nil, fmt.Errorf("session %q already exists: %w", id, ErrAlreadyExists)
	}
	s := &Session{
		SessionID:        id,
		Role:             role,
		Command:          cmd,
		Status:           statusProvisioning,
		CreatedOn:        float64(now.Unix()),
		Timeout:          opts.Timeout,
		IdleTimeout:      opts.IdleTimeout,
		MaxCapacity:      opts.MaxCapacity,
		Description:      opts.Description,
		DefaultArguments: opts.DefaultArguments,
		Connections:      opts.Connections,
		GlueVersion:      opts.GlueVersion,
		WorkerType:       opts.WorkerType,

		SecurityConfiguration: opts.SecurityConfiguration,
		NumberOfWorkers:       opts.NumberOfWorkers,
		Tags:                  maps.Clone(opts.Tags),
	}
	b.sessions.Put(s)
	b.sessionStatements[id] = nil

	if b.sessionEngine != nil {
		b.launchSessionLocked(s, now)
	} else {
		b.sessionReadyAt[id] = now.Add(sessionTransitionDelay)
	}

	return cloneSession(s), nil
}

func (b *InMemoryBackend) GetSession(id string) (*Session, error) {
	b.advanceStates(b.clockNow())

	b.mu.RLock("GetSession")
	defer b.mu.RUnlock()

	s, ok := b.sessions.Get(id)
	if !ok {
		return nil, fmt.Errorf("session %q not found: %w", id, ErrNotFound)
	}

	return cloneSession(s), nil
}

func (b *InMemoryBackend) ListSessions() []*Session {
	b.advanceStates(b.clockNow())

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
	b.advanceStates(b.clockNow())

	b.mu.Lock("DeleteSession")
	defer b.mu.Unlock()

	if !b.sessions.Has(id) {
		return fmt.Errorf("session %q not found: %w", id, ErrValidation)
	}
	b.sessions.Delete(id)

	if ls, live := b.liveSessions[id]; live {
		delete(b.liveSessions, id)
		ls.cancel()
	}

	delete(b.sessionStatements, id)
	delete(b.sessionReadyAt, id)
	delete(b.sessionStopAt, id)

	return nil
}

// StopSession stops a session. Its error switch also has no
// EntityNotFoundException case.
func (b *InMemoryBackend) StopSession(id string) error {
	now := b.clockNow()

	b.advanceStates(now)

	b.mu.Lock("StopSession")
	defer b.mu.Unlock()

	s, ok := b.sessions.Get(id)
	if !ok {
		return fmt.Errorf("session %q not found: %w", id, ErrValidation)
	}
	s.Status = stateStopping
	delete(b.sessionReadyAt, id)

	if ls, live := b.liveSessions[id]; live {
		ls.cancel()

		return nil
	}

	b.sessionStopAt[id] = now.Add(sessionTransitionDelay)

	return nil
}

// RunStatement queues code on a READY session. Without a session engine the
// statement fails immediately with a message naming how to enable one.
func (b *InMemoryBackend) RunStatement(sessionID, code string) (*Statement, error) {
	b.advanceStates(b.clockNow())

	b.mu.Lock("RunStatement")
	defer b.mu.Unlock()

	s, ok := b.sessions.Get(sessionID)
	if !ok {
		return nil, fmt.Errorf("session %q not found: %w", sessionID, ErrNotFound)
	}

	if s.Status != stateReady {
		return nil, fmt.Errorf(
			"%w: session %q is not READY (status=%s)", ErrIllegalSessionState, sessionID, s.Status)
	}

	now := b.nowLocked()
	stmts := b.sessionStatements[sessionID]
	st := &Statement{
		Id:        int32(len(stmts) + 1), //nolint:gosec // statement count always small
		SessionId: sessionID,
		Code:      code,
		State:     stmtWaiting,
	}

	ls := b.liveSessions[sessionID]

	switch {
	case b.sessionEngine == nil:
		failStatement(st, "EngineNotEnabled", engineDisabledMessage, now)
	case ls == nil:
		s.Status = statusFailed
		s.ErrorMessage = "session interpreter is no longer running (lost across a restart)"

		return nil, fmt.Errorf("%w: session %q interpreter is gone", ErrIllegalSessionState, sessionID)
	default:
		ls.lastActivity = now

		select {
		case ls.wake <- struct{}{}:
		default:
		}
	}

	b.sessionStatements[sessionID] = append(stmts, st)

	return cloneStatement(st), nil
}

func failStatement(st *Statement, name, msg string, now time.Time) {
	st.State = stmtError
	st.CompletedOn = float64(now.Unix())
	st.Progress = 1
	st.Output = &StatementOutput{Status: stmtError, ErrorName: name, ErrorValue: msg}
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

// CancelStatement cancels a WAITING statement outright and interrupts a RUNNING one (CANCELLING, then CANCELLED).
func (b *InMemoryBackend) CancelStatement(sessionID string, statementID int32) error {
	proc, err := b.cancelStatementLocked(sessionID, statementID)
	if err != nil || proc == nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), sessionStopTimeout)
	defer cancel()

	return proc.Interrupt(ctx)
}

func (b *InMemoryBackend) cancelStatementLocked(sessionID string, statementID int32) (SessionProcess, error) {
	b.mu.Lock("CancelStatement")
	defer b.mu.Unlock()

	stmts, ok := b.sessionStatements[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found: %w", sessionID, ErrNotFound)
	}

	for _, st := range stmts {
		if st.Id != statementID {
			continue
		}

		switch st.State {
		case stmtWaiting:
			st.State = stmtCancelled
			st.CompletedOn = float64(b.nowLocked().Unix())
			st.Output = &StatementOutput{Status: stmtCancelled}
		case stmtRunning:
			st.State = stmtCancelling

			if ls := b.liveSessions[sessionID]; ls != nil {
				return ls.proc, nil
			}
		}

		return nil, nil //nolint:nilnil // nothing to interrupt
	}

	return nil, fmt.Errorf(
		"statement %d not found in session %q: %w",
		statementID,
		sessionID,
		ErrNotFound,
	)
}
