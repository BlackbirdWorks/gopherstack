package gremlin

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

// DefaultSessionLifetime is the longest a session may stay open before it is rolled back.
const DefaultSessionLifetime = 10 * time.Minute

type session struct {
	timer    *time.Timer
	tx       *opencypher.Tx
	busy     chan struct{}
	bytecode bool
	gone     bool
}

type sessionTable struct {
	mu       *lockmetrics.RWMutex
	m        map[string]*session
	graph    *opencypher.Graph
	lifetime time.Duration
}

func newSessionTable(g *opencypher.Graph, lifetime time.Duration) *sessionTable {
	if lifetime <= 0 {
		lifetime = DefaultSessionLifetime
	}

	return &sessionTable{
		mu: lockmetrics.New("neptune-gremlin-sessions"), m: map[string]*session{}, graph: g, lifetime: lifetime,
	}
}

// acquire returns the session with exclusive use, creating it when create is set; nil means it does not exist.
func (st *sessionTable) acquire(ctx context.Context, id string, create, bytecode bool) (*session, error) {
	for {
		st.mu.Lock("acquire")

		s := st.m[id]
		if s == nil && create {
			s = &session{busy: make(chan struct{}, 1), bytecode: bytecode}
			s.timer = time.AfterFunc(st.lifetime, func() { st.expire(id, s) })
			st.m[id] = s
		}

		st.mu.Unlock()

		if s == nil {
			return nil, nil //nolint:nilnil // unknown session
		}

		select {
		case s.busy <- struct{}{}:
		case <-ctx.Done():
			return nil, ctxError(ctx.Err())
		}

		if !s.gone {
			return s, nil
		}

		<-s.busy

		if !create {
			return nil, nil //nolint:nilnil // expired while waiting
		}
	}
}

func (st *sessionTable) release(s *session) { <-s.busy }

// end rolls back or commits any open transaction and removes the session. The caller holds s.busy.
func (st *sessionTable) end(id string, s *session, commit bool) {
	if s.tx != nil {
		if commit {
			s.tx.Commit()
		} else {
			s.tx.Rollback()
		}

		s.tx = nil
	}

	s.gone = true
	s.timer.Stop()

	st.mu.Lock("end")
	if st.m[id] == s {
		delete(st.m, id)
	}
	st.mu.Unlock()
}

func (st *sessionTable) expire(id string, s *session) {
	s.busy <- struct{}{}
	defer func() { <-s.busy }()

	if !s.gone {
		st.end(id, s, false)
	}
}

// run executes fn inside the session's transaction, starting one if none is open. A failure rolls the
// transaction back; the session stays open and the next request starts a new transaction.
func (st *sessionTable) run(ctx context.Context, s *session, fn func(*opencypher.Store) ([]any, error)) ([]any, error) {
	if s.tx == nil {
		tx, err := st.graph.Begin(ctx)
		if err != nil {
			return nil, mapGraphError(ctx, err)
		}

		s.tx = tx
	}

	out, err := fn(s.tx.Store())
	if err != nil {
		s.tx.Rollback()
		s.tx = nil

		return nil, mapGraphError(ctx, err)
	}

	return out, nil
}

// finishTx commits or rolls back the open transaction, keeping the session.
func (st *sessionTable) finishTx(s *session, commit bool) {
	if s.tx == nil {
		return
	}

	if commit {
		s.tx.Commit()
	} else {
		s.tx.Rollback()
	}

	s.tx = nil
}

// close ends a session: script sessions commit on close, bytecode transactions roll back.
func (st *sessionTable) close(ctx context.Context, id string) error {
	s, err := st.acquire(ctx, id, false, false)
	if err != nil || s == nil {
		return err
	}

	defer func() { <-s.busy }()

	st.end(id, s, !s.bytecode)

	return nil
}

// abort rolls back and removes a session whose connection went away.
func (st *sessionTable) abort(id string) {
	st.mu.RLock("abort")
	s := st.m[id]
	st.mu.RUnlock()

	if s != nil {
		st.expire(id, s)
	}
}

func (st *sessionTable) abortAll() {
	st.mu.RLock("abortAll")

	ids := make([]string, 0, len(st.m))
	for id := range st.m {
		ids = append(ids, id)
	}

	st.mu.RUnlock()

	for _, id := range ids {
		st.abort(id)
	}
}
