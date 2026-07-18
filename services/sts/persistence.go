package sts

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Sessions             map[string]*SessionInfo `json:"sessions"`
	TotalSessionsCreated int64                   `json:"total_sessions_created"`
	OpsAssumeRole        int64                   `json:"ops_assume_role"`
	OpsAssumeRoleWithWI  int64                   `json:"ops_assume_role_with_wi"`
	OpsGetCallerIdentity int64                   `json:"ops_get_caller_identity"`
	OpsGetSessionToken   int64                   `json:"ops_get_session_token"`
	OpsGetFedToken       int64                   `json:"ops_get_fed_token"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	sessions := make(map[string]*SessionInfo, b.sessions.Len())
	b.sessions.Range(func(v *SessionInfo) bool {
		if v != nil {
			cp := *v
			sessions[cp.AccessKeyID] = &cp
		}

		return true
	})

	snap := backendSnapshot{
		Sessions:             sessions,
		TotalSessionsCreated: b.totalSessionsCreated.Load(),
		OpsAssumeRole:        b.cntAssumeRole.Load(),
		OpsAssumeRoleWithWI:  b.cntAssumeRoleWithWebIdentity.Load(),
		OpsGetCallerIdentity: b.cntGetCallerIdentity.Load(),
		OpsGetSessionToken:   b.cntGetSessionToken.Load(),
		OpsGetFedToken:       b.cntGetFederationToken.Load(),
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "sts: failed to snapshot backend state", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// Expired sessions are discarded on load.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "sts", data, &snap); err != nil {
		return err
	}

	now := time.Now().UTC()

	func() {
		b.mu.Lock("Restore")
		defer b.mu.Unlock()

		b.sessions.Reset()

		for _, s := range snap.Sessions {
			if s == nil {
				continue
			}

			// Discard already-expired sessions; keep zero-valued (non-expiring) sessions.
			if !s.Expiration.IsZero() && !now.Before(s.Expiration) {
				continue
			}

			b.sessions.Put(s)
		}
	}()

	// Restore operation counters (atomics are safe without the mutex).
	b.totalSessionsCreated.Store(snap.TotalSessionsCreated)
	b.cntAssumeRole.Store(snap.OpsAssumeRole)
	b.cntAssumeRoleWithWebIdentity.Store(snap.OpsAssumeRoleWithWI)
	b.cntGetCallerIdentity.Store(snap.OpsGetCallerIdentity)
	b.cntGetSessionToken.Store(snap.OpsGetSessionToken)
	b.cntGetFederationToken.Store(snap.OpsGetFedToken)

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
