package redshiftdata

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// regionSnapshot holds the serialized state for a single region.
type regionSnapshot struct {
	Statements map[string]*Statement `json:"statements"`
	RingBuf    []string              `json:"ringBuf"`
}

type backendSnapshot struct {
	Stores    map[string]*regionSnapshot `json:"stores"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	storesSnap := make(map[string]*regionSnapshot, len(b.stores))

	for region, store := range b.stores {
		ringCopy := make([]string, store.ringLen)
		for i := range store.ringLen {
			ringCopy[i] = store.ringBuf[(store.ringHead+i)%maxStatementHistory]
		}

		stmtsCopy := make(map[string]*Statement, len(store.statements))
		for k, v := range store.statements {
			stmtsCopy[k] = cloneStatement(v)
		}

		storesSnap[region] = &regionSnapshot{
			Statements: stmtsCopy,
			RingBuf:    ringCopy,
		}
	}

	snap := backendSnapshot{
		Stores:    storesSnap,
		AccountID: b.accountID,
		Region:    b.defaultRegion,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "redshiftdata: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.accountID = snap.AccountID
	b.defaultRegion = snap.Region
	b.stores = make(map[string]*regionStore, len(snap.Stores))

	for region, rs := range snap.Stores {
		if rs.Statements == nil {
			rs.Statements = make(map[string]*Statement)
		}

		store := &regionStore{
			statements: rs.Statements,
		}

		n := len(rs.RingBuf)
		if n > maxStatementHistory {
			rs.RingBuf = rs.RingBuf[n-maxStatementHistory:]
		}

		for _, id := range rs.RingBuf {
			if _, ok := store.statements[id]; ok {
				store.ringBuf[store.ringLen] = id
				store.ringLen++
			}
		}

		b.stores[region] = store
	}

	return nil
}
