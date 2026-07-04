package rdsdata

import (
	"context"
	"encoding/json"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	Transactions       map[string]map[string]*Transaction `json:"transactions"`
	ExecutedStatements map[string][]ExecutedStatement     `json:"executedStatements"`
	TxCounter          map[string]int                     `json:"txCounter"`
	AccountID          string                             `json:"accountID"`
	Region             string                             `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	txCopy := make(map[string]map[string]*Transaction, len(b.transactions))
	for region, store := range b.transactions {
		inner := make(map[string]*Transaction, len(store))
		for k, v := range store {
			cp := *v
			inner[k] = &cp
		}
		txCopy[region] = inner
	}

	stmtsCopy := make(map[string][]ExecutedStatement, len(b.executedStatements))
	for region, stmts := range b.executedStatements {
		cp := make([]ExecutedStatement, len(stmts))
		copy(cp, stmts)
		stmtsCopy[region] = cp
	}

	counterCopy := make(map[string]int, len(b.txCounter))
	maps.Copy(counterCopy, b.txCounter)

	snap := backendSnapshot{
		Transactions:       txCopy,
		ExecutedStatements: stmtsCopy,
		TxCounter:          counterCopy,
		AccountID:          b.accountID,
		Region:             b.defaultRegion,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "rdsdata: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "rdsdata", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.transactions = snap.Transactions
	b.executedStatements = snap.ExecutedStatements
	b.txCounter = snap.TxCounter
	b.accountID = snap.AccountID
	b.defaultRegion = snap.Region

	// Rebuild the SQL engine from the recorded statement log so table state
	// survives a snapshot/restore cycle. Best-effort: parameterised writes
	// (whose bound values are not persisted) and statements trimmed from the
	// capped log cannot be replayed.
	if b.engine == nil {
		b.engine = newSQLEngine()
	}

	b.engine.reset()

	for region, stmts := range b.executedStatements {
		b.engine.replay(ctx, region, stmts)
	}

	return nil
}

// ensureNonNilMaps initialises nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Transactions == nil {
		snap.Transactions = make(map[string]map[string]*Transaction)
	}

	if snap.ExecutedStatements == nil {
		snap.ExecutedStatements = make(map[string][]ExecutedStatement)
	}

	if snap.TxCounter == nil {
		snap.TxCounter = make(map[string]int)
	}
}
