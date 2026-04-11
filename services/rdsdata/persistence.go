package rdsdata

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Transactions       map[string]*Transaction `json:"transactions"`
	AccountID          string                  `json:"accountID"`
	Region             string                  `json:"region"`
	ExecutedStatements []ExecutedStatement     `json:"executedStatements"`
	TxCounter          int                     `json:"txCounter"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	stmtsCopy := make([]ExecutedStatement, len(b.executedStatements))
	copy(stmtsCopy, b.executedStatements)

	snap := backendSnapshot{
		Transactions:       b.transactions,
		ExecutedStatements: stmtsCopy,
		AccountID:          b.accountID,
		Region:             b.region,
		TxCounter:          b.txCounter,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("rdsdata: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.transactions = snap.Transactions
	b.executedStatements = snap.ExecutedStatements
	b.accountID = snap.AccountID
	b.region = snap.Region
	b.txCounter = snap.TxCounter

	return nil
}

// ensureNonNilMaps initialises nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Transactions == nil {
		snap.Transactions = make(map[string]*Transaction)
	}

	if snap.ExecutedStatements == nil {
		snap.ExecutedStatements = []ExecutedStatement{}
	}
}
