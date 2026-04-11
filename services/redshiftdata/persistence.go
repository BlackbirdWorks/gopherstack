package redshiftdata

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Statements    map[string]*Statement `json:"statements"`
	AccountID     string                `json:"accountID"`
	Region        string                `json:"region"`
	StatementKeys []string              `json:"statementKeys"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	keysCopy := make([]string, len(b.statementKeys))
	copy(keysCopy, b.statementKeys)

	stmtsCopy := make(map[string]*Statement, len(b.statements))
	for k, v := range b.statements {
		s := cloneStatement(v)
		stmtsCopy[k] = s
	}

	snap := backendSnapshot{
		Statements:    stmtsCopy,
		StatementKeys: keysCopy,
		AccountID:     b.accountID,
		Region:        b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("redshiftdata: failed to marshal snapshot", "error", err)

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

	b.statements = snap.Statements
	b.statementKeys = snap.StatementKeys
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNilMaps initialises nil maps and slices in the snapshot to empty values.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Statements == nil {
		snap.Statements = make(map[string]*Statement)
	}

	if snap.StatementKeys == nil {
		snap.StatementKeys = []string{}
	}
}
