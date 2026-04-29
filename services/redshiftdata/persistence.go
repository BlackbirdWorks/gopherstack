package redshiftdata

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Statements map[string]*Statement `json:"statements"`
	AccountID  string                `json:"accountID"`
	Region     string                `json:"region"`
	// RingBuf stores the IDs in insertion order so the ring buffer can be
	// reconstructed faithfully after a Restore.
	RingBuf  []string `json:"ringBuf"`
	RingHead int      `json:"ringHead"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	// Flatten the ring buffer into a plain slice for JSON serialisation.
	ringCopy := make([]string, b.ringLen)
	for i := range b.ringLen {
		ringCopy[i] = b.ringBuf[(b.ringHead+i)%maxStatementHistory]
	}

	stmtsCopy := make(map[string]*Statement, len(b.statements))
	for k, v := range b.statements {
		stmtsCopy[k] = cloneStatement(v)
	}

	snap := backendSnapshot{
		Statements: stmtsCopy,
		RingBuf:    ringCopy,
		AccountID:  b.accountID,
		Region:     b.region,
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

	if snap.Statements == nil {
		snap.Statements = make(map[string]*Statement)
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.statements = snap.Statements
	b.accountID = snap.AccountID
	b.region = snap.Region

	// Re-fill the ring buffer from the flat slice.
	b.ringLen = 0
	b.ringHead = 0
	for i := range b.ringBuf {
		b.ringBuf[i] = ""
	}

	n := len(snap.RingBuf)
	if n > maxStatementHistory {
		// Keep only the most recent maxStatementHistory entries.
		snap.RingBuf = snap.RingBuf[n-maxStatementHistory:]
	}

	for _, id := range snap.RingBuf {
		if _, ok := b.statements[id]; ok {
			b.ringBuf[b.ringLen] = id
			b.ringLen++
		}
	}

	return nil
}
