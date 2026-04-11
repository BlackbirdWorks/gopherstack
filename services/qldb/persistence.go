package qldb

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Ledgers        map[string]*Ledger                          `json:"ledgers"`
	LedgerARNIndex map[string]string                           `json:"ledgerARNIndex"`
	JournalStreams map[string]map[string]*JournalKinesisStream `json:"journalStreams"`
	S3Exports      map[string]*JournalS3Export                 `json:"s3Exports"`
	AccountID      string                                      `json:"accountID"`
	Region         string                                      `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Ledgers:        b.ledgers,
		LedgerARNIndex: b.ledgerARNIndex,
		JournalStreams: b.journalStreams,
		S3Exports:      b.s3Exports,
		AccountID:      b.accountID,
		Region:         b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("qldb: failed to marshal snapshot", "error", err)

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

	b.ledgers = snap.Ledgers
	b.ledgerARNIndex = snap.LedgerARNIndex
	b.journalStreams = snap.JournalStreams
	b.s3Exports = snap.S3Exports
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNilMaps initialises nil maps in the snapshot to empty maps.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Ledgers == nil {
		snap.Ledgers = make(map[string]*Ledger)
	}

	if snap.LedgerARNIndex == nil {
		snap.LedgerARNIndex = make(map[string]string)
	}

	if snap.JournalStreams == nil {
		snap.JournalStreams = make(map[string]map[string]*JournalKinesisStream)
	}

	if snap.S3Exports == nil {
		snap.S3Exports = make(map[string]*JournalS3Export)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
