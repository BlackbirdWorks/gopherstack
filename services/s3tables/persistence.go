package s3tables

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	TableBuckets      map[string]*TableBucket             `json:"tableBuckets"`
	Namespaces        map[string]*Namespace               `json:"namespaces"`
	Tables            map[string]*Table                   `json:"tables"`
	TableIndex        map[string]string                   `json:"tableIndex"`
	BucketReplication map[string]*BucketReplicationConfig `json:"bucketReplication"`
	TableReplication  map[string]bool                     `json:"tableReplication"`
	TableRecordExpiry map[string]*TableRecordExpiryConfig `json:"tableRecordExpiry"`
	AccountID         string                              `json:"accountID"`
	Region            string                              `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		TableBuckets:      b.tableBuckets,
		Namespaces:        b.namespaces,
		Tables:            b.tables,
		TableIndex:        b.tableIndex,
		BucketReplication: b.bucketReplication,
		TableReplication:  b.tableReplication,
		TableRecordExpiry: b.tableRecordExpiry,
		AccountID:         b.accountID,
		Region:            b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("s3tables: failed to marshal snapshot", "error", err)

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

	b.tableBuckets = snap.TableBuckets
	b.namespaces = snap.Namespaces
	b.tables = snap.Tables
	b.tableIndex = snap.TableIndex
	b.bucketReplication = snap.BucketReplication
	b.tableReplication = snap.TableReplication
	b.tableRecordExpiry = snap.TableRecordExpiry
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.TableBuckets == nil {
		snap.TableBuckets = make(map[string]*TableBucket)
	}

	if snap.Namespaces == nil {
		snap.Namespaces = make(map[string]*Namespace)
	}

	if snap.Tables == nil {
		snap.Tables = make(map[string]*Table)
	}

	if snap.TableIndex == nil {
		snap.TableIndex = make(map[string]string)
	}

	if snap.BucketReplication == nil {
		snap.BucketReplication = make(map[string]*BucketReplicationConfig)
	}

	if snap.TableReplication == nil {
		snap.TableReplication = make(map[string]bool)
	}

	if snap.TableRecordExpiry == nil {
		snap.TableRecordExpiry = make(map[string]*TableRecordExpiryConfig)
	}
}
