package s3tables

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

type backendSnapshot struct {
	TableBuckets            map[string]*TableBucket             `json:"tableBuckets"`
	Namespaces              map[string]*Namespace               `json:"namespaces"`
	Tables                  map[string]*Table                   `json:"tables"`
	TableIndex              map[string]string                   `json:"tableIndex"`
	BucketReplication       map[string]*BucketReplicationConfig `json:"bucketReplication"`
	TableReplication        map[string]bool                     `json:"tableReplication"`
	TableRecordExpiry       map[string]*TableRecordExpiryConfig `json:"tableRecordExpiry"`
	Tags                    map[string]map[string]string        `json:"tags"`
	TableReplicationConfigs map[string]map[string]any           `json:"tableReplicationConfigs"`
	AccountID               string                              `json:"accountID"`
	Region                  string                              `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.muBuckets.RLock("Snapshot")
	b.muNamespaces.RLock("Snapshot")
	b.muTables.RLock("Snapshot")
	b.muState.RLock("Snapshot")
	defer b.muBuckets.RUnlock()
	defer b.muNamespaces.RUnlock()
	defer b.muTables.RUnlock()
	defer b.muState.RUnlock()

	snap := backendSnapshot{
		TableBuckets:            b.tableBuckets,
		Namespaces:              b.namespaces,
		Tables:                  b.tables,
		TableIndex:              b.tableIndex,
		BucketReplication:       b.bucketReplication,
		TableReplication:        b.tableReplication,
		TableRecordExpiry:       b.tableRecordExpiry,
		Tags:                    b.tags,
		TableReplicationConfigs: b.tableReplicationConfigs,
		AccountID:               b.accountID,
		Region:                  b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "s3tables: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "s3tables", data, &snap); err != nil {
		return err
	}

	ensureNonNilMaps(&snap)

	b.muBuckets.Lock("Restore")
	b.muNamespaces.Lock("Restore")
	b.muTables.Lock("Restore")
	b.muState.Lock("Restore")
	defer b.muBuckets.Unlock()
	defer b.muNamespaces.Unlock()
	defer b.muTables.Unlock()
	defer b.muState.Unlock()

	b.tableBuckets = snap.TableBuckets
	b.namespaces = snap.Namespaces
	b.tables = snap.Tables
	b.tableIndex = snap.TableIndex
	b.bucketReplication = snap.BucketReplication
	b.tableReplication = snap.TableReplication
	b.tableRecordExpiry = snap.TableRecordExpiry
	b.tags = snap.Tags
	b.tableReplicationConfigs = snap.TableReplicationConfigs
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

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}

	if snap.TableReplicationConfigs == nil {
		snap.TableReplicationConfigs = make(map[string]map[string]any)
	}
}
