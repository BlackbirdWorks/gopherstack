package dynamodb

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// txnTokenTTL is how long a committed idempotency token is retained.
// AWS DynamoDB expires tokens after 10 minutes.
const txnTokenTTL = 10 * time.Minute

// txnPendingTTL is the maximum time an in-progress idempotency token is retained.
// Entries older than this are considered orphaned (e.g. due to a crash) and are
// removed by the janitor so the token can be reused.
const txnPendingTTL = 5 * time.Minute

// InMemoryDB stores tables and items organized by region.
type InMemoryDB struct {
	Tables               map[string]map[string]*Table
	deletingTables       map[string]map[string]*Table
	Backups              map[string]*Backup   // backupARN → Backup
	txnTokens            map[string]time.Time // committed idempotency tokens → expiry time
	txnPending           map[string]time.Time // in-progress idempotency tokens → start time
	streamARNIndex       map[string]*Table    // streamARN → Table (reverse index)
	fisReplicationPaused map[string]time.Time // keyed by table ARN; value is expiry (zero = no expiry)
	exprCache            *ExpressionCache
	throttler            *Throttler
	mu                   *lockmetrics.RWMutex
	defaultRegion        string
	accountID            string
	// createDelay is the time to wait before transitioning a new table to ACTIVE.
	// Zero means immediate ACTIVE (no lifecycle simulation).
	createDelay time.Duration
}

// Backup holds the metadata and a point-in-time item snapshot for a DynamoDB on-demand backup.
type Backup struct {
	CreationDateTime     time.Time                    `json:"CreationDateTime"`
	TableArn             string                       `json:"TableArn"`
	TableID              string                       `json:"TableID"`
	BackupArn            string                       `json:"BackupArn"`
	BackupName           string                       `json:"BackupName"`
	BackupStatus         string                       `json:"BackupStatus"`
	BackupType           string                       `json:"BackupType"`
	TableName            string                       `json:"TableName"`
	Items                []map[string]any             `json:"Items"`
	KeySchema            []models.KeySchemaElement    `json:"KeySchema"`
	AttributeDefinitions []models.AttributeDefinition `json:"AttributeDefinitions"`
	SizeBytes            int64                        `json:"SizeBytes"`
}

// StreamRecord captures a single item-level change event for DynamoDB Streams.
// Uses models.StreamRecord for storage and wire format.

const (
	// streamEventInsert is emitted when a new item is created.
	streamEventInsert = "INSERT"
	// streamEventModify is emitted when an existing item is updated.
	streamEventModify = "MODIFY"
	// streamEventRemove is emitted when an item is deleted.
	streamEventRemove = "REMOVE"
	// maxStreamRecords is the maximum number of records in the ring buffer.
	maxStreamRecords = 1000
	// streamViewTypeNewAndOldImages captures both old and new images.
	streamViewTypeNewAndOldImages = "NEW_AND_OLD_IMAGES"
	// streamViewTypeNewImage captures only the new image.
	streamViewTypeNewImage = "NEW_IMAGE"
	// streamViewTypeOldImage captures only the old image.
	streamViewTypeOldImage = "OLD_IMAGE"
	// streamViewTypeKeysOnly captures only keys.
	streamViewTypeKeysOnly = "KEYS_ONLY"
)

type Table struct {
	CreationDateTime          time.Time `json:"CreationDateTime"`
	pkIndex                   map[string]int
	pkskIndex                 map[string]map[string]int
	mu                        *lockmetrics.RWMutex
	activateTimer             *time.Timer
	Tags                      *tags.Tags                              `json:"Tags,omitempty"`
	Name                      string                                  `json:"Name"`
	TTLAttribute              string                                  `json:"TTLAttribute,omitempty"`
	StreamViewType            string                                  `json:"StreamViewType,omitempty"`
	StreamARN                 string                                  `json:"StreamARN,omitempty"`
	TableArn                  string                                  `json:"TableArn"`
	Status                    string                                  `json:"Status"`
	TableID                   string                                  `json:"TableID"`
	TableClass                string                                  `json:"TableClass,omitempty"`
	Items                     []map[string]any                        `json:"Items"`
	AttributeDefinitions      []models.AttributeDefinition            `json:"AttributeDefinitions"`
	Replicas                  []models.ReplicaDescription             `json:"Replicas,omitempty"`
	GlobalSecondaryIndexes    []models.GlobalSecondaryIndex           `json:"GlobalSecondaryIndexes,omitempty"`
	StreamRecords             []models.StreamRecord                   `json:"StreamRecords,omitempty"`
	KeySchema                 []models.KeySchemaElement               `json:"KeySchema"`
	LocalSecondaryIndexes     []models.LocalSecondaryIndex            `json:"LocalSecondaryIndexes,omitempty"`
	ProvisionedThroughput     models.ProvisionedThroughputDescription `json:"ProvisionedThroughput"`
	streamSeq                 int64
	StreamHead                int  `json:"StreamHead,omitempty"`
	PITREnabled               bool `json:"PITREnabled,omitempty"`
	StreamsEnabled            bool `json:"StreamsEnabled"`
	DeletionProtectionEnabled bool `json:"DeletionProtectionEnabled"`
}

func NewInMemoryDB() *InMemoryDB {
	const exprCacheSize = 1000

	return &InMemoryDB{
		Tables:               make(map[string]map[string]*Table),
		deletingTables:       make(map[string]map[string]*Table),
		Backups:              make(map[string]*Backup),
		txnTokens:            make(map[string]time.Time),
		txnPending:           make(map[string]time.Time),
		streamARNIndex:       make(map[string]*Table),
		fisReplicationPaused: make(map[string]time.Time),
		exprCache:            NewExpressionCache(exprCacheSize),
		defaultRegion:        config.DefaultRegion,
		accountID:            config.DefaultAccountID,
		mu:                   lockmetrics.New("ddb"),
		throttler:            NewThrottler(false),
	}
}

// Close releases all backend resources.
func (db *InMemoryDB) Close() {
	db.mu.Lock("Close")
	defer db.mu.Unlock()

	for _, regionTables := range db.Tables {
		for _, table := range regionTables {
			stopTableTimers(table)
		}
	}

	if db.exprCache != nil {
		db.exprCache.Close()
	}
	db.mu.Close()
}

// SetEnforceThroughput enables or disables provisioned throughput throttling.
// Call before CreateTable calls; intended for CLI configuration.
func (db *InMemoryDB) SetEnforceThroughput(enabled bool) {
	db.throttler = NewThrottler(enabled)
}

// appendStreamRecord adds a new record to the table's stream ring buffer.
// Must be called with table.mu held (write lock).
func (t *Table) appendStreamRecord(eventName string, oldItem, newImage map[string]any) {
	if !t.StreamsEnabled {
		return
	}

	t.streamSeq++
	seq := fmt.Sprintf("%020d", t.streamSeq)

	record := models.StreamRecord{
		EventID:                     fmt.Sprintf("%s-%s", t.Name, seq),
		EventName:                   eventName,
		SequenceNumber:              seq,
		ApproximateCreationDateTime: time.Now().Unix(),
	}

	switch t.StreamViewType {
	case streamViewTypeNewAndOldImages:
		record.OldImage = oldItem
		record.NewImage = newImage
	case streamViewTypeNewImage:
		record.NewImage = newImage
	case streamViewTypeOldImage:
		record.OldImage = oldItem
	case streamViewTypeKeysOnly:
		// keys only — captured below from key schema
	default:
		record.OldImage = oldItem
		record.NewImage = newImage
	}

	// O(1) ring buffer: pre-allocate once, then overwrite in-place.
	// When the buffer is not yet full, append normally. Once full, overwrite
	// the oldest slot (at StreamHead) and advance the head pointer.
	if len(t.StreamRecords) < maxStreamRecords {
		t.StreamRecords = append(t.StreamRecords, record)
	} else {
		t.StreamRecords[t.StreamHead] = record
		t.StreamHead = (t.StreamHead + 1) % maxStreamRecords
	}
}

// streamSeqRange returns the first and last sequence numbers in the ring buffer
// without allocating a new slice. Intended for DescribeStream which only needs
// the range boundaries.
// Must be called with table.mu held (at least read lock).
func (t *Table) streamSeqRange() (string, string) {
	n := len(t.StreamRecords)
	if n == 0 {
		return "", ""
	}

	if n < maxStreamRecords {
		// Buffer not yet full: records are in insertion order.
		return t.StreamRecords[0].SequenceNumber, t.StreamRecords[n-1].SequenceNumber
	}

	// Ring is full: oldest record is at StreamHead, newest is at (StreamHead-1+n) % n.
	firstIdx := t.StreamHead
	lastIdx := (t.StreamHead - 1 + maxStreamRecords) % maxStreamRecords

	return t.StreamRecords[firstIdx].SequenceNumber, t.StreamRecords[lastIdx].SequenceNumber
}

// streamRecordsInOrder returns the two halves of the ring buffer in insertion
// order as a pair of slices: (tail, head). Callers should iterate tail first,
// then head. This avoids allocating a new slice on every call.
//
// When the buffer is not yet full, tail is the full slice and head is nil.
// When full, tail is StreamRecords[StreamHead:] (oldest records) and head is
// StreamRecords[:StreamHead] (newest records that wrapped around).
//
// Must be called with table.mu held (at least read lock).
func (t *Table) streamRecordsInOrder() ([]models.StreamRecord, []models.StreamRecord) {
	n := len(t.StreamRecords)
	if n == 0 {
		return nil, nil
	}

	if n < maxStreamRecords {
		// Buffer not yet full: already in insertion order.
		return t.StreamRecords, nil
	}

	// Ring is full: split at StreamHead.
	return t.StreamRecords[t.StreamHead:], t.StreamRecords[:t.StreamHead]
}

func BuildKeyString(item map[string]any, attrName string) string {
	if attrName == "" {
		return ""
	}

	return dynamoattr.ToString(item[attrName])
}

// initializeIndexes creates empty index maps for a table.
func (t *Table) initializeIndexes() {
	hasSortKey := len(t.KeySchema) > 1

	if hasSortKey {
		t.pkskIndex = make(map[string]map[string]int)
	} else {
		t.pkIndex = make(map[string]int)
	}
}

// rebuildIndexes rebuilds all indexes from existing items (used after table creation or batch updates).
func (t *Table) rebuildIndexes() {
	t.initializeIndexes()

	pkDef, skDef := getPKAndSK(t.KeySchema)
	hasSortKey := skDef.AttributeName != ""

	for i, item := range t.Items {
		pkVal := BuildKeyString(item, pkDef.AttributeName)

		if hasSortKey {
			skVal := BuildKeyString(item, skDef.AttributeName)
			if t.pkskIndex[pkVal] == nil {
				t.pkskIndex[pkVal] = make(map[string]int)
			}
			t.pkskIndex[pkVal][skVal] = i
		} else {
			t.pkIndex[pkVal] = i
		}
	}
}

// Regions returns all distinct regions that contain at least one table.
func (db *InMemoryDB) Regions() []string {
	db.mu.RLock("Regions")
	defer db.mu.RUnlock()

	var regions []string

	for region, regionTables := range db.Tables {
		if len(regionTables) > 0 {
			regions = append(regions, region)
		}
	}

	sort.Strings(regions)

	return regions
}

// TableNamesByRegion returns table names in the given region, or all regions if region is empty.
func (db *InMemoryDB) TableNamesByRegion(region string) []string {
	db.mu.RLock("TableNamesByRegion")
	defer db.mu.RUnlock()

	var names []string

	for r, regionTables := range db.Tables {
		if region != "" && r != region {
			continue
		}

		for name := range regionTables {
			names = append(names, name)
		}
	}

	sort.Strings(names)

	return names
}

// ListAllTables returns a slice of all tables across all regions (for UI).
func (db *InMemoryDB) ListAllTables() []*Table {
	db.mu.RLock("ListAllTables")
	defer db.mu.RUnlock()

	var tables []*Table
	for _, regionTables := range db.Tables {
		for _, table := range regionTables {
			tables = append(tables, table)
		}
	}

	return tables
}

// GetTable returns a table by name from the default region (for UI/backward compatibility).
func (db *InMemoryDB) GetTable(name string) (*Table, bool) {
	return db.GetTableInRegion(name, db.defaultRegion)
}

// GetTableInRegion returns a table by name from a specific region.
func (db *InMemoryDB) GetTableInRegion(name string, region string) (*Table, bool) {
	db.mu.RLock("GetTableInRegion")
	defer db.mu.RUnlock()

	if region == "" {
		region = db.defaultRegion
	}

	regionTables, exists := db.Tables[region]
	if !exists {
		return nil, false
	}

	table, exists := regionTables[name]

	return table, exists
}

// SetDefaultRegion sets the default region for this backend.
func (db *InMemoryDB) SetDefaultRegion(region string) {
	if region == "" {
		region = config.DefaultRegion
	}
	db.defaultRegion = region
}

// SetCreateDelay sets the CREATING → ACTIVE transition delay.
// Call before CreateTable calls; intended for tests and CLI configuration.
func (db *InMemoryDB) SetCreateDelay(d time.Duration) {
	db.createDelay = d
}

// TaggedTableInfo contains a DynamoDB table's ARN and tag snapshot.
// Used by the Resource Groups Tagging API cross-service listing.
type TaggedTableInfo struct {
	Tags map[string]string
	ARN  string
}

// TaggedTables returns a snapshot of all DynamoDB tables with their ARNs and tags.
// Intended for use by the Resource Groups Tagging API provider.
func (db *InMemoryDB) TaggedTables() []TaggedTableInfo {
	db.mu.RLock("TaggedTables")
	defer db.mu.RUnlock()

	var result []TaggedTableInfo

	for _, regionTables := range db.Tables {
		for _, table := range regionTables {
			var tagMap map[string]string
			if table.Tags != nil {
				table.mu.RLock("TaggedTables.tag")
				tagMap = table.Tags.Clone()
				table.mu.RUnlock()
			}

			result = append(result, TaggedTableInfo{ARN: table.TableArn, Tags: tagMap})
		}
	}

	return result
}

// stopTableTimers stops all in-flight timers held by the table — the activation
// timer for newly-created tables and the index-status timers for any GSI that is
// mid-CREATING or mid-DELETING transition. Must be called before the table is
// discarded so that the AfterFunc goroutines are not left running.
// Idempotent: safe to call even when timers are nil or already stopped.
func stopTableTimers(table *Table) {
	if table.activateTimer != nil {
		table.activateTimer.Stop()
	}

	for i := range table.GlobalSecondaryIndexes {
		if table.GlobalSecondaryIndexes[i].IndexStatusTimer != nil {
			table.GlobalSecondaryIndexes[i].IndexStatusTimer.Stop()
		}
	}
}

// Reset clears all in-memory state from the database. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (db *InMemoryDB) Reset() {
	const exprCacheSize = 1000

	db.mu.Lock("Reset")
	defer db.mu.Unlock()

	// Stop activation timers and close mutex metrics for existing tables
	// (both active and deleting) to avoid goroutine leaks and metric registry leaks.
	for _, regionTables := range db.Tables {
		for _, table := range regionTables {
			stopTableTimers(table)
			if table.Tags != nil {
				table.Tags.Close()
			}

			table.mu.Close()
		}
	}

	for _, regionTables := range db.deletingTables {
		for _, table := range regionTables {
			stopTableTimers(table)
			if table.Tags != nil {
				table.Tags.Close()
			}

			table.mu.Close()
		}
	}

	db.Tables = make(map[string]map[string]*Table)
	db.deletingTables = make(map[string]map[string]*Table)
	db.streamARNIndex = make(map[string]*Table)
	db.Backups = make(map[string]*Backup)
	db.txnTokens = make(map[string]time.Time)
	db.txnPending = make(map[string]time.Time)
	db.fisReplicationPaused = make(map[string]time.Time)
	if db.exprCache != nil {
		db.exprCache.Close()
	}
	db.exprCache = NewExpressionCache(exprCacheSize)
}
