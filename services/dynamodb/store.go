package dynamodb

import (
	"context"
	"fmt"
	"sort"
	"strings"
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

// StoredGlobalTable holds the metadata for a DynamoDB global table.
type StoredGlobalTable struct {
	CreationDateTime time.Time `json:"CreationDateTime"`
	GlobalTableName  string    `json:"GlobalTableName"`
	GlobalTableArn   string    `json:"GlobalTableArn"`
	// ReplicationGroup is the list of region names in the global table.
	ReplicationGroup []string `json:"ReplicationGroup"`
}

// storedExport holds the fields needed to satisfy DescribeExport and ListExports.
type storedExport struct {
	CreatedAt    time.Time
	ExportArn    string
	ExportStatus string
	TableArn     string
	S3Bucket     string
}

// storedImport holds the fields needed to satisfy DescribeImport and ListImports.
type storedImport struct {
	CreatedAt    time.Time
	ImportArn    string
	ImportStatus string
	TableArn     string
	S3Bucket     string
	InputFormat  string
}

// autoScalingSettings records the last UpdateTableReplicaAutoScaling input
// for a table so DescribeTableReplicaAutoScaling can return the same shape
// AWS does. Capacities are simple int pointers (nil = unspecified). Per-GSI
// settings are keyed by index name.
type autoScalingSettings struct {
	GlobalSecondaryIndexes map[string]*autoScalingThroughput `json:"GlobalSecondaryIndexes,omitempty"`
	Read                   *autoScalingThroughput            `json:"Read,omitempty"`
	Write                  *autoScalingThroughput            `json:"Write,omitempty"`
}

// autoScalingThroughput captures the min/max/target settings for one direction
// (read or write). Mirrors types.AutoScalingSettingsUpdate but stripped to the
// fields LocalStack and most callers care about.
type autoScalingThroughput struct {
	MinCapacity     *int64   `json:"MinCapacity,omitempty"`
	MaxCapacity     *int64   `json:"MaxCapacity,omitempty"`
	TargetUtilizPct *float64 `json:"TargetUtilizationPct,omitempty"`
	Disabled        bool     `json:"AutoScalingDisabled,omitempty"`
}

// pitrSnapshot captures the items of a PITR-enabled table at a point in time.
// Snapshots are taken by the janitor on its sweep interval (typically 1 minute);
// RestoreTableToPointInTime returns the latest snapshot at or before the
// requested RestoreDateTime.
type pitrSnapshot struct {
	Taken time.Time
	Items []map[string]any
}

// maxPITRSnapshots bounds the per-table snapshot ring so memory cost stays
// predictable. With a 1-minute janitor sweep this gives ~1 hour of
// point-in-time recovery coverage — enough for tests, well short of AWS's
// real 35-day window.
const maxPITRSnapshots = 60

// Caps for retained metadata maps. Beyond these counts the oldest entries are
// evicted on each insert so long-running instances do not leak memory. Real
// AWS has account-wide quotas (1,000 backups, 100 exports, 50 active imports);
// we use generous-but-bounded numbers so tests don't trip the cap accidentally.
const (
	maxBackupsRetained = 10_000
	maxExportsRetained = 5_000
	maxImportsRetained = 5_000
)

// InMemoryDB stores tables and items organized by region.
type InMemoryDB struct {
	Tables               map[string]map[string]*Table
	deletingTables       map[string]map[string]*Table
	Backups              map[string]*Backup            // backupARN → Backup
	GlobalTables         map[string]*StoredGlobalTable // globalTableName → StoredGlobalTable
	exports              map[string]storedExport       // exportARN → storedExport
	imports              map[string]storedImport       // importARN → storedImport
	txnTokens            map[string]time.Time          // committed idempotency tokens → expiry time
	txnPending           map[string]time.Time          // in-progress idempotency tokens → start time
	streamARNIndex       map[string]*Table             // streamARN → Table (reverse index)
	fisReplicationPaused map[string]time.Time          // keyed by table ARN; value is expiry (zero = no expiry)
	exprCache            *ExpressionCache
	throttler            *Throttler
	mu                   *lockmetrics.RWMutex
	// kinesisEmitter forwards stream records to Kinesis destinations when configured.
	kinesisEmitter KinesisEmitter
	defaultRegion  string
	accountID      string
	// createDelay is the time to wait before transitioning a new table to ACTIVE.
	// Zero means immediate ACTIVE (no lifecycle simulation).
	createDelay time.Duration
}

// Backup holds the metadata and a point-in-time item snapshot for a DynamoDB on-demand backup.
type Backup struct {
	CreationDateTime      time.Time                               `json:"CreationDateTime"`
	TableArn              string                                  `json:"TableArn"`
	TableID               string                                  `json:"TableID"`
	BackupArn             string                                  `json:"BackupArn"`
	BackupName            string                                  `json:"BackupName"`
	BackupStatus          string                                  `json:"BackupStatus"`
	BackupType            string                                  `json:"BackupType"`
	TableName             string                                  `json:"TableName"`
	Items                 []map[string]any                        `json:"Items"`
	KeySchema             []models.KeySchemaElement               `json:"KeySchema"`
	AttributeDefinitions  []models.AttributeDefinition            `json:"AttributeDefinitions"`
	ProvisionedThroughput models.ProvisionedThroughputDescription `json:"ProvisionedThroughput"`
	SizeBytes             int64                                   `json:"SizeBytes"`
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

// Table is a stored DynamoDB table.
//
// Field ordering trades a few bytes of padding for readability: related
// fields (SSE, autoscaling, streams) are grouped together rather than
// strictly sorted by alignment requirement.
//
//nolint:govet // fieldalignment: prefer logical grouping over byte packing
type Table struct {
	CreationDateTime time.Time `json:"CreationDateTime"`
	pkIndex          map[string]int
	pkskIndex        map[string]map[string]int
	mu               *lockmetrics.RWMutex
	activateTimer    *time.Timer
	Tags             *tags.Tags `json:"Tags,omitempty"`
	kinesisEmitter   KinesisEmitter
	// AutoScaling stores the most recent UpdateTableReplicaAutoScaling input
	// so DescribeTableReplicaAutoScaling can echo it back. The in-memory
	// backend doesn't actually scale; this is metadata round-tripping only,
	// matching LocalStack's behaviour.
	AutoScaling *autoScalingSettings `json:"AutoScaling,omitempty"`
	// SSEType is "AES256" (SSE-S3) or "KMS". Empty when encryption is not
	// configured (the table is then treated as using owned-key AES256 by AWS).
	SSEType string `json:"SSEType,omitempty"`
	// SSEKMSMasterKeyArn is the customer-managed KMS key ARN when SSEType=KMS.
	SSEKMSMasterKeyArn     string                                  `json:"SSEKMSMasterKeyArn,omitempty"`
	TableClass             string                                  `json:"TableClass,omitempty"`
	GlobalTableName        string                                  `json:"GlobalTableName,omitempty"`
	TTLAttribute           string                                  `json:"TTLAttribute,omitempty"`
	StreamViewType         string                                  `json:"StreamViewType,omitempty"`
	StreamARN              string                                  `json:"StreamARN,omitempty"`
	TableArn               string                                  `json:"TableArn"`
	Status                 string                                  `json:"Status"`
	TableID                string                                  `json:"TableID"`
	Name                   string                                  `json:"Name"`
	BillingMode            string                                  `json:"BillingMode,omitempty"`
	ResourcePolicy         string                                  `json:"ResourcePolicy,omitempty"`
	Replicas               []models.ReplicaDescription             `json:"Replicas,omitempty"`
	Items                  []map[string]any                        `json:"Items"`
	GlobalSecondaryIndexes []models.GlobalSecondaryIndex           `json:"GlobalSecondaryIndexes,omitempty"`
	StreamRecords          []models.StreamRecord                   `json:"StreamRecords,omitempty"`
	KeySchema              []models.KeySchemaElement               `json:"KeySchema"`
	LocalSecondaryIndexes  []models.LocalSecondaryIndex            `json:"LocalSecondaryIndexes,omitempty"`
	AttributeDefinitions   []models.AttributeDefinition            `json:"AttributeDefinitions"`
	KinesisDestinations    []string                                `json:"KinesisDestinations,omitempty"`
	ProvisionedThroughput  models.ProvisionedThroughputDescription `json:"ProvisionedThroughput"`
	// pitrSnapshots is a ring of past Items + KeySchema captured by the
	// janitor when PITREnabled. Used by RestoreTableToPointInTime to honour
	// the requested RestoreDateTime. Bounded by maxPITRSnapshots.
	pitrSnapshots              []pitrSnapshot
	streamSeq                  int64
	StreamHead                 int  `json:"StreamHead,omitempty"`
	PITREnabled                bool `json:"PITREnabled,omitempty"`
	SSEEnabled                 bool `json:"SSEEnabled,omitempty"`
	StreamsEnabled             bool `json:"StreamsEnabled"`
	DeletionProtectionEnabled  bool `json:"DeletionProtectionEnabled"`
	ContributorInsightsEnabled bool `json:"ContributorInsightsEnabled,omitempty"`
}

func NewInMemoryDB() *InMemoryDB {
	const exprCacheSize = 1000

	return &InMemoryDB{
		Tables:               make(map[string]map[string]*Table),
		deletingTables:       make(map[string]map[string]*Table),
		Backups:              make(map[string]*Backup),
		GlobalTables:         make(map[string]*StoredGlobalTable),
		exports:              make(map[string]storedExport),
		imports:              make(map[string]storedImport),
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

// extractStreamKeys returns a map containing only the key attributes from item,
// using the table's KeySchema as the filter. Returns nil if item is nil.
// Must be called with table.mu held (at least read lock).
func (t *Table) extractStreamKeys(item map[string]any) map[string]any {
	if item == nil || len(t.KeySchema) == 0 {
		return nil
	}
	keys := make(map[string]any, len(t.KeySchema))
	for _, ks := range t.KeySchema {
		if v, ok := item[ks.AttributeName]; ok {
			keys[ks.AttributeName] = v
		}
	}

	return keys
}

// appendStreamRecord adds a new record to the table's stream ring buffer.
// Must be called with table.mu held (write lock).
func (t *Table) appendStreamRecord(eventName string, oldItem, newImage map[string]any) {
	if !t.StreamsEnabled {
		return
	}

	t.streamSeq++
	seq := fmt.Sprintf("%020d", t.streamSeq)

	// Keys are always included in stream records regardless of view type.
	// Prefer the new image for key extraction; fall back to old image on REMOVE.
	keySource := newImage
	if keySource == nil {
		keySource = oldItem
	}

	record := models.StreamRecord{
		EventID:                     fmt.Sprintf("%s-%s", t.Name, seq),
		EventName:                   eventName,
		SequenceNumber:              seq,
		ApproximateCreationDateTime: time.Now().Unix(),
		Keys:                        t.extractStreamKeys(keySource),
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
		// Keys only — no image data included.
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

	// Forward to any configured Kinesis destinations. The emitter is invoked
	// asynchronously via a goroutine inside KinesisEmitter implementations to
	// avoid holding table.mu during the network/RPC.
	if len(t.KinesisDestinations) > 0 && t.kinesisEmitter != nil {
		for _, arn := range t.KinesisDestinations {
			t.kinesisEmitter.EmitDynamoDBStreamRecord(arn, t.Name, record)
		}
	}
}

// KinesisEmitter forwards DynamoDB stream records to configured Kinesis destinations.
// Implementations must be safe to call while the caller holds locks; they should
// return promptly and dispatch work to a background goroutine if needed.
type KinesisEmitter interface {
	EmitDynamoDBStreamRecord(streamARN, tableName string, record models.StreamRecord)
}

// SetKinesisEmitter installs a Kinesis emitter for all tables in this DB.
// Safe to call once during service wiring.
func (db *InMemoryDB) SetKinesisEmitter(emitter KinesisEmitter) {
	db.mu.Lock("SetKinesisEmitter")
	defer db.mu.Unlock()

	db.kinesisEmitter = emitter
	for _, region := range db.Tables {
		for _, t := range region {
			t.kinesisEmitter = emitter
		}
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

// Purge removes tables and backups created before the cutoff time.
func (db *InMemoryDB) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}

	db.mu.Lock("Purge")
	defer db.mu.Unlock()

	if !db.purgeActiveTables(ctx, cutoff) {
		return
	}

	if !db.purgeStreamARNIndex(ctx, cutoff) {
		return
	}

	if !db.purgeBackups(ctx, cutoff) {
		return
	}

	db.purgeGlobalTables(ctx, cutoff)
}

// purgeActiveTables removes active tables created before cutoff.
// Returns false if ctx is cancelled mid-loop.
func (db *InMemoryDB) purgeActiveTables(ctx context.Context, cutoff time.Time) bool {
	for _, regionTables := range db.Tables {
		for n, table := range regionTables {
			if ctx.Err() != nil {
				return false
			}
			if table.CreationDateTime.Before(cutoff) {
				stopTableTimers(table)
				if table.Tags != nil {
					table.Tags.Close()
				}
				table.mu.Close()
				delete(regionTables, n)
			}
		}
	}

	return true
}

// purgeStreamARNIndex removes stream ARN index entries for tables deleted before cutoff.
// Returns false if ctx is cancelled mid-loop.
func (db *InMemoryDB) purgeStreamARNIndex(ctx context.Context, cutoff time.Time) bool {
	for arn, table := range db.streamARNIndex {
		if ctx.Err() != nil {
			return false
		}
		if table.CreationDateTime.Before(cutoff) {
			delete(db.streamARNIndex, arn)
		}
	}

	return true
}

// purgeBackups removes backups created before cutoff.
// Returns false if ctx is cancelled mid-loop.
func (db *InMemoryDB) purgeBackups(ctx context.Context, cutoff time.Time) bool {
	for n, backup := range db.Backups {
		if ctx.Err() != nil {
			return false
		}
		if backup.CreationDateTime.Before(cutoff) {
			delete(db.Backups, n)
		}
	}

	return true
}

// purgeGlobalTables removes global tables created before cutoff.
func (db *InMemoryDB) purgeGlobalTables(ctx context.Context, cutoff time.Time) {
	for n, gt := range db.GlobalTables {
		if ctx.Err() != nil {
			return
		}
		if gt.CreationDateTime.Before(cutoff) {
			delete(db.GlobalTables, n)
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
	db.GlobalTables = make(map[string]*StoredGlobalTable)
	db.exports = make(map[string]storedExport)
	db.imports = make(map[string]storedImport)
	db.txnTokens = make(map[string]time.Time)
	db.txnPending = make(map[string]time.Time)
	db.fisReplicationPaused = make(map[string]time.Time)
	if db.exprCache != nil {
		db.exprCache.Close()
	}
	db.exprCache = NewExpressionCache(exprCacheSize)
}

// regionFromARN extracts the region from a DynamoDB table ARN.
// ARN format: arn:aws:dynamodb:<region>:<account>:table/<name>
// Returns the default region if the ARN is empty or cannot be parsed.
func (db *InMemoryDB) regionFromARN(tableARN string) string {
	// arn:aws:dynamodb:us-east-1:123456789012:table/MyTable
	// split by ":" gives ["arn", "aws", "dynamodb", "<region>", ...]
	parts := strings.Split(tableARN, ":")
	if len(parts) >= 4 && parts[3] != "" {
		return parts[3]
	}

	return db.defaultRegion
}

// storeExport persists an export record so it can be retrieved by DescribeExport/ListExports.
func (db *InMemoryDB) storeExport(desc exportDescriptionFields) {
	db.mu.Lock("storeExport")
	defer db.mu.Unlock()

	rec := storedExport{
		CreatedAt:    time.Now(),
		ExportArn:    desc.ExportArn,
		ExportStatus: desc.ExportStatus,
		TableArn:     desc.TableArn,
		S3Bucket:     desc.S3Bucket,
	}
	db.exports[desc.ExportArn] = rec
	evictOldest(db.exports, maxExportsRetained, func(v storedExport) time.Time { return v.CreatedAt })
}

// evictOldest drops oldest-by-CreatedAt entries from m until len(m) <= keep.
// We evict on insert rather than on a timer so memory stays bounded even when
// the janitor is disabled. timeOf returns the entry's creation timestamp.
func evictOldest[V any](m map[string]V, keep int, timeOf func(V) time.Time) {
	if len(m) <= keep {
		return
	}

	type kv struct {
		t time.Time
		k string
	}

	entries := make([]kv, 0, len(m))
	for k, v := range m {
		entries = append(entries, kv{timeOf(v), k})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].t.Before(entries[j].t) })

	for i := range len(m) - keep {
		delete(m, entries[i].k)
	}
}

// lookupExport retrieves a stored export by ARN.
func (db *InMemoryDB) lookupExport(exportARN string) (exportDescriptionFields, bool) {
	db.mu.RLock("lookupExport")
	defer db.mu.RUnlock()

	e, ok := db.exports[exportARN]
	if !ok {
		return exportDescriptionFields{}, false
	}

	return exportDescriptionFields{
		ExportArn:    e.ExportArn,
		ExportStatus: e.ExportStatus,
		TableArn:     e.TableArn,
		S3Bucket:     e.S3Bucket,
	}, true
}

// listExportsWire returns all stored exports as wire-format structs, optionally
// filtered by tableArn. nextToken is reserved for future pagination support.
func (db *InMemoryDB) listExportsWire(tableArn, _ string) *listExportsOutput {
	db.mu.RLock("listExportsWire")

	summaries := make([]exportDescriptionFields, 0, len(db.exports))
	for _, e := range db.exports {
		if tableArn != "" && e.TableArn != tableArn {
			continue
		}

		summaries = append(summaries, exportDescriptionFields{
			ExportArn:    e.ExportArn,
			ExportStatus: e.ExportStatus,
			TableArn:     e.TableArn,
			S3Bucket:     e.S3Bucket,
		})
	}

	db.mu.RUnlock()

	// Sort by ARN for deterministic ordering.
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].ExportArn < summaries[j].ExportArn
	})

	return &listExportsOutput{ExportSummaries: summaries}
}

// storeImport persists an import record so it can be retrieved by DescribeImport/ListImports.
func (db *InMemoryDB) storeImport(imp storedImport) {
	db.mu.Lock("storeImport")
	defer db.mu.Unlock()

	if imp.CreatedAt.IsZero() {
		imp.CreatedAt = time.Now()
	}
	db.imports[imp.ImportArn] = imp
	evictOldest(db.imports, maxImportsRetained, func(v storedImport) time.Time { return v.CreatedAt })
}

// lookupImport retrieves a stored import by ARN.
func (db *InMemoryDB) lookupImport(importARN string) (storedImport, bool) {
	db.mu.RLock("lookupImport")
	defer db.mu.RUnlock()

	imp, ok := db.imports[importARN]

	return imp, ok
}

// listImportsStored returns all stored imports as a slice, sorted by ARN.
func (db *InMemoryDB) listImportsStored() []storedImport {
	db.mu.RLock("listImportsStored")

	result := make([]storedImport, 0, len(db.imports))
	for _, imp := range db.imports {
		result = append(result, imp)
	}

	db.mu.RUnlock()

	sort.Slice(result, func(i, j int) bool {
		return result[i].ImportArn < result[j].ImportArn
	})

	return result
}
