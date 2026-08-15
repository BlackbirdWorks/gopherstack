package dynamodb

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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
	CreationDateTime   time.Time                         `json:"CreationDateTime"`
	WriteCapacityUnits *int64                            `json:"WriteCapacityUnits,omitempty"`
	ReplicaSettings    map[string]*StoredReplicaSettings `json:"ReplicaSettings,omitempty"`
	GlobalTableName    string                            `json:"GlobalTableName"`
	GlobalTableArn     string                            `json:"GlobalTableArn"`
	BillingMode        string                            `json:"BillingMode,omitempty"`
	ReplicationGroup   []string                          `json:"ReplicationGroup"`
}

// StoredReplicaSettings holds per-replica settings persisted by UpdateGlobalTableSettings.
type StoredReplicaSettings struct {
	ReadCapacityUnits *int64 `json:"ReadCapacityUnits,omitempty"`
	TableClass        string `json:"TableClass,omitempty"`
}

// KinesisDestinationEntry stores a Kinesis streaming destination with its configuration.
type KinesisDestinationEntry struct {
	StreamARN string `json:"StreamARN"`
	// Precision is "MICROSECOND" or "MILLISECOND"; empty means MILLISECOND (AWS default).
	Precision string `json:"Precision,omitempty"`
}

// storedExport holds the fields needed to satisfy DescribeExport and ListExports.
type storedExport struct {
	CreatedAt       time.Time
	StartTime       time.Time
	EndTime         time.Time
	ExportTime      time.Time
	ExportArn       string
	ExportStatus    string
	TableArn        string
	S3Bucket        string
	S3Prefix        string
	ExportFormat    string
	ExportType      string
	ExportManifest  string
	FailureCode     string
	FailureMessage  string
	BilledSizeBytes int64
	ItemCount       int64
}

// storedImport holds the fields needed to satisfy DescribeImport and ListImports.
type storedImport struct {
	CreatedAt             time.Time
	StartTime             time.Time
	EndTime               time.Time
	S3Prefix              string
	InputFormat           string
	TableArn              string
	TableID               string
	ClientToken           string
	CloudWatchLogGroupArn string
	S3Bucket              string
	ImportArn             string
	S3BucketOwner         string
	ImportStatus          string
	InputCompression      string
	CsvDelimiter          string
	FailureMessage        string
	FailureCode           string
	CsvHeaderList         []string
	ImportedItemCount     int64
	ProcessedItemCount    int64
	ProcessedSizeBytes    int64
	ErrorCount            int64
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
// Snapshots are taken by the janitor's PITR ticker (defaultPITRSnapshotInterval
// in janitor.go); RestoreTableToPointInTime returns the latest snapshot at or
// before the requested RestoreDateTime. The type stays unexported, but its
// fields carry json tags since it's serialised as part of Table.PITRSnapshots.
type pitrSnapshot struct {
	Taken time.Time        `json:"Taken"`
	Items []map[string]any `json:"Items"`
}

// maxPITRSnapshots bounds the per-table snapshot ring so memory cost stays
// predictable. With the janitor's 1-minute PITR ticker (defaultPITRSnapshotInterval
// in janitor.go) this gives ~1 hour of point-in-time recovery coverage — enough for
// tests, well short of AWS's real 35-day window.
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
//
// tables, deletingTables, backups, globalTables, exports, imports, and
// streamARNIndex are *store.Table wrappers (see pkgs/store and
// store_setup.go) registered once on registry at construction time;
// txnTokens, txnPending, and fisReplicationPaused remain plain maps because
// their value type (time.Time) carries no identity a store.Table key
// function could extract -- see store_setup.go's doc comment.
type InMemoryDB struct {
	tables         *store.Table[Table] // key: tableKey(region, name)
	tablesByRegion *store.Index[Table] // secondary index: region -> tables in that region
	// deletingTables uses the same key scheme as tables; it is a staging area
	// for deleted tables, drained by the janitor's runTableCleaner.
	deletingTables *store.Table[Table]
	backups        *store.Table[Backup]            // key: BackupArn
	globalTables   *store.Table[StoredGlobalTable] // key: GlobalTableName
	exports        *store.Table[storedExport]      // key: ExportArn
	imports        *store.Table[storedImport]      // key: ImportArn
	// streamARNIndex is a reverse index (key: StreamARN) onto the same
	// *Table pointers stored in `tables` -- see store_setup.go's
	// streamARNKeyFn doc for why this can't be a store.Index.
	streamARNIndex       *store.Table[Table]
	registry             *store.Registry
	txnTokens            map[string]time.Time // committed idempotency tokens → expiry time
	txnPending           map[string]time.Time // in-progress idempotency tokens → start time
	fisReplicationPaused map[string]time.Time // keyed by table ARN; value is expiry (zero = no expiry)
	exprCache            *ExpressionCache
	throttler            *Throttler
	iteratorStore        *ShardIteratorStore // opaque shard iterator tokens
	mu                   *lockmetrics.RWMutex
	// kinesisEmitter forwards stream records to Kinesis destinations when configured.
	kinesisEmitter KinesisEmitter
	// s3 is the cross-service S3 backend used by ImportTable (reads source objects)
	// and ExportTableToPointInTime (writes export data). nil when not wired.
	s3            S3Accessor
	defaultRegion string
	accountID     string
	// createDelay is the time to wait before transitioning a new table to ACTIVE.
	// Zero means immediate ACTIVE (no lifecycle simulation).
	createDelay time.Duration
}

// Backup holds the metadata and a point-in-time item snapshot for a DynamoDB on-demand backup.
type Backup struct {
	CreationDateTime       time.Time                               `json:"CreationDateTime"`
	SSEKMSMasterKeyArn     string                                  `json:"SSEKMSMasterKeyArn,omitempty"`
	SSEType                string                                  `json:"SSEType,omitempty"`
	StreamViewType         string                                  `json:"StreamViewType,omitempty"`
	BackupName             string                                  `json:"BackupName"`
	BackupType             string                                  `json:"BackupType"`
	TableArn               string                                  `json:"TableArn"`
	TableID                string                                  `json:"TableID"`
	BackupArn              string                                  `json:"BackupArn"`
	BackupStatus           string                                  `json:"BackupStatus"`
	TableName              string                                  `json:"TableName"`
	BillingMode            string                                  `json:"BillingMode,omitempty"`
	KeySchema              []models.KeySchemaElement               `json:"KeySchema"`
	LocalSecondaryIndexes  []models.LocalSecondaryIndex            `json:"LocalSecondaryIndexes,omitempty"`
	GlobalSecondaryIndexes []models.GlobalSecondaryIndex           `json:"GlobalSecondaryIndexes,omitempty"`
	Items                  []map[string]any                        `json:"Items"`
	AttributeDefinitions   []models.AttributeDefinition            `json:"AttributeDefinitions"`
	ProvisionedThroughput  models.ProvisionedThroughputDescription `json:"ProvisionedThroughput"`
	SizeBytes              int64                                   `json:"SizeBytes"`
	SSEEnabled             bool                                    `json:"SSEEnabled,omitempty"`
	StreamsEnabled         bool                                    `json:"StreamsEnabled,omitempty"`
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

type Table struct {
	StreamCreatedAt               time.Time `json:"StreamCreatedAt"`
	CreationDateTime              time.Time `json:"CreationDateTime"`
	ContributorInsightsLastUpdate time.Time `json:"ContributorInsightsLastUpdate"`
	kinesisEmitter                KinesisEmitter
	pkIndex                       map[string]int
	pkskIndex                     map[string]map[string]int
	// gsiIndexes and lsiIndexes are keyed by IndexName; each maps that index's
	// key value(s) to the set of item offsets sharing them (see
	// secondary_index.go). Derived from Items + GlobalSecondaryIndexes/
	// LocalSecondaryIndexes and rebuilt by rebuildIndexes -- never persisted,
	// so adding them is not a snapshot-version change.
	gsiIndexes map[string]*secondaryIndex
	lsiIndexes map[string]*secondaryIndex
	// activeSecondaryIndex is scratch space set on the throwaway snapshot
	// Table built per-Query call (see snapshotTableForQuery); it holds a
	// deep copy of the one GSI/LSI index the query targets, same role as
	// itemsByOffset plays for primary-key queries.
	activeSecondaryIndex    *secondaryIndex
	itemsByOffset           map[int]map[string]any
	mu                      *lockmetrics.RWMutex
	activateTimer           *time.Timer
	Tags                    *tags.Tags                    `json:"Tags,omitempty"`
	AutoScaling             *autoScalingSettings          `json:"AutoScaling,omitempty"`
	OnDemandMaxWriteRRU     *int64                        `json:"OnDemandMaxWriteRRU,omitempty"`
	OnDemandMaxReadRRU      *int64                        `json:"OnDemandMaxReadRRU,omitempty"`
	ResourcePolicy          string                        `json:"ResourcePolicy,omitempty"`
	ResourcePolicyRevision  string                        `json:"ResourcePolicyRevision,omitempty"`
	TTLAttribute            string                        `json:"TTLAttribute,omitempty"`
	StreamViewType          string                        `json:"StreamViewType,omitempty"`
	StreamARN               string                        `json:"StreamARN,omitempty"`
	GlobalTableName         string                        `json:"GlobalTableName,omitempty"`
	TableArn                string                        `json:"TableArn"`
	Status                  string                        `json:"Status"`
	TableID                 string                        `json:"TableID"`
	SSEType                 string                        `json:"SSEType,omitempty"`
	TableClass              string                        `json:"TableClass,omitempty"`
	BillingMode             string                        `json:"BillingMode,omitempty"`
	Name                    string                        `json:"Name"`
	SSEKMSMasterKeyArn      string                        `json:"SSEKMSMasterKeyArn,omitempty"`
	ContributorInsightsMode string                        `json:"ContributorInsightsMode,omitempty"`
	AttributeDefinitions    []models.AttributeDefinition  `json:"AttributeDefinitions"`
	GlobalSecondaryIndexes  []models.GlobalSecondaryIndex `json:"GlobalSecondaryIndexes,omitempty"`
	Replicas                []models.ReplicaDescription   `json:"Replicas,omitempty"`
	LocalSecondaryIndexes   []models.LocalSecondaryIndex  `json:"LocalSecondaryIndexes,omitempty"`
	KeySchema               []models.KeySchemaElement     `json:"KeySchema"`
	KinesisDestinations     []KinesisDestinationEntry     `json:"KinesisDestinations,omitempty"`
	Items                   []map[string]any              `json:"Items"`
	itemSizes               []int
	// PITRSnapshots is the per-table PITR ring buffer (see pitrSnapshot). It must be
	// exported with a json tag -- encoding/json silently skips unexported fields, so an
	// unexported name here means every PITR snapshot is discarded on restart even
	// though PITREnabled (persisted) still reports ENABLED. Do not revert this to
	// unexported; see dynamodbSnapshotVersion's comment in persistence.go for why
	// adding this field did not require bumping the snapshot version.
	PITRSnapshots              []pitrSnapshot `json:"PITRSnapshots,omitempty"`
	streamShards               []StreamShard
	StreamRecords              []models.StreamRecord                   `json:"StreamRecords,omitempty"`
	ProvisionedThroughput      models.ProvisionedThroughputDescription `json:"ProvisionedThroughput"`
	totalItemSizeBytes         int64
	streamSeq                  int64
	StreamHead                 int `json:"StreamHead,omitempty"`
	streamTrimSeq              int64
	PITREnabled                bool  `json:"PITREnabled,omitempty"`
	RecoveryPeriodInDays       int32 `json:"RecoveryPeriodInDays,omitempty"`
	SSEEnabled                 bool  `json:"SSEEnabled,omitempty"`
	StreamsEnabled             bool  `json:"StreamsEnabled"`
	DeletionProtectionEnabled  bool  `json:"DeletionProtectionEnabled"`
	ContributorInsightsEnabled bool  `json:"ContributorInsightsEnabled,omitempty"`
}

func NewInMemoryDB() *InMemoryDB {
	const exprCacheSize = 1000

	db := &InMemoryDB{
		registry:             store.NewRegistry(),
		txnTokens:            make(map[string]time.Time),
		txnPending:           make(map[string]time.Time),
		fisReplicationPaused: make(map[string]time.Time),
		exprCache:            NewExpressionCache(exprCacheSize),
		iteratorStore:        NewShardIteratorStore(),
		defaultRegion:        config.DefaultRegion,
		accountID:            config.DefaultAccountID,
		mu:                   lockmetrics.New("ddb"),
		throttler:            NewThrottler(false),
	}
	registerAllTables(db)

	return db
}

// Close releases all backend resources.
func (db *InMemoryDB) Close() {
	db.mu.Lock("Close")
	defer db.mu.Unlock()

	for _, table := range db.tables.All() {
		stopTableTimers(table)
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
func (t *Table) appendStreamRecord(
	eventName string,
	oldItem, newImage map[string]any,
	principalID, principalType string,
) {
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
		EventID:                     strings.ReplaceAll(uuid.NewString(), "-", ""),
		EventName:                   eventName,
		SequenceNumber:              seq,
		ApproximateCreationDateTime: time.Now().Unix(),
		Keys:                        t.extractStreamKeys(keySource),
		UserIdentityPrincipalID:     principalID,
		UserIdentityType:            principalType,
	}

	switch t.StreamViewType {
	case streamViewTypeNewAndOldImages:
		record.OldImage = oldItem
		record.NewImage = newImage
		record.StreamViewType = "NEW_AND_OLD_IMAGES"
	case streamViewTypeNewImage:
		record.NewImage = newImage
		record.StreamViewType = "NEW_IMAGE"
	case streamViewTypeOldImage:
		record.OldImage = oldItem
		record.StreamViewType = "OLD_IMAGE"
	case streamViewTypeKeysOnly:
		record.StreamViewType = "KEYS_ONLY"
	default:
		record.OldImage = oldItem
		record.NewImage = newImage
		record.StreamViewType = "NEW_AND_OLD_IMAGES"
	}

	var size int64
	if s, err := CalculateItemSize(record.Keys); err == nil {
		size += int64(s)
	}
	if s, err := CalculateItemSize(record.OldImage); err == nil {
		size += int64(s)
	}
	if s, err := CalculateItemSize(record.NewImage); err == nil {
		size += int64(s)
	}
	record.SizeBytes = size

	// O(1) ring buffer: pre-allocate once, then overwrite in-place.
	// When the buffer is not yet full, append normally. Once full, overwrite
	// the oldest slot (at StreamHead) and advance the head pointer.
	if len(t.StreamRecords) < maxStreamRecords {
		t.StreamRecords = append(t.StreamRecords, record)
	} else {
		t.overwriteRingSlot(record)
	}

	// Forward to any configured Kinesis destinations. The emitter is invoked
	// asynchronously via a goroutine inside KinesisEmitter implementations to
	// avoid holding table.mu during the network/RPC.
	if len(t.KinesisDestinations) > 0 && t.kinesisEmitter != nil {
		for _, dest := range t.KinesisDestinations {
			t.kinesisEmitter.EmitDynamoDBStreamRecord(dest.StreamARN, t.Name, record)
		}
	}
}

// overwriteRingSlot handles the full-buffer path: optionally splits the current shard,
// advances the trim horizon, and overwrites the oldest ring slot.
// Must be called with table.mu held (write lock).
func (t *Table) overwriteRingSlot(record models.StreamRecord) {
	// When StreamHead wraps to 0 we completed a full ring rotation — model as a shard split.
	if t.StreamHead == 0 && len(t.streamShards) > 0 {
		t.splitActiveShard()
	}

	// Advance the trim horizon: the record at StreamHead is being evicted.
	trimmedRecord := t.StreamRecords[t.StreamHead]
	if seq, perr := parseSeqNum(trimmedRecord.SequenceNumber); perr == nil {
		t.streamTrimSeq = seq + 1
	}

	t.StreamRecords[t.StreamHead] = record
	t.StreamHead = (t.StreamHead + 1) % maxStreamRecords
}

// splitActiveShard closes the current open shard and opens a new child shard.
// Must be called with table.mu held (write lock).
func (t *Table) splitActiveShard() {
	last := &t.streamShards[len(t.streamShards)-1]
	if last.EndingSequenceNum != 0 {
		return // already closed
	}

	endSeq := max(t.streamSeq-int64(maxStreamRecords), last.StartingSequenceNum)
	last.EndingSequenceNum = endSeq
	newIdx := int64(len(t.streamShards) + 1)
	t.streamShards = append(t.streamShards, StreamShard{
		ShardID:             fmt.Sprintf("shardId-%020d-00000001", newIdx),
		ParentShardID:       last.ShardID,
		StartingSequenceNum: t.streamSeq - int64(maxStreamRecords) + 1,
	})
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
	for _, t := range db.tables.All() {
		t.kinesisEmitter = emitter
	}
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
		// Buffer not yet full.
		res := make([]models.StreamRecord, n)
		copy(res, t.StreamRecords)

		return res, nil
	}

	// Ring is full: split at StreamHead.
	tail := make([]models.StreamRecord, n-t.StreamHead)
	copy(tail, t.StreamRecords[t.StreamHead:])

	head := make([]models.StreamRecord, t.StreamHead)
	copy(head, t.StreamRecords[:t.StreamHead])

	return tail, head
}

func BuildKeyString(item map[string]any, attrName string) string {
	if attrName == "" {
		return ""
	}

	return dynamoattr.ToString(item[attrName])
}

// initializeIndexes creates empty index maps for a table, including one
// secondaryIndex per currently-defined GSI/LSI.
func (t *Table) initializeIndexes() {
	hasSortKey := len(t.KeySchema) > 1

	if hasSortKey {
		t.pkskIndex = make(map[string]map[string]int)
	} else {
		t.pkIndex = make(map[string]int)
	}

	t.gsiIndexes = make(map[string]*secondaryIndex, len(t.GlobalSecondaryIndexes))
	for _, gsi := range t.GlobalSecondaryIndexes {
		_, skDef := getPKAndSK(gsi.KeySchema)
		t.gsiIndexes[gsi.IndexName] = newSecondaryIndex(skDef.AttributeName != "")
	}

	t.lsiIndexes = make(map[string]*secondaryIndex, len(t.LocalSecondaryIndexes))
	for _, lsi := range t.LocalSecondaryIndexes {
		_, skDef := getPKAndSK(lsi.KeySchema)
		t.lsiIndexes[lsi.IndexName] = newSecondaryIndex(skDef.AttributeName != "")
	}
}

// rebuildIndexes rebuilds all indexes from existing items (used after table creation or batch updates).
func (t *Table) rebuildIndexes() {
	t.initializeIndexes()

	// Rebuild the item-size accounting alongside the key indexes. itemSizes has
	// no JSON tag, so it is empty after a snapshot Restore even though Items is
	// populated; recomputing here restores the len(itemSizes) == len(Items)
	// invariant that the CRUD paths (doUpdate/doPut/deleteItemAtIndex) rely on.
	// Without this, the first write against a restored/rebuilt item panics with
	// an itemSizes index out of range.
	t.itemSizes = make([]int, len(t.Items))
	t.totalItemSizeBytes = 0

	pkDef, skDef := getPKAndSK(t.KeySchema)
	hasSortKey := skDef.AttributeName != ""

	for i, item := range t.Items {
		size, _ := CalculateItemSize(item)
		t.itemSizes[i] = size
		t.totalItemSizeBytes += int64(size)

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

		t.updateSecondaryIndexes(nil, 0, item, i)
	}
}

// Regions returns all distinct regions that contain at least one table.
func (db *InMemoryDB) Regions() []string {
	db.mu.RLock("Regions")
	defer db.mu.RUnlock()

	seen := make(map[string]struct{})
	for _, t := range db.tables.All() {
		seen[tableRegion(t)] = struct{}{}
	}

	regions := make([]string, 0, len(seen))
	for r := range seen {
		regions = append(regions, r)
	}

	sort.Strings(regions)

	return regions
}

// TableNamesByRegion returns table names in the given region, or all regions if region is empty.
func (db *InMemoryDB) TableNamesByRegion(region string) []string {
	db.mu.RLock("TableNamesByRegion")
	defer db.mu.RUnlock()

	var names []string

	if region != "" {
		for _, t := range db.tablesByRegion.Get(region) {
			names = append(names, t.Name)
		}
	} else {
		for _, t := range db.tables.All() {
			names = append(names, t.Name)
		}
	}

	sort.Strings(names)

	return names
}

// ListAllTables returns a slice of all tables across all regions (for UI).
func (db *InMemoryDB) ListAllTables() []*Table {
	db.mu.RLock("ListAllTables")
	defer db.mu.RUnlock()

	return db.tables.All()
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

	return db.tables.Get(tableKey(region, name))
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

	all := db.tables.All()
	result := make([]TaggedTableInfo, 0, len(all))

	for _, table := range all {
		tagMap := cloneTagsRLocked(table)
		result = append(result, TaggedTableInfo{ARN: table.TableArn, Tags: tagMap})
	}

	return result
}

// stopTableTimers stops all in-flight timers held by the table -- the activation
// timer for newly-created tables and the index-status timers for any GSI mid-
// CREATING or mid-DELETING transition. Must be called before the table is
// discarded so the AfterFunc goroutines aren't left running. Idempotent.
//
// Takes table.mu itself; callers (DeleteTable, the janitor's runTableCleaner)
// must NOT already hold it. Required, not cosmetic -- see
// TestStopTableTimers_ConcurrentGSIDelete_NoPanic for the index-out-of-range
// panic this prevents.
func stopTableTimers(table *Table) {
	table.mu.Lock("stopTableTimers")
	defer table.mu.Unlock()

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
	for _, table := range db.tables.All() {
		if ctx.Err() != nil {
			return false
		}
		if table.CreationDateTime.Before(cutoff) {
			stopTableTimers(table)
			if table.Tags != nil {
				table.Tags.Close()
			}
			table.mu.Close()
			db.tables.Delete(tableKeyFn(table))
		}
	}

	return true
}

// purgeStreamARNIndex removes stream ARN index entries for tables deleted before cutoff.
// Returns false if ctx is cancelled mid-loop.
func (db *InMemoryDB) purgeStreamARNIndex(ctx context.Context, cutoff time.Time) bool {
	for _, table := range db.streamARNIndex.All() {
		if ctx.Err() != nil {
			return false
		}
		if table.CreationDateTime.Before(cutoff) {
			db.streamARNIndex.Delete(table.StreamARN)
		}
	}

	return true
}

// purgeBackups removes backups created before cutoff.
// Returns false if ctx is cancelled mid-loop.
func (db *InMemoryDB) purgeBackups(ctx context.Context, cutoff time.Time) bool {
	for _, backup := range db.backups.All() {
		if ctx.Err() != nil {
			return false
		}
		if backup.CreationDateTime.Before(cutoff) {
			db.backups.Delete(backup.BackupArn)
		}
	}

	return true
}

// purgeGlobalTables removes global tables created before cutoff.
func (db *InMemoryDB) purgeGlobalTables(ctx context.Context, cutoff time.Time) {
	for _, gt := range db.globalTables.All() {
		if ctx.Err() != nil {
			return
		}
		if gt.CreationDateTime.Before(cutoff) {
			db.globalTables.Delete(gt.GlobalTableName)
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
	for _, table := range db.tables.All() {
		stopTableTimers(table)
		if table.Tags != nil {
			table.Tags.Close()
		}

		table.mu.Close()
	}

	for _, table := range db.deletingTables.All() {
		stopTableTimers(table)
		if table.Tags != nil {
			table.Tags.Close()
		}

		table.mu.Close()
	}

	// registry.ResetAll() collapses tables/deletingTables/backups/globalTables/
	// exports/imports/streamARNIndex to one call; only the plain (non-store)
	// maps need explicit resets below.
	db.registry.ResetAll()
	db.txnTokens = make(map[string]time.Time)
	db.txnPending = make(map[string]time.Time)
	db.fisReplicationPaused = make(map[string]time.Time)
	db.iteratorStore = NewShardIteratorStore()
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

	now := time.Now()
	rec := &storedExport{
		CreatedAt:       now,
		StartTime:       now,
		ExportArn:       desc.ExportArn,
		ExportStatus:    desc.ExportStatus,
		TableArn:        desc.TableArn,
		S3Bucket:        desc.S3Bucket,
		S3Prefix:        desc.S3Prefix,
		ExportFormat:    desc.ExportFormat,
		ExportType:      desc.ExportType,
		ExportManifest:  desc.ExportManifest,
		FailureCode:     desc.FailureCode,
		FailureMessage:  desc.FailureMessage,
		BilledSizeBytes: desc.BilledSizeBytes,
		ItemCount:       desc.ItemCount,
	}
	if desc.ExportTime != 0 {
		rec.ExportTime = time.Unix(int64(desc.ExportTime), 0)
	} else {
		rec.ExportTime = time.Now()
	}

	db.exports.Put(rec)
	evictOldestFromTable(
		db.exports,
		maxExportsRetained,
		exportKeyFn,
		func(v *storedExport) time.Time { return v.CreatedAt },
	)
}

// lookupExport retrieves a stored export by ARN.
func (db *InMemoryDB) lookupExport(exportARN string) (exportDescriptionFields, bool) {
	db.mu.RLock("lookupExport")
	defer db.mu.RUnlock()

	e, ok := db.exports.Get(exportARN)
	if !ok {
		return exportDescriptionFields{}, false
	}

	desc := exportDescriptionFields{
		ExportArn:       e.ExportArn,
		ExportStatus:    e.ExportStatus,
		TableArn:        e.TableArn,
		S3Bucket:        e.S3Bucket,
		S3Prefix:        e.S3Prefix,
		ExportFormat:    e.ExportFormat,
		ExportType:      e.ExportType,
		ExportManifest:  e.ExportManifest,
		FailureCode:     e.FailureCode,
		FailureMessage:  e.FailureMessage,
		BilledSizeBytes: e.BilledSizeBytes,
		ItemCount:       e.ItemCount,
	}
	if !e.StartTime.IsZero() {
		desc.StartTime = float64(e.StartTime.Unix())
	}
	if !e.EndTime.IsZero() {
		desc.EndTime = float64(e.EndTime.Unix())
	}
	if !e.ExportTime.IsZero() {
		desc.ExportTime = float64(e.ExportTime.Unix())
	}

	return desc, true
}

// updateExport merges completed-export fields into an existing storedExport record.
func (db *InMemoryDB) updateExport(
	exportARN string,
	status, manifest, failureCode, failureMessage string,
	itemCount, billedBytes int64,
) {
	db.mu.Lock("updateExport")
	defer db.mu.Unlock()

	e, ok := db.exports.Get(exportARN)
	if !ok {
		return
	}
	e.ExportStatus = status
	e.ExportManifest = manifest
	e.FailureCode = failureCode
	e.FailureMessage = failureMessage
	e.ItemCount = itemCount
	e.BilledSizeBytes = billedBytes
	e.EndTime = time.Now()
	// e is the live pointer stored in db.exports (ExportArn, its key, never
	// changes), so mutating it in place is sufficient -- no Put needed.
}

// listExportsWire returns stored exports filtered by requestRegion and optionally by
// tableArn. nextToken is an opaque cursor (exclusive-start ARN); maxResults caps page size.
// exportToSummaryFields projects a stored export into its wire summary,
// deriving the optional timestamp fields (defaulting ExportTime to StartTime).
func exportToSummaryFields(e storedExport) exportDescriptionFields {
	d := exportDescriptionFields{
		ExportArn:       e.ExportArn,
		ExportStatus:    e.ExportStatus,
		TableArn:        e.TableArn,
		S3Bucket:        e.S3Bucket,
		S3Prefix:        e.S3Prefix,
		ExportFormat:    e.ExportFormat,
		ExportType:      e.ExportType,
		BilledSizeBytes: e.BilledSizeBytes,
		ItemCount:       e.ItemCount,
	}
	if !e.StartTime.IsZero() {
		d.StartTime = float64(e.StartTime.Unix())
	}

	if !e.EndTime.IsZero() {
		d.EndTime = float64(e.EndTime.Unix())
	}

	switch {
	case !e.ExportTime.IsZero():
		d.ExportTime = float64(e.ExportTime.Unix())
	case !e.StartTime.IsZero():
		d.ExportTime = float64(e.StartTime.Unix())
	}

	return d
}

// collectExportSummariesRLocked filters db.exports by region/tableArn and
// converts matches to their wire summary shape, under a defer-protected
// db.mu.RLock.
func (db *InMemoryDB) collectExportSummariesRLocked(tableArn, requestRegion string) []exportDescriptionFields {
	db.mu.RLock("listExportsWire")
	defer db.mu.RUnlock()

	all := db.exports.All()
	summaries := make([]exportDescriptionFields, 0, len(all))
	for _, e := range all {
		if db.regionFromARN(e.ExportArn) != requestRegion {
			continue
		}

		if tableArn != "" && e.TableArn != tableArn {
			continue
		}

		summaries = append(summaries, exportToSummaryFields(*e))
	}

	return summaries
}

// storeImport persists an import record so it can be retrieved by DescribeImport/ListImports.
func (db *InMemoryDB) storeImport(imp storedImport) {
	db.mu.Lock("storeImport")
	defer db.mu.Unlock()

	if imp.CreatedAt.IsZero() {
		imp.CreatedAt = time.Now()
	}
	db.imports.Put(&imp)
	evictOldestFromTable(
		db.imports,
		maxImportsRetained,
		importKeyFn,
		func(v *storedImport) time.Time { return v.CreatedAt },
	)
}

// lookupImport retrieves a stored import by ARN.
func (db *InMemoryDB) lookupImport(importARN string) (storedImport, bool) {
	db.mu.RLock("lookupImport")
	defer db.mu.RUnlock()

	imp, ok := db.imports.Get(importARN)
	if !ok {
		return storedImport{}, false
	}

	return *imp, true
}

// listImportsStored returns all stored imports as a slice, sorted by ARN.
func (db *InMemoryDB) listImportsStored() []storedImport {
	result := db.allImportsRLocked()

	sort.Slice(result, func(i, j int) bool {
		return result[i].ImportArn < result[j].ImportArn
	})

	return result
}

// allImportsRLocked returns a copy of every stored import under a
// defer-protected db.mu.RLock.
func (db *InMemoryDB) allImportsRLocked() []storedImport {
	db.mu.RLock("listImportsStored")
	defer db.mu.RUnlock()

	all := db.imports.All()
	result := make([]storedImport, 0, len(all))
	for _, imp := range all {
		result = append(result, *imp)
	}

	return result
}
