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

// InMemoryDB stores tables and items organized by region.
type InMemoryDB struct {
	Tables         map[string]map[string]*Table
	deletingTables map[string]map[string]*Table
	txnTokens      map[string]struct{} // committed idempotency tokens
	txnPending     map[string]struct{} // in-progress idempotency tokens
	exprCache      *ExpressionCache
	throttler      *Throttler
	mu             *lockmetrics.RWMutex
	defaultRegion  string
	accountID      string
	// createDelay is the time to wait before transitioning a new table to ACTIVE.
	// Zero means immediate ACTIVE (no lifecycle simulation).
	createDelay time.Duration
}

// StreamRecord captures a single item-level change event for DynamoDB Streams.
type StreamRecord struct {
	OldImage                    map[string]any `json:"oldImage,omitempty"`
	NewImage                    map[string]any `json:"newImage,omitempty"`
	EventID                     string         `json:"eventID"`
	EventName                   string         `json:"eventName"`
	SequenceNumber              string         `json:"sequenceNumber"`
	ApproximateCreationDateTime int64          `json:"approximateCreationDateTime"`
}

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
	pkIndex   map[string]int
	pkskIndex map[string]map[string]int
	mu        *lockmetrics.RWMutex
	Tags      *tags.Tags `json:"Tags,omitempty"`
	Name      string     `json:"Name"`
	// Status is the current table status: "CREATING", "ACTIVE", "DELETING", etc.
	Status                 string                                  `json:"Status"`
	TTLAttribute           string                                  `json:"TTLAttribute,omitempty"`
	StreamViewType         string                                  `json:"StreamViewType,omitempty"`
	StreamARN              string                                  `json:"StreamARN,omitempty"`
	TableArn               string                                  `json:"TableArn"`
	TableID                string                                  `json:"TableID"`
	CreationDateTime       time.Time                               `json:"CreationDateTime"`
	AttributeDefinitions   []models.AttributeDefinition            `json:"AttributeDefinitions"`
	GlobalSecondaryIndexes []models.GlobalSecondaryIndex           `json:"GlobalSecondaryIndexes,omitempty"`
	LocalSecondaryIndexes  []models.LocalSecondaryIndex            `json:"LocalSecondaryIndexes,omitempty"`
	Items                  []map[string]any                        `json:"Items"`
	StreamRecords          []StreamRecord                          `json:"StreamRecords,omitempty"`
	KeySchema              []models.KeySchemaElement               `json:"KeySchema"`
	ProvisionedThroughput  models.ProvisionedThroughputDescription `json:"ProvisionedThroughput"`
	streamSeq              int64
	StreamsEnabled         bool `json:"StreamsEnabled"`
}

func NewInMemoryDB() *InMemoryDB {
	const exprCacheSize = 1000

	return &InMemoryDB{
		Tables:         make(map[string]map[string]*Table),
		deletingTables: make(map[string]map[string]*Table),
		txnTokens:      make(map[string]struct{}),
		txnPending:     make(map[string]struct{}),
		exprCache:      NewExpressionCache(exprCacheSize),
		defaultRegion:  config.DefaultRegion,
		accountID:      config.DefaultAccountID,
		mu:             lockmetrics.New("ddb"),
		throttler:      NewThrottler(false),
	}
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

	record := StreamRecord{
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

	// Cap at maxStreamRecords (ring buffer — evict oldest)
	t.StreamRecords = append(t.StreamRecords, record)
	if len(t.StreamRecords) > maxStreamRecords {
		t.StreamRecords = t.StreamRecords[len(t.StreamRecords)-maxStreamRecords:]
	}
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
