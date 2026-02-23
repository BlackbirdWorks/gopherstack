package dynamodb

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// InMemoryDB stores tables and items organized by region.
type InMemoryDB struct {
	Tables         map[string]map[string]*Table
	deletingTables map[string]map[string]*Table
	exprCache      *ExpressionCache
	mu             *lockmetrics.RWMutex
	defaultRegion  string
	accountID      string
	// createDelay is the time to wait before transitioning a new table to ACTIVE.
	// Zero means immediate ACTIVE (no lifecycle simulation).
	createDelay time.Duration
}

// StreamRecord captures a single item-level change event for DynamoDB Streams.
type StreamRecord struct {
	OldImage                    map[string]any
	NewImage                    map[string]any
	EventID                     string
	EventName                   string
	SequenceNumber              string
	ApproximateCreationDateTime int64
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
	Tags      map[string]string
	Name      string
	// Status is the current table status: "CREATING", "ACTIVE", "DELETING", etc.
	Status                 string
	TTLAttribute           string
	StreamViewType         string
	StreamARN              string
	TableArn               string
	TableID                string
	CreationDateTime       time.Time
	AttributeDefinitions   []models.AttributeDefinition
	GlobalSecondaryIndexes []models.GlobalSecondaryIndex
	LocalSecondaryIndexes  []models.LocalSecondaryIndex
	Items                  []map[string]any
	StreamRecords          []StreamRecord
	KeySchema              []models.KeySchemaElement
	ProvisionedThroughput  models.ProvisionedThroughputDescription
	streamSeq              int64
	StreamsEnabled         bool
}

func NewInMemoryDB() *InMemoryDB {
	const exprCacheSize = 1000

	return &InMemoryDB{
		Tables:         make(map[string]map[string]*Table),
		deletingTables: make(map[string]map[string]*Table),
		exprCache:      NewExpressionCache(exprCacheSize),
		defaultRegion:  "us-east-1",
		accountID:      "000000000000",
		mu:             lockmetrics.New("ddb"),
	}
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
		region = "us-east-1"
	}
	db.defaultRegion = region
}

// SetCreateDelay sets the CREATING → ACTIVE transition delay.
// Call before CreateTable calls; intended for tests and CLI configuration.
func (db *InMemoryDB) SetCreateDelay(d time.Duration) {
	db.createDelay = d
}
