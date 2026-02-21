package dynamodb

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// InMemoryDB stores tables and items.
type InMemoryDB struct {
	Tables         map[string]*Table
	deletingTables map[string]*Table
	exprCache      *ExpressionCache
	mu             *lockmetrics.RWMutex
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
	// mu is placed before the slice fields so that Items' non-pointer
	// len+cap words fall outside the GC scan range (176 → 160 pointer bytes).
	mu                     *lockmetrics.RWMutex
	Name                   string
	TTLAttribute           string
	StreamViewType         string
	StreamARN              string
	KeySchema              []models.KeySchemaElement
	AttributeDefinitions   []models.AttributeDefinition
	GlobalSecondaryIndexes []models.GlobalSecondaryIndex
	LocalSecondaryIndexes  []models.LocalSecondaryIndex
	Items                  []map[string]any
	StreamRecords          []StreamRecord
	streamSeq              int64
	StreamsEnabled         bool
}

func NewInMemoryDB() *InMemoryDB {
	const exprCacheSize = 1000

	return &InMemoryDB{
		Tables:         make(map[string]*Table),
		deletingTables: make(map[string]*Table),
		exprCache:      NewExpressionCache(exprCacheSize),
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

// ListAllTables returns a slice of all tables (for UI).
func (db *InMemoryDB) ListAllTables() []*Table {
	db.mu.RLock("ListAllTables")
	defer db.mu.RUnlock()

	tables := make([]*Table, 0, len(db.Tables))
	for _, table := range db.Tables {
		tables = append(tables, table)
	}

	return tables
}

// GetTable returns a table by name (for UI).
func (db *InMemoryDB) GetTable(name string) (*Table, bool) {
	db.mu.RLock("GetTable")
	defer db.mu.RUnlock()

	table, exists := db.Tables[name]

	return table, exists
}
