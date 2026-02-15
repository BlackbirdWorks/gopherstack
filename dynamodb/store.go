package dynamodb

import (
	"sync"
)

// InMemoryDB stores tables and items.
type InMemoryDB struct {
	Tables    map[string]*Table
	exprCache *ExpressionCache
	mu        sync.RWMutex
}

type Table struct {
	pkIndex                map[string]int
	pkskIndex              map[string]map[string]int
	Name                   string
	TTLAttribute           string // empty when TTL is disabled
	KeySchema              []KeySchemaElement
	AttributeDefinitions   []AttributeDefinition
	GlobalSecondaryIndexes []GlobalSecondaryIndex
	LocalSecondaryIndexes  []LocalSecondaryIndex
	Items                  []map[string]any
	mu                     sync.RWMutex // per-table lock for better concurrency
}

type KeySchemaElement struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"` // KeyTypeHash or "RANGE"
}

type AttributeDefinition struct {
	AttributeName string `json:"AttributeName"`
	AttributeType string `json:"AttributeType"`
}

func NewInMemoryDB() *InMemoryDB {
	const exprCacheSize = 1000

	return &InMemoryDB{
		Tables:    make(map[string]*Table),
		exprCache: NewExpressionCache(exprCacheSize),
	}
}

func BuildKeyString(item map[string]any, attrName string) string {
	if attrName == "" {
		return ""
	}

	return toString(item[attrName])
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
	db.mu.RLock()
	defer db.mu.RUnlock()

	tables := make([]*Table, 0, len(db.Tables))
	for _, table := range db.Tables {
		tables = append(tables, table)
	}

	return tables
}

// GetTable returns a table by name (for UI).
func (db *InMemoryDB) GetTable(name string) (*Table, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	table, exists := db.Tables[name]

	return table, exists
}
