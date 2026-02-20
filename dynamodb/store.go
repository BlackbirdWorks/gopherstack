package dynamodb

import (
	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// InMemoryDB stores tables and items.
type InMemoryDB struct {
	Tables    map[string]*Table
	exprCache *ExpressionCache
	mu        *lockmetrics.RWMutex
}

type Table struct {
	pkIndex   map[string]int
	pkskIndex map[string]map[string]int
	// mu is placed before the slice fields so that Items' non-pointer
	// len+cap words fall outside the GC scan range (176 → 160 pointer bytes).
	mu                     *lockmetrics.RWMutex
	Name                   string
	TTLAttribute           string
	KeySchema              []models.KeySchemaElement
	AttributeDefinitions   []models.AttributeDefinition
	GlobalSecondaryIndexes []models.GlobalSecondaryIndex
	LocalSecondaryIndexes  []models.LocalSecondaryIndex
	Items                  []map[string]any
}

func NewInMemoryDB() *InMemoryDB {
	const exprCacheSize = 1000

	return &InMemoryDB{
		Tables:    make(map[string]*Table),
		exprCache: NewExpressionCache(exprCacheSize),
		mu:        lockmetrics.New("ddb"),
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
