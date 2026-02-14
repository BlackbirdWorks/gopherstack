package dynamodb

import (
	"reflect"
	"sync"
)

// InMemoryDB stores tables and items
type InMemoryDB struct {
	Tables map[string]*Table
	mu     sync.RWMutex
}

type Table struct {
	Name                   string
	KeySchema              []KeySchemaElement
	AttributeDefinitions   []AttributeDefinition
	GlobalSecondaryIndexes []GlobalSecondaryIndex
	LocalSecondaryIndexes  []LocalSecondaryIndex
	Items                  []map[string]interface{}
}

type KeySchemaElement struct {
	AttributeName string `json:"AttributeName"`
	KeyType       string `json:"KeyType"` // "HASH" or "RANGE"
}

type AttributeDefinition struct {
	AttributeName string `json:"AttributeName"`
	AttributeType string `json:"AttributeType"`
}

func NewInMemoryDB() *InMemoryDB {
	return &InMemoryDB{
		Tables: make(map[string]*Table),
	}
}

// Helper to check if an item matches the key schema
// item1 is usually the stored item, item2 can be the Key map or another Item map
func itemsMatchKey(item1, item2 map[string]interface{}, schema []KeySchemaElement) bool {
	for _, keyElem := range schema {
		val1, ok1 := item1[keyElem.AttributeName]
		val2, ok2 := item2[keyElem.AttributeName]

		if !ok1 || !ok2 {
			return false
		}

		if !reflect.DeepEqual(val1, val2) {
			return false
		}
	}
	return true
}
