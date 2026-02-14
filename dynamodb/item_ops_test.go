package dynamodb_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"Gopherstack/dynamodb"
)

func TestItemOperations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		setup         func(*dynamodb.InMemoryDB)
		operation     func(*dynamodb.InMemoryDB) (interface{}, error)
		validate      func(*testing.T, *dynamodb.InMemoryDB, interface{}, error)
		expectedError bool
	}{
		// ... (content omitted for brevity, will rely on start/end lines matching or Context)
		// Actually I need to be careful with replace.
		// Let's just add t.Parallel() at start and inside loop.

		{
			name: "PutItem_Success",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "ItemsTable")
			},
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.PutItemInput{
					TableName: "ItemsTable",
					Item: map[string]interface{}{
						"id":  map[string]interface{}{"S": "1"},
						"val": map[string]interface{}{"S": "data"},
					},
				}
				body, _ := json.Marshal(input)
				return db.PutItem(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				// Verify item exists
				input := dynamodb.GetItemInput{
					TableName: "ItemsTable",
					Key: map[string]interface{}{
						"id": map[string]interface{}{"S": "1"},
					},
				}
				body, _ := json.Marshal(input)
				getResp, _ := db.GetItem(body)
				output := getResp.(dynamodb.GetItemOutput)
				if output.Item == nil {
					t.Error("Item should exist after PutItem")
				}
			},
		},
		{
			name: "PutItem_Overwrite",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "ItemsTable")
				putItem(db, "ItemsTable", "1", "original")
			},
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.PutItemInput{
					TableName: "ItemsTable",
					Item: map[string]interface{}{
						"id":  map[string]interface{}{"S": "1"},
						"val": map[string]interface{}{"S": "updated"},
					},
				}
				body, _ := json.Marshal(input)
				return db.PutItem(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				// Verify item updated
				input := dynamodb.GetItemInput{
					TableName: "ItemsTable",
					Key: map[string]interface{}{
						"id": map[string]interface{}{"S": "1"},
					},
				}
				body, _ := json.Marshal(input)
				getResp, _ := db.GetItem(body)
				output := getResp.(dynamodb.GetItemOutput)
				val := output.Item["val"].(map[string]interface{})["S"]
				if val != "updated" {
					t.Errorf("Expected value 'updated', got '%v'", val)
				}
			},
		},
		{
			name: "PutItem_ReturnValues_ALL_OLD",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "ItemsTable")
				putItem(db, "ItemsTable", "1", "original")
			},
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.PutItemInput{
					TableName: "ItemsTable",
					Item: map[string]interface{}{
						"id":  map[string]interface{}{"S": "1"},
						"val": map[string]interface{}{"S": "updated"},
					},
					ReturnValues: "ALL_OLD",
				}
				body, _ := json.Marshal(input)
				return db.PutItem(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				output := resp.(dynamodb.PutItemOutput)
				if output.Attributes == nil {
					t.Fatal("Expected Attributes to be returned")
				}
				val := output.Attributes["val"].(map[string]interface{})["S"]
				if val != "original" {
					t.Errorf("Expected old value 'original', got '%v'", val)
				}
			},
		},
		{
			name: "PutItem_ReturnConsumedCapacity",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "ItemsTable")
			},
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.PutItemInput{
					TableName:              "ItemsTable",
					Item:                   map[string]interface{}{"pk": map[string]interface{}{"S": "1"}},
					ReturnConsumedCapacity: "TOTAL",
				}
				body, _ := json.Marshal(input)
				return db.PutItem(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				output := resp.(dynamodb.PutItemOutput)
				if output.ConsumedCapacity == nil {
					t.Fatal("Expected ConsumedCapacity to be returned")
				}
				if output.ConsumedCapacity.CapacityUnits != 1.0 {
					t.Errorf("Expected CapacityUnits 1.0, got %f", output.ConsumedCapacity.CapacityUnits)
				}
			},
		},
		{
			name: "PutItem_ReturnItemCollectionMetrics",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "ItemsTable")
			},
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.PutItemInput{
					TableName:                   "ItemsTable",
					Item:                        map[string]interface{}{"pk": map[string]interface{}{"S": "1"}},
					ReturnItemCollectionMetrics: "SIZE",
				}
				body, _ := json.Marshal(input)
				return db.PutItem(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				output := resp.(dynamodb.PutItemOutput)
				if output.ItemCollectionMetrics == nil {
					t.Fatal("Expected ItemCollectionMetrics to be returned")
				}
				// Verify pk is in ItemCollectionKey
				pkVal, ok := output.ItemCollectionMetrics.ItemCollectionKey["pk"].(map[string]interface{})["S"]
				if !ok || pkVal != "1" {
					t.Errorf("Expected ItemCollectionKey to contain pk='1', got %v", output.ItemCollectionMetrics.ItemCollectionKey)
				}
			},
		},
		{
			name: "GetItem_Success",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "ItemsTable")
				putItem(db, "ItemsTable", "1", "data")
			},
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.GetItemInput{
					TableName: "ItemsTable",
					Key: map[string]interface{}{
						"id": map[string]interface{}{"S": "1"},
					},
				}
				body, _ := json.Marshal(input)
				return db.GetItem(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				output := resp.(dynamodb.GetItemOutput)
				expected := map[string]interface{}{
					"id":  map[string]interface{}{"S": "1"},
					"val": map[string]interface{}{"S": "data"},
				}
				if !reflect.DeepEqual(output.Item, expected) {
					t.Errorf("Expected item %v, got %v", expected, output.Item)
				}
			},
		},
		{
			name: "GetItem_NotFound",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "ItemsTable")
			},
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.GetItemInput{
					TableName: "ItemsTable",
					Key: map[string]interface{}{
						"id": map[string]interface{}{"S": "999"},
					},
				}
				body, _ := json.Marshal(input)
				return db.GetItem(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				output := resp.(dynamodb.GetItemOutput)
				if output.Item != nil {
					t.Errorf("Expected nil item, got %v", output.Item)
				}
			},
		},
		{
			name: "DeleteItem_Success",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "ItemsTable")
				putItem(db, "ItemsTable", "1", "data")
			},
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.DeleteItemInput{
					TableName: "ItemsTable",
					Key: map[string]interface{}{
						"id": map[string]interface{}{"S": "1"},
					},
				}
				body, _ := json.Marshal(input)
				return db.DeleteItem(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				// Verify item is gone
				input := dynamodb.GetItemInput{
					TableName: "ItemsTable",
					Key: map[string]interface{}{
						"id": map[string]interface{}{"S": "1"},
					},
				}
				body, _ := json.Marshal(input)
				getResp, _ := db.GetItem(body)
				output := getResp.(dynamodb.GetItemOutput)
				if output.Item != nil {
					t.Error("Item should be nil after deletion")
				}
			},
		},
		{
			name: "Scan_Success",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "ItemsTable")
				putItem(db, "ItemsTable", "1", "data1")
				putItem(db, "ItemsTable", "2", "data2")
			},
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.ScanInput{TableName: "ItemsTable"}
				body, _ := json.Marshal(input)
				return db.Scan(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				output := resp.(dynamodb.ScanOutput)
				if output.Count != 2 {
					t.Errorf("Expected count 2, got %d", output.Count)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			if tt.setup != nil {
				tt.setup(db)
			}
			resp, err := tt.operation(db)
			if tt.expectedError {
				if err == nil {
					t.Errorf("Expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
			if tt.validate != nil {
				tt.validate(t, db, resp, err)
			}
		})
	}
}

func putItem(db *dynamodb.InMemoryDB, tableName, id, val string) {
	input := dynamodb.PutItemInput{
		TableName: tableName,
		Item: map[string]interface{}{
			"id":  map[string]interface{}{"S": id},
			"val": map[string]interface{}{"S": val},
		},
	}
	body, _ := json.Marshal(input)
	_, _ = db.PutItem(body)
}
