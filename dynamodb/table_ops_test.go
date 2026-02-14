package dynamodb_test

import (
	"encoding/json"
	"testing"

	"Gopherstack/dynamodb"
)

func TestTableOperations(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(*dynamodb.InMemoryDB)
		operation     func(*dynamodb.InMemoryDB) (interface{}, error)
		validate      func(*testing.T, *dynamodb.InMemoryDB, interface{}, error)
		expectedError bool
	}{
		{
			name: "CreateTable_Success",
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.CreateTableInput{
					TableName:            "TestTable",
					KeySchema:            []dynamodb.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
					AttributeDefinitions: []dynamodb.AttributeDefinition{{AttributeName: "pk", AttributeType: "S"}},
				}
				body, _ := json.Marshal(input)
				return db.CreateTable(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				output := resp.(dynamodb.CreateTableOutput)
				if output.TableDescription.TableName != "TestTable" {
					t.Errorf("Expected table name TestTable, got %s", output.TableDescription.TableName)
				}
			},
		},
		{
			name: "CreateTable_AlreadyExists",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "ExistingTable")
			},
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.CreateTableInput{
					TableName:            "ExistingTable",
					KeySchema:            []dynamodb.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
					AttributeDefinitions: []dynamodb.AttributeDefinition{{AttributeName: "pk", AttributeType: "S"}},
				}
				body, _ := json.Marshal(input)
				return db.CreateTable(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err == nil {
					t.Error("Expected error for existing table, got nil")
				}
			},
			expectedError: true,
		},
		{
			name: "DescribeTable_Success",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "TestTable")
			},
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.DescribeTableInput{TableName: "TestTable"}
				body, _ := json.Marshal(input)
				return db.DescribeTable(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				output := resp.(dynamodb.DescribeTableOutput)
				if output.Table.TableName != "TestTable" {
					t.Errorf("Expected table name TestTable, got %s", output.Table.TableName)
				}
				if output.Table.TableStatus != "ACTIVE" {
					t.Errorf("Expected status ACTIVE, got %s", output.Table.TableStatus)
				}
			},
		},
		{
			name: "DescribeTable_NotFound",
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.DescribeTableInput{TableName: "NonExistent"}
				body, _ := json.Marshal(input)
				return db.DescribeTable(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err == nil {
					t.Error("Expected error for non-existent table, got nil")
				}
			},
			expectedError: true,
		},
		{
			name: "ListTables_Success",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "Table1")
			},
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				return db.ListTables([]byte("{}"))
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				output := resp.(dynamodb.ListTablesOutput)
				found := false
				for _, name := range output.TableNames {
					if name == "Table1" {
						found = true
						break
					}
				}
				if !found {
					t.Error("Expected Table1 in list")
				}
			},
		},
		{
			name: "DeleteTable_Success",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "DeleteMe")
			},
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.DeleteTableInput{TableName: "DeleteMe"}
				body, _ := json.Marshal(input)
				return db.DeleteTable(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				// Verify deletion by trying to describe it
				input := dynamodb.DescribeTableInput{TableName: "DeleteMe"}
				body, _ := json.Marshal(input)
				_, err = db.DescribeTable(body)
				if err == nil {
					t.Error("Table should not exist after deletion")
				}
			},
		},
		{
			name: "DeleteTable_NotFound",
			operation: func(db *dynamodb.InMemoryDB) (interface{}, error) {
				input := dynamodb.DeleteTableInput{TableName: "NonExistent"}
				body, _ := json.Marshal(input)
				return db.DeleteTable(body)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, resp interface{}, err error) {
				if err == nil {
					t.Error("Expected error for non-existent table, got nil")
				}
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

// Helpers

func createTable(db *dynamodb.InMemoryDB, name string) {
	input := dynamodb.CreateTableInput{
		TableName:            name,
		KeySchema:            []dynamodb.KeySchemaElement{{AttributeName: "id", KeyType: "HASH"}},
		AttributeDefinitions: []dynamodb.AttributeDefinition{{AttributeName: "id", AttributeType: "S"}},
	}
	body, _ := json.Marshal(input)
	_, _ = db.CreateTable(body)
}
