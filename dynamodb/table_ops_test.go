package dynamodb_test

import (
	"Gopherstack/dynamodb/models"
	"slices"
	"testing"

	"Gopherstack/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func TestTableOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*dynamodb.InMemoryDB)
		validate func(*testing.T, *dynamodb.InMemoryDB, any, error)
		run      func(*dynamodb.InMemoryDB) (any, error)
		name     string
	}{
		{
			name: "CreateTable_Success",
			run: func(db *dynamodb.InMemoryDB) (any, error) {
				input := models.CreateTableInput{
					TableName: "TestTable",
					KeySchema: []models.KeySchemaElement{
						{AttributeName: "pk", KeyType: models.KeyTypeHash},
					},
					AttributeDefinitions: []models.AttributeDefinition{
						{AttributeName: "pk", AttributeType: "S"},
					},
				}
				sdkInput := models.ToSDKCreateTableInput(&input)

				return db.CreateTable(sdkInput)
			},
			validate: func(t *testing.T, _ *dynamodb.InMemoryDB, resp any, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				output := resp.(*dynamodb_sdk.CreateTableOutput)
				if aws.ToString(output.TableDescription.TableName) != "TestTable" {
					t.Errorf("Expected table name TestTable, got %s", aws.ToString(output.TableDescription.TableName))
				}
			},
		},
		{
			name: "CreateTable_AlreadyExists",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "ExistingTable")
			},
			run: func(db *dynamodb.InMemoryDB) (any, error) {
				input := models.CreateTableInput{
					TableName: "ExistingTable",
					KeySchema: []models.KeySchemaElement{
						{AttributeName: "pk", KeyType: models.KeyTypeHash},
					},
					AttributeDefinitions: []models.AttributeDefinition{
						{AttributeName: "pk", AttributeType: "S"},
					},
				}
				sdkInput := models.ToSDKCreateTableInput(&input)

				return db.CreateTable(sdkInput)
			},
			validate: func(t *testing.T, _ *dynamodb.InMemoryDB, _ any, err error) {
				t.Helper()
				if err == nil {
					t.Error("Expected error for existing table, got nil")
				}
			},
		},
		{
			name: "DescribeTable_Success",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "TestTable")
			},
			run: func(db *dynamodb.InMemoryDB) (any, error) {
				input := models.DescribeTableInput{TableName: "TestTable"}
				sdkInput := models.ToSDKDescribeTableInput(&input)

				return db.DescribeTable(sdkInput)
			},
			validate: func(t *testing.T, _ *dynamodb.InMemoryDB, resp any, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				output := resp.(*dynamodb_sdk.DescribeTableOutput)
				if aws.ToString(output.Table.TableName) != "TestTable" {
					t.Errorf("Expected table name TestTable, got %s", aws.ToString(output.Table.TableName))
				}
				if output.Table.TableStatus != "ACTIVE" {
					t.Errorf("Expected status ACTIVE, got %s", output.Table.TableStatus)
				}
			},
		},
		{
			name: "DescribeTable_NotFound",
			run: func(db *dynamodb.InMemoryDB) (any, error) {
				input := models.DescribeTableInput{TableName: "NonExistent"}
				sdkInput := models.ToSDKDescribeTableInput(&input)

				return db.DescribeTable(sdkInput)
			},
			validate: func(t *testing.T, _ *dynamodb.InMemoryDB, _ any, err error) {
				t.Helper()
				if err == nil {
					t.Error("Expected error for non-existent table, got nil")
				}
			},
		},
		{
			name: "ListTables_Success",
			setup: func(db *dynamodb.InMemoryDB) {
				createTable(db, "Table1")
			},
			run: func(db *dynamodb.InMemoryDB) (any, error) {
				return db.ListTables(&dynamodb_sdk.ListTablesInput{})
			},
			validate: func(t *testing.T, _ *dynamodb.InMemoryDB, resp any, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				output := resp.(*dynamodb_sdk.ListTablesOutput)
				found := slices.Contains(output.TableNames, "Table1")
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
			run: func(db *dynamodb.InMemoryDB) (any, error) {
				input := models.DeleteTableInput{TableName: "DeleteMe"}
				sdkInput := models.ToSDKDeleteTableInput(&input)

				return db.DeleteTable(sdkInput)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, _ any, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				// Verify deletion by trying to describe it
				descInput := models.DescribeTableInput{TableName: "DeleteMe"}
				sdkDesc := models.ToSDKDescribeTableInput(&descInput)
				_, err = db.DescribeTable(sdkDesc)
				if err == nil {
					t.Error("Table should not exist after deletion")
				}
			},
		},
		{
			name: "DeleteTable_NotFound",
			run: func(db *dynamodb.InMemoryDB) (any, error) {
				input := models.DeleteTableInput{TableName: "NonExistent"}
				sdkInput := models.ToSDKDeleteTableInput(&input)

				return db.DeleteTable(sdkInput)
			},
			validate: func(t *testing.T, _ *dynamodb.InMemoryDB, _ any, err error) {
				t.Helper()
				if err == nil {
					t.Error("Expected error for non-existent table, got nil")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			if tt.setup != nil {
				tt.setup(db)
			}

			resp, err := tt.run(db)

			if tt.validate != nil {
				tt.validate(t, db, resp, err)
			}
		})
	}
}

func createTable(db *dynamodb.InMemoryDB, name string) {
	input := models.CreateTableInput{
		TableName:            name,
		KeySchema:            []models.KeySchemaElement{{AttributeName: "id", KeyType: models.KeyTypeHash}},
		AttributeDefinitions: []models.AttributeDefinition{{AttributeName: "id", AttributeType: "S"}},
	}
	_, _ = db.CreateTable(models.ToSDKCreateTableInput(&input))
}
