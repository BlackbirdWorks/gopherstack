package dynamodb_test

import (
	"Gopherstack/dynamodb/models"
	"errors"
	"testing"

	"Gopherstack/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *dynamodb.InMemoryDB)
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
				require.NoError(t, err)
				output := resp.(*dynamodb_sdk.CreateTableOutput)
				assert.Equal(t, "TestTable", aws.ToString(output.TableDescription.TableName))
			},
		},
		{
			name: "CreateTable_AlreadyExists",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTable(t, db, "ExistingTable")
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
				require.Error(t, err)
			},
		},
		{
			name: "DescribeTable_Success",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTable(t, db, "TestTable")
			},
			run: func(db *dynamodb.InMemoryDB) (any, error) {
				input := models.DescribeTableInput{TableName: "TestTable"}
				sdkInput := models.ToSDKDescribeTableInput(&input)

				return db.DescribeTable(sdkInput)
			},
			validate: func(t *testing.T, _ *dynamodb.InMemoryDB, resp any, err error) {
				t.Helper()
				require.NoError(t, err)
				output := resp.(*dynamodb_sdk.DescribeTableOutput)
				assert.Equal(t, "TestTable", aws.ToString(output.Table.TableName))
				assert.Equal(t, "ACTIVE", string(output.Table.TableStatus))
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
				require.Error(t, err)
			},
		},
		{
			name: "ListTables_Success",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTable(t, db, "Table1")
			},
			run: func(db *dynamodb.InMemoryDB) (any, error) {
				return db.ListTables(&dynamodb_sdk.ListTablesInput{})
			},
			validate: func(t *testing.T, _ *dynamodb.InMemoryDB, resp any, err error) {
				t.Helper()
				require.NoError(t, err)
				output := resp.(*dynamodb_sdk.ListTablesOutput)
				assert.Contains(t, output.TableNames, "Table1")
			},
		},
		{
			name: "DeleteTable_Success",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTable(t, db, "DeleteMe")
			},
			run: func(db *dynamodb.InMemoryDB) (any, error) {
				input := models.DeleteTableInput{TableName: "DeleteMe"}
				sdkInput := models.ToSDKDeleteTableInput(&input)

				return db.DeleteTable(sdkInput)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, _ any, err error) {
				t.Helper()
				require.NoError(t, err)
				// Verify deletion by trying to describe it
				descInput := models.DescribeTableInput{TableName: "DeleteMe"}
				sdkDesc := models.ToSDKDescribeTableInput(&descInput)
				_, err = db.DescribeTable(sdkDesc)
				require.Error(t, err)
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
				require.Error(t, err)
				// Verify it's a ResourceNotFoundException (returns as HTTP 400, not 404)
				var ddbErr *dynamodb.Error
				if errors.As(err, &ddbErr) {
					assert.Contains(t, ddbErr.Type, "ResourceNotFoundException")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			if tt.setup != nil {
				tt.setup(t, db)
			}

			resp, err := tt.run(db)

			if tt.validate != nil {
				tt.validate(t, db, resp, err)
			}
		})
	}
}

func createTable(t *testing.T, db *dynamodb.InMemoryDB, name string) {
	t.Helper()
	input := models.CreateTableInput{
		TableName:            name,
		KeySchema:            []models.KeySchemaElement{{AttributeName: "id", KeyType: models.KeyTypeHash}},
		AttributeDefinitions: []models.AttributeDefinition{{AttributeName: "id", AttributeType: "S"}},
	}
	_, err := db.CreateTable(models.ToSDKCreateTableInput(&input))
	require.NoError(t, err)
}
