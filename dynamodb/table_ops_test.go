package dynamodb_test

import (
	"context"
	"errors"
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"

	"github.com/blackbirdworks/gopherstack/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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

				return db.CreateTable(context.Background(), sdkInput)
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

				return db.CreateTable(context.Background(), sdkInput)
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

				return db.DescribeTable(context.Background(), sdkInput)
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

				return db.DescribeTable(context.Background(), sdkInput)
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
				return db.ListTables(context.Background(), &dynamodb_sdk.ListTablesInput{})
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

				return db.DeleteTable(context.Background(), sdkInput)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, _ any, err error) {
				t.Helper()
				require.NoError(t, err)
				// Verify deletion by trying to describe it
				descInput := models.DescribeTableInput{TableName: "DeleteMe"}
				sdkDesc := models.ToSDKDescribeTableInput(&descInput)
				_, err = db.DescribeTable(context.Background(), sdkDesc)
				require.Error(t, err)
			},
		},
		{
			name: "DeleteTable_WithGSI_NilProvisionedThroughput",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				// Create a table with a GSI but no ProvisionedThroughput (on-demand billing)
				_, err := db.CreateTable(context.Background(), &dynamodb_sdk.CreateTableInput{
					TableName: aws.String("GSITable"),
					AttributeDefinitions: []types.AttributeDefinition{
						{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
						{AttributeName: aws.String("gsiPK"), AttributeType: types.ScalarAttributeTypeS},
					},
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					},
					GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
						{
							IndexName: aws.String("GSI1"),
							KeySchema: []types.KeySchemaElement{
								{AttributeName: aws.String("gsiPK"), KeyType: types.KeyTypeHash},
							},
							Projection: &types.Projection{
								ProjectionType: types.ProjectionTypeAll,
							},
						},
					},
					BillingMode: types.BillingModePayPerRequest,
				})
				require.NoError(t, err)
			},
			run: func(db *dynamodb.InMemoryDB) (any, error) {
				input := models.DeleteTableInput{TableName: "GSITable"}
				sdkInput := models.ToSDKDeleteTableInput(&input)

				return db.DeleteTable(context.Background(), sdkInput)
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, _ any, err error) {
				t.Helper()
				require.NoError(t, err)
				// Verify deletion
				descInput := models.DescribeTableInput{TableName: "GSITable"}
				sdkDesc := models.ToSDKDescribeTableInput(&descInput)
				_, err = db.DescribeTable(context.Background(), sdkDesc)
				require.Error(t, err)
			},
		},
		{
			name: "DeleteTable_NotFound",
			run: func(db *dynamodb.InMemoryDB) (any, error) {
				input := models.DeleteTableInput{TableName: "NonExistent"}
				sdkInput := models.ToSDKDeleteTableInput(&input)

				return db.DeleteTable(context.Background(), sdkInput)
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
		{
			name: "DeleteTable_Cleanup",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTable(t, db, "CleanupTable")
			},
			run: func(db *dynamodb.InMemoryDB) (any, error) {
				input := models.DeleteTableInput{TableName: "CleanupTable"}
				sdkInput := models.ToSDKDeleteTableInput(&input)

				return db.DeleteTable(context.Background(), sdkInput)
			},
			validate: func(t *testing.T, _ *dynamodb.InMemoryDB, _ any, err error) {
				t.Helper()
				require.NoError(t, err)
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
		TableName: name,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "id", KeyType: models.KeyTypeHash},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
		},
	}
	_, err := db.CreateTable(context.Background(), models.ToSDKCreateTableInput(&input))
	require.NoError(t, err)
}
