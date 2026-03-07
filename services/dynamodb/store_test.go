package dynamodb_test

import (
	"context"

	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_Operations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*testing.T, *dynamodb.InMemoryDB)
		check func(*testing.T, *dynamodb.InMemoryDB)
		name  string
	}{
		{
			name: "ListEmpty",
			check: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				tables := db.ListAllTables()
				assert.Empty(t, tables)
			},
		},
		{
			name: "CreateTableAndList",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "Table1", "id")
			},
			check: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				tables := db.ListAllTables()
				require.Len(t, tables, 1)
				assert.Equal(t, "Table1", tables[0].Name)
			},
		},
		{
			name: "GetTable",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "Table1", "id")
			},
			check: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				table, ok := db.GetTable("Table1")
				require.True(t, ok)
				assert.Equal(t, "Table1", table.Name)

				_, ok = db.GetTable("Missing")
				assert.False(t, ok)
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

			if tt.check != nil {
				tt.check(t, db)
			}
		})
	}
}

func TestInMemoryDB_TaggedTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, db *dynamodb.InMemoryDB)
		name    string
		wantLen int
	}{
		{
			name:    "no tables returns empty slice",
			wantLen: 0,
		},
		{
			name: "table without tags appears in result",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "NoTagTable", "pk")
			},
			wantLen: 1,
		},
		{
			name: "table with tags appears in result",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "TaggedTable", "pk")
				ctx := context.Background()
				_, err := db.TagResource(ctx, &dynamodb_sdk.TagResourceInput{
					ResourceArn: aws.String("arn:aws:dynamodb:us-east-1:123456789012:table/TaggedTable"),
					Tags: []types.Tag{
						{Key: aws.String("env"), Value: aws.String("test")},
					},
				})
				require.NoError(t, err)
			},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			if tt.setup != nil {
				tt.setup(t, db)
			}

			result := db.TaggedTables()
			assert.Len(t, result, tt.wantLen)
		})
	}
}

func TestInMemoryDB_CreateTableInRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableName string
		region    string
		wantErr   bool
	}{
		{
			name:      "creates table in specified region",
			tableName: "RegionTestTable",
			region:    "eu-west-1",
		},
		{
			name:      "duplicate table in same region returns error",
			tableName: "DupTable",
			region:    "ap-southeast-1",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			ctx := context.Background()

			input := &dynamodb_sdk.CreateTableInput{
				TableName: aws.String(tt.tableName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
			}

			_, err := db.CreateTableInRegion(ctx, input, tt.region)
			require.NoError(t, err)

			if tt.wantErr {
				_, err = db.CreateTableInRegion(ctx, input, tt.region)
				require.Error(t, err)
			} else {
				tbl, ok := db.GetTableInRegion(tt.tableName, tt.region)
				require.True(t, ok)
				assert.Equal(t, tt.tableName, tbl.Name)
			}
		})
	}
}
