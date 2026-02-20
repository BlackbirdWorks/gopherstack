package dynamodb_test

import (
	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractKeySchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*testing.T, *dynamodb.InMemoryDB, string)
		check func(*testing.T, *dynamodb.InMemoryDB, string)
		name  string
	}{
		{
			name: "MainTableSchema",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB, tableName string) {
				t.Helper()
				ctInput := models.CreateTableInput{
					TableName: tableName,
					KeySchema: []models.KeySchemaElement{
						{AttributeName: "pk", KeyType: models.KeyTypeHash},
						{AttributeName: "sk", KeyType: models.KeyTypeRange},
					},
					AttributeDefinitions: []models.AttributeDefinition{
						{AttributeName: "pk", AttributeType: "S"},
						{AttributeName: "sk", AttributeType: "N"},
					},
				}
				_, err := db.CreateTable(t.Context(), models.ToSDKCreateTableInput(&ctInput))
				require.NoError(t, err)
			},
			check: func(t *testing.T, db *dynamodb.InMemoryDB, tableName string) {
				t.Helper()
				table := db.Tables[tableName]
				schema, idx, err := db.ExtractKeySchema(table, "")
				require.NoError(t, err)
				assert.Nil(t, idx)
				require.Len(t, schema, 2)
				assert.Equal(t, "pk", schema[0].AttributeName)
				assert.Equal(t, "sk", schema[1].AttributeName)
			},
		},
		{
			name: "GSISchema",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB, tableName string) {
				t.Helper()
				ctInput := models.CreateTableInput{
					TableName: tableName,
					KeySchema: []models.KeySchemaElement{
						{AttributeName: "pk", KeyType: models.KeyTypeHash},
					},
					AttributeDefinitions: []models.AttributeDefinition{
						{AttributeName: "pk", AttributeType: "S"},
						{AttributeName: "gsiPK", AttributeType: "S"},
						{AttributeName: "gsiSK", AttributeType: "N"},
					},
					GlobalSecondaryIndexes: []models.GlobalSecondaryIndex{
						{
							IndexName: "GSI1",
							KeySchema: []models.KeySchemaElement{
								{AttributeName: "gsiPK", KeyType: models.KeyTypeHash},
								{AttributeName: "gsiSK", KeyType: models.KeyTypeRange},
							},
							Projection: models.Projection{ProjectionType: "ALL"},
						},
					},
				}
				_, err := db.CreateTable(t.Context(), models.ToSDKCreateTableInput(&ctInput))
				require.NoError(t, err)
			},
			check: func(t *testing.T, db *dynamodb.InMemoryDB, tableName string) {
				t.Helper()
				table := db.Tables[tableName]
				schema, idx, err := db.ExtractKeySchema(table, "GSI1")
				require.NoError(t, err)
				require.NotNil(t, idx)
				assert.Equal(t, "ALL", idx.ProjectionType)
				require.Len(t, schema, 2)
				assert.Equal(t, "gsiPK", schema[0].AttributeName)
				assert.Equal(t, "gsiSK", schema[1].AttributeName)
			},
		},
		{
			name: "InvalidIndex",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB, tableName string) {
				t.Helper()
				ctInput := models.CreateTableInput{
					TableName: tableName,
					KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
					AttributeDefinitions: []models.AttributeDefinition{
						{AttributeName: "pk", AttributeType: "S"},
					},
				}
				_, _ = db.CreateTable(t.Context(), models.ToSDKCreateTableInput(&ctInput))
			},
			check: func(t *testing.T, db *dynamodb.InMemoryDB, tableName string) {
				t.Helper()
				table := db.Tables[tableName]
				_, _, err := db.ExtractKeySchema(table, "InvalidIndex")
				require.Error(t, err)
				assert.Contains(t, err.Error(), "not found")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			tableName := "SchemaTable"

			if tt.setup != nil {
				tt.setup(t, db, tableName)
			}
			if tt.check != nil {
				tt.check(t, db, tableName)
			}
		})
	}
}

func TestSortCandidates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		skAttr     string
		skType     string
		candidates []map[string]any
		want       []string
		forward    bool
	}{
		{
			name: "NumberSortingForward",
			candidates: []map[string]any{
				{"sk_n": map[string]any{"N": "10"}},
				{"sk_n": map[string]any{"N": "2"}},
				{"sk_n": map[string]any{"N": "30"}},
			},
			skAttr:  "sk_n",
			skType:  "N",
			forward: true,
			want:    []string{"2", "10", "30"},
		},
		{
			name: "NumberSortingReverse",
			candidates: []map[string]any{
				{"sk_n": map[string]any{"N": "10"}},
				{"sk_n": map[string]any{"N": "2"}},
				{"sk_n": map[string]any{"N": "30"}},
			},
			skAttr:  "sk_n",
			skType:  "N",
			forward: false,
			want:    []string{"30", "10", "2"},
		},
		{
			name: "StringSortingForward",
			candidates: []map[string]any{
				{"sk_s": map[string]any{"S": "c"}},
				{"sk_s": map[string]any{"S": "a"}},
				{"sk_s": map[string]any{"S": "b"}},
			},
			skAttr:  "sk_s",
			skType:  "S",
			forward: true,
			want:    []string{"a", "b", "c"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			table := &dynamodb.Table{
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: tt.skAttr, AttributeType: tt.skType},
				},
			}
			skDef := models.KeySchemaElement{AttributeName: tt.skAttr, KeyType: "RANGE"}

			db.SortCandidates(tt.candidates, skDef, table, tt.forward)

			for i, val := range tt.want {
				got := tt.candidates[i][tt.skAttr].(map[string]any)[tt.skType]
				assert.Equal(t, val, got)
			}
		})
	}
}

func TestFindExclusiveStartIndex(t *testing.T) {
	t.Parallel()

	items := []map[string]any{
		{"pk": map[string]any{"S": "1"}},
		{"pk": map[string]any{"S": "2"}},
		{"pk": map[string]any{"S": "3"}},
	}
	keySchema := []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}}

	tests := []struct {
		startKey map[string]any
		name     string
		want     int
	}{
		{
			name:     "Found",
			startKey: map[string]any{"pk": map[string]any{"S": "2"}},
			want:     2,
		},
		{
			name:     "NotFound",
			startKey: map[string]any{"pk": map[string]any{"S": "4"}},
			want:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			idx := dynamodb.FindExclusiveStartIndex(items, tt.startKey, keySchema)
			assert.Equal(t, tt.want, idx)
		})
	}
}

func TestCompareAny(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		v1   any
		v2   any
		typ  string
		want int
	}{
		{"Number <", "10", "20", "N", -1},
		{"Number >", "20", "10", "N", 1},
		{"Number =", "10", "10", "N", 0},
		{"String <", "a", "b", "S", -1},
		{"String >", "b", "a", "S", 1},
		{"String =", "a", "a", "S", 0},
		{"Nil v1", nil, "a", "S", 0},
		{"Nil v2", "a", nil, "S", 0},
		{"Float <", 10.5, 20.5, "N", -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dynamodb.CompareAny(tc.v1, tc.v2, tc.typ)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestToString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		val  any
		want string
	}{
		{"str", "str"},
		{123, "123"},
		{true, "true"},
		{false, "false"},
		{nil, ""},
		{[]string{"a", "b"}, `["a","b"]`},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			got := dynamodb.ToString(tc.val)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestApplyGSIProjection(t *testing.T) {
	t.Parallel()

	item := map[string]any{
		"pk":    map[string]any{"S": "p"},
		"sk":    map[string]any{"N": "1"},
		"other": map[string]any{"S": "o"},
		"gsiPK": map[string]any{"S": "gp"},
		"gsiSK": map[string]any{"N": "2"},
	}
	keySchema := []models.KeySchemaElement{
		{AttributeName: "gsiPK", KeyType: "HASH"},
		{AttributeName: "gsiSK", KeyType: "RANGE"},
	}
	tableKeySchema := []models.KeySchemaElement{
		{AttributeName: "pk", KeyType: "HASH"},
		{AttributeName: "sk", KeyType: "RANGE"},
	}

	tests := []struct {
		name string
		proj models.Projection
		want int
	}{
		{
			name: "KEYS_ONLY",
			proj: models.Projection{ProjectionType: "KEYS_ONLY"},
			want: 4,
		},
		{
			name: "INCLUDE",
			proj: models.Projection{
				ProjectionType:   "INCLUDE",
				NonKeyAttributes: []string{"other"},
			},
			want: 5,
		},
		{
			name: "ALL",
			proj: models.Projection{ProjectionType: "ALL"},
			want: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			filtered := dynamodb.ApplyGSIProjection(item, tt.proj, tableKeySchema, keySchema)
			assert.Len(t, filtered, tt.want)
			if tt.name == "KEYS_ONLY" {
				assert.NotContains(t, filtered, "other")
			}
		})
	}
}

func TestRebuildIndexes(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	tableName := "RebuildTable"

	ctInput := models.CreateTableInput{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: models.KeyTypeHash},
			{AttributeName: "sk", KeyType: models.KeyTypeRange},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
			{AttributeName: "sk", AttributeType: "N"},
		},
	}
	_, err := db.CreateTable(context.Background(), models.ToSDKCreateTableInput(&ctInput))
	require.NoError(t, err)

	table := db.Tables[tableName]
	table.InitializeIndexes()
	assert.Empty(t, table.PKIndex())

	item1 := map[string]any{"pk": map[string]any{"S": "p1"}, "sk": map[string]any{"N": "1"}}
	item2 := map[string]any{"pk": map[string]any{"S": "p1"}, "sk": map[string]any{"N": "2"}}
	table.Items = append(table.Items, item1, item2)

	table.RebuildIndexes()

	assert.NotNil(t, table.PKSKIndex()["p1"])
	assert.Equal(t, 0, table.PKSKIndex()["p1"]["1"])
}

func TestParseStr(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "test", dynamodb.ParseStr("test"))
	assert.Equal(t, "123", dynamodb.ParseStr(123))
	assert.Equal(t, "true", dynamodb.ParseStr(true))
}

func TestCalculateItemSize_Complex(t *testing.T) {
	t.Parallel()
	item := map[string]any{
		"pk": map[string]any{"S": strings.Repeat("a", 1000)},
		"list": map[string]any{"L": []any{
			map[string]any{"S": "val1"},
			map[string]any{"N": "123"},
		}},
	}
	size, err := dynamodb.CalculateItemSize(item)
	require.NoError(t, err)
	assert.Greater(t, size, 1000)
}
