package dynamodb_test

import (
	"context"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/dynamodb/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateDataTypes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		item    map[string]any
		name    string
		wantErr bool
	}{
		{
			name:    "Valid String",
			item:    map[string]any{"pk": map[string]any{"S": "val"}},
			wantErr: false,
		},
		{
			name:    "Valid Number",
			item:    map[string]any{"val": map[string]any{"N": "123"}},
			wantErr: false,
		},
		{
			name:    "Invalid Number",
			item:    map[string]any{"val": map[string]any{"N": "abc"}},
			wantErr: true,
		},
		{
			name:    "Valid Bool",
			item:    map[string]any{"flag": map[string]any{"BOOL": true}},
			wantErr: false,
		},
		{
			name:    "Valid Null",
			item:    map[string]any{"void": map[string]any{"NULL": true}},
			wantErr: false,
		},
		{
			name: "Valid List",
			item: map[string]any{"list": map[string]any{"L": []any{
				map[string]any{"S": "a"},
				map[string]any{"N": "1"},
			}}},
			wantErr: false,
		},
		{
			name: "Valid Map",
			item: map[string]any{"map": map[string]any{"M": map[string]any{
				"key": map[string]any{"S": "val"},
			}}},
			wantErr: false,
		},
		{
			name:    "Unknown Type",
			item:    map[string]any{"bad": map[string]any{"UNKNOWN": "val"}},
			wantErr: true,
		},
		{
			name:    "Multiple Types in Attribute",
			item:    map[string]any{"bad": map[string]any{"S": "val", "N": "1"}},
			wantErr: true,
		},
		{
			name:    "Empty Attribute Value",
			item:    map[string]any{"bad": map[string]any{}},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := dynamodb.ValidateDataTypes(tc.item)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBuildKeyString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		attrName string
		item     map[string]any
		want     string
	}{
		{
			name:     "Single PK (String)",
			attrName: "pk",
			item:     map[string]any{"pk": map[string]any{"S": "val"}},
			want:     "val",
		},
		{
			name:     "Single PK (Number)",
			attrName: "pk",
			item:     map[string]any{"pk": map[string]any{"N": "123"}},
			want:     "123",
		},
		{
			name:     "Missing Key",
			attrName: "other",
			item:     map[string]any{"pk": map[string]any{"S": "val"}},
			want:     "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dynamodb.BuildKeyString(tc.item, tc.attrName)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestCalculateItemSize(t *testing.T) {
	t.Parallel()
	item := map[string]any{
		"pk": map[string]any{"S": "value"},
		"n":  map[string]any{"N": "123"},
	}

	size, err := dynamodb.CalculateItemSize(item)
	require.NoError(t, err)
	assert.Positive(t, size)
}

func TestPutItem_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		item      string
		name      string
		wantError string
	}{
		{
			name:      "Missing PK",
			item:      `{"other": {"S": "val"}}`,
			wantError: "Missing key element: pk",
		},
		{
			name:      "Invalid Data Type (Number)",
			item:      `{"pk": {"S": "val"}, "num": {"N": "abc"}}`,
			wantError: "Attribute num of type N must be a valid number",
		},
		{
			name:      "Nested Map Validation",
			item:      `{"pk": {"S": "val"}, "map": {"M": {"bad": {"N": "abc"}}}}`,
			wantError: "Attribute bad of type N must be a valid number",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			tableName := "ValidationTable"
			ctInput := models.CreateTableInput{
				TableName: tableName,
				KeySchema: []models.KeySchemaElement{
					{AttributeName: "pk", KeyType: models.KeyTypeHash},
				},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
				},
			}
			_, _ = db.CreateTable(context.Background(), models.ToSDKCreateTableInput(&ctInput))

			inputStr := `{"TableName": "` + tableName + `", "Item": ` + tc.item + `}`
			putInput := mustUnmarshal[models.PutItemInput](t, inputStr)
			sdkPut, _ := models.ToSDKPutItemInput(&putInput)

			_, pErr := db.PutItem(context.Background(), sdkPut)
			require.Error(t, pErr)
			if tc.wantError != "" {
				assert.Contains(t, pErr.Error(), tc.wantError)
			}
		})
	}
}

func TestPutItem_ItemTooLarge(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	tableName := "LargeItemTable"
	ctInput := models.CreateTableInput{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: models.KeyTypeHash},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
	}
	_, err := db.CreateTable(context.Background(), models.ToSDKCreateTableInput(&ctInput))
	require.NoError(t, err)

	largeVal := strings.Repeat("a", 400*1024+100)
	input := `{
		"TableName": "` + tableName + `",
		"Item": {
			"pk": {"S": "large"},
			"val": {"S": "` + largeVal + `"}
		}
	}`

	putInput := mustUnmarshal[models.PutItemInput](t, input)
	sdkPut, _ := models.ToSDKPutItemInput(&putInput)
	_, err = db.PutItem(context.Background(), sdkPut)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds limit")
}

func TestCapacityUnits(t *testing.T) {
	t.Parallel()
	item := map[string]any{
		"pk":  map[string]any{"S": "large"},
		"val": map[string]any{"S": strings.Repeat("a", 2000)},
	}

	wcu := dynamodb.WriteCapacityUnits(item)
	assert.GreaterOrEqual(t, wcu, 1.0)

	rcu := dynamodb.ReadCapacityUnits(item)
	assert.GreaterOrEqual(t, rcu, 0.5)

	assert.InDelta(t, 1.0, dynamodb.WriteCapacityUnits(nil), 0.0001)
	assert.InDelta(t, 0.5, dynamodb.ReadCapacityUnits(nil), 0.0001)
}

func TestValidateDataTypes_Sets(t *testing.T) {
	t.Parallel()
	tests := []struct {
		item    map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "Valid SS",
			item: map[string]any{"set": map[string]any{"SS": []any{"a", "b"}}},
		},
		{
			name:    "Empty SS",
			item:    map[string]any{"set": map[string]any{"SS": []any{}}},
			wantErr: true,
		},
		{
			name: "Valid NS",
			item: map[string]any{"set": map[string]any{"NS": []any{"1", "2.5"}}},
		},
		{
			name:    "Invalid NS element",
			item:    map[string]any{"set": map[string]any{"NS": []any{"1", "abc"}}},
			wantErr: true,
		},
		{
			name: "Valid BS",
			item: map[string]any{"set": map[string]any{"BS": []any{"YmFzZTY0", "YmFzZTY0"}}},
		},
		{
			name:    "Invalid BS element type",
			item:    map[string]any{"set": map[string]any{"BS": []any{123}}},
			wantErr: true,
		},
		{
			name:    "Invalid SS element type",
			item:    map[string]any{"set": map[string]any{"SS": []any{123}}},
			wantErr: true,
		},
		{
			name:    "Invalid Scalar BOOL",
			item:    map[string]any{"val": map[string]any{"BOOL": "string"}},
			wantErr: true,
		},
		{
			name:    "Invalid Scalar B",
			item:    map[string]any{"val": map[string]any{"B": 123}},
			wantErr: true,
		},
		{
			name:    "Invalid List Type",
			item:    map[string]any{"val": map[string]any{"L": "not a list"}},
			wantErr: true,
		},
		{
			name:    "Invalid Map Type",
			item:    map[string]any{"val": map[string]any{"M": "not a map"}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := dynamodb.ValidateDataTypes(tt.item)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNormalizeSetList(t *testing.T) {
	t.Parallel()
	item1 := map[string]any{"set": map[string]any{"SS": []string{"a", "b"}}}
	require.NoError(t, dynamodb.ValidateDataTypes(item1))

	item2 := map[string]any{"set": map[string]any{"BS": [][]byte{[]byte("a"), []byte("b")}}}
	require.NoError(t, dynamodb.ValidateDataTypes(item2))
}
