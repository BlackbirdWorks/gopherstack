package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"encoding/json"
	"strings"
	"testing"

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
			wantErr: true, // "abc" is not a valid number
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
			itemJSON, _ := json.Marshal(tc.item)
			err := dynamodb.ValidateDataTypes(tc.item)
			if tc.wantErr {
				require.Error(t, err, "Item: %s", string(itemJSON))
			} else {
				require.NoError(t, err, "Item: %s", string(itemJSON))
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
		"pk": map[string]any{"S": "value"}, // "pk" (2) + "value" (5) = 7
		"n":  map[string]any{"N": "123"},   // "n" (1) + "123" (3) = 4
	}

	size, err := dynamodb.CalculateItemSize(item)
	require.NoError(t, err)
	assert.Positive(t, size)
}

func TestPutItem_ValidationErrors(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	tableName := "ValidationTable"
	db.CreateTable([]byte(`{
		"TableName": "` + tableName + `",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
		"ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
	}`))

	tests := []struct {
		name      string
		item      string
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
			input := `{"TableName": "` + tableName + `", "Item": ` + tc.item + `}`
			_, err := db.PutItem([]byte(input))
			require.Error(t, err)
			if tc.wantError != "" {
				assert.Contains(t, err.Error(), tc.wantError)
			}
		})
	}
}

func TestPutItem_ItemTooLarge(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	tableName := "LargeItemTable"
	db.CreateTable([]byte(`{
		"TableName": "` + tableName + `",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
		"ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
	}`))

	// Create item > 400KB
	largeVal := strings.Repeat("a", 400*1024+100)
	input := `{
		"TableName": "` + tableName + `",
		"Item": {
			"pk": {"S": "large"},
			"val": {"S": "` + largeVal + `"}
		}
	}`

	_, err := db.PutItem([]byte(input))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds limit")
}
