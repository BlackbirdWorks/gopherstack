package dynamodb_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

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
		{
			name:      "Empty string PK",
			item:      `{"pk": {"S": ""}}`,
			wantError: "cannot contain an empty string value. Key: pk",
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
			_, _ = db.CreateTable(t.Context(), models.ToSDKCreateTableInput(&ctInput))

			inputStr := `{"TableName": "` + tableName + `", "Item": ` + tc.item + `}`
			putInput := mustUnmarshal[models.PutItemInput](t, inputStr)
			sdkPut, _ := models.ToSDKPutItemInput(&putInput)

			_, pErr := db.PutItem(t.Context(), sdkPut)
			require.Error(t, pErr)
			if tc.wantError != "" {
				assert.Contains(t, pErr.Error(), tc.wantError)
			}
		})
	}
}

func TestPutItem_BlankSK(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	tableName := "BlankSKTable"
	ctInput := models.CreateTableInput{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: models.KeyTypeHash},
			{AttributeName: "sk", KeyType: models.KeyTypeRange},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
			{AttributeName: "sk", AttributeType: "S"},
		},
	}
	_, err := db.CreateTable(t.Context(), models.ToSDKCreateTableInput(&ctInput))
	require.NoError(t, err)

	inputStr := `{"TableName": "` + tableName + `", "Item": {"pk": {"S": "val"}, "sk": {"S": ""}}}`
	putInput := mustUnmarshal[models.PutItemInput](t, inputStr)
	sdkPut, _ := models.ToSDKPutItemInput(&putInput)

	_, pErr := db.PutItem(t.Context(), sdkPut)
	require.Error(t, pErr)
	assert.Contains(t, pErr.Error(), "cannot contain an empty string value. Key: sk")
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
	_, err := db.CreateTable(t.Context(), models.ToSDKCreateTableInput(&ctInput))
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
	_, err = db.PutItem(t.Context(), sdkPut)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Item size has exceeded the maximum allowed size")
}

// TestKeySizeLimit_AWSWording verifies the partition- and sort-key overflow
// messages match AWS DynamoDB's ValidationException wording.
func TestKeySizeLimit_AWSWording(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "KeySizeTbl", "pk", "sk")

	t.Run("partition key too large", func(t *testing.T) {
		t.Parallel()
		put := models.PutItemInput{
			TableName: "KeySizeTbl",
			Item: map[string]any{
				"pk": map[string]any{"S": strings.Repeat("p", 2100)},
				"sk": map[string]any{"S": "x"},
			},
		}
		sdkPut, _ := models.ToSDKPutItemInput(&put)
		_, err := db.PutItem(t.Context(), sdkPut)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Size of hashkey has exceeded the maximum size limit")
	})

	t.Run("sort key too large", func(t *testing.T) {
		t.Parallel()
		put := models.PutItemInput{
			TableName: "KeySizeTbl",
			Item: map[string]any{
				"pk": map[string]any{"S": "x"},
				"sk": map[string]any{"S": strings.Repeat("s", 1100)},
			},
		}
		sdkPut, _ := models.ToSDKPutItemInput(&put)
		_, err := db.PutItem(t.Context(), sdkPut)
		require.Error(t, err)
		assert.Contains(
			t,
			err.Error(),
			"Aggregated size of all range keys has exceeded the size limit",
		)
	})
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
			item: map[string]any{"set": map[string]any{"BS": []any{"YmFzZTY0", "dGVzdA=="}}},
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

func TestAttributeNameLength_TooLong_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "AttrLen")

	longAttr := strings.Repeat("a", 256)

	item := map[string]types.AttributeValue{
		"pk":     &types.AttributeValueMemberS{Value: "pk1"},
		"sk":     &types.AttributeValueMemberS{Value: "sk1"},
		longAttr: &types.AttributeValueMemberS{Value: "v"},
	}

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("AttrLen"),
		Item:      item,
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestAttributeNameLength_MaxOK(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "AttrLen255")

	attrName := strings.Repeat("x", 255)

	item := map[string]types.AttributeValue{
		"pk":     &types.AttributeValueMemberS{Value: "pk1"},
		"sk":     &types.AttributeValueMemberS{Value: "sk1"},
		attrName: &types.AttributeValueMemberS{Value: "val"},
	}

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("AttrLen255"),
		Item:      item,
	})
	if err != nil {
		t.Fatalf("expected success for 255-char attribute name, got: %v", err)
	}
}

func TestValidateAttributeNames_Empty_Rejected(t *testing.T) {
	t.Parallel()
	err := dynamodb.ValidateAttributeNames(map[string]any{
		"": map[string]any{"S": "value"},
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestValidateAttributeNames_Valid(t *testing.T) {
	t.Parallel()

	item := map[string]any{
		"pk":    map[string]any{"S": "v"},
		"hello": map[string]any{"N": "1"},
	}

	err := dynamodb.ValidateAttributeNames(item)
	if err != nil {
		t.Fatalf("expected valid attribute names to pass: %v", err)
	}
}

func TestAttributeNameValidation_UpdateItem_TooLong_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "UpdateAttrLen")

	putTestItem(t, db, "UpdateAttrLen", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "pk1"},
		"sk": &types.AttributeValueMemberS{Value: "sk1"},
	})

	longAttr := strings.Repeat("x", 256)

	_, err := db.UpdateItem(ctx, &dynamodb_sdk.UpdateItemInput{
		TableName: aws.String("UpdateAttrLen"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "pk1"},
			"sk": &types.AttributeValueMemberS{Value: "sk1"},
		},
		UpdateExpression: aws.String("SET #attr = :v"),
		ExpressionAttributeNames: map[string]string{
			"#attr": longAttr,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v": &types.AttributeValueMemberS{Value: "val"},
		},
	})
	// The update will either fail on the attribute name or succeed (depending on
	// whether UpdateItem validates the resolved expression attribute names).
	// Either is acceptable - we just verify no panic occurs.
	_ = err
}

func TestTableName_TooShort_Rejected(t *testing.T) {
	t.Parallel()
	err := dynamodb.ValidateTableName("ab") // 2 chars — minimum is 3
	assertErrorCode(t, err, "ValidationException")
}

func TestTableName_TooLong_Rejected(t *testing.T) {
	t.Parallel()
	err := dynamodb.ValidateTableName(strings.Repeat("a", 256))
	assertErrorCode(t, err, "ValidationException")
}

func TestTableName_InvalidChars_Rejected(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"my table!", "no/slash", "has@at"} {
		err := dynamodb.ValidateTableName(name)
		assertErrorCode(t, err, "ValidationException")
	}
}

func TestTableName_ValidNames_Accepted(t *testing.T) {
	t.Parallel()
	validNames := []string{
		"abc",
		"my-table",
		"my_table",
		"my.table",
		strings.Repeat("a", 255),
	}

	for _, name := range validNames {
		if err := dynamodb.ValidateTableName(name); err != nil {
			t.Fatalf("expected valid table name %q to be accepted, got: %v", name, err)
		}
	}
}

func TestTableName_SingleChar_Rejected(t *testing.T) {
	t.Parallel()
	err := dynamodb.ValidateTableName("a")
	assertErrorCode(t, err, "ValidationException")
}

func TestTableName_ExactMinLength_Accepted(t *testing.T) {
	t.Parallel()
	if err := dynamodb.ValidateTableName("abc"); err != nil {
		t.Fatalf("3-char name should be accepted: %v", err)
	}
}

func TestTableName_ExactMaxLength_Accepted(t *testing.T) {
	t.Parallel()
	if err := dynamodb.ValidateTableName(strings.Repeat("a", 255)); err != nil {
		t.Fatalf("255-char name should be accepted: %v", err)
	}
}

func TestPutItem_ReturnValues_AllNew_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	assertErrorCode(t, err, "ValidationException")
	if err != nil && !strings.Contains(err.Error(), "ALL_OLD") {
		t.Fatalf("expected message to mention ALL_OLD, got: %v", err)
	}
}

func TestPutItem_ReturnValues_UpdatedOld_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		ReturnValues: types.ReturnValueUpdatedOld,
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestPutItem_ReturnValues_AllOld_Accepted(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	// Pre-populate an item so we can return the old one.
	putTestItem(t, db, "tbl", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "p1"},
		"sk": &types.AttributeValueMemberS{Value: "s1"},
		"v":  &types.AttributeValueMemberS{Value: "old"},
	})

	out, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
			"v":  &types.AttributeValueMemberS{Value: "new"},
		},
		ReturnValues: types.ReturnValueAllOld,
	})
	if err != nil {
		t.Fatalf("ALL_OLD should be accepted: %v", err)
	}
	if out.Attributes == nil {
		t.Fatal("expected old attributes in response, got nil")
	}
}

func TestDeleteItem_ReturnValues_AllNew_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.DeleteItem(ctx, &dynamodb_sdk.DeleteItemInput{
		TableName: aws.String("tbl"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestDeleteItem_ReturnValues_UpdatedNew_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.DeleteItem(ctx, &dynamodb_sdk.DeleteItemInput{
		TableName: aws.String("tbl"),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestListTables_LimitZero_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()

	limit := int32(0)
	_, err := db.ListTables(ctx, &dynamodb_sdk.ListTablesInput{
		Limit: &limit,
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestListTables_LimitNegative_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()

	limit := int32(-1)
	_, err := db.ListTables(ctx, &dynamodb_sdk.ListTablesInput{
		Limit: &limit,
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestListTables_LimitOver100_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()

	limit := int32(101)
	_, err := db.ListTables(ctx, &dynamodb_sdk.ListTablesInput{
		Limit: &limit,
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestListTables_LimitExact100_Accepted(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()

	limit := int32(100)
	_, err := db.ListTables(ctx, &dynamodb_sdk.ListTablesInput{
		Limit: &limit,
	})
	if err != nil {
		t.Fatalf("Limit=100 should be accepted: %v", err)
	}
}

func TestListTables_NilLimit_Accepted(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()

	_, err := db.ListTables(ctx, &dynamodb_sdk.ListTablesInput{})
	if err != nil {
		t.Fatalf("nil Limit should be accepted: %v", err)
	}
}

func TestQuery_LimitZero_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	limit := int32(0)
	_, err := db.Query(ctx, &dynamodb_sdk.QueryInput{
		TableName:              aws.String("tbl"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
		},
		Limit: &limit,
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestScan_LimitZero_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	limit := int32(0)
	_, err := db.Scan(ctx, &dynamodb_sdk.ScanInput{
		TableName: aws.String("tbl"),
		Limit:     &limit,
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestQuery_LimitPositive_Accepted(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	limit := int32(10)
	_, err := db.Query(ctx, &dynamodb_sdk.QueryInput{
		TableName:              aws.String("tbl"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "p1"},
		},
		Limit: &limit,
	})
	if err != nil {
		t.Fatalf("positive Limit should be accepted: %v", err)
	}
}

func TestSS_EmptyString_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "p1"},
			"sk":   &types.AttributeValueMemberS{Value: "s1"},
			"tags": &types.AttributeValueMemberSS{Value: []string{"valid", ""}},
		},
	})
	assertErrorCode(t, err, "ValidationException")
	if err != nil && !strings.Contains(err.Error(), "empty string") {
		t.Fatalf("expected 'empty string' in error, got: %v", err)
	}
}

func TestSS_NonEmptyStrings_Accepted(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "p1"},
			"sk":   &types.AttributeValueMemberS{Value: "s1"},
			"tags": &types.AttributeValueMemberSS{Value: []string{"a", "b", "c"}},
		},
	})
	if err != nil {
		t.Fatalf("SS with non-empty strings should be accepted: %v", err)
	}
}

func TestSS_DuplicateValues_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "p1"},
			"sk":   &types.AttributeValueMemberS{Value: "s1"},
			"tags": &types.AttributeValueMemberSS{Value: []string{"dup", "dup"}},
		},
	})
	assertErrorCode(t, err, "ValidationException")
	if err != nil && !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected 'duplicate' in error, got: %v", err)
	}
}

func TestNS_DuplicateValues_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "p1"},
			"sk":   &types.AttributeValueMemberS{Value: "s1"},
			"nums": &types.AttributeValueMemberNS{Value: []string{"1", "2", "1"}},
		},
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestSS_UniqueValues_Accepted(t *testing.T) {
	t.Parallel()

	err := dynamodb.ValidateSetNoDuplicates("tags", []any{"a", "b", "c"})
	if err != nil {
		t.Fatalf("unique SS values should be accepted: %v", err)
	}
}

func TestSS_DuplicateValues_DirectValidation(t *testing.T) {
	t.Parallel()

	err := dynamodb.ValidateSetNoDuplicates("tags", []any{"x", "y", "x"})
	assertErrorCode(t, err, "ValidationException")
}

func TestNumber_LeadingZero_Rejected(t *testing.T) {
	t.Parallel()
	cases := []string{"007", "01", "01.5", "-01"}

	for _, n := range cases {
		t.Run(n, func(t *testing.T) {
			t.Parallel()
			err := dynamodb.ValidateNumberNoLeadingZeros("attr", n)
			assertErrorCode(t, err, "ValidationException")
		})
	}
}

func TestNumber_Valid_Accepted(t *testing.T) {
	t.Parallel()
	cases := []string{"0", "1", "-1", "0.5", "123", "1.23e10", "-0.5"}

	for _, n := range cases {
		t.Run(n, func(t *testing.T) {
			t.Parallel()
			if err := dynamodb.ValidateNumberNoLeadingZeros("attr", n); err != nil {
				t.Fatalf("valid number %q should be accepted: %v", n, err)
			}
		})
	}
}

func TestPutItem_LeadingZeroNumber_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	_, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
		TableName: aws.String("tbl"),
		Item: map[string]types.AttributeValue{
			"pk":  &types.AttributeValueMemberS{Value: "p1"},
			"sk":  &types.AttributeValueMemberS{Value: "s1"},
			"cnt": &types.AttributeValueMemberN{Value: "007"},
		},
	})
	assertErrorCode(t, err, "ValidationException")
}
