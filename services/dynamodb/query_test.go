package dynamodb_test

import (
	"strconv"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		input      string
		errMessage string
		wantItems  []map[string]any
		wantErr    bool
	}{
		{
			name: "Simple PK Query",
			input: `{
				"TableName": "QueryTestTable",
				"KeyConditionExpression": "pk = :pk",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"}
				}
			}`,
			wantItems: makeItems("A", 1, 5),
		},
		{
			name: "PK + SK Exact Match",
			input: `{
				"TableName": "QueryTestTable",
				"KeyConditionExpression": "pk = :pk AND sk = :sk",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"},
					":sk": {"N": "3"}
				}
			}`,
			wantItems: makeItems("A", 3, 3),
		},
		{
			name: "SK Condition: Multiple comparisons (BETWEEN)",
			input: `{
				"TableName": "QueryTestTable",
				"KeyConditionExpression": "pk = :pk AND sk BETWEEN :min AND :max",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"},
					":min": {"N": "2"},
					":max": {"N": "4"}
				}
			}`,
			wantItems: makeItems("A", 2, 4),
		},
		{
			name: "ScanIndexForward = false (Reverse)",
			input: `{
				"TableName": "QueryTestTable",
				"KeyConditionExpression": "pk = :pk",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"}
				},
				"ScanIndexForward": false
			}`,
			wantItems: reverseItems(makeItems("A", 1, 5)),
		},
		{
			name: "Limit + ExclusiveStartKey (Pagination)",
			input: `{
				"TableName": "QueryTestTable",
				"KeyConditionExpression": "pk = :pk",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"}
				},
				"Limit": 2
			}`,
			wantItems: makeItems("A", 1, 2),
		},
		{
			name: "FilterExpression: Only even numbers",
			input: `{
				"TableName": "QueryTestTable",
				"KeyConditionExpression": "pk = :pk",
				"FilterExpression": "sk = :v2 OR sk = :v4",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"},
					":v2": {"N": "2"},
					":v4": {"N": "4"}
				}
			}`,
			wantItems: []map[string]any{
				makeItem("A", 2),
				makeItem("A", 4),
			},
		},
		{
			name: "ProjectionExpression",
			input: `{
				"TableName": "QueryTestTable",
				"KeyConditionExpression": "pk = :pk AND sk = :sk",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"},
					":sk": {"N": "1"}
				},
				"ProjectionExpression": "pk, sk"
			}`,
			wantItems: []map[string]any{
				{"pk": map[string]any{"S": "A"}, "sk": map[string]any{"N": "1"}},
			},
		},
		{
			name: "PK B Query",
			input: `{
				"TableName": "QueryTestTable",
				"KeyConditionExpression": "pk = :pk",
				"ExpressionAttributeValues": {
					":pk": {"S": "B"}
				}
			}`,
			wantItems: makeItems("B", 1, 5),
		},
		{
			name: "Missing Table",
			input: `{
				"TableName": "NonExistentTable",
				"KeyConditionExpression": "pk = :pk",
				"ExpressionAttributeValues": {":pk": {"S": "A"}}
			}`,
			wantErr:    true,
			errMessage: "Requested resource not found",
		},
		{
			name: "Malformed FilterExpression",
			input: `{
				"TableName": "QueryTestTable",
				"KeyConditionExpression": "pk = :pk",
				"FilterExpression": "data >",
				"ExpressionAttributeValues": {":pk": {"S": "A"}}
			}`,
			wantErr:    true,
			errMessage: "ValidationException",
		},
		{
			name: "Malformed ProjectionExpression",
			input: `{
				"TableName": "QueryTestTable",
				"KeyConditionExpression": "pk = :pk",
				"ProjectionExpression": "data[",
				"ExpressionAttributeValues": {":pk": {"S": "A"}}
			}`,
			wantErr:    true,
			errMessage: "ValidationException",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()

			// Setup table
			tableName := "QueryTestTable"
			ctInput := mustUnmarshal[models.CreateTableInput](t, `{
				"TableName": "`+tableName+`",
				"KeySchema": [
					{"AttributeName": "pk", "KeyType": "HASH"},
					{"AttributeName": "sk", "KeyType": "RANGE"}
				],
				"AttributeDefinitions": [
					{"AttributeName": "pk", "AttributeType": "S"},
					{"AttributeName": "sk", "AttributeType": "N"}
				]
			}`)
			_, _ = db.CreateTable(t.Context(), models.ToSDKCreateTableInput(&ctInput))

			// Insert items
			for _, pk := range []string{"A", "B"} {
				for i := 1; i <= 5; i++ {
					putInput := models.PutItemInput{
						TableName: tableName,
						Item: map[string]any{
							"pk":   map[string]any{"S": pk},
							"sk":   map[string]any{"N": strconv.Itoa(i)},
							"data": map[string]any{"S": "data-" + pk + "-" + strconv.Itoa(i)},
						},
					}
					sdkPut, _ := models.ToSDKPutItemInput(&putInput)
					_, _ = db.PutItem(t.Context(), sdkPut)
				}
			}

			queryInput := mustUnmarshal[models.QueryInput](t, tc.input)
			sdkQuery, _ := models.ToSDKQueryInput(&queryInput)

			res, queryErr := db.Query(t.Context(), sdkQuery)
			if tc.wantErr {
				require.Error(t, queryErr)
				if tc.errMessage != "" {
					assert.Contains(t, queryErr.Error(), tc.errMessage)
				}

				return
			}

			require.NoError(t, queryErr)

			var gotItems []map[string]any
			for _, item := range res.Items {
				gotItems = append(gotItems, models.FromSDKItem(item))
			}

			assert.Equal(t, tc.wantItems, gotItems)
		})
	}
}

// Helper functions for test data generation

func toStr(i int) string {
	return strconv.Itoa(i)
}

func makeItem(pk string, sk int) map[string]any {
	return map[string]any{
		"pk":   map[string]any{"S": pk},
		"sk":   map[string]any{"N": toStr(sk)},
		"data": map[string]any{"S": "data-" + pk + "-" + toStr(sk)},
	}
}

func makeItems(pk string, startSk, endSk int) []map[string]any {
	var items []map[string]any
	for i := startSk; i <= endSk; i++ {
		items = append(items, makeItem(pk, i))
	}

	return items
}

func reverseItems(items []map[string]any) []map[string]any {
	reversed := make([]map[string]any, len(items))
	for i, item := range items {
		reversed[len(items)-1-i] = item
	}

	return reversed
}

func TestQuery_BlankSKValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		skValue    string
		errMessage string
		wantErr    bool
	}{
		{
			name:    "Non-blank SK is valid",
			skValue: `{"S": "valid"}`,
			wantErr: false,
		},
		{
			name:       "Blank SK string value is rejected",
			skValue:    `{"S": ""}`,
			wantErr:    true,
			errMessage: "cannot contain an empty string value. Key: sk",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()

			createTableHelper(t, db, "BlankSKTable", "pk", "sk")

			queryInput := mustUnmarshal[models.QueryInput](t, `{
				"TableName": "BlankSKTable",
				"KeyConditionExpression": "pk = :pk AND sk = :sk",
				"ExpressionAttributeValues": {
					":pk": {"S": "somekey"},
					":sk": `+tc.skValue+`
				}
			}`)
			sdkQuery, _ := models.ToSDKQueryInput(&queryInput)

			_, err := db.Query(t.Context(), sdkQuery)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestQuery_SelectCount_OmitsItems verifies AWS's documented Select=COUNT
// behaviour: "Returns the number of matching items, rather than the matching
// items themselves." Count/ScannedCount must still reflect the real totals,
// but Items must come back empty.
func TestQuery_SelectCount_OmitsItems(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	tableName := "QuerySelectCountTable"
	createTableHelper(t, db, tableName, "pk", "sk")

	for i := range 3 {
		putInput := models.PutItemInput{
			TableName: tableName,
			Item: map[string]any{
				"pk": map[string]any{"S": "key"},
				"sk": map[string]any{"N": strconv.Itoa(i)},
			},
		}
		sdkPut, _ := models.ToSDKPutItemInput(&putInput)
		_, _ = db.PutItem(t.Context(), sdkPut)
	}

	queryInput := mustUnmarshal[models.QueryInput](t, `{
		"TableName": "`+tableName+`",
		"KeyConditionExpression": "pk = :pk",
		"ExpressionAttributeValues": {":pk": {"S": "key"}}
	}`)
	sdkQuery, _ := models.ToSDKQueryInput(&queryInput)
	sdkQuery.Select = types.SelectCount

	out, err := db.Query(t.Context(), sdkQuery)
	require.NoError(t, err)
	assert.Equal(t, int32(3), out.Count)
	assert.Equal(t, int32(3), out.ScannedCount)
	assert.Empty(t, out.Items, "Select=COUNT must not return Items")
}

// TestQuery_SelectConstraints_Rejected covers the documented restrictions on
// the Select parameter's interaction with ProjectionExpression/AttributesToGet:
// "If you use the ProjectionExpression parameter, then the value for Select
// can only be SPECIFIC_ATTRIBUTES. Any other value for Select will return an
// error." (see API_Query.html "Select" parameter docs).
func TestQuery_SelectConstraints_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*awsdynamodb.QueryInput)
		name   string
	}{
		{
			name: "COUNT with ProjectionExpression",
			mutate: func(in *awsdynamodb.QueryInput) {
				in.Select = types.SelectCount
				in.ProjectionExpression = aws.String("pk")
			},
		},
		{
			name: "ALL_ATTRIBUTES with ProjectionExpression",
			mutate: func(in *awsdynamodb.QueryInput) {
				in.Select = types.SelectAllAttributes
				in.ProjectionExpression = aws.String("pk")
			},
		},
		{
			name: "SPECIFIC_ATTRIBUTES without a projection",
			mutate: func(in *awsdynamodb.QueryInput) {
				in.Select = types.SelectSpecificAttributes
			},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			tableName := "QuerySelectRejectTable" + strconv.Itoa(i)
			createTableHelper(t, db, tableName, "pk", "sk")

			queryInput := mustUnmarshal[models.QueryInput](t, `{
				"TableName": "`+tableName+`",
				"KeyConditionExpression": "pk = :pk",
				"ExpressionAttributeValues": {":pk": {"S": "key"}}
			}`)
			sdkQuery, _ := models.ToSDKQueryInput(&queryInput)
			tc.mutate(sdkQuery)

			_, err := db.Query(t.Context(), sdkQuery)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "ValidationException")
		})
	}
}

func TestQuery_ConsumedCapacity(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	tableName := "QueryCapTable"
	createTableHelper(t, db, tableName, "pk", "sk")

	for i := range 3 {
		putInput := models.PutItemInput{
			TableName: tableName,
			Item: map[string]any{
				"pk": map[string]any{"S": "key"},
				"sk": map[string]any{"N": strconv.Itoa(i)},
			},
		}
		sdkPut, _ := models.ToSDKPutItemInput(&putInput)
		_, _ = db.PutItem(t.Context(), sdkPut)
	}

	queryInput := mustUnmarshal[models.QueryInput](t, `{
		"TableName": "QueryCapTable",
		"KeyConditionExpression": "pk = :pk",
		"ExpressionAttributeValues": {":pk": {"S": "key"}}
	}`)
	sdkQuery, _ := models.ToSDKQueryInput(&queryInput)
	sdkQuery.ReturnConsumedCapacity = types.ReturnConsumedCapacityTotal

	out, err := db.Query(t.Context(), sdkQuery)
	require.NoError(t, err)
	require.NotNil(t, out.ConsumedCapacity, "ConsumedCapacity should be populated when requested")
	assert.Greater(t, *out.ConsumedCapacity.CapacityUnits, 0.0)

	// A strongly-consistent query reports twice the capacity of an
	// eventually-consistent one, matching real DynamoDB.
	eventual := *out.ConsumedCapacity.CapacityUnits

	sdkQuery.ConsistentRead = aws.Bool(true)
	outConsistent, err := db.Query(t.Context(), sdkQuery)
	require.NoError(t, err)
	require.NotNil(t, outConsistent.ConsumedCapacity)
	assert.InDelta(t, eventual*2, *outConsistent.ConsumedCapacity.CapacityUnits, 1e-9,
		"strongly-consistent query should report 2x the capacity")
}

// TestQuery_ReturnConsumedCapacity_SurvivesWireConversion verifies that
// ToSDKQueryInput itself copies ReturnConsumedCapacity from the wire-format
// models.QueryInput onto the SDK input struct. ToSDKQueryInput previously left
// this field unset even though models.QueryInput declared it, so a real client's
// "ReturnConsumedCapacity": "TOTAL" was silently dropped when parsed off the wire.
// TestQuery_ConsistentRead_ConsumedCapacity above manually overwrites
// sdkQuery.ReturnConsumedCapacity after calling ToSDKQueryInput, which is why it
// never caught this: it bypasses the exact conversion step this test exercises.
func TestQuery_ReturnConsumedCapacity_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createSimpleTestTable(t, db, "QueryCCWireTable")
	_, err := db.PutItem(t.Context(), &awsdynamodb.PutItemInput{
		TableName: aws.String("QueryCCWireTable"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	require.NoError(t, err)

	input := models.QueryInput{
		TableName:                 "QueryCCWireTable",
		KeyConditionExpression:    "pk = :pk",
		ExpressionAttributeValues: map[string]any{":pk": map[string]any{"S": "p1"}},
		ReturnConsumedCapacity:    "TOTAL",
	}

	sdkInput, convErr := models.ToSDKQueryInput(&input)
	require.NoError(t, convErr)
	require.Equal(t, types.ReturnConsumedCapacityTotal, sdkInput.ReturnConsumedCapacity)

	out, queryErr := db.Query(t.Context(), sdkInput)
	require.NoError(t, queryErr)
	require.NotNil(t, out.ConsumedCapacity, "backend must populate ConsumedCapacity when requested")
}

// TestQuery_Select_SurvivesWireConversion verifies that ToSDKQueryInput copies
// Select onto the SDK input, so a real client requesting Select=COUNT actually
// gets the COUNT-only response (Items omitted). models.QueryInput previously had
// no Select field at all.
func TestQuery_Select_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createSimpleTestTable(t, db, "QuerySelectTable")
	_, err := db.PutItem(t.Context(), &awsdynamodb.PutItemInput{
		TableName: aws.String("QuerySelectTable"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	require.NoError(t, err)

	input := models.QueryInput{
		TableName:                 "QuerySelectTable",
		KeyConditionExpression:    "pk = :pk",
		ExpressionAttributeValues: map[string]any{":pk": map[string]any{"S": "p1"}},
		Select:                    "COUNT",
	}

	sdkInput, convErr := models.ToSDKQueryInput(&input)
	require.NoError(t, convErr)
	require.Equal(t, types.SelectCount, sdkInput.Select)

	out, queryErr := db.Query(t.Context(), sdkInput)
	require.NoError(t, queryErr)
	assert.Empty(t, out.Items, "Select=COUNT must omit Items")
	assert.Equal(t, int32(1), out.Count)
}
