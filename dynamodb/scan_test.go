package dynamodb_test

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/blackbirdworks/gopherstack/dynamodb/models"

	"github.com/blackbirdworks/gopherstack/dynamodb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		verifyFunc func(t *testing.T, items []map[string]any)
		name       string
		input      string
		wantCount  int
		wantErr    bool
	}{
		{
			name: "Full Table Scan",
			input: `{
				"TableName": "ScanTestTable"
			}`,
			wantCount: 10,
		},
		{
			name: "Scan with Limit (Pagination)",
			input: `{
				"TableName": "ScanTestTable",
				"Limit": 3
			}`,
			wantCount: 3,
		},
		{
			name: "Scan with FilterExpression (Value > 50)",
			input: `{
				"TableName": "ScanTestTable",
				"FilterExpression": "val > :v",
				"ExpressionAttributeValues": {
					":v": {"N": "50"}
				}
			}`,
			wantCount: 5, // 60, 70, 80, 90, 100
		},
		{
			name: "Scan GSI (Sparse Index - Only even items have gsiPK)",
			input: `{
				"TableName": "ScanTestTable",
				"IndexName": "GSI1"
			}`,
			wantCount: 5, // All even items have gsiPK
		},
		{
			name: "ProjectionExpression",
			input: `{
				"TableName": "ScanTestTable",
				"ProjectionExpression": "pk, val",
				"Limit": 1
			}`,
			wantCount: 1,
			verifyFunc: func(t *testing.T, items []map[string]any) {
				t.Helper()
				require.Len(t, items, 1)
				item := items[0]
				_, hasPK := item["pk"]
				_, hasVal := item["val"]
				_, hasStatus := item["status"]
				assert.True(t, hasPK, "pk should be present")
				assert.True(t, hasVal, "val should be present")
				assert.False(t, hasStatus, "status should NOT be present")
			},
		},
		{
			name: "Invalid Table",
			input: `{
				"TableName": "NonExistentTable"
			}`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()

			// Setup table
			tableName := "ScanTestTable"
			createTableJSON := `{
				"TableName": "` + tableName + `",
				"KeySchema": [
					{"AttributeName": "pk", "KeyType": "HASH"}
				],
				"AttributeDefinitions": [
					{"AttributeName": "pk", "AttributeType": "S"},
					{"AttributeName": "gsiPK", "AttributeType": "S"}
				],
				"GlobalSecondaryIndexes": [
					{
						"IndexName": "GSI1",
						"KeySchema": [
							{"AttributeName": "gsiPK", "KeyType": "HASH"}
						],
						"Projection": {
							"ProjectionType": "ALL"
						}
					}
				]
			}`

			ctInput := mustUnmarshal[models.CreateTableInput](t, createTableJSON)
			_, _ = db.CreateTable(t.Context(), models.ToSDKCreateTableInput(&ctInput))

			// Insert 10 items
			for i := 1; i <= 10; i++ {
				status := "inactive"
				if i%2 == 0 {
					status = "active"
				}

				item := map[string]any{
					"pk":     map[string]any{"S": "item-" + strconv.Itoa(i)},
					"status": map[string]any{"S": status},
					"val":    map[string]any{"N": strconv.Itoa(i * 10)},
				}
				if i%2 == 0 {
					item["gsiPK"] = map[string]any{"S": "gsi-val"}
				}

				putInput := models.PutItemInput{
					TableName: tableName,
					Item:      item,
				}
				sdkPutInput, _ := models.ToSDKPutItemInput(&putInput)
				_, _ = db.PutItem(t.Context(), sdkPutInput)
			}

			scanInput := mustUnmarshal[models.ScanInput](t, tc.input)
			sdkScanInput, _ := models.ToSDKScanInput(&scanInput)

			res, scanErr := db.Scan(t.Context(), sdkScanInput)
			if tc.wantErr {
				require.Error(t, scanErr)

				return
			}

			require.NoError(t, scanErr)

			wireItems := make([]map[string]any, len(res.Items))
			for i, item := range res.Items {
				wireItems[i] = models.FromSDKItem(item)
			}

			if tc.wantCount >= 0 {
				assert.Len(t, wireItems, tc.wantCount)
			}

			if tc.verifyFunc != nil {
				tc.verifyFunc(t, wireItems)
			}
		})
	}
}

func TestScan_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		input     string
		wantError string
	}{
		{
			name: "Table Not Found",
			input: `{
				"TableName": "MissingTable"
			}`,
			wantError: "not found",
		},
		{
			name: "Index Not Found",
			input: `{
				"TableName": "ScanValTable",
				"IndexName": "MissingIndex"
			}`,
			wantError: "Index: MissingIndex not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			tableName := "ScanValTable"

			ctInput := models.CreateTableInput{
				TableName: tableName,
				KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
				},
			}
			_, _ = db.CreateTable(t.Context(), models.ToSDKCreateTableInput(&ctInput))

			scanInput := mustUnmarshal[models.ScanInput](t, tc.input)
			sdkScanInput, _ := models.ToSDKScanInput(&scanInput)

			_, scanErr := db.Scan(t.Context(), sdkScanInput)
			require.Error(t, scanErr)
			if tc.wantError != "" {
				assert.Contains(t, scanErr.Error(), tc.wantError)
			}
		})
	}
}

func TestScan_SnapshotIsolation(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	tableName := "IsolationTable"
	createTableHelper(t, db, tableName, "pk")

	// 1. Put item with nested map
	item := map[string]any{
		"pk": map[string]any{"S": "1"},
		"nested": map[string]any{
			"M": map[string]any{
				"key": map[string]any{"S": "initial"},
			},
		},
	}
	putInput := &dynamodb_sdk.PutItemInput{
		TableName: &tableName,
		Item:      mustToAttributeValueMap(t, item),
	}
	_, err := db.PutItem(t.Context(), putInput)
	require.NoError(t, err)

	// 2. Scan the table
	scanInput := &dynamodb_sdk.ScanInput{TableName: &tableName}
	res, err := db.Scan(t.Context(), scanInput)
	require.NoError(t, err)
	require.Len(t, res.Items, 1)

	// 3. Modify the nested map in the DB via UpdateItem or another PutItem
	newItem := map[string]any{
		"pk": map[string]any{"S": "1"},
		"nested": map[string]any{
			"M": map[string]any{
				"key": map[string]any{"S": "modified"},
			},
		},
	}
	_, err = db.PutItem(t.Context(), &dynamodb_sdk.PutItemInput{
		TableName: &tableName,
		Item:      mustToAttributeValueMap(t, newItem),
	})
	require.NoError(t, err)

	// 4. Verify that the ALREADY RETURNED scan item still has the "initial" value
	// (This confirms it's a deep copy and not just a reference to the internal map)
	gotItem := models.FromSDKItem(res.Items[0])
	nested := gotItem["nested"].(map[string]any)["M"].(map[string]any)
	assert.Equal(t, "initial", nested["key"].(map[string]any)["S"],
		"Scan results should be isolated from subsequent mutations")
}

func mustToAttributeValueMap(t *testing.T, m map[string]any) map[string]types.AttributeValue {
	t.Helper()
	sdk, err := models.ToSDKItem(m)
	require.NoError(t, err)

	return sdk
}

func TestScan_PaginationWithLastEvaluatedKey(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	tableName := "PaginationTable"
	_, err := db.CreateTable(t.Context(), &dynamodb_sdk.CreateTableInput{
		TableName: &tableName,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	for i := range 9 {
		item := map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item-" + strconv.Itoa(i)},
		}
		_, err = db.PutItem(t.Context(), &dynamodb_sdk.PutItemInput{
			TableName: &tableName,
			Item:      item,
		})
		require.NoError(t, err)
	}

	// First page
	limit := int32(3)
	out, err := db.Scan(t.Context(), &dynamodb_sdk.ScanInput{
		TableName: &tableName,
		Limit:     &limit,
	})
	require.NoError(t, err)
	assert.LessOrEqual(t, int(out.Count), 3)
	require.NotNil(t, out.LastEvaluatedKey, "expected LastEvaluatedKey for paginated scan")

	// Collect all pages
	total := int(out.Count)
	lastKey := out.LastEvaluatedKey
	for lastKey != nil {
		nextOut, nextErr := db.Scan(t.Context(), &dynamodb_sdk.ScanInput{
			TableName:         &tableName,
			Limit:             &limit,
			ExclusiveStartKey: lastKey,
		})
		require.NoError(t, nextErr)
		total += int(nextOut.Count)
		lastKey = nextOut.LastEvaluatedKey
	}
	assert.Equal(t, 9, total)
}

func TestScan_ParallelSegments(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	tableName := "ParallelTable"
	_, err := db.CreateTable(t.Context(), &dynamodb_sdk.CreateTableInput{
		TableName: &tableName,
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	const n = 10
	for i := range n {
		item := map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p-item-" + strconv.Itoa(i)},
		}
		_, err = db.PutItem(t.Context(), &dynamodb_sdk.PutItemInput{
			TableName: &tableName,
			Item:      item,
		})
		require.NoError(t, err)
	}

	totalSegments := int32(3)
	totalItems := 0
	for seg := range totalSegments {
		seg32 := seg
		out, scanErr := db.Scan(t.Context(), &dynamodb_sdk.ScanInput{
			TableName:     &tableName,
			Segment:       &seg32,
			TotalSegments: &totalSegments,
		})
		require.NoError(t, scanErr)
		totalItems += int(out.Count)
	}
	assert.Equal(t, n, totalItems, "all items should be returned across all segments exactly once")
}
