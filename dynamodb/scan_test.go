package dynamodb_test

import (
	"strconv"
	"testing"

	"Gopherstack/dynamodb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScan(t *testing.T) {
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
				},
				"ProvisionedThroughput": {
					"ReadCapacityUnits": 5,
					"WriteCapacityUnits": 5
				}
			}
		],
		"ProvisionedThroughput": {
			"ReadCapacityUnits": 5,
			"WriteCapacityUnits": 5
		}
	}`

	ctInput := mustUnmarshal[dynamodb.CreateTableInput](t, createTableJSON)
	_, err := db.CreateTable(dynamodb.ToSDKCreateTableInput(&ctInput))
	require.NoError(t, err)

	// Insert 10 items
	for i := 1; i <= 10; i++ {
		status := "inactive"
		if i%2 == 0 {
			status = "active"
		}
		itemJSON := `{
			"TableName": "` + tableName + `",
			"Item": {
				"pk": {"S": "item-` + strconv.Itoa(i) + `"},
				"status": {"S": "` + status + `"},
				"val": {"N": "` + strconv.Itoa(i*10) + `"}
			}
		}`
		if i%2 == 0 {
			itemJSON = `{
				"TableName": "` + tableName + `",
				"Item": {
					"pk": {"S": "item-` + strconv.Itoa(i) + `"},
					"gsiPK": {"S": "gsi-val"},
					"status": {"S": "` + status + `"},
					"val": {"N": "` + strconv.Itoa(i*10) + `"}
				}
			}`
		}

		putInput := mustUnmarshal[dynamodb.PutItemInput](t, itemJSON)
		sdkPutInput, _ := dynamodb.ToSDKPutItemInput(&putInput)
		_, putErr := db.PutItem(sdkPutInput)
		require.NoError(t, putErr)
	}

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
				"TableName": "` + tableName + `"
			}`,
			wantCount: 10,
		},
		{
			name: "Scan with Limit (Pagination)",
			input: `{
				"TableName": "` + tableName + `",
				"Limit": 3
			}`,
			wantCount: 3,
		},
		{
			name: "Scan with FilterExpression (Value > 50)",
			input: `{
				"TableName": "` + tableName + `",
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
				"TableName": "` + tableName + `",
				"IndexName": "GSI1"
			}`,
			wantCount: 5, // All even items have gsiPK
		},
		{
			name: "ProjectionExpression",
			input: `{
				"TableName": "` + tableName + `",
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

			scanInput := mustUnmarshal[dynamodb.ScanInput](t, tc.input)
			sdkScanInput, _ := dynamodb.ToSDKScanInput(&scanInput)

			res, scanErr := db.Scan(sdkScanInput)
			if tc.wantErr {
				require.Error(t, scanErr)

				return
			}

			require.NoError(t, scanErr)
			// Unwrap items to map[string]any for verification
			// ScanOutput.Items is []map[string]types.AttributeValue
			// To verify, we can convert back to wire format or check logic.
			// The existing verification functions expect []map[string]any (wire).

			wireItems := make([]map[string]any, len(res.Items))
			for i, item := range res.Items {
				wireItems[i] = dynamodb.FromSDKItem(item)
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
	db := dynamodb.NewInMemoryDB()
	tableName := "ScanValTable"

	ctInput := dynamodb.CreateTableInput{
		TableName: tableName,
		KeySchema: []dynamodb.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
	}
	_, _ = db.CreateTable(dynamodb.ToSDKCreateTableInput(&ctInput))

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
				"TableName": "` + tableName + `",
				"IndexName": "MissingIndex"
			}`,
			wantError: "Index: MissingIndex not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			scanInput := mustUnmarshal[dynamodb.ScanInput](t, tc.input)
			sdkScanInput, _ := dynamodb.ToSDKScanInput(&scanInput)

			_, err := db.Scan(sdkScanInput)
			require.Error(t, err)
			if tc.wantError != "" && err != nil {
				assert.Contains(t, err.Error(), tc.wantError)
			}
		})
	}
}
