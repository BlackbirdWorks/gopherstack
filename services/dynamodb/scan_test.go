package dynamodb_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

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

func TestScan_ScannedCount(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	tableName := "ScannedCountTable"
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

	// Insert 10 items with val 1..10
	for i := 1; i <= 10; i++ {
		_, err = db.PutItem(t.Context(), &dynamodb_sdk.PutItemInput{
			TableName: &tableName,
			Item: map[string]types.AttributeValue{
				"pk":  &types.AttributeValueMemberS{Value: "item-" + strconv.Itoa(i)},
				"val": &types.AttributeValueMemberN{Value: strconv.Itoa(i)},
			},
		})
		require.NoError(t, err)
	}

	// Scan with Limit=5 and a FilterExpression that matches only even val (2,4,6,8,10).
	// ScannedCount should be 5 (items examined before filter);
	// Count should be < 5 since the filter excludes odd items in the page.
	limit := int32(5)
	filterExpr := "val IN (:v2, :v4, :v6, :v8, :v10)"
	out, err := db.Scan(t.Context(), &dynamodb_sdk.ScanInput{
		TableName:        &tableName,
		Limit:            &limit,
		FilterExpression: aws.String(filterExpr),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v2":  &types.AttributeValueMemberN{Value: "2"},
			":v4":  &types.AttributeValueMemberN{Value: "4"},
			":v6":  &types.AttributeValueMemberN{Value: "6"},
			":v8":  &types.AttributeValueMemberN{Value: "8"},
			":v10": &types.AttributeValueMemberN{Value: "10"},
		},
	})
	require.NoError(t, err)

	assert.Equal(
		t,
		int32(5),
		out.ScannedCount,
		"ScannedCount must equal Limit (items examined before filter)",
	)
	assert.Less(
		t,
		out.Count,
		out.ScannedCount,
		"Count must be less than ScannedCount when filter excludes some items",
	)
}

func TestScan_ConsumedCapacity(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	tableName := "ScanCapTable"
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

	for i := range 3 {
		_, err = db.PutItem(t.Context(), &dynamodb_sdk.PutItemInput{
			TableName: &tableName,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item-" + strconv.Itoa(i)},
			},
		})
		require.NoError(t, err)
	}

	out, err := db.Scan(t.Context(), &dynamodb_sdk.ScanInput{
		TableName:              &tableName,
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	})
	require.NoError(t, err)
	require.NotNil(t, out.ConsumedCapacity, "ConsumedCapacity should be populated when requested")
	assert.Greater(t, *out.ConsumedCapacity.CapacityUnits, 0.0)
}

// TestScan_SelectCount_OmitsItems verifies AWS's documented Select=COUNT
// behaviour: "Returns the number of matching items, rather than the matching
// items themselves." Count/ScannedCount must still reflect the real totals,
// but Items must come back empty.
func TestScan_SelectCount_OmitsItems(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	tableName := "ScanSelectCountTable"
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

	for i := range 4 {
		_, err = db.PutItem(t.Context(), &dynamodb_sdk.PutItemInput{
			TableName: &tableName,
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item-" + strconv.Itoa(i)},
			},
		})
		require.NoError(t, err)
	}

	out, err := db.Scan(t.Context(), &dynamodb_sdk.ScanInput{
		TableName: &tableName,
		Select:    types.SelectCount,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(4), out.Count)
	assert.Equal(t, int32(4), out.ScannedCount)
	assert.Empty(t, out.Items, "Select=COUNT must not return Items")
}

// TestScan_SelectConstraints_Rejected mirrors the equivalent Query coverage:
// Select values other than SPECIFIC_ATTRIBUTES cannot be combined with a
// ProjectionExpression, and SPECIFIC_ATTRIBUTES requires one.
func TestScan_SelectConstraints_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(*dynamodb_sdk.ScanInput)
		name   string
	}{
		{
			name: "COUNT with ProjectionExpression",
			mutate: func(in *dynamodb_sdk.ScanInput) {
				in.Select = types.SelectCount
				in.ProjectionExpression = aws.String("pk")
			},
		},
		{
			name: "SPECIFIC_ATTRIBUTES without a projection",
			mutate: func(in *dynamodb_sdk.ScanInput) {
				in.Select = types.SelectSpecificAttributes
			},
		},
	}

	for i, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			tableName := "ScanSelectRejectTable" + strconv.Itoa(i)
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

			scanInput := &dynamodb_sdk.ScanInput{TableName: &tableName}
			tc.mutate(scanInput)

			_, err = db.Scan(t.Context(), scanInput)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "ValidationException")
		})
	}
}

func TestScan_SegmentValidation_InvalidTotalSegments(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "SegTable")

	_, err := db.Scan(ctx, &dynamodb_sdk.ScanInput{
		TableName:     aws.String("SegTable"),
		TotalSegments: aws.Int32(0), // invalid: must be ≥ 1
		Segment:       aws.Int32(0),
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestScan_SegmentValidation_SegmentOutOfRange(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "SegTable2")

	_, err := db.Scan(ctx, &dynamodb_sdk.ScanInput{
		TableName:     aws.String("SegTable2"),
		TotalSegments: aws.Int32(3),
		Segment:       aws.Int32(3), // invalid: must be < TotalSegments
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestScan_SegmentValidation_Valid(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "SegTable3")

	_, err := db.Scan(ctx, &dynamodb_sdk.ScanInput{
		TableName:     aws.String("SegTable3"),
		TotalSegments: aws.Int32(4),
		Segment:       aws.Int32(2),
	})
	if err != nil {
		t.Fatalf("expected valid segment scan to succeed, got: %v", err)
	}
}

func TestScan_SegmentValidation_TooHighTotalSegments(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "SegTable4")

	_, err := db.Scan(ctx, &dynamodb_sdk.ScanInput{
		TableName:     aws.String("SegTable4"),
		TotalSegments: aws.Int32(1_000_001),
		Segment:       aws.Int32(0),
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestValidateScanSegment_MaxSegments(t *testing.T) {
	t.Parallel()
	err := dynamodb.ValidateScanSegment(0, 1_000_000)
	if err != nil {
		t.Fatalf("max allowed TotalSegments should be valid: %v", err)
	}
}

func TestValidateScanSegment_NegativeSegment(t *testing.T) {
	t.Parallel()
	err := dynamodb.ValidateScanSegment(-1, 5)
	assertErrorCode(t, err, "ValidationException")
}

// TestScan_ReturnConsumedCapacity_SurvivesWireConversion verifies that
// ToSDKScanInput copies ReturnConsumedCapacity onto the SDK input and
// FromSDKScanOutput copies the resulting ConsumedCapacity back onto the wire
// output. models.ScanInput previously had no ReturnConsumedCapacity field (nor
// ConsistentRead nor Select) and models.ScanOutput had no ConsumedCapacity field,
// so a real client's "ReturnConsumedCapacity": "TOTAL" on Scan -- one of the two
// ops this service is most likely to be hit with in a hot loop -- was silently
// dropped on the way in and the computed capacity silently dropped on the way out.
func TestScan_ReturnConsumedCapacity_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createSimpleTestTable(t, db, "ScanCCTable")
	_, err := db.PutItem(t.Context(), &dynamodb_sdk.PutItemInput{
		TableName: aws.String("ScanCCTable"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	require.NoError(t, err)

	input := models.ScanInput{
		TableName:              "ScanCCTable",
		ReturnConsumedCapacity: "TOTAL",
	}

	sdkInput, convErr := models.ToSDKScanInput(&input)
	require.NoError(t, convErr)
	require.Equal(t, types.ReturnConsumedCapacityTotal, sdkInput.ReturnConsumedCapacity)

	resp, scanErr := db.Scan(t.Context(), sdkInput)
	require.NoError(t, scanErr)
	require.NotNil(t, resp.ConsumedCapacity, "backend must populate ConsumedCapacity when requested")

	wireOut := models.FromSDKScanOutput(resp)
	require.NotNil(t, wireOut.ConsumedCapacity, "wire output must carry ConsumedCapacity through")
	assert.Positive(t, wireOut.ConsumedCapacity.CapacityUnits)
}

// TestScan_ConsistentRead_SurvivesWireConversion verifies that ToSDKScanInput
// copies ConsistentRead onto the SDK input. models.ScanInput previously had no
// ConsistentRead field, so a client's ConsistentRead=true request was always
// parsed as false.
func TestScan_ConsistentRead_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	consistentRead := true
	input := models.ScanInput{
		TableName:      "ScanCCTable",
		ConsistentRead: &consistentRead,
	}

	sdkInput, err := models.ToSDKScanInput(&input)
	require.NoError(t, err)
	require.NotNil(t, sdkInput.ConsistentRead)
	assert.True(t, *sdkInput.ConsistentRead)
}

// TestScan_Select_SurvivesWireConversion verifies that ToSDKScanInput copies
// Select onto the SDK input, so a real client requesting Select=COUNT actually
// gets the COUNT-only response (Items omitted) that AWS documents, instead of the
// full item list. models.ScanInput previously had no Select field at all.
func TestScan_Select_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createSimpleTestTable(t, db, "ScanSelectTable")
	_, err := db.PutItem(t.Context(), &dynamodb_sdk.PutItemInput{
		TableName: aws.String("ScanSelectTable"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "p1"},
			"sk": &types.AttributeValueMemberS{Value: "s1"},
		},
	})
	require.NoError(t, err)

	input := models.ScanInput{
		TableName: "ScanSelectTable",
		Select:    "COUNT",
	}

	sdkInput, convErr := models.ToSDKScanInput(&input)
	require.NoError(t, convErr)
	require.Equal(t, types.SelectCount, sdkInput.Select)

	resp, scanErr := db.Scan(t.Context(), sdkInput)
	require.NoError(t, scanErr)
	assert.Empty(t, resp.Items, "Select=COUNT must omit Items")
	assert.Equal(t, int32(1), resp.Count)
}

func TestScan_GSI_Projection_Masking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		projType       types.ProjectionType
		nonKeyAttrs    []string
		wantHasPayload bool
		wantHasExtra   bool
	}{
		{
			name:           "keys_only",
			projType:       types.ProjectionTypeKeysOnly,
			wantHasPayload: false,
			wantHasExtra:   false,
		},
		{
			name:           "include",
			projType:       types.ProjectionTypeInclude,
			nonKeyAttrs:    []string{"payload"},
			wantHasPayload: true,
			wantHasExtra:   false,
		},
		{
			name:           "all",
			projType:       types.ProjectionTypeAll,
			wantHasPayload: true,
			wantHasExtra:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			db := dynamodb.NewInMemoryDB()

			tableName := "ScanProjTable_" + tt.name
			gsiName := "gsi_proj"
			_, err := db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
				TableName: aws.String(tableName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("gsi_pk"), AttributeType: types.ScalarAttributeTypeS},
				},
				GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
					{
						IndexName: aws.String(gsiName),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("gsi_pk"), KeyType: types.KeyTypeHash},
						},
						Projection: &types.Projection{
							ProjectionType:   tt.projType,
							NonKeyAttributes: tt.nonKeyAttrs,
						},
					},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			require.NoError(t, err)

			_, err = db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]types.AttributeValue{
					"pk":      &types.AttributeValueMemberS{Value: "pk1"},
					"gsi_pk":  &types.AttributeValueMemberS{Value: "g1"},
					"payload": &types.AttributeValueMemberS{Value: "important_data"},
					"extra":   &types.AttributeValueMemberS{Value: "extra_data"},
				},
			})
			require.NoError(t, err)

			resp, err := db.Scan(ctx, &dynamodb_sdk.ScanInput{
				TableName: aws.String(tableName),
				IndexName: aws.String(gsiName),
			})
			require.NoError(t, err)
			require.Len(t, resp.Items, 1)

			item := resp.Items[0]
			_, hasPK := item["pk"]
			_, hasGSIPK := item["gsi_pk"]
			_, hasPayload := item["payload"]
			_, hasExtra := item["extra"]

			assert.True(t, hasPK, "pk must always be projected")
			assert.True(t, hasGSIPK, "gsi_pk must always be projected")
			assert.Equal(t, tt.wantHasPayload, hasPayload, "payload presence mismatch")
			assert.Equal(t, tt.wantHasExtra, hasExtra, "extra presence mismatch")
		})
	}
}
