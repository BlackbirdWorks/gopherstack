package dynamodb_test

// Golden byte-equality fixtures for the Query/Scan/TransactWriteItems perf
// sweep (gopherstack perf sweep, secondary-index copy-on-write +
// double-unwrap removal). These tests only exercise the public HTTP handler,
// so the same file runs unmodified against the pre-change code (a detached
// worktree at the commit before this sweep) to capture testdata/*.golden.json,
// and against the optimized code to assert byte-for-byte identical responses.
//
// To regenerate the golden files: DYNAMODB_GOLDEN_UPDATE=1 go test -run TestGolden ./services/dynamodb/...

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func goldenPath(name string) string {
	return filepath.Join("testdata", name)
}

// checkGolden writes got to testdata/name when DYNAMODB_GOLDEN_UPDATE=1 is
// set, otherwise asserts got is byte-identical to the committed golden file.
func checkGolden(t *testing.T, name string, got []byte) {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, json.Indent(&buf, got, "", "  "))
	pretty := buf.Bytes()

	path := goldenPath(name)
	if os.Getenv("DYNAMODB_GOLDEN_UPDATE") != "" {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, pretty, 0o600))

		return
	}

	want, err := os.ReadFile(path)
	require.NoError(t, err, "golden file %s missing -- run with DYNAMODB_GOLDEN_UPDATE=1 first", path)
	require.Equal(t, string(want), string(pretty), "response for %s changed", name)
}

func createGoldenTableWithGSI(t *testing.T, h *dynamodb.DynamoDBHandler, tblName string) {
	t.Helper()
	code, resp := invokeOp(t, h, "CreateTable", map[string]any{
		"TableName": tblName,
		"KeySchema": []map[string]any{
			{"AttributeName": "pk", "KeyType": "HASH"},
			{"AttributeName": "sk", "KeyType": "RANGE"},
		},
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": "pk", "AttributeType": "S"},
			{"AttributeName": "sk", "AttributeType": "S"},
			{"AttributeName": "gsipk", "AttributeType": "S"},
		},
		"GlobalSecondaryIndexes": []map[string]any{
			{
				"IndexName": "gsi1",
				"KeySchema": []map[string]any{
					{"AttributeName": "gsipk", "KeyType": "HASH"},
				},
				"Projection": map[string]any{"ProjectionType": "ALL"},
			},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, 200, code, "CreateTable %s: %v", tblName, resp)
}

func TestGolden_QueryKeyConditionFilterProjection(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	db.SetDefaultRegion("us-east-1")
	h := dynamodb.NewHandler(db)
	createGoldenTableWithGSI(t, h, "GoldenQueryTable")

	for i := range 20 {
		seedItemViaHandler(t, h, "GoldenQueryTable", map[string]any{
			"pk":    map[string]any{"S": "cust#1"},
			"sk":    map[string]any{"S": fmt.Sprintf("order#%03d", i)},
			"val":   map[string]any{"N": strconv.Itoa(i * 10)},
			"gsipk": map[string]any{"S": fmt.Sprintf("g%d", i%3)},
		})
	}
	for i := range 5 {
		seedItemViaHandler(t, h, "GoldenQueryTable", map[string]any{
			"pk":    map[string]any{"S": "cust#2"},
			"sk":    map[string]any{"S": fmt.Sprintf("order#%03d", i)},
			"val":   map[string]any{"N": strconv.Itoa(i)},
			"gsipk": map[string]any{"S": "other"},
		})
	}

	code, resp := invokeOp(t, h, "Query", map[string]any{
		"TableName":              "GoldenQueryTable",
		"KeyConditionExpression": "pk = :pk",
		"FilterExpression":       "val > :val",
		"ProjectionExpression":   "pk, sk, val",
		"ExpressionAttributeValues": map[string]any{
			":pk":  map[string]any{"S": "cust#1"},
			":val": map[string]any{"N": "100"},
		},
		"ReturnConsumedCapacity": "TOTAL",
	})
	require.Equal(t, 200, code, "Query: %v", resp)

	raw, err := json.Marshal(resp)
	require.NoError(t, err)
	checkGolden(t, "golden_query.json", raw)
}

func TestGolden_ScanFilter(t *testing.T) {
	t.Parallel()

	h := newHandlerWithTable(t, "GoldenScanTable")

	for i := range 50 {
		seedItemViaHandler(t, h, "GoldenScanTable", map[string]any{
			"pk":  map[string]any{"S": fmt.Sprintf("item#%03d", i)},
			"val": map[string]any{"N": strconv.Itoa(i)},
		})
	}

	code, resp := invokeOp(t, h, "Scan", map[string]any{
		"TableName":        "GoldenScanTable",
		"FilterExpression": "val > :val",
		"ExpressionAttributeValues": map[string]any{
			":val": map[string]any{"N": "25"},
		},
		"ReturnConsumedCapacity": "TOTAL",
	})
	require.Equal(t, 200, code, "Scan: %v", resp)

	raw, err := json.Marshal(resp)
	require.NoError(t, err)
	checkGolden(t, "golden_scan.json", raw)
}

func TestGolden_TransactWriteItems(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	db.SetDefaultRegion("us-east-1")
	h := dynamodb.NewHandler(db)
	createGoldenTableWithGSI(t, h, "GoldenTransactTable")

	for i := range 20 {
		seedItemViaHandler(t, h, "GoldenTransactTable", map[string]any{
			"pk":    map[string]any{"S": "item"},
			"sk":    map[string]any{"S": fmt.Sprintf("%03d", i)},
			"val":   map[string]any{"N": strconv.Itoa(i)},
			"gsipk": map[string]any{"S": fmt.Sprintf("g%d", i%4)},
		})
	}

	transactItems := make([]map[string]any, 0, 10)
	for i := range 10 {
		transactItems = append(transactItems, map[string]any{
			"Put": map[string]any{
				"TableName": "GoldenTransactTable",
				"Item": map[string]any{
					"pk":    map[string]any{"S": "item"},
					"sk":    map[string]any{"S": fmt.Sprintf("%03d", i)},
					"val":   map[string]any{"N": strconv.Itoa(i + 1000)},
					"gsipk": map[string]any{"S": fmt.Sprintf("g%d", (i+1)%4)},
				},
			},
		})
	}

	code, resp := invokeOp(t, h, "TransactWriteItems", map[string]any{
		"TransactItems":          transactItems,
		"ReturnConsumedCapacity": "INDEXES",
	})
	require.Equal(t, 200, code, "TransactWriteItems: %v", resp)

	raw, err := json.Marshal(resp)
	require.NoError(t, err)
	checkGolden(t, "golden_transact.json", raw)
}
