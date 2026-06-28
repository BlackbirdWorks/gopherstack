package dynamodb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// newAuditHandler creates a fresh InMemoryDB + handler + table named tblName.
func newAuditHandler(t *testing.T, tblName string) *dynamodb.DynamoDBHandler {
	t.Helper()
	db := dynamodb.NewInMemoryDB()
	h := dynamodb.NewHandler(db)
	code, _ := invokeOp(t, h, "CreateTable", map[string]any{
		"TableName": tblName,
		"KeySchema": []map[string]any{
			{"AttributeName": "pk", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": "pk", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, 200, code, "CreateTable %s", tblName)

	return h
}

// auditSeedItem puts one item (a map of attr → {"S":"..."}) into tblName.
func auditSeedItem(t *testing.T, h *dynamodb.DynamoDBHandler, tblName string, item map[string]any) {
	t.Helper()
	code, _ := invokeOp(t, h, "PutItem", map[string]any{"TableName": tblName, "Item": item})
	require.Equal(t, 200, code, "PutItem seed")
}

// getItem retrieves a single item by string pk from tblName; returns nil if not found.
func auditGetItemAttr(t *testing.T, h *dynamodb.DynamoDBHandler, tblName, pkVal string) map[string]any {
	t.Helper()
	_, resp := invokeOp(t, h, "GetItem", map[string]any{
		"TableName": tblName,
		"Key":       map[string]any{"pk": map[string]string{"S": pkVal}},
	})
	item, _ := resp["Item"].(map[string]any)

	return item
}

// TestAuditDDB_ExecuteTransaction_Atomicity verifies rollback when any statement fails.
func TestAuditDDB_ExecuteTransaction_Atomicity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantRollback map[string]string // pkVal → original val["S"] value to check
		seed         []map[string]any
		stmts        []string
		wantCode     int
	}{
		{
			name: "single statement succeeds",
			seed: []map[string]any{
				{"pk": map[string]string{"S": "x"}, "val": map[string]string{"S": "old"}},
			},
			stmts:    []string{`UPDATE "TXNTBL" SET val='new' WHERE pk='x'`},
			wantCode: 200,
		},
		{
			name: "second statement fails rolls back first",
			seed: []map[string]any{
				{"pk": map[string]string{"S": "a"}, "val": map[string]string{"S": "original"}},
			},
			stmts: []string{
				`UPDATE "TXNTBL" SET val='modified' WHERE pk='a'`,
				`UPDATE "DOES_NOT_EXIST" SET val='x' WHERE pk='a'`,
			},
			wantCode:     400,
			wantRollback: map[string]string{"a": "original"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t, "TXNTBL")
			for _, item := range tc.seed {
				auditSeedItem(t, h, "TXNTBL", item)
			}

			stmtList := make([]map[string]any, len(tc.stmts))
			for i, s := range tc.stmts {
				stmtList[i] = map[string]any{"Statement": s}
			}
			code, _ := invokeOp(t, h, "ExecuteTransaction", map[string]any{
				"TransactStatements": stmtList,
			})
			assert.Equal(t, tc.wantCode, code)

			for pkVal, origVal := range tc.wantRollback {
				item := auditGetItemAttr(t, h, "TXNTBL", pkVal)
				require.NotNil(t, item, "item pk=%s must exist after rollback", pkVal)
				valAttr, ok := item["val"].(map[string]any)
				require.True(t, ok, "val must be a map")
				assert.Equal(t, origVal, valAttr["S"], "val must be rolled back for pk=%s", pkVal)
			}
		})
	}
}

// TestAuditDDB_ListImports_Pagination verifies NextToken cursor and PageSize.
func TestAuditDDB_ListImports_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		seedCount     int
		wantCount     int
		pageSize      int32
		wantNextToken bool
	}{
		{
			name:          "no imports returns empty",
			seedCount:     0,
			pageSize:      25,
			wantCount:     0,
			wantNextToken: false,
		},
		{
			name:          "fewer than page size returns all",
			seedCount:     2,
			pageSize:      25,
			wantCount:     2,
			wantNextToken: false,
		},
		{
			name:          "more than page size returns NextToken",
			seedCount:     5,
			pageSize:      3,
			wantCount:     3,
			wantNextToken: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			h := dynamodb.NewHandler(db)

			// Seed by calling ImportTable once per needed import, each with a unique table name.
			for i := range tc.seedCount {
				tblName := "IMP" + string(rune('A'+i))
				invokeOp(t, h, "ImportTable", map[string]any{
					"TableCreationParameters": map[string]any{
						"TableName": tblName,
						"KeySchema": []map[string]any{
							{"AttributeName": "pk", "KeyType": "HASH"},
						},
						"AttributeDefinitions": []map[string]any{
							{"AttributeName": "pk", "AttributeType": "S"},
						},
						"BillingMode": "PAY_PER_REQUEST",
					},
					"S3BucketSource": map[string]any{"S3Bucket": "my-bucket"},
					"InputFormat":    "DYNAMODB_JSON",
				})
			}

			code, resp := invokeOp(t, h, "ListImports", map[string]any{"PageSize": tc.pageSize})
			assert.Equal(t, 200, code)

			imports, _ := resp["ImportSummaryList"].([]any)
			assert.Len(t, imports, tc.wantCount)
			_, hasNextToken := resp["NextToken"]
			assert.Equal(t, tc.wantNextToken, hasNextToken, "NextToken presence mismatch")
		})
	}
}

// TestAuditDDB_PartiQL_UpdateREMOVE verifies REMOVE clause in PartiQL UPDATE.
func TestAuditDDB_PartiQL_UpdateREMOVE(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		seed        map[string]any
		stmt        string
		wantPresent []string // attrs that must still be in item
		wantAbsent  []string // attrs that must not be in item
	}{
		{
			name: "REMOVE only removes specified attribute",
			seed: map[string]any{
				"pk":     map[string]string{"S": "r1"},
				"keep":   map[string]string{"S": "kept"},
				"remove": map[string]string{"S": "gone"},
			},
			stmt:        `UPDATE "PQTBL" REMOVE remove WHERE pk='r1'`,
			wantPresent: []string{"pk", "keep"},
			wantAbsent:  []string{"remove"},
		},
		{
			name: "SET and REMOVE combined",
			seed: map[string]any{
				"pk":     map[string]string{"S": "r2"},
				"keep":   map[string]string{"S": "v1"},
				"remove": map[string]string{"S": "old"},
			},
			stmt:        `UPDATE "PQTBL" SET keep='v2' REMOVE remove WHERE pk='r2'`,
			wantPresent: []string{"pk", "keep"},
			wantAbsent:  []string{"remove"},
		},
		{
			name: "REMOVE non-existent attribute is no-op",
			seed: map[string]any{
				"pk": map[string]string{"S": "r3"},
			},
			stmt:        `UPDATE "PQTBL" REMOVE missing WHERE pk='r3'`,
			wantPresent: []string{"pk"},
			wantAbsent:  []string{"missing"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAuditHandler(t, "PQTBL")
			auditSeedItem(t, h, "PQTBL", tc.seed)

			code, _ := invokeOp(t, h, "ExecuteStatement", map[string]any{"Statement": tc.stmt})
			assert.Equal(t, 200, code, "ExecuteStatement must succeed")

			pkVal := tc.seed["pk"].(map[string]string)["S"]
			item := auditGetItemAttr(t, h, "PQTBL", pkVal)
			require.NotNil(t, item, "item must exist after UPDATE")

			for _, attr := range tc.wantPresent {
				_, ok := item[attr]
				assert.True(t, ok, "attribute %q must be present", attr)
			}
			for _, attr := range tc.wantAbsent {
				_, ok := item[attr]
				assert.False(t, ok, "attribute %q must be absent after REMOVE", attr)
			}
		})
	}
}
