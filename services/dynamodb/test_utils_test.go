package dynamodb_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

	"github.com/stretchr/testify/require"
)

func mustUnmarshal[T any](t *testing.T, jsonStr string) T {
	t.Helper()
	var val T
	err := json.Unmarshal([]byte(jsonStr), &val)
	require.NoError(t, err)

	return val
}

func createTableHelper(
	t *testing.T,
	db *dynamodb.InMemoryDB,
	name string,
	pk string,
	sk ...string,
) {
	t.Helper()
	keySchema := []models.KeySchemaElement{
		{AttributeName: pk, KeyType: models.KeyTypeHash},
	}
	attributeDefinitions := []models.AttributeDefinition{
		{AttributeName: pk, AttributeType: "S"},
	}

	if len(sk) > 0 {
		keySchema = append(keySchema, models.KeySchemaElement{
			AttributeName: sk[0], KeyType: models.KeyTypeRange,
		})
		attributeDefinitions = append(
			attributeDefinitions, models.AttributeDefinition{
				AttributeName: sk[0], AttributeType: "S",
			},
		)
	}

	rcDefault := int64(5)
	wcDefault := int64(5)
	createInput := models.CreateTableInput{
		TableName:            name,
		KeySchema:            keySchema,
		AttributeDefinitions: attributeDefinitions,
	}
	sdkInput := models.ToSDKCreateTableInput(&createInput)
	// DynamoDB requires ProvisionedThroughput when BillingMode is PROVISIONED (the default).
	sdkInput.ProvisionedThroughput = &sdktypes.ProvisionedThroughput{
		ReadCapacityUnits:  &rcDefault,
		WriteCapacityUnits: &wcDefault,
	}
	_, err := db.CreateTable(t.Context(), sdkInput)
	require.NoError(t, err)
}

// newInMemoryTestDB creates a fresh InMemoryDB backend scoped to us-east-1.
func newInMemoryTestDB(t *testing.T) *dynamodb.InMemoryDB {
	t.Helper()
	db := dynamodb.NewInMemoryDB()
	db.SetDefaultRegion("us-east-1")

	return db
}

// createSimpleTestTable creates a PROVISIONED table with a pk/sk key schema.
func createSimpleTestTable(t *testing.T, db *dynamodb.InMemoryDB, tableName string) {
	t.Helper()
	ctx := context.Background()
	_, err := db.CreateTable(ctx, &sdkddb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []sdktypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: sdktypes.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: sdktypes.KeyTypeRange},
		},
		AttributeDefinitions: []sdktypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: sdktypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: sdktypes.ScalarAttributeTypeS},
		},
		BillingMode: sdktypes.BillingModeProvisioned,
		ProvisionedThroughput: &sdktypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	})
	if err != nil {
		t.Fatalf("createSimpleTestTable: %v", err)
	}
}

// createOnDemandTestTable creates a PAY_PER_REQUEST table with a pk-only key schema.
func createOnDemandTestTable(t *testing.T, db *dynamodb.InMemoryDB, tableName string) {
	t.Helper()
	ctx := context.Background()
	_, err := db.CreateTable(ctx, &sdkddb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []sdktypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: sdktypes.KeyTypeHash},
		},
		AttributeDefinitions: []sdktypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: sdktypes.ScalarAttributeTypeS},
		},
		BillingMode: sdktypes.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("createOnDemandTestTable: %v", err)
	}
}

// putTestItem writes a single wire-format item to tableName via PutItem.
func putTestItem(
	t *testing.T,
	db *dynamodb.InMemoryDB,
	tableName string,
	item map[string]sdktypes.AttributeValue,
) {
	t.Helper()
	_, err := db.PutItem(context.Background(), &sdkddb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	})
	if err != nil {
		t.Fatalf("putTestItem: %v", err)
	}
}

// assertErrorCode fails the test unless err is non-nil and its message contains wantCode.
func assertErrorCode(t *testing.T, err error, wantCode string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error containing %q but got nil", wantCode)
	}

	if !strings.Contains(err.Error(), wantCode) {
		t.Fatalf("expected error containing %q, got: %v", wantCode, err)
	}
}

// newTestDBWithCleanup creates an InMemoryDB that is closed via t.Cleanup.
func newTestDBWithCleanup(t *testing.T) *dynamodb.InMemoryDB {
	t.Helper()

	db := dynamodb.NewInMemoryDB()
	t.Cleanup(db.Close)

	return db
}

// createSimplePPRTable creates a PAY_PER_REQUEST table with a pk-only key schema.
func createSimplePPRTable(t *testing.T, db *dynamodb.InMemoryDB, tableName string) {
	t.Helper()

	ctx := t.Context()
	_, err := db.CreateTable(ctx, &sdkddb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []sdktypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: sdktypes.KeyTypeHash},
		},
		AttributeDefinitions: []sdktypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: sdktypes.ScalarAttributeTypeS},
		},
		BillingMode: sdktypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)
}

func invokeOp(
	t *testing.T,
	handler *dynamodb.DynamoDBHandler,
	action string,
	body any,
) (int, map[string]any) {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+action)
	w := httptest.NewRecorder()

	_ = serveEchoHandler(handler.Handler(), w, req)

	var resp map[string]any
	if w.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	}

	return w.Code, resp
}

// newHandlerWithTable creates a fresh InMemoryDB + handler + table named tblName.
func newHandlerWithTable(t *testing.T, tblName string) *dynamodb.DynamoDBHandler {
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

// seedItemViaHandler puts one item (a map of attr → {"S":"..."}) into tblName via the handler.
func seedItemViaHandler(t *testing.T, h *dynamodb.DynamoDBHandler, tblName string, item map[string]any) {
	t.Helper()
	code, _ := invokeOp(t, h, "PutItem", map[string]any{"TableName": tblName, "Item": item})
	require.Equal(t, 200, code, "PutItem seed")
}

// getItemAttrsViaHandler retrieves a single item by string pk from tblName via the
// handler; returns nil if not found.
func getItemAttrsViaHandler(
	t *testing.T,
	h *dynamodb.DynamoDBHandler,
	tblName, pkVal string,
) map[string]any {
	t.Helper()
	_, resp := invokeOp(t, h, "GetItem", map[string]any{
		"TableName": tblName,
		"Key":       map[string]any{"pk": map[string]string{"S": pkVal}},
	})
	item, _ := resp["Item"].(map[string]any)

	return item
}
