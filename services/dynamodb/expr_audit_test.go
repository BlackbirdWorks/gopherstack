package dynamodb_test

import (
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// auditTableName is the table name used by every gopherstack-hpzv
// expr/PartiQL audit test below.
const auditTableName = "T"

// setupAuditTable creates the single-PK table used by the gopherstack-hpzv
// expr/PartiQL audit tests below.
func setupAuditTable(t *testing.T, db *dynamodb.InMemoryDB) {
	t.Helper()
	ctInput := models.CreateTableInput{
		TableName: auditTableName,
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: "HASH"},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
	}
	_, err := db.CreateTable(t.Context(), models.ToSDKCreateTableInput(&ctInput))
	require.NoError(t, err)
}

// asValidationException asserts err is a *dynamodb.Error carrying a
// ValidationException type, and returns it for message inspection.
func asValidationException(t *testing.T, err error) *dynamodb.Error {
	t.Helper()
	require.Error(t, err)
	var wireErr *dynamodb.Error
	require.ErrorAs(t, err, &wireErr, "expected *dynamodb.Error, got %T: %v", err, err)
	assert.Contains(t, wireErr.Type, "ValidationException",
		"expected ValidationException (400), not InternalServerError (500); got %q: %s",
		wireErr.Type, wireErr.Message)

	return wireErr
}

// TestUpdateItem_ExprAudit_NestedPathRequiresExistingParent verifies the
// documented rule: "You cannot update nested map attributes if the parent
// map does not exist. ... DynamoDB returns a ValidationException with the
// message 'The document path provided in the update expression is invalid
// for update.'"
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
func TestUpdateItem_ExprAudit_NestedPathRequiresExistingParent(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	setupAuditTable(t, db)

	putInput := models.PutItemInput{
		TableName: "T",
		Item:      map[string]any{"pk": map[string]any{"S": "1"}},
	}
	sdkPut, _ := models.ToSDKPutItemInput(&putInput)
	_, err := db.PutItem(t.Context(), sdkPut)
	require.NoError(t, err)

	updateInput := mustUnmarshal[models.UpdateItemInput](t, `{
		"TableName": "T",
		"Key": {"pk": {"S": "1"}},
		"UpdateExpression": "SET reviews.fiveStar = :v",
		"ExpressionAttributeValues": {":v": {"S": "great"}}
	}`)
	sdkUpdate, _ := models.ToSDKUpdateItemInput(&updateInput)

	_, err = db.UpdateItem(t.Context(), sdkUpdate)
	wireErr := asValidationException(t, err)
	assert.Contains(t, wireErr.Message, "The document path provided in the update expression is invalid for update")
}

// TestUpdateItem_ExprAudit_OverlappingPaths verifies that referencing the
// same or overlapping document paths twice in one UpdateExpression is
// rejected (AWS observed error: "Two document paths overlap").
func TestUpdateItem_ExprAudit_OverlappingPaths(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	setupAuditTable(t, db)

	putInput := models.PutItemInput{
		TableName: "T",
		Item: map[string]any{
			"pk": map[string]any{"S": "1"},
			"a":  map[string]any{"M": map[string]any{}},
		},
	}
	sdkPut, _ := models.ToSDKPutItemInput(&putInput)
	_, err := db.PutItem(t.Context(), sdkPut)
	require.NoError(t, err)

	updateInput := mustUnmarshal[models.UpdateItemInput](t, `{
		"TableName": "T",
		"Key": {"pk": {"S": "1"}},
		"UpdateExpression": "SET a = :v, a.b = :v",
		"ExpressionAttributeValues": {":v": {"S": "x"}}
	}`)
	sdkUpdate, _ := models.ToSDKUpdateItemInput(&updateInput)

	_, err = db.UpdateItem(t.Context(), sdkUpdate)
	wireErr := asValidationException(t, err)
	assert.Contains(t, strings.ToLower(wireErr.Message), "overlap")
}

// TestUpdateItem_ExprAudit_DuplicateSection verifies AWS rejects an
// UpdateExpression that repeats the same action keyword, per the documented
// rule that each action keyword can appear only once.
func TestUpdateItem_ExprAudit_DuplicateSection(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	setupAuditTable(t, db)

	updateInput := mustUnmarshal[models.UpdateItemInput](t, `{
		"TableName": "T",
		"Key": {"pk": {"S": "1"}},
		"UpdateExpression": "SET a = :v SET b = :v",
		"ExpressionAttributeValues": {":v": {"S": "x"}}
	}`)
	sdkUpdate, _ := models.ToSDKUpdateItemInput(&updateInput)

	_, err := db.UpdateItem(t.Context(), sdkUpdate)
	asValidationException(t, err)
}

// TestUpdateItem_ExprAudit_ADDTypeMismatch verifies ADD against an existing
// non-Number/Set attribute is rejected rather than silently ignored.
func TestUpdateItem_ExprAudit_ADDTypeMismatch(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	setupAuditTable(t, db)

	putInput := models.PutItemInput{
		TableName: "T",
		Item: map[string]any{
			"pk":   map[string]any{"S": "1"},
			"data": map[string]any{"S": "x"},
		},
	}
	sdkPut, _ := models.ToSDKPutItemInput(&putInput)
	_, err := db.PutItem(t.Context(), sdkPut)
	require.NoError(t, err)

	updateInput := mustUnmarshal[models.UpdateItemInput](t, `{
		"TableName": "T",
		"Key": {"pk": {"S": "1"}},
		"UpdateExpression": "ADD data :v",
		"ExpressionAttributeValues": {":v": {"N": "1"}}
	}`)
	sdkUpdate, _ := models.ToSDKUpdateItemInput(&updateInput)

	_, err = db.UpdateItem(t.Context(), sdkUpdate)
	asValidationException(t, err)

	// The attribute must be unchanged: ADD must not have silently no-op'd
	// past a rejected write, and it must not have coerced the value either.
	item := getItem(t, db, "T", "1")
	assert.Equal(t, "x", item["data"].(map[string]any)["S"])
}

// TestUpdateItem_ExprAudit_UndefinedValuePlaceholder verifies that a
// ConditionExpression referencing an undefined :value placeholder surfaces
// as a 400 ValidationException, not a 500 InternalServerError. Previously
// the raw expr package error (not a *dynamodb.Error) fell through
// classifyError's default branch to InternalServerError.
func TestUpdateItem_ExprAudit_UndefinedValuePlaceholder(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	setupAuditTable(t, db)

	putInput := models.PutItemInput{
		TableName: "T",
		Item:      map[string]any{"pk": map[string]any{"S": "1"}},
	}
	sdkPut, _ := models.ToSDKPutItemInput(&putInput)
	_, err := db.PutItem(t.Context(), sdkPut)
	require.NoError(t, err)

	updateInput := mustUnmarshal[models.UpdateItemInput](t, `{
		"TableName": "T",
		"Key": {"pk": {"S": "1"}},
		"UpdateExpression": "SET x = :v",
		"ConditionExpression": "y = :missing",
		"ExpressionAttributeValues": {":v": {"S": "z"}}
	}`)
	sdkUpdate, _ := models.ToSDKUpdateItemInput(&updateInput)

	_, err = db.UpdateItem(t.Context(), sdkUpdate)
	wireErr := asValidationException(t, err)
	assert.Contains(t, wireErr.Message, ":missing")
}

// TestQuery_ExprAudit_UndefinedFilterPlaceholder verifies a FilterExpression
// referencing an undefined :value placeholder is rejected up front rather
// than silently matching zero items (ParsedCondition.Evaluate previously
// swallowed all per-item evaluation errors as "no match").
func TestQuery_ExprAudit_UndefinedFilterPlaceholder(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	setupAuditTable(t, db)

	putInput := models.PutItemInput{
		TableName: "T",
		Item:      map[string]any{"pk": map[string]any{"S": "1"}, "n": map[string]any{"N": "5"}},
	}
	sdkPut, _ := models.ToSDKPutItemInput(&putInput)
	_, err := db.PutItem(t.Context(), sdkPut)
	require.NoError(t, err)

	queryInput := mustUnmarshal[models.QueryInput](t, `{
		"TableName": "T",
		"KeyConditionExpression": "pk = :pk",
		"FilterExpression": "n = :undefined",
		"ExpressionAttributeValues": {":pk": {"S": "1"}}
	}`)
	sdkQuery, _ := models.ToSDKQueryInput(&queryInput)

	_, err = db.Query(t.Context(), sdkQuery)
	wireErr := asValidationException(t, err)
	assert.Contains(t, wireErr.Message, ":undefined")
}

// TestScan_ExprAudit_UndefinedFilterPlaceholder mirrors the Query case for Scan.
func TestScan_ExprAudit_UndefinedFilterPlaceholder(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	setupAuditTable(t, db)

	putInput := models.PutItemInput{
		TableName: "T",
		Item:      map[string]any{"pk": map[string]any{"S": "1"}, "n": map[string]any{"N": "5"}},
	}
	sdkPut, _ := models.ToSDKPutItemInput(&putInput)
	_, err := db.PutItem(t.Context(), sdkPut)
	require.NoError(t, err)

	scanInput := mustUnmarshal[models.ScanInput](t, `{
		"TableName": "T",
		"FilterExpression": "n = :undefined"
	}`)
	sdkScan, _ := models.ToSDKScanInput(&scanInput)

	_, err = db.Scan(t.Context(), sdkScan)
	wireErr := asValidationException(t, err)
	assert.Contains(t, wireErr.Message, ":undefined")
}

// TestPutItem_ExprAudit_UndefinedNamePlaceholder verifies a ConditionExpression
// referencing an undefined #name placeholder is rejected as a ValidationException.
func TestPutItem_ExprAudit_UndefinedNamePlaceholder(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	setupAuditTable(t, db)

	putInput := mustUnmarshal[models.PutItemInput](t, `{
		"TableName": "T",
		"Item": {"pk": {"S": "1"}},
		"ConditionExpression": "attribute_not_exists(#missing)"
	}`)
	sdkPut, _ := models.ToSDKPutItemInput(&putInput)

	_, err := db.PutItem(t.Context(), sdkPut)
	wireErr := asValidationException(t, err)
	assert.Contains(t, wireErr.Message, "#missing")
}
