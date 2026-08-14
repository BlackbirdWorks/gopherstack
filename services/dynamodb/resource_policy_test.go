package dynamodb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestDeleteResourcePolicy(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	createTableHelper(t, backend, "MyTable", "pk")

	// Get the table ARN from DescribeTable.
	code, resp := invokeOp(t, handler, "DescribeTable", map[string]any{"TableName": "MyTable"})
	require.Equal(t, http.StatusOK, code)
	tableDesc, _ := resp["Table"].(map[string]any)
	tableARN, _ := tableDesc["TableArn"].(string)
	require.NotEmpty(t, tableARN)

	code, _ = invokeOp(t, handler, "DeleteResourcePolicy", map[string]any{
		"ResourceArn": tableARN,
	})
	assert.Equal(t, http.StatusOK, code)
}

func getTestTableARN(
	t *testing.T,
	backend *dynamodb.InMemoryDB,
	handler *dynamodb.DynamoDBHandler,
	tableName string,
) string {
	t.Helper()

	createTableHelper(t, backend, tableName, "pk")

	code, resp := invokeOp(t, handler, "DescribeTable", map[string]any{"TableName": tableName})
	require.Equal(t, http.StatusOK, code)

	tableDesc, _ := resp["Table"].(map[string]any)
	tableARN, _ := tableDesc["TableArn"].(string)
	require.NotEmpty(t, tableARN)

	return tableARN
}

func TestResourcePolicy(t *testing.T) {
	t.Parallel()

	t.Run("GetResourcePolicy_success", func(t *testing.T) {
		t.Parallel()
		backend := dynamodb.NewInMemoryDB()
		handler := dynamodb.NewHandler(backend)
		tableARN := getTestTableARN(t, backend, handler, "RPGetTable")
		code, _ := invokeOp(
			t,
			handler,
			"GetResourcePolicy",
			map[string]any{"ResourceArn": tableARN},
		)
		assert.Equal(t, http.StatusOK, code)
	})

	t.Run("GetResourcePolicy_missing_arn", func(t *testing.T) {
		t.Parallel()
		backend := dynamodb.NewInMemoryDB()
		handler := dynamodb.NewHandler(backend)
		code, resp := invokeOp(t, handler, "GetResourcePolicy", map[string]any{"ResourceArn": ""})
		assert.Equal(t, http.StatusBadRequest, code)
		bodyBytes, _ := json.Marshal(resp)
		assert.Contains(t, string(bodyBytes), "ValidationException")
	})

	t.Run("PutResourcePolicy_success", func(t *testing.T) {
		t.Parallel()
		backend := dynamodb.NewInMemoryDB()
		handler := dynamodb.NewHandler(backend)
		tableARN := getTestTableARN(t, backend, handler, "RPPutTable")
		code, _ := invokeOp(t, handler, "PutResourcePolicy", map[string]any{
			"ResourceArn": tableARN,
			"Policy":      `{"Version":"2012-10-17","Statement":[]}`,
		})
		assert.Equal(t, http.StatusOK, code)

		// Verify round-trip: GetResourcePolicy returns the stored policy.
		code2, resp2 := invokeOp(
			t,
			handler,
			"GetResourcePolicy",
			map[string]any{"ResourceArn": tableARN},
		)
		assert.Equal(t, http.StatusOK, code2)
		bodyBytes, _ := json.Marshal(resp2)
		assert.Contains(t, string(bodyBytes), "2012-10-17")
	})

	t.Run("PutResourcePolicy_missing_policy", func(t *testing.T) {
		t.Parallel()
		backend := dynamodb.NewInMemoryDB()
		handler := dynamodb.NewHandler(backend)
		tableARN := getTestTableARN(t, backend, handler, "RPMissingPolicyTable")
		code, resp := invokeOp(
			t,
			handler,
			"PutResourcePolicy",
			map[string]any{"ResourceArn": tableARN},
		)
		assert.Equal(t, http.StatusBadRequest, code)
		bodyBytes, _ := json.Marshal(resp)
		assert.Contains(t, string(bodyBytes), "ValidationException")
	})

	t.Run("DeleteResourcePolicy_success", func(t *testing.T) {
		t.Parallel()
		backend := dynamodb.NewInMemoryDB()
		handler := dynamodb.NewHandler(backend)
		tableARN := getTestTableARN(t, backend, handler, "RPDeleteTable")
		code, _ := invokeOp(
			t,
			handler,
			"DeleteResourcePolicy",
			map[string]any{"ResourceArn": tableARN},
		)
		assert.Equal(t, http.StatusOK, code)
	})

	t.Run("DeleteResourcePolicy_missing_arn", func(t *testing.T) {
		t.Parallel()
		backend := dynamodb.NewInMemoryDB()
		handler := dynamodb.NewHandler(backend)
		code, resp := invokeOp(
			t,
			handler,
			"DeleteResourcePolicy",
			map[string]any{"ResourceArn": ""},
		)
		assert.Equal(t, http.StatusBadRequest, code)
		bodyBytes, _ := json.Marshal(resp)
		assert.Contains(t, string(bodyBytes), "ValidationException")
	})
}

// TestPutResourcePolicy_ExpectedRevisionId_SurvivesWireConversion verifies
// that PutResourcePolicyInput.ExpectedRevisionId reaches the backend and is
// enforced. resourcePolicyInput previously had no ExpectedRevisionId field at
// all, so a client's optimistic-concurrency check was silently ignored and
// every Put succeeded no matter what revision the caller expected.
func TestPutResourcePolicy_ExpectedRevisionId_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
	keySchema, attrDefs := wireTestKeySchema()

	_, err := client.CreateTable(t.Context(), &dynamodbsdk.CreateTableInput{
		TableName:            aws.String("rp-revision-table"),
		KeySchema:            keySchema,
		AttributeDefinitions: attrDefs,
		BillingMode:          "PAY_PER_REQUEST",
	})
	require.NoError(t, err)

	desc, err := client.DescribeTable(t.Context(), &dynamodbsdk.DescribeTableInput{
		TableName: aws.String("rp-revision-table"),
	})
	require.NoError(t, err)
	tableARN := aws.ToString(desc.Table.TableArn)

	put1, err := client.PutResourcePolicy(t.Context(), &dynamodbsdk.PutResourcePolicyInput{
		ResourceArn: aws.String(tableARN),
		Policy:      aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)
	firstRevision := aws.ToString(put1.RevisionId)
	require.NotEmpty(t, firstRevision, "RevisionId must survive the wire round-trip")

	// A stale ExpectedRevisionId must be rejected with PolicyNotFoundException,
	// and must not overwrite the existing policy.
	_, err = client.PutResourcePolicy(t.Context(), &dynamodbsdk.PutResourcePolicyInput{
		ResourceArn:        aws.String(tableARN),
		Policy:             aws.String(`{"Version":"2012-10-17","Statement":[{"Sid":"stale"}]}`),
		ExpectedRevisionId: aws.String("not-the-real-revision"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PolicyNotFoundException")

	// The correct ExpectedRevisionId must succeed and advance the revision.
	put2, err := client.PutResourcePolicy(t.Context(), &dynamodbsdk.PutResourcePolicyInput{
		ResourceArn:        aws.String(tableARN),
		Policy:             aws.String(`{"Version":"2012-10-17","Statement":[{"Sid":"v2"}]}`),
		ExpectedRevisionId: aws.String(firstRevision),
	})
	require.NoError(t, err)
	assert.NotEqual(t, firstRevision, aws.ToString(put2.RevisionId))

	got, err := client.GetResourcePolicy(t.Context(), &dynamodbsdk.GetResourcePolicyInput{
		ResourceArn: aws.String(tableARN),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(got.Policy), "v2")
}

// TestDeleteResourcePolicy_ExpectedRevisionId_SurvivesWireConversion verifies
// that DeleteResourcePolicyInput.ExpectedRevisionId reaches the backend and is
// enforced. deleteResourcePolicyInput previously had no ExpectedRevisionId
// field, so a conditional delete always succeeded regardless of the policy's
// current revision, and the handler discarded the backend's RevisionId output
// entirely.
func TestDeleteResourcePolicy_ExpectedRevisionId_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
	keySchema, attrDefs := wireTestKeySchema()

	_, err := client.CreateTable(t.Context(), &dynamodbsdk.CreateTableInput{
		TableName:            aws.String("rp-delete-revision-table"),
		KeySchema:            keySchema,
		AttributeDefinitions: attrDefs,
		BillingMode:          "PAY_PER_REQUEST",
	})
	require.NoError(t, err)

	desc, err := client.DescribeTable(t.Context(), &dynamodbsdk.DescribeTableInput{
		TableName: aws.String("rp-delete-revision-table"),
	})
	require.NoError(t, err)
	tableARN := aws.ToString(desc.Table.TableArn)

	put, err := client.PutResourcePolicy(t.Context(), &dynamodbsdk.PutResourcePolicyInput{
		ResourceArn: aws.String(tableARN),
		Policy:      aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)
	revision := aws.ToString(put.RevisionId)
	require.NotEmpty(t, revision)

	// A stale ExpectedRevisionId must be rejected, leaving the policy in place.
	_, err = client.DeleteResourcePolicy(t.Context(), &dynamodbsdk.DeleteResourcePolicyInput{
		ResourceArn:        aws.String(tableARN),
		ExpectedRevisionId: aws.String("not-the-real-revision"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PolicyNotFoundException")

	got, err := client.GetResourcePolicy(t.Context(), &dynamodbsdk.GetResourcePolicyInput{
		ResourceArn: aws.String(tableARN),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(got.Policy), "policy must survive the rejected delete")

	// The correct ExpectedRevisionId must succeed and echo the deleted revision.
	del, err := client.DeleteResourcePolicy(t.Context(), &dynamodbsdk.DeleteResourcePolicyInput{
		ResourceArn:        aws.String(tableARN),
		ExpectedRevisionId: aws.String(revision),
	})
	require.NoError(t, err)
	assert.Equal(t, revision, aws.ToString(del.RevisionId), "RevisionId must survive the wire round-trip")
}
