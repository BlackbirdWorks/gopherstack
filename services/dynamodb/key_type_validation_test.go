package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func newKeyTypeClient(t *testing.T) *dynamodbsdk.Client {
	t.Helper()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
	_, err := client.CreateTable(t.Context(), &dynamodbsdk.CreateTableInput{
		TableName: aws.String("keytypes"),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("gk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
			IndexName: aws.String("by-gk"),
			KeySchema: []dynamodbtypes.KeySchemaElement{
				{AttributeName: aws.String("gk"), KeyType: dynamodbtypes.KeyTypeHash},
			},
			Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
		}},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	return client
}

func TestKeyTypeMismatch_Rejected_SDK(t *testing.T) {
	t.Parallel()

	numKey := map[string]dynamodbtypes.AttributeValue{"pk": &dynamodbtypes.AttributeValueMemberN{Value: "1"}}

	tests := []struct {
		run     func(t *testing.T, c *dynamodbsdk.Client) error
		name    string
		wantMsg string
	}{
		{
			name: "put",
			run: func(t *testing.T, c *dynamodbsdk.Client) error {
				t.Helper()
				_, err := c.PutItem(
					t.Context(),
					&dynamodbsdk.PutItemInput{TableName: aws.String("keytypes"), Item: numKey},
				)

				return err
			},
			wantMsg: "Type mismatch for key pk expected: S actual: N",
		},
		{
			name: "put index key",
			run: func(t *testing.T, c *dynamodbsdk.Client) error {
				t.Helper()
				_, err := c.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
					TableName: aws.String("keytypes"),
					Item: map[string]dynamodbtypes.AttributeValue{
						"pk": &dynamodbtypes.AttributeValueMemberS{Value: "1"},
						"gk": &dynamodbtypes.AttributeValueMemberN{Value: "1"},
					},
				})

				return err
			},
			wantMsg: "Type mismatch for Index Key gk Expected: S Actual: N",
		},
		{
			name: "get",
			run: func(t *testing.T, c *dynamodbsdk.Client) error {
				t.Helper()
				_, err := c.GetItem(
					t.Context(),
					&dynamodbsdk.GetItemInput{TableName: aws.String("keytypes"), Key: numKey},
				)

				return err
			},
			wantMsg: "The provided key element does not match the schema",
		},
		{
			name: "delete",
			run: func(t *testing.T, c *dynamodbsdk.Client) error {
				t.Helper()
				_, err := c.DeleteItem(
					t.Context(),
					&dynamodbsdk.DeleteItemInput{TableName: aws.String("keytypes"), Key: numKey},
				)

				return err
			},
			wantMsg: "The provided key element does not match the schema",
		},
		{
			name: "update",
			run: func(t *testing.T, c *dynamodbsdk.Client) error {
				t.Helper()
				_, err := c.UpdateItem(t.Context(), &dynamodbsdk.UpdateItemInput{
					TableName:        aws.String("keytypes"),
					Key:              numKey,
					UpdateExpression: aws.String("SET a = :v"),
					ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
						":v": &dynamodbtypes.AttributeValueMemberS{Value: "x"},
					},
				})

				return err
			},
			wantMsg: "The provided key element does not match the schema",
		},
		{
			name: "batch write put",
			run: func(t *testing.T, c *dynamodbsdk.Client) error {
				t.Helper()
				_, err := c.BatchWriteItem(t.Context(), &dynamodbsdk.BatchWriteItemInput{
					RequestItems: map[string][]dynamodbtypes.WriteRequest{
						"keytypes": {{PutRequest: &dynamodbtypes.PutRequest{Item: numKey}}},
					},
				})

				return err
			},
			wantMsg: "Type mismatch for key pk expected: S actual: N",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.run(t, newKeyTypeClient(t))
			require.Error(t, err)

			var ve interface{ ErrorCode() string }
			require.ErrorAs(t, err, &ve)
			assert.Equal(t, "ValidationException", ve.ErrorCode())
			assert.Contains(t, err.Error(), tc.wantMsg)
		})
	}
}

func TestListTables_LimitBounds_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantMsg string
		limit   int32
	}{
		{name: "negative", limit: -5, wantMsg: "greater than or equal to 1"},
		{name: "over max", limit: 101, wantMsg: "less than or equal to 100"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newKeyTypeClient(t)
			_, err := client.ListTables(t.Context(), &dynamodbsdk.ListTablesInput{Limit: aws.Int32(tc.limit)})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantMsg, err.Error())
		})
	}
}

func TestQuery_MissingKeyCondition_SDK(t *testing.T) {
	t.Parallel()

	client := newKeyTypeClient(t)
	_, err := client.Query(t.Context(), &dynamodbsdk.QueryInput{TableName: aws.String("keytypes")})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ValidationException")
}

func TestPutItem_NestingDepthLimit_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		depth   int
		wantErr bool
	}{
		{name: "at limit", depth: 31},
		{name: "over limit", depth: 40, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var nested dynamodbtypes.AttributeValue = &dynamodbtypes.AttributeValueMemberS{Value: "x"}
			for range tc.depth {
				nested = &dynamodbtypes.AttributeValueMemberM{
					Value: map[string]dynamodbtypes.AttributeValue{"a": nested},
				}
			}

			client := newKeyTypeClient(t)
			_, err := client.PutItem(t.Context(), &dynamodbsdk.PutItemInput{
				TableName: aws.String("keytypes"),
				Item: map[string]dynamodbtypes.AttributeValue{
					"pk":   &dynamodbtypes.AttributeValueMemberS{Value: "1"},
					"deep": nested,
				},
			})
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")

				return
			}

			require.NoError(t, err)
		})
	}
}
