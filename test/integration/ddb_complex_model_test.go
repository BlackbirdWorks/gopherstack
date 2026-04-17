package integration_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_ComplexDataModel(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)
	ctx := t.Context()
	tableName := "ComplexModelTest-" + uuid.NewString()

	// Create table with composite key
	out, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out)
	require.NotNil(t, out.TableDescription)
	assert.Equal(t, tableName, aws.ToString(out.TableDescription.TableName))
	assert.Equal(t, types.TableStatusActive, out.TableDescription.TableStatus)

	t.Cleanup(func() {
		_, deleteErr := client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
		assert.NoError(t, deleteErr)
	})

	time.Sleep(100 * time.Millisecond)

	// Seed data: 3 users across 2 orgs with deep nested attributes
	type seedUser struct {
		id      string
		orgID   string
		source  string
		theme   string
		tags    []string
		version int
	}

	seeds := []seedUser{
		{
			id:      "user-1",
			orgID:   "org-123",
			source:  "web",
			theme:   "dark",
			version: 1,
			tags:    []string{"admin", "editor"},
		},
		{
			id:      "user-2",
			orgID:   "org-123",
			source:  "mobile",
			theme:   "light",
			version: 1,
			tags:    []string{"viewer"},
		},
		{id: "user-3", orgID: "org-999", source: "api", theme: "auto", version: 2},
	}

	modelType := "USER_PROFILE"
	for _, u := range seeds {
		sk := fmt.Sprintf("ORG#%s#USER#%s", u.orgID, u.id)
		item := map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: modelType},
			"sk": &types.AttributeValueMemberS{Value: sk},
			"DeepData": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"ID":  &types.AttributeValueMemberS{Value: u.id},
				"Org": &types.AttributeValueMemberS{Value: u.orgID},
				"Meta": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
					"Source": &types.AttributeValueMemberS{Value: u.source},
					"Ver":    &types.AttributeValueMemberN{Value: strconv.Itoa(u.version)},
				}},
				"Config": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
					"Theme": &types.AttributeValueMemberS{Value: u.theme},
				}},
			}},
			"Tags": &types.AttributeValueMemberL{Value: []types.AttributeValue{}},
		}
		if len(u.tags) > 0 {
			tagList := make([]types.AttributeValue, len(u.tags))
			for i, tag := range u.tags {
				tagList[i] = &types.AttributeValueMemberS{Value: tag}
			}
			item["Tags"] = &types.AttributeValueMemberL{Value: tagList}
		}
		_, putErr := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		})
		require.NoError(t, putErr)
	}

	tests := []struct {
		input  *dynamodb.QueryInput
		verify func(t *testing.T, out *dynamodb.QueryOutput)
		name   string
	}{
		{
			name: "QueryByOrg_WithDeepProjection",
			input: &dynamodb.QueryInput{
				TableName:              aws.String(tableName),
				KeyConditionExpression: aws.String("pk = :type AND begins_with(sk, :orgPrefix)"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":type":      &types.AttributeValueMemberS{Value: modelType},
					":orgPrefix": &types.AttributeValueMemberS{Value: "ORG#org-123"},
				},
				ProjectionExpression: aws.String(
					"sk, DeepData.Config.Theme, DeepData.Meta.Source, Tags",
				),
			},
			verify: func(t *testing.T, out *dynamodb.QueryOutput) {
				t.Helper()
				assert.Equal(t, int32(2), out.Count)
				assert.Len(t, out.Items, 2)

				// Use AssertItem for more robust checks
				for _, item := range out.Items {
					sk := ""
					if v, ok := item["sk"].(*types.AttributeValueMemberS); ok {
						sk = v.Value
					}

					expected := map[string]any{
						"sk": sk,
						"DeepData": map[string]any{
							"Config": map[string]any{"Theme": "dark"},
							"Meta":   map[string]any{"Source": "web"},
						},
						"Tags": []any{"admin", "editor"},
					}

					if sk == "ORG#org-123#USER#user-2" {
						expected["DeepData"] = map[string]any{
							"Config": map[string]any{"Theme": "light"},
							"Meta":   map[string]any{"Source": "mobile"},
						}
						expected["Tags"] = []any{"viewer"}
					}

					AssertItem(t, item, expected)
				}
			},
		},
		{
			name: "QueryAllUsers_NoProjection",
			input: &dynamodb.QueryInput{
				TableName:              aws.String(tableName),
				KeyConditionExpression: aws.String("pk = :type"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":type": &types.AttributeValueMemberS{Value: modelType},
				},
			},
			verify: func(t *testing.T, out *dynamodb.QueryOutput) {
				t.Helper()
				assert.Equal(t, int32(3), out.Count)
				assert.Len(t, out.Items, 3)

				for _, item := range out.Items {
					assert.Contains(t, item, "pk")
					assert.Contains(t, item, "sk")
					assert.Contains(t, item, "DeepData")
					assert.Contains(t, item, "Tags")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			queryOut, queryErr := client.Query(t.Context(), tt.input)
			require.NoError(t, queryErr)
			tt.verify(t, queryOut)
		})
	}
}
