package integration_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ComplexModel represents a deep data model with mixed types
type ComplexModel struct {
	ID        string
	Type      string
	OrgID     string
	Metadata  ModelMetadata
	Settings  UserSettings
	CreatedAt time.Time
	Tags      []string
}

type ModelMetadata struct {
	Version   int
	Source    string
	Flags     map[string]bool
	ExtraData []byte
}

type UserSettings struct {
	Theme         string
	Notifications NotificationPrefs
}

type NotificationPrefs struct {
	Email   bool
	SMS     bool
	Push    bool
	Channel string
}

func TestDDB_ComplexDataModel(t *testing.T) {
	t.Parallel()
	client := createDynamoDBClient(t)
	ctx := t.Context()
	tableName := "ComplexModelTest-" + uuid.NewString()

	// 1. Create Table
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
	assert.Equal(t, int64(0), aws.ToInt64(out.TableDescription.ItemCount))

	t.Cleanup(func() {
		_, _ = client.DeleteTable(t.Context(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
		require.NoError(t, err)
	})

	// Wait for table creation (simulated delay for eventual consistency in real DynamoDB, instant in Gopherstack usually)
	time.Sleep(100 * time.Millisecond)

	// 2. Prepare Data
	orgID := "org-123"
	modelType := "USER_PROFILE"

	users := []ComplexModel{
		{
			ID:    "user-1",
			Type:  modelType,
			OrgID: orgID,
			Metadata: ModelMetadata{
				Version: 1,
				Source:  "web",
				Flags:   map[string]bool{"active": true, "verified": true},
			},
			Settings: UserSettings{
				Theme: "dark",
				Notifications: NotificationPrefs{
					Email: true,
					SMS:   false,
				},
			},
			CreatedAt: time.Now(),
			Tags:      []string{"admin", "editor"},
		},
		{
			ID:    "user-2",
			Type:  modelType,
			OrgID: orgID,
			Metadata: ModelMetadata{
				Version: 1,
				Source:  "mobile",
				Flags:   map[string]bool{"active": true},
			},
			Settings: UserSettings{
				Theme: "light",
				Notifications: NotificationPrefs{
					Email: false,
					SMS:   true,
				},
			},
			CreatedAt: time.Now(),
			Tags:      []string{"viewer"},
		},
		{
			ID:    "user-3",
			Type:  modelType,
			OrgID: "org-999", // Different Org
			Metadata: ModelMetadata{
				Version: 2,
				Source:  "api",
			},
			Settings: UserSettings{Theme: "auto"},
		},
	}

	// // 3. Insert Data
	// // PK: MODEL_TYPE
	// // SK: ORG#<OrgID>#USER#<UserID>
	for _, user := range users {
		sk := fmt.Sprintf("ORG#%s#USER#%s", user.OrgID, user.ID)

		item := map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: user.Type},
			"sk": &types.AttributeValueMemberS{Value: sk},
			"DeepData": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"ID":  &types.AttributeValueMemberS{Value: user.ID},
				"Org": &types.AttributeValueMemberS{Value: user.OrgID},
				"Meta": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
					"Source": &types.AttributeValueMemberS{Value: user.Metadata.Source},
					"Ver":    &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", user.Metadata.Version)},
				}},
				"Config": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
					"Theme": &types.AttributeValueMemberS{Value: user.Settings.Theme},
				}},
			}},
			"Tags": &types.AttributeValueMemberL{Value: []types.AttributeValue{}}, // Simplified for test
		}

		if len(user.Tags) > 0 {
			listMembers := make([]types.AttributeValue, len(user.Tags))
			for i, tag := range user.Tags {
				listMembers[i] = &types.AttributeValueMemberS{Value: tag}
			}
			item["Tags"] = &types.AttributeValueMemberL{Value: listMembers}
		}

		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		})
		require.NoError(t, err)
	}

	// // 4. Query with BeginsWith
	// // Get all users in org-123
	queryOut, err := client.Query(ctx, &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("pk = :type AND begins_with(sk, :orgPrefix)"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":type":      &types.AttributeValueMemberS{Value: modelType},
			":orgPrefix": &types.AttributeValueMemberS{Value: "ORG#" + orgID},
		},
		// Projection to fetch specific deep fields
		ProjectionExpression: aws.String("sk, DeepData.Config.Theme, DeepData.Meta.Source, Tags"),
	})
	require.NoError(t, err)

	// 5. Verify Results
	assert.Equal(t, int32(2), queryOut.Count) // Should find user-1 and user-2
	assert.Len(t, queryOut.Items, 2)

	for _, item := range queryOut.Items {
		// Verify Projection
		assert.Contains(t, item, "sk")
		assert.Contains(t, item, "DeepData")
		assert.Contains(t, item, "Tags")
		assert.NotContains(t, item, "pk") // Not projected explicitly (though Keys usually are, but Projection logic handles it)

		// Verify Deep Data Projection
		deepData := item["DeepData"].(*types.AttributeValueMemberM).Value
		assert.Contains(t, deepData, "Config")
		assert.Contains(t, deepData, "Meta")
		assert.NotContains(t, deepData, "ID") // Not projected

		config := deepData["Config"].(*types.AttributeValueMemberM).Value
		assert.Contains(t, config, "Theme")

		meta := deepData["Meta"].(*types.AttributeValueMemberM).Value
		assert.Contains(t, meta, "Source")
		assert.NotContains(t, meta, "Ver") // Not projected
	}
}
