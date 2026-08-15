package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestListBackups_BackupSizeBytes_Populated verifies ListBackups' BackupSummary
// carries BackupSizeBytes (dynamodb@v1.63.1 types/types.go:511, a real,
// always-populated field on the real service). models.BackupSummary had no
// such field at all, so CreateBackup/DescribeBackup showed a real size for a
// backup that ListBackups always reported without one.
func TestListBackups_BackupSizeBytes_Populated(t *testing.T) {
	t.Parallel()

	client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
	ctx := t.Context()

	tableName := "backup-size-table"
	_, err := client.CreateTable(ctx, &dynamodbsdk.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []dynamodbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
		},
		BillingMode: dynamodbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	_, err = client.PutItem(ctx, &dynamodbsdk.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]dynamodbtypes.AttributeValue{
			"pk":      &dynamodbtypes.AttributeValueMemberS{Value: "k1"},
			"payload": &dynamodbtypes.AttributeValueMemberS{Value: "some real content to size"},
		},
	})
	require.NoError(t, err)

	createOut, err := client.CreateBackup(ctx, &dynamodbsdk.CreateBackupInput{
		TableName:  aws.String(tableName),
		BackupName: aws.String("backup1"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.BackupDetails.BackupSizeBytes)
	require.Positive(t, *createOut.BackupDetails.BackupSizeBytes)

	listOut, err := client.ListBackups(ctx, &dynamodbsdk.ListBackupsInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	require.Len(t, listOut.BackupSummaries, 1)
	require.NotNil(t, listOut.BackupSummaries[0].BackupSizeBytes)
	require.Equal(
		t,
		*createOut.BackupDetails.BackupSizeBytes,
		*listOut.BackupSummaries[0].BackupSizeBytes,
	)
}
