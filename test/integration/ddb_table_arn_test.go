package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_DDB_TableArn drives CreateTable -> DescribeTable -> UpdateTable ->
// DeleteTable through the real SDK and asserts every response carries a non-empty
// TableArn that matches DescribeTable's. CreateTable/UpdateTable/DeleteTable used to
// build the TableDescription without ever setting TableArn, even though the ARN was
// already computed and stored on the table -- a wire bug invisible to unit tests
// because they marshal through the same Go structs the handler builds.
func TestIntegration_DDB_TableArn(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name string
	}{
		{name: "arn set on every table lifecycle op"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createDynamoDBClient(t)

			tableName := "arn-test-" + uuid.NewString()

			createOut, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
				TableName: aws.String(tableName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			require.NoError(t, err, "CreateTable should succeed")
			require.NotNil(t, createOut.TableDescription)
			createArn := aws.ToString(createOut.TableDescription.TableArn)
			assert.NotEmpty(t, createArn, "CreateTable must return a TableArn")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteTable(cleanupCtx, &dynamodb.DeleteTableInput{
					TableName: aws.String(tableName),
				})
			})

			descOut, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
				TableName: aws.String(tableName),
			})
			require.NoError(t, err, "DescribeTable should succeed")
			require.NotNil(t, descOut.Table)
			describeArn := aws.ToString(descOut.Table.TableArn)
			assert.NotEmpty(t, describeArn, "DescribeTable must return a TableArn")
			assert.Equal(t, describeArn, createArn, "CreateTable's TableArn must match DescribeTable's")

			updateOut, err := client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
				TableName:                 aws.String(tableName),
				DeletionProtectionEnabled: aws.Bool(true),
			})
			require.NoError(t, err, "UpdateTable should succeed")
			require.NotNil(t, updateOut.TableDescription)
			updateArn := aws.ToString(updateOut.TableDescription.TableArn)
			assert.NotEmpty(t, updateArn, "UpdateTable must return a TableArn")
			assert.Equal(t, describeArn, updateArn, "UpdateTable's TableArn must match DescribeTable's")

			// DeletionProtectionEnabled must be off before DeleteTable succeeds.
			_, err = client.UpdateTable(ctx, &dynamodb.UpdateTableInput{
				TableName:                 aws.String(tableName),
				DeletionProtectionEnabled: aws.Bool(false),
			})
			require.NoError(t, err, "UpdateTable (disable deletion protection) should succeed")

			deleteOut, err := client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
				TableName: aws.String(tableName),
			})
			require.NoError(t, err, "DeleteTable should succeed")
			require.NotNil(t, deleteOut.TableDescription)
			deleteArn := aws.ToString(deleteOut.TableDescription.TableArn)
			assert.NotEmpty(t, deleteArn, "DeleteTable must return a TableArn")
			assert.Equal(t, describeArn, deleteArn, "DeleteTable's TableArn must match DescribeTable's")
		})
	}
}
