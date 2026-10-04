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

func assertZeroThroughput(t *testing.T, label string, pt *dynamodbtypes.ProvisionedThroughputDescription) {
	t.Helper()
	require.NotNil(t, pt, label)
	assert.Equal(t, int64(0), aws.ToInt64(pt.ReadCapacityUnits), label)
	assert.Equal(t, int64(0), aws.ToInt64(pt.WriteCapacityUnits), label)
	require.NotNil(t, pt.NumberOfDecreasesToday, label)
	assert.Equal(t, int64(0), *pt.NumberOfDecreasesToday, label)
}

func TestOnDemandTable_ReportsZeroProvisionedThroughput(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		billing    dynamodbtypes.BillingMode
		switchToOD bool
	}{
		{name: "created on demand", billing: dynamodbtypes.BillingModePayPerRequest},
		{name: "switched to on demand", billing: dynamodbtypes.BillingModeProvisioned, switchToOD: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestDynamoDBClient(t, dynamodb.NewHandler(dynamodb.NewInMemoryDB()))
			ctx := t.Context()
			in := &dynamodbsdk.CreateTableInput{
				TableName: aws.String("od-table"),
				KeySchema: []dynamodbtypes.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: dynamodbtypes.KeyTypeHash},
				},
				AttributeDefinitions: []dynamodbtypes.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
					{AttributeName: aws.String("gk"), AttributeType: dynamodbtypes.ScalarAttributeTypeS},
				},
				BillingMode: tc.billing,
				GlobalSecondaryIndexes: []dynamodbtypes.GlobalSecondaryIndex{{
					IndexName: aws.String("gsi"),
					KeySchema: []dynamodbtypes.KeySchemaElement{
						{AttributeName: aws.String("gk"), KeyType: dynamodbtypes.KeyTypeHash},
					},
					Projection: &dynamodbtypes.Projection{ProjectionType: dynamodbtypes.ProjectionTypeAll},
				}},
			}

			if tc.billing == dynamodbtypes.BillingModeProvisioned {
				in.ProvisionedThroughput = &dynamodbtypes.ProvisionedThroughput{
					ReadCapacityUnits: aws.Int64(7), WriteCapacityUnits: aws.Int64(9),
				}
				in.GlobalSecondaryIndexes[0].ProvisionedThroughput = in.ProvisionedThroughput
			}

			createOut, err := client.CreateTable(ctx, in)
			require.NoError(t, err)

			if !tc.switchToOD {
				assertZeroThroughput(t, "create table", createOut.TableDescription.ProvisionedThroughput)
				assertZeroThroughput(t, "create gsi",
					createOut.TableDescription.GlobalSecondaryIndexes[0].ProvisionedThroughput)
			} else {
				assert.Equal(t, int64(7),
					aws.ToInt64(createOut.TableDescription.ProvisionedThroughput.ReadCapacityUnits))
				assert.Equal(t, int64(9), aws.ToInt64(
					createOut.TableDescription.GlobalSecondaryIndexes[0].ProvisionedThroughput.WriteCapacityUnits))

				updOut, uerr := client.UpdateTable(ctx, &dynamodbsdk.UpdateTableInput{
					TableName: aws.String("od-table"), BillingMode: dynamodbtypes.BillingModePayPerRequest,
				})
				require.NoError(t, uerr)
				assertZeroThroughput(t, "update table", updOut.TableDescription.ProvisionedThroughput)
				assertZeroThroughput(t, "update gsi",
					updOut.TableDescription.GlobalSecondaryIndexes[0].ProvisionedThroughput)
			}

			descOut, err := client.DescribeTable(ctx,
				&dynamodbsdk.DescribeTableInput{TableName: aws.String("od-table")})
			require.NoError(t, err)
			assertZeroThroughput(t, "describe table", descOut.Table.ProvisionedThroughput)
			assertZeroThroughput(t, "describe gsi", descOut.Table.GlobalSecondaryIndexes[0].ProvisionedThroughput)

			delOut, err := client.DeleteTable(ctx, &dynamodbsdk.DeleteTableInput{TableName: aws.String("od-table")})
			require.NoError(t, err)
			assertZeroThroughput(t, "delete gsi",
				delOut.TableDescription.GlobalSecondaryIndexes[0].ProvisionedThroughput)
		})
	}
}
