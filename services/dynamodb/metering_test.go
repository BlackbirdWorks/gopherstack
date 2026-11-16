package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestDrainCapacityUsage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		billing   types.BillingMode
		enable    bool
		wantTable bool
	}{
		{name: "on demand", billing: types.BillingModePayPerRequest, enable: true, wantTable: true},
		{name: "provisioned", billing: types.BillingModeProvisioned, enable: true},
		{name: "metering off", billing: types.BillingModePayPerRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			if tc.enable {
				db.EnableCapacityMetering()
			}

			in := &ddbsdk.CreateTableInput{
				TableName:   aws.String("t"),
				BillingMode: tc.billing,
				KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash}},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
				},
			}
			if tc.billing == types.BillingModeProvisioned {
				in.ProvisionedThroughput = &types.ProvisionedThroughput{
					ReadCapacityUnits: aws.Int64(5), WriteCapacityUnits: aws.Int64(5),
				}
			}

			_, err := db.CreateTable(t.Context(), in)
			require.NoError(t, err)

			_, err = db.PutItem(t.Context(), &ddbsdk.PutItemInput{
				TableName: aws.String("t"),
				Item:      map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "a"}},
			})
			require.NoError(t, err)

			got := db.DrainCapacityUsage()
			if !tc.wantTable {
				assert.Empty(t, got)

				return
			}

			require.Len(t, got, 1)
			assert.InDelta(t, 1, got[0].WriteUnits, 1e-9)
			assert.Contains(t, got[0].TableARN, ":table/t")
			assert.Empty(t, db.DrainCapacityUsage(), "a drain clears the counters")
		})
	}
}
