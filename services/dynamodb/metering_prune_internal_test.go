package dynamodb

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableCleanerPrunesCapacityCounters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		deleted    bool
		wantTables int
	}{
		{name: "deleted table counter released", deleted: true, wantTables: 0},
		{name: "live table counter kept", deleted: false, wantTables: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := NewInMemoryDB()
			db.EnableCapacityMetering()

			_, err := db.CreateTable(t.Context(), &ddbsdk.CreateTableInput{
				TableName:   aws.String("t"),
				BillingMode: types.BillingModePayPerRequest,
				KeySchema:   []types.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash}},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
				},
			})
			require.NoError(t, err)

			_, err = db.PutItem(t.Context(), &ddbsdk.PutItemInput{
				TableName: aws.String("t"),
				Item:      map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "a"}},
			})
			require.NoError(t, err)
			require.Len(t, *db.meterCounters.Load(), 1)

			if tc.deleted {
				_, err = db.DeleteTable(t.Context(), &ddbsdk.DeleteTableInput{TableName: aws.String("t")})
				require.NoError(t, err)
			}

			h := NewHandler(db).WithJanitor(Settings{})
			h.DrainDeletedTables(t.Context())

			assert.Len(t, *db.meterCounters.Load(), tc.wantTables)
		})
	}
}
