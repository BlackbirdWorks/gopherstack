package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestWireDynamoDBFaultStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wire         bool
		wantConflict bool
	}{
		{name: "wired store drives conflicts", wire: true, wantConflict: true},
		{name: "unwired store leaves behaviour unchanged"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			db.SetDefaultRegion("us-east-1")

			store := chaos.NewFaultStore()
			store.SetRules([]chaos.FaultRule{{
				Effect: chaos.EffectDynamoDBTransactionConflict,
				Target: &chaos.EffectTarget{Table: "tbl"},
			}})

			if tt.wire {
				wireDynamoDBFaultStore([]service.Registerable{dynamodb.NewHandler(db)}, store)
			}

			_, err := db.CreateTable(t.Context(), &sdk.CreateTableInput{
				TableName: aws.String("tbl"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			require.NoError(t, err)

			_, err = db.PutItem(t.Context(), &sdk.PutItemInput{
				TableName: aws.String("tbl"),
				Item:      map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
			})

			if tt.wantConflict {
				require.ErrorContains(t, err, "TransactionConflictException")

				return
			}

			require.NoError(t, err)
		})
	}
}
