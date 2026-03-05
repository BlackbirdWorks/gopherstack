package dynamodb_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func TestJanitor_TTLSweep(t *testing.T) {
	t.Parallel()

	now := time.Now().Unix()

	tests := []struct {
		name          string
		ttlAttr       string
		items         []map[string]types.AttributeValue
		wantCount     int
		wantEvictions int
	}{
		{
			name:    "EvictExpiredItems",
			ttlAttr: "expires",
			items: []map[string]types.AttributeValue{
				{
					"pk":      &types.AttributeValueMemberS{Value: "past"},
					"expires": &types.AttributeValueMemberN{Value: "-100"},
				},
				{
					"pk":      &types.AttributeValueMemberS{Value: "future"},
					"expires": &types.AttributeValueMemberN{Value: "10000000000"},
				},
			},
			wantCount:     1,
			wantEvictions: 1,
		},
		{
			name:    "NoTTLenabled",
			ttlAttr: "",
			items: []map[string]types.AttributeValue{
				{
					"pk":      &types.AttributeValueMemberS{Value: "item1"},
					"expires": &types.AttributeValueMemberN{Value: "1"},
				},
			},
			wantCount:     1,
			wantEvictions: 0,
		},
		{
			name:    "MissingAttribute",
			ttlAttr: "expires",
			items: []map[string]types.AttributeValue{
				{
					"pk": &types.AttributeValueMemberS{Value: "no-ttl"},
				},
			},
			wantCount:     1,
			wantEvictions: 0,
		},
		{
			name:    "InvalidAttributeType",
			ttlAttr: "expires",
			items: []map[string]types.AttributeValue{
				{
					"pk":      &types.AttributeValueMemberS{Value: "bad-type"},
					"expires": &types.AttributeValueMemberS{Value: "not-a-number"},
				},
			},
			wantCount:     1,
			wantEvictions: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			ctx := context.Background()

			// Create table
			tableName := "Table_" + tt.name
			_, err := db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
				TableName: aws.String(tableName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
			})
			require.NoError(t, err)

			// Enable TTL if specified
			if tt.ttlAttr != "" {
				_, err = db.UpdateTimeToLive(ctx, &dynamodb_sdk.UpdateTimeToLiveInput{
					TableName: aws.String(tableName),
					TimeToLiveSpecification: &types.TimeToLiveSpecification{
						AttributeName: aws.String(tt.ttlAttr),
						Enabled:       aws.Bool(true),
					},
				})
				require.NoError(t, err)
			}

			// Add items
			for _, item := range tt.items {
				if tt.ttlAttr != "" {
					if v, ok := item[tt.ttlAttr].(*types.AttributeValueMemberN); ok {
						if v.Value == "-100" {
							v.Value = strconv.FormatInt(now-100, 10)
						}
					}
				}

				_, err = db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
					TableName: aws.String(tableName),
					Item:      item,
				})
				require.NoError(t, err)
			}

			// Trigger sweep via exported helper
			j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})
			j.SweepTTL(ctx)

			// Verify results
			scan, err := db.Scan(ctx, &dynamodb_sdk.ScanInput{
				TableName: aws.String(tableName),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantCount, int(scan.Count), "Item count mismatch for %s", tt.name)
		})
	}
}
