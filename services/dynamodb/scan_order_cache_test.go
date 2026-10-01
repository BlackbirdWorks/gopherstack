package dynamodb_test

import (
	"sort"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type scanCacheStep struct {
	op  string
	key string
	val string
}

func scanCacheItem(key, val string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: key},
		"v":  &types.AttributeValueMemberS{Value: val},
	}
}

func scanCacheKey(key string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: key}}
}

func applyScanCacheStep(t *testing.T, db *dynamodb.InMemoryDB, tbl string, s scanCacheStep) {
	t.Helper()

	var err error

	switch s.op {
	case "put":
		_, err = db.PutItem(t.Context(), &sdk.PutItemInput{
			TableName: aws.String(tbl),
			Item:      scanCacheItem(s.key, s.val),
		})
	case "delete":
		_, err = db.DeleteItem(t.Context(), &sdk.DeleteItemInput{
			TableName: aws.String(tbl),
			Key:       scanCacheKey(s.key),
		})
	case "update":
		_, err = db.UpdateItem(t.Context(), &sdk.UpdateItemInput{
			TableName:        aws.String(tbl),
			Key:              scanCacheKey(s.key),
			UpdateExpression: aws.String("SET v = :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":v": &types.AttributeValueMemberS{Value: s.val},
			},
		})
	case "batchput":
		_, err = db.BatchWriteItem(t.Context(), &sdk.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{tbl: {
				{PutRequest: &types.PutRequest{Item: scanCacheItem(s.key, s.val)}},
			}},
		})
	case "batchdelete":
		_, err = db.BatchWriteItem(t.Context(), &sdk.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{tbl: {
				{DeleteRequest: &types.DeleteRequest{Key: scanCacheKey(s.key)}},
			}},
		})
	case "txput":
		_, err = db.TransactWriteItems(t.Context(), &sdk.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{Put: &types.Put{TableName: aws.String(tbl), Item: scanCacheItem(s.key, s.val)}},
			},
		})
	case "txdelete":
		_, err = db.TransactWriteItems(t.Context(), &sdk.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{Delete: &types.Delete{TableName: aws.String(tbl), Key: scanCacheKey(s.key)}},
			},
		})
	default:
		t.Fatalf("unknown op %q", s.op)
	}

	require.NoError(t, err)
}

func scanCachePaged(t *testing.T, db *dynamodb.InMemoryDB, tbl string, limit int32) []string {
	t.Helper()

	var (
		got   = []string{}
		start map[string]types.AttributeValue
	)

	for {
		out, err := db.Scan(t.Context(), &sdk.ScanInput{
			TableName:         aws.String(tbl),
			Limit:             aws.Int32(limit),
			ExclusiveStartKey: start,
		})
		require.NoError(t, err)

		for _, it := range out.Items {
			pk, _ := it["pk"].(*types.AttributeValueMemberS)
			v, _ := it["v"].(*types.AttributeValueMemberS)
			got = append(got, pk.Value+"="+v.Value)
		}

		if len(out.LastEvaluatedKey) == 0 {
			return got
		}

		start = out.LastEvaluatedKey
	}
}

func TestScanOrderCacheMatchesFreshSort(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		steps []scanCacheStep
	}{
		{
			name: "put_and_overwrite",
			steps: []scanCacheStep{
				{"put", "c", "1"}, {"put", "a", "1"}, {"put", "b", "1"},
				{"put", "a", "2"}, {"put", "d", "1"}, {"put", "b", "3"},
			},
		},
		{
			name: "deletes_swap_last",
			steps: []scanCacheStep{
				{"put", "a", "1"}, {"put", "b", "1"}, {"put", "c", "1"}, {"put", "d", "1"},
				{"delete", "a", ""}, {"delete", "d", ""}, {"put", "a", "9"}, {"delete", "zz", ""},
			},
		},
		{
			name: "update_in_place",
			steps: []scanCacheStep{
				{"put", "b", "1"}, {"put", "a", "1"}, {"update", "a", "u1"}, {"update", "b", "u2"},
				{"update", "new", "u3"},
			},
		},
		{
			name: "batch_writes",
			steps: []scanCacheStep{
				{"batchput", "m", "1"}, {"batchput", "k", "1"}, {"batchput", "m", "2"},
				{"batchput", "z", "1"}, {"batchdelete", "k", ""}, {"batchdelete", "z", ""},
			},
		},
		{
			name: "transact_writes",
			steps: []scanCacheStep{
				{"txput", "q", "1"}, {"txput", "p", "1"}, {"txput", "q", "2"},
				{"txdelete", "p", ""}, {"put", "r", "1"},
			},
		},
		{
			name: "mixed",
			steps: []scanCacheStep{
				{"put", "e", "1"}, {"batchput", "c", "1"}, {"txput", "a", "1"}, {"update", "e", "2"},
				{"delete", "c", ""}, {"batchdelete", "a", ""}, {"txdelete", "e", ""}, {"put", "f", "1"},
				{"put", "b", "1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			tbl := "T"

			_, err := db.CreateTable(t.Context(), &sdk.CreateTableInput{
				TableName: aws.String(tbl),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				},
			})
			require.NoError(t, err)

			model := map[string]string{}

			for i, s := range tt.steps {
				applyScanCacheStep(t, db, tbl, s)

				switch s.op {
				case "delete", "batchdelete", "txdelete":
					delete(model, s.key)
				default:
					model[s.key] = s.val
				}

				keys := make([]string, 0, len(model))
				for k := range model {
					keys = append(keys, k)
				}

				sort.Strings(keys)

				want := make([]string, 0, len(keys))
				for _, k := range keys {
					want = append(want, k+"="+model[k])
				}

				for _, limit := range []int32{1, 2, 100} {
					assert.Equal(t, want, scanCachePaged(t, db, tbl, limit),
						"step %d (%s %s) limit %d", i, s.op, s.key, limit)
				}
			}
		})
	}
}
