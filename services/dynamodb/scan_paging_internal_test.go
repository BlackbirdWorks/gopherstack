package dynamodb

import (
	"maps"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type pagingSchema struct {
	name     string
	keyType  types.ScalarAttributeType
	rangeKey bool
}

func pagingKey(schema pagingSchema, rng *rand.Rand) map[string]types.AttributeValue {
	val := func() types.AttributeValue {
		if schema.keyType == types.ScalarAttributeTypeN {
			return &types.AttributeValueMemberN{Value: strconv.Itoa(rng.IntN(40)-10) + []string{"", ".5"}[rng.IntN(2)]}
		}

		return &types.AttributeValueMemberS{Value: "k" + strconv.Itoa(rng.IntN(40))}
	}

	key := map[string]types.AttributeValue{"pk": val()}
	if schema.rangeKey {
		key["sk"] = val()
	}

	return key
}

func newPagingTable(t *testing.T, db *InMemoryDB, name string, schema pagingSchema, ttl bool) {
	t.Helper()

	ks := []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}}
	ad := []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: schema.keyType}}

	if schema.rangeKey {
		ks = append(ks, types.KeySchemaElement{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange})
		ad = append(ad, types.AttributeDefinition{AttributeName: aws.String("sk"), AttributeType: schema.keyType})
	}

	_, err := db.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName: aws.String(name), KeySchema: ks, AttributeDefinitions: ad,
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	if ttl {
		_, err = db.UpdateTimeToLive(t.Context(), &sdk.UpdateTimeToLiveInput{
			TableName: aws.String(name),
			TimeToLiveSpecification: &types.TimeToLiveSpecification{
				AttributeName: aws.String("ttl"), Enabled: aws.Bool(true),
			},
		})
		require.NoError(t, err)
	}
}

func applyPagingMutation(t *testing.T, db *InMemoryDB, tables []string, schema pagingSchema, rng *rand.Rand) {
	t.Helper()

	key := pagingKey(schema, rng)
	payload := strconv.Itoa(rng.IntN(1000))
	del := rng.IntN(4) == 0

	for _, tbl := range tables {
		var err error

		switch {
		case del:
			_, err = db.DeleteItem(t.Context(), &sdk.DeleteItemInput{TableName: aws.String(tbl), Key: key})
		default:
			item := map[string]types.AttributeValue{"v": &types.AttributeValueMemberS{Value: payload}}
			maps.Copy(item, key)

			_, err = db.PutItem(t.Context(), &sdk.PutItemInput{TableName: aws.String(tbl), Item: item})
		}

		require.NoError(t, err)
	}
}

func scanAllPages(t *testing.T, db *InMemoryDB, tbl string, limit int32) []*sdk.ScanOutput {
	t.Helper()

	var (
		pages []*sdk.ScanOutput
		start map[string]types.AttributeValue
	)

	for range 200 {
		out, err := db.Scan(t.Context(), &sdk.ScanInput{
			TableName: aws.String(tbl), Limit: aws.Int32(limit), ExclusiveStartKey: start,
		})
		require.NoError(t, err)

		pages = append(pages, out)

		if len(out.LastEvaluatedKey) == 0 {
			break
		}

		start = out.LastEvaluatedKey
	}

	return pages
}

// TestScanPaging_FastPathMatchesLinearPath compares paged scans on a plain table (cached order,
// binary-searched start key) against an identical TTL-enabled twin that takes the linear path.
func TestScanPaging_FastPathMatchesLinearPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		schema pagingSchema
		limit  int32
	}{
		{schema: pagingSchema{name: "string hash", keyType: types.ScalarAttributeTypeS}, limit: 3},
		{
			schema: pagingSchema{name: "string hash range", keyType: types.ScalarAttributeTypeS, rangeKey: true},
			limit:  4,
		},
		{schema: pagingSchema{name: "number hash", keyType: types.ScalarAttributeTypeN}, limit: 2},
		{
			schema: pagingSchema{name: "number hash range", keyType: types.ScalarAttributeTypeN, rangeKey: true},
			limit:  5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.schema.name, func(t *testing.T) {
			t.Parallel()

			db := NewInMemoryDB()
			db.SetDefaultRegion("us-east-1")
			newPagingTable(t, db, "plain", tt.schema, false)
			newPagingTable(t, db, "twin", tt.schema, true)

			rng := rand.New(rand.NewPCG(9, 10))

			for round := range 12 {
				for range 25 {
					applyPagingMutation(t, db, []string{"plain", "twin"}, tt.schema, rng)
				}

				want := scanAllPages(t, db, "twin", tt.limit)
				got := scanAllPages(t, db, "plain", tt.limit)

				require.Len(t, got, len(want), "round %d", round)

				for i := range want {
					assert.Equal(t, want[i].Items, got[i].Items, "round %d page %d", round, i)
					assert.Equal(t, want[i].LastEvaluatedKey, got[i].LastEvaluatedKey, "round %d page %d", round, i)
					assert.Equal(t, want[i].Count, got[i].Count)
					assert.Equal(t, want[i].ScannedCount, got[i].ScannedCount)
				}
			}
		})
	}
}

func TestScanPaging_StartKeyOfDeletedItemRestartsLikeLinearPath(t *testing.T) {
	t.Parallel()

	schema := pagingSchema{name: "s", keyType: types.ScalarAttributeTypeS}
	db := NewInMemoryDB()
	db.SetDefaultRegion("us-east-1")
	newPagingTable(t, db, "plain", schema, false)
	newPagingTable(t, db, "twin", schema, true)

	for _, tbl := range []string{"plain", "twin"} {
		for i := range 6 {
			_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
				TableName: aws.String(tbl),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "k" + strconv.Itoa(i)},
				},
			})
			require.NoError(t, err)
		}
	}

	ghost := map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "k25"}}
	want, err := db.Scan(t.Context(), &sdk.ScanInput{TableName: aws.String("twin"), ExclusiveStartKey: ghost})
	require.NoError(t, err)

	got, err := db.Scan(t.Context(), &sdk.ScanInput{TableName: aws.String("plain"), ExclusiveStartKey: ghost})
	require.NoError(t, err)

	assert.Equal(t, want.Items, got.Items)
	assert.Equal(t, want.Count, got.Count)
}

func TestScanOrderCache_SizesMatchItems(t *testing.T) {
	t.Parallel()

	schema := pagingSchema{name: "s", keyType: types.ScalarAttributeTypeS, rangeKey: true}
	db := NewInMemoryDB()
	db.SetDefaultRegion("us-east-1")
	newPagingTable(t, db, "plain", schema, false)

	rng := rand.New(rand.NewPCG(3, 4))

	for range 120 {
		applyPagingMutation(t, db, []string{"plain"}, schema, rng)
	}

	_, err := db.Scan(t.Context(), &sdk.ScanInput{TableName: aws.String("plain")})
	require.NoError(t, err)

	table, err := db.getTable(t.Context(), "plain")
	require.NoError(t, err)

	cache := table.scanOrder.Load()
	require.NotNil(t, cache)
	require.Len(t, cache.sizes, len(cache.items))

	for i, item := range cache.items {
		want, _ := CalculateItemSize(item)
		assert.Equal(t, want, cache.sizes[i])
	}
}
