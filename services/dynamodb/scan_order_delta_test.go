package dynamodb_test

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type deltaSchema struct {
	name     string
	keyType  types.ScalarAttributeType
	rangeKey bool
}

const deltaTable = "T"

type deltaKey struct{ pk, sk int }

func (s deltaSchema) val(n int) types.AttributeValue {
	if s.keyType == types.ScalarAttributeTypeN {
		return &types.AttributeValueMemberN{Value: strconv.Itoa(n - 20)}
	}

	return &types.AttributeValueMemberS{Value: "k" + strconv.Itoa(1000+n)}
}

func (s deltaSchema) key(k deltaKey) map[string]types.AttributeValue {
	out := map[string]types.AttributeValue{"pk": s.val(k.pk)}
	if s.rangeKey {
		out["sk"] = s.val(k.sk)
	}

	return out
}

func (s deltaSchema) item(k deltaKey, v string) map[string]types.AttributeValue {
	out := s.key(k)
	out["v"] = &types.AttributeValueMemberS{Value: v}

	return out
}

func (s deltaSchema) create(t *testing.T, db *dynamodb.InMemoryDB, gsi bool) {
	t.Helper()

	ks := []types.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash}}
	ad := []types.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: s.keyType}}

	if s.rangeKey {
		ks = append(ks, types.KeySchemaElement{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange})
		ad = append(ad, types.AttributeDefinition{AttributeName: aws.String("sk"), AttributeType: s.keyType})
	}

	in := &sdk.CreateTableInput{
		TableName: aws.String(deltaTable), KeySchema: ks, AttributeDefinitions: ad,
		BillingMode: types.BillingModePayPerRequest,
	}

	if gsi {
		in.AttributeDefinitions = append(in.AttributeDefinitions,
			types.AttributeDefinition{AttributeName: aws.String("v"), AttributeType: types.ScalarAttributeTypeS})
		in.GlobalSecondaryIndexes = []types.GlobalSecondaryIndex{{
			IndexName:  aws.String("byv"),
			KeySchema:  []types.KeySchemaElement{{AttributeName: aws.String("v"), KeyType: types.KeyTypeHash}},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}}
	}

	_, err := db.CreateTable(t.Context(), in)
	require.NoError(t, err)
}

func (s deltaSchema) less(a, b deltaKey) int {
	if a.pk != b.pk {
		return a.pk - b.pk
	}

	if s.rangeKey {
		return a.sk - b.sk
	}

	return 0
}

func (s deltaSchema) write(t *testing.T, db *dynamodb.InMemoryDB, op string, k deltaKey, v string) {
	t.Helper()

	var err error

	ctx := t.Context()

	switch op {
	case "put":
		_, err = db.PutItem(ctx, &sdk.PutItemInput{TableName: aws.String(deltaTable), Item: s.item(k, v)})
	case "delete":
		_, err = db.DeleteItem(ctx, &sdk.DeleteItemInput{TableName: aws.String(deltaTable), Key: s.key(k)})
	case "update":
		_, err = db.UpdateItem(ctx, &sdk.UpdateItemInput{
			TableName: aws.String(deltaTable), Key: s.key(k), UpdateExpression: aws.String("SET v = :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{":v": &types.AttributeValueMemberS{Value: v}},
		})
	case "batchput":
		_, err = db.BatchWriteItem(ctx, &sdk.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
			deltaTable: {{PutRequest: &types.PutRequest{Item: s.item(k, v)}}},
		}})
	case "batchdelete":
		_, err = db.BatchWriteItem(ctx, &sdk.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
			deltaTable: {{DeleteRequest: &types.DeleteRequest{Key: s.key(k)}}},
		}})
	case "txput":
		_, err = db.TransactWriteItems(ctx, &sdk.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
			{Put: &types.Put{TableName: aws.String(deltaTable), Item: s.item(k, v)}},
		}})
	case "txdelete":
		_, err = db.TransactWriteItems(ctx, &sdk.TransactWriteItemsInput{TransactItems: []types.TransactWriteItem{
			{Delete: &types.Delete{TableName: aws.String(deltaTable), Key: s.key(k)}},
		}})
	}

	require.NoError(t, err)
}

type deltaModel map[deltaKey]string

func (m deltaModel) apply(op string, k deltaKey, v string) {
	switch op {
	case "delete", "batchdelete", "txdelete":
		delete(m, k)
	default:
		m[k] = v
	}
}

func (s deltaSchema) want(m deltaModel) []string {
	keys := make([]deltaKey, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.SortFunc(keys, s.less)

	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, strconv.Itoa(k.pk)+"/"+strconv.Itoa(k.sk)+"="+m[k])
	}

	return out
}

func (s deltaSchema) decode(item map[string]types.AttributeValue) string {
	num := func(av types.AttributeValue) int {
		if n, ok := av.(*types.AttributeValueMemberN); ok {
			i, _ := strconv.Atoi(n.Value)

			return i + 20
		}

		i, _ := strconv.Atoi(av.(*types.AttributeValueMemberS).Value[1:])

		return i - 1000
	}

	sk := 0
	if s.rangeKey {
		sk = num(item["sk"])
	}

	return strconv.Itoa(num(item["pk"])) + "/" + strconv.Itoa(sk) + "=" + item["v"].(*types.AttributeValueMemberS).Value
}

func (s deltaSchema) scanPaged(t *testing.T, db *dynamodb.InMemoryDB, limit int32) []string {
	t.Helper()

	got := []string{}

	var start map[string]types.AttributeValue

	for range 5000 {
		out, err := db.Scan(t.Context(), &sdk.ScanInput{
			TableName: aws.String(deltaTable), Limit: aws.Int32(limit), ExclusiveStartKey: start,
		})
		require.NoError(t, err)

		for _, it := range out.Items {
			got = append(got, s.decode(it))
		}

		if len(out.LastEvaluatedKey) == 0 {
			return got
		}

		start = out.LastEvaluatedKey
	}

	t.Fatal("scan did not terminate")

	return nil
}

func deltaSchemas() []deltaSchema {
	return []deltaSchema{
		{name: "hash_s", keyType: types.ScalarAttributeTypeS},
		{name: "hash_n", keyType: types.ScalarAttributeTypeN},
		{name: "range_s", keyType: types.ScalarAttributeTypeS, rangeKey: true},
		{name: "range_n", keyType: types.ScalarAttributeTypeN, rangeKey: true},
	}
}

func deltaOps() []string {
	return []string{"put", "delete", "update", "batchput", "batchdelete", "txput", "txdelete"}
}

func (s deltaSchema) randomKey(rng *rand.Rand) deltaKey {
	k := deltaKey{pk: rng.IntN(12)}
	if s.rangeKey {
		k.sk = rng.IntN(6)
	}

	return k
}

func TestScanOrderDelta_WritesBetweenScans(t *testing.T) {
	t.Parallel()

	for _, s := range deltaSchemas() {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			s.create(t, db, false)

			rng := rand.New(rand.NewPCG(7, 9))
			model := deltaModel{}

			for round := range 60 {
				batch := 1 + rng.IntN(8)
				for range batch {
					op := deltaOps()[rng.IntN(len(deltaOps()))]
					k := s.randomKey(rng)
					v := strconv.Itoa(rng.IntN(1000))

					if op == "update" {
						if _, ok := model[k]; !ok {
							op = "put"
						}
					}

					s.write(t, db, op, k, v)
					model.apply(op, k, v)
				}

				want := s.want(model)
				for _, limit := range []int32{1, 4, 1000} {
					require.Equal(t, want, s.scanPaged(t, db, limit), "round %d limit %d", round, limit)
				}
			}
		})
	}
}

func TestScanOrderDelta_LogOverflow(t *testing.T) {
	t.Parallel()

	for _, s := range deltaSchemas() {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			s.create(t, db, false)

			model := deltaModel{}
			s.write(t, db, "put", deltaKey{}, "seed")
			model.apply("put", deltaKey{}, "seed")
			require.Equal(t, s.want(model), s.scanPaged(t, db, 100))

			for i := range 1500 {
				k := deltaKey{pk: i % 53}
				if s.rangeKey {
					k.sk = i % 7
				}

				v := strconv.Itoa(i)
				s.write(t, db, "put", k, v)
				model.apply("put", k, v)
			}

			assert.Equal(t, s.want(model), s.scanPaged(t, db, 100))
		})
	}
}

func TestScanOrderDelta_PaginationAcrossWrites(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		op   string
		rest []string
		key  int
	}{
		{name: "insert_after_cursor", op: "put", key: 5, rest: []string{"5/0=y", "7/0=x", "9/0=x"}},
		{name: "insert_before_cursor", op: "put", key: 0, rest: []string{"7/0=x", "9/0=x"}},
		{name: "delete_after_cursor", op: "delete", key: 7, rest: []string{"9/0=x"}},
		{name: "delete_before_cursor", op: "delete", key: 1, rest: []string{"7/0=x", "9/0=x"}},
		{name: "overwrite_after_cursor", op: "put", key: 7, rest: []string{"7/0=y", "9/0=x"}},
	}

	s := deltaSchema{name: "hash_n", keyType: types.ScalarAttributeTypeN}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			s.create(t, db, false)

			for _, n := range []int{1, 2, 3, 7, 9} {
				s.write(t, db, "put", deltaKey{pk: n}, "x")
			}

			first, err := db.Scan(t.Context(), &sdk.ScanInput{TableName: aws.String(deltaTable), Limit: aws.Int32(3)})
			require.NoError(t, err)
			require.Len(t, first.Items, 3)
			require.NotEmpty(t, first.LastEvaluatedKey)

			s.write(t, db, tt.op, deltaKey{pk: tt.key}, "y")

			rest, err := db.Scan(t.Context(), &sdk.ScanInput{
				TableName: aws.String(deltaTable), ExclusiveStartKey: first.LastEvaluatedKey,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(rest.Items))
			for _, it := range rest.Items {
				got = append(got, s.decode(it))
			}

			assert.Equal(t, tt.rest, got)
		})
	}
}

func TestScanOrderDelta_GSIScanAfterWrites(t *testing.T) {
	t.Parallel()

	s := deltaSchema{name: "hash_s", keyType: types.ScalarAttributeTypeS}
	db := dynamodb.NewInMemoryDB()
	s.create(t, db, true)

	scanIdx := func() []string {
		out, err := db.Scan(t.Context(), &sdk.ScanInput{
			TableName: aws.String(deltaTable), IndexName: aws.String("byv"),
		})
		require.NoError(t, err)

		got := make([]string, 0, len(out.Items))
		for _, it := range out.Items {
			got = append(got, s.decode(it))
		}

		slices.Sort(got)

		return got
	}

	model := deltaModel{}
	steps := []struct {
		op string
		v  string
		k  int
	}{
		{"put", "a", 3}, {"put", "b", 1}, {"put", "c", 2}, {"update", "z", 1}, {"delete", "", 3},
		{"batchput", "d", 4}, {"txput", "e", 5}, {"batchdelete", "", 2}, {"txdelete", "", 5}, {"put", "q", 4},
	}

	for i, st := range steps {
		k := deltaKey{pk: st.k}
		s.write(t, db, st.op, k, st.v)
		model.apply(st.op, k, st.v)

		want := s.want(model)
		slices.Sort(want)
		assert.Equal(t, want, scanIdx(), "step %d", i)
		assert.Equal(t, s.want(model), s.scanPaged(t, db, 2), "base step %d", i)
	}
}

func TestScanOrderDelta_SegmentedScansAfterWrites(t *testing.T) {
	t.Parallel()

	for _, s := range deltaSchemas() {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			s.create(t, db, false)

			rng := rand.New(rand.NewPCG(11, 13))
			model := deltaModel{}

			for round := range 25 {
				for range 1 + rng.IntN(6) {
					op := deltaOps()[rng.IntN(len(deltaOps()))]
					k := s.randomKey(rng)
					v := strconv.Itoa(rng.IntN(1000))

					if op == "update" {
						if _, ok := model[k]; !ok {
							op = "put"
						}
					}

					s.write(t, db, op, k, v)
					model.apply(op, k, v)
				}

				var union []string

				for seg := range int32(3) {
					out, err := db.Scan(t.Context(), &sdk.ScanInput{
						TableName: aws.String(deltaTable), Segment: aws.Int32(seg), TotalSegments: aws.Int32(3),
					})
					require.NoError(t, err)

					for _, it := range out.Items {
						union = append(union, s.decode(it))
					}
				}

				want := s.want(model)
				slices.Sort(want)
				slices.Sort(union)
				require.Equal(t, want, union, "round %d", round)
			}
		})
	}
}

func TestScanOrderDelta_ConcurrentScansAndWrites(t *testing.T) {
	t.Parallel()

	for _, s := range deltaSchemas() {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			s.create(t, db, false)

			var wg sync.WaitGroup

			for w := range 4 {
				wg.Go(func() {
					for i := range 150 {
						k := deltaKey{pk: w*100 + i%50}
						if s.rangeKey {
							k.sk = i % 3
						}

						op := deltaOps()[(i+w)%len(deltaOps())]
						if op == "update" {
							op = "put"
						}

						s.write(t, db, op, k, strconv.Itoa(i))
					}
				})
			}

			for range 3 {
				wg.Go(func() {
					for range 100 {
						out, err := db.Scan(t.Context(), &sdk.ScanInput{TableName: aws.String(deltaTable)})
						if err != nil {
							t.Errorf("scan: %v", err)

							return
						}

						got := make([]string, 0, len(out.Items))
						for _, it := range out.Items {
							got = append(got, s.decode(it))
						}

						assert.True(t, slices.IsSortedFunc(got, compareDecoded))
					}
				})
			}

			wg.Wait()

			final := s.scanPaged(t, db, 1000)
			assert.True(t, slices.IsSortedFunc(final, compareDecoded))
		})
	}
}

func compareDecoded(a, b string) int {
	ak, bk := decodedKey(a), decodedKey(b)
	if ak[0] != bk[0] {
		return ak[0] - bk[0]
	}

	return ak[1] - bk[1]
}

func decodedKey(s string) [2]int {
	var pk, sk int

	slash := slices.Index([]byte(s), '/')
	eq := slices.Index([]byte(s), '=')
	pk, _ = strconv.Atoi(s[:slash])
	sk, _ = strconv.Atoi(s[slash+1 : eq])

	return [2]int{pk, sk}
}
