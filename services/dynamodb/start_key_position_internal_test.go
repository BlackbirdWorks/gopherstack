package dynamodb

import (
	"fmt"
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func skey(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }

func bk(pk, sk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"pk": skey(pk), "sk": skey(sk)}
}

func gk(gs, sk string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{"gk": skey("ga"), "gs": skey(gs), "pk": skey("a"), "sk": skey(sk)}
}

func startKeyTable(t *testing.T, ttl bool) *InMemoryDB {
	t.Helper()

	db := NewInMemoryDB()
	db.SetDefaultRegion("us-east-1")

	str := func(n string) types.AttributeDefinition {
		return types.AttributeDefinition{AttributeName: aws.String(n), AttributeType: types.ScalarAttributeTypeS}
	}

	_, err := db.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName: aws.String("t"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{str("pk"), str("sk"), str("gk"), str("gs")},
		BillingMode:          types.BillingModePayPerRequest,
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{{
			IndexName: aws.String("gsi"),
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("gk"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("gs"), KeyType: types.KeyTypeRange},
			},
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		}},
	})
	require.NoError(t, err)

	if ttl {
		_, err = db.UpdateTimeToLive(t.Context(), &sdk.UpdateTimeToLiveInput{
			TableName: aws.String("t"),
			TimeToLiveSpecification: &types.TimeToLiveSpecification{
				AttributeName: aws.String("ttl"), Enabled: aws.Bool(true),
			},
		})
		require.NoError(t, err)
	}

	for _, id := range []string{"a1", "a3", "a5", "b1", "b3"} {
		_, err = db.PutItem(t.Context(), &sdk.PutItemInput{
			TableName: aws.String("t"),
			Item: map[string]types.AttributeValue{
				"pk": skey(id[:1]), "sk": skey("s" + id[1:]), "gk": skey("g" + id[:1]), "gs": skey("x" + id[1:]),
			},
		})
		require.NoError(t, err)
	}

	return db
}

func ids(items []map[string]types.AttributeValue) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, it["pk"].(*types.AttributeValueMemberS).Value+it["sk"].(*types.AttributeValueMemberS).Value)
	}

	return out
}

func TestExclusiveStartKey_IsPositionNotItemReference(t *testing.T) {
	t.Parallel()

	all := []string{"as1", "as3", "as5", "bs1", "bs3"}
	tests := []struct {
		start map[string]types.AttributeValue
		name  string
		want  []string
	}{
		{bk("a", "s2"), "deleted middle", all[1:]},
		{bk("a", "s9"), "deleted end of partition", all[3:]},
		{bk("c", "s1"), "deleted at end", nil},
		{bk("0", "s0"), "deleted before first", all},
		{bk("a", "s3"), "existing", all[2:]},
	}

	for _, ttl := range []bool{false, true} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("scan ttl=%v %s", ttl, tt.name), func(t *testing.T) {
				t.Parallel()

				db := startKeyTable(t, ttl)
				out, err := db.Scan(t.Context(), &sdk.ScanInput{
					TableName: aws.String("t"), ExclusiveStartKey: tt.start,
				})
				require.NoError(t, err)
				assert.Len(t, out.Items, len(tt.want))
				if len(tt.want) > 0 {
					assert.Equal(t, tt.want, ids(out.Items))
				}
			})
		}
	}
}

func TestExclusiveStartKey_HashOnlyScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		start string
		want  []string
	}{
		{"deleted middle", "k4", []string{"k5", "k7"}},
		{"deleted at end", "k9", nil},
		{"deleted before first", "k0", []string{"k1", "k3", "k5", "k7"}},
		{"existing", "k3", []string{"k5", "k7"}},
	}

	for _, ttl := range []bool{false, true} {
		for _, tt := range tests {
			t.Run(fmt.Sprintf("ttl=%v %s", ttl, tt.name), func(t *testing.T) {
				t.Parallel()

				db := NewInMemoryDB()
				db.SetDefaultRegion("us-east-1")
				newPagingTable(t, db, "h", pagingSchema{name: "h", keyType: types.ScalarAttributeTypeS}, ttl)

				for _, k := range []string{"k1", "k3", "k5", "k7"} {
					_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
						TableName: aws.String("h"), Item: map[string]types.AttributeValue{"pk": skey(k)},
					})
					require.NoError(t, err)
				}

				out, err := db.Scan(t.Context(), &sdk.ScanInput{
					TableName:         aws.String("h"),
					ExclusiveStartKey: map[string]types.AttributeValue{"pk": skey(tt.start)},
				})
				require.NoError(t, err)

				got := make([]string, 0, len(out.Items))
				for _, it := range out.Items {
					got = append(got, it["pk"].(*types.AttributeValueMemberS).Value)
				}

				assert.Equal(t, tt.want, append([]string(nil), got...))
				assert.Len(t, got, len(tt.want))
			})
		}
	}
}

func TestExclusiveStartKey_SegmentedScan(t *testing.T) {
	t.Parallel()

	const segments = 3

	db := startKeyTable(t, false)
	start := bk("a", "s2")

	got := make([]string, 0, 5)
	want := make([]string, 0, 5)

	for seg := range int32(segments) {
		full, err := db.Scan(t.Context(), &sdk.ScanInput{
			TableName: aws.String("t"), Segment: aws.Int32(seg), TotalSegments: aws.Int32(segments),
		})
		require.NoError(t, err)

		out, err := db.Scan(t.Context(), &sdk.ScanInput{
			TableName: aws.String("t"), Segment: aws.Int32(seg), TotalSegments: aws.Int32(segments),
			ExclusiveStartKey: start,
		})
		require.NoError(t, err)

		for _, id := range ids(full.Items) {
			if id > "as2" {
				want = append(want, id)
			}
		}

		got = append(got, ids(out.Items)...)
	}

	slices.Sort(got)
	slices.Sort(want)
	assert.Equal(t, []string{"as3", "as5", "bs1", "bs3"}, got)
	assert.Equal(t, want, got)
}

func TestExclusiveStartKey_GSIScanAndQuery(t *testing.T) {
	t.Parallel()

	all := []string{"as1", "as3", "as5", "bs1", "bs3"}
	pastEnd := map[string]types.AttributeValue{"gk": skey("gz"), "gs": skey("x9"), "pk": skey("z"), "sk": skey("s9")}
	tests := []struct {
		start   map[string]types.AttributeValue
		name    string
		want    []string
		query   bool
		reverse bool
	}{
		{gk("x2", "s2"), "scan deleted middle", all[1:], false, false},
		{pastEnd, "scan deleted end", nil, false, false},
		{gk("x0", "s0"), "scan deleted before first", all, false, false},
		{gk("x2", "s2"), "query deleted middle", []string{"as3", "as5"}, true, false},
		{gk("x9", "s9"), "query deleted end", nil, true, false},
		{gk("x0", "s0"), "query deleted before first", all[:3], true, false},
		{gk("x4", "s4"), "reverse deleted middle", []string{"as3", "as1"}, true, true},
		{gk("x0", "s0"), "reverse deleted end", nil, true, true},
		{gk("x9", "s9"), "reverse before first", []string{"as5", "as3", "as1"}, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := startKeyTable(t, false)

			var items []map[string]types.AttributeValue

			if tt.query {
				out, err := db.Query(t.Context(), &sdk.QueryInput{
					TableName: aws.String("t"), IndexName: aws.String("gsi"),
					KeyConditionExpression:    aws.String("gk = :g"),
					ExpressionAttributeValues: map[string]types.AttributeValue{":g": skey("ga")},
					ScanIndexForward:          aws.Bool(!tt.reverse),
					ExclusiveStartKey:         tt.start,
				})
				require.NoError(t, err)

				items = out.Items
			} else {
				out, err := db.Scan(t.Context(), &sdk.ScanInput{
					TableName: aws.String("t"), IndexName: aws.String("gsi"), ExclusiveStartKey: tt.start,
				})
				require.NoError(t, err)

				items = out.Items
			}

			got := ids(items)
			assert.Len(t, got, len(tt.want))
			if len(tt.want) > 0 {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestExclusiveStartKey_BaseTableQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sk      string
		want    []string
		reverse bool
	}{
		{"deleted middle", "s2", []string{"as3", "as5"}, false},
		{"deleted end", "s9", nil, false},
		{"deleted before first", "s0", []string{"as1", "as3", "as5"}, false},
		{"reverse deleted middle", "s4", []string{"as3", "as1"}, true},
		{"reverse deleted end", "s0", nil, true},
		{"reverse before first", "s9", []string{"as5", "as3", "as1"}, true},
		{"existing", "s3", []string{"as5"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := startKeyTable(t, false)
			out, err := db.Query(t.Context(), &sdk.QueryInput{
				TableName:                 aws.String("t"),
				KeyConditionExpression:    aws.String("pk = :p"),
				ExpressionAttributeValues: map[string]types.AttributeValue{":p": skey("a")},
				ScanIndexForward:          aws.Bool(!tt.reverse),
				ExclusiveStartKey:         map[string]types.AttributeValue{"pk": skey("a"), "sk": skey(tt.sk)},
			})
			require.NoError(t, err)

			got := ids(out.Items)
			assert.Len(t, got, len(tt.want))
			if len(tt.want) > 0 {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
