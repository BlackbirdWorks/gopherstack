package dynamodb

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	sdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// --- secondaryIndex unit tests (no backend involved) ---

func TestSecondaryIndex_AddRemove(t *testing.T) {
	t.Parallel()

	t.Run("no sort key", func(t *testing.T) {
		t.Parallel()

		si := newSecondaryIndex(false)
		si.add("g1", "", false, 0)
		si.add("g1", "", false, 1)

		set := si.offsetsForPK("g1", false)
		require.Len(t, set, 2)
		_, has0 := set[0]
		_, has1 := set[1]
		require.True(t, has0)
		require.True(t, has1)

		si.remove("g1", "", false, 0)
		set = si.offsetsForPK("g1", false)
		require.Len(t, set, 1)
		_, has1 = set[1]
		require.True(t, has1)

		si.remove("g1", "", false, 1)
		require.Nil(t, si.offsetsForPK("g1", false), "removing the last offset must prune the key entirely")
	})

	t.Run("with sort key, shared pk+sk", func(t *testing.T) {
		t.Parallel()

		si := newSecondaryIndex(true)
		// Two different base-table items sharing the exact same GSI pk+sk pair --
		// legal in DynamoDB, unlike the base table's own key.
		si.add("g1", "10", true, 0)
		si.add("g1", "10", true, 1)
		si.add("g1", "20", true, 2)

		all := si.offsetsForPK("g1", true)
		require.Len(t, all, 3, "offsetsForPK unions every sort-key bucket under the pk")

		si.remove("g1", "10", true, 0)
		all = si.offsetsForPK("g1", true)
		require.Len(t, all, 2, "one of the two same-key items is gone, the other and the third remain")

		si.remove("g1", "10", true, 1)
		si.remove("g1", "20", true, 2)
		require.Nil(t, si.offsetsForPK("g1", true))
	})

	t.Run("unknown pk", func(t *testing.T) {
		t.Parallel()

		si := newSecondaryIndex(true)
		require.Nil(t, si.offsetsForPK("missing", true))

		si2 := newSecondaryIndex(false)
		require.Nil(t, si2.offsetsForPK("missing", false))
	})
}

func TestSecondaryItemKeyValues_Sparse(t *testing.T) {
	t.Parallel()

	pkDef := models.KeySchemaElement{AttributeName: "grp", KeyType: models.KeyTypeHash}
	skDef := models.KeySchemaElement{AttributeName: "score", KeyType: models.KeyTypeRange}
	noSK := models.KeySchemaElement{}

	tests := []struct {
		item   map[string]any
		pkDef  models.KeySchemaElement
		skDef  models.KeySchemaElement
		name   string
		wantOK bool
	}{
		{name: "nil item", item: nil, pkDef: pkDef, skDef: skDef, wantOK: false},
		{
			name:  "missing pk attr",
			item:  map[string]any{"score": map[string]any{"N": "1"}},
			pkDef: pkDef, skDef: skDef,
			wantOK: false,
		},
		{
			name:  "missing sk attr, index has sort key",
			item:  map[string]any{"grp": map[string]any{"S": "g1"}},
			pkDef: pkDef, skDef: skDef,
			wantOK: false,
		},
		{
			name:  "both present",
			item:  map[string]any{"grp": map[string]any{"S": "g1"}, "score": map[string]any{"N": "1"}},
			pkDef: pkDef, skDef: skDef,
			wantOK: true,
		},
		{
			name:  "pk-only index, sk attr irrelevant",
			item:  map[string]any{"grp": map[string]any{"S": "g1"}},
			pkDef: pkDef, skDef: noSK,
			wantOK: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, _, ok := secondaryItemKeyValues(tc.item, tc.pkDef, tc.skDef)
			require.Equal(t, tc.wantOK, ok)
		})
	}
}

// --- backend-level whitebox tests: exercise real write paths, assert on the
// unexported table.gsiIndexes/lsiIndexes structure directly. ---

const (
	secIdxTableName = "sec-idx-table"
	secIdxGSIName   = "gsi1"
	secIdxLSIName   = "lsi1"
)

func newSecIdxTestDB(t *testing.T) *InMemoryDB {
	t.Helper()

	db := NewInMemoryDB()
	t.Cleanup(db.Close)

	return db
}

// createSecIdxTable creates a table with a base composite key (id, seq), a
// GSI (grp, score) with ALL projection, and an LSI (id, tier) with ALL
// projection -- enough surface to exercise sparse membership, non-unique
// keys, and range conditions on both index kinds.
func createSecIdxTable(t *testing.T, db *InMemoryDB) {
	t.Helper()

	rc, wc := int64(50), int64(50)
	_, err := db.CreateTable(context.Background(), &sdkdynamodb.CreateTableInput{
		TableName: aws.String(secIdxTableName),
		KeySchema: []sdktypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: sdktypes.KeyTypeHash},
			{AttributeName: aws.String("seq"), KeyType: sdktypes.KeyTypeRange},
		},
		AttributeDefinitions: []sdktypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: sdktypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("seq"), AttributeType: sdktypes.ScalarAttributeTypeN},
			{AttributeName: aws.String("grp"), AttributeType: sdktypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("score"), AttributeType: sdktypes.ScalarAttributeTypeN},
			{AttributeName: aws.String("tier"), AttributeType: sdktypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []sdktypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String(secIdxGSIName),
				KeySchema: []sdktypes.KeySchemaElement{
					{AttributeName: aws.String("grp"), KeyType: sdktypes.KeyTypeHash},
					{AttributeName: aws.String("score"), KeyType: sdktypes.KeyTypeRange},
				},
				Projection: &sdktypes.Projection{ProjectionType: sdktypes.ProjectionTypeAll},
				ProvisionedThroughput: &sdktypes.ProvisionedThroughput{
					ReadCapacityUnits: aws.Int64(rc), WriteCapacityUnits: aws.Int64(wc),
				},
			},
		},
		LocalSecondaryIndexes: []sdktypes.LocalSecondaryIndex{
			{
				IndexName: aws.String(secIdxLSIName),
				KeySchema: []sdktypes.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: sdktypes.KeyTypeHash},
					{AttributeName: aws.String("tier"), KeyType: sdktypes.KeyTypeRange},
				},
				Projection: &sdktypes.Projection{ProjectionType: sdktypes.ProjectionTypeAll},
			},
		},
		ProvisionedThroughput: &sdktypes.ProvisionedThroughput{
			ReadCapacityUnits: aws.Int64(rc), WriteCapacityUnits: aws.Int64(wc),
		},
	})
	require.NoError(t, err)
}

// secIdxItem builds an item's AttributeValue map. grp/score/tier are omitted
// from the item entirely when the pointer is nil, to exercise sparse-index
// membership.
func secIdxItem(id string, seq int, grp *string, score *int, tier *string) map[string]sdktypes.AttributeValue {
	item := map[string]sdktypes.AttributeValue{
		"id":  &sdktypes.AttributeValueMemberS{Value: id},
		"seq": &sdktypes.AttributeValueMemberN{Value: strconv.Itoa(seq)},
	}
	if grp != nil {
		item["grp"] = &sdktypes.AttributeValueMemberS{Value: *grp}
	}
	if score != nil {
		item["score"] = &sdktypes.AttributeValueMemberN{Value: strconv.Itoa(*score)}
	}
	if tier != nil {
		item["tier"] = &sdktypes.AttributeValueMemberS{Value: *tier}
	}

	return item
}

func putSecIdxItem(t *testing.T, db *InMemoryDB, item map[string]sdktypes.AttributeValue) {
	t.Helper()

	_, err := db.PutItem(context.Background(), &sdkdynamodb.PutItemInput{
		TableName: aws.String(secIdxTableName),
		Item:      item,
	})
	require.NoError(t, err)
}

func TestSecondaryIndex_SharedKey_MultipleItems(t *testing.T) {
	t.Parallel()

	db := newSecIdxTestDB(t)
	createSecIdxTable(t, db)

	putSecIdxItem(t, db, secIdxItem("a", 1, new("g1"), new(1), nil))
	putSecIdxItem(t, db, secIdxItem("b", 1, new("g1"), new(2), nil))
	putSecIdxItem(t, db, secIdxItem("c", 1, new("g1"), new(3), nil))

	table, ok := db.GetTable(secIdxTableName)
	require.True(t, ok)

	table.mu.RLock("test")
	offsets := table.gsiIndexes[secIdxGSIName].offsetsForPK("g1", true)
	table.mu.RUnlock()
	require.Len(t, offsets, 3, "three distinct items sharing one GSI pk must all be indexed")

	out, err := db.Query(context.Background(), &sdkdynamodb.QueryInput{
		TableName:              aws.String(secIdxTableName),
		IndexName:              aws.String(secIdxGSIName),
		KeyConditionExpression: aws.String("grp = :g"),
		ExpressionAttributeValues: map[string]sdktypes.AttributeValue{
			":g": &sdktypes.AttributeValueMemberS{Value: "g1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 3)
}

func TestSecondaryIndex_Sparse_MissingGSIAttr(t *testing.T) {
	t.Parallel()

	db := newSecIdxTestDB(t)
	createSecIdxTable(t, db)

	putSecIdxItem(t, db, secIdxItem("a", 1, new("g1"), new(1), nil))
	putSecIdxItem(t, db, secIdxItem("sparse", 1, nil, nil, nil)) // no grp -- must never appear in the GSI

	table, ok := db.GetTable(secIdxTableName)
	require.True(t, ok)

	table.mu.RLock("test")
	si := table.gsiIndexes[secIdxGSIName]
	for pk, skMap := range si.pksk {
		for _, set := range skMap {
			for idx := range set {
				require.NotEqual(t, "sparse", table.Items[idx]["id"].(map[string]any)["S"],
					"item missing the GSI key attribute must not appear under any pk (found under %q)", pk)
			}
		}
	}
	table.mu.RUnlock()

	// A GSI query for any group never surfaces the sparse item.
	out, err := db.Query(context.Background(), &sdkdynamodb.QueryInput{
		TableName:              aws.String(secIdxTableName),
		IndexName:              aws.String(secIdxGSIName),
		KeyConditionExpression: aws.String("grp = :g"),
		ExpressionAttributeValues: map[string]sdktypes.AttributeValue{
			":g": &sdktypes.AttributeValueMemberS{Value: "g1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 1)

	// But the item is still a real, fully-present base-table row.
	getOut, err := db.GetItem(context.Background(), &sdkdynamodb.GetItemInput{
		TableName: aws.String(secIdxTableName),
		Key: map[string]sdktypes.AttributeValue{
			"id":  &sdktypes.AttributeValueMemberS{Value: "sparse"},
			"seq": &sdktypes.AttributeValueMemberN{Value: "1"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, getOut.Item)
}

func gsiQueryCount(t *testing.T, db *InMemoryDB, grpVal string) int {
	t.Helper()

	out, err := db.Query(context.Background(), &sdkdynamodb.QueryInput{
		TableName:              aws.String(secIdxTableName),
		IndexName:              aws.String(secIdxGSIName),
		KeyConditionExpression: aws.String("grp = :g"),
		ExpressionAttributeValues: map[string]sdktypes.AttributeValue{
			":g": &sdktypes.AttributeValueMemberS{Value: grpVal},
		},
	})
	require.NoError(t, err)

	return len(out.Items)
}

func TestSecondaryIndex_Update_MovesGSIKey(t *testing.T) {
	t.Parallel()

	db := newSecIdxTestDB(t)
	createSecIdxTable(t, db)

	putSecIdxItem(t, db, secIdxItem("a", 1, new("g1"), new(1), nil))
	require.Equal(t, 1, gsiQueryCount(t, db, "g1"))
	require.Equal(t, 0, gsiQueryCount(t, db, "g2"))

	_, err := db.UpdateItem(context.Background(), &sdkdynamodb.UpdateItemInput{
		TableName: aws.String(secIdxTableName),
		Key: map[string]sdktypes.AttributeValue{
			"id":  &sdktypes.AttributeValueMemberS{Value: "a"},
			"seq": &sdktypes.AttributeValueMemberN{Value: "1"},
		},
		UpdateExpression: aws.String("SET grp = :g"),
		ExpressionAttributeValues: map[string]sdktypes.AttributeValue{
			":g": &sdktypes.AttributeValueMemberS{Value: "g2"},
		},
	})
	require.NoError(t, err)

	require.Equal(t, 0, gsiQueryCount(t, db, "g1"), "item must leave its old GSI key")
	require.Equal(t, 1, gsiQueryCount(t, db, "g2"), "item must appear under its new GSI key")
}

func TestSecondaryIndex_Update_RemovesGSIKeyAttribute(t *testing.T) {
	t.Parallel()

	db := newSecIdxTestDB(t)
	createSecIdxTable(t, db)

	putSecIdxItem(t, db, secIdxItem("a", 1, new("g1"), new(1), nil))
	require.Equal(t, 1, gsiQueryCount(t, db, "g1"))

	_, err := db.UpdateItem(context.Background(), &sdkdynamodb.UpdateItemInput{
		TableName: aws.String(secIdxTableName),
		Key: map[string]sdktypes.AttributeValue{
			"id":  &sdktypes.AttributeValueMemberS{Value: "a"},
			"seq": &sdktypes.AttributeValueMemberN{Value: "1"},
		},
		UpdateExpression: aws.String("REMOVE grp, score"),
	})
	require.NoError(t, err)

	require.Equal(t, 0, gsiQueryCount(t, db, "g1"), "item whose GSI key attr was removed must leave the index")

	// The item is still there, just outside the (now sparse) GSI.
	getOut, err := db.GetItem(context.Background(), &sdkdynamodb.GetItemInput{
		TableName: aws.String(secIdxTableName),
		Key: map[string]sdktypes.AttributeValue{
			"id":  &sdktypes.AttributeValueMemberS{Value: "a"},
			"seq": &sdktypes.AttributeValueMemberN{Value: "1"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, getOut.Item)
}

func TestSecondaryIndex_Delete_RemovesFromIndex(t *testing.T) {
	t.Parallel()

	db := newSecIdxTestDB(t)
	createSecIdxTable(t, db)

	putSecIdxItem(t, db, secIdxItem("a", 1, new("g1"), new(1), nil))
	putSecIdxItem(t, db, secIdxItem("b", 1, new("g1"), new(2), nil))
	require.Equal(t, 2, gsiQueryCount(t, db, "g1"))

	_, err := db.DeleteItem(context.Background(), &sdkdynamodb.DeleteItemInput{
		TableName: aws.String(secIdxTableName),
		Key: map[string]sdktypes.AttributeValue{
			"id":  &sdktypes.AttributeValueMemberS{Value: "a"},
			"seq": &sdktypes.AttributeValueMemberN{Value: "1"},
		},
	})
	require.NoError(t, err)

	require.Equal(t, 1, gsiQueryCount(t, db, "g1"))

	// Deleting the survivor too must fully empty the index bucket, exercising
	// the swap-with-last-item path in deleteItemAtIndex.
	_, err = db.DeleteItem(context.Background(), &sdkdynamodb.DeleteItemInput{
		TableName: aws.String(secIdxTableName),
		Key: map[string]sdktypes.AttributeValue{
			"id":  &sdktypes.AttributeValueMemberS{Value: "b"},
			"seq": &sdktypes.AttributeValueMemberN{Value: "1"},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 0, gsiQueryCount(t, db, "g1"))
}

func TestSecondaryIndex_DeleteSwap_RetargetsSurvivor(t *testing.T) {
	t.Parallel()

	db := newSecIdxTestDB(t)
	createSecIdxTable(t, db)

	// Three items in three different GSI groups. Deleting the first forces
	// deleteItemAtIndex's swap-with-last-item optimisation to move the
	// physically-last item (c, group g3) into the freed base-table slot --
	// its GSI membership must move with it, at its NEW offset.
	putSecIdxItem(t, db, secIdxItem("a", 1, new("g1"), new(1), nil))
	putSecIdxItem(t, db, secIdxItem("b", 1, new("g2"), new(1), nil))
	putSecIdxItem(t, db, secIdxItem("c", 1, new("g3"), new(1), nil))

	_, err := db.DeleteItem(context.Background(), &sdkdynamodb.DeleteItemInput{
		TableName: aws.String(secIdxTableName),
		Key: map[string]sdktypes.AttributeValue{
			"id":  &sdktypes.AttributeValueMemberS{Value: "a"},
			"seq": &sdktypes.AttributeValueMemberN{Value: "1"},
		},
	})
	require.NoError(t, err)

	require.Equal(t, 1, gsiQueryCount(t, db, "g2"))
	require.Equal(t, 1, gsiQueryCount(t, db, "g3"), "the swapped-in survivor must remain queryable under its GSI key")
}

func TestSecondaryIndex_BatchWrite(t *testing.T) {
	t.Parallel()

	db := newSecIdxTestDB(t)
	createSecIdxTable(t, db)

	putSecIdxItem(t, db, secIdxItem("a", 1, new("g1"), new(1), nil))
	putSecIdxItem(t, db, secIdxItem("b", 1, new("g1"), new(2), nil))

	_, err := db.BatchWriteItem(context.Background(), &sdkdynamodb.BatchWriteItemInput{
		RequestItems: map[string][]sdktypes.WriteRequest{
			secIdxTableName: {
				// Overwrite "a", moving it from g1 to g2.
				{PutRequest: &sdktypes.PutRequest{Item: secIdxItem("a", 1, new("g2"), new(9), nil)}},
				// Brand new item straight into g1.
				{PutRequest: &sdktypes.PutRequest{Item: secIdxItem("c", 1, new("g1"), new(3), nil)}},
				// Delete "b".
				{DeleteRequest: &sdktypes.DeleteRequest{Key: map[string]sdktypes.AttributeValue{
					"id":  &sdktypes.AttributeValueMemberS{Value: "b"},
					"seq": &sdktypes.AttributeValueMemberN{Value: "1"},
				}}},
			},
		},
	})
	require.NoError(t, err)

	require.Equal(t, 1, gsiQueryCount(t, db, "g1"), "only the brand-new item c should remain under g1")
	require.Equal(t, 1, gsiQueryCount(t, db, "g2"), "a should have moved to g2")
}

func TestSecondaryIndex_TransactWrite_CommitAndRollback(t *testing.T) {
	t.Parallel()

	t.Run("commit", func(t *testing.T) {
		t.Parallel()

		db := newSecIdxTestDB(t)
		createSecIdxTable(t, db)

		_, err := db.TransactWriteItems(context.Background(), &sdkdynamodb.TransactWriteItemsInput{
			TransactItems: []sdktypes.TransactWriteItem{
				{
					Put: &sdktypes.Put{
						TableName: aws.String(secIdxTableName),
						Item:      secIdxItem("a", 1, new("g1"), new(1), nil),
					},
				},
				{
					Put: &sdktypes.Put{
						TableName: aws.String(secIdxTableName),
						Item:      secIdxItem("b", 1, new("g1"), new(2), nil),
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, 2, gsiQueryCount(t, db, "g1"))
	})

	t.Run("rollback leaves index untouched", func(t *testing.T) {
		t.Parallel()

		db := newSecIdxTestDB(t)
		createSecIdxTable(t, db)

		putSecIdxItem(t, db, secIdxItem("a", 1, new("g1"), new(1), nil))

		table, ok := db.GetTable(secIdxTableName)
		require.True(t, ok)

		table.mu.RLock("test")
		before := copySecondaryIndexMap(table.gsiIndexes)
		table.mu.RUnlock()

		// The first Put is well-formed and would succeed on its own. The second
		// carries an oversized attribute that only fails validation once
		// applyTransactWrite actually runs it (item_ops_crud.go's validateItem,
		// called from applyTransactPut) -- unlike a failing ConditionExpression,
		// which is rejected in TransactWriteItems' earlier condition-check phase
		// before any write is applied at all. This is the only way to force a
		// real mid-apply rollback and so genuinely exercise
		// snapshotTables/rollbackTables's GSI/LSI restoration, not just prove
		// the transaction never touched the table.
		oversized := secIdxItem("toobig", 1, new("g9"), new(1), nil)
		oversized["blob"] = &sdktypes.AttributeValueMemberS{Value: strings.Repeat("x", MaxItemSize+1024)}

		_, err := db.TransactWriteItems(context.Background(), &sdkdynamodb.TransactWriteItemsInput{
			TransactItems: []sdktypes.TransactWriteItem{
				{Put: &sdktypes.Put{
					TableName: aws.String(secIdxTableName),
					Item:      secIdxItem("z", 1, new("g9"), new(1), nil),
				}},
				{Put: &sdktypes.Put{
					TableName: aws.String(secIdxTableName),
					Item:      oversized,
				}},
			},
		})
		require.Error(t, err)

		require.Equal(t, 1, gsiQueryCount(t, db, "g1"), "pre-transaction state must be intact")
		require.Equal(t, 0, gsiQueryCount(t, db, "g9"), "the rolled-back put must not linger in the GSI")

		table.mu.RLock("test")
		after := table.gsiIndexes
		table.mu.RUnlock()

		requireSameSecondaryIndexes(t, before, after)
	})
}

// requireSameSecondaryIndexes asserts two name->*secondaryIndex maps hold
// identical offset sets, used to prove a rolled-back transaction left GSI/LSI
// state byte-for-byte as it was before the transaction started.
func requireSameSecondaryIndexes(t *testing.T, a, b map[string]*secondaryIndex) {
	t.Helper()

	require.Len(t, b, len(a))

	for name, siA := range a {
		siB, ok := b[name]
		require.True(t, ok, "index %q missing after rollback", name)
		require.Equal(t, offsetSetsOf(siA), offsetSetsOf(siB), "index %q diverged after rollback", name)
	}
}

// offsetSetsOf flattens a secondaryIndex into a comparable
// map[pk+"\x00"+sk][]int-as-set representation for require.Equal.
func offsetSetsOf(si *secondaryIndex) map[string]map[int]struct{} {
	out := make(map[string]map[int]struct{})

	maps.Copy(out, si.pkOnly)

	for pk, skMap := range si.pksk {
		for sk, set := range skMap {
			out[pk+"\x00"+sk] = set
		}
	}

	return out
}

func TestSecondaryIndex_ProjectionTypes(t *testing.T) {
	t.Parallel()

	rc, wc := int64(50), int64(50)

	tests := []struct {
		verify   func(t *testing.T, attrs map[string]sdktypes.AttributeValue)
		name     string
		projType sdktypes.ProjectionType
		nonKey   []string
	}{
		{
			name:     "keys only",
			projType: sdktypes.ProjectionTypeKeysOnly,
			verify: func(t *testing.T, attrs map[string]sdktypes.AttributeValue) {
				t.Helper()
				// Base table key (id, seq) + GSI key (grp, score) only.
				require.ElementsMatch(t, []string{"id", "seq", "grp", "score"}, keysOf(attrs))
			},
		},
		{
			name:     "include",
			projType: sdktypes.ProjectionTypeInclude,
			nonKey:   []string{"extra1"},
			verify: func(t *testing.T, attrs map[string]sdktypes.AttributeValue) {
				t.Helper()
				require.ElementsMatch(t, []string{"id", "seq", "grp", "score", "extra1"}, keysOf(attrs))
			},
		},
		{
			name:     "all",
			projType: sdktypes.ProjectionTypeAll,
			verify: func(t *testing.T, attrs map[string]sdktypes.AttributeValue) {
				t.Helper()
				require.ElementsMatch(t, []string{"id", "seq", "grp", "score", "extra1", "extra2"}, keysOf(attrs))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tableName := "proj-table-" + tc.name
			db := newSecIdxTestDB(t)

			gsi := sdktypes.GlobalSecondaryIndex{
				IndexName: aws.String("gsi1"),
				KeySchema: []sdktypes.KeySchemaElement{
					{AttributeName: aws.String("grp"), KeyType: sdktypes.KeyTypeHash},
					{AttributeName: aws.String("score"), KeyType: sdktypes.KeyTypeRange},
				},
				Projection: &sdktypes.Projection{
					ProjectionType:   tc.projType,
					NonKeyAttributes: tc.nonKey,
				},
				ProvisionedThroughput: &sdktypes.ProvisionedThroughput{
					ReadCapacityUnits: aws.Int64(rc), WriteCapacityUnits: aws.Int64(wc),
				},
			}

			_, err := db.CreateTable(context.Background(), &sdkdynamodb.CreateTableInput{
				TableName: aws.String(tableName),
				KeySchema: []sdktypes.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: sdktypes.KeyTypeHash},
					{AttributeName: aws.String("seq"), KeyType: sdktypes.KeyTypeRange},
				},
				AttributeDefinitions: []sdktypes.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: sdktypes.ScalarAttributeTypeS},
					{AttributeName: aws.String("seq"), AttributeType: sdktypes.ScalarAttributeTypeN},
					{AttributeName: aws.String("grp"), AttributeType: sdktypes.ScalarAttributeTypeS},
					{AttributeName: aws.String("score"), AttributeType: sdktypes.ScalarAttributeTypeN},
				},
				GlobalSecondaryIndexes: []sdktypes.GlobalSecondaryIndex{gsi},
				ProvisionedThroughput: &sdktypes.ProvisionedThroughput{
					ReadCapacityUnits: aws.Int64(rc), WriteCapacityUnits: aws.Int64(wc),
				},
			})
			require.NoError(t, err)

			_, err = db.PutItem(context.Background(), &sdkdynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]sdktypes.AttributeValue{
					"id":     &sdktypes.AttributeValueMemberS{Value: "a"},
					"seq":    &sdktypes.AttributeValueMemberN{Value: "1"},
					"grp":    &sdktypes.AttributeValueMemberS{Value: "g1"},
					"score":  &sdktypes.AttributeValueMemberN{Value: "1"},
					"extra1": &sdktypes.AttributeValueMemberS{Value: "x"},
					"extra2": &sdktypes.AttributeValueMemberS{Value: "y"},
				},
			})
			require.NoError(t, err)

			out, err := db.Query(context.Background(), &sdkdynamodb.QueryInput{
				TableName:              aws.String(tableName),
				IndexName:              aws.String("gsi1"),
				KeyConditionExpression: aws.String("grp = :g"),
				ExpressionAttributeValues: map[string]sdktypes.AttributeValue{
					":g": &sdktypes.AttributeValueMemberS{Value: "g1"},
				},
			})
			require.NoError(t, err)
			require.Len(t, out.Items, 1)
			tc.verify(t, out.Items[0])
		})
	}
}

func keysOf(m map[string]sdktypes.AttributeValue) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	return out
}

func TestSecondaryIndex_LSI_RangeCondition(t *testing.T) {
	t.Parallel()

	db := newSecIdxTestDB(t)
	createSecIdxTable(t, db)

	// Tier values are picked to sort lexicographically as bronze < gold < silver
	// (DynamoDB's S-type BETWEEN is a byte-string comparison, not domain-aware),
	// so BETWEEN bronze..gold deterministically covers exactly two of them.
	putSecIdxItem(t, db, secIdxItem("a", 1, nil, nil, new("bronze")))
	putSecIdxItem(t, db, secIdxItem("a", 2, nil, nil, new("silver")))
	putSecIdxItem(t, db, secIdxItem("a", 3, nil, nil, new("gold")))
	putSecIdxItem(t, db, secIdxItem("a", 4, nil, nil, nil)) // sparse: no tier, must not appear in the LSI

	out, err := db.Query(context.Background(), &sdkdynamodb.QueryInput{
		TableName:              aws.String(secIdxTableName),
		IndexName:              aws.String(secIdxLSIName),
		KeyConditionExpression: aws.String("id = :id AND tier BETWEEN :lo AND :hi"),
		ExpressionAttributeValues: map[string]sdktypes.AttributeValue{
			":id": &sdktypes.AttributeValueMemberS{Value: "a"},
			":lo": &sdktypes.AttributeValueMemberS{Value: "bronze"},
			":hi": &sdktypes.AttributeValueMemberS{Value: "gold"},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 2, "BETWEEN bronze..gold should match bronze and gold, not silver or the sparse item")
}

func TestSecondaryIndex_GSIBackfill_UpdateTable(t *testing.T) {
	t.Parallel()

	db := newSecIdxTestDB(t)
	rc, wc := int64(50), int64(50)

	_, err := db.CreateTable(context.Background(), &sdkdynamodb.CreateTableInput{
		TableName: aws.String("backfill-table"),
		KeySchema: []sdktypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: sdktypes.KeyTypeHash},
		},
		AttributeDefinitions: []sdktypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: sdktypes.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &sdktypes.ProvisionedThroughput{
			ReadCapacityUnits: aws.Int64(rc), WriteCapacityUnits: aws.Int64(wc),
		},
	})
	require.NoError(t, err)

	// Populate the table BEFORE the GSI exists -- some items have "grp", one doesn't.
	for i, grp := range []string{"g1", "g1", "g2"} {
		_, putErr := db.PutItem(context.Background(), &sdkdynamodb.PutItemInput{
			TableName: aws.String("backfill-table"),
			Item: map[string]sdktypes.AttributeValue{
				"id":  &sdktypes.AttributeValueMemberS{Value: fmt.Sprintf("item%d", i)},
				"grp": &sdktypes.AttributeValueMemberS{Value: grp},
			},
		})
		require.NoError(t, putErr)
	}
	_, err = db.PutItem(context.Background(), &sdkdynamodb.PutItemInput{
		TableName: aws.String("backfill-table"),
		Item:      map[string]sdktypes.AttributeValue{"id": &sdktypes.AttributeValueMemberS{Value: "sparse-item"}},
	})
	require.NoError(t, err)

	_, err = db.UpdateTable(context.Background(), &sdkdynamodb.UpdateTableInput{
		TableName: aws.String("backfill-table"),
		AttributeDefinitions: []sdktypes.AttributeDefinition{
			{AttributeName: aws.String("grp"), AttributeType: sdktypes.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexUpdates: []sdktypes.GlobalSecondaryIndexUpdate{
			{
				Create: &sdktypes.CreateGlobalSecondaryIndexAction{
					IndexName: aws.String("grp-index"),
					KeySchema: []sdktypes.KeySchemaElement{
						{AttributeName: aws.String("grp"), KeyType: sdktypes.KeyTypeHash},
					},
					Projection: &sdktypes.Projection{ProjectionType: sdktypes.ProjectionTypeAll},
					ProvisionedThroughput: &sdktypes.ProvisionedThroughput{
						ReadCapacityUnits: aws.Int64(rc), WriteCapacityUnits: aws.Int64(wc),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	out, err := db.Query(context.Background(), &sdkdynamodb.QueryInput{
		TableName:              aws.String("backfill-table"),
		IndexName:              aws.String("grp-index"),
		KeyConditionExpression: aws.String("grp = :g"),
		ExpressionAttributeValues: map[string]sdktypes.AttributeValue{
			":g": &sdktypes.AttributeValueMemberS{Value: "g1"},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Items, 2, "backfill must index pre-existing items retroactively")

	out2, err := db.Query(context.Background(), &sdkdynamodb.QueryInput{
		TableName:              aws.String("backfill-table"),
		IndexName:              aws.String("grp-index"),
		KeyConditionExpression: aws.String("grp = :g"),
		ExpressionAttributeValues: map[string]sdktypes.AttributeValue{
			":g": &sdktypes.AttributeValueMemberS{Value: "g2"},
		},
	})
	require.NoError(t, err)
	require.Len(t, out2.Items, 1)

	// 4 items were put before the GSI existed; only the 3 with grp set were
	// backfilled (2 into g1, 1 into g2) -- the sparse item is provably absent
	// from the index entirely, not just from these two probed values.
	table, ok := db.GetTable("backfill-table")
	require.True(t, ok)

	table.mu.RLock("test")
	total := 0
	for _, set := range table.gsiIndexes["grp-index"].pkOnly {
		total += len(set)
	}
	table.mu.RUnlock()
	require.Equal(t, 3, total, "the sparse item must not have been backfilled into any GSI bucket")
}

// --- differential test: the indexed Query path must return exactly what the
// old full-table-scan path returns, for random data and random conditions. ---

func TestQuery_GSI_DifferentialAgainstScan(t *testing.T) {
	t.Parallel()

	db := newSecIdxTestDB(t)
	createSecIdxTable(t, db)

	rng := rand.New(rand.NewSource(7))

	groups := []string{"alpha", "beta", "gamma", "delta"}
	const numItems = 250

	for i := range numItems {
		id := "item-" + strconv.Itoa(i)
		seq := rng.Intn(5)

		// ~15% of items are sparse (no grp/score at all).
		if rng.Intn(100) < 15 {
			putSecIdxItem(t, db, secIdxItem(id, seq, nil, nil, nil))

			continue
		}

		grp := groups[rng.Intn(len(groups))]
		score := rng.Intn(40) // deliberately narrow range so duplicate scores are common
		putSecIdxItem(t, db, secIdxItem(id, seq, &grp, &score, nil))
	}

	table, ok := db.GetTable(secIdxTableName)
	require.True(t, ok)

	const numTrials = 200

	for trial := range numTrials {
		// math/rand.Rand is not safe for concurrent use, and every subtest
		// below runs in parallel -- each trial gets its own independently
		// (but deterministically) seeded source rather than sharing rng.
		trialRng := rand.New(rand.NewSource(int64(trial) + 1))

		t.Run(fmt.Sprintf("trial-%d", trial), func(t *testing.T) {
			t.Parallel()

			pkVal := groups[trialRng.Intn(len(groups))]
			if trialRng.Intn(10) == 0 {
				pkVal = "nonexistent-group" // exercise the "no matches" path too
			}

			input := randomGSIQueryInput(trialRng, pkVal)
			assertQueryMatchesScan(t, db, table, input)
		})
	}
}

// randomGSIQueryInput builds a QueryInput against secIdxGSIName with a
// partition-key equality condition on pkVal, and a randomly chosen
// (possibly absent) condition on the numeric sort key "score".
func randomGSIQueryInput(rng *rand.Rand, pkVal string) *sdkdynamodb.QueryInput {
	eav := map[string]sdktypes.AttributeValue{
		":g": &sdktypes.AttributeValueMemberS{Value: pkVal},
	}
	expr := "grp = :g"

	switch rng.Intn(5) {
	case 0: // pk only
	case 1:
		v := strconv.Itoa(rng.Intn(40))
		eav[":s"] = &sdktypes.AttributeValueMemberN{Value: v}
		expr += " AND score = :s"
	case 2:
		v := strconv.Itoa(rng.Intn(40))
		eav[":s"] = &sdktypes.AttributeValueMemberN{Value: v}
		expr += " AND score > :s"
	case 3:
		v := strconv.Itoa(rng.Intn(40))
		eav[":s"] = &sdktypes.AttributeValueMemberN{Value: v}
		expr += " AND score <= :s"
	default:
		lo, hi := rng.Intn(40), rng.Intn(40)
		if lo > hi {
			lo, hi = hi, lo
		}
		eav[":lo"] = &sdktypes.AttributeValueMemberN{Value: strconv.Itoa(lo)}
		eav[":hi"] = &sdktypes.AttributeValueMemberN{Value: strconv.Itoa(hi)}
		expr += " AND score BETWEEN :lo AND :hi"
	}

	return &sdkdynamodb.QueryInput{
		TableName:                 aws.String(secIdxTableName),
		IndexName:                 aws.String(secIdxGSIName),
		KeyConditionExpression:    aws.String(expr),
		ExpressionAttributeValues: eav,
	}
}

// assertQueryMatchesScan runs input through both the real (now indexed)
// query path and the original full-table-scan path against the identical
// snapshot, and asserts they select exactly the same base-table items. This
// is the strongest evidence available that the index never diverges from
// ground truth: filterCandidatesScan is the pre-existing, trusted-by-age
// code path this whole change is built to bypass for GSI/LSI queries.
func assertQueryMatchesScan(t *testing.T, db *InMemoryDB, table *Table, input *sdkdynamodb.QueryInput) {
	t.Helper()

	idxName := aws.ToString(input.IndexName)
	precomputedPKValue := preParseQueryPKValue(input)

	snap, _, _ := db.snapshotTableForQuery(table, idxName, precomputedPKValue)

	keySchema, projection, err := db.extractKeySchema(snap, idxName, false)
	require.NoError(t, err)

	indexed, err := db.filterCandidatesForKeyCondition(context.Background(), snap, input, projection, keySchema)
	require.NoError(t, err)

	// Ground truth: a full, unoptimised scan over its OWN full-Items copy of
	// the live table, independent of snapshotTableForQuery's targeted-copy
	// optimisation (which, for a known PK, deliberately leaves snap.Items
	// empty in favour of the smaller itemsByOffset map -- exactly the O(table)
	// copy this change exists to avoid, so reusing snap here would scan
	// nothing rather than everything).
	scanned, err := scanGroundTruth(db, table, input, projection, keySchema)
	require.NoError(t, err)

	require.Equal(t, canonicalItemSet(scanned), canonicalItemSet(indexed),
		"indexed path diverged from scan ground truth for %q", aws.ToString(input.KeyConditionExpression))
}

// scanGroundTruth re-derives the parsed key-condition parts exactly as
// filterCandidatesForKeyCondition does, then calls filterCandidatesScan
// directly against a full copy of the live table's items -- bypassing the
// index entirely -- so it always reflects the old, unoptimised behaviour
// regardless of what tryFilterUsingSecondaryIndex does.
func scanGroundTruth(
	db *InMemoryDB,
	table *Table,
	input *sdkdynamodb.QueryInput,
	projection *models.Projection,
	keySchema []models.KeySchemaElement,
) ([]map[string]any, error) {
	table.mu.RLock("test.scanGroundTruth")
	itemsCopy := make([]map[string]any, len(table.Items))
	copy(itemsCopy, table.Items)
	baseKeySchema := table.KeySchema
	table.mu.RUnlock()

	scanTable := &Table{Items: itemsCopy, KeySchema: baseKeySchema}

	cond := aws.ToString(input.KeyConditionExpression)
	exprParts := dynamoattr.SplitANDConditions(cond)

	parsedParts := make([]*ParsedCondition, 0, len(exprParts))
	for _, part := range exprParts {
		pc, err := ParseConditionStr(part)
		if err != nil {
			return nil, err
		}
		parsedParts = append(parsedParts, pc)
	}

	eav := models.FromSDKItem(input.ExpressionAttributeValues)

	return db.filterCandidatesScan(scanTable, input, projection, keySchema, parsedParts, eav)
}

func canonicalItemSet(items []map[string]any) map[string]bool {
	out := make(map[string]bool, len(items))
	for _, item := range items {
		id, _ := item["id"].(map[string]any)["S"].(string)
		seq, _ := item["seq"].(map[string]any)["N"].(string)
		out[id+"\x00"+seq] = true
	}

	return out
}
