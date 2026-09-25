package dynamodb_test

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"strconv"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// This file differentially tests TransactWriteItems (gopherstack-wdapu) against a
// small in-test reference model, driving random sequences of Put/Update/Delete/
// ConditionCheck (some of which fail their condition) through the exported API
// only, and comparing every response/error plus the resulting table and index
// state against the reference after each transaction.

const (
	diffTable = "DiffTransactTable"
	diffGSI1  = "gsi1"
	diffGSI2  = "gsi2"
	diffLSI1  = "lsi1"
)

// The domain functions below (rather than package-level slices, banned by
// gochecknoglobals) return the generator's closed value sets for each attribute.
func diffPKs() []string     { return []string{"p0", "p1", "p2"} }
func diffSKs() []string     { return []string{"s0", "s1", "s2", "s3"} }
func diffGSIPKs() []string  { return []string{"g0", "g1", "g2"} }
func diffGSISKs() []string  { return []string{"gs0", "gs1"} }
func diffGSIPK2s() []string { return []string{"h0", "h1"} }
func diffLSISKs() []string  { return []string{"l0", "l1", "l2"} }

// refItem is the reference model's plain-Go representation of one item.
type refItem struct {
	PK, SK, GSIPK, GSISK, GSIPK2, LSISK string
	Val                                 int
	Flag, HasFlag                       bool
	HasVal                              bool
}

func refKey(pk, sk string) string { return pk + "|" + sk }

// condKind is a closed set of ConditionExpression shapes the generator drives.
type condKind int

const (
	condNone condKind = iota
	condExists
	condNotExists
	condValGT
	condValEQ
)

func (k condKind) eval(item *refItem, threshold int) bool {
	switch k {
	case condNone:
		return true
	case condExists:
		return item != nil
	case condNotExists:
		return item == nil
	case condValGT:
		return item != nil && item.HasVal && item.Val > threshold
	case condValEQ:
		return item != nil && item.HasVal && item.Val == threshold
	default:
		return true
	}
}

func (k condKind) expr() string {
	switch k {
	case condExists:
		return "attribute_exists(pk)"
	case condNotExists:
		return "attribute_not_exists(pk)"
	case condValGT:
		return "val > :thresh"
	case condValEQ:
		return "val = :thresh"
	default:
		return ""
	}
}

// updKind is a closed set of UpdateExpression shapes the generator drives.
type updKind int

const (
	updSetVal updKind = iota
	updIncrVal
	updSetFlag
	updRemoveFlag
)

func (k updKind) expr() string {
	switch k {
	case updSetVal:
		return "SET val = :v"
	case updIncrVal:
		return "SET val = if_not_exists(val, :zero) + :one"
	case updSetFlag:
		return "SET flag = :true"
	case updRemoveFlag:
		return "REMOVE flag"
	default:
		return ""
	}
}

func (k updKind) apply(existing *refItem, pk, sk string, setValArg int) refItem {
	var out refItem
	if existing != nil {
		out = *existing
	}
	out.PK, out.SK = pk, sk

	switch k {
	case updSetVal:
		out.Val, out.HasVal = setValArg, true
	case updIncrVal:
		out.Val++
		out.HasVal = true
	case updSetFlag:
		out.Flag, out.HasFlag = true, true
	case updRemoveFlag:
		out.Flag, out.HasFlag = false, false
	}

	return out
}

// diffAction is one generated TransactWriteItem, in both its reference-model form
// and enough detail to build the real SDK request.
type diffAction struct {
	kind      string
	pk        string
	sk        string
	put       refItem
	cond      condKind
	threshold int
	upd       updKind
	setValArg int
	rvoccf    bool
}

// diffReason is the reference model's prediction for one action's
// CancellationReason.
type diffReason struct {
	item   *refItem
	failed bool
}

func createDiffTable(t *testing.T, db *dynamodb.InMemoryDB) {
	t.Helper()

	_, err := db.CreateTable(context.Background(), &sdk.CreateTableInput{
		TableName: aws.String(diffTable),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsipk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsisk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("gsipk2"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("lsisk"), AttributeType: types.ScalarAttributeTypeS},
		},
		GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String(diffGSI1),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("gsipk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("gsisk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
			{
				IndexName: aws.String(diffGSI2),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("gsipk2"), KeyType: types.KeyTypeHash},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String(diffLSI1),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("lsisk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)
}

// genActions builds a random transaction of n actions targeting n distinct keys
// (duplicate keys within one TransactWriteItems call are already rejected
// pre-phase, so the generator never produces them).
func genActions(rng *rand.Rand, n int) []diffAction {
	type kp struct{ pk, sk string }

	pks, sks := diffPKs(), diffSKs()
	all := make([]kp, 0, len(pks)*len(sks))
	for _, pk := range pks {
		for _, sk := range sks {
			all = append(all, kp{pk, sk})
		}
	}
	rng.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })

	if n > len(all) {
		n = len(all)
	}

	kinds := []string{"put", "update", "delete", "check"}
	actions := make([]diffAction, 0, n)

	for i := range n {
		a := diffAction{kind: kinds[rng.Intn(len(kinds))], pk: all[i].pk, sk: all[i].sk}

		switch a.kind {
		case "put":
			gsipks, gsisks, gsipk2s, lsisks := diffGSIPKs(), diffGSISKs(), diffGSIPK2s(), diffLSISKs()
			a.put = refItem{
				PK: a.pk, SK: a.sk,
				GSIPK:  gsipks[rng.Intn(len(gsipks))],
				GSISK:  gsisks[rng.Intn(len(gsisks))],
				GSIPK2: gsipk2s[rng.Intn(len(gsipk2s))],
				LSISK:  lsisks[rng.Intn(len(lsisks))],
				Val:    rng.Intn(10),
				HasVal: true,
			}
			if rng.Intn(2) == 0 {
				a.put.Flag, a.put.HasFlag = true, true
			}
			a.cond = condKind(rng.Intn(5))
		case "update":
			a.upd = updKind(rng.Intn(4))
			a.setValArg = rng.Intn(10)
			a.cond = condKind(rng.Intn(5))
		case "delete":
			a.cond = condKind(rng.Intn(5))
		case "check":
			a.cond = condKind(1 + rng.Intn(4)) // ConditionCheck always needs a condition
		}

		if a.cond != condNone {
			a.threshold = rng.Intn(10)
			a.rvoccf = rng.Intn(2) == 0
		}

		actions = append(actions, a)
	}

	return actions
}

// simulateTxn predicts one TransactWriteItems call's outcome against ref, mutating
// ref in place when the transaction is predicted to commit. It mirrors the real
// backend's two-phase contract: every condition is evaluated against the
// pre-transaction state first (order-independent, since keys are unique within one
// call), and only if none fail does the apply phase run, in order.
func simulateTxn(ref map[string]refItem, actions []diffAction) ([]diffReason, bool) {
	reasons := make([]diffReason, len(actions))
	canceled := false

	for i, a := range actions {
		if a.cond == condNone {
			continue
		}

		existing, ok := ref[refKey(a.pk, a.sk)]

		var existingPtr *refItem
		if ok {
			existingPtr = &existing
		}

		if !a.cond.eval(existingPtr, a.threshold) {
			canceled = true
			reasons[i].failed = true

			if a.rvoccf && existingPtr != nil {
				item := *existingPtr
				reasons[i].item = &item
			}
		}
	}

	if canceled {
		return reasons, true
	}

	for _, a := range actions {
		key := refKey(a.pk, a.sk)

		switch a.kind {
		case "put":
			ref[key] = a.put
		case "update":
			existing, ok := ref[key]

			var existingPtr *refItem
			if ok {
				existingPtr = &existing
			}

			ref[key] = a.upd.apply(existingPtr, a.pk, a.sk, a.setValArg)
		case "delete":
			delete(ref, key)
		case "check":
			// No write.
		}
	}

	return reasons, false
}

func buildTransactItem(a diffAction) types.TransactWriteItem {
	key := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: a.pk},
		"sk": &types.AttributeValueMemberS{Value: a.sk},
	}

	condExpr := a.cond.expr()

	var rvoccf types.ReturnValuesOnConditionCheckFailure
	if a.rvoccf {
		rvoccf = types.ReturnValuesOnConditionCheckFailureAllOld
	}

	condEAV := map[string]types.AttributeValue{}
	if a.cond == condValGT || a.cond == condValEQ {
		condEAV[":thresh"] = &types.AttributeValueMemberN{Value: strconv.Itoa(a.threshold)}
	}

	switch a.kind {
	case "put":
		return types.TransactWriteItem{Put: buildDiffPut(a, condExpr, condEAV, rvoccf)}
	case "update":
		return types.TransactWriteItem{Update: buildDiffUpdate(a, key, condExpr, condEAV, rvoccf)}
	case "delete":
		del := &types.Delete{TableName: aws.String(diffTable), Key: key, ReturnValuesOnConditionCheckFailure: rvoccf}
		if condExpr != "" {
			del.ConditionExpression = aws.String(condExpr)
			del.ExpressionAttributeValues = condEAV
		}

		return types.TransactWriteItem{Delete: del}
	default: // "check"
		return types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
			TableName:                           aws.String(diffTable),
			Key:                                 key,
			ConditionExpression:                 aws.String(condExpr),
			ExpressionAttributeValues:           condEAV,
			ReturnValuesOnConditionCheckFailure: rvoccf,
		}}
	}
}

func buildDiffPut(
	a diffAction,
	condExpr string,
	condEAV map[string]types.AttributeValue,
	rvoccf types.ReturnValuesOnConditionCheckFailure,
) *types.Put {
	item := map[string]types.AttributeValue{
		"pk":     &types.AttributeValueMemberS{Value: a.pk},
		"sk":     &types.AttributeValueMemberS{Value: a.sk},
		"gsipk":  &types.AttributeValueMemberS{Value: a.put.GSIPK},
		"gsisk":  &types.AttributeValueMemberS{Value: a.put.GSISK},
		"gsipk2": &types.AttributeValueMemberS{Value: a.put.GSIPK2},
		"lsisk":  &types.AttributeValueMemberS{Value: a.put.LSISK},
		"val":    &types.AttributeValueMemberN{Value: strconv.Itoa(a.put.Val)},
	}
	if a.put.HasFlag {
		item["flag"] = &types.AttributeValueMemberBOOL{Value: a.put.Flag}
	}

	put := &types.Put{TableName: aws.String(diffTable), Item: item, ReturnValuesOnConditionCheckFailure: rvoccf}
	if condExpr != "" {
		put.ConditionExpression = aws.String(condExpr)
		put.ExpressionAttributeValues = condEAV
	}

	return put
}

func buildDiffUpdate(
	a diffAction,
	key map[string]types.AttributeValue,
	condExpr string,
	condEAV map[string]types.AttributeValue,
	rvoccf types.ReturnValuesOnConditionCheckFailure,
) *types.Update {
	upd := &types.Update{
		TableName:                           aws.String(diffTable),
		Key:                                 key,
		UpdateExpression:                    aws.String(a.upd.expr()),
		ReturnValuesOnConditionCheckFailure: rvoccf,
	}

	eav := map[string]types.AttributeValue{}

	switch a.upd {
	case updSetVal:
		eav[":v"] = &types.AttributeValueMemberN{Value: strconv.Itoa(a.setValArg)}
	case updIncrVal:
		eav[":zero"] = &types.AttributeValueMemberN{Value: "0"}
		eav[":one"] = &types.AttributeValueMemberN{Value: "1"}
	case updSetFlag:
		eav[":true"] = &types.AttributeValueMemberBOOL{Value: true}
	case updRemoveFlag:
		// No values needed.
	}

	if condExpr != "" {
		upd.ConditionExpression = aws.String(condExpr)
		maps.Copy(eav, condEAV)
	}

	if len(eav) > 0 {
		upd.ExpressionAttributeValues = eav
	}

	return upd
}

func strAttr(item map[string]types.AttributeValue, name string) string {
	v, ok := item[name]
	if !ok {
		return ""
	}

	sv, ok := v.(*types.AttributeValueMemberS)
	if !ok {
		return ""
	}

	return sv.Value
}

func itemToRef(t *testing.T, item map[string]types.AttributeValue) refItem {
	t.Helper()

	ri := refItem{
		PK: strAttr(item, "pk"), SK: strAttr(item, "sk"),
		GSIPK: strAttr(item, "gsipk"), GSISK: strAttr(item, "gsisk"),
		GSIPK2: strAttr(item, "gsipk2"), LSISK: strAttr(item, "lsisk"),
	}

	if n, ok := item["val"]; ok {
		nv, ok2 := n.(*types.AttributeValueMemberN)
		require.True(t, ok2, "val attribute is not N")
		v, err := strconv.Atoi(nv.Value)
		require.NoError(t, err)
		ri.Val, ri.HasVal = v, true
	}

	if f, ok := item["flag"]; ok {
		bv, ok2 := f.(*types.AttributeValueMemberBOOL)
		require.True(t, ok2, "flag attribute is not BOOL")
		ri.Flag = bv.Value
		ri.HasFlag = true
	}

	return ri
}

// wireStr/wireToRef decode the wire-format map[string]any (e.g. {"S": "x"}) that
// CancellationReason.Item carries, as opposed to itemToRef's SDK AttributeValue form.
func wireStr(m map[string]any, name string) string {
	v, ok := m[name]
	if !ok {
		return ""
	}

	vm, ok := v.(map[string]any)
	if !ok {
		return ""
	}

	s, _ := vm["S"].(string)

	return s
}

func wireToRef(t *testing.T, m map[string]any) refItem {
	t.Helper()

	ri := refItem{
		PK: wireStr(m, "pk"), SK: wireStr(m, "sk"),
		GSIPK: wireStr(m, "gsipk"), GSISK: wireStr(m, "gsisk"),
		GSIPK2: wireStr(m, "gsipk2"), LSISK: wireStr(m, "lsisk"),
	}

	if v, ok := m["val"]; ok {
		vm, ok2 := v.(map[string]any)
		require.True(t, ok2)
		n, ok3 := vm["N"].(string)
		require.True(t, ok3)
		iv, err := strconv.Atoi(n)
		require.NoError(t, err)
		ri.Val, ri.HasVal = iv, true
	}

	if v, ok := m["flag"]; ok {
		vm, ok2 := v.(map[string]any)
		require.True(t, ok2)
		b, ok3 := vm["BOOL"].(bool)
		require.True(t, ok3)
		ri.Flag = b
		ri.HasFlag = true
	}

	return ri
}

func scanAllRef(t *testing.T, db *dynamodb.InMemoryDB) map[string]refItem {
	t.Helper()

	out, err := db.Scan(context.Background(), &sdk.ScanInput{TableName: aws.String(diffTable)})
	require.NoError(t, err)

	got := make(map[string]refItem, len(out.Items))
	for _, item := range out.Items {
		ri := itemToRef(t, item)
		got[refKey(ri.PK, ri.SK)] = ri
	}

	return got
}

// queryIndexRef runs a single-condition Query against indexName and decodes the
// results into a ref-shaped map, keyed like the reference model.
func queryIndexRef(
	t *testing.T,
	db *dynamodb.InMemoryDB,
	indexName, keyCondExpr, eavName, eavVal string,
) map[string]refItem {
	t.Helper()

	out, err := db.Query(context.Background(), &sdk.QueryInput{
		TableName:              aws.String(diffTable),
		IndexName:              aws.String(indexName),
		KeyConditionExpression: aws.String(keyCondExpr),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			eavName: &types.AttributeValueMemberS{Value: eavVal},
		},
	})
	require.NoError(t, err)

	got := make(map[string]refItem, len(out.Items))
	for _, item := range out.Items {
		ri := itemToRef(t, item)
		got[refKey(ri.PK, ri.SK)] = ri
	}

	return got
}

func compareIndexes(t *testing.T, db *dynamodb.InMemoryDB, ref map[string]refItem, label string) {
	t.Helper()

	for _, g := range diffGSIPKs() {
		want := map[string]refItem{}
		for k, item := range ref {
			if item.GSIPK == g {
				want[k] = item
			}
		}
		got := queryIndexRef(t, db, diffGSI1, "gsipk = :g", ":g", g)
		require.Equal(t, want, got, "%s: gsi1 gsipk=%s mismatch", label, g)
	}

	for _, h := range diffGSIPK2s() {
		want := map[string]refItem{}
		for k, item := range ref {
			if item.GSIPK2 == h {
				want[k] = item
			}
		}
		got := queryIndexRef(t, db, diffGSI2, "gsipk2 = :h", ":h", h)
		require.Equal(t, want, got, "%s: gsi2 gsipk2=%s mismatch", label, h)
	}

	for _, pk := range diffPKs() {
		want := map[string]refItem{}
		for k, item := range ref {
			if item.PK == pk && item.LSISK != "" {
				want[k] = item
			}
		}
		got := queryIndexRef(t, db, diffLSI1, "pk = :p", ":p", pk)
		require.Equal(t, want, got, "%s: lsi1 pk=%s mismatch", label, pk)
	}
}

// TestTransactWriteItems_Differential drives random TransactWriteItems sequences
// through the exported API only and checks every response/error, the resulting
// table (via Scan), and every secondary index (via Query) against a from-scratch
// reference model. Covers the gopherstack-wdapu prepare/commit split: since the
// prepare phase now decides success/failure entirely before any table is touched,
// a canceled transaction must leave the table exactly as the reference predicts.
func TestTransactWriteItems_Differential(t *testing.T) {
	t.Parallel()

	for seed := int64(1); seed <= 200; seed++ {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			db.SetDefaultRegion("us-east-1")
			t.Cleanup(db.Close)

			createDiffTable(t, db)

			rng := rand.New(rand.NewSource(seed))
			ref := map[string]refItem{}

			const numTxns = 40

			for txn := range numTxns {
				n := 1 + rng.Intn(5)
				actions := genActions(rng, n)

				items := make([]types.TransactWriteItem, len(actions))
				for i, a := range actions {
					items[i] = buildTransactItem(a)
				}

				wantReasons, wantCanceled := simulateTxn(ref, actions)

				_, err := db.TransactWriteItems(context.Background(), &sdk.TransactWriteItemsInput{
					TransactItems: items,
				})

				label := fmt.Sprintf("seed %d txn %d", seed, txn)
				checkTxnOutcome(t, err, wantCanceled, wantReasons, label)

				require.Equal(t, ref, scanAllRef(t, db), "%s: table mismatch", label)
				compareIndexes(t, db, ref, label)
			}
		})
	}
}

func checkTxnOutcome(t *testing.T, err error, wantCanceled bool, wantReasons []diffReason, label string) {
	t.Helper()

	if !wantCanceled {
		require.NoError(t, err, label)

		return
	}

	require.Error(t, err, label)

	var dbErr *dynamodb.Error
	require.ErrorAs(t, err, &dbErr, "%s: got %T: %v", label, err, err)
	require.Contains(t, dbErr.Type, "TransactionCanceledException", label)
	require.Len(t, dbErr.CancellationReasons, len(wantReasons), label)

	for i, r := range wantReasons {
		got := dbErr.CancellationReasons[i]

		if !r.failed {
			require.Equal(t, "None", got.Code, "%s action %d", label, i)

			continue
		}

		require.Equal(t, "ConditionalCheckFailed", got.Code, "%s action %d", label, i)

		if r.item == nil {
			require.Nil(t, got.Item, "%s action %d", label, i)

			continue
		}

		gotItem, ok := got.Item.(map[string]any)
		require.True(t, ok, "%s action %d: Item type %T", label, i, got.Item)
		require.Equal(t, *r.item, wireToRef(t, gotItem), "%s action %d item", label, i)
	}
}
