package appsync

import (
	"context"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	ddbTransactMaxItems    = 100
	ddbSyncDefaultLimit    = 100
	ddbSyncMaxLimit        = 1000
	ddbCancelNone          = "None"
	ddbResultCancelReasons = "cancellationReasons"
)

func transactList(req map[string]any) ([]map[string]any, error) {
	raw, _ := req["transactItems"].([]any)
	if len(raw) == 0 || len(raw) > ddbTransactMaxItems {
		return nil, fmt.Errorf(
			"%w: %s requires 1 to %d transactItems",
			ErrValidation,
			req["operation"],
			ddbTransactMaxItems,
		)
	}

	out := make([]map[string]any, len(raw))

	for i, r := range raw {
		m := mapOf(r)
		if m == nil {
			return nil, fmt.Errorf("%w: transactItems[%d] must be an object", ErrValidation, i)
		}

		if t, _ := m["table"].(string); t == "" {
			return nil, fmt.Errorf("%w: transactItems[%d] requires table", ErrValidation, i)
		}

		out[i] = m
	}

	return out, nil
}

func ddbTransactGet(ctx context.Context, ddb DynamoDBBackend, req map[string]any) (any, error) {
	list, err := transactList(req)
	if err != nil {
		return nil, err
	}

	gets := make([]DDBTransactGet, len(list))
	for i, m := range list {
		gets[i].Table, _ = m["table"].(string)
		gets[i].Key = mapOf(m["key"])
	}

	items, err := ddb.TransactGetItemsRaw(ctx, gets)
	if err != nil {
		return nil, transactFailure(err, "items")
	}

	out := make([]any, len(items))

	for i, it := range items {
		if len(it) > 0 {
			out[i] = ddbPlainItem(it)
		}
	}

	return map[string]any{ddbResultItems: out, ddbResultCancelReasons: nil}, nil
}

func ddbTransactWrite(ctx context.Context, ddb DynamoDBBackend, req map[string]any) (any, error) {
	list, err := transactList(req)
	if err != nil {
		return nil, err
	}

	writes := make([]DDBTransactWrite, len(list))
	keys := make([]any, len(list))

	for i, m := range list {
		w := DDBTransactWrite{Key: mapOf(m["key"]), Condition: condOf(m), ReturnOnFailure: true}
		w.Table, _ = m["table"].(string)
		w.Operation, _ = m["operation"].(string)

		if rv, ok := mapOf(m["condition"])["returnValuesOnConditionCheckFailure"].(bool); ok {
			w.ReturnOnFailure = rv
		}

		switch w.Operation {
		case ddbOpPutItem:
			w.Item = ddbPutItem(m)
		case ddbOpUpdateItem:
			u := exprOf(m["update"])
			if u.Expression == "" {
				return nil, fmt.Errorf("%w: transactItems[%d] UpdateItem requires update.expression", ErrValidation, i)
			}

			w.Update = &u
		case ddbOpDeleteItem:
		case "ConditionCheck":
			if w.Condition == nil {
				return nil, fmt.Errorf("%w: transactItems[%d] ConditionCheck requires condition", ErrValidation, i)
			}
		default:
			return nil, fmt.Errorf("%w: transactItems[%d] operation %q", ErrUnsupportedDynamoDBOp, i, w.Operation)
		}

		writes[i], keys[i] = w, ddbPlainItem(w.Key)
	}

	if err = ddb.TransactWriteItemsRaw(ctx, writes); err != nil {
		return nil, transactFailure(err, "keys")
	}

	return map[string]any{"keys": keys, ddbResultCancelReasons: nil}, nil
}

// transactFailure attaches the documented {<field>: null, cancellationReasons}
// result to a cancelled transaction's error.
func transactFailure(err error, field string) error {
	de, ok := errors.AsType[*DynamoDBError](err)
	if !ok || len(de.Cancellations) == 0 {
		return err
	}

	reasons := make([]any, len(de.Cancellations))

	for i, c := range de.Cancellations {
		r := map[string]any{"type": c.Type, "message": c.Message}
		if c.Type == "" {
			r["type"], r["message"] = ddbCancelNone, ddbCancelNone
		}

		if len(c.Item) > 0 {
			r["item"] = ddbPlainItem(c.Item)
		}

		reasons[i] = r
	}

	de.Result = map[string]any{field: nil, ddbResultCancelReasons: reasons}

	return de
}

type syncToken struct {
	Key       map[string]any `json:"k,omitempty"`
	Day       string         `json:"d,omitempty"`
	StartedAt int64          `json:"t"`
}

// decodeSyncToken returns the zero token when the request has none.
func decodeSyncToken(req map[string]any) (syncToken, error) {
	var st syncToken

	tok, _ := req["nextToken"].(string)
	if tok == "" {
		return st, nil
	}

	raw, err := base64.RawURLEncoding.DecodeString(tok)

	if err != nil || json.Unmarshal(raw, &st, jsonx.V1()) != nil || (len(st.Key) == 0 && st.Day == "") {
		return syncToken{}, fmt.Errorf("%w: invalid nextToken", ErrValidation)
	}

	return st, nil
}

func encodeSyncToken(st syncToken) string {
	b, _ := json.Marshal(st, jsonx.V1())

	return base64.RawURLEncoding.EncodeToString(b)
}

// ddbSync reads a versioned data source: the base table, or, when lastSync is within the delta table's
// retention, the delta table (AppSync developer guide, "Using DynamoDB sync operations on versioned data sources").
func ddbSync(ctx context.Context, vs versionedSource, req map[string]any) (any, error) {
	limit := ddbSyncDefaultLimit
	if n := intOf(req["limit"]); n > 0 {
		limit = n
	}

	if limit > ddbSyncMaxLimit {
		return nil, fmt.Errorf("%w: Sync limit is at most %d", ErrValidation, ddbSyncMaxLimit)
	}

	if req["basePartitionKey"] != nil || req["deltaIndexName"] != nil {
		return nil, fmt.Errorf("%w: Sync on a custom partition key delta layout", ErrUnsupportedDynamoDBOp)
	}

	st, err := decodeSyncToken(req)
	if err != nil {
		return nil, err
	}

	startedAt := time.Now().UnixMilli()
	if st.StartedAt != 0 {
		startedAt = st.StartedAt
	}

	if last := int64Of(req["lastSync"]); req["lastSync"] != nil && vs.deltaReadable(last, startedAt) {
		return ddbSyncDelta(ctx, vs, req, last, limit, startedAt, st)
	}

	return ddbSyncBase(ctx, vs, req, limit, startedAt, st)
}

func (vs versionedSource) deltaReadable(lastSync, now int64) bool {
	if _, ok := vs.delta(); !ok {
		return false
	}

	return now-lastSync <= vs.deltaTTLMinutes()*millisPerMinute
}

func ddbSyncBase(
	ctx context.Context, vs versionedSource, req map[string]any, limit int, startedAt int64, st syncToken,
) (any, error) {
	q := &DDBQuery{
		Table:  vs.cfg.TableName,
		Limit:  int32Of(limit),
		Names:  map[string]string{},
		Values: map[string]any{},
	}
	f := exprOf(req["filter"])
	q.Filter = f.Expression
	q.Names, q.Values = f.Names, f.Values

	q.StartKey = st.Key

	page, err := vs.ddb.ScanRaw(ctx, q)
	if err != nil {
		return nil, err
	}

	var next any
	if len(page.LastKey) > 0 {
		next = encodeSyncToken(syncToken{Key: page.LastKey, StartedAt: startedAt})
	}

	return syncResult(page.Items, next, page.ScannedCount, startedAt), nil
}

func syncResult(rows []map[string]any, next any, scanned int, startedAt int64) map[string]any {
	items := make([]any, len(rows))
	for i, it := range rows {
		items[i] = ddbPlainItem(it)
	}

	return map[string]any{
		ddbResultItems: items, "nextToken": next, "scannedCount": scanned, "startedAt": startedAt,
	}
}

// deltaDays lists the UTC dates from lastSync through startedAt, the delta partitions a sync must read.
func deltaDays(lastSync, startedAt int64) []string {
	from := time.UnixMilli(lastSync).UTC().Truncate(deltaPartitionSpan)
	to := time.UnixMilli(startedAt).UTC()

	var days []string

	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		days = append(days, d.Format(deltaDayLayout))
	}

	return days
}

func ddbSyncDelta(
	ctx context.Context, vs versionedSource, req map[string]any, lastSync int64, limit int, startedAt int64,
	st syncToken,
) (any, error) {
	table, _ := vs.delta()
	days := deltaDays(lastSync, startedAt)

	startKey := st.Key

	if i := indexOf(days, st.Day); i >= 0 {
		days = days[i:]
	}

	var (
		rows    []map[string]any
		scanned int
	)

	for i, day := range days {
		q := vs.deltaQuery(table, day, req, lastSync, limit-len(rows), startKey)

		page, err := vs.ddb.QueryRaw(ctx, q)
		if err != nil {
			return nil, err
		}

		rows, scanned, startKey = append(rows, page.Items...), scanned+page.ScannedCount, nil

		switch {
		case len(page.LastKey) > 0:
			return syncResult(rows, encodeSyncToken(syncToken{Key: page.LastKey, Day: day, StartedAt: startedAt}),
				scanned, startedAt), nil
		case len(rows) >= limit && i+1 < len(days):
			return syncResult(rows, encodeSyncToken(syncToken{Day: days[i+1], StartedAt: startedAt}),
				scanned, startedAt), nil
		}
	}

	return syncResult(rows, nil, scanned, startedAt), nil
}

func indexOf(list []string, v string) int {
	for i, s := range list {
		if s == v {
			return i
		}
	}

	return -1
}

func (vs versionedSource) deltaQuery(
	table, day string, req map[string]any, lastSync int64, limit int, startKey map[string]any,
) *DDBQuery {
	f := exprOf(req["filter"])
	names := map[string]string{nameChanged: attrLastChanged}
	values := map[string]any{":__pk": strAttr(vs.dsName + ":" + day), ":__ls": numAttr(lastSync)}

	maps.Copy(names, f.Names)
	maps.Copy(values, f.Values)

	filter := nameChanged + " > :__ls"
	if f.Expression != "" {
		filter = "(" + f.Expression + ") AND " + filter
	}

	return &DDBQuery{
		Table: table, KeyCondition: attrDeltaPK + " = :__pk", Filter: filter,
		Names: names, Values: values, Limit: int32Of(limit), StartKey: startKey,
	}
}
