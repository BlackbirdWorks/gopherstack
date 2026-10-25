package appsync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
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
	Key       map[string]any `json:"k"`
	StartedAt int64          `json:"t"`
}

// ddbSync reads the base table of a versioned data source. A lastSync (delta
// table read) is rejected: delta-sync writes are not maintained.
func ddbSync(ctx context.Context, ddb DynamoDBBackend, cfg *DynamoDBDataSourceConfig, req map[string]any) (any, error) {
	if !cfg.Versioned {
		return nil, fmt.Errorf("%w: Sync requires a versioned DynamoDB data source", ErrValidation)
	}

	if _, has := req["lastSync"]; has && req["lastSync"] != nil {
		return nil, fmt.Errorf("%w: Sync with lastSync (delta table) is not supported", ErrUnsupportedDynamoDBOp)
	}

	limit := ddbSyncDefaultLimit
	if n := intOf(req["limit"]); n > 0 {
		limit = n
	}

	if limit > ddbSyncMaxLimit {
		return nil, fmt.Errorf("%w: Sync limit is at most %d", ErrValidation, ddbSyncMaxLimit)
	}

	q := &DDBQuery{
		Table:  cfg.TableName,
		Limit:  int32(limit),
		Names:  map[string]string{},
		Values: map[string]any{},
	}
	f := exprOf(req["filter"])
	q.Filter = f.Expression
	q.Names, q.Values = f.Names, f.Values

	startedAt := time.Now().UnixMilli()

	if tok, _ := req["nextToken"].(string); tok != "" {
		raw, err := base64.RawURLEncoding.DecodeString(tok)

		var st syncToken
		if err != nil || json.Unmarshal(raw, &st) != nil || len(st.Key) == 0 {
			return nil, fmt.Errorf("%w: invalid nextToken", ErrValidation)
		}

		q.StartKey, startedAt = st.Key, st.StartedAt
	}

	page, err := ddb.ScanRaw(ctx, q)
	if err != nil {
		return nil, err
	}

	items := make([]any, len(page.Items))
	for i, it := range page.Items {
		items[i] = ddbPlainItem(it)
	}

	var next any

	if len(page.LastKey) > 0 {
		b, _ := json.Marshal(syncToken{Key: page.LastKey, StartedAt: startedAt})
		next = base64.RawURLEncoding.EncodeToString(b)
	}

	return map[string]any{
		ddbResultItems: items, "nextToken": next, "scannedCount": page.ScannedCount, "startedAt": startedAt,
	}, nil
}
