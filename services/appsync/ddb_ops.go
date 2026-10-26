package appsync

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
)

const (
	ddbDefaultLimit       = 100
	ddbCheckFailedCode    = "ConditionalCheckFailedException"
	ddbStrategyReject     = "Reject"
	ddbKeyTables          = "tables"
	ddbKeyExpression      = "expression"
	ddbKeyExprNames       = "expressionNames"
	ddbKeyExprValues      = "expressionValues"
	ddbResultItems        = "items"
	ddbResultData         = "data"
	ddbResultUnprocKeys   = "unprocessedKeys"
	ddbResultUnprocItems  = "unprocessedItems"
	ddbOpGetItem          = "GetItem"
	ddbOpPutItem          = "PutItem"
	ddbOpUpdateItem       = "UpdateItem"
	ddbOpDeleteItem       = "DeleteItem"
	ddbOpQuery            = "Query"
	ddbOpScan             = "Scan"
	ddbOpBatchGetItem     = "BatchGetItem"
	ddbOpBatchPutItem     = "BatchPutItem"
	ddbOpBatchDeleteItem  = "BatchDeleteItem"
	ddbOpSync             = "Sync"
	ddbOpTransactGet      = "TransactGetItems"
	ddbOpTransactWrite    = "TransactWriteItems"
	ddbBatchGetMaxKeys    = 100
	ddbBatchWriteMaxItems = 25
)

func isCondFailed(err error) bool {
	de, ok := errors.AsType[*DynamoDBError](err)

	return ok && de.Code == ddbCheckFailedCode
}

// executeDDBRequest runs one AppSync DynamoDB request object against ddb and
// returns the plain-JSON result AppSync exposes as $ctx.result.
func executeDDBRequest(
	ctx context.Context, ddb DynamoDBBackend, cfg *DynamoDBDataSourceConfig, req map[string]any,
) (any, error) {
	operation, _ := req["operation"].(string)
	table := cfg.TableName

	switch operation {
	case ddbOpGetItem:
		item, err := ddb.GetItemRaw(ctx, table, mapOf(req["key"]))
		if err != nil || len(item) == 0 {
			return nil, err
		}

		return ddbPlainItem(item), nil
	case ddbOpPutItem:
		return ddbPut(ctx, ddb, table, req)
	case ddbOpUpdateItem:
		return ddbUpdate(ctx, ddb, table, req)
	case ddbOpDeleteItem:
		return ddbDelete(ctx, ddb, table, req)
	case ddbOpQuery, ddbOpScan:
		return ddbQueryScan(ctx, ddb, table, req, operation == ddbOpScan)
	case ddbOpBatchGetItem:
		return ddbBatchGet(ctx, ddb, req)
	case ddbOpBatchPutItem, ddbOpBatchDeleteItem:
		return ddbBatchWrite(ctx, ddb, req, operation == ddbOpBatchPutItem)
	case ddbOpSync:
		return ddbSync(ctx, ddb, cfg, req)
	case ddbOpTransactGet:
		return ddbTransactGet(ctx, ddb, req)
	case ddbOpTransactWrite:
		return ddbTransactWrite(ctx, ddb, req)
	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedDynamoDBOp, operation)
	}
}

func mapOf(v any) map[string]any {
	m, _ := v.(map[string]any)

	return m
}

// exprOf decodes an {expression, expressionNames, expressionValues} object.
func exprOf(v any) DDBExpression {
	m := mapOf(v)
	expr, _ := m[ddbKeyExpression].(string)
	out := DDBExpression{Expression: expr, Values: mapOf(m[ddbKeyExprValues])}

	if names := mapOf(m[ddbKeyExprNames]); len(names) > 0 {
		out.Names = make(map[string]string, len(names))
		for k, n := range names {
			out.Names[k], _ = n.(string)
		}
	}

	return out
}

// condOf returns the request's condition, or nil when it has no expression.
func condOf(req map[string]any) *DDBExpression {
	c := exprOf(req["condition"])
	if c.Expression == "" {
		return nil
	}

	return &c
}

// checkStrategy rejects a conditionalCheckFailedHandler other than Reject.
func checkStrategy(req map[string]any) error {
	h := mapOf(mapOf(req["condition"])["conditionalCheckFailedHandler"])

	if s, _ := h["strategy"].(string); s != "" && s != ddbStrategyReject {
		return fmt.Errorf("%w: conditionalCheckFailedHandler strategy %s", ErrUnsupportedDynamoDBOp, s)
	}

	return nil
}

func ddbPut(ctx context.Context, ddb DynamoDBBackend, table string, req map[string]any) (any, error) {
	item := ddbPutItem(req)
	cond := condOf(req)

	err := ddb.PutItemRaw(ctx, table, item, cond)
	if err == nil {
		return ddbPlainItem(item), nil
	}

	if cond == nil || !isCondFailed(err) {
		return nil, err
	}

	if sErr := checkStrategy(req); sErr != nil {
		return nil, sErr
	}

	current, gErr := ddb.GetItemRaw(ctx, table, mapOf(req["key"]))
	if gErr == nil && len(current) > 0 && sameItem(current, item, ignoreList(req)) {
		return ddbPlainItem(current), nil
	}

	return nil, err
}

func ignoreList(req map[string]any) []string {
	list, _ := mapOf(req["condition"])["equalsIgnore"].([]any)
	out := make([]string, 0, len(list))

	for _, v := range list {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

func sameItem(a, b map[string]any, ignore []string) bool {
	a, b = maps.Clone(a), maps.Clone(b)

	for _, k := range ignore {
		delete(a, k)
		delete(b, k)
	}

	return reflect.DeepEqual(a, b)
}

func ddbUpdate(ctx context.Context, ddb DynamoDBBackend, table string, req map[string]any) (any, error) {
	update := exprOf(req["update"])
	if update.Expression == "" {
		return nil, fmt.Errorf("%w: UpdateItem requires update.expression", ErrValidation)
	}

	item, err := ddb.UpdateItemRaw(ctx, table, mapOf(req["key"]), update, condOf(req))
	if err != nil {
		return nil, checkFailedErr(req, err)
	}

	return ddbPlainItem(item), nil
}

func ddbDelete(ctx context.Context, ddb DynamoDBBackend, table string, req map[string]any) (any, error) {
	key := mapOf(req["key"])

	var missing any

	item, err := ddb.DeleteItemRaw(ctx, table, key, condOf(req))
	if err != nil {
		if !isCondFailed(err) {
			return nil, err
		}

		if sErr := checkStrategy(req); sErr != nil {
			return nil, sErr
		}

		if current, gErr := ddb.GetItemRaw(ctx, table, key); gErr == nil && len(current) == 0 {
			return missing, nil
		}

		return nil, err
	}

	if len(item) == 0 {
		return missing, nil
	}

	return ddbPlainItem(item), nil
}

func checkFailedErr(req map[string]any, err error) error {
	if isCondFailed(err) {
		if sErr := checkStrategy(req); sErr != nil {
			return sErr
		}
	}

	return err
}

func intOf(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case json.Number:
		i, _ := n.Int64()

		return int(i)
	default:
		return 0
	}
}

func ddbQueryScan(
	ctx context.Context, ddb DynamoDBBackend, table string, req map[string]any, scan bool,
) (any, error) {
	q := &DDBQuery{Table: table, Limit: ddbDefaultLimit, Descending: false}
	q.Index, _ = req["index"].(string)
	q.Consistent, _ = req["consistentRead"].(bool)

	if n := intOf(req["limit"]); n > 0 {
		q.Limit = int32(n) //nolint:gosec // bounded by request
	}

	if fwd, ok := req["scanIndexForward"].(bool); ok {
		q.Descending = !fwd
	}

	q.Names, q.Values = map[string]string{}, map[string]any{}

	if !scan {
		kc := exprOf(req["query"])
		q.KeyCondition = kc.Expression
		maps.Copy(q.Names, kc.Names)
		maps.Copy(q.Values, kc.Values)
	}

	f := exprOf(req["filter"])
	q.Filter = f.Expression
	maps.Copy(q.Names, f.Names)
	maps.Copy(q.Values, f.Values)

	if tok, _ := req["nextToken"].(string); tok != "" {
		start, err := decodeNextToken(tok)
		if err != nil {
			return nil, err
		}

		q.StartKey = start
	}

	run := ddb.QueryRaw
	if scan {
		run = ddb.ScanRaw
	}

	page, err := run(ctx, q)
	if err != nil {
		return nil, err
	}

	items := make([]any, len(page.Items))
	for i, it := range page.Items {
		items[i] = ddbPlainItem(it)
	}

	var next any
	if len(page.LastKey) > 0 {
		next = encodeNextToken(page.LastKey)
	}

	return map[string]any{ddbResultItems: items, "nextToken": next, "scannedCount": page.ScannedCount}, nil
}

func encodeNextToken(key map[string]any) string {
	b, _ := json.Marshal(key)

	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeNextToken(tok string) (map[string]any, error) {
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid nextToken", ErrValidation)
	}

	var key map[string]any
	if err = json.Unmarshal(raw, &key); err != nil || len(key) == 0 {
		return nil, fmt.Errorf("%w: invalid nextToken", ErrValidation)
	}

	return key, nil
}

// batchTables decodes the "tables" object of a batch request; each table maps
// to a list (the key/item list) or, for BatchGetItem, to {"keys": [...]}.
func batchTables(req map[string]any, limit int) (map[string][]map[string]any, error) {
	tables := mapOf(req[ddbKeyTables])
	if len(tables) == 0 {
		return nil, fmt.Errorf("%w: %s requires at least one table", ErrValidation, req["operation"])
	}

	out := make(map[string][]map[string]any, len(tables))
	total := 0

	for name, v := range tables {
		list, ok := v.([]any)
		if !ok {
			list, _ = mapOf(v)["keys"].([]any)
		}

		for _, e := range list {
			out[name] = append(out[name], mapOf(e))
		}

		if _, present := out[name]; !present {
			out[name] = []map[string]any{}
		}

		total += len(list)
	}

	if total > limit {
		return nil, fmt.Errorf("%w: %s is limited to %d entries", ErrValidation, req["operation"], limit)
	}

	return out, nil
}

func ddbBatchGet(ctx context.Context, ddb DynamoDBBackend, req map[string]any) (any, error) {
	keys, err := batchTables(req, ddbBatchGetMaxKeys)
	if err != nil {
		return nil, err
	}

	found, err := ddb.BatchGetItemRaw(ctx, keys)
	if err != nil {
		return nil, err
	}

	data, unprocessed := map[string]any{}, map[string]any{}

	for table, tableKeys := range keys {
		list := make([]any, len(tableKeys))

		for i, k := range tableKeys {
			for _, it := range found[table] {
				if hasKey(it, k) {
					list[i] = ddbPlainItem(it)

					break
				}
			}
		}

		data[table] = list
		unprocessed[table] = []any{}
	}

	return map[string]any{ddbResultData: data, ddbResultUnprocKeys: unprocessed}, nil
}

func hasKey(item, key map[string]any) bool {
	for k, v := range key {
		if !reflect.DeepEqual(item[k], v) {
			return false
		}
	}

	return true
}

func ddbBatchWrite(ctx context.Context, ddb DynamoDBBackend, req map[string]any, put bool) (any, error) {
	entries, err := batchTables(req, ddbBatchWriteMaxItems)
	if err != nil {
		return nil, err
	}

	writes := make(map[string][]DDBWrite, len(entries))
	data, unprocessed := map[string]any{}, map[string]any{}

	for table, list := range entries {
		out := make([]any, len(list))

		for i, e := range list {
			if put {
				writes[table] = append(writes[table], DDBWrite{Item: e})
			} else {
				writes[table] = append(writes[table], DDBWrite{Key: e})
			}

			out[i] = ddbPlainItem(e)
		}

		data[table] = out
		unprocessed[table] = []any{}
	}

	if err = ddb.BatchWriteItemRaw(ctx, writes); err != nil {
		return nil, err
	}

	return map[string]any{ddbResultData: data, ddbResultUnprocItems: unprocessed}, nil
}
