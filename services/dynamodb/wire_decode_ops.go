package dynamodb

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

// Bit flags identifying which top-level request fields were present.
const (
	fTableName uint32 = 1 << iota
	fKey
	fItem
	fNames
	fValues
	fCondExpr
	fUpdExpr
	fFilterExpr
	fProjExpr
	fKeyCondExpr
	fIndexName
	fSelect
	fReturnValues
	fRCC
	fRICM
	fRVOCCF
	fCondOp
	fStartKey
	fAttrsToGet
	fConsistent
	fScanForward
	fLimit
	fSegment
	fTotalSegments
)

const (
	wireReturnConsumedCapacity = "ReturnConsumedCapacity"
	writeRequestHint           = 8
)

const (
	putFields = fTableName | fItem | fNames | fValues | fCondExpr | fReturnValues | fRCC | fRICM | fRVOCCF | fCondOp
	delFields = fTableName | fKey | fNames | fValues | fCondExpr | fReturnValues | fRCC | fRICM | fRVOCCF | fCondOp
	updFields = delFields | fUpdExpr
	getFields = fTableName | fKey | fNames | fProjExpr | fRCC | fAttrsToGet | fConsistent
	qryFields = fTableName | fNames | fValues | fKeyCondExpr | fFilterExpr | fProjExpr | fIndexName | fSelect |
		fRCC | fCondOp | fStartKey | fAttrsToGet | fConsistent | fScanForward | fLimit
	scnFields = fTableName | fNames | fValues | fFilterExpr | fProjExpr | fIndexName | fSelect | fRCC | fCondOp |
		fStartKey | fAttrsToGet | fConsistent | fLimit | fSegment | fTotalSegments
)

// itemReq is the superset of top-level fields shared by the single-item, Query and Scan requests.
type itemReq struct {
	key, item, startKey map[string]types.AttributeValue
	values              map[string]types.AttributeValue
	names               map[string]string
	consistent          *bool
	scanForward         *bool
	limit               *int32
	segment             *int32
	totalSegments       *int32
	tableName           string
	condExpr            string
	updExpr             string
	filterExpr          string
	projExpr            string
	keyCondExpr         string
	indexName           string
	selectVal           string
	returnValues        string
	rcc                 string
	ricm                string
	rvoccf              string
	condOp              string
	attrsToGet          []string
	seen                uint32
}

// parseItemReq decodes body into an itemReq, allowing only the fields in allowed.
func parseItemReq(body []byte, allowed uint32) (*itemReq, bool) {
	req := &itemReq{}
	r := wireReader{b: body}

	ok := r.members(func(key []byte, escaped bool) bool {
		bit := itemFieldBit(key)
		if escaped || bit == 0 || bit&allowed == 0 || req.seen&bit != 0 {
			return false
		}

		req.seen |= bit

		return req.readField(&r, bit)
	})

	return req, ok && r.atEnd()
}

//nolint:gochecknoglobals // immutable request-field lookup
var itemFieldBits = map[string]uint32{
	wireTableName:                         fTableName,
	"Key":                                 fKey,
	"Item":                                fItem,
	"ExpressionAttributeNames":            fNames,
	"ExpressionAttributeValues":           fValues,
	"ConditionExpression":                 fCondExpr,
	"UpdateExpression":                    fUpdExpr,
	"FilterExpression":                    fFilterExpr,
	"ProjectionExpression":                fProjExpr,
	"KeyConditionExpression":              fKeyCondExpr,
	"IndexName":                           fIndexName,
	"Select":                              fSelect,
	"ReturnValues":                        fReturnValues,
	wireReturnConsumedCapacity:            fRCC,
	"ReturnItemCollectionMetrics":         fRICM,
	"ReturnValuesOnConditionCheckFailure": fRVOCCF,
	"ConditionalOperator":                 fCondOp,
	"ExclusiveStartKey":                   fStartKey,
	"AttributesToGet":                     fAttrsToGet,
	"ConsistentRead":                      fConsistent,
	"ScanIndexForward":                    fScanForward,
	"Limit":                               fLimit,
	"Segment":                             fSegment,
	"TotalSegments":                       fTotalSegments,
}

func itemFieldBit(key []byte) uint32 { return itemFieldBits[string(key)] }

func (q *itemReq) readField(r *wireReader, bit uint32) bool {
	var ok bool

	switch bit {
	case fKey:
		q.key, ok = r.attrMap(0)
	case fItem:
		q.item, ok = r.attrMap(0)
	case fValues:
		q.values, ok = r.attrMap(0)
	case fStartKey:
		q.startKey, ok = r.attrMap(0)
	case fNames:
		q.names, ok = r.stringMap()
	case fAttrsToGet:
		q.attrsToGet, ok = r.stringList()
	case fConsistent:
		q.consistent, ok = r.boolPtr()
	case fScanForward:
		q.scanForward, ok = r.boolPtr()
	case fLimit:
		q.limit, ok = r.int32Ptr()
	case fSegment:
		q.segment, ok = r.int32Ptr()
	case fTotalSegments:
		q.totalSegments, ok = r.int32Ptr()
	default:
		return q.readStringField(r, bit)
	}

	return ok
}

func (q *itemReq) readStringField(r *wireReader, bit uint32) bool {
	var dst *string

	switch bit {
	case fTableName:
		dst = &q.tableName
	case fCondExpr:
		dst = &q.condExpr
	case fUpdExpr:
		dst = &q.updExpr
	case fFilterExpr:
		dst = &q.filterExpr
	case fProjExpr:
		dst = &q.projExpr
	case fKeyCondExpr:
		dst = &q.keyCondExpr
	case fIndexName:
		dst = &q.indexName
	case fSelect:
		dst = &q.selectVal
	case fReturnValues:
		dst = &q.returnValues
	case fRCC:
		dst = &q.rcc
	case fRICM:
		dst = &q.ricm
	case fRVOCCF:
		dst = &q.rvoccf
	case fCondOp:
		dst = &q.condOp
	default:
		return false
	}

	s, ok := r.str()
	*dst = s

	return ok
}

func (r *wireReader) boolPtr() (*bool, bool) {
	b, ok := r.boolean()

	return &b, ok
}

func (r *wireReader) int32Ptr() (*int32, bool) {
	v, ok := r.int32Val()

	return &v, ok
}

func nonNilItem(m map[string]types.AttributeValue) map[string]types.AttributeValue {
	if m == nil {
		return map[string]types.AttributeValue{}
	}

	return m
}

func nonEmptyItem(m map[string]types.AttributeValue) map[string]types.AttributeValue {
	if len(m) == 0 {
		return nil
	}

	return m
}

func decodePutItem(body []byte) (*dynamodb.PutItemInput, bool) {
	q, ok := parseItemReq(body, putFields)
	if !ok {
		return nil, false
	}

	return &dynamodb.PutItemInput{
		TableName:                           &q.tableName,
		Item:                                nonNilItem(q.item),
		ConditionExpression:                 ptrconv.NilIfEmpty(q.condExpr),
		ExpressionAttributeNames:            q.names,
		ExpressionAttributeValues:           nonEmptyItem(q.values),
		ReturnValues:                        types.ReturnValue(q.returnValues),
		ReturnConsumedCapacity:              types.ReturnConsumedCapacity(q.rcc),
		ReturnItemCollectionMetrics:         types.ReturnItemCollectionMetrics(q.ricm),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailure(q.rvoccf),
		ConditionalOperator:                 types.ConditionalOperator(q.condOp),
	}, true
}

func decodeDeleteItem(body []byte) (*dynamodb.DeleteItemInput, bool) {
	q, ok := parseItemReq(body, delFields)
	if !ok {
		return nil, false
	}

	return &dynamodb.DeleteItemInput{
		TableName:                           &q.tableName,
		Key:                                 nonNilItem(q.key),
		ConditionExpression:                 ptrconv.NilIfEmpty(q.condExpr),
		ExpressionAttributeNames:            q.names,
		ExpressionAttributeValues:           nonEmptyItem(q.values),
		ReturnValues:                        types.ReturnValue(q.returnValues),
		ReturnConsumedCapacity:              types.ReturnConsumedCapacity(q.rcc),
		ReturnItemCollectionMetrics:         types.ReturnItemCollectionMetrics(q.ricm),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailure(q.rvoccf),
		ConditionalOperator:                 types.ConditionalOperator(q.condOp),
	}, true
}

func decodeUpdateItem(body []byte) (*dynamodb.UpdateItemInput, bool) {
	q, ok := parseItemReq(body, updFields)
	if !ok {
		return nil, false
	}

	return &dynamodb.UpdateItemInput{
		TableName:                           &q.tableName,
		Key:                                 nonNilItem(q.key),
		UpdateExpression:                    ptrconv.NilIfEmpty(q.updExpr),
		ConditionExpression:                 ptrconv.NilIfEmpty(q.condExpr),
		ExpressionAttributeNames:            q.names,
		ExpressionAttributeValues:           nonEmptyItem(q.values),
		ReturnValues:                        types.ReturnValue(q.returnValues),
		ReturnConsumedCapacity:              types.ReturnConsumedCapacity(q.rcc),
		ReturnItemCollectionMetrics:         types.ReturnItemCollectionMetrics(q.ricm),
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailure(q.rvoccf),
		ConditionalOperator:                 types.ConditionalOperator(q.condOp),
	}, true
}

func decodeGetItem(body []byte) (*dynamodb.GetItemInput, bool) {
	q, ok := parseItemReq(body, getFields)
	if !ok {
		return nil, false
	}

	return &dynamodb.GetItemInput{
		TableName:                &q.tableName,
		Key:                      nonNilItem(q.key),
		ExpressionAttributeNames: q.names,
		ProjectionExpression:     ptrconv.NilIfEmpty(q.projExpr),
		AttributesToGet:          q.attrsToGet,
		ConsistentRead:           q.consistent,
		ReturnConsumedCapacity:   types.ReturnConsumedCapacity(q.rcc),
	}, true
}

func decodeQuery(body []byte) (*dynamodb.QueryInput, bool) {
	q, ok := parseItemReq(body, qryFields)
	if !ok {
		return nil, false
	}

	out := &dynamodb.QueryInput{
		TableName:                 &q.tableName,
		IndexName:                 ptrconv.NilIfEmpty(q.indexName),
		KeyConditionExpression:    ptrconv.NilIfEmpty(q.keyCondExpr),
		FilterExpression:          ptrconv.NilIfEmpty(q.filterExpr),
		ProjectionExpression:      ptrconv.NilIfEmpty(q.projExpr),
		AttributesToGet:           q.attrsToGet,
		ExpressionAttributeNames:  q.names,
		ExpressionAttributeValues: nonEmptyItem(q.values),
		ExclusiveStartKey:         nonEmptyItem(q.startKey),
		ScanIndexForward:          q.scanForward,
		ReturnConsumedCapacity:    types.ReturnConsumedCapacity(q.rcc),
		Select:                    types.Select(q.selectVal),
		ConditionalOperator:       types.ConditionalOperator(q.condOp),
	}

	if q.limit != nil && *q.limit > 0 {
		out.Limit = q.limit
	}

	if q.consistent != nil && *q.consistent {
		out.ConsistentRead = q.consistent
	}

	return out, true
}

func decodeScan(body []byte) (*dynamodb.ScanInput, bool) {
	q, ok := parseItemReq(body, scnFields)
	if !ok {
		return nil, false
	}

	return &dynamodb.ScanInput{
		TableName:                 &q.tableName,
		IndexName:                 ptrconv.NilIfEmpty(q.indexName),
		FilterExpression:          ptrconv.NilIfEmpty(q.filterExpr),
		ProjectionExpression:      ptrconv.NilIfEmpty(q.projExpr),
		AttributesToGet:           q.attrsToGet,
		ExpressionAttributeNames:  q.names,
		ExpressionAttributeValues: nonEmptyItem(q.values),
		ExclusiveStartKey:         nonEmptyItem(q.startKey),
		Limit:                     q.limit,
		Segment:                   q.segment,
		TotalSegments:             q.totalSegments,
		ConsistentRead:            q.consistent,
		ReturnConsumedCapacity:    types.ReturnConsumedCapacity(q.rcc),
		Select:                    types.Select(q.selectVal),
		ConditionalOperator:       types.ConditionalOperator(q.condOp),
	}, true
}

// parseTop decodes a top-level object whose members are handled by field.
func parseTop(body []byte, field func(r *wireReader, key []byte) bool) bool {
	r := wireReader{b: body}

	ok := r.members(func(key []byte, escaped bool) bool {
		return !escaped && field(&r, key)
	})

	return ok && r.atEnd()
}

func decodeBatchWriteItem(body []byte) (*dynamodb.BatchWriteItemInput, bool) {
	out := &dynamodb.BatchWriteItemInput{RequestItems: make(map[string][]types.WriteRequest)}
	seen := uint32(0)

	ok := parseTop(body, func(r *wireReader, key []byte) bool {
		var bit uint32

		switch string(key) {
		case "RequestItems":
			bit = fItem
		case wireReturnConsumedCapacity:
			bit = fRCC
		case "ReturnItemCollectionMetrics":
			bit = fRICM
		default:
			return false
		}

		if seen&bit != 0 {
			return false
		}

		seen |= bit

		switch bit {
		case fItem:
			return r.writeRequestsByTable(out.RequestItems)
		case fRCC:
			s, good := r.str()
			out.ReturnConsumedCapacity = types.ReturnConsumedCapacity(s)

			return good
		default:
			s, good := r.str()
			out.ReturnItemCollectionMetrics = types.ReturnItemCollectionMetrics(s)

			return good
		}
	})

	return out, ok
}

func (r *wireReader) writeRequestsByTable(dst map[string][]types.WriteRequest) bool {
	return r.members(func(key []byte, escaped bool) bool {
		table, good := r.keyString(key, escaped)
		if !good {
			return false
		}

		reqs := make([]types.WriteRequest, 0, writeRequestHint)

		good = r.elements(func() bool {
			wr, okReq := r.writeRequest()
			reqs = append(reqs, wr)

			return okReq
		})
		dst[table] = reqs

		return good
	})
}

func (r *wireReader) writeRequest() (types.WriteRequest, bool) {
	var wr types.WriteRequest

	seenPut, seenDel := false, false

	ok := r.members(func(key []byte, escaped bool) bool {
		if escaped {
			return false
		}

		switch string(key) {
		case "PutRequest":
			if seenPut {
				return false
			}

			seenPut = true

			item, good := r.singleItemObject("Item")
			wr.PutRequest = &types.PutRequest{Item: item}

			return good
		case "DeleteRequest":
			if seenDel {
				return false
			}

			seenDel = true

			k, good := r.singleItemObject("Key")
			wr.DeleteRequest = &types.DeleteRequest{Key: k}

			return good
		default:
			return false
		}
	})

	return wr, ok
}

// singleItemObject decodes {"<field>": {attrs}} and returns the (non-nil) attribute map.
func (r *wireReader) singleItemObject(field string) (map[string]types.AttributeValue, bool) {
	var item map[string]types.AttributeValue

	seen := false

	ok := r.members(func(key []byte, escaped bool) bool {
		if escaped || seen || string(key) != field {
			return false
		}

		seen = true

		var good bool

		item, good = r.attrMap(0)

		return good
	})

	return nonNilItem(item), ok
}

func decodeBatchGetItem(body []byte) (*dynamodb.BatchGetItemInput, bool) {
	out := &dynamodb.BatchGetItemInput{RequestItems: make(map[string]types.KeysAndAttributes)}
	seenReq, seenRCC := false, false

	ok := parseTop(body, func(r *wireReader, key []byte) bool {
		switch string(key) {
		case "RequestItems":
			if seenReq {
				return false
			}

			seenReq = true

			return r.keysByTable(out.RequestItems)
		case wireReturnConsumedCapacity:
			if seenRCC {
				return false
			}

			seenRCC = true
			s, good := r.str()
			out.ReturnConsumedCapacity = types.ReturnConsumedCapacity(s)

			return good
		default:
			return false
		}
	})

	return out, ok
}

func (r *wireReader) keysByTable(dst map[string]types.KeysAndAttributes) bool {
	return r.members(func(key []byte, escaped bool) bool {
		table, good := r.keyString(key, escaped)
		if !good {
			return false
		}

		ka, good := r.keysAndAttributes()
		dst[table] = ka

		return good
	})
}

func (r *wireReader) keysAndAttributes() (types.KeysAndAttributes, bool) {
	var (
		ka   types.KeysAndAttributes
		seen uint32
		proj string
	)

	ok := r.members(func(key []byte, escaped bool) bool {
		bit := keysAndAttributesBit(key)
		if escaped || bit == 0 || seen&bit != 0 {
			return false
		}

		seen |= bit

		var good bool

		switch bit {
		case fKey:
			ka.Keys, good = r.keyList()
		case fNames:
			ka.ExpressionAttributeNames, good = r.stringMap()
		case fAttrsToGet:
			ka.AttributesToGet, good = r.stringList()
		case fConsistent:
			ka.ConsistentRead, good = r.boolPtr()
		default:
			proj, good = r.str()
		}

		return good
	})

	ka.ProjectionExpression = ptrconv.NilIfEmpty(proj)

	return ka, ok
}

func keysAndAttributesBit(key []byte) uint32 {
	switch string(key) {
	case "Keys":
		return fKey
	case "ExpressionAttributeNames":
		return fNames
	case "AttributesToGet":
		return fAttrsToGet
	case "ConsistentRead":
		return fConsistent
	case "ProjectionExpression":
		return fProjExpr
	default:
		return 0
	}
}

func (r *wireReader) keyList() ([]map[string]types.AttributeValue, bool) {
	var keys []map[string]types.AttributeValue

	ok := r.elements(func() bool {
		k, good := r.attrMap(0)
		keys = append(keys, nonNilItem(k))

		return good
	})

	return keys, ok
}

// fastPathEnabled reports whether the direct decoder may run; debug logging needs the legacy path.
func fastPathEnabled(ctx context.Context) bool {
	return !logger.Load(ctx).Enabled(ctx, slog.LevelDebug)
}

func tryFast[In, Out any](
	ctx context.Context,
	body []byte,
	decode func([]byte) (*In, bool),
	op func(context.Context, *In) (*Out, error),
	encode func(*Out) any,
) (any, bool, error) {
	in, ok := decode(body)
	if !ok {
		return nil, false, nil
	}

	out, err := op(ctx, in)
	if err != nil {
		return nil, true, err
	}

	return encode(out), true, nil
}

// dispatchItemFast runs the direct wire decoder/encoder for hot item ops; handled=false means fall back.
func (h *DynamoDBHandler) dispatchItemFast(ctx context.Context, action string, body []byte) (any, bool, error) {
	if len(body) == 0 || !fastPathEnabled(ctx) {
		return nil, false, nil
	}

	b := h.Backend

	switch action {
	case opPutItem:
		return tryFast(
			ctx,
			body,
			decodePutItem,
			b.PutItem,
			func(o *dynamodb.PutItemOutput) any { return encodePutItemOutput(o) },
		)
	case opGetItem:
		return fastGetItem(ctx, b, body)
	case opDeleteItem:
		return tryFast(
			ctx,
			body,
			decodeDeleteItem,
			b.DeleteItem,
			func(o *dynamodb.DeleteItemOutput) any { return encodeDeleteItemOutput(o) },
		)
	case opUpdateItem:
		return tryFast(
			ctx,
			body,
			decodeUpdateItem,
			b.UpdateItem,
			func(o *dynamodb.UpdateItemOutput) any { return encodeUpdateItemOutput(o) },
		)
	case opQuery:
		return fastQuery(ctx, b, body)
	case opScan:
		return fastScan(ctx, b, body)
	case opBatchGetItem:
		return tryFast(ctx, body, decodeBatchGetItem, b.BatchGetItem, encodeBatchGetItemOutput)
	case opBatchWriteItem:
		return tryFast(ctx, body, decodeBatchWriteItem, b.BatchWriteItem, encodeBatchWriteItemOutput)
	default:
		return nil, false, nil
	}
}

func fastGetItem(ctx context.Context, b StorageBackend, body []byte) (any, bool, error) {
	wb, ok := b.(wireReadBackend)
	if !ok {
		return tryFast(
			ctx,
			body,
			decodeGetItem,
			b.GetItem,
			func(o *dynamodb.GetItemOutput) any { return encodeGetItemOutput(o) },
		)
	}

	in, ok := decodeGetItem(body)
	if !ok {
		return nil, false, nil
	}

	item, cc, err := wb.getItemWire(ctx, in)
	if err != nil {
		return nil, true, err
	}

	resp, err := encodeGetItemWire(item, cc)
	if err != nil {
		return nil, true, err
	}

	return resp, true, nil
}

func fastQuery(ctx context.Context, b StorageBackend, body []byte) (any, bool, error) {
	wb, ok := b.(wireReadBackend)
	if !ok {
		return tryFast(
			ctx,
			body,
			decodeQuery,
			b.Query,
			func(o *dynamodb.QueryOutput) any { return encodeQueryOutput(o) },
		)
	}

	return tryFast(ctx, body, decodeQuery, wb.queryWire, func(r *pageResult) any { return encodeQueryPage(r) })
}

func fastScan(ctx context.Context, b StorageBackend, body []byte) (any, bool, error) {
	wb, ok := b.(wireReadBackend)
	if !ok {
		return tryFast(ctx, body, decodeScan, b.Scan, func(o *dynamodb.ScanOutput) any { return encodeScanOutput(o) })
	}

	return tryFast(ctx, body, decodeScan, wb.scanWire, func(r *pageResult) any { return encodeScanPage(r) })
}
