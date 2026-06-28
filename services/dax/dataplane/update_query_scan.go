package dataplane

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This file serves the DAX UpdateItem, Query and Scan data-plane operations.
// These carry their UpdateExpression / KeyConditionExpression /
// FilterExpression as a pre-parsed CBOR s-expression blob (see expression.go).
// Each handler decodes the request, reconstructs the DynamoDB expression
// string(s) plus ExpressionAttributeNames/Values, and delegates to the
// gopherstack DynamoDB backend, then encodes the result back into the DAX wire
// format.

// Request optional-parameter ordinals for Scan/Query/UpdateItem, matching the
// DAX client's requestParam* constants.
const (
	reqParamProjectionExpression = 0
	reqParamConditionExpression  = 4
	reqParamUpdateExpression     = 8
	reqParamExclusiveStartKey    = 9
	reqParamFilterExpression     = 10
	reqParamIndexName            = 11
	reqParamLimit                = 13
	reqParamScanIndexForward     = 14
	reqParamSelect               = 15
	reqParamSegment              = 16
	reqParamTotalSegments        = 17
)

// handleUpdateItem decodes an UpdateItem request and delegates to the backend.
func (s *Server) handleUpdateItem(r *Reader, w *Writer) error {
	table, err := readTable(r)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	ctx, cancel := s.requestContext()
	defer cancel()

	ks, err := s.schemaFor(ctx, table)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ResourceNotFoundException", err.Error())
	}

	key, err := decodeItemKey(r, ks)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	in := &awsddb.UpdateItemInput{TableName: &table, Key: key}

	rv, err := s.decodeUpdateParams(r, in)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	out, err := s.backend.UpdateItem(ctx, in)
	if err != nil {
		return s.writeBackendError(w, err)
	}

	if err = writeOK(w); err != nil {
		return err
	}

	return s.writeUpdateResponse(w, out, rv, ks)
}

// decodeUpdateParams reads the UpdateItem optional-params map into the SDK input
// and returns the requested returnValues ordinal.
func (s *Server) decodeUpdateParams(r *Reader, in *awsddb.UpdateItemInput) (int, error) {
	dec := newDecodedExpression()
	rv := rvNone

	err := forEachOptionalParam(r, func(key int) error {
		switch key {
		case reqParamConsistentRead:
			_, e := r.readBoolOrInt()

			return e
		case reqParamReturnValues:
			v, e := r.ReadInt()
			rv = v
			in.ReturnValues = returnValuesType(v)

			return e
		case reqParamUpdateExpression:
			expr, e := dec.readExpressionBlob(r, kindUpdate)
			if e != nil {
				return e
			}

			in.UpdateExpression = aws.String(expr)

			return nil
		case reqParamConditionExpression:
			expr, e := dec.readExpressionBlob(r, kindCondition)
			if e != nil {
				return e
			}

			in.ConditionExpression = aws.String(expr)

			return nil
		default:
			return r.skip()
		}
	})
	if err != nil {
		return rv, err
	}

	in.ExpressionAttributeNames = dec.namesMap()
	in.ExpressionAttributeValues = dec.valuesMap()

	return rv, nil
}

func (s *Server) writeUpdateResponse(
	w *Writer, out *awsddb.UpdateItemOutput, rv int, ks keySchema,
) error {
	if rv == rvNone || len(out.Attributes) == 0 {
		return w.WriteNull()
	}

	if err := w.WriteMapHeader(1); err != nil {
		return err
	}

	if err := w.WriteInt(respParamAttributes); err != nil {
		return err
	}

	switch rv {
	case rvUpdatedOld, rvUpdatedNew:
		// UPDATED_OLD/UPDATED_NEW use the byte-wrapped [attrListId, {ord: value}]
		// attribute-projection shape the client decodes via
		// decodeAttributeProjection. The backend already narrows out.Attributes
		// to only the attributes the update touched.
		return s.writeAttributeProjection(w, out.Attributes)
	default:
		// For ALL_OLD / ALL_NEW the client decodes the byte-wrapped non-key
		// attribute blob and merges the request key back in.
		return s.writeNonKeyAttributes(w, out.Attributes, ks)
	}
}

// handleQuery decodes a Query request and delegates to the backend.
func (s *Server) handleQuery(r *Reader, w *Writer) error {
	table, err := readTable(r)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	in := &awsddb.QueryInput{TableName: &table}
	dec := newDecodedExpression()

	keyCond, err := dec.readExpressionBlob(r, kindCondition)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	in.KeyConditionExpression = aws.String(keyCond)

	params := &queryScanParams{query: in}

	if err = s.decodeScanQueryParams(r, dec, params); err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	in.ExpressionAttributeNames = dec.namesMap()
	in.ExpressionAttributeValues = dec.valuesMap()

	ctx, cancel := s.requestContext()
	defer cancel()

	out, err := s.backend.Query(ctx, in)
	if err != nil {
		return s.writeBackendError(w, err)
	}

	if err = writeOK(w); err != nil {
		return err
	}

	ks, err := s.schemaFor(ctx, table)
	if err != nil {
		return err
	}

	return s.writeScanQueryResponse(w, scanQueryResult{
		items:        out.Items,
		count:        out.Count,
		scannedCount: out.ScannedCount,
		lastKey:      out.LastEvaluatedKey,
	}, ks, params.proj)
}

// handleScan decodes a Scan request and delegates to the backend.
func (s *Server) handleScan(r *Reader, w *Writer) error {
	table, err := readTable(r)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	in := &awsddb.ScanInput{TableName: &table}
	dec := newDecodedExpression()
	params := &queryScanParams{scan: in}

	if err = s.decodeScanQueryParams(r, dec, params); err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	in.ExpressionAttributeNames = dec.namesMap()
	in.ExpressionAttributeValues = dec.valuesMap()

	ctx, cancel := s.requestContext()
	defer cancel()

	out, err := s.backend.Scan(ctx, in)
	if err != nil {
		return s.writeBackendError(w, err)
	}

	if err = writeOK(w); err != nil {
		return err
	}

	ks, err := s.schemaFor(ctx, table)
	if err != nil {
		return err
	}

	return s.writeScanQueryResponse(w, scanQueryResult{
		items:        out.Items,
		count:        out.Count,
		scannedCount: out.ScannedCount,
		lastKey:      out.LastEvaluatedKey,
	}, ks, params.proj)
}

// queryScanParams holds exactly one of a Query or Scan input being populated
// from the shared scan/query optional-params map. proj, when set, carries the
// projection ordinals used to shape the response.
type queryScanParams struct {
	query *awsddb.QueryInput
	scan  *awsddb.ScanInput
	proj  *projection
}

func (p *queryScanParams) table() string {
	if p.query != nil {
		return *p.query.TableName
	}

	return *p.scan.TableName
}

func (p *queryScanParams) setFilter(expr string) {
	if p.query != nil {
		p.query.FilterExpression = aws.String(expr)
	} else {
		p.scan.FilterExpression = aws.String(expr)
	}
}

func (p *queryScanParams) setLimit(n int32) {
	if p.query != nil {
		p.query.Limit = aws.Int32(n)
	} else {
		p.scan.Limit = aws.Int32(n)
	}
}

func (p *queryScanParams) setStartKey(k map[string]types.AttributeValue) {
	if p.query != nil {
		p.query.ExclusiveStartKey = k
	} else {
		p.scan.ExclusiveStartKey = k
	}
}

func (p *queryScanParams) setProjection(proj *projection) {
	p.proj = proj

	if p.query != nil {
		p.query.ProjectionExpression = aws.String(proj.expression)
	} else {
		p.scan.ProjectionExpression = aws.String(proj.expression)
	}
}

func (p *queryScanParams) setForward(fwd bool) {
	if p.query != nil {
		p.query.ScanIndexForward = aws.Bool(fwd)
	}
}

func (p *queryScanParams) setSegment(seg int32) {
	if p.scan != nil {
		p.scan.Segment = aws.Int32(seg)
	}
}

func (p *queryScanParams) setTotalSegments(total int32) {
	if p.scan != nil {
		p.scan.TotalSegments = aws.Int32(total)
	}
}

// decodeScanQueryParams reads the shared Scan/Query optional-params map.
func (s *Server) decodeScanQueryParams(r *Reader, dec *decodedExpression, p *queryScanParams) error {
	ctx, cancel := s.requestContext()
	defer cancel()

	ks, err := s.schemaFor(ctx, p.table())
	if err != nil {
		return err
	}

	return forEachOptionalParam(r, func(key int) error {
		return s.decodeOneScanQueryParam(r, dec, p, ks, key)
	})
}

func (s *Server) decodeOneScanQueryParam(
	r *Reader, dec *decodedExpression, p *queryScanParams, ks keySchema, key int,
) error {
	switch key {
	case reqParamFilterExpression:
		expr, e := dec.readExpressionBlob(r, kindCondition)
		if e != nil {
			return e
		}

		p.setFilter(expr)

		return nil
	case reqParamProjectionExpression:
		blob, e := r.ReadBytes()
		if e != nil {
			return e
		}

		proj, e := dec.decodeProjectionBlob(blob)
		if e != nil {
			return e
		}

		p.setProjection(proj)

		return nil
	case reqParamLimit:
		v, e := r.ReadInt64()
		p.setLimit(clampInt32(v))

		return e
	case reqParamScanIndexForward:
		v, e := r.ReadInt()
		p.setForward(v != 0)

		return e
	case reqParamExclusiveStartKey:
		k, e := decodeItemKey(r, ks)
		if e != nil {
			return e
		}

		p.setStartKey(k)

		return nil
	case reqParamSegment:
		v, e := r.ReadInt64()
		p.setSegment(clampInt32(v))

		return e
	case reqParamTotalSegments:
		v, e := r.ReadInt64()
		p.setTotalSegments(clampInt32(v))

		return e
	case reqParamConsistentRead:
		_, e := r.readBoolOrInt()

		return e
	case reqParamIndexName, reqParamSelect:
		return r.skip()
	default:
		return r.skip()
	}
}

// Response body ordinals for Scan/Query, matching the DAX client's
// responseParam* constants.
const (
	respParamItems            = 7
	respParamCount            = 8
	respParamLastEvaluatedKey = 9
	respParamScannedCount     = 10
)

// scanQueryResult bundles the fields of a Scan/Query backend result that the
// DAX response encoder needs.
type scanQueryResult struct {
	lastKey      map[string]types.AttributeValue
	items        []map[string]types.AttributeValue
	count        int32
	scannedCount int32
}

// writeScanQueryResponse encodes a Scan/Query result map: items, count,
// scannedCount, and (when present) the lastEvaluatedKey. Without a projection,
// each item is a [keyBytes, nonKeyAttrsBlob] pair. With a projection, each item
// is the bare ordinal-keyed projection map the client decodes via
// decodeProjection.
func (s *Server) writeScanQueryResponse(
	w *Writer,
	res scanQueryResult,
	ks keySchema,
	proj *projection,
) error {
	entries := 3
	if len(res.lastKey) > 0 {
		entries++
	}

	if err := w.WriteMapHeader(entries); err != nil {
		return err
	}

	if err := s.writeScanQueryItems(w, res.items, ks, proj); err != nil {
		return err
	}

	if err := writeMapInt(w, respParamCount, int(res.count)); err != nil {
		return err
	}

	if err := writeMapInt(w, respParamScannedCount, int(res.scannedCount)); err != nil {
		return err
	}

	if len(res.lastKey) > 0 {
		if err := w.WriteInt(respParamLastEvaluatedKey); err != nil {
			return err
		}

		if err := encodeItemKey(w, res.lastKey, ks); err != nil {
			return err
		}
	}

	return nil
}

func (s *Server) writeScanQueryItems(
	w *Writer, items []map[string]types.AttributeValue, ks keySchema, proj *projection,
) error {
	if err := w.WriteInt(respParamItems); err != nil {
		return err
	}

	if err := w.WriteArrayHeader(len(items)); err != nil {
		return err
	}

	if proj.hasProjection() {
		for _, item := range items {
			if err := writeProjectionMap(w, projectedEntries(item, proj.ordinals)); err != nil {
				return err
			}
		}

		return nil
	}

	const itemPairArity = 2

	for _, item := range items {
		if err := w.WriteArrayHeader(itemPairArity); err != nil {
			return err
		}

		if err := encodeItemKey(w, item, ks); err != nil {
			return err
		}

		if err := s.writeNonKeyAttributes(w, item, ks); err != nil {
			return err
		}
	}

	return nil
}

// clampInt32 narrows an int64 to int32, saturating at the int32 bounds. DAX
// Limit/Segment/TotalSegments values are small in practice; the clamp keeps the
// conversion safe for malformed input.
func clampInt32(v int64) int32 {
	const (
		i32Bits = 31
		maxI32  = 1<<i32Bits - 1
		minI32  = -(1 << i32Bits)
	)

	switch {
	case v > maxI32:
		return maxI32
	case v < minI32:
		return minI32
	default:
		return int32(v)
	}
}

func returnValuesType(v int) types.ReturnValue {
	switch v {
	case rvAllOld:
		return types.ReturnValueAllOld
	case rvUpdatedOld:
		return types.ReturnValueUpdatedOld
	case rvAllNew:
		return types.ReturnValueAllNew
	case rvUpdatedNew:
		return types.ReturnValueUpdatedNew
	default:
		return types.ReturnValueNone
	}
}
