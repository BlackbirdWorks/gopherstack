package dataplane

import (
	"context"

	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	// writePairFields is the number of CBOR objects per batch item: key + value.
	writePairFields = 2
	// batchGetTupleLen is the element count of a BatchGetItem response envelope:
	// [responsesMap, unprocessedKeysMap].
	batchGetTupleLen = 2
)

// handleBatchWriteItem decodes a BatchWriteItem request and delegates each
// put/delete to the backend. Request shape:
//
//	map{ table: array(2*n)[ keyBytes, nonKeyBlobOrNil, ... ] } , optionalParams
func (s *Server) handleBatchWriteItem(r *Reader, w *Writer) error {
	ctx, cancel := requestContext()
	defer cancel()

	numTables, err := r.ReadMapLength()
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	requestItems := make(map[string][]types.WriteRequest, numTables)

	for range numTables {
		table, ks, e := s.readBatchTableHeader(ctx, r)
		if e != nil {
			return s.writeError(w, statusBadRequest, "ValidationException", e.Error())
		}

		writes, e := s.readWriteRequests(r, ks)
		if e != nil {
			return s.writeError(w, statusBadRequest, "ValidationException", e.Error())
		}

		requestItems[table] = writes
	}

	if _, err = readItemOptionalParams(r); err != nil {
		return err
	}

	if _, err = s.backend.BatchWriteItem(ctx, &awsddb.BatchWriteItemInput{RequestItems: requestItems}); err != nil {
		return s.writeBackendError(w, err)
	}

	return s.writeBatchWriteResponse(w)
}

func (s *Server) readBatchTableHeader(ctx context.Context, r *Reader) (string, keySchema, error) {
	table, err := r.ReadString()
	if err != nil {
		return "", nil, err
	}

	ks, err := s.schemaFor(ctx, table)
	if err != nil {
		return "", nil, err
	}

	return table, ks, nil
}

func (s *Server) readWriteRequests(r *Reader, ks keySchema) ([]types.WriteRequest, error) {
	numObjs, err := r.ReadArrayLength()
	if err != nil {
		return nil, err
	}

	count := numObjs / writePairFields
	writes := make([]types.WriteRequest, 0, count)

	for range count {
		key, e := decodeItemKey(r, ks)
		if e != nil {
			return nil, e
		}

		isNull, e := r.ReadNull()
		if e != nil {
			return nil, e
		}

		if isNull {
			writes = append(writes, types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{Key: key},
			})

			continue
		}

		nonKey, e := s.readNonKeyAttributes(r)
		if e != nil {
			return nil, e
		}

		writes = append(writes, types.WriteRequest{
			PutRequest: &types.PutRequest{Item: mergeItem(key, nonKey)},
		})
	}

	return writes, nil
}

// writeBatchWriteResponse writes an all-processed BatchWriteItem response:
// empty UnprocessedItems map, empty ConsumedCapacity array, empty ICM map.
func (s *Server) writeBatchWriteResponse(w *Writer) error {
	if err := writeOK(w); err != nil {
		return err
	}

	if err := w.WriteMapHeader(0); err != nil { // unprocessed items
		return err
	}

	if err := w.WriteArrayHeader(0); err != nil { // consumed capacity
		return err
	}

	return w.WriteMapHeader(0) // item collection metrics
}

// handleBatchGetItem decodes a BatchGetItem request and delegates to the
// backend. Request shape:
//
//	map{ table: array(3)[ consistentRead(bool), projectionOrNil, array(keys) ] }
func (s *Server) handleBatchGetItem(r *Reader, w *Writer) error {
	ctx, cancel := requestContext()
	defer cancel()

	numTables, err := r.ReadMapLength()
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	requestItems := make(map[string]types.KeysAndAttributes, numTables)
	schemas := make(map[string]keySchema, numTables)

	for range numTables {
		table, ks, kaa, e := s.readBatchGetTable(ctx, r)
		if e != nil {
			return s.writeError(w, statusBadRequest, "ValidationException", e.Error())
		}

		requestItems[table] = kaa
		schemas[table] = ks
	}

	out, err := s.backend.BatchGetItem(ctx, &awsddb.BatchGetItemInput{RequestItems: requestItems})
	if err != nil {
		return s.writeBackendError(w, err)
	}

	return s.writeBatchGetResponse(w, out, schemas)
}

func (s *Server) readBatchGetTable(
	ctx context.Context, r *Reader,
) (string, keySchema, types.KeysAndAttributes, error) {
	table, err := r.ReadString()
	if err != nil {
		return "", nil, types.KeysAndAttributes{}, err
	}

	ks, err := s.schemaFor(ctx, table)
	if err != nil {
		return "", nil, types.KeysAndAttributes{}, err
	}

	if _, err = r.ReadArrayLength(); err != nil { // always 3
		return "", nil, types.KeysAndAttributes{}, err
	}

	consistent, err := r.readBoolOrInt()
	if err != nil {
		return "", nil, types.KeysAndAttributes{}, err
	}

	if _, err = r.skipNullable(); err != nil { // projection blob (unsupported -> ignored)
		return "", nil, types.KeysAndAttributes{}, err
	}

	keys, err := s.readKeyList(r, ks)
	if err != nil {
		return "", nil, types.KeysAndAttributes{}, err
	}

	kaa := types.KeysAndAttributes{Keys: keys}
	if consistent {
		kaa.ConsistentRead = &consistent
	}

	return table, ks, kaa, nil
}

func (s *Server) readKeyList(r *Reader, ks keySchema) ([]map[string]types.AttributeValue, error) {
	numKeys, err := r.ReadArrayLength()
	if err != nil {
		return nil, err
	}

	keys := make([]map[string]types.AttributeValue, numKeys)

	for j := range numKeys {
		if keys[j], err = decodeItemKey(r, ks); err != nil {
			return nil, err
		}
	}

	return keys, nil
}

// writeBatchGetResponse writes a BatchGetItem response:
// [ map{table: array(2*n)[keyBytes, nonKeyBlob,...]}, emptyUnprocessedMap ] then CC array.
func (s *Server) writeBatchGetResponse(
	w *Writer, out *awsddb.BatchGetItemOutput, schemas map[string]keySchema,
) error {
	if err := writeOK(w); err != nil {
		return err
	}

	if err := w.WriteArrayHeader(batchGetTupleLen); err != nil {
		return err
	}

	if err := w.WriteMapHeader(len(out.Responses)); err != nil {
		return err
	}

	for table, items := range out.Responses {
		ks := schemas[table]

		if err := w.WriteString(table); err != nil {
			return err
		}

		if err := w.WriteArrayHeader(len(items) * writePairFields); err != nil {
			return err
		}

		for _, item := range items {
			if err := encodeItemKey(w, item, ks); err != nil {
				return err
			}

			if err := s.writeNonKeyAttributes(w, item, ks); err != nil {
				return err
			}
		}
	}

	if err := w.WriteMapHeader(0); err != nil { // unprocessed keys
		return err
	}

	return w.WriteArrayHeader(0) // consumed capacity
}

// skipNullable consumes either a null or one complete CBOR item. It returns
// true when the value was null.
func (r *Reader) skipNullable() (bool, error) {
	isNull, err := r.ReadNull()
	if err != nil {
		return false, err
	}

	if isNull {
		return true, nil
	}

	return false, r.skip()
}
