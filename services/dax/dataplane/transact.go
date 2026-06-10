package dataplane

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This file serves the DAX TransactWriteItems and TransactGetItems data-plane
// operations. Unlike the single-item ops, these arrive as a fixed sequence of
// parallel arrays (one element per transaction item) rather than a per-item
// frame, mirroring github.com/aws/aws-dax-go's encodeTransactWriteItemsInput /
// encodeTransactGetItemsInput. Each handler decodes those arrays, rebuilds the
// SDK TransactItems, delegates to the DynamoDB backend's transaction path (which
// provides atomicity and condition-check semantics), and encodes the DAX
// response.

// DAX transaction operation codes, matching the client's operation constants.
const (
	txnOpPut           = 2
	txnOpDelete        = 7
	txnOpPartialUpdate = 9
	txnOpCheck         = 12
)

// rvOnConditionCheckFailureAllOld is the DAX ordinal for
// ReturnValuesOnConditionCheckFailure=ALL_OLD (1 is NONE).
const rvOnConditionCheckFailureAllOld = 2

// txnWriteItem is one decoded TransactWriteItems element, before it is turned
// into an SDK TransactWriteItem.
type txnWriteItem struct {
	nonKey                map[string]types.AttributeValue
	key                   map[string]types.AttributeValue
	table                 string
	conditionBlob         []byte
	updateBlob            []byte
	operation             int
	rvOnConditionCheckOld bool
}

// handleTransactWriteItems decodes a TransactWriteItems request and delegates to
// the backend's transactional write path.
func (s *Server) handleTransactWriteItems(r *Reader, w *Writer) error {
	items, err := s.decodeTransactWriteItems(r)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	in, err := buildTransactWriteInput(items)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	ctx, cancel := requestContext()
	defer cancel()

	if _, err = s.backend.TransactWriteItems(ctx, in); err != nil {
		return s.writeBackendError(w, err)
	}

	if err = writeOK(w); err != nil {
		return err
	}

	return writeTransactWriteResponse(w)
}

// decodeTransactWriteItems reads the parallel arrays of a TransactWriteItems
// request and assembles per-item structs.
func (s *Server) decodeTransactWriteItems(r *Reader) ([]txnWriteItem, error) {
	operations, err := readIntArray(r)
	if err != nil {
		return nil, err
	}

	n := len(operations)

	tables, err := readByteStringArray(r, n)
	if err != nil {
		return nil, err
	}

	items := make([]txnWriteItem, n)
	for i := range items {
		items[i] = txnWriteItem{operation: operations[i], table: tables[i]}
	}

	if err = s.decodeTransactKeysAndValues(r, items); err != nil {
		return nil, err
	}

	if err = readSingleNull(r); err != nil { // returnValues placeholder
		return nil, err
	}

	rvCheck, err := readIntArray(r)
	if err != nil {
		return nil, err
	}

	for i := range items {
		if i < len(rvCheck) {
			items[i].rvOnConditionCheckOld = rvCheck[i] == rvOnConditionCheckFailureAllOld
		}
	}

	if err = readNullableBlobArrayInto(r, n, func(i int, b []byte) { items[i].conditionBlob = b }); err != nil {
		return nil, err
	}

	if err = readNullableBlobArrayInto(r, n, func(i int, b []byte) { items[i].updateBlob = b }); err != nil {
		return nil, err
	}

	return items, consumeOptionalParams(r)
}

// decodeTransactKeysAndValues reads the keys array and the values array. For put
// operations the value carries the non-key attribute blob and the key array
// entry is the encoded key of the item; for the other operations the value is
// null and the key array entry is the encoded primary key.
func (s *Server) decodeTransactKeysAndValues(r *Reader, items []txnWriteItem) error {
	if err := s.decodeTransactKeys(r, items); err != nil {
		return err
	}

	return s.decodeTransactValues(r, items)
}

func (s *Server) decodeTransactKeys(r *Reader, items []txnWriteItem) error {
	if _, err := r.ReadArrayLength(); err != nil {
		return err
	}

	for i := range items {
		ks, err := s.schemaForTable(items[i].table)
		if err != nil {
			return err
		}

		key, err := decodeItemKey(r, ks)
		if err != nil {
			return err
		}

		items[i].key = key
	}

	return nil
}

func (s *Server) decodeTransactValues(r *Reader, items []txnWriteItem) error {
	if _, err := r.ReadArrayLength(); err != nil {
		return err
	}

	for i := range items {
		isNull, err := r.ReadNull()
		if err != nil {
			return err
		}

		if isNull {
			continue
		}

		nonKey, err := s.readNonKeyAttributes(r)
		if err != nil {
			return err
		}

		items[i].nonKey = nonKey
	}

	return nil
}

// buildTransactWriteInput turns decoded items into an SDK TransactWriteItemsInput.
func buildTransactWriteInput(items []txnWriteItem) (*awsddb.TransactWriteItemsInput, error) {
	txItems := make([]types.TransactWriteItem, len(items))

	for i := range items {
		ti, err := items[i].toTransactWriteItem()
		if err != nil {
			return nil, err
		}

		txItems[i] = ti
	}

	return &awsddb.TransactWriteItemsInput{TransactItems: txItems}, nil
}

func (it *txnWriteItem) toTransactWriteItem() (types.TransactWriteItem, error) {
	switch it.operation {
	case txnOpPut:
		return it.buildPut()
	case txnOpDelete:
		return it.buildDelete()
	case txnOpPartialUpdate:
		return it.buildUpdate()
	case txnOpCheck:
		return it.buildConditionCheck()
	default:
		return types.TransactWriteItem{}, errBadExpression
	}
}

func (it *txnWriteItem) buildPut() (types.TransactWriteItem, error) {
	dec := newDecodedExpression()

	cond, err := it.condition(dec)
	if err != nil {
		return types.TransactWriteItem{}, err
	}

	item := mergeItem(it.key, it.nonKey)

	return types.TransactWriteItem{Put: &types.Put{
		TableName:                           aws.String(it.table),
		Item:                                item,
		ConditionExpression:                 cond,
		ExpressionAttributeNames:            dec.namesMap(),
		ExpressionAttributeValues:           dec.valuesMap(),
		ReturnValuesOnConditionCheckFailure: it.rvOnConditionCheck(),
	}}, nil
}

func (it *txnWriteItem) buildDelete() (types.TransactWriteItem, error) {
	dec := newDecodedExpression()

	cond, err := it.condition(dec)
	if err != nil {
		return types.TransactWriteItem{}, err
	}

	return types.TransactWriteItem{Delete: &types.Delete{
		TableName:                           aws.String(it.table),
		Key:                                 it.key,
		ConditionExpression:                 cond,
		ExpressionAttributeNames:            dec.namesMap(),
		ExpressionAttributeValues:           dec.valuesMap(),
		ReturnValuesOnConditionCheckFailure: it.rvOnConditionCheck(),
	}}, nil
}

func (it *txnWriteItem) buildUpdate() (types.TransactWriteItem, error) {
	dec := newDecodedExpression()

	cond, err := it.condition(dec)
	if err != nil {
		return types.TransactWriteItem{}, err
	}

	if len(it.updateBlob) == 0 {
		return types.TransactWriteItem{}, errBadExpression
	}

	updateExpr, err := dec.decodeBlob(it.updateBlob, kindUpdate)
	if err != nil {
		return types.TransactWriteItem{}, err
	}

	return types.TransactWriteItem{Update: &types.Update{
		TableName:                           aws.String(it.table),
		Key:                                 it.key,
		UpdateExpression:                    aws.String(updateExpr),
		ConditionExpression:                 cond,
		ExpressionAttributeNames:            dec.namesMap(),
		ExpressionAttributeValues:           dec.valuesMap(),
		ReturnValuesOnConditionCheckFailure: it.rvOnConditionCheck(),
	}}, nil
}

func (it *txnWriteItem) buildConditionCheck() (types.TransactWriteItem, error) {
	dec := newDecodedExpression()

	cond, err := it.condition(dec)
	if err != nil {
		return types.TransactWriteItem{}, err
	}

	if cond == nil {
		return types.TransactWriteItem{}, errBadExpression
	}

	return types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
		TableName:                           aws.String(it.table),
		Key:                                 it.key,
		ConditionExpression:                 cond,
		ExpressionAttributeNames:            dec.namesMap(),
		ExpressionAttributeValues:           dec.valuesMap(),
		ReturnValuesOnConditionCheckFailure: it.rvOnConditionCheck(),
	}}, nil
}

// condition decodes the item's condition blob (if any) into dec and returns the
// reconstructed condition-expression string.
func (it *txnWriteItem) condition(dec *decodedExpression) (*string, error) {
	if len(it.conditionBlob) == 0 {
		return nil, nil //nolint:nilnil // absent condition is a valid nil expression
	}

	expr, err := dec.decodeBlob(it.conditionBlob, kindCondition)
	if err != nil {
		return nil, err
	}

	return aws.String(expr), nil
}

func (it *txnWriteItem) rvOnConditionCheck() types.ReturnValuesOnConditionCheckFailure {
	if it.rvOnConditionCheckOld {
		return types.ReturnValuesOnConditionCheckFailureAllOld
	}

	return types.ReturnValuesOnConditionCheckFailureNone
}

// writeTransactWriteResponse emits the TransactWriteItems success body:
// [returnValues, consumedCapacity, itemCollectionMetrics]. gopherstack reports
// empty returnValues / consumed capacity / collection metrics.
func writeTransactWriteResponse(w *Writer) error {
	const responseArity = 3
	if err := w.WriteArrayHeader(responseArity); err != nil {
		return err
	}

	if err := w.WriteArrayHeader(0); err != nil { // returnValues
		return err
	}

	if err := w.WriteArrayHeader(0); err != nil { // consumedCapacity
		return err
	}

	return w.WriteMapHeader(0) // itemCollectionMetrics
}

// txnGetItem is one decoded TransactGetItems element.
type txnGetItem struct {
	key   map[string]types.AttributeValue
	proj  *projection
	dec   *decodedExpression
	table string
}

// handleTransactGetItems decodes a TransactGetItems request, delegates to the
// backend, and encodes the responses (one per item, in order).
func (s *Server) handleTransactGetItems(r *Reader, w *Writer) error {
	items, err := s.decodeTransactGetItems(r)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	in := buildTransactGetInput(items)

	ctx, cancel := requestContext()
	defer cancel()

	out, err := s.backend.TransactGetItems(ctx, in)
	if err != nil {
		return s.writeBackendError(w, err)
	}

	if err = writeOK(w); err != nil {
		return err
	}

	return s.writeTransactGetResponse(w, items, out)
}

func (s *Server) decodeTransactGetItems(r *Reader) ([]txnGetItem, error) {
	tables, err := readByteStringArrayLenPrefixed(r)
	if err != nil {
		return nil, err
	}

	n := len(tables)
	items := make([]txnGetItem, n)

	for i := range items {
		items[i] = txnGetItem{table: tables[i], dec: newDecodedExpression()}
	}

	if err = s.decodeTransactGetKeys(r, items); err != nil {
		return nil, err
	}

	if err = s.decodeTransactGetProjections(r, items); err != nil {
		return nil, err
	}

	return items, consumeOptionalParams(r)
}

func (s *Server) decodeTransactGetKeys(r *Reader, items []txnGetItem) error {
	if _, err := r.ReadArrayLength(); err != nil {
		return err
	}

	for i := range items {
		ks, err := s.schemaForTable(items[i].table)
		if err != nil {
			return err
		}

		key, err := decodeItemKey(r, ks)
		if err != nil {
			return err
		}

		items[i].key = key
	}

	return nil
}

func (s *Server) decodeTransactGetProjections(r *Reader, items []txnGetItem) error {
	if _, err := r.ReadArrayLength(); err != nil {
		return err
	}

	for i := range items {
		isNull, err := r.ReadNull()
		if err != nil {
			return err
		}

		if isNull {
			continue
		}

		blob, err := r.ReadBytes()
		if err != nil {
			return err
		}

		proj, err := items[i].dec.decodeProjectionBlob(blob)
		if err != nil {
			return err
		}

		items[i].proj = proj
	}

	return nil
}

func buildTransactGetInput(items []txnGetItem) *awsddb.TransactGetItemsInput {
	txItems := make([]types.TransactGetItem, len(items))

	for i := range items {
		get := &types.Get{
			TableName: aws.String(items[i].table),
			Key:       items[i].key,
		}

		if items[i].proj.hasProjection() {
			get.ProjectionExpression = aws.String(items[i].proj.expression)
			get.ExpressionAttributeNames = items[i].dec.namesMap()
		}

		txItems[i] = types.TransactGetItem{Get: get}
	}

	return &awsddb.TransactGetItemsInput{TransactItems: txItems}
}

// writeTransactGetResponse emits [responses, consumedCapacity]. Each response is
// the non-key attribute blob for a found item (projection map when the get
// carried a ProjectionExpression), or null when the item was not found.
func (s *Server) writeTransactGetResponse(
	w *Writer, items []txnGetItem, out *awsddb.TransactGetItemsOutput,
) error {
	const responseArity = 2
	if err := w.WriteArrayHeader(responseArity); err != nil {
		return err
	}

	if err := w.WriteArrayHeader(len(items)); err != nil {
		return err
	}

	for i := range items {
		item := transactGetItemAt(out, i)

		if err := s.writeTransactGetItem(w, items[i], item); err != nil {
			return err
		}
	}

	return w.WriteArrayHeader(0) // consumedCapacity
}

func (s *Server) writeTransactGetItem(
	w *Writer, get txnGetItem, item map[string]types.AttributeValue,
) error {
	if len(item) == 0 {
		return w.WriteNull()
	}

	if get.proj.hasProjection() {
		return writeProjectionMap(w, projectedEntries(item, get.proj.ordinals))
	}

	ks, err := s.schemaForTable(get.table)
	if err != nil {
		return err
	}

	return s.writeNonKeyAttributes(w, item, ks)
}

// transactGetItemAt extracts the item at index i from a TransactGetItems result,
// tolerating a short or sparse Responses slice.
func transactGetItemAt(out *awsddb.TransactGetItemsOutput, i int) map[string]types.AttributeValue {
	if out == nil || i >= len(out.Responses) {
		return nil
	}

	return out.Responses[i].Item
}

// schemaForTable resolves a table's key schema using a bounded context.
func (s *Server) schemaForTable(table string) (keySchema, error) {
	ctx, cancel := requestContext()
	defer cancel()

	return s.schemaFor(ctx, table)
}

// readIntArray reads a definite-length array of CBOR ints.
func readIntArray(r *Reader) ([]int, error) {
	n, err := r.ReadArrayLength()
	if err != nil {
		return nil, err
	}

	out := make([]int, n)
	for i := range out {
		if out[i], err = r.ReadInt(); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// readByteStringArray reads exactly n byte strings into string values.
func readByteStringArray(r *Reader, n int) ([]string, error) {
	if _, err := r.ReadArrayLength(); err != nil {
		return nil, err
	}

	out := make([]string, n)
	for i := range out {
		b, err := r.ReadBytes()
		if err != nil {
			return nil, err
		}

		out[i] = string(b)
	}

	return out, nil
}

// readByteStringArrayLenPrefixed reads a length-prefixed array of byte strings.
func readByteStringArrayLenPrefixed(r *Reader) ([]string, error) {
	n, err := r.ReadArrayLength()
	if err != nil {
		return nil, err
	}

	out := make([]string, n)
	for i := range out {
		b, e := r.ReadBytes()
		if e != nil {
			return nil, e
		}

		out[i] = string(b)
	}

	return out, nil
}

// readNullableBlobArrayInto reads an array of n (null | byte-string) entries,
// passing each non-null blob to set with its index.
func readNullableBlobArrayInto(r *Reader, n int, set func(i int, b []byte)) error {
	if _, err := r.ReadArrayLength(); err != nil {
		return err
	}

	for i := range n {
		isNull, err := r.ReadNull()
		if err != nil {
			return err
		}

		if isNull {
			continue
		}

		b, err := r.ReadBytes()
		if err != nil {
			return err
		}

		set(i, b)
	}

	return nil
}

// readSingleNull consumes a single CBOR null (the transact returnValues slot).
func readSingleNull(r *Reader) error {
	isNull, err := r.ReadNull()
	if err != nil {
		return err
	}

	if !isNull {
		return errBadExpression
	}

	return nil
}

// consumeOptionalParams reads and discards the trailing optional-params map of a
// transact request. gopherstack does not act on ReturnConsumedCapacity or
// ReturnItemCollectionMetrics; ClientRequestToken would only affect idempotency
// dedup, which the local emulator does not need across a single connection.
func consumeOptionalParams(r *Reader) error {
	return forEachOptionalParam(r, func(int) error {
		return r.skip()
	})
}
