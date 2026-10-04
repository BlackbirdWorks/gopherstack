package dynamodb

import (
	"encoding/base64"
	"encoding/json"
	"slices"
	"strconv"
	"sync"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

const (
	hexDigits       = "0123456789abcdef"
	maxPooledBuffer = 4 << 20
	initialBufSize  = 2048
	itemKeyScratch  = 32
)

//nolint:gochecknoglobals // sync.Pool of response buffers
var encodeBufPool = sync.Pool{New: func() any {
	b := make([]byte, 0, initialBufSize)

	return &b
}}

// encodedResponse is a pre-serialised JSON response body held in a pooled buffer.
type encodedResponse struct {
	buf *[]byte
}

func newEncodedResponse() (*encodedResponse, []byte) {
	bp, _ := encodeBufPool.Get().(*[]byte)

	return &encodedResponse{buf: bp}, (*bp)[:0]
}

func (e *encodedResponse) finish(b []byte) *encodedResponse {
	*e.buf = b

	return e
}

func (e *encodedResponse) bytes() []byte { return *e.buf }

func (e *encodedResponse) release() {
	if cap(*e.buf) > maxPooledBuffer {
		return
	}

	encodeBufPool.Put(e.buf)
}

func noopRelease() {}

// marshalResponse serialises a dispatch result, returning a release func for pooled buffers.
func marshalResponse(v any) ([]byte, func(), error) {
	if e, ok := v.(*encodedResponse); ok {
		return e.bytes(), e.release, nil
	}

	b, err := json.Marshal(v)

	return b, noopRelease, err
}

var jsonEscapeNeeded = buildEscapeTable() //nolint:gochecknoglobals // lookup table

func buildEscapeTable() [utf8.RuneSelf]bool {
	var t [utf8.RuneSelf]bool

	for c := range asciiPrintable {
		t[c] = true
	}

	for _, c := range `"\<>&` {
		t[c] = true
	}

	return t
}

// appendJSONString appends s as a JSON string exactly as encoding/json would.
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0

	for i := 0; i < len(s); {
		i = skipSafeASCII(s, i)
		if i >= len(s) {
			break
		}

		c := s[i]
		if c < utf8.RuneSelf {
			if !jsonEscapeNeeded[c] {
				i++

				continue
			}

			dst = append(dst, s[start:i]...)
			dst = appendEscapedByte(dst, c)
			i++
			start = i

			continue
		}

		r, size := utf8.DecodeRuneInString(s[i:])
		switch {
		case r == utf8.RuneError && size == 1:
			dst = append(dst, s[start:i]...)
			dst = append(dst, "\xef\xbf\xbd"...)
		case r == ' ' || r == ' ':
			dst = append(dst, s[start:i]...)
			dst = append(dst, `\u202`...)
			dst = append(dst, hexDigits[r&0xF])
		default:
			i += size

			continue
		}

		i += size
		start = i
	}

	dst = append(dst, s[start:]...)

	return append(dst, '"')
}

const (
	lowBits  = 0x0101010101010101
	highBits = 0x8080808080808080
)

func load64(s string, i int) uint64 {
	return uint64(s[i]) | uint64(s[i+1])<<8 | uint64(s[i+2])<<16 | uint64(s[i+3])<<24 |
		uint64(s[i+4])<<32 | uint64(s[i+5])<<40 | uint64(s[i+6])<<48 | uint64(s[i+7])<<56
}

func hasByte(w uint64, c byte) uint64 {
	v := w ^ (lowBits * uint64(c))

	return (v - lowBits) & ^v & highBits
}

// skipSafeASCII advances i over 8-byte blocks that need no escaping and returns the new index.
func skipSafeASCII(s string, i int) int {
	for i+8 <= len(s) {
		w := load64(s, i)

		bad := w & highBits
		bad |= (w - lowBits*0x20) & ^w & highBits
		bad |= hasByte(w, '"') | hasByte(w, '\\') | hasByte(w, '<') | hasByte(w, '>') | hasByte(w, '&')

		if bad != 0 {
			return i
		}

		i += 8
	}

	return i
}

func appendEscapedByte(dst []byte, c byte) []byte {
	switch c {
	case '"', '\\':
		return append(dst, '\\', c)
	case '\b':
		return append(dst, '\\', 'b')
	case '\f':
		return append(dst, '\\', 'f')
	case '\n':
		return append(dst, '\\', 'n')
	case '\r':
		return append(dst, '\\', 'r')
	case '\t':
		return append(dst, '\\', 't')
	default:
		return append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xF])
	}
}

func appendStringList(dst []byte, list []string) []byte {
	if list == nil {
		return append(dst, "null"...)
	}

	dst = append(dst, '[')

	for i, s := range list {
		if i > 0 {
			dst = append(dst, ',')
		}

		dst = appendJSONString(dst, s)
	}

	return append(dst, ']')
}

func appendBase64String(dst []byte, b []byte) []byte {
	dst = append(dst, '"')
	dst = base64.StdEncoding.AppendEncode(dst, b)

	return append(dst, '"')
}

func appendSDKAttr(dst []byte, av types.AttributeValue) []byte {
	switch v := av.(type) {
	case *types.AttributeValueMemberS:
		dst = append(dst, `{"S":`...)
		dst = appendJSONString(dst, v.Value)
	case *types.AttributeValueMemberN:
		dst = append(dst, `{"N":`...)
		dst = appendJSONString(dst, v.Value)
	case *types.AttributeValueMemberB:
		dst = append(dst, `{"B":`...)
		dst = appendBase64String(dst, v.Value)
	case *types.AttributeValueMemberBOOL:
		dst = append(dst, `{"BOOL":`...)
		dst = strconv.AppendBool(dst, v.Value)
	case *types.AttributeValueMemberNULL:
		dst = append(dst, `{"NULL":`...)
		dst = strconv.AppendBool(dst, v.Value)
	default:
		return appendSDKContainer(dst, av)
	}

	return append(dst, '}')
}

func appendSDKContainer(dst []byte, av types.AttributeValue) []byte {
	switch v := av.(type) {
	case *types.AttributeValueMemberM:
		dst = append(dst, `{"M":`...)
		dst = appendSDKItem(dst, v.Value)
	case *types.AttributeValueMemberL:
		dst = append(dst, `{"L":[`...)

		for i, e := range v.Value {
			if i > 0 {
				dst = append(dst, ',')
			}

			dst = appendSDKAttr(dst, e)
		}

		dst = append(dst, ']')
	case *types.AttributeValueMemberSS:
		dst = append(dst, `{"SS":`...)
		dst = appendStringList(dst, v.Value)
	case *types.AttributeValueMemberNS:
		dst = append(dst, `{"NS":`...)
		dst = appendStringList(dst, v.Value)
	case *types.AttributeValueMemberBS:
		dst = append(dst, `{"BS":[`...)

		for i, e := range v.Value {
			if i > 0 {
				dst = append(dst, ',')
			}

			dst = appendBase64String(dst, e)
		}

		dst = append(dst, ']')
	default:
		return append(dst, "null"...)
	}

	return append(dst, '}')
}

// appendSDKItem appends an attribute map as a JSON object with keys sorted like encoding/json.
func appendSDKItem(dst []byte, item map[string]types.AttributeValue) []byte {
	var scratch [itemKeyScratch]string

	keys := scratch[:0]
	for k := range item {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	dst = append(dst, '{')

	for i, k := range keys {
		if i > 0 {
			dst = append(dst, ',')
		}

		dst = appendJSONString(dst, k)
		dst = append(dst, ':')
		dst = appendSDKAttr(dst, item[k])
	}

	return append(dst, '}')
}

func appendSDKItems(dst []byte, items []map[string]types.AttributeValue) []byte {
	dst = append(dst, '[')

	for i, it := range items {
		if i > 0 {
			dst = append(dst, ',')
		}

		dst = appendSDKItem(dst, it)
	}

	return append(dst, ']')
}

// appendMarshaled splices json.Marshal(v) for rarely-populated sub-documents.
func appendMarshaled(dst []byte, v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return append(dst, "null"...)
	}

	return append(dst, b...)
}

// objectWriter appends comma-separated object members.
type objectWriter struct {
	dst   []byte
	first bool
}

func newObjectWriter(dst []byte) *objectWriter {
	return &objectWriter{dst: append(dst, '{'), first: true}
}

func (w *objectWriter) key(name string) {
	if !w.first {
		w.dst = append(w.dst, ',')
	}

	w.first = false
	w.dst = append(w.dst, '"')
	w.dst = append(w.dst, name...)
	w.dst = append(w.dst, '"', ':')
}

func (w *objectWriter) end() []byte { return append(w.dst, '}') }

func (w *objectWriter) consumedCapacity(cc *types.ConsumedCapacity) {
	if cc == nil {
		return
	}

	w.key("ConsumedCapacity")
	w.dst = appendMarshaled(w.dst, models.FromSDKConsumedCapacity(cc))
}

func (w *objectWriter) itemCollectionMetrics(icm *types.ItemCollectionMetrics) {
	if icm == nil {
		return
	}

	w.key("ItemCollectionMetrics")
	w.dst = appendMarshaled(w.dst, models.FromSDKItemCollectionMetrics(icm))
}

func (w *objectWriter) counts(count, scanned int32) {
	w.key("Count")
	w.dst = strconv.AppendInt(w.dst, int64(count), decimalBase)
	w.key("ScannedCount")
	w.dst = strconv.AppendInt(w.dst, int64(scanned), decimalBase)
}

func encodeGetItemOutput(out *dynamodb.GetItemOutput) *encodedResponse {
	resp, buf := newEncodedResponse()
	w := newObjectWriter(buf)

	if out != nil {
		w.consumedCapacity(out.ConsumedCapacity)

		if len(out.Item) > 0 {
			w.key("Item")
			w.dst = appendSDKItem(w.dst, out.Item)
		}
	}

	return resp.finish(w.end())
}

func encodeWriteOutput(
	attrs map[string]types.AttributeValue,
	cc *types.ConsumedCapacity,
	icm *types.ItemCollectionMetrics,
) *encodedResponse {
	resp, buf := newEncodedResponse()
	w := newObjectWriter(buf)

	if len(attrs) > 0 {
		w.key("Attributes")
		w.dst = appendSDKItem(w.dst, attrs)
	}

	w.consumedCapacity(cc)
	w.itemCollectionMetrics(icm)

	return resp.finish(w.end())
}

func encodePutItemOutput(out *dynamodb.PutItemOutput) *encodedResponse {
	if out == nil {
		return encodeWriteOutput(nil, nil, nil)
	}

	return encodeWriteOutput(out.Attributes, out.ConsumedCapacity, out.ItemCollectionMetrics)
}

func encodeUpdateItemOutput(out *dynamodb.UpdateItemOutput) *encodedResponse {
	if out == nil {
		return encodeWriteOutput(nil, nil, nil)
	}

	return encodeWriteOutput(out.Attributes, out.ConsumedCapacity, out.ItemCollectionMetrics)
}

func encodeDeleteItemOutput(out *dynamodb.DeleteItemOutput) *encodedResponse {
	if out == nil {
		return encodeWriteOutput(nil, nil, nil)
	}

	return encodeWriteOutput(out.Attributes, out.ConsumedCapacity, out.ItemCollectionMetrics)
}

func encodeQueryOutput(out *dynamodb.QueryOutput) *encodedResponse {
	resp, buf := newEncodedResponse()
	w := newObjectWriter(buf)

	if len(out.LastEvaluatedKey) > 0 {
		w.key("LastEvaluatedKey")
		w.dst = appendSDKItem(w.dst, out.LastEvaluatedKey)
	}

	w.consumedCapacity(out.ConsumedCapacity)

	if len(out.Items) > 0 {
		w.key("Items")
		w.dst = appendSDKItems(w.dst, out.Items)
	}

	w.counts(out.Count, out.ScannedCount)

	return resp.finish(w.end())
}

func encodeScanOutput(out *dynamodb.ScanOutput) *encodedResponse {
	resp, buf := newEncodedResponse()
	w := newObjectWriter(buf)

	w.consumedCapacity(out.ConsumedCapacity)

	if len(out.LastEvaluatedKey) > 0 {
		w.key("LastEvaluatedKey")
		w.dst = appendSDKItem(w.dst, out.LastEvaluatedKey)
	}

	w.key("Items")
	w.dst = appendSDKItems(w.dst, out.Items)
	w.counts(out.Count, out.ScannedCount)

	return resp.finish(w.end())
}

// encodeBatchGetItemOutput falls back to the generic path when UnprocessedKeys is populated.
func encodeBatchGetItemOutput(out *dynamodb.BatchGetItemOutput) any {
	if len(out.UnprocessedKeys) > 0 {
		return models.FromSDKBatchGetItemOutput(out)
	}

	resp, buf := newEncodedResponse()
	w := newObjectWriter(buf)

	if len(out.Responses) > 0 {
		w.key("Responses")

		tables := make([]string, 0, len(out.Responses))
		for t := range out.Responses {
			tables = append(tables, t)
		}

		slices.Sort(tables)

		w.dst = append(w.dst, '{')

		for i, t := range tables {
			if i > 0 {
				w.dst = append(w.dst, ',')
			}

			w.dst = appendJSONString(w.dst, t)
			w.dst = append(w.dst, ':')
			w.dst = appendSDKItems(w.dst, out.Responses[t])
		}

		w.dst = append(w.dst, '}')
	}

	return resp.finish(w.end())
}

// encodeBatchWriteItemOutput falls back to the generic path unless every field is empty.
func encodeBatchWriteItemOutput(out *dynamodb.BatchWriteItemOutput) any {
	if len(out.UnprocessedItems) > 0 || len(out.ItemCollectionMetrics) > 0 || len(out.ConsumedCapacity) > 0 {
		return models.FromSDKBatchWriteItemOutput(out)
	}

	resp, buf := newEncodedResponse()

	return resp.finish(append(buf, '{', '}'))
}

const (
	b64FullQuad = 4
	padSingle   = 1
	padDouble   = 2
	b64Pad2Mask = 0x0F
	b64Pad1Mask = 0x03
)

var b64Index = buildBase64Index() //nolint:gochecknoglobals // lookup table

func buildBase64Index() [256]int8 {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

	var t [256]int8

	for i := range t {
		t[i] = -1
	}

	for i := range len(alphabet) {
		t[alphabet[i]] = int8(i)
	}

	return t
}

// isCanonicalBase64 reports whether re-encoding the decoded bytes would reproduce s exactly.
func isCanonicalBase64(s string) bool {
	n := len(s)
	if n%b64FullQuad != 0 {
		return false
	}

	pad := 0
	for pad < padDouble && n-pad > 0 && s[n-1-pad] == '=' {
		pad++
	}

	for i := range n - pad {
		if b64Index[s[i]] < 0 {
			return false
		}
	}

	switch pad {
	case padSingle:
		return b64Index[s[n-2]]&b64Pad1Mask == 0
	case padDouble:
		return b64Index[s[n-3]]&b64Pad2Mask == 0
	default:
		return true
	}
}

func appendWireString(dst []byte, typeKey string, val any) ([]byte, bool) {
	s, ok := val.(string)
	if !ok {
		return dst, false
	}

	dst = append(dst, `{"`...)
	dst = append(dst, typeKey...)
	dst = append(dst, `":`...)

	return append(appendJSONString(dst, s), '}'), true
}

// appendWireAttr appends one wire AttributeValue; ok is false for any shape that the
// SDK round trip would normalise or reject, so the caller can fall back to that path.
func appendWireAttr(dst []byte, v any) ([]byte, bool) {
	m, isMap := v.(map[string]any)
	if !isMap || len(m) != 1 {
		return dst, false
	}

	for _, t := range attrTypeKeys {
		if val, found := m[t]; found {
			return appendWireTyped(dst, t, val)
		}
	}

	return dst, false
}

func appendWireTyped(dst []byte, typeKey string, val any) ([]byte, bool) {
	switch typeKey {
	case "S", "N":
		return appendWireString(dst, typeKey, val)
	case typeBOOL, typeNULL:
		b, ok := val.(bool)
		if !ok {
			return dst, false
		}

		dst = append(dst, `{"`...)
		dst = append(dst, typeKey...)
		dst = append(dst, `":`...)

		return append(strconv.AppendBool(dst, b), '}'), true
	case "B":
		s, ok := val.(string)
		if !ok || !isCanonicalBase64(s) {
			return dst, false
		}

		return append(appendJSONString(append(dst, `{"B":`...), s), '}'), true
	case "M":
		nested, ok := val.(map[string]any)
		if !ok {
			return dst, false
		}

		dst, ok = appendWireItem(append(dst, `{"M":`...), nested)

		return append(dst, '}'), ok
	default:
		return appendWireCollection(dst, typeKey, val)
	}
}

func appendWireCollection(dst []byte, typeKey string, val any) ([]byte, bool) {
	switch typeKey {
	case "L":
		return appendWireList(dst, val)
	case "SS", "NS":
		return appendWireSet(dst, typeKey, val)
	case "BS":
		return appendWireBinarySet(dst, val)
	default:
		return dst, false
	}
}

func appendWireList(dst []byte, val any) ([]byte, bool) {
	list, ok := val.([]any)
	if !ok {
		return dst, false
	}

	dst = append(dst, `{"L":[`...)

	for i, e := range list {
		if i > 0 {
			dst = append(dst, ',')
		}

		if dst, ok = appendWireAttr(dst, e); !ok {
			return dst, false
		}
	}

	return append(dst, ']', '}'), true
}

func appendWireBinarySet(dst []byte, val any) ([]byte, bool) {
	list, ok := val.([]any)
	if !ok {
		return dst, false
	}

	dst = append(dst, `{"BS":[`...)

	for i, e := range list {
		s, isStr := e.(string)
		if !isStr || !isCanonicalBase64(s) {
			return dst, false
		}

		if i > 0 {
			dst = append(dst, ',')
		}

		dst = appendJSONString(dst, s)
	}

	return append(dst, ']', '}'), true
}

func appendWireSet(dst []byte, typeKey string, val any) ([]byte, bool) {
	dst = append(dst, `{"`...)
	dst = append(dst, typeKey...)
	dst = append(dst, `":`...)

	switch set := val.(type) {
	case []string:
		dst = appendStringList(dst, set)
	case []any:
		if len(set) == 0 {
			dst = append(dst, "null"...)

			break
		}

		dst = append(dst, '[')

		for i, e := range set {
			s, ok := e.(string)
			if !ok {
				return dst, false
			}

			if i > 0 {
				dst = append(dst, ',')
			}

			dst = appendJSONString(dst, s)
		}

		dst = append(dst, ']')
	default:
		return dst, false
	}

	return append(dst, '}'), true
}

// appendWireItem appends a wire item as a sorted-key JSON object.
func appendWireItem(dst []byte, item map[string]any) ([]byte, bool) {
	var scratch [itemKeyScratch]string

	keys := scratch[:0]
	for k := range item {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	dst = append(dst, '{')

	for i, k := range keys {
		if i > 0 {
			dst = append(dst, ',')
		}

		dst = appendJSONString(dst, k)
		dst = append(dst, ':')

		var ok bool
		if dst, ok = appendWireAttr(dst, item[k]); !ok {
			return dst, false
		}
	}

	return append(dst, '}'), true
}

// appendWireItemOrLegacy appends item directly, or through the SDK conversion when its
// shape is not plain wire form; err is non-nil only when that conversion itself fails.
func appendWireItemOrLegacy(dst []byte, item map[string]any) ([]byte, error) {
	start := len(dst)

	if out, ok := appendWireItem(dst, item); ok {
		return out, nil
	}

	sdkItem, err := models.ToSDKItem(item)
	if err != nil {
		return dst[:start], err
	}

	return appendSDKItem(dst[:start], sdkItem), nil
}

func (w *objectWriter) lastKey(key map[string]any) {
	if key == nil {
		return
	}

	if sdk, _ := models.ToSDKItem(key); len(sdk) > 0 {
		w.key("LastEvaluatedKey")
		w.dst = appendSDKItem(w.dst, sdk)
	}
}

func (w *objectWriter) wireItems(items []map[string]any) {
	w.dst = append(w.dst, '[')

	for i, it := range items {
		if i > 0 {
			w.dst = append(w.dst, ',')
		}

		var err error
		if w.dst, err = appendWireItemOrLegacy(w.dst, it); err != nil {
			w.dst = append(w.dst, '{', '}')
		}
	}

	w.dst = append(w.dst, ']')
}

func encodeGetItemWire(item map[string]any, cc *types.ConsumedCapacity) (*encodedResponse, error) {
	resp, buf := newEncodedResponse()
	w := newObjectWriter(buf)
	w.consumedCapacity(cc)

	if len(item) > 0 {
		w.key("Item")

		var err error
		if w.dst, err = appendWireItemOrLegacy(w.dst, item); err != nil {
			resp.release()

			return nil, err
		}
	}

	return resp.finish(w.end()), nil
}

func encodeQueryPage(res *pageResult) *encodedResponse {
	resp, buf := newEncodedResponse()
	w := newObjectWriter(buf)

	w.lastKey(res.lastKey)
	w.consumedCapacity(res.consumed)

	if len(res.items) > 0 {
		w.key("Items")
		w.wireItems(res.items)
	}

	w.counts(res.count, res.scannedCount)

	return resp.finish(w.end())
}

func encodeScanPage(res *pageResult) *encodedResponse {
	resp, buf := newEncodedResponse()
	w := newObjectWriter(buf)

	w.consumedCapacity(res.consumed)
	w.lastKey(res.lastKey)
	w.key("Items")
	w.wireItems(res.items)
	w.counts(res.count, res.scannedCount)

	return resp.finish(w.end())
}
