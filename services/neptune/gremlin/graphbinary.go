package gremlin

import (
	"bytes"
	"encoding/binary"
	"math"
)

const mimeGraphBinary = "application/vnd.graphbinary-v1.0"

// GraphBinary v1 data type codes.
const (
	gbInt         = 0x01
	gbLong        = 0x02
	gbString      = 0x03
	gbDate        = 0x04
	gbTimestamp   = 0x05
	gbDouble      = 0x07
	gbFloat       = 0x08
	gbList        = 0x09
	gbMap         = 0x0a
	gbSet         = 0x0b
	gbUUID        = 0x0c
	gbEdge        = 0x0d
	gbPath        = 0x0e
	gbProperty    = 0x0f
	gbVertex      = 0x11
	gbVertexProp  = 0x12
	gbBinding     = 0x14
	gbBytecode    = 0x15
	gbCardinality = 0x16
	gbColumn      = 0x17
	gbDirection   = 0x18
	gbOperator    = 0x19
	gbOrder       = 0x1a
	gbPick        = 0x1b
	gbPop         = 0x1c
	gbLambda      = 0x1d
	gbP           = 0x1e
	gbScope       = 0x1f
	gbT           = 0x20
	gbByte        = 0x24
	gbShort       = 0x26
	gbBoolean     = 0x27
	gbTextP       = 0x28
	gbMerge       = 0x2e
	gbNull        = 0xfe

	gbVersion     = 0x81
	maxBinaryDeep = 64
	maxItems      = 1 << 20
)

var errShort error = &Error{
	Code: CodeInvalidParameter, Status: StatusInvalidRequest, Detail: "Malformed GraphBinary message",
}

func enumKind(code byte) (string, bool) {
	switch code {
	case gbCardinality:
		return kindCardinality, true
	case gbColumn:
		return kindColumn, true
	case gbDirection:
		return kindDirection, true
	case gbOperator:
		return kindOperator, true
	case gbOrder:
		return kindOrder, true
	case gbPick:
		return kindPick, true
	case gbPop:
		return kindPop, true
	case gbScope:
		return kindScope, true
	case gbT:
		return kindT, true
	case gbMerge:
		return kindMerge, true
	}

	return "", false
}

func enumCode(kind string) byte {
	for c := byte(gbCardinality); c <= gbMerge; c++ {
		if k, ok := enumKind(c); ok && k == kind {
			return c
		}
	}

	return gbT
}

type bbReader struct {
	b     []byte
	i     int
	depth int
}

func (r *bbReader) take(n int) ([]byte, error) {
	if n < 0 || n > len(r.b)-r.i {
		return nil, errShort
	}

	out := r.b[r.i : r.i+n]
	r.i += n

	return out, nil
}

func (r *bbReader) u8() (byte, error) {
	b, err := r.take(1)
	if err != nil {
		return 0, err
	}

	return b[0], nil
}

func (r *bbReader) i32() (int, error) {
	b, err := r.take(4) //nolint:mnd // int32
	if err != nil {
		return 0, err
	}

	return beInt32(b), nil
}

func (r *bbReader) i64() (int64, error) {
	b, err := r.take(8) //nolint:mnd // int64
	if err != nil {
		return 0, err
	}

	return beInt64(b), nil
}

func (r *bbReader) count() (int, error) {
	n, err := r.i32()
	if err != nil {
		return 0, err
	}

	if n < 0 || n > maxItems || n > len(r.b)-r.i+1 {
		return 0, errShort
	}

	return n, nil
}

func (r *bbReader) str() (string, error) {
	n, err := r.i32()
	if err != nil {
		return "", err
	}

	b, err := r.take(n)

	return string(b), err
}

// fq reads a fully-qualified value: type code, null flag, payload.
func (r *bbReader) fq() (any, error) {
	r.depth++
	defer func() { r.depth-- }()

	if r.depth > maxBinaryDeep {
		return nil, invalidArg("GraphBinary value nesting is too deep")
	}

	code, err := r.u8()
	if err != nil {
		return nil, err
	}

	if code == gbNull {
		_, err = r.u8()

		return nil, err
	}

	flag, err := r.u8()
	if err != nil || flag == 1 {
		return nil, err
	}

	return r.payload(code)
}

func (r *bbReader) payload(code byte) (any, error) {
	if kind, ok := enumKind(code); ok {
		return r.enum(kind)
	}

	switch code {
	case gbInt:
		n, err := r.i32()

		return int64(n), err
	case gbLong, gbDate, gbTimestamp:
		return r.i64()
	case gbShort:
		b, err := r.take(2) //nolint:mnd // int16

		return beInt16(b), err
	case gbByte:
		b, err := r.u8()

		return signedByte(b), err
	case gbString:
		return r.str()
	case gbDouble, gbFloat:
		return r.float(code)
	case gbBoolean:
		b, err := r.u8()

		return b != 0, err
	case gbUUID:
		return r.uuid()
	case gbList, gbSet:
		return r.list(code == gbSet)
	case gbMap:
		return r.mapValue()
	}

	return r.complexPayload(code)
}

func (r *bbReader) complexPayload(code byte) (any, error) {
	switch code {
	case gbBytecode:
		return r.bytecode()
	case gbP, gbTextP:
		return r.pred()
	case gbVertex:
		return r.vertexRef()
	case gbBinding:
		if _, err := r.str(); err != nil {
			return nil, err
		}

		return r.fq()
	case gbLambda:
		return nil, unsupported("Lambdas are not supported")
	}

	return nil, unsupported("Unsupported GraphBinary type 0x%02x", code)
}

func (r *bbReader) float(code byte) (any, error) {
	if code == gbDouble {
		n, err := r.i64()

		return beFloat64(n), err
	}

	b, err := r.take(4) //nolint:mnd // float32
	if err != nil {
		return nil, err
	}

	return float64(math.Float32frombits(binary.BigEndian.Uint32(b))), nil
}

func (r *bbReader) uuid() (any, error) {
	b, err := r.take(16) //nolint:mnd // uuid bytes
	if err != nil {
		return nil, err
	}

	return formatUUID(b), nil
}

func (r *bbReader) enum(kind string) (any, error) {
	if _, err := r.take(2); err != nil { //nolint:mnd // string type code and null flag
		return nil, err
	}

	name, err := r.str()

	return Token{Kind: kind, Name: name}, err
}

func (r *bbReader) list(set bool) (any, error) {
	n, err := r.count()
	if err != nil {
		return nil, err
	}

	out := make([]any, 0, n)

	for range n {
		v, verr := r.fq()
		if verr != nil {
			return nil, verr
		}

		out = append(out, v)
	}

	if set {
		return Set(out), nil
	}

	return out, nil
}

func (r *bbReader) mapValue() (any, error) {
	n, err := r.count()
	if err != nil {
		return nil, err
	}

	out := NewMap()

	for range n {
		k, kerr := r.fq()
		if kerr != nil {
			return nil, kerr
		}

		v, verr := r.fq()
		if verr != nil {
			return nil, verr
		}

		out.Put(k, v)
	}

	return out, nil
}

func (r *bbReader) vertexRef() (any, error) {
	id, err := r.fq()
	if err != nil {
		return nil, err
	}

	if _, err = r.str(); err != nil {
		return nil, err
	}

	if _, err = r.fq(); err != nil {
		return nil, err
	}

	return VertexRef{ID: idString(id)}, nil
}

func (r *bbReader) pred() (any, error) {
	op, err := r.str()
	if err != nil {
		return nil, err
	}

	n, err := r.count()
	if err != nil {
		return nil, err
	}

	vals := make([]any, 0, n)

	for range n {
		v, verr := r.fq()
		if verr != nil {
			return nil, verr
		}

		vals = append(vals, v)
	}

	if op == opAnd || op == opOr {
		return connective(op, vals)
	}

	return Pred{Op: op, Args: vals}, nil
}

func (r *bbReader) bytecode() (any, error) {
	tr := &Traversal{}

	if err := r.instructions(func(name string, args []any) { tr.add(Step{Name: name, Args: args}) }); err != nil {
		return nil, err
	}

	if err := r.instructions(func(string, []any) {}); err != nil {
		return nil, err
	}

	return tr, nil
}

func (r *bbReader) instructions(add func(string, []any)) error {
	n, err := r.count()
	if err != nil {
		return err
	}

	for range n {
		name, nerr := r.str()
		if nerr != nil {
			return nerr
		}

		argc, aerr := r.count()
		if aerr != nil {
			return aerr
		}

		args := make([]any, 0, argc)

		for range argc {
			v, verr := r.fq()
			if verr != nil {
				return verr
			}

			args = append(args, v)
		}

		add(name, args)
	}

	return nil
}

// bbRequest is a decoded GraphBinary request message body (after the mime header).
type bbRequest struct {
	args      map[string]any
	id        string
	op        string
	processor string
}

func decodeBinaryRequest(body []byte) (*bbRequest, error) {
	r := &bbReader{b: body}

	ver, err := r.u8()
	if err != nil || ver != gbVersion {
		return nil, invalidArg("Unsupported GraphBinary message version")
	}

	idb, err := r.take(16) //nolint:mnd // request uuid
	if err != nil {
		return nil, err
	}

	req := &bbRequest{id: formatUUID(idb), args: map[string]any{}}

	if req.op, err = r.str(); err != nil {
		return nil, err
	}

	if req.processor, err = r.str(); err != nil {
		return nil, err
	}

	n, err := r.count()
	if err != nil {
		return nil, err
	}

	for range n {
		k, kerr := r.fq()
		if kerr != nil {
			return nil, kerr
		}

		v, verr := r.fq()
		if verr != nil {
			return nil, verr
		}

		if ks, ok := k.(string); ok {
			req.args[ks] = v
		}
	}

	return req, nil
}

type bbWriter struct {
	buf bytes.Buffer
}

func (w *bbWriter) i32(n int) { w.buf.Write(putInt32(n)) }

func (w *bbWriter) i64(n int64) { w.buf.Write(putInt64(n)) }

func (w *bbWriter) str(s string) {
	w.i32(len(s))
	w.buf.WriteString(s)
}

func (w *bbWriter) null() { w.buf.Write([]byte{gbNull, 1}) }

func (w *bbWriter) head(code byte) { w.buf.Write([]byte{code, 0}) }

func (w *bbWriter) fqString(s string) {
	w.head(gbString)
	w.str(s)
}

func (w *bbWriter) fq(v any) {
	switch t := v.(type) {
	case nil:
		w.null()
	case bool:
		w.head(gbBoolean)
		w.buf.WriteByte(boolByte(t))
	case string:
		w.fqString(t)
	case int64:
		w.head(gbLong)
		w.i64(t)
	case int32:
		w.head(gbInt)
		w.i32(int(t))
	case int:
		w.head(gbInt)
		w.i32(t)
	case float64:
		w.head(gbDouble)
		w.i64(floatBits64(t))
	case float32:
		w.head(gbFloat)
		w.i32(floatBits32(t))
	case []any:
		w.head(gbList)
		w.items(t)
	case Set:
		w.head(gbSet)
		w.items(t)
	case *Map:
		w.mapValue(t)
	case Token:
		w.enum(t)
	default:
		w.element(v)
	}
}

func boolByte(b bool) byte {
	if b {
		return 1
	}

	return 0
}

func (w *bbWriter) items(items []any) {
	w.i32(len(items))

	for _, e := range items {
		w.fq(e)
	}
}

func (w *bbWriter) mapValue(m *Map) {
	w.head(gbMap)
	w.i32(m.Len())

	for i := range m.Len() {
		k, v := m.Entry(i)
		w.fq(k)
		w.fq(v)
	}
}

func (w *bbWriter) enum(t Token) {
	w.head(enumCode(t.Kind))
	w.fqString(t.Name)
}

func (w *bbWriter) element(v any) {
	switch t := v.(type) {
	case *Vertex:
		w.head(gbVertex)
		w.fqString(t.ID)
		w.str(t.Label)
		w.null()
	case *Edge:
		w.head(gbEdge)
		w.fqString(t.ID)
		w.str(t.Label)
		w.fqString(t.InID)
		w.str(t.InLabel)
		w.fqString(t.OutID)
		w.str(t.OutLabel)
		w.null()
		w.null()
	case *Property:
		w.property(t)
	case *Path:
		w.path(t)
	default:
		w.fqString(keyOf(v))
	}
}

func (w *bbWriter) property(p *Property) {
	if p.VP {
		w.head(gbVertexProp)
		w.head(gbLong)
		w.i64(p.ID)
		w.str(p.Key)
		w.fq(p.Val)
		w.null()
		w.null()

		return
	}

	w.head(gbProperty)
	w.str(p.Key)
	w.fq(p.Val)
	w.null()
}

func (w *bbWriter) path(p *Path) {
	w.head(gbPath)
	w.head(gbList)
	w.i32(len(p.Labels))

	for _, ls := range p.Labels {
		set := make([]any, len(ls))
		for i, l := range ls {
			set[i] = l
		}

		w.head(gbSet)
		w.items(set)
	}

	w.head(gbList)
	w.items(p.Objects)
}

// response renders one GraphBinary response message.
func (w *bbWriter) response(id string, code int, message string, data []any, hasData bool) []byte {
	w.buf.Reset()
	w.buf.Write([]byte{gbVersion, 0})
	w.buf.Write(parseUUID(id))
	w.i32(code)

	if message == "" {
		w.buf.WriteByte(1)
	} else {
		w.buf.WriteByte(0)
		w.str(message)
	}

	w.i32(0)
	w.i32(0)

	if hasData {
		w.head(gbList)
		w.items(data)
	} else {
		w.null()
	}

	return w.buf.Bytes()
}
