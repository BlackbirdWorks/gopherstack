//nolint:mnd // protobuf field numbers of the ORC footer messages
package orcio

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/blackbirdworks/gopherstack/pkgs/hivetype"
)

const (
	stripeRowLimit = 50000
	writerVersion  = 9
	versionMajor   = 0
	versionMinor   = 12
	psFieldFooter  = 1
	softwareName   = "gopherstack"
)

// Column is one top-level column to write; Type is a Hive/Athena type name.
type Column struct {
	// NestedIDs gives iceberg.id attributes to nested members by logical path ("col.member",
	// "col.element", "col.key", "col.value").
	NestedIDs map[string]int
	Name      string
	Type      string
	// FieldID is written as the iceberg.id type attribute when positive.
	FieldID int
}

// Write encodes rows as one ORC file. Cells are nil, bool, int64, float64 or string; date,
// timestamp and decimal cells are strings; nested types take decoded JSON values
// (map[string]any / []any) or JSON text.
func Write(w io.Writer, cols []Column, rows [][]any, compression string) error {
	codec, err := codecByName(compression)
	if err != nil {
		return err
	}

	tree, err := buildTree(cols)
	if err != nil {
		return err
	}

	var out bytes.Buffer

	out.WriteString(orcMagic)

	var infos []stripeInfo

	rowsLeft := rows
	for len(rowsLeft) > 0 || len(infos) == 0 {
		n := min(stripeRowLimit, len(rowsLeft))

		info, serr := writeStripe(&out, tree, cols, rowsLeft[:n], codec)
		if serr != nil {
			return serr
		}

		infos = append(infos, info)
		rowsLeft = rowsLeft[n:]

		if len(rows) == 0 {
			break
		}
	}

	return finish(w, &out, tree, infos, u64(len(rows)), codec)
}

type node struct {
	typ      *hivetype.Type
	attrs    map[string]string
	name     string
	children []*node
	id       int
	kind     int
}

func buildTree(cols []Column) ([]*node, error) {
	root := &node{kind: kindStruct}
	next := 1

	for _, c := range cols {
		t, err := hivetype.Parse(c.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", c.Name, err)
		}

		n, err := newNode(t, &next, c.Name, c.NestedIDs)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", c.Name, err)
		}

		n.name = c.Name

		if c.FieldID > 0 {
			n.attrs = map[string]string{icebergIDAttr: strconv.Itoa(c.FieldID), "iceberg.required": "false"}
		}

		root.children = append(root.children, n)
	}

	return flatten(root), nil
}

// orcKind maps hivetype.Kind (in declaration order) to the ORC type kind.
func orcKind(k hivetype.Kind) int {
	return []int{
		kindBoolean, kindByte, kindShort, kindInt, kindLong, kindFloat, kindDouble, kindString, kindVarchar,
		kindChar, kindBinary, kindDate, kindTimestamp, kindDecimal, kindList, kindMap, kindStruct,
	}[k]
}

func newNode(t *hivetype.Type, next *int, lpath string, ids map[string]int) (*node, error) {
	n := &node{typ: t, id: *next, kind: orcKind(t.Kind)}
	*next++

	if id := ids[lpath]; id > 0 {
		n.attrs = map[string]string{icebergIDAttr: strconv.Itoa(id), "iceberg.required": "false"}
	}

	switch t.Kind {
	case hivetype.Array:
		return n, addChildren(n, next, lpath, ids, []string{"element"}, t.Elem)
	case hivetype.Map:
		return n, addChildren(n, next, lpath, ids, []string{"key", "value"}, t.Key, t.Value)
	case hivetype.Struct:
		for _, f := range t.Fields {
			if err := addChildren(n, next, lpath, ids, []string{f.Name}, f.Type); err != nil {
				return nil, err
			}

			n.children[len(n.children)-1].name = f.Name
		}
	default:
	}

	return n, nil
}

func addChildren(n *node, next *int, lpath string, ids map[string]int, names []string, types ...*hivetype.Type) error {
	for i, t := range types {
		c, err := newNode(t, next, lpath+"."+names[i], ids)
		if err != nil {
			return err
		}

		n.children = append(n.children, c)
	}

	return nil
}

func flatten(root *node) []*node {
	out := []*node{root}

	var walkNode func(n *node)

	walkNode = func(n *node) {
		for _, c := range n.children {
			out = append(out, c)
			walkNode(c)
		}
	}

	walkNode(root)

	return out
}

type stripeBuf struct {
	streams  []streamInfo
	payloads [][]byte
	encoding []int
	codec    int
}

func (sb *stripeBuf) add(col, kind int, raw []byte) error {
	body, err := compress(sb.codec, defaultBlockSize, raw)
	if err != nil {
		return err
	}

	sb.streams = append(sb.streams, streamInfo{kind: kind, column: col, length: u64(len(body))})
	sb.payloads = append(sb.payloads, body)

	return nil
}

func writeStripe(out *bytes.Buffer, tree []*node, cols []Column, rows [][]any, codec int) (stripeInfo, error) {
	sb := &stripeBuf{codec: codec, encoding: make([]int, len(tree))}

	for i, top := range tree[0].children {
		cells := make([]any, len(rows))

		for r, row := range rows {
			if i < len(row) {
				cells[r] = row[i]
			}
		}

		if err := sb.column(top, cells); err != nil {
			return stripeInfo{}, fmt.Errorf("column %q: %w", cols[i].Name, err)
		}
	}

	info := stripeInfo{offset: u64(out.Len()), rows: u64(len(rows))}

	for _, p := range sb.payloads {
		out.Write(p)
		info.dataLen += u64(len(p))
	}

	foot := stripeFooterBytes(sb)

	body, err := compress(codec, defaultBlockSize, foot)
	if err != nil {
		return info, err
	}

	out.Write(body)

	info.footerLen = u64(len(body))

	return info, nil
}

func stripeFooterBytes(sb *stripeBuf) []byte {
	var b []byte

	for _, s := range sb.streams {
		var m []byte

		m = protowire.AppendTag(m, 1, protowire.VarintType)
		m = protowire.AppendVarint(m, u64(s.kind))
		m = protowire.AppendTag(m, 2, protowire.VarintType)
		m = protowire.AppendVarint(m, u64(s.column))
		m = protowire.AppendTag(m, 3, protowire.VarintType)
		m = protowire.AppendVarint(m, s.length)
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendBytes(b, m)
	}

	for _, e := range sb.encoding {
		var m []byte

		m = protowire.AppendTag(m, 1, protowire.VarintType)
		m = protowire.AppendVarint(m, u64(e))
		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendBytes(b, m)
	}

	b = protowire.AppendTag(b, 3, protowire.BytesType)

	return protowire.AppendString(b, "UTC")
}

func finish(w io.Writer, out *bytes.Buffer, tree []*node, infos []stripeInfo, rows uint64, codec int) error {
	content := u64(out.Len() - len(orcMagic))

	var foot []byte

	foot = protowire.AppendTag(foot, 1, protowire.VarintType)
	foot = protowire.AppendVarint(foot, u64(len(orcMagic)))
	foot = protowire.AppendTag(foot, 2, protowire.VarintType)
	foot = protowire.AppendVarint(foot, content)

	for _, s := range infos {
		var m []byte

		for i, v := range []uint64{s.offset, 0, s.dataLen, s.footerLen, s.rows} {
			m = protowire.AppendTag(m, protowire.Number(i+1), protowire.VarintType)
			m = protowire.AppendVarint(m, v)
		}

		foot = protowire.AppendTag(foot, 3, protowire.BytesType)
		foot = protowire.AppendBytes(foot, m)
	}

	for _, n := range tree {
		foot = protowire.AppendTag(foot, 4, protowire.BytesType)
		foot = protowire.AppendBytes(foot, typeBytes(n))
	}

	foot = protowire.AppendTag(foot, 6, protowire.VarintType)
	foot = protowire.AppendVarint(foot, rows)

	for range tree {
		var st []byte

		st = protowire.AppendTag(st, 1, protowire.VarintType)
		st = protowire.AppendVarint(st, rows)
		foot = protowire.AppendTag(foot, 7, protowire.BytesType)
		foot = protowire.AppendBytes(foot, st)
	}

	foot = protowire.AppendTag(foot, 12, protowire.BytesType)
	foot = protowire.AppendString(foot, softwareName)

	body, err := compress(codec, defaultBlockSize, foot)
	if err != nil {
		return err
	}

	out.Write(body)

	var ps []byte

	ps = protowire.AppendTag(ps, psFieldFooter, protowire.VarintType)
	ps = protowire.AppendVarint(ps, u64(len(body)))
	ps = protowire.AppendTag(ps, 2, protowire.VarintType)
	ps = protowire.AppendVarint(ps, u64(codec))
	ps = protowire.AppendTag(ps, 3, protowire.VarintType)
	ps = protowire.AppendVarint(ps, defaultBlockSize)
	ps = protowire.AppendTag(ps, 4, protowire.BytesType)
	ps = protowire.AppendBytes(ps, []byte{versionMajor, versionMinor})
	ps = protowire.AppendTag(ps, 5, protowire.VarintType)
	ps = protowire.AppendVarint(ps, 0)
	ps = protowire.AppendTag(ps, 6, protowire.VarintType)
	ps = protowire.AppendVarint(ps, writerVersion)
	ps = protowire.AppendTag(ps, 8000, protowire.BytesType)
	ps = protowire.AppendString(ps, orcMagic)

	out.Write(ps)
	out.WriteByte(lowByte(u64(len(ps))))

	_, err = w.Write(out.Bytes())

	return err
}

func typeBytes(n *node) []byte {
	var b []byte

	b = protowire.AppendTag(b, 1, protowire.VarintType)
	b = protowire.AppendVarint(b, u64(n.kind))

	if len(n.children) > 0 {
		var packed []byte
		for _, c := range n.children {
			packed = protowire.AppendVarint(packed, u64(c.id))
		}

		b = protowire.AppendTag(b, 2, protowire.BytesType)
		b = protowire.AppendBytes(b, packed)
	}

	if n.kind == kindStruct {
		for _, c := range n.children {
			b = protowire.AppendTag(b, 3, protowire.BytesType)
			b = protowire.AppendString(b, c.name)
		}
	}

	if n.typ != nil {
		b = appendTypeParams(b, n)
	}

	for k, v := range n.attrs {
		var kv []byte

		kv = protowire.AppendTag(kv, 1, protowire.BytesType)
		kv = protowire.AppendString(kv, k)
		kv = protowire.AppendTag(kv, 2, protowire.BytesType)
		kv = protowire.AppendString(kv, v)
		b = protowire.AppendTag(b, 7, protowire.BytesType)
		b = protowire.AppendBytes(b, kv)
	}

	return b
}

func appendTypeParams(b []byte, n *node) []byte {
	switch n.kind {
	case kindVarchar, kindChar:
		b = protowire.AppendTag(b, 4, protowire.VarintType)
		b = protowire.AppendVarint(b, u64(n.typ.Len))
	case kindDecimal:
		b = protowire.AppendTag(b, 5, protowire.VarintType)
		b = protowire.AppendVarint(b, u64(n.typ.Precision))
		b = protowire.AppendTag(b, 6, protowire.VarintType)
		b = protowire.AppendVarint(b, u64(n.typ.Scale))
	}

	return b
}

// column writes the streams of node n for vals (nil entries are nulls) and its descendants.
func (sb *stripeBuf) column(n *node, vals []any) error {
	nonNull := make([]any, 0, len(vals))
	present := make([]bool, len(vals))
	hasNull := false

	for i, v := range vals {
		if v == nil {
			hasNull = true

			continue
		}

		present[i] = true

		nonNull = append(nonNull, v)
	}

	if hasNull {
		if err := sb.add(n.id, streamPresent, encodeBoolRLE(present)); err != nil {
			return err
		}
	}

	return sb.values(n, nonNull)
}

func (sb *stripeBuf) values(n *node, vals []any) error {
	switch n.kind {
	case kindBoolean:
		return sb.boolean(n, vals)
	case kindByte:
		return sb.bytesCol(n, vals)
	case kindShort, kindInt, kindLong:
		return sb.integers(n, vals)
	case kindFloat, kindDouble:
		return sb.floating(n, vals)
	case kindString, kindVarchar, kindChar, kindBinary:
		return sb.text(n, vals)
	case kindDate:
		return sb.dates(n, vals)
	case kindTimestamp:
		return sb.timestamps(n, vals)
	case kindDecimal:
		return sb.decimals(n, vals)
	case kindStruct:
		return sb.structs(n, vals)
	case kindList:
		return sb.lists(n, vals)
	case kindMap:
		return sb.maps(n, vals)
	default:
		return fmt.Errorf("%w: type kind %d", ErrUnsupported, n.kind)
	}
}

func (sb *stripeBuf) boolean(n *node, vals []any) error {
	b := make([]bool, len(vals))

	for i, v := range vals {
		x, err := toBool(v)
		if err != nil {
			return err
		}

		b[i] = x
	}

	return sb.add(n.id, streamData, encodeBoolRLE(b))
}

func (sb *stripeBuf) bytesCol(n *node, vals []any) error {
	b := make([]byte, len(vals))

	for i, v := range vals {
		x, err := toInt64(v)
		if err != nil {
			return err
		}

		b[i] = lowByte(asBits(x))
	}

	sb.encoding[n.id] = encDirect

	return sb.add(n.id, streamData, encodeByteRLE(b))
}

func (sb *stripeBuf) ints(n *node, kind int, vals []int64, signed bool) error {
	sb.encoding[n.id] = encDirectV2

	return sb.add(n.id, kind, encodeIntsV2(vals, signed))
}

func (sb *stripeBuf) integers(n *node, vals []any) error {
	out := make([]int64, len(vals))

	for i, v := range vals {
		x, err := toInt64(v)
		if err != nil {
			return err
		}

		out[i] = x
	}

	return sb.ints(n, streamData, out, true)
}

func (sb *stripeBuf) floating(n *node, vals []any) error {
	var b []byte

	for _, v := range vals {
		f, err := toFloat(v)
		if err != nil {
			return err
		}

		if n.kind == kindFloat {
			b = appendFloat32(b, f)
		} else {
			b = appendFloat64(b, f)
		}
	}

	return sb.add(n.id, streamData, b)
}

func (sb *stripeBuf) text(n *node, vals []any) error {
	var data []byte

	lens := make([]int64, len(vals))

	for i, v := range vals {
		s := asString(v)
		if n.kind == kindChar && n.typ.Len > 0 {
			s = strings.TrimRight(s, " ")
		}

		lens[i] = int64(len(s))
		data = append(data, s...)
	}

	if err := sb.add(n.id, streamData, data); err != nil {
		return err
	}

	return sb.ints(n, streamLength, lens, false)
}

func (sb *stripeBuf) dates(n *node, vals []any) error {
	out := make([]int64, len(vals))

	for i, v := range vals {
		d, err := toDays(v)
		if err != nil {
			return err
		}

		out[i] = d
	}

	return sb.ints(n, streamData, out, true)
}

func (sb *stripeBuf) timestamps(n *node, vals []any) error {
	secs := make([]int64, len(vals))
	nanos := make([]int64, len(vals))

	for i, v := range vals {
		t, err := toTime(v)
		if err != nil {
			return err
		}

		secs[i] = t.Unix() - timestampBaseUnix
		nanos[i] = encodeNanos(int64(t.Nanosecond()))
	}

	if err := sb.ints(n, streamData, secs, true); err != nil {
		return err
	}

	return sb.add(n.id, streamSecondary, encodeIntsV2(nanos, false))
}

func encodeNanos(n int64) int64 {
	if n == 0 {
		return 0
	}

	if n%100 != 0 {
		return n << 3
	}

	n /= 100

	zeros := int64(1)
	for n%decimalBase == 0 && zeros < maxNanoZeros {
		n /= decimalBase
		zeros++
	}

	return n<<3 | zeros
}

func (sb *stripeBuf) decimals(n *node, vals []any) error {
	var data []byte

	scales := make([]int64, len(vals))

	for i, v := range vals {
		u, err := toDecimal(v, n.typ.Scale)
		if err != nil {
			return err
		}

		data = appendBigVarint(data, u)
		scales[i] = int64(n.typ.Scale)
	}

	if err := sb.add(n.id, streamData, data); err != nil {
		return err
	}

	sb.encoding[n.id] = encDirectV2

	return sb.add(n.id, streamSecondary, encodeIntsV2(scales, true))
}

func (sb *stripeBuf) structs(n *node, vals []any) error {
	rows := make([]map[string]any, len(vals))

	for i, v := range vals {
		m, err := toObject(v)
		if err != nil {
			return err
		}

		rows[i] = m
	}

	for _, c := range n.children {
		col := make([]any, len(rows))
		for i, m := range rows {
			col[i] = lookupField(m, c.name)
		}

		if err := sb.column(c, col); err != nil {
			return err
		}
	}

	return nil
}

func (sb *stripeBuf) lists(n *node, vals []any) error {
	lens := make([]int64, len(vals))

	var elems []any

	for i, v := range vals {
		arr, err := toArray(v)
		if err != nil {
			return err
		}

		lens[i] = int64(len(arr))
		elems = append(elems, arr...)
	}

	if err := sb.ints(n, streamLength, lens, false); err != nil {
		return err
	}

	return sb.column(n.children[0], elems)
}

func (sb *stripeBuf) maps(n *node, vals []any) error {
	lens := make([]int64, len(vals))

	var keys, items []any

	for i, v := range vals {
		obj, err := toObject(v)
		if err != nil {
			return err
		}

		lens[i] = int64(len(obj))

		for _, k := range sortedKeys(obj) {
			keys = append(keys, k)
			items = append(items, obj[k])
		}
	}

	if err := sb.ints(n, streamLength, lens, false); err != nil {
		return err
	}

	if err := sb.column(n.children[0], keys); err != nil {
		return err
	}

	return sb.column(n.children[1], items)
}
