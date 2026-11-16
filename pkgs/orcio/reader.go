package orcio

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	mapChildren    = 2
	tsMillisLayout = "2006-01-02 15:04:05.000"
	dateLayout     = "2006-01-02"
	secondsPerDay  = 86400
	nanosPerMilli  = 1_000_000
	decimalBase    = 10
	maxNanoZeros   = 7
	tailLenBytes   = 1
)

// File is an opened ORC file held in memory.
type File struct {
	names     map[string]int
	cols      []string
	data      []byte
	foot      footer
	ps        postScript
	tailBytes int64
}

// Open parses the tail (postscript and footer) of an in-memory ORC file.
func Open(data []byte) (*File, error) {
	if len(data) < len(orcMagic)+tailLenBytes+1 || string(data[:len(orcMagic)]) != orcMagic {
		return nil, fmt.Errorf("%w: missing ORC header", ErrBadFile)
	}

	psLen := int(data[len(data)-1])
	if psLen+tailLenBytes+len(orcMagic) > len(data) {
		return nil, fmt.Errorf("%w: postscript overruns file", ErrBadFile)
	}

	ps, err := parsePostScript(data[len(data)-tailLenBytes-psLen : len(data)-tailLenBytes])
	if err != nil {
		return nil, err
	}

	end := len(data) - tailLenBytes - psLen

	footLen := capInt(ps.footerLen)
	if footLen > end-len(orcMagic) || capInt(ps.metadataLen) > end-len(orcMagic)-footLen {
		return nil, fmt.Errorf("%w: footer overruns file", ErrBadFile)
	}

	switch ps.compression {
	case compNone, compZlib, compSnappy, compLZ4, compZstd:
	default:
		return nil, fmt.Errorf("%w: compression kind %d", ErrUnsupported, ps.compression)
	}

	raw, err := decompress(ps.compression, ps.blockSize, data[end-footLen:end])
	if err != nil {
		return nil, err
	}

	foot, err := parseFooter(raw)
	if err != nil {
		return nil, err
	}

	if err = validateFooter(foot, len(data)); err != nil {
		return nil, err
	}

	f := &File{
		data: data, ps: ps, foot: foot, names: map[string]int{},
		tailBytes: int64(footLen) + capInt64(ps.metadataLen) + int64(psLen) + tailLenBytes,
	}
	root := foot.types[0]
	f.cols = root.fieldName

	for i, n := range root.fieldName {
		if i < len(root.subtypes) {
			f.names[strings.ToLower(n)] = root.subtypes[i]
		}
	}

	return f, nil
}

// NumRows is the total row count from the footer.
func (f *File) NumRows() int64 { return capInt64(f.foot.rows) }

// Columns lists the top-level column names in file order.
func (f *File) Columns() []string { return f.cols }

// ColumnIDs maps each top-level Iceberg field id (the iceberg.id type attribute) to its column name.
func (f *File) ColumnIDs() map[int]string {
	out := map[int]string{}
	root := f.foot.types[0]

	for i, sub := range root.subtypes {
		if i >= len(root.fieldName) || sub >= len(f.foot.types) {
			continue
		}

		if id, err := strconv.Atoi(f.foot.types[sub].attrs[icebergIDAttr]); err == nil && id > 0 {
			out[id] = root.fieldName[i]
		}
	}

	return out
}

// ScanBytes is the tail plus the length of every stripe stream belonging to the named top-level columns.
func (f *File) ScanBytes(cols []string) int64 {
	want := f.columnSet(cols)
	total := f.tailBytes + int64(len(orcMagic))

	for _, s := range f.foot.stripes {
		sf, err := f.stripeFooter(s)
		if err != nil {
			continue
		}

		for _, st := range sf.streams {
			if want[st.column] {
				total += capInt64(st.length)
			}
		}
	}

	return total
}

// columnSet expands the named top-level columns to every type id beneath them.
func (f *File) columnSet(cols []string) map[int]bool {
	want := map[int]bool{}

	var add func(id int)

	add = func(id int) {
		if want[id] || id >= len(f.foot.types) {
			return
		}

		want[id] = true

		for _, s := range f.foot.types[id].subtypes {
			add(s)
		}
	}

	for _, c := range cols {
		if id, ok := f.names[strings.ToLower(c)]; ok {
			add(id)
		}
	}

	return want
}

func (f *File) stripeFooter(s stripeInfo) (stripeFooter, error) {
	start := s.offset + s.indexLen + s.dataLen
	if start+s.footerLen > uint64(len(f.data)) { // sums are bounded by validateFooter
		return stripeFooter{}, fmt.Errorf("%w: stripe footer overruns file", ErrBadFile)
	}

	raw, err := decompress(f.ps.compression, f.ps.blockSize, f.data[start:start+s.footerLen])
	if err != nil {
		return stripeFooter{}, err
	}

	return parseStripeFooter(raw)
}

// ForEach streams every row, yielding one value per requested column (case-insensitive;
// nil when null or absent). Leaf values are bool, int64, float64 or string (date, timestamp
// and decimal are formatted as Athena prints them); nested columns yield map[string]any / []any.
func (f *File) ForEach(cols []string, fn func(vals []any) error) error {
	for _, s := range f.foot.stripes {
		batch, err := f.readStripe(s, cols)
		if err != nil {
			return err
		}

		for r := range capInt(s.rows) {
			vals := make([]any, len(cols))

			for i := range cols {
				if batch[i] != nil {
					vals[i] = batch[i][r]
				}
			}

			if err = fn(vals); err != nil {
				return err
			}
		}
	}

	return nil
}

type stripeReader struct {
	f       *File
	streams map[[2]int][]byte
	enc     []int
	dict    []int
}

func (f *File) readStripe(s stripeInfo, cols []string) (_ [][]any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrBadFile, r)
		}
	}()

	sf, err := f.stripeFooter(s)
	if err != nil {
		return nil, err
	}

	want := f.columnSet(cols)
	sr := &stripeReader{f: f, streams: map[[2]int][]byte{}, enc: sf.encodings, dict: sf.dictSizes}
	pos := s.offset

	for _, st := range sf.streams {
		if st.length > uint64(len(f.data)) || pos+st.length > uint64(len(f.data)) {
			return nil, fmt.Errorf("%w: stream overruns file", ErrBadFile)
		}

		if want[st.column] && st.kind != streamRowIndex && st.kind < streamRowIndex {
			raw, derr := decompress(f.ps.compression, f.ps.blockSize, f.data[pos:pos+st.length])
			if derr != nil {
				return nil, derr
			}

			sr.streams[[2]int{st.column, st.kind}] = raw
		}

		pos += st.length
	}

	out := make([][]any, len(cols))
	rows := capInt(s.rows)

	for i, c := range cols {
		id, ok := f.names[strings.ToLower(c)]
		if !ok {
			continue
		}

		if out[i], err = sr.column(id, rows); err != nil {
			return nil, err
		}
	}

	return out, nil
}

func (sr *stripeReader) v2(id int) bool {
	return id < len(sr.enc) && (sr.enc[id] == encDirectV2 || sr.enc[id] == encDictionaryV2)
}

func (sr *stripeReader) stream(id, kind int) []byte { return sr.streams[[2]int{id, kind}] }

// column decodes n entries of one column (nulls included) into Go values.
func (sr *stripeReader) column(id, n int) ([]any, error) {
	present := make([]bool, n)

	if raw, ok := sr.streams[[2]int{id, streamPresent}]; ok {
		p, err := decodeBoolRLE(raw, n)
		if err != nil {
			return nil, err
		}

		present = p
	} else {
		for i := range present {
			present[i] = true
		}
	}

	m := 0

	for _, p := range present {
		if p {
			m++
		}
	}

	vals, err := sr.values(id, m)
	if err != nil {
		return nil, err
	}

	out := make([]any, n)
	j := 0

	for i, p := range present {
		if p {
			out[i] = vals[j]
			j++
		}
	}

	return out, nil
}

func (sr *stripeReader) values(id, m int) ([]any, error) {
	t := sr.f.foot.types[id]

	switch t.kind {
	case kindBoolean:
		b, err := decodeBoolRLE(sr.stream(id, streamData), m)

		return mapSlice(b, func(v bool) any { return v }), err
	case kindByte:
		b, err := decodeByteRLE(sr.stream(id, streamData), m)

		return mapSlice(b, func(v byte) any { return signedByte(v) }), err
	case kindShort, kindInt, kindLong:
		n, err := decodeInts(sr.stream(id, streamData), m, true, sr.v2(id))

		return mapSlice(n, func(v int64) any { return v }), err
	case kindFloat, kindDouble:
		return floats(sr.stream(id, streamData), m, t.kind == kindFloat)
	case kindString, kindVarchar, kindChar, kindBinary:
		return sr.strings(id, m, t.kind == kindBinary)
	case kindDate:
		n, err := decodeInts(sr.stream(id, streamData), m, true, sr.v2(id))

		return mapSlice(n, func(d int64) any { return time.Unix(d*secondsPerDay, 0).UTC().Format(dateLayout) }), err
	case kindTimestamp, kindTimestampInstant:
		return sr.timestamps(id, m)
	case kindDecimal:
		return sr.decimals(id, m, t.scale)
	case kindStruct:
		return sr.structs(id, m)
	case kindList:
		return sr.lists(id, m)
	case kindMap:
		return sr.maps(id, m)
	default:
		return nil, fmt.Errorf("%w: column type kind %d", ErrUnsupported, t.kind)
	}
}

func mapSlice[T any](in []T, fn func(T) any) []any {
	out := make([]any, len(in))
	for i, v := range in {
		out[i] = fn(v)
	}

	return out
}

func floats(b []byte, m int, single bool) ([]any, error) {
	width := 8
	if single {
		width = 4
	}

	if len(b) < m*width {
		return nil, fmt.Errorf("%w: float stream too short", ErrBadFile)
	}

	out := make([]any, m)

	for i := range out {
		if single {
			f := math.Float32frombits(binary.LittleEndian.Uint32(b[i*4:]))

			d, err := strconv.ParseFloat(strconv.FormatFloat(float64(f), 'g', -1, 32), 64)
			if err != nil {
				d = float64(f)
			}

			out[i] = d
		} else {
			out[i] = math.Float64frombits(binary.LittleEndian.Uint64(b[i*8:]))
		}
	}

	return out, nil
}

func bytesString(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}

	return hex.EncodeToString(b)
}

func (sr *stripeReader) strings(id, m int, binary bool) ([]any, error) {
	conv := func(b []byte) any { return string(b) }
	if binary {
		conv = func(b []byte) any { return bytesString(b) }
	}

	v2 := sr.v2(id)
	out := make([]any, m)

	if id < len(sr.enc) && (sr.enc[id] == encDictionary || sr.enc[id] == encDictionaryV2) {
		return sr.dictionary(id, m, v2, conv)
	}

	lens, err := decodeInts(sr.stream(id, streamLength), m, false, v2)
	if err != nil {
		return nil, err
	}

	data := &byteReader{b: sr.stream(id, streamData)}

	for i, l := range lens {
		b, terr := data.take(int(l))
		if terr != nil {
			return nil, terr
		}

		out[i] = conv(b)
	}

	return out, nil
}

func (sr *stripeReader) dictionary(id, m int, v2 bool, conv func([]byte) any) ([]any, error) {
	size := 0
	if id < len(sr.dict) {
		size = sr.dict[id]
	}

	if size < 0 || size > maxValues {
		return nil, fmt.Errorf("%w: dictionary size %d", ErrBadFile, size)
	}

	lens, err := decodeInts(sr.stream(id, streamLength), size, false, v2)
	if err != nil {
		return nil, err
	}

	data := &byteReader{b: sr.stream(id, streamDictionaryData)}
	dict := make([]any, size)

	for i, l := range lens {
		b, terr := data.take(int(l))
		if terr != nil {
			return nil, terr
		}

		dict[i] = conv(b)
	}

	idx, err := decodeInts(sr.stream(id, streamData), m, false, v2)
	if err != nil {
		return nil, err
	}

	out := make([]any, m)

	for i, k := range idx {
		if k < 0 || int(k) >= len(dict) {
			return nil, fmt.Errorf("%w: dictionary index %d out of range", ErrBadFile, k)
		}

		out[i] = dict[k]
	}

	return out, nil
}

func (sr *stripeReader) timestamps(id, m int) ([]any, error) {
	secs, err := decodeInts(sr.stream(id, streamData), m, true, sr.v2(id))
	if err != nil {
		return nil, err
	}

	nanos, err := decodeInts(sr.stream(id, streamSecondary), m, false, sr.v2(id))
	if err != nil {
		return nil, err
	}

	out := make([]any, m)

	for i := range out {
		z := nanos[i] & nanoZeroMask

		n := nanos[i] >> nanoShift
		if z != 0 {
			for range z + 1 {
				n *= decimalBase
			}
		}

		out[i] = time.Unix(secs[i]+timestampBaseUnix, n).UTC().Format(tsMillisLayout)
	}

	return out, nil
}

func (sr *stripeReader) decimals(id, m, scale int) ([]any, error) {
	r := &byteReader{b: sr.stream(id, streamData)}

	scales, err := decodeInts(sr.stream(id, streamSecondary), m, true, sr.v2(id))
	if err != nil {
		return nil, err
	}

	out := make([]any, m)

	for i := range out {
		u, verr := bigVarint(r)
		if verr != nil {
			return nil, verr
		}

		if scales[i] < 0 || scales[i] > maxDecimalScale || scale < 0 || scale > maxDecimalScale {
			return nil, fmt.Errorf("%w: decimal scale %d", ErrBadFile, scales[i])
		}

		s := int(scales[i])
		for ; s < scale; s++ {
			u.Mul(u, big.NewInt(decimalBase))
		}

		for ; s > scale; s-- {
			u.Quo(u, big.NewInt(decimalBase))
		}

		out[i] = formatDecimal(u, scale)
	}

	return out, nil
}

// bigVarint reads an unbounded zigzag base-128 varint.
func bigVarint(r *byteReader) (*big.Int, error) {
	u := new(big.Int)

	for shift := uint(0); ; shift += varintShift {
		if shift >= maxDecimalBytes*varintShift {
			return nil, fmt.Errorf("%w: decimal varint too long", ErrBadFile)
		}

		c, err := r.byte()
		if err != nil {
			return nil, err
		}

		u.Or(u, new(big.Int).Lsh(big.NewInt(int64(c&varintPayload)), shift))

		if c&varintMore == 0 {
			break
		}
	}

	neg := u.Bit(0) == 1
	u.Rsh(u, 1)

	if neg {
		u.Neg(u)
		u.Sub(u, big.NewInt(1))
	}

	return u, nil
}

func formatDecimal(u *big.Int, scale int) string {
	s := new(big.Int).Abs(u).String()

	if scale > 0 {
		if len(s) <= scale {
			s = strings.Repeat("0", scale-len(s)+1) + s
		}

		s = s[:len(s)-scale] + "." + s[len(s)-scale:]
	}

	if u.Sign() < 0 {
		s = "-" + s
	}

	return s
}

func (sr *stripeReader) structs(id, m int) ([]any, error) {
	t := sr.f.foot.types[id]
	kids := make([][]any, len(t.subtypes))

	for i, sub := range t.subtypes {
		var err error
		if kids[i], err = sr.column(sub, m); err != nil {
			return nil, err
		}
	}

	out := make([]any, m)

	for r := range out {
		row := make(map[string]any, len(kids))

		for i := range kids {
			if i < len(t.fieldName) {
				row[t.fieldName[i]] = kids[i][r]
			}
		}

		out[r] = row
	}

	return out, nil
}

func (sr *stripeReader) lengths(id, m int) ([]int64, int, error) {
	lens, err := decodeInts(sr.stream(id, streamLength), m, false, sr.v2(id))
	if err != nil {
		return nil, 0, err
	}

	total := 0
	for _, l := range lens {
		if l < 0 || l > maxValues {
			return nil, 0, fmt.Errorf("%w: entry length %d", ErrBadFile, l)
		}

		if total += int(l); total > maxValues {
			return nil, 0, fmt.Errorf("%w: more than %d nested values", ErrBadFile, maxValues)
		}
	}

	return lens, total, nil
}

func (sr *stripeReader) lists(id, m int) ([]any, error) {
	t := sr.f.foot.types[id]

	lens, total, err := sr.lengths(id, m)
	if err != nil {
		return nil, err
	}

	elems, err := sr.column(t.subtypes[0], total)
	if err != nil {
		return nil, err
	}

	out := make([]any, m)
	pos := 0

	for i, l := range lens {
		out[i] = append([]any{}, elems[pos:pos+int(l)]...)
		pos += int(l)
	}

	return out, nil
}

func (sr *stripeReader) maps(id, m int) ([]any, error) {
	t := sr.f.foot.types[id]

	lens, total, err := sr.lengths(id, m)
	if err != nil {
		return nil, err
	}

	keys, err := sr.column(t.subtypes[0], total)
	if err != nil {
		return nil, err
	}

	vals, err := sr.column(t.subtypes[1], total)
	if err != nil {
		return nil, err
	}

	out := make([]any, m)
	pos := 0

	for i, l := range lens {
		obj := make(map[string]any, l)

		for j := range int(l) {
			obj[fmt.Sprint(keys[pos+j])] = vals[pos+j]
		}

		out[i] = obj
		pos += int(l)
	}

	return out, nil
}

// validateFooter checks the type tree and stripe table against the file so no later read can loop,
// recurse without bound or allocate more than the file can justify.
func validateFooter(f footer, size int) error {
	if len(f.types) == 0 || f.types[0].kind != kindStruct {
		return fmt.Errorf("%w: root type is not a struct", ErrBadFile)
	}

	if err := validateTypes(f.types); err != nil {
		return err
	}

	limit := min(uint64(maxValues), valuesPerByte*(u64(size)+1))

	var total uint64

	for _, s := range f.stripes {
		for _, v := range []uint64{s.offset, s.indexLen, s.dataLen, s.footerLen} {
			if v > u64(size) {
				return fmt.Errorf("%w: stripe extent exceeds the file", ErrBadFile)
			}
		}

		if s.offset+s.indexLen+s.dataLen+s.footerLen > u64(size) || s.rows > limit {
			return fmt.Errorf("%w: stripe extent or row count exceeds the file", ErrBadFile)
		}

		total += s.rows
	}

	if total != f.rows && f.rows <= limit*uint64(len(f.stripes)+1) {
		return fmt.Errorf("%w: stripe rows %d do not add up to %d", ErrBadFile, total, f.rows)
	}

	return nil
}

func validateTypes(types []typeInfo) error {
	depth := make([]int, len(types))

	for i := len(types) - 1; i >= 0; i-- {
		if err := validateType(types[i], i, len(types)); err != nil {
			return err
		}

		d := 1
		for _, c := range types[i].subtypes {
			d = max(d, depth[c]+1)
		}

		if depth[i] = d; d > maxNesting {
			return fmt.Errorf("%w: types nest deeper than %d", ErrBadFile, maxNesting)
		}
	}

	return nil
}

func validateType(t typeInfo, i, count int) error {
	if t.kind < 0 || t.kind > kindTimestampInstant {
		return fmt.Errorf("%w: unknown type kind %d", ErrUnsupported, t.kind)
	}

	if n, ok := childCount(t.kind); ok && len(t.subtypes) != n {
		return fmt.Errorf("%w: type %d has %d children", ErrBadFile, i, len(t.subtypes))
	}

	if t.kind == kindStruct && len(t.subtypes) != len(t.fieldName) {
		return fmt.Errorf("%w: struct %d field names and children differ", ErrBadFile, i)
	}

	if t.kind == kindDecimal && (t.scale > maxDecimalScale || t.precision > maxDecimalScale) {
		return fmt.Errorf("%w: decimal precision %d scale %d", ErrBadFile, t.precision, t.scale)
	}

	for _, c := range t.subtypes {
		if c <= i || c >= count {
			return fmt.Errorf("%w: type %d references child %d", ErrBadFile, i, c)
		}
	}

	return nil
}

func childCount(kind int) (int, bool) {
	switch kind {
	case kindList:
		return 1, true
	case kindMap:
		return mapChildren, true
	default:
		return 0, false
	}
}
