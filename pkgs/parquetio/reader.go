package parquetio

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

const (
	rowBatch        = 256
	julianUnixEpoch = 2440588
	nanosPerSecond  = int64(time.Second)
	tsMillisLayout  = "2006-01-02 15:04:05.000"
	dateLayout      = "2006-01-02"
	timeLayout      = "15:04:05.000"
	millisPerSecond = 1000
	microsPerSecond = 1_000_000
)

// File is an opened Parquet file held in memory.
type File struct {
	pf    *parquet.File
	names map[string]string
}

// Open parses the footer of an in-memory Parquet file.
func Open(data []byte) (*File, error) {
	pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadFile, err)
	}

	f := &File{pf: pf, names: map[string]string{}}

	for _, fld := range pf.Schema().Fields() {
		f.names[strings.ToLower(fld.Name())] = fld.Name()
	}

	return f, nil
}

// NumRows is the total row count from the footer.
func (f *File) NumRows() int64 { return f.pf.NumRows() }

// MaxRowGroupBytes is the largest uncompressed row-group size in the footer.
func (f *File) MaxRowGroupBytes() int64 {
	var most int64

	for _, rg := range f.pf.Metadata().RowGroups {
		most = max(most, rg.TotalByteSize)
	}

	return most
}

// Columns lists the top-level column names in file order.
func (f *File) Columns() []string {
	fields := f.pf.Schema().Fields()
	out := make([]string, len(fields))

	for i, fld := range fields {
		out[i] = fld.Name()
	}

	return out
}

// ColumnIDs maps each top-level Parquet field id to its column name.
func (f *File) ColumnIDs() map[int]string {
	out := map[int]string{}

	for _, fld := range f.pf.Schema().Fields() {
		if id := fld.ID(); id > 0 {
			out[id] = fld.Name()
		}
	}

	return out
}

// ScanBytes is the footer plus the compressed size of every column chunk
// belonging to the named top-level columns (case-insensitive).
func (f *File) ScanBytes(cols []string) int64 {
	want := map[string]bool{}

	for _, c := range cols {
		if n, ok := f.names[strings.ToLower(c)]; ok {
			want[n] = true
		}
	}

	total := f.pf.Size() - footerStart(f.pf)

	for _, rg := range f.pf.Metadata().RowGroups {
		for _, cc := range rg.Columns {
			if len(cc.MetaData.PathInSchema) > 0 && want[cc.MetaData.PathInSchema[0]] {
				total += cc.MetaData.TotalCompressedSize
			}
		}
	}

	return total
}

func footerStart(pf *parquet.File) int64 {
	var end int64

	for _, rg := range pf.Metadata().RowGroups {
		for _, cc := range rg.Columns {
			off := cc.MetaData.DataPageOffset
			if cc.MetaData.DictionaryPageOffset > 0 && cc.MetaData.DictionaryPageOffset < off {
				off = cc.MetaData.DictionaryPageOffset
			}

			if e := off + cc.MetaData.TotalCompressedSize; e > end {
				end = e
			}
		}
	}

	return end
}

type leafReader struct {
	conv func(parquet.Value) any
	idx  int
}

// ForEach streams every row, yielding one value per requested column
// (case-insensitive match; nil when the column is null or absent). Leaf
// values are bool, int64, float64 or string (date, timestamp, time and
// decimal are formatted as Athena prints them); nested columns yield
// JSON-marshalable map[string]any / []any.
func (f *File) ForEach(cols []string, fn func(vals []any) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrBadFile, r)
		}
	}()

	fields := f.pf.Schema().Fields()
	byName := make(map[string]parquet.Field, len(fields))

	for _, fld := range fields {
		byName[fld.Name()] = fld
	}

	group := parquet.Group{}
	picked := make([]string, len(cols))

	for i, c := range cols {
		if n, ok := f.names[strings.ToLower(c)]; ok {
			picked[i] = n
			group[n] = byName[n]
		}
	}

	if len(group) == 0 {
		return f.countRows(len(cols), fn)
	}

	proj := parquet.NewSchema("projection", group)
	readers := leafReaders(proj, group)
	rd := parquet.NewReader(f.pf, proj)

	defer func() { _ = rd.Close() }()

	buf := make([]parquet.Row, rowBatch)

	for {
		n, rerr := rd.ReadRows(buf)

		for _, row := range buf[:n] {
			vals, verr := rowValues(proj, readers, picked, row)
			if verr != nil {
				return verr
			}

			if err = fn(vals); err != nil {
				return err
			}
		}

		if errors.Is(rerr, io.EOF) {
			return nil
		}

		if rerr != nil {
			return fmt.Errorf("%w: %w", ErrBadFile, rerr)
		}
	}
}

func (f *File) countRows(width int, fn func([]any) error) error {
	for range f.pf.NumRows() {
		if err := fn(make([]any, width)); err != nil {
			return err
		}
	}

	return nil
}

func leafReaders(proj *parquet.Schema, group parquet.Group) map[string]leafReader {
	out := map[string]leafReader{}

	for name, node := range group {
		if node.Leaf() {
			if lc, ok := proj.Lookup(name); ok {
				out[name] = leafReader{idx: lc.ColumnIndex, conv: leafConverter(node)}
			}
		}
	}

	return out
}

func rowValues(
	proj *parquet.Schema, readers map[string]leafReader, picked []string, row parquet.Row,
) ([]any, error) {
	var nested map[string]any

	vals := make([]any, len(picked))

	for i, name := range picked {
		if name == "" {
			continue
		}

		if lr, ok := readers[name]; ok {
			vals[i] = leafValue(row, lr)

			continue
		}

		if nested == nil {
			nested = map[string]any{}
			if err := proj.Reconstruct(&nested, row); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrBadFile, err)
			}
		}

		vals[i] = nestedValue(nested[name])
	}

	return vals, nil
}

func leafValue(row parquet.Row, lr leafReader) any {
	for _, v := range row {
		if v.Column() == lr.idx {
			if v.IsNull() {
				return nil
			}

			return lr.conv(v)
		}
	}

	return nil
}

func nestedValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return bytesString(t)
	case map[string]any:
		for k, x := range t {
			t[k] = nestedValue(x)
		}

		return t
	case []any:
		for i, x := range t {
			t[i] = nestedValue(x)
		}

		return t
	default:
		return v
	}
}

func bytesString(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}

	return hex.EncodeToString(b)
}

func leafConverter(node parquet.Node) func(parquet.Value) any {
	t := node.Type()
	lt := t.LogicalType()

	var logical format.LogicalTypeValue
	if lt != nil {
		logical = lt.Value
	}

	switch l := logical.(type) {
	case *format.DateType:
		return func(v parquet.Value) any {
			return time.Unix(int64(v.Int32())*secondsPerDay, 0).UTC().Format(dateLayout)
		}
	case *format.TimestampType:
		return timestampConv(l.Unit)
	case *format.TimeType:
		return timeConv(l.Unit)
	case *format.DecimalType:
		return decimalConv(int(l.Scale))
	}

	return physicalConv(t.Kind())
}

func unitNanos(u format.TimeUnit) int64 {
	switch u.Value.(type) {
	case *format.MilliSeconds:
		return nanosPerSecond / millisPerSecond
	case *format.NanoSeconds:
		return 1
	default:
		return nanosPerSecond / microsPerSecond
	}
}

func timestampConv(u format.TimeUnit) func(parquet.Value) any {
	per := unitNanos(u)

	return func(v parquet.Value) any {
		n := v.Int64()
		sec, rem := n/(nanosPerSecond/per), n%(nanosPerSecond/per)

		return time.Unix(sec, rem*per).UTC().Format(tsMillisLayout)
	}
}

func timeConv(u format.TimeUnit) func(parquet.Value) any {
	per := unitNanos(u)

	return func(v parquet.Value) any {
		var n int64
		if v.Kind() == parquet.Int32 {
			n = int64(v.Int32())
		} else {
			n = v.Int64()
		}

		return time.Unix(0, n*per).UTC().Format(timeLayout)
	}
}

func decimalConv(scale int) func(parquet.Value) any {
	return func(v parquet.Value) any {
		var u *big.Int

		switch v.Kind() {
		case parquet.Int32:
			u = big.NewInt(int64(v.Int32()))
		case parquet.Int64:
			u = big.NewInt(v.Int64())
		default:
			u = bigFromTwos(v.ByteArray())
		}

		return formatDecimal(u, scale)
	}
}

func bigFromTwos(b []byte) *big.Int {
	u := new(big.Int).SetBytes(b)
	if len(b) > 0 && b[0]&0x80 != 0 {
		u.Sub(u, new(big.Int).Lsh(big.NewInt(1), uint(len(b)*bitsPerByte)))
	}

	return u
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

func physicalConv(k parquet.Kind) func(parquet.Value) any {
	switch k {
	case parquet.Boolean:
		return func(v parquet.Value) any { return v.Boolean() }
	case parquet.Int32:
		return func(v parquet.Value) any { return int64(v.Int32()) }
	case parquet.Int64:
		return func(v parquet.Value) any { return v.Int64() }
	case parquet.Int96:
		return func(v parquet.Value) any { return int96String(v) }
	case parquet.Float:
		return func(v parquet.Value) any { return floatNative(v.Float()) }
	case parquet.Double:
		return func(v parquet.Value) any { return v.Double() }
	default:
		return func(v parquet.Value) any { return bytesString(v.ByteArray()) }
	}
}

func floatNative(f float32) float64 {
	d, err := strconv.ParseFloat(strconv.FormatFloat(float64(f), 'g', -1, 32), 64)
	if err != nil {
		return float64(f)
	}

	return d
}

func int96String(v parquet.Value) string {
	i := v.Int96()
	nanos := int64(uint64(i[0]) | uint64(i[1])<<32) //nolint:gosec // nanos-of-day fits int64
	days := int64(i[2]) - julianUnixEpoch

	return time.Unix(days*secondsPerDay, nanos).UTC().Format(tsMillisLayout)
}
