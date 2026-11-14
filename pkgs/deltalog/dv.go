package deltalog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/bits"
	"path"
)

// DeletionVector is the descriptor of a deletion vector on an add action
// (PROTOCOL.md, "Deletion Vector Descriptor Schema").
type DeletionVector struct {
	Offset         *int64 `json:"offset"`
	StorageType    string `json:"storageType"`
	PathOrInlineDv string `json:"pathOrInlineDv"`
	SizeInBytes    int64  `json:"sizeInBytes"`
	Cardinality    int64  `json:"cardinality"`
}

const (
	dvStorageUUID     = "u"
	dvStorageInline   = "i"
	dvStoragePath     = "p"
	dvMagicPortable   = 1681511377
	dvMagicNative     = 1681511376
	uuidZ85Len        = 20
	roaringCookieNoRC = 12346
	roaringCookieRC   = 12347
	roaringNoOffsetN  = 4
	roaringArrayMax   = 4096
	roaringBitmapSize = 8192
	z85Group          = 5
	z85Bytes          = 4
	z85Base           = 85
	dvFramingBytes    = 4
	maxUint32         = 0xFFFFFFFF
	bitmapCountBytes  = 8
	highKeyShift      = 32
	containerKeyShift = 16
	bitsPerByte       = 8
	bitsPerWord       = 64
)

// ErrBadDeletionVector is returned for a deletion vector that fails validation.
var ErrBadDeletionVector = errors.New("invalid deletion vector")

const z85Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"

func z85Decode(s string) ([]byte, error) {
	if len(s)%z85Group != 0 {
		return nil, fmt.Errorf("%w: z85 length %d", ErrBadDeletionVector, len(s))
	}

	out := make([]byte, 0, len(s)/z85Group*z85Bytes)

	for i := 0; i < len(s); i += z85Group {
		var v uint64

		for _, ch := range []byte(s[i : i+z85Group]) {
			idx := indexByte(z85Alphabet, ch)
			if idx < 0 {
				return nil, fmt.Errorf("%w: z85 character %q", ErrBadDeletionVector, ch)
			}

			v = v*z85Base + uint64(idx)
		}

		if v > maxUint32 {
			return nil, fmt.Errorf("%w: z85 group overflow", ErrBadDeletionVector)
		}

		out = binary.BigEndian.AppendUint32(out, uint32(v))
	}

	return out, nil
}

func indexByte(s string, b byte) int {
	for i := range len(s) {
		if s[i] == b {
			return i
		}
	}

	return -1
}

// Inline returns the serialized bitmap of a storageType "i" descriptor.
func (d *DeletionVector) Inline() ([]byte, error) {
	if d.StorageType != dvStorageInline {
		return nil, fmt.Errorf("%w: storageType %q is not inline", ErrBadDeletionVector, d.StorageType)
	}

	return z85Decode(d.PathOrInlineDv)
}

// FilePath returns the object a storageType "u" or "p" descriptor lives in:
// relative to the table root for "u", absolute for "p".
func (d *DeletionVector) FilePath() (string, bool, error) {
	switch d.StorageType {
	case dvStoragePath:
		return d.PathOrInlineDv, true, nil
	case dvStorageUUID:
		s := d.PathOrInlineDv
		if len(s) < uuidZ85Len {
			return "", false, fmt.Errorf("%w: path %q", ErrBadDeletionVector, s)
		}

		prefix, enc := s[:len(s)-uuidZ85Len], s[len(s)-uuidZ85Len:]

		raw, err := z85Decode(enc)
		if err != nil {
			return "", false, err
		}

		id := fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])

		return path.Join(prefix, "deletion_vector_"+id+".bin"), false, nil
	default:
		return "", false, fmt.Errorf("%w: storageType %q", ErrBadDeletionVector, d.StorageType)
	}
}

// ParseFileDV extracts one framed deletion vector (4-byte big-endian size, data, 4-byte big-endian CRC-32)
// from the bytes of a deletion vector file and returns the deleted row indexes.
func (d *DeletionVector) ParseFileDV(file []byte) (map[int64]bool, error) {
	off := int64(1)
	if d.Offset != nil {
		off = *d.Offset
	}

	if off < 0 || off+dvFramingBytes > int64(len(file)) {
		return nil, fmt.Errorf("%w: offset %d outside file", ErrBadDeletionVector, off)
	}

	size := int64(binary.BigEndian.Uint32(file[off:]))
	if size != d.SizeInBytes {
		return nil, fmt.Errorf("%w: size %d does not match descriptor %d", ErrBadDeletionVector, size, d.SizeInBytes)
	}

	start := off + dvFramingBytes
	if start+size+dvFramingBytes > int64(len(file)) {
		return nil, fmt.Errorf("%w: truncated file", ErrBadDeletionVector)
	}

	data := file[start : start+size]
	if crc32.ChecksumIEEE(data) != binary.BigEndian.Uint32(file[start+size:]) {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrBadDeletionVector)
	}

	return d.finish(data)
}

// ParseInline decodes a storageType "i" descriptor.
func (d *DeletionVector) ParseInline() (map[int64]bool, error) {
	data, err := d.Inline()
	if err != nil {
		return nil, err
	}

	return d.finish(data)
}

func (d *DeletionVector) finish(data []byte) (map[int64]bool, error) {
	rows, err := decodeBitmapArray(data)
	if err != nil {
		return nil, err
	}

	if int64(len(rows)) != d.Cardinality {
		return nil, fmt.Errorf(
			"%w: cardinality %d does not match descriptor %d",
			ErrBadDeletionVector,
			len(rows),
			d.Cardinality,
		)
	}

	return rows, nil
}

// decodeBitmapArray reads a 64-bit roaring bitmap array in either the portable
// (magic 1681511377, little-endian) or native (magic 1681511376, big-endian) layout.
func decodeBitmapArray(data []byte) (map[int64]bool, error) {
	if len(data) < z85Bytes {
		return nil, fmt.Errorf("%w: short bitmap", ErrBadDeletionVector)
	}

	if binary.LittleEndian.Uint32(data) == dvMagicPortable {
		return decodePortable(data[z85Bytes:])
	}

	if binary.BigEndian.Uint32(data) == dvMagicNative {
		return decodeNative(data[z85Bytes:])
	}

	return nil, fmt.Errorf("%w: unknown magic number", ErrBadDeletionVector)
}

func decodePortable(data []byte) (map[int64]bool, error) {
	if len(data) < bitmapCountBytes {
		return nil, fmt.Errorf("%w: short bitmap", ErrBadDeletionVector)
	}

	n := binary.LittleEndian.Uint64(data)
	data = data[bitmapCountBytes:]
	rows := map[int64]bool{}

	for range n {
		if len(data) < z85Bytes {
			return nil, fmt.Errorf("%w: short bitmap", ErrBadDeletionVector)
		}

		high := int64(binary.LittleEndian.Uint32(data)) << highKeyShift
		data = data[z85Bytes:]

		used, err := readRoaring(data, high, rows)
		if err != nil {
			return nil, err
		}

		data = data[used:]
	}

	return rows, nil
}

func decodeNative(data []byte) (map[int64]bool, error) {
	if len(data) < z85Bytes {
		return nil, fmt.Errorf("%w: short bitmap", ErrBadDeletionVector)
	}

	n := binary.BigEndian.Uint32(data)
	data = data[z85Bytes:]
	rows := map[int64]bool{}

	for i := range n {
		if len(data) < z85Bytes {
			return nil, fmt.Errorf("%w: short bitmap", ErrBadDeletionVector)
		}

		size := int(binary.BigEndian.Uint32(data))
		data = data[z85Bytes:]

		if size > len(data) {
			return nil, fmt.Errorf("%w: truncated bitmap", ErrBadDeletionVector)
		}

		if _, err := readRoaring(data[:size], int64(i)<<highKeyShift, rows); err != nil {
			return nil, err
		}

		data = data[size:]
	}

	return rows, nil
}

type roaringReader struct {
	err  error
	data []byte
	pos  int
}

func (r *roaringReader) u16() int {
	if r.pos+2 > len(r.data) {
		r.err = fmt.Errorf("%w: truncated roaring bitmap", ErrBadDeletionVector)

		return 0
	}

	v := int(binary.LittleEndian.Uint16(r.data[r.pos:]))
	r.pos += 2

	return v
}

func (r *roaringReader) u32() uint32 {
	if r.pos+z85Bytes > len(r.data) {
		r.err = fmt.Errorf("%w: truncated roaring bitmap", ErrBadDeletionVector)

		return 0
	}

	v := binary.LittleEndian.Uint32(r.data[r.pos:])
	r.pos += z85Bytes

	return v
}

func (r *roaringReader) take(n int) []byte {
	if r.err != nil || n < 0 || r.pos+n > len(r.data) {
		r.err = fmt.Errorf("%w: truncated roaring bitmap", ErrBadDeletionVector)

		return nil
	}

	b := r.data[r.pos : r.pos+n]
	r.pos += n

	return b
}

// readRoaring decodes one 32-bit portable roaring bitmap, adding high|value to rows,
// and returns the bytes it consumed.
func readRoaring(data []byte, high int64, rows map[int64]bool) (int, error) {
	r := &roaringReader{data: data}
	cookie := r.u32()

	var (
		n      int
		runs   []byte
		hasRun bool
	)

	switch cookie & 0xFFFF {
	case roaringCookieRC:
		hasRun = true
		n = int(cookie>>containerKeyShift) + 1
		runs = r.take((n + bitsPerByte - 1) / bitsPerByte)
	case roaringCookieNoRC:
		n = int(r.u32())
	default:
		return 0, fmt.Errorf("%w: roaring cookie %d", ErrBadDeletionVector, cookie)
	}

	keys, cards := make([]int, n), make([]int, n)

	for i := range n {
		keys[i], cards[i] = r.u16(), r.u16()+1
	}

	if !hasRun || n >= roaringNoOffsetN {
		r.take(n * z85Bytes)
	}

	for i := 0; i < n && r.err == nil; i++ {
		base := high | int64(keys[i])<<containerKeyShift

		switch {
		case hasRun && runs[i/bitsPerByte]&(1<<(i%bitsPerByte)) != 0:
			readRuns(r, base, rows)
		case cards[i] > roaringArrayMax:
			readBitmapContainer(r, base, rows)
		default:
			for range cards[i] {
				rows[base|int64(r.u16())] = true
			}
		}
	}

	return r.pos, r.err
}

func readRuns(r *roaringReader, base int64, rows map[int64]bool) {
	for range r.u16() {
		start, length := r.u16(), r.u16()
		for v := start; v <= start+length && r.err == nil; v++ {
			rows[base|int64(v)] = true
		}
	}
}

func readBitmapContainer(r *roaringReader, base int64, rows map[int64]bool) {
	raw := r.take(roaringBitmapSize)

	for w := 0; w+bitsPerByte <= len(raw); w += bitsPerByte {
		word := binary.LittleEndian.Uint64(raw[w:])

		for word != 0 {
			b := bits.TrailingZeros64(word)
			rows[base|int64(w/bitsPerByte*bitsPerWord+b)] = true
			word &= word - 1
		}
	}
}
