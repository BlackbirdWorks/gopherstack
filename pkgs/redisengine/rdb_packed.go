package redisengine

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
)

const (
	rdbTypeListZiplist    = 10
	rdbTypeIntset         = 11
	rdbTypeZSetZiplist    = 12
	rdbTypeHashZiplist    = 13
	rdbTypeListQuicklist  = 14
	rdbTypeHashListpack   = 16
	rdbTypeZSetListpack   = 17
	rdbTypeQuicklist2     = 18
	rdbTypeSetListpack    = 20
	quicklistContainerRaw = 1
	quicklistContainerLP  = 2

	zipEnd        = 0xff
	zipBigPrev    = 0xfe
	zipHeader     = 10
	lpHeader      = 6
	lpEnd         = 0xff
	maxPackedLen  = 1 << 30
	pairSize      = 2
	intsetHeader  = 8
	lpBackBoundA  = 127
	lpBackBoundB  = 16383
	lpBackBoundC  = 2097151
	lpBackBoundD  = 268435455
	zipImmediate  = 0xf0
	zipImmedFirst = 0xf1
	zipImmedLast  = 0xfd
	int24Bytes    = 3
	int16Bytes    = 2

	zipStr14      = 0x40
	zipStr32      = 0x80
	zipLen6Mask   = 0x3f
	zipLen6Shift  = 6
	immediateMask = 0x0f
	twoBytes      = 2
	lpTag7        = 0x80
	lpTag6Str     = 0x80
	lpTag13Int    = 0xc0
	lpTag12Str    = 0xe0
	lpTag32Str    = 0xf0
	lpMask2       = 0xc0
	lpMask3       = 0xe0
	lpMask4       = 0xf0
	lpLen6Mask    = 0x3f
	lpInt13Mask   = 0x1f
	lpLen12Mask   = 0x0f
	lpInt13Sign   = 1 << 12
	lpInt13Range  = 1 << 13
	backLen3      = 3
	backLen4      = 4
	backLen5      = 5
)

// packedStrings decodes a ziplist or listpack blob into the string form of each element.
type packedStrings func(blob []byte) ([]string, error)

func (r *rdbReader) packedBlob(decode packedStrings) ([]string, error) {
	s, err := r.str()
	if err != nil {
		return nil, err
	}

	return decode([]byte(s))
}

func (r *rdbReader) packedList(kd *keyDump, decode packedStrings) error {
	items, err := r.packedBlob(decode)
	kd.List = items

	return err
}

func (r *rdbReader) packedSet(kd *keyDump, decode packedStrings) error {
	items, err := r.packedBlob(decode)
	kd.Set = items

	return err
}

func (r *rdbReader) packedHash(kd *keyDump, decode packedStrings) error {
	items, err := r.packedBlob(decode)
	if err != nil {
		return err
	}

	if len(items)%pairSize != 0 {
		return r.fail("hash blob holds an odd number of elements")
	}

	kd.Hash = make(map[string]string, len(items)/pairSize)

	for i := 0; i < len(items); i += pairSize {
		kd.Hash[items[i]] = items[i+1]
	}

	return nil
}

func (r *rdbReader) packedZSet(kd *keyDump, decode packedStrings) error {
	items, err := r.packedBlob(decode)
	if err != nil {
		return err
	}

	if len(items)%pairSize != 0 {
		return r.fail("sorted-set blob holds an odd number of elements")
	}

	kd.ZSet = make([]zMember, 0, len(items)/pairSize)

	for i := 0; i < len(items); i += pairSize {
		score, convErr := strconv.ParseFloat(items[i+1], 64)
		if convErr != nil {
			return r.fail("bad sorted-set score")
		}

		kd.ZSet = append(kd.ZSet, zMember{Member: items[i], Score: score})
	}

	return nil
}

func (r *rdbReader) quicklist(kd *keyDump, v2 bool) error {
	n, err := r.count()
	if err != nil {
		return err
	}

	for range n {
		container := uint64(quicklistContainerLP)

		if v2 {
			c, enc, lErr := r.length()
			if lErr != nil || enc >= 0 {
				return r.fail("bad quicklist container")
			}

			container = c
		}

		blob, sErr := r.str()
		if sErr != nil {
			return sErr
		}

		switch {
		case v2 && container == quicklistContainerRaw:
			kd.List = append(kd.List, blob)
		default:
			decode := decodeListpack
			if !v2 {
				decode = decodeZiplist
			}

			items, dErr := decode([]byte(blob))
			if dErr != nil {
				return dErr
			}

			kd.List = append(kd.List, items...)
		}
	}

	return nil
}

func (r *rdbReader) intset(kd *keyDump) error {
	blob, err := r.str()
	if err != nil {
		return err
	}

	items, err := decodeIntset([]byte(blob))
	kd.Set = items

	return err
}

func badPacked(what string) error { return fmt.Errorf("%w: %s", ErrBadRDB, what) }

func decodeIntset(b []byte) ([]string, error) {
	if len(b) < intsetHeader {
		return nil, badPacked("truncated intset")
	}

	width := int(binary.LittleEndian.Uint32(b))
	n := int(binary.LittleEndian.Uint32(b[size32:]))

	if width != int16Bytes && width != size32 && width != size64 || n < 0 || len(b)-intsetHeader != n*width {
		return nil, badPacked("malformed intset")
	}

	out := make([]string, 0, n)

	for i := range n {
		out = append(out, strconv.FormatInt(littleInt(b[intsetHeader+i*width:intsetHeader+(i+1)*width]), decimalBase))
	}

	return out, nil
}

const decimalBase = 10

// littleInt reads a signed little-endian integer of len(b) bytes.
func littleInt(b []byte) int64 {
	var v uint64

	for _, c := range slices.Backward(b) {
		v = v<<bitsPerByte | uint64(c)
	}

	shift := uint(bitsPerByte * (size64 - len(b)))

	return int64(v<<shift) >> shift //nolint:gosec // sign extension of a fixed-width integer
}

func decodeZiplist(b []byte) ([]string, error) {
	if len(b) < zipHeader+1 || b[len(b)-1] != zipEnd {
		return nil, badPacked("malformed ziplist")
	}

	count := int(binary.LittleEndian.Uint16(b[size32*2:]))
	pos := zipHeader
	out := make([]string, 0, count)

	for pos < len(b)-1 {
		if b[pos] == zipBigPrev {
			pos += 1 + size32
		} else {
			pos++
		}

		s, n, err := ziplistEntry(b[pos : len(b)-1])
		if err != nil {
			return nil, err
		}

		out = append(out, s)
		pos += n
	}

	if pos != len(b)-1 || count != len(out) && count != 0xffff {
		return nil, badPacked("ziplist length mismatch")
	}

	return out, nil
}

// ziplistEntry decodes the encoding and payload at the start of b and returns them with their length.
func ziplistEntry(b []byte) (string, int, error) {
	if len(b) == 0 {
		return "", 0, badPacked("truncated ziplist entry")
	}

	first := b[0]

	switch {
	case first>>zipLen6Shift == 0:
		return packedString(b, 1, int(first&zipLen6Mask))
	case first&zipStr32 == 0 && first&zipStr14 != 0:
		if len(b) < twoBytes {
			return "", 0, badPacked("truncated ziplist entry")
		}

		return packedString(b, twoBytes, int(first&zipLen6Mask)<<bitsPerByte|int(b[1]))
	case first == zipStr32:
		if len(b) < 1+size32 {
			return "", 0, badPacked("truncated ziplist entry")
		}

		return packedString(b, 1+size32, int(binary.BigEndian.Uint32(b[1:])))
	case first >= zipImmedFirst && first <= zipImmedLast:
		return strconv.Itoa(int(first&immediateMask) - 1), 1, nil
	}

	width := map[byte]int{0xc0: int16Bytes, 0xd0: size32, 0xe0: size64, zipImmediate: int24Bytes, 0xfe: 1}[first]
	if width == 0 || len(b) < 1+width {
		return "", 0, badPacked("unknown ziplist encoding")
	}

	return strconv.FormatInt(littleInt(b[1:1+width]), decimalBase), 1 + width, nil
}

func packedString(b []byte, head, n int) (string, int, error) {
	if n < 0 || n > maxPackedLen || len(b) < head+n {
		return "", 0, badPacked("truncated packed string")
	}

	return string(b[head : head+n]), head + n, nil
}

func decodeListpack(b []byte) ([]string, error) {
	if len(b) < lpHeader+1 || b[len(b)-1] != lpEnd {
		return nil, badPacked("malformed listpack")
	}

	count := int(binary.LittleEndian.Uint16(b[size32:]))
	pos := lpHeader
	out := make([]string, 0, count)

	for pos < len(b)-1 {
		s, n, err := listpackEntry(b[pos : len(b)-1])
		if err != nil {
			return nil, err
		}

		back := lpBackLen(n)
		if pos+n+back > len(b)-1 {
			return nil, badPacked("truncated listpack entry")
		}

		out = append(out, s)
		pos += n + back
	}

	if count != len(out) && count != 0xffff {
		return nil, badPacked("listpack length mismatch")
	}

	return out, nil
}

func lpBackLen(n int) int {
	switch {
	case n <= lpBackBoundA:
		return 1
	case n < lpBackBoundB:
		return twoBytes
	case n < lpBackBoundC:
		return backLen3
	case n < lpBackBoundD:
		return backLen4
	}

	return backLen5
}

// listpackEntry decodes one element (without its trailing back-length) and returns it with its length.
func listpackEntry(b []byte) (string, int, error) {
	first := b[0]

	switch {
	case first&lpTag7 == 0:
		return strconv.Itoa(int(first)), 1, nil
	case first&lpMask2 == lpTag6Str:
		return packedString(b, 1, int(first&lpLen6Mask))
	case first&lpMask3 == lpTag13Int:
		if len(b) < twoBytes {
			return "", 0, badPacked("truncated listpack entry")
		}

		v := int(first&lpInt13Mask)<<bitsPerByte | int(b[1])
		if v >= lpInt13Sign {
			v -= lpInt13Range
		}

		return strconv.Itoa(v), twoBytes, nil
	case first&lpMask4 == lpTag12Str:
		if len(b) < twoBytes {
			return "", 0, badPacked("truncated listpack entry")
		}

		return packedString(b, twoBytes, int(first&lpLen12Mask)<<bitsPerByte|int(b[1]))
	case first == lpTag32Str:
		if len(b) < 1+size32 {
			return "", 0, badPacked("truncated listpack entry")
		}

		return packedString(b, 1+size32, int(binary.LittleEndian.Uint32(b[1:])))
	}

	width := map[byte]int{0xf1: int16Bytes, 0xf2: int24Bytes, 0xf3: size32, 0xf4: size64}[first]
	if width == 0 || len(b) < 1+width {
		return "", 0, badPacked("unknown listpack encoding")
	}

	return strconv.FormatInt(littleInt(b[1:1+width]), decimalBase), 1 + width, nil
}
