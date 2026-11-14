//nolint:mnd // RLE bit-width tables and wire constants
package orcio

import (
	"fmt"
	"math/bits"
)

const (
	maxRunV2     = 512
	minRunByte   = 3
	maxRunByte   = 130
	maxLiteral   = 128
	encShortRep  = 0
	encDirectRLE = 1
	encPatched   = 2
	encDeltaRLE  = 3
	maxGapRLE    = 255
	byteBits     = 8
)

type byteReader struct {
	b   []byte
	pos int
}

func (r *byteReader) byte() (byte, error) {
	if r.pos >= len(r.b) {
		return 0, fmt.Errorf("%w: unexpected end of stream", ErrBadFile)
	}

	c := r.b[r.pos]
	r.pos++

	return c, nil
}

func (r *byteReader) take(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.b) {
		return nil, fmt.Errorf("%w: unexpected end of stream", ErrBadFile)
	}

	out := r.b[r.pos : r.pos+n]
	r.pos += n

	return out, nil
}

func (r *byteReader) varint() (uint64, error) {
	var v uint64

	for shift := uint(0); ; shift += 7 {
		c, err := r.byte()
		if err != nil {
			return 0, err
		}

		v |= uint64(c&0x7f) << shift
		if c&0x80 == 0 {
			return v, nil
		}

		if shift > 63 {
			return 0, fmt.Errorf("%w: varint overflow", ErrBadFile)
		}
	}
}

func unzig(v uint64) int64 { return int64(v>>1) ^ -int64(v&1) }

func zig(v int64) uint64 { return asBits(v<<1) ^ asBits(v>>63) }

func decodeWidth(code int) int {
	switch {
	case code <= 23:
		return code + 1
	case code == 24:
		return 26
	case code == 25:
		return 28
	case code == 26:
		return 30
	case code == 27:
		return 32
	case code == 28:
		return 40
	case code == 29:
		return 48
	case code == 30:
		return 56
	default:
		return 64
	}
}

func closestFixed(n int) int {
	switch {
	case n <= 24:
		return max(n, 1)
	case n <= 26:
		return 26
	case n <= 28:
		return 28
	case n <= 30:
		return 30
	case n <= 32:
		return 32
	case n <= 40:
		return 40
	case n <= 48:
		return 48
	case n <= 56:
		return 56
	default:
		return 64
	}
}

func encodeWidth(w int) int {
	switch {
	case w <= 24:
		return max(w, 1) - 1
	case w == 26:
		return 24
	case w == 28:
		return 25
	case w == 30:
		return 26
	case w == 32:
		return 27
	case w == 40:
		return 28
	case w == 48:
		return 29
	case w == 56:
		return 30
	default:
		return 31
	}
}

type bitReader struct {
	b    []byte
	pos  int
	cur  byte
	left int
}

func (r *bitReader) read(w int) (uint64, error) {
	var v uint64

	for w > 0 {
		if r.left == 0 {
			if r.pos >= len(r.b) {
				return 0, fmt.Errorf("%w: unexpected end of bit-packed run", ErrBadFile)
			}

			r.cur, r.left = r.b[r.pos], byteBits
			r.pos++
		}

		take := min(w, r.left)
		v = v<<take | uint64(r.cur>>(r.left-take))&(1<<take-1)
		r.left -= take
		w -= take
	}

	return v, nil
}

func unpack(r *byteReader, n, w int) ([]uint64, error) {
	size := (n*w + byteBits - 1) / byteBits

	raw, err := r.take(size)
	if err != nil {
		return nil, err
	}

	br := &bitReader{b: raw}
	out := make([]uint64, n)

	for i := range out {
		if out[i], err = br.read(w); err != nil {
			return nil, err
		}
	}

	return out, nil
}

// decodeInts reads n integers from an RLE stream (version 1 or 2).
func decodeInts(b []byte, n int, signed, v2 bool) ([]int64, error) {
	r := &byteReader{b: b}
	out := make([]int64, 0, n)

	for len(out) < n {
		var (
			run []int64
			err error
		)

		if v2 {
			run, err = readRunV2(r, signed)
		} else {
			run, err = readRunV1(r, signed)
		}

		if err != nil {
			return nil, err
		}

		out = append(out, run...)
	}

	return out[:n], nil
}

func readRunV1(r *byteReader, signed bool) ([]int64, error) {
	ctrl, err := r.byte()
	if err != nil {
		return nil, err
	}

	if ctrl < 0x80 {
		count := int(ctrl) + minRunByte

		d, derr := r.byte()
		if derr != nil {
			return nil, derr
		}

		base, verr := r.varint()
		if verr != nil {
			return nil, verr
		}

		start := asSigned(base)
		if signed {
			start = unzig(base)
		}

		out := make([]int64, count)
		for i := range out {
			out[i] = start + signedByte(d)*int64(i)
		}

		return out, nil
	}

	count := 256 - int(ctrl)
	out := make([]int64, count)

	for i := range out {
		v, verr := r.varint()
		if verr != nil {
			return nil, verr
		}

		if signed {
			out[i] = unzig(v)
		} else {
			out[i] = asSigned(v)
		}
	}

	return out, nil
}

func readRunV2(r *byteReader, signed bool) ([]int64, error) {
	b0, err := r.byte()
	if err != nil {
		return nil, err
	}

	switch int(b0 >> 6) {
	case encShortRep:
		return readShortRepeat(r, b0, signed)
	case encDirectRLE:
		return readDirect(r, b0, signed)
	case encPatched:
		return readPatched(r, b0)
	default:
		return readDelta(r, b0, signed)
	}
}

func readShortRepeat(r *byteReader, b0 byte, signed bool) ([]int64, error) {
	width := int(b0>>3)&7 + 1
	count := int(b0&7) + minRunByte

	raw, err := r.take(width)
	if err != nil {
		return nil, err
	}

	var v uint64
	for _, c := range raw {
		v = v<<byteBits | uint64(c)
	}

	val := int64(v)
	if signed {
		val = unzig(v)
	}

	out := make([]int64, count)
	for i := range out {
		out[i] = val
	}

	return out, nil
}

func runHeader(r *byteReader, b0 byte) (int, int, error) {
	b1, err := r.byte()
	if err != nil {
		return 0, 0, err
	}

	return decodeWidth(int(b0>>1) & 0x1f), (int(b0&1)<<byteBits | int(b1)) + 1, nil
}

func readDirect(r *byteReader, b0 byte, signed bool) ([]int64, error) {
	w, n, err := runHeader(r, b0)
	if err != nil {
		return nil, err
	}

	vals, err := unpack(r, n, w)
	if err != nil {
		return nil, err
	}

	out := make([]int64, n)
	for i, v := range vals {
		if signed {
			out[i] = unzig(v)
		} else {
			out[i] = asSigned(v)
		}
	}

	return out, nil
}

func readPatched(r *byteReader, b0 byte) ([]int64, error) {
	w, n, err := runHeader(r, b0)
	if err != nil {
		return nil, err
	}

	hdr, err := r.take(2)
	if err != nil {
		return nil, err
	}

	baseBytes := int(hdr[0]>>5) + 1
	patchW := decodeWidth(int(hdr[0] & 0x1f))
	gapW := int(hdr[1]>>5) + 1
	patchLen := int(hdr[1] & 0x1f)

	raw, err := r.take(baseBytes)
	if err != nil {
		return nil, err
	}

	var base int64
	for _, c := range raw {
		base = base<<byteBits | int64(c)
	}

	if sign := int64(1) << (uint(baseBytes)*byteBits - 1); base&sign != 0 {
		base = -(base &^ sign)
	}

	vals, err := unpack(r, n, w)
	if err != nil {
		return nil, err
	}

	if err = applyPatches(r, vals, w, patchW, gapW, patchLen); err != nil {
		return nil, err
	}

	out := make([]int64, n)
	for i, v := range vals {
		out[i] = base + asSigned(v)
	}

	return out, nil
}

func applyPatches(r *byteReader, vals []uint64, w, patchW, gapW, patchLen int) error {
	entries, err := unpack(r, patchLen, closestFixed(gapW+patchW))
	if err != nil {
		return err
	}

	idx := 0

	for _, e := range entries {
		gap := capInt(e >> patchW)
		patch := e & (1<<patchW - 1)

		idx += gap
		if gap == maxGapRLE && patch == 0 {
			continue
		}

		if idx >= len(vals) {
			return fmt.Errorf("%w: patch index out of range", ErrBadFile)
		}

		vals[idx] |= patch << w
	}

	return nil
}

func readDelta(r *byteReader, b0 byte, signed bool) ([]int64, error) {
	code := int(b0>>1) & 0x1f
	b1, err := r.byte()
	if err != nil {
		return nil, err
	}

	n := (int(b0&1)<<byteBits | int(b1)) + 1

	raw, err := r.varint()
	if err != nil {
		return nil, err
	}

	base := asSigned(raw)
	if signed {
		base = unzig(raw)
	}

	rawDelta, err := r.varint()
	if err != nil {
		return nil, err
	}

	delta := unzig(rawDelta)
	out := make([]int64, 0, n)
	out = append(out, base)

	if n == 1 {
		return out, nil
	}

	out = append(out, base+delta)

	if code == 0 {
		for len(out) < n {
			out = append(out, out[len(out)-1]+delta)
		}

		return out, nil
	}

	deltas, err := unpack(r, n-2, decodeWidth(code))
	if err != nil {
		return nil, err
	}

	for _, d := range deltas {
		step := asSigned(d)
		if delta < 0 {
			step = -step
		}

		out = append(out, out[len(out)-1]+step)
	}

	return out, nil
}

func decodeByteRLE(b []byte, n int) ([]byte, error) {
	r := &byteReader{b: b}
	out := make([]byte, 0, n)

	for len(out) < n {
		ctrl, err := r.byte()
		if err != nil {
			return nil, err
		}

		if ctrl < 0x80 {
			v, verr := r.byte()
			if verr != nil {
				return nil, verr
			}

			for range int(ctrl) + minRunByte {
				out = append(out, v)
			}

			continue
		}

		lit, err := r.take(256 - int(ctrl))
		if err != nil {
			return nil, err
		}

		out = append(out, lit...)
	}

	return out[:n], nil
}

func decodeBoolRLE(b []byte, n int) ([]bool, error) {
	raw, err := decodeByteRLE(b, (n+byteBits-1)/byteBits)
	if err != nil {
		return nil, err
	}

	out := make([]bool, n)
	for i := range out {
		out[i] = raw[i/byteBits]&(0x80>>(i%byteBits)) != 0
	}

	return out, nil
}

func encodeByteRLE(vals []byte) []byte {
	var out []byte

	for i := 0; i < len(vals); {
		run := 1
		for i+run < len(vals) && vals[i+run] == vals[i] && run < maxRunByte {
			run++
		}

		if run >= minRunByte {
			out = append(out, byte(run-minRunByte), vals[i])
			i += run

			continue
		}

		j := i

		for j < len(vals) && j-i < maxLiteral {
			if j+minRunByte <= len(vals) && vals[j] == vals[j+1] && vals[j] == vals[j+2] {
				break
			}

			j++
		}

		if j == i {
			j = i + 1
		}

		out = append(out, lowByte(u64(256-(j-i))))
		out = append(out, vals[i:j]...)
		i = j
	}

	return out
}

func encodeBoolRLE(vals []bool) []byte {
	packed := make([]byte, (len(vals)+byteBits-1)/byteBits)

	for i, v := range vals {
		if v {
			packed[i/byteBits] |= 0x80 >> (i % byteBits)
		}
	}

	return encodeByteRLE(packed)
}

// encodeIntsV2 writes integers as RLE v2 DIRECT runs.
func encodeIntsV2(vals []int64, signed bool) []byte {
	var out []byte

	for len(vals) > 0 {
		n := min(maxRunV2, len(vals))
		run := vals[:n]
		vals = vals[n:]

		raw := make([]uint64, n)

		var most uint64

		for i, v := range run {
			if signed {
				raw[i] = zig(v)
			} else {
				raw[i] = asBits(v)
			}

			most = max(most, raw[i])
		}

		w := closestFixed(bits.Len64(most))
		out = append(out, lowByte(u64(encDirectRLE<<6|encodeWidth(w)<<1|((n-1)>>byteBits)&1)), lowByte(u64(n-1)))
		out = append(out, pack(raw, w)...)
	}

	return out
}

func pack(vals []uint64, w int) []byte {
	out := make([]byte, (len(vals)*w+byteBits-1)/byteBits)
	bit := 0

	for _, v := range vals {
		for i := w - 1; i >= 0; i-- {
			if v>>uint(i)&1 == 1 {
				out[bit/byteBits] |= 0x80 >> (bit % byteBits)
			}

			bit++
		}
	}

	return out
}
