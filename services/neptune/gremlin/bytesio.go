package gremlin

import (
	"encoding/binary"
	"math"
)

// The GraphBinary integer conversions are explicit two's-complement reinterpretations of fixed-width fields.

func beInt32(b []byte) int {
	return int(int32(binary.BigEndian.Uint32(b))) //nolint:gosec // reinterpret 4 bytes as signed
}

func beInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b)) //nolint:gosec // reinterpret 8 bytes as signed
}

func beInt16(b []byte) int64 {
	return int64(int16(binary.BigEndian.Uint16(b))) //nolint:gosec // reinterpret 2 bytes as signed
}

func beFloat64(n int64) float64 {
	return math.Float64frombits(uint64(n)) //nolint:gosec // reinterpret bits
}

func putInt32(n int) []byte {
	var b [4]byte

	binary.BigEndian.PutUint32(b[:], uint32(int32(n))) //nolint:gosec // wire int32

	return b[:]
}

func putInt64(n int64) []byte {
	var b [8]byte

	binary.BigEndian.PutUint64(b[:], uint64(n)) //nolint:gosec // wire int64

	return b[:]
}

func floatBits64(f float64) int64 {
	return int64(math.Float64bits(f)) //nolint:gosec // reinterpret bits
}

func floatBits32(f float32) int {
	return int(int32(math.Float32bits(f))) //nolint:gosec // reinterpret bits
}

func signedByte(b byte) int64 {
	return int64(int8(b)) //nolint:gosec // reinterpret as signed
}
