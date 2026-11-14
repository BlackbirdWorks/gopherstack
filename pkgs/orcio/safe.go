package orcio

import "math"

const (
	// maxValues bounds the entries any one column, dictionary or list total may claim.
	maxValues = 1 << 24
	// maxStreamBytes bounds one decompressed stream.
	maxStreamBytes = 1 << 28
	// maxBlockSize bounds the declared compression block size.
	maxBlockSize = 1 << 24
	// maxNesting bounds the depth of the type tree.
	maxNesting = 64
	// valuesPerByte is the densest RLE packing (a 130-byte boolean run in two bytes) rounded up.
	valuesPerByte = 4096
	// maxDecimalBytes bounds one base-128 decimal varint (38 digits need 17 bytes).
	maxDecimalBytes = 24
	maxDecimalScale = 38
	lowByteMask     = 0xff
	varintPayload   = 0x7f
	varintMore      = 0x80
	varintShift     = 7
	signBit         = 1 << 63
	byteMagnitude   = 0x7f
	byteSign        = 0x80
	shiftByte1      = 8
	shiftByte2      = 16
	nanoZeroMask    = 7
	nanoShift       = 3
)

// capInt narrows an untrusted unsigned length; anything above MaxInt32 saturates and fails later bounds checks.
func capInt(u uint64) int {
	if u > math.MaxInt32 {
		return math.MaxInt32
	}

	return int(u)
}

func capInt64(u uint64) int64 {
	if u > math.MaxInt64 {
		return math.MaxInt64
	}

	return int64(u)
}

// u64 widens a non-negative length.
func u64(n int) uint64 {
	if n < 0 {
		return 0
	}

	return uint64(n)
}

func lowByte(v uint64) byte { return byte(v & lowByteMask) }

// asSigned reinterprets the bits of u as a two's complement integer.
func asSigned(u uint64) int64 {
	v := int64(u & math.MaxInt64)
	if u&signBit != 0 {
		return v + math.MinInt64
	}

	return v
}

// asBits is the inverse of asSigned.
func asBits(i int64) uint64 {
	v := uint64(i & math.MaxInt64)
	if i < 0 {
		return v | signBit
	}

	return v
}

// signedByte reads a byte as int8.
func signedByte(b byte) int64 { return int64(b&byteMagnitude) - int64(b&byteSign) }
