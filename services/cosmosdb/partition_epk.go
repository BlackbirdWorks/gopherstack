package cosmosdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
)

// Effective partition key hashing for partition key version 2. Ported from the hashing azcosmos v1.5.0 uses to
// route change-feed feed ranges (sdk/data/azcosmos/internal/epk/epk.go): MurmurHash3 x64 128 over the typed
// component bytes, the two top bits masked so every hash is below the service's "FF" sentinel.
const (
	epkComponentNull   byte = 0x01
	epkComponentFalse  byte = 0x02
	epkComponentTrue   byte = 0x03
	epkComponentNumber byte = 0x05
	epkComponentString byte = 0x08
	epkStringSuffix    byte = 0xFF
	epkTopMask              = uint64(0x3FFFFFFFFFFFFFFF)
	epkSpaceBits            = 126
	murmurC1                = uint64(0x87c37b91114253d5)
	murmurC2                = uint64(0x4cf5ad432745937f)
	rotA                    = 31
	rotB                    = 27
	rotC                    = 33
	bitsPerByte             = 8
	blockHalf               = 8
	fmixShift               = 33
	wordBits                = 64
	murmurMul               = 5
	murmurAddA              = 0x52dce729
	murmurAddB              = 0x38495ab5
)

// u128 is a point in the 128-bit effective partition key space, most significant half first.
type u128 struct{ hi, lo uint64 }

func (a u128) cmp(b u128) int {
	switch {
	case a.hi != b.hi:
		if a.hi < b.hi {
			return -1
		}

		return 1
	case a.lo != b.lo:
		if a.lo < b.lo {
			return -1
		}

		return 1
	}

	return 0
}

func (a u128) big() *big.Int {
	var raw [16]byte

	binary.BigEndian.PutUint64(raw[:8], a.hi)
	binary.BigEndian.PutUint64(raw[8:], a.lo)

	return new(big.Int).SetBytes(raw[:])
}

func u128FromBig(v *big.Int) u128 {
	var raw [16]byte

	v.FillBytes(raw[:])

	return u128{hi: binary.BigEndian.Uint64(raw[:8]), lo: binary.BigEndian.Uint64(raw[8:])}
}

func (a u128) String() string { return fmt.Sprintf("%016X%016X", a.hi, a.lo) }

func (a u128) MarshalText() ([]byte, error) { return []byte(a.String()), nil }

func (a *u128) UnmarshalText(text []byte) error {
	_, err := fmt.Sscanf(string(text), "%016X%016X", &a.hi, &a.lo)

	return err
}

func rotl64(v uint64, r uint) uint64 { return v<<r | v>>(wordBits-r) }

func fmix64(v uint64) uint64 {
	v ^= v >> fmixShift
	v *= 0xff51afd7ed558ccd
	v ^= v >> fmixShift
	v *= 0xc4ceb9fe1a85ec53
	v ^= v >> fmixShift

	return v
}

// murmur3x64 is MurmurHash3_x64_128 with seed 0, returning (low, high).
func murmur3x64(data []byte) (uint64, uint64) {
	var h1, h2 uint64

	pos := 0
	for ; pos+16 <= len(data); pos += 16 {
		k1 := binary.LittleEndian.Uint64(data[pos:])
		k2 := binary.LittleEndian.Uint64(data[pos+8:])

		k1 = rotl64(k1*murmurC1, rotA) * murmurC2
		h1 ^= k1
		h1 = (rotl64(h1, rotB)+h2)*murmurMul + murmurAddA

		k2 = rotl64(k2*murmurC2, rotC) * murmurC1
		h2 ^= k2
		h2 = (rotl64(h2, rotA)+h1)*murmurMul + murmurAddB
	}

	tail := data[pos:]

	var k1, k2 uint64

	for i := len(tail) - 1; i >= blockHalf; i-- {
		k2 ^= uint64(tail[i]) << (bitsPerByte * uint(i-blockHalf))
	}

	if len(tail) > blockHalf {
		h2 ^= rotl64(k2*murmurC2, rotC) * murmurC1
	}

	for i := min(len(tail), blockHalf) - 1; i >= 0; i-- {
		k1 ^= uint64(tail[i]) << (bitsPerByte * uint(i))
	}

	if len(tail) > 0 {
		h1 ^= rotl64(k1*murmurC1, rotA) * murmurC2
	}

	n := uint64(len(data))
	h1 ^= n
	h2 ^= n
	h1 += h2
	h2 += h1
	h1, h2 = fmix64(h1), fmix64(h2)
	h1 += h2
	h2 += h1

	return h1, h2
}

// appendEPKComponent encodes one partition key component for hashing.
func appendEPKComponent(buf []byte, v any) []byte {
	switch x := v.(type) {
	case nil:
		return append(buf, epkComponentNull)
	case bool:
		if x {
			return append(buf, epkComponentTrue)
		}

		return append(buf, epkComponentFalse)
	case json.Number:
		f, _ := x.Float64()

		return appendEPKNumber(buf, f)
	case float64:
		return appendEPKNumber(buf, x)
	case string:
		buf = append(buf, epkComponentString)
		buf = append(buf, x...)

		return append(buf, epkStringSuffix)
	}

	return buf
}

func appendEPKNumber(buf []byte, f float64) []byte {
	buf = append(buf, epkComponentNumber)

	return binary.LittleEndian.AppendUint64(buf, math.Float64bits(f))
}

func epkOfValues(vals []any) u128 {
	var buf []byte
	for _, v := range vals {
		buf = appendEPKComponent(buf, v)
	}

	low, high := murmur3x64(buf)

	return u128{hi: high & epkTopMask, lo: low}
}

// epkHash is the routing hash of a canonical partition key: a scalar hashes as a whole; a hierarchical key (a JSON
// array) routes by its first component, whose hash opens its effective partition key.
func epkHash(pkJSON string) u128 {
	dec := json.NewDecoder(bytes.NewReader([]byte(pkJSON)))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return epkOfValues([]any{pkJSON})
	}

	if arr, ok := v.([]any); ok {
		if len(arr) == 0 {
			return u128{}
		}

		return epkOfValues(arr[:1])
	}

	return epkOfValues([]any{v})
}

func epkSpace() *big.Int { return new(big.Int).Lsh(big.NewInt(1), epkSpaceBits) }

// evenLow is the inclusive lower bound of range i of n: ceil(i * 2^126 / n).
func evenLow(i, n int) u128 {
	if i <= 0 || n <= 0 {
		return u128{}
	}

	num := new(big.Int).Mul(epkSpace(), big.NewInt(int64(i)))
	num.Add(num, big.NewInt(int64(n-1)))

	return u128FromBig(num.Div(num, big.NewInt(int64(n))))
}

// midpoint of [lo, hi).
func midpoint(lo, hi u128) u128 {
	sum := new(big.Int).Add(lo.big(), hi.big())

	return u128FromBig(sum.Rsh(sum, 1))
}
