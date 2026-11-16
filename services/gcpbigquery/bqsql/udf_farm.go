package bqsql

import (
	"database/sql/driver"
	"encoding/binary"
	"math/bits"
)

// FarmHash Fingerprint64 (farmhashna::Hash64 of the open-source FarmHash library).
const (
	farmK0     = 0xc3a5c85c97cb3127
	farmK1     = 0xb492b66fbe98f273
	farmK2     = 0x9ae16a3b2f90404f
	farmSeed   = 81
	farmAdd    = 113
	farmBlk    = 64
	farmLo     = 0xff
	lenWord    = 8
	lenHalf    = 4
	lenQuad    = 16
	lenMid     = 32
	shiftTwo   = 2
	shiftThree = 3
	shiftEight = 8
	farmTail   = 9

	rot18   = 18
	rot21   = 21
	rot25   = 25
	rot30   = 30
	rot33   = 33
	rot37   = 37
	rot42   = 42
	rot43   = 43
	rot44   = 44
	shift47 = 47
)

func addFarmUDFs(d map[string]udfDef) {
	d["farm_fingerprint"] = udfDef{n: 1, fn: func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil //nolint:nilnil // NULL in, NULL out
		}

		return int64(farmFingerprint64(bytesOf(a[0]))), nil //nolint:gosec // two's complement INT64
	}}
}

func fetch64(s []byte) uint64 { return binary.LittleEndian.Uint64(s) }
func fetch32(s []byte) uint64 { return uint64(binary.LittleEndian.Uint32(s)) }

func farmRotate(v uint64, shift uint) uint64 { return bits.RotateLeft64(v, -int(shift)) }

func farmShiftMix(v uint64) uint64 { return v ^ (v >> shift47) }

func farmHashLen16(u, v, mul uint64) uint64 {
	a := (u ^ v) * mul
	a ^= a >> shift47
	b := (v ^ a) * mul
	b ^= b >> shift47
	b *= mul

	return b
}

func farmHashLen0to16(s []byte) uint64 {
	n := uint64(len(s))

	switch {
	case n >= lenWord:
		mul := farmK2 + n*shiftTwo
		a := fetch64(s) + farmK2
		b := fetch64(s[n-lenWord:])
		c := farmRotate(b, rot37)*mul + a
		d := (farmRotate(a, rot25) + b) * mul

		return farmHashLen16(c, d, mul)
	case n >= lenHalf:
		mul := farmK2 + n*shiftTwo
		a := fetch32(s)

		return farmHashLen16(n+(a<<shiftThree), fetch32(s[n-lenHalf:]), mul)
	case n > 0:
		a, b, c := uint32(s[0]), uint32(s[n>>1]), uint32(s[n-1])
		y := a + b<<shiftEight
		z := uint32(n) + c<<shiftTwo

		return farmShiftMix(uint64(y)*farmK2^uint64(z)*farmK0) * farmK2
	}

	return farmK2
}

func farmHashLen17to32(s []byte) uint64 {
	n := uint64(len(s))
	mul := farmK2 + n*shiftTwo
	a := fetch64(s) * farmK1
	b := fetch64(s[8:])
	c := fetch64(s[n-lenWord:]) * mul
	d := fetch64(s[n-16:]) * farmK2

	return farmHashLen16(farmRotate(a+b, rot43)+farmRotate(c, rot30)+d, a+farmRotate(b+farmK2, rot18)+c, mul)
}

func farmHashLen33to64(s []byte) uint64 {
	n := uint64(len(s))
	mul := farmK2 + n*shiftTwo
	a := fetch64(s) * farmK2
	b := fetch64(s[8:])
	c := fetch64(s[n-lenWord:]) * mul
	d := fetch64(s[n-16:]) * farmK2
	y := farmRotate(a+b, rot43) + farmRotate(c, rot30) + d
	z := farmHashLen16(y, a+farmRotate(b+farmK2, rot18)+c, mul)
	e := fetch64(s[16:]) * mul
	f := fetch64(s[24:])
	g := (y + fetch64(s[n-32:])) * mul
	h := (z + fetch64(s[n-24:])) * mul

	return farmHashLen16(farmRotate(e+f, rot43)+farmRotate(g, rot30)+h, e+farmRotate(f+a, rot18)+g, mul)
}

func farmWeak(w, x, y, z, a, b uint64) (uint64, uint64) {
	a += w
	b = farmRotate(b+a+z, rot21)
	c := a
	a += x
	a += y
	b += farmRotate(a, rot44)

	return a + z, b + c
}

func farmWeakAt(s *[32]byte, a, b uint64) (uint64, uint64) {
	return farmWeak(fetch64(s[0:]), fetch64(s[8:]), fetch64(s[16:]), fetch64(s[24:]), a, b)
}

type farmState struct {
	x, y, z, v1, v2, w1, w2 uint64
}

func (st *farmState) round(p []byte, mul, k uint64) {
	blk := (*[farmBlk]byte)(p)
	st.x = farmRotate(st.x+st.y+st.v1+fetch64(blk[8:]), rot37) * mul
	st.y = farmRotate(st.y+st.v2+fetch64(blk[48:]), rot42) * mul
	st.x ^= st.w2 * k
	st.y += st.v1*k + fetch64(blk[40:])
	st.z = farmRotate(st.z+st.w1, rot33) * mul
	st.v1, st.v2 = farmWeakAt((*[32]byte)(blk[:32]), st.v2*mul, st.x+st.w1)
	st.w1, st.w2 = farmWeakAt((*[32]byte)(blk[32:]), st.z+st.w2, st.y+fetch64(blk[16:]))
	st.z, st.x = st.x, st.z
}

func farmFingerprint64(s []byte) uint64 {
	n := len(s)

	switch {
	case n <= lenQuad:
		return farmHashLen0to16(s)
	case n <= lenMid:
		return farmHashLen17to32(s)
	case n <= farmBlk:
		return farmHashLen33to64(s)
	}

	seed := uint64(farmSeed)
	st := &farmState{x: seed, y: seed*farmK1 + farmAdd}
	st.z = farmShiftMix(st.y*farmK2+farmAdd) * farmK2
	st.x = st.x*farmK2 + fetch64(s)

	end := ((n - 1) / farmBlk) * farmBlk
	last64 := end + ((n - 1) & (farmBlk - 1)) - (farmBlk - 1)

	for off := 0; off != end; off += farmBlk {
		st.round(s[off:], farmK1, 1)
	}

	mul := farmK1 + (st.z&farmLo)<<1
	st.w1 += uint64((n - 1) & (farmBlk - 1))
	st.v1 += st.w1
	st.w1 += st.v1
	st.round(s[last64:], mul, farmTail)

	return farmHashLen16(
		farmHashLen16(st.v1, st.w1, mul)+farmShiftMix(st.y)*farmK0+st.z,
		farmHashLen16(st.v2, st.w2, mul)+st.x,
		mul,
	)
}
