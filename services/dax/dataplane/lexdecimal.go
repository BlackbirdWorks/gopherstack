package dataplane

import (
	"bufio"
	"encoding/binary"
	"errors"
	"math/big"
)

// This file ports the DAX lexicographic-decimal range-key codec from
// github.com/aws/aws-dax-go/dax/internal/cbor/lexdecimal.go. Numeric sort keys
// in a compound primary key are serialized as a sort-order-preserving byte
// sequence (no CBOR header) appended after the CBOR-encoded hash component.
//
// Encoding of the leading header byte:
//
//	0x00:       null low (unused)
//	0x01:       negative signum; four bytes follow for positive exponent
//	0x02..0x3f: negative signum; positive exponent; range 61..0
//	0x40..0x7d: negative signum; negative exponent; range -1..-62
//	0x7e:       negative signum; four bytes follow for negative exponent
//	0x7f:       negative zero (unused)
//	0x80:       zero
//	0x81:       positive signum; four bytes follow for negative exponent
//	0x82..0xbf: positive signum; negative exponent; range -62..-1
//	0xc0..0xfd: positive signum; positive exponent; range 0..61
//	0xfe:       positive signum; four bytes follow for positive exponent
//	0xff:       null high
//
// The significand is base-1000 encoded to preserve sort order while staying
// more compact than base-10.
// Header bytes and ranges for the lexicographic-decimal encoding.
const (
	lexNullLow      = 0x00
	lexZero         = 0x80
	lexNegZero      = 0x7f
	lexNullHigh     = 0xff
	lexNegPosExpLd  = 0x01 // negative signum, four-byte positive exponent
	lexNegNegExpLd  = 0x7e // negative signum, four-byte negative exponent
	lexPosNegExpLd  = 0x81 // positive signum, four-byte negative exponent
	lexPosPosExpLd  = 0xfe // positive signum, four-byte positive exponent
	lexNegExpBase   = 0x3f // negative-signum inline exponent base
	lexPosExpBase   = 0xc0 // positive-signum inline exponent base
	lexInlineExpLo  = -0x3e
	lexInlineExpHi  = 0x3e
	lexByteMask     = 0xff
	lexDigitMask    = 0x3ff
	lexPosExpXOR    = 0x7fffffff
	lexNegExpXOR    = -0x80000000
	lexBitsPerByte  = 8
	lexBitsPerDigit = 10
	lexBase         = 1000
	lexBaseSq       = 100
	lexBaseTen      = 10
	lexBaseCube     = 1000000000
	lexDigitAdjust  = 12
	lexNegAdjust    = 999 + lexDigitAdjust
	lexTermFull     = 2
	lexTermMax      = 1023
	lexDigitsPerGrp = 9
	lexExpDigits9   = 9
)

func lexThousand() *big.Int { return big.NewInt(lexBase) }
func lexHundred() *big.Int  { return big.NewInt(lexBaseSq) }
func lexTen() *big.Int      { return big.NewInt(lexBaseTen) }
func lexBillion() *big.Int  { return big.NewInt(lexBaseCube) }

// errBadLexDecimal indicates a malformed lexicographic-decimal range key.
var errBadLexDecimal = errors.New("dax: malformed lexdecimal range key")

// encodeLexDecimal writes d to bw in the DAX lexicographic-decimal byte form.
func encodeLexDecimal(d *decimal, bw *bufio.Writer) error {
	if d.unscaled().Sign() == 0 {
		return bw.WriteByte(lexZero)
	}

	prec := lexPrecision(d)
	exponent := prec - d.scale
	val := new(big.Int).Set(d.unscaled())

	if err := writeLexHeader(bw, val.Sign() < 0, exponent); err != nil {
		return err
	}

	return writeLexSignificand(bw, val, prec)
}

func writeLexHeader(bw *bufio.Writer, negative bool, exponent int) error {
	if negative {
		return writeLexNegativeHeader(bw, exponent)
	}

	return writeLexPositiveHeader(bw, exponent)
}

func writeLexNegativeHeader(bw *bufio.Writer, exponent int) error {
	if exponent >= lexInlineExpLo && exponent < lexInlineExpHi {
		return bw.WriteByte(byte(lexNegExpBase - exponent))
	}

	lead := byte(lexNegPosExpLd)
	if exponent < 0 {
		lead = lexNegNegExpLd
	}

	if err := bw.WriteByte(lead); err != nil {
		return err
	}

	return encodeInt32BE(exponent^lexPosExpXOR, bw)
}

func writeLexPositiveHeader(bw *bufio.Writer, exponent int) error {
	if exponent >= lexInlineExpLo && exponent < lexInlineExpHi {
		return bw.WriteByte(byte(exponent + lexPosExpBase))
	}

	lead := byte(lexPosNegExpLd)
	if exponent >= 0 {
		lead = lexPosPosExpLd
	}

	if err := bw.WriteByte(lead); err != nil {
		return err
	}

	return encodeInt32BE(exponent^lexNegExpXOR, bw)
}

func writeLexSignificand(bw *bufio.Writer, val *big.Int, prec int) error {
	terminator, val := lexTerminator(val, prec)

	digitAdjust := lexDigitAdjust
	if val.Sign() < 0 {
		digitAdjust = lexNegAdjust
		terminator = lexTermMax - terminator
	}

	pos := ((val.BitLen() + lexExpDigits9) / lexBitsPerDigit) + 1
	digits := make([]int, pos)
	pos--
	digits[pos] = terminator

	thousand := lexThousand()

	var rem big.Int

	for val.Sign() != 0 {
		val.QuoRem(val, thousand, &rem)

		pos--
		v := int(rem.Int64()) + digitAdjust

		if pos < 0 {
			digits = append([]int{v}, digits...)
		} else {
			digits[pos] = v
		}
	}

	return writeLexDigits(bw, digits)
}

func lexTerminator(val *big.Int, prec int) (int, *big.Int) {
	const (
		groupSize = 3
		oneExtra  = 1
		twoExtra  = 2
	)

	switch prec % groupSize {
	case oneExtra:
		return 0, new(big.Int).Mul(val, lexHundred())
	case twoExtra:
		return 1, new(big.Int).Mul(val, lexTen())
	default:
		return lexTermFull, val
	}
}

func writeLexDigits(bw *bufio.Writer, digits []int) error {
	accum := 0

	var bits uint

	for _, v := range digits {
		accum = accum<<lexBitsPerDigit | v
		bits += lexBitsPerDigit

		for {
			bits -= lexBitsPerByte
			if err := bw.WriteByte(byte((accum >> bits) & lexByteMask)); err != nil {
				return err
			}

			if bits < lexBitsPerByte {
				break
			}
		}
	}

	if bits != 0 {
		return bw.WriteByte(byte((accum << (lexBitsPerByte - bits)) & lexByteMask))
	}

	return nil
}

func lexNumDigits() []int64 {
	return []int64{0, 9, 99, 999, 9999, 99999, 999999, 9999999, 99999999, 999999999, lexBaseCube}
}

func lexPrecision(d *decimal) int {
	b := new(big.Int).Set(d.unscaled())
	if b.Sign() == 0 {
		return 1
	}

	b.Abs(b)

	billion := lexBillion()

	digits := 0
	for b.Cmp(billion) > 0 {
		b.Quo(b, billion)
		digits += lexDigitsPerGrp
	}

	small := b.Int64()
	for i, dd := range lexNumDigits() {
		if small <= dd {
			return digits + i
		}
	}

	return digits
}

// decodeLexDecimal reads a lexicographic-decimal range key from br.
func decodeLexDecimal(br *bufio.Reader) (*decimal, error) {
	b, err := br.ReadByte()
	if err != nil {
		return nil, err
	}

	header := int(b) & lexByteMask

	switch header {
	case lexNullHigh, lexNullLow:
		return nil, errBadLexDecimal
	case lexNegZero, lexZero:
		return newDecimal(big.NewInt(0), 0), nil
	}

	digitAdjust, exponent, err := decodeLexExponent(br, b, header)
	if err != nil {
		return nil, err
	}

	return decodeLexSignificand(br, exponent, digitAdjust)
}

func decodeLexExponent(br *bufio.Reader, first byte, header int) (int, int, error) {
	// lexPosNegExpThreshold separates inline positive- and negative-signum
	// exponents in the default header range.
	const lexPosNegExpThreshold = 0x82

	switch header {
	case lexNegPosExpLd, lexNegNegExpLd:
		v, err := decodeInt32BE(br)
		if err != nil {
			return 0, 0, err
		}

		return lexNegAdjust, v ^ lexPosExpXOR, nil
	case lexPosNegExpLd, lexPosPosExpLd:
		v, err := decodeInt32BE(br)
		if err != nil {
			return 0, 0, err
		}

		return lexDigitAdjust, v ^ lexNegExpXOR, nil
	default:
		exponent := int(first) & lexByteMask
		if exponent >= lexPosNegExpThreshold {
			return lexDigitAdjust, exponent - lexPosExpBase, nil
		}

		return lexNegAdjust, lexNegExpBase - exponent, nil
	}
}

func decodeLexSignificand(br *bufio.Reader, exponent, digitAdjust int) (*decimal, error) {
	st := &lexDecodeState{digitAdjust: digitAdjust}

	for !st.done {
		b, err := br.ReadByte()
		if err != nil {
			return nil, err
		}

		st.consume(b)
	}

	scale := st.precision - exponent

	return newDecimal(st.unscaled, scale), nil
}

// lexDecodeState accumulates base-1000 digits while decoding a significand.
type lexDecodeState struct {
	unscaled    *big.Int
	lastDigit   *big.Int
	accum       int
	bits        uint
	precision   int
	digitAdjust int
	done        bool
}

func (s *lexDecodeState) consume(b byte) {
	s.accum = (s.accum << lexBitsPerByte) | (int(b) & lexByteMask)
	s.bits += lexBitsPerByte

	if s.bits < lexBitsPerDigit {
		return
	}

	digit := (s.accum >> (s.bits - lexBitsPerDigit)) & lexDigitMask

	// The two-digit and one-digit terminators are the symmetric pairs around
	// the base, e.g. 0/1023 and 1/1022.
	const (
		termOneA, termOneB     = 0, lexTermMax
		termTwoA, termTwoB     = 1, lexTermMax - 1
		termThreeA, termThreeB = 2, lexTermMax - 2
		precOne                = 1
		precTwo                = 2
	)

	switch digit {
	case termOneA, termOneB:
		s.terminate(lexHundred(), precOne)
	case termTwoA, termTwoB:
		s.terminate(lexTen(), precTwo)
	case termThreeA, termThreeB:
		s.terminateExact()
	default:
		s.appendDigit(digit)
	}
}

// terminate finalizes the significand for the 1- or 2-trailing-digit cases.
func (s *lexDecodeState) terminate(div *big.Int, precInc int) {
	s.lastDigit.Quo(s.lastDigit, div)

	if s.unscaled == nil {
		s.unscaled = s.lastDigit
	} else {
		const precTwo = 2

		mul := lexTen()
		if precInc == precTwo {
			mul = lexHundred()
		}

		s.unscaled.Mul(s.unscaled, mul)
		s.unscaled.Add(s.unscaled, s.lastDigit)
	}

	s.precision += precInc
	s.done = true
}

// terminateExact finalizes the significand when the last group is a full triple.
func (s *lexDecodeState) terminateExact() {
	const groupSize = 3

	if s.unscaled == nil {
		s.unscaled = s.lastDigit
	} else {
		s.unscaled.Mul(s.unscaled, lexThousand())
		s.unscaled.Add(s.unscaled, s.lastDigit)
	}

	s.precision += groupSize
	s.done = true
}

func (s *lexDecodeState) appendDigit(digit int) {
	const groupSize = 3

	if s.unscaled == nil {
		s.unscaled = s.lastDigit
		if s.unscaled != nil {
			s.precision += groupSize
		}
	} else {
		s.unscaled.Mul(s.unscaled, lexThousand())
		s.unscaled.Add(s.unscaled, s.lastDigit)
		s.precision += groupSize
	}

	s.bits -= lexBitsPerDigit
	s.lastDigit = big.NewInt(int64(digit - s.digitAdjust))
}

// lexWord32 masks an int to its low 32 bits.
const lexWord32 = 0xffffffff

func encodeInt32BE(v int, bw *bufio.Writer) error {
	var buf [4]byte

	// v carries a 32-bit value; the explicit mask keeps the conversion
	// well-defined and bounded.
	binary.BigEndian.PutUint32(buf[:], uint32(v&lexWord32))

	_, err := bw.Write(buf[:])

	return err
}

func decodeInt32BE(br *bufio.Reader) (int, error) {
	var buf [4]byte

	if _, err := readFull(br, buf[:]); err != nil {
		return 0, err
	}

	u := binary.BigEndian.Uint32(buf[:])

	// Reinterpret the 32-bit pattern as signed without an int32 conversion that
	// the linter flags; values >= 2^31 become negative as intended.
	const (
		signBit = 1 << 31
		wrap    = 1 << 32
	)

	if u >= signBit {
		return int(u) - wrap, nil
	}

	return int(u), nil
}

func readFull(br *bufio.Reader, p []byte) (int, error) {
	n := 0
	for n < len(p) {
		b, err := br.ReadByte()
		if err != nil {
			return n, err
		}

		p[n] = b
		n++
	}

	return n, nil
}
