package dataplane

import (
	"math/big"
	"strconv"
	"strings"
)

// decimal is an arbitrary-precision signed decimal: unscaled * 10^-scale. It
// backs the CBOR tag-4 number encoding used by DAX for non-integer numeric
// attribute values. This mirrors the Decimal type in the DAX client's cbor
// package.
type decimal struct {
	value big.Int
	scale int
}

func newDecimal(unscaled *big.Int, scale int) *decimal {
	d := &decimal{scale: scale}
	d.value.Set(unscaled)

	return d
}

// setString parses a DynamoDB number literal into a decimal.
func (d *decimal) setString(s string) bool {
	if s == "" {
		return false
	}

	var exp int64

	if sep := strings.IndexAny(s, "Ee"); sep >= 0 {
		e, err := strconv.ParseInt(s[sep+1:], base10, 32)
		if err != nil {
			return false
		}

		exp = e
		s = s[:sep]
	}

	dot := strings.LastIndexByte(s, '.')
	if dot < 0 {
		if _, ok := d.value.SetString(s, base10); !ok {
			return false
		}

		d.scale = int(-exp)

		return true
	}

	exp -= int64(len(s) - dot - 1)
	if _, ok := d.value.SetString(s[:dot]+s[dot+1:], base10); !ok {
		return false
	}

	d.scale = int(-exp)

	return true
}

// String renders the decimal as a DynamoDB-compatible number literal.
func (d *decimal) String() string {
	if d.scale == 0 {
		return d.value.String()
	}

	var buf []byte
	buf = d.value.Append(buf, base10)
	buf = append(buf, 'E')
	buf = strconv.AppendInt(buf, -int64(d.scale), base10)

	return string(buf)
}

func (d *decimal) unscaled() *big.Int { return &d.value }
