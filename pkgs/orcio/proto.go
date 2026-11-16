//nolint:mnd // protobuf field numbers and ORC wire constants
package orcio

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

// walk calls fn for every field of a protobuf message; v is the varint or the length-delimited payload.
func walk(b []byte, fn func(num protowire.Number, v uint64, raw []byte) error) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return fmt.Errorf("%w: bad protobuf tag", ErrBadFile)
		}

		b = b[n:]

		var (
			v   uint64
			raw []byte
			m   int
		)

		switch typ {
		case protowire.VarintType:
			v, m = protowire.ConsumeVarint(b)
		case protowire.BytesType:
			raw, m = protowire.ConsumeBytes(b)
		case protowire.Fixed32Type:
			var f uint32

			f, m = protowire.ConsumeFixed32(b)
			v = uint64(f)
		case protowire.Fixed64Type:
			v, m = protowire.ConsumeFixed64(b)
		default:
			return fmt.Errorf("%w: unsupported protobuf wire type %d", ErrBadFile, typ)
		}

		if m < 0 {
			return fmt.Errorf("%w: truncated protobuf field %d", ErrBadFile, num)
		}

		b = b[m:]

		if err := fn(num, v, raw); err != nil {
			return err
		}
	}

	return nil
}

func packedUints(raw []byte) ([]uint64, error) {
	var out []uint64

	for len(raw) > 0 {
		v, n := protowire.ConsumeVarint(raw)
		if n < 0 {
			return nil, fmt.Errorf("%w: bad packed varint", ErrBadFile)
		}

		out = append(out, v)
		raw = raw[n:]
	}

	return out, nil
}

type postScript struct {
	footerLen   uint64
	metadataLen uint64
	blockSize   uint64
	compression int
}

func parsePostScript(b []byte) (postScript, error) {
	ps := postScript{blockSize: defaultBlockSize}
	magic := ""

	err := walk(b, func(num protowire.Number, v uint64, raw []byte) error {
		switch int32(num) {
		case 1:
			ps.footerLen = v
		case 2:
			ps.compression = capInt(v)
		case 3:
			ps.blockSize = v
		case 5:
			ps.metadataLen = v
		case 8000:
			magic = string(raw)
		}

		return nil
	})
	if err != nil {
		return ps, err
	}

	if magic != orcMagic {
		return ps, fmt.Errorf("%w: missing ORC magic in postscript", ErrBadFile)
	}

	return ps, nil
}

type stripeInfo struct {
	offset, indexLen, dataLen, footerLen, rows uint64
}

type typeInfo struct {
	attrs     map[string]string
	fieldName []string
	subtypes  []int
	kind      int
	precision int
	scale     int
}

type footer struct {
	stripes []stripeInfo
	types   []typeInfo
	rows    uint64
}

func parseFooter(b []byte) (footer, error) {
	var f footer

	err := walk(b, func(num protowire.Number, v uint64, raw []byte) error {
		switch int32(num) {
		case 3:
			s, err := parseStripeInfo(raw)
			f.stripes = append(f.stripes, s)

			return err
		case 4:
			t, err := parseType(raw)
			f.types = append(f.types, t)

			return err
		case 6:
			f.rows = v
		}

		return nil
	})

	return f, err
}

func parseStripeInfo(b []byte) (stripeInfo, error) {
	var s stripeInfo

	err := walk(b, func(num protowire.Number, v uint64, _ []byte) error {
		switch int32(num) {
		case 1:
			s.offset = v
		case 2:
			s.indexLen = v
		case 3:
			s.dataLen = v
		case 4:
			s.footerLen = v
		case 5:
			s.rows = v
		}

		return nil
	})

	return s, err
}

func parseType(b []byte) (typeInfo, error) {
	t := typeInfo{attrs: map[string]string{}}

	err := walk(b, func(num protowire.Number, v uint64, raw []byte) error {
		switch int32(num) {
		case 1:
			t.kind = capInt(v)
		case 2:
			if raw == nil {
				t.subtypes = append(t.subtypes, capInt(v))

				return nil
			}

			ids, err := packedUints(raw)
			for _, id := range ids {
				t.subtypes = append(t.subtypes, capInt(id))
			}

			return err
		case 3:
			t.fieldName = append(t.fieldName, string(raw))
		case 5:
			t.precision = capInt(v)
		case 6:
			t.scale = capInt(v)
		case 7:
			var k, val string

			err := walk(raw, func(n protowire.Number, _ uint64, r []byte) error {
				switch int32(n) {
				case 1:
					k = string(r)
				case 2:
					val = string(r)
				}

				return nil
			})
			t.attrs[k] = val

			return err
		}

		return nil
	})

	return t, err
}

type streamInfo struct {
	kind   int
	column int
	length uint64
}

type stripeFooter struct {
	timezone  string
	streams   []streamInfo
	encodings []int
	dictSizes []int
}

func parseStripeFooter(b []byte) (stripeFooter, error) {
	var sf stripeFooter

	err := walk(b, func(num protowire.Number, _ uint64, raw []byte) error {
		switch int32(num) {
		case 1:
			var s streamInfo

			err := walk(raw, func(n protowire.Number, v uint64, _ []byte) error {
				switch int32(n) {
				case 1:
					s.kind = capInt(v)
				case 2:
					s.column = capInt(v)
				case 3:
					s.length = v
				}

				return nil
			})
			sf.streams = append(sf.streams, s)

			return err
		case 2:
			kind, size := 0, 0

			err := walk(raw, func(n protowire.Number, v uint64, _ []byte) error {
				switch int32(n) {
				case 1:
					kind = capInt(v)
				case 2:
					size = capInt(v)
				}

				return nil
			})
			sf.encodings = append(sf.encodings, kind)
			sf.dictSizes = append(sf.dictSizes, size)

			return err
		case 3:
			sf.timezone = string(raw)
		}

		return nil
	})

	return sf, err
}
