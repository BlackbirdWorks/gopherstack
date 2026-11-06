package amp

import (
	"fmt"
	"math"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

// Field numbers of the remote-write Histogram, BucketSpan and Exemplar messages.
const (
	fieldHistCountInt    = 1
	fieldHistCountFloat  = 2
	fieldHistSum         = 3
	fieldHistSchema      = 4
	fieldHistZeroThresh  = 5
	fieldHistZeroInt     = 6
	fieldHistZeroFloat   = 7
	fieldHistNegSpans    = 8
	fieldHistNegDeltas   = 9
	fieldHistNegCounts   = 10
	fieldHistPosSpans    = 11
	fieldHistPosDeltas   = 12
	fieldHistPosCounts   = 13
	fieldHistTimestamp   = 15
	fieldSpanOffset      = 1
	fieldSpanLength      = 2
	fieldExemplarLabels  = 1
	fieldExemplarValue   = 2
	fieldExemplarTime    = 3
	maxExemplarRunes     = 128
	maxExemplarsPerWrite = 10000
)

func (e *histDecoder) bad(what string) error {
	return fmt.Errorf("%w: bad histogram %s", errBadWrite, what)
}

type histDecoder struct {
	h                    promql.FloatHistogram
	posDeltas, negDeltas []int64
	countInt, zeroInt    uint64
	intCount             bool
	t                    int64
}

func (d *histDecoder) field(num protowire.Number, typ protowire.Type, msg []byte) (int, error) {
	switch typ {
	case protowire.VarintType:
		v, n := protowire.ConsumeVarint(msg)
		if n < 0 {
			return 0, d.bad("varint")
		}

		d.varint(num, v)

		return n, nil
	case protowire.Fixed64Type:
		v, n := protowire.ConsumeFixed64(msg)
		if n < 0 {
			return 0, d.bad("double")
		}

		d.double(num, math.Float64frombits(v))

		return n, nil
	case protowire.BytesType:
		b, n := protowire.ConsumeBytes(msg)
		if n < 0 {
			return 0, d.bad("bytes")
		}

		return n, d.bytes(num, b)
	}

	n := protowire.ConsumeFieldValue(num, typ, msg)
	if n < 0 {
		return 0, d.bad("field")
	}

	return n, nil
}

func (d *histDecoder) varint(num protowire.Number, v uint64) {
	switch num {
	case fieldHistCountInt:
		d.countInt, d.intCount = v, true
	case fieldHistZeroInt:
		d.zeroInt = v
	case fieldHistSchema:
		d.h.Schema = int32(protowire.DecodeZigZag(v)) //nolint:gosec // sint32 range
	case fieldHistTimestamp:
		d.t = int64(v) //nolint:gosec // protobuf int64 round-trips through uint64
	}
}

func (d *histDecoder) double(num protowire.Number, v float64) {
	switch num {
	case fieldHistCountFloat:
		d.h.Count = v
	case fieldHistSum:
		d.h.Sum = v
	case fieldHistZeroThresh:
		d.h.ZeroThreshold = v
	case fieldHistZeroFloat:
		d.h.ZeroCount = v
	}
}

func (d *histDecoder) bytes(num protowire.Number, b []byte) error {
	var err error

	switch num {
	case fieldHistNegSpans:
		d.h.NegativeSpans, err = appendSpan(d.h.NegativeSpans, b)
	case fieldHistPosSpans:
		d.h.PositiveSpans, err = appendSpan(d.h.PositiveSpans, b)
	case fieldHistNegDeltas:
		d.negDeltas, err = packedSint(d.negDeltas, b)
	case fieldHistPosDeltas:
		d.posDeltas, err = packedSint(d.posDeltas, b)
	case fieldHistNegCounts:
		d.h.NegativeBuckets, err = packedDouble(d.h.NegativeBuckets, b)
	case fieldHistPosCounts:
		d.h.PositiveBuckets, err = packedDouble(d.h.PositiveBuckets, b)
	}

	return err
}

func appendSpan(out []promql.Span, b []byte) ([]promql.Span, error) {
	var sp promql.Span

	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || typ != protowire.VarintType {
			return out, fmt.Errorf("%w: bad bucket span", errBadWrite)
		}

		v, m := protowire.ConsumeVarint(b[n:])
		if m < 0 {
			return out, fmt.Errorf("%w: bad bucket span", errBadWrite)
		}

		switch num {
		case fieldSpanOffset:
			sp.Offset = int32(protowire.DecodeZigZag(v)) //nolint:gosec // sint32 range
		case fieldSpanLength:
			sp.Length = uint32(v) //nolint:gosec // uint32 field
		}

		b = b[n+m:]
	}

	return append(out, sp), nil
}

func packedSint(out []int64, b []byte) ([]int64, error) {
	for len(b) > 0 {
		v, n := protowire.ConsumeVarint(b)
		if n < 0 {
			return out, fmt.Errorf("%w: bad bucket deltas", errBadWrite)
		}

		out = append(out, protowire.DecodeZigZag(v))
		b = b[n:]
	}

	return out, nil
}

func packedDouble(out []float64, b []byte) ([]float64, error) {
	for len(b) > 0 {
		v, n := protowire.ConsumeFixed64(b)
		if n < 0 {
			return out, fmt.Errorf("%w: bad bucket counts", errBadWrite)
		}

		out = append(out, math.Float64frombits(v))
		b = b[n:]
	}

	return out, nil
}

func absolute(deltas []int64) []float64 {
	out := make([]float64, len(deltas))

	var cur int64

	for i, d := range deltas {
		cur += d
		out[i] = float64(cur)
	}

	return out
}

// decodeHistogram unpacks a remote-write Histogram into a sample carrying a float histogram.
func decodeHistogram(msg []byte) (promql.Sample, error) {
	d := &histDecoder{}

	for len(msg) > 0 {
		num, typ, n := protowire.ConsumeTag(msg)
		if n < 0 {
			return promql.Sample{}, d.bad("tag")
		}

		m, err := d.field(num, typ, msg[n:])
		if err != nil {
			return promql.Sample{}, err
		}

		msg = msg[n+m:]
	}

	h := d.h
	if d.intCount {
		h.Count = float64(d.countInt)
		h.ZeroCount = float64(d.zeroInt)
		h.PositiveBuckets = absolute(d.posDeltas)
		h.NegativeBuckets = absolute(d.negDeltas)
	}

	if promql.IsStale(h.Sum) {
		return promql.Sample{T: d.t, V: promql.StaleNaN()}, nil
	}

	if !h.Valid() || !finite(h.Sum, h.Count, h.ZeroCount, h.ZeroThreshold) {
		return promql.Sample{}, fmt.Errorf("%w: invalid native histogram (schema %d, span/bucket mismatch)",
			errBadWrite, h.Schema)
	}

	return promql.Sample{T: d.t, H: &h}, nil
}

func decodeExemplar(msg []byte) (Exemplar, error) {
	ex := Exemplar{Labels: promql.Labels{}}

	for len(msg) > 0 {
		num, typ, n := protowire.ConsumeTag(msg)
		if n < 0 {
			return ex, fmt.Errorf("%w: bad exemplar tag", errBadWrite)
		}

		msg = msg[n:]

		var m int

		switch {
		case num == fieldExemplarLabels && typ == protowire.BytesType:
			sub, k := protowire.ConsumeBytes(msg)
			m = k

			if k >= 0 {
				name, value, err := decodeLabel(sub)
				if err != nil {
					return ex, err
				}

				ex.Labels[name] = value
			}
		case num == fieldExemplarValue && typ == protowire.Fixed64Type:
			v, k := protowire.ConsumeFixed64(msg)
			m, ex.V = k, math.Float64frombits(v)
		case num == fieldExemplarTime && typ == protowire.VarintType:
			v, k := protowire.ConsumeVarint(msg)
			m, ex.T = k, int64(v) //nolint:gosec // protobuf int64 round-trips through uint64
		default:
			m = protowire.ConsumeFieldValue(num, typ, msg)
		}

		if m < 0 {
			return ex, fmt.Errorf("%w: bad exemplar field", errBadWrite)
		}

		msg = msg[m:]
	}

	return ex, nil
}

// exemplarRunes is the combined rune length of an exemplar's label names and values.
func exemplarRunes(ex Exemplar) int {
	n := 0

	for k, v := range ex.Labels {
		n += len([]rune(k)) + len([]rune(v))
	}

	return n
}

func finite(vs ...float64) bool {
	for _, v := range vs {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return false
		}
	}

	return true
}
