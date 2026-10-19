package amp

import (
	"errors"
	"fmt"
	"math"

	"github.com/klauspost/compress/snappy"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

// Field numbers of the Prometheus remote-write WriteRequest message.
const (
	fieldWriteTimeseries = 1
	fieldSeriesLabels    = 1
	fieldSeriesSamples   = 2
	fieldLabelName       = 1
	fieldLabelValue      = 2
	fieldSampleValue     = 1
	fieldSampleTimestamp = 2

	maxDecodedWrite = 32 << 20
)

var errBadWrite = errors.New("invalid remote write payload")

type writeSeries struct {
	Labels  promql.Labels
	Samples []promql.Sample
}

// decodeWriteRequest unpacks a snappy-compressed remote-write body.
func decodeWriteRequest(body []byte) ([]writeSeries, error) {
	n, err := snappy.DecodedLen(body)
	if err != nil || n > maxDecodedWrite {
		return nil, fmt.Errorf("%w: snappy decode failed", errBadWrite)
	}

	raw, err := snappy.Decode(nil, body)
	if err != nil {
		return nil, fmt.Errorf("%w: snappy decode failed", errBadWrite)
	}

	var out []writeSeries

	for len(raw) > 0 {
		num, typ, nn := protowire.ConsumeTag(raw)
		if nn < 0 {
			return nil, fmt.Errorf("%w: bad tag", errBadWrite)
		}

		raw = raw[nn:]

		if num == fieldWriteTimeseries && typ == protowire.BytesType {
			msg, m := protowire.ConsumeBytes(raw)
			if m < 0 {
				return nil, fmt.Errorf("%w: bad timeseries", errBadWrite)
			}

			ws, ierr := decodeTimeSeries(msg)
			if ierr != nil {
				return nil, ierr
			}

			out = append(out, ws)
			raw = raw[m:]

			continue
		}

		m := protowire.ConsumeFieldValue(num, typ, raw)
		if m < 0 {
			return nil, fmt.Errorf("%w: bad field", errBadWrite)
		}

		raw = raw[m:]
	}

	return out, nil
}

func decodeTimeSeries(msg []byte) (writeSeries, error) {
	ws := writeSeries{Labels: promql.Labels{}}

	for len(msg) > 0 {
		num, typ, n := protowire.ConsumeTag(msg)
		if n < 0 {
			return ws, fmt.Errorf("%w: bad series tag", errBadWrite)
		}

		msg = msg[n:]

		if typ == protowire.BytesType && (num == fieldSeriesLabels || num == fieldSeriesSamples) {
			sub, m := protowire.ConsumeBytes(msg)
			if m < 0 {
				return ws, fmt.Errorf("%w: bad series field", errBadWrite)
			}

			if err := ws.add(num, sub); err != nil {
				return ws, err
			}

			msg = msg[m:]

			continue
		}

		m := protowire.ConsumeFieldValue(num, typ, msg)
		if m < 0 {
			return ws, fmt.Errorf("%w: bad series field", errBadWrite)
		}

		msg = msg[m:]
	}

	return ws, nil
}

func (ws *writeSeries) add(num protowire.Number, sub []byte) error {
	if num == fieldSeriesLabels {
		name, value, err := decodeLabel(sub)
		if err != nil {
			return err
		}

		ws.Labels[name] = value

		return nil
	}

	s, err := decodeSample(sub)
	if err != nil {
		return err
	}

	ws.Samples = append(ws.Samples, s)

	return nil
}

func decodeLabel(msg []byte) (string, string, error) {
	var name, value string

	for len(msg) > 0 {
		num, typ, n := protowire.ConsumeTag(msg)
		if n < 0 {
			return "", "", fmt.Errorf("%w: bad label tag", errBadWrite)
		}

		msg = msg[n:]

		if typ == protowire.BytesType && (num == fieldLabelName || num == fieldLabelValue) {
			b, m := protowire.ConsumeBytes(msg)
			if m < 0 {
				return "", "", fmt.Errorf("%w: bad label", errBadWrite)
			}

			if num == fieldLabelName {
				name = string(b)
			} else {
				value = string(b)
			}

			msg = msg[m:]

			continue
		}

		m := protowire.ConsumeFieldValue(num, typ, msg)
		if m < 0 {
			return "", "", fmt.Errorf("%w: bad label field", errBadWrite)
		}

		msg = msg[m:]
	}

	return name, value, nil
}

func decodeSample(msg []byte) (promql.Sample, error) {
	var s promql.Sample

	for len(msg) > 0 {
		num, typ, n := protowire.ConsumeTag(msg)
		if n < 0 {
			return s, fmt.Errorf("%w: bad sample tag", errBadWrite)
		}

		msg = msg[n:]

		switch {
		case num == fieldSampleValue && typ == protowire.Fixed64Type:
			v, m := protowire.ConsumeFixed64(msg)
			if m < 0 {
				return s, fmt.Errorf("%w: bad sample value", errBadWrite)
			}

			s.V = math.Float64frombits(v)
			msg = msg[m:]
		case num == fieldSampleTimestamp && typ == protowire.VarintType:
			v, m := protowire.ConsumeVarint(msg)
			if m < 0 {
				return s, fmt.Errorf("%w: bad sample timestamp", errBadWrite)
			}

			s.T = int64(v) //nolint:gosec // protobuf int64 timestamp round-trips through uint64
			msg = msg[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, msg)
			if m < 0 {
				return s, fmt.Errorf("%w: bad sample field", errBadWrite)
			}

			msg = msg[m:]
		}
	}

	return s, nil
}

// encodeWriteRequest builds a snappy-compressed remote-write body (used by tests and tooling).
func encodeWriteRequest(series []writeSeries) []byte {
	var raw []byte

	for _, ws := range series {
		var ts []byte

		for k, v := range ws.Labels {
			var lb []byte

			lb = protowire.AppendTag(lb, fieldLabelName, protowire.BytesType)
			lb = protowire.AppendString(lb, k)
			lb = protowire.AppendTag(lb, fieldLabelValue, protowire.BytesType)
			lb = protowire.AppendString(lb, v)
			ts = protowire.AppendTag(ts, fieldSeriesLabels, protowire.BytesType)
			ts = protowire.AppendBytes(ts, lb)
		}

		for _, s := range ws.Samples {
			var sb []byte

			sb = protowire.AppendTag(sb, fieldSampleValue, protowire.Fixed64Type)
			sb = protowire.AppendFixed64(sb, math.Float64bits(s.V))
			sb = protowire.AppendTag(sb, fieldSampleTimestamp, protowire.VarintType)
			sb = protowire.AppendVarint(sb, uint64(s.T)) //nolint:gosec // int64 timestamp encoded as protobuf varint
			ts = protowire.AppendTag(ts, fieldSeriesSamples, protowire.BytesType)
			ts = protowire.AppendBytes(ts, sb)
		}

		raw = protowire.AppendTag(raw, fieldWriteTimeseries, protowire.BytesType)
		raw = protowire.AppendBytes(raw, ts)
	}

	return snappy.Encode(nil, raw)
}
