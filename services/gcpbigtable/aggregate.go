package gcpbigtable

import (
	"encoding/binary"
	"math"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

type aggregator int

const (
	aggNone aggregator = iota
	aggSum
	aggMax
	aggMin
)

// aggregatorOf returns the aggregator of an Aggregate family.
func aggregatorOf(cf *adminpb.ColumnFamily) aggregator {
	switch cf.GetValueType().GetAggregateType().GetAggregator().(type) {
	case *adminpb.Type_Aggregate_Sum_:
		return aggSum
	case *adminpb.Type_Aggregate_Max_:
		return aggMax
	case *adminpb.Type_Aggregate_Min_:
		return aggMin
	default:
		return aggNone
	}
}

// validateAggregateFamily checks an Aggregate value_type against the Type.Aggregate docs: Sum, Max and Min take an
// Int64 input with a full encoding, and the state type equals the input type.
func validateAggregateFamily(cf *adminpb.ColumnFamily) error {
	agg := cf.GetValueType().GetAggregateType()

	switch agg.GetAggregator().(type) {
	case *adminpb.Type_Aggregate_Sum_, *adminpb.Type_Aggregate_Max_, *adminpb.Type_Aggregate_Min_:
	case *adminpb.Type_Aggregate_HllppUniqueCount:
		return errUnimplemented("HyperLogLogPlusPlusUniqueCount aggregates are not supported by this emulator.")
	default:
		return errInvalid("An aggregate type needs an aggregator.")
	}

	in := agg.GetInputType().GetInt64Type()
	if in == nil {
		return errInvalid("Sum, Max and Min aggregates take an Int64 input_type.")
	}

	if _, ok := in.GetEncoding().GetEncoding().(*adminpb.Type_Int64_Encoding_BigEndianBytes_); !ok {
		return errInvalid("The input_type of an aggregate must specify the BigEndianBytes Int64 encoding.")
	}

	agg.StateType = clone(agg.GetInputType())

	return nil
}

func isAggregate(cf *adminpb.ColumnFamily) bool {
	return cf.GetValueType().GetAggregateType() != nil
}

// checkPlainFamily rejects direct writes to an Aggregate family.
func (t *tableRec) checkPlainFamily(fam string) error {
	if err := t.checkFamily(fam); err != nil {
		return err
	}

	if isAggregate(t.pb.GetColumnFamilies()[fam]) {
		return errInvalid("Column family %q is an aggregate family: write it with AddToCell or MergeToCell.", fam)
	}

	return nil
}

type aggMutation struct {
	fam   string
	qual  string
	ts    int64
	in    int64
	agg   aggregator
	isNil bool
}

func (t *tableRec) validateAggregateMutation(
	fam string, qual, ts, in *btpb.Value, merge bool,
) (aggMutation, error) {
	if err := t.checkFamily(fam); err != nil {
		return aggMutation{}, err
	}

	agg := aggregatorOf(t.pb.GetColumnFamilies()[fam])
	if agg == aggNone {
		return aggMutation{}, errInvalid("Column family %q is not a Sum, Max or Min aggregate family.", fam)
	}

	q, ok := qual.GetKind().(*btpb.Value_RawValue)
	if !ok {
		return aggMutation{}, errInvalid("column_qualifier must be a raw_value.")
	}

	if len(q.RawValue) > maxQualLen {
		return aggMutation{}, errInvalid("Column qualifier is too long (maximum %d bytes).", maxQualLen)
	}

	tv, ok := ts.GetKind().(*btpb.Value_RawTimestampMicros)
	if !ok {
		return aggMutation{}, errInvalid("timestamp must be a raw_timestamp_micros.")
	}

	if tv.RawTimestampMicros < 0 || tv.RawTimestampMicros%t.gran() != 0 {
		return aggMutation{}, errInvalid(
			"Timestamp granularity mismatch. Expected a non-negative multiple of %d microseconds, got %d.",
			t.gran(), tv.RawTimestampMicros)
	}

	m := aggMutation{fam: fam, qual: string(q.RawValue), ts: tv.RawTimestampMicros, agg: agg}

	switch iv := in.GetKind().(type) {
	case *btpb.Value_IntValue:
		m.in = iv.IntValue
	case *btpb.Value_RawValue:
		if len(iv.RawValue) != int64Len {
			return aggMutation{}, errInvalid("A raw input of an Int64 aggregate must be %d big-endian bytes.", int64Len)
		}

		m.in = int64(binary.BigEndian.Uint64(iv.RawValue)) //nolint:gosec // two's complement reinterpretation
	case nil:
		if !merge {
			return aggMutation{}, errInvalid("AddToCell needs an input value.")
		}

		m.isNil = true
	default:
		return aggMutation{}, errInvalid("The input of an Int64 aggregate must be an int_value or 8 raw bytes.")
	}

	return m, nil
}

func combine(agg aggregator, cur, in int64) int64 {
	switch agg {
	case aggMax:
		return max(cur, in)
	case aggMin:
		return min(cur, in)
	case aggNone, aggSum:
	}

	switch {
	case in > 0 && cur > math.MaxInt64-in:
		return math.MaxInt64
	case in < 0 && cur < math.MinInt64-in:
		return math.MinInt64
	}

	return cur + in
}

// add accumulates m into the cell at its timestamp; a missing cell takes the input as its state.
func (r *rowRec) add(m aggMutation) {
	if m.isNil {
		return
	}

	next := m.in

	for _, c := range r.fams[m.fam][m.qual] {
		if c.ts == m.ts && len(c.val) == int64Len {
			cur := int64(binary.BigEndian.Uint64(c.val)) //nolint:gosec // two's complement reinterpretation
			next = combine(m.agg, cur, m.in)

			break
		}
	}

	val := make([]byte, int64Len)
	binary.BigEndian.PutUint64(val, uint64(next)) //nolint:gosec // two's complement reinterpretation
	r.set(m.fam, m.qual, m.ts, val)
}
