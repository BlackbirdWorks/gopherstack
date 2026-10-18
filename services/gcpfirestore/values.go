package gcpfirestore

import (
	"bytes"
	"cmp"
	"math"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	rankNull = iota
	rankBool
	rankNumber
	rankTimestamp
	rankString
	rankBytes
	rankRef
	rankGeo
	rankArray
	rankMap

	maxValueDepth = 20
	two63         = 9223372036854775808.0
)

func valueRank(v *firestorepb.Value) int {
	switch v.GetValueType().(type) {
	case *firestorepb.Value_BooleanValue:
		return rankBool
	case *firestorepb.Value_IntegerValue, *firestorepb.Value_DoubleValue:
		return rankNumber
	case *firestorepb.Value_TimestampValue:
		return rankTimestamp
	case *firestorepb.Value_StringValue:
		return rankString
	case *firestorepb.Value_BytesValue:
		return rankBytes
	case *firestorepb.Value_ReferenceValue:
		return rankRef
	case *firestorepb.Value_GeoPointValue:
		return rankGeo
	case *firestorepb.Value_ArrayValue:
		return rankArray
	case *firestorepb.Value_MapValue:
		return rankMap
	default:
		return rankNull
	}
}

func isNaN(v *firestorepb.Value) bool {
	d, ok := v.GetValueType().(*firestorepb.Value_DoubleValue)

	return ok && math.IsNaN(d.DoubleValue)
}

func isNumber(v *firestorepb.Value) bool { return valueRank(v) == rankNumber }

func compareIntDouble(i int64, d float64) int {
	switch {
	case math.IsNaN(d):
		return 1
	case d >= two63:
		return -1
	case d < -two63:
		return 1
	}

	t := math.Trunc(d)
	if c := cmp.Compare(i, int64(t)); c != 0 {
		return c
	}

	switch frac := d - t; {
	case frac > 0:
		return -1
	case frac < 0:
		return 1
	}

	return 0
}

func compareDoubles(a, b float64) int {
	an, bn := math.IsNaN(a), math.IsNaN(b)

	switch {
	case an && bn:
		return 0
	case an:
		return -1
	case bn:
		return 1
	}

	return cmp.Compare(a, b)
}

func compareNumbers(a, b *firestorepb.Value) int {
	ai, aInt := a.GetValueType().(*firestorepb.Value_IntegerValue)
	bi, bInt := b.GetValueType().(*firestorepb.Value_IntegerValue)

	switch {
	case aInt && bInt:
		return cmp.Compare(ai.IntegerValue, bi.IntegerValue)
	case aInt:
		return compareIntDouble(ai.IntegerValue, b.GetDoubleValue())
	case bInt:
		return -compareIntDouble(bi.IntegerValue, a.GetDoubleValue())
	}

	return compareDoubles(a.GetDoubleValue(), b.GetDoubleValue())
}

func compareRefs(a, b string) int {
	as, bs := strings.Split(a, "/"), strings.Split(b, "/")

	for i := 0; i < len(as) && i < len(bs); i++ {
		if c := strings.Compare(as[i], bs[i]); c != 0 {
			return c
		}
	}

	return cmp.Compare(len(as), len(bs))
}

func compareTimestamps(a, b *timestamppb.Timestamp) int {
	if c := cmp.Compare(a.GetSeconds(), b.GetSeconds()); c != 0 {
		return c
	}

	return cmp.Compare(a.GetNanos(), b.GetNanos())
}

func compareArrays(a, b []*firestorepb.Value) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if c := compareValues(a[i], b[i]); c != 0 {
			return c
		}
	}

	return cmp.Compare(len(a), len(b))
}

func sortedKeys(m map[string]*firestorepb.Value) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

func compareMaps(a, b map[string]*firestorepb.Value) int {
	ak, bk := sortedKeys(a), sortedKeys(b)

	for i := 0; i < len(ak) && i < len(bk); i++ {
		if c := strings.Compare(ak[i], bk[i]); c != 0 {
			return c
		}

		if c := compareValues(a[ak[i]], b[bk[i]]); c != 0 {
			return c
		}
	}

	return cmp.Compare(len(ak), len(bk))
}

// compareValues orders values by the Firestore cross-type ordering.
func compareValues(a, b *firestorepb.Value) int {
	ra, rb := valueRank(a), valueRank(b)
	if ra != rb {
		return cmp.Compare(ra, rb)
	}

	switch ra {
	case rankBool:
		return compareBools(a.GetBooleanValue(), b.GetBooleanValue())
	case rankNumber:
		return compareNumbers(a, b)
	case rankTimestamp:
		return compareTimestamps(a.GetTimestampValue(), b.GetTimestampValue())
	case rankString:
		return strings.Compare(a.GetStringValue(), b.GetStringValue())
	case rankBytes:
		return bytes.Compare(a.GetBytesValue(), b.GetBytesValue())
	case rankRef:
		return compareRefs(a.GetReferenceValue(), b.GetReferenceValue())
	case rankGeo:
		if c := cmp.Compare(a.GetGeoPointValue().GetLatitude(), b.GetGeoPointValue().GetLatitude()); c != 0 {
			return c
		}

		return cmp.Compare(a.GetGeoPointValue().GetLongitude(), b.GetGeoPointValue().GetLongitude())
	case rankArray:
		return compareArrays(a.GetArrayValue().GetValues(), b.GetArrayValue().GetValues())
	case rankMap:
		return compareMaps(a.GetMapValue().GetFields(), b.GetMapValue().GetFields())
	}

	return 0
}

func compareBools(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	}

	return 1
}

// valuesEqual is Firestore equality: 1 equals 1.0 and NaN equals nothing.
func valuesEqual(a, b *firestorepb.Value) bool {
	if isNaN(a) || isNaN(b) {
		return false
	}

	return compareValues(a, b) == 0
}

func intVal(i int64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_IntegerValue{IntegerValue: i}}
}

func doubleVal(d float64) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_DoubleValue{DoubleValue: d}}
}

func refVal(name string) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_ReferenceValue{ReferenceValue: name}}
}

func tsVal(t time.Time) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_TimestampValue{TimestampValue: timestamppb.New(t)}}
}

func nullVal() *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_NullValue{}}
}

func arrayVal(vs []*firestorepb.Value) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_ArrayValue{
		ArrayValue: &firestorepb.ArrayValue{Values: vs},
	}}
}

func mapVal(fields map[string]*firestorepb.Value) *firestorepb.Value {
	return &firestorepb.Value{ValueType: &firestorepb.Value_MapValue{
		MapValue: &firestorepb.MapValue{Fields: fields},
	}}
}

// normalizeFields validates fields and returns a deep copy with timestamps truncated to microseconds.
func normalizeFields(fields map[string]*firestorepb.Value) (map[string]*firestorepb.Value, error) {
	out := make(map[string]*firestorepb.Value, len(fields))

	for k, v := range fields {
		if k == "" {
			return nil, errInvalid("Document field names must not be empty.")
		}

		nv, err := normalizeValue(v, 1)
		if err != nil {
			return nil, err
		}

		out[k] = nv
	}

	return out, nil
}

func normalizeValue(v *firestorepb.Value, depth int) (*firestorepb.Value, error) {
	if depth > maxValueDepth {
		return nil, errInvalid("Document fields nested too deeply (maximum depth is %d).", maxValueDepth)
	}

	if v == nil || v.GetValueType() == nil {
		return nil, errInvalid("Value must have a value type set.")
	}

	switch t := v.GetValueType().(type) {
	case *firestorepb.Value_TimestampValue:
		ts := t.TimestampValue
		if err := ts.CheckValid(); err != nil {
			return nil, errInvalid("Invalid timestamp value: %v", err)
		}

		return tsVal(ts.AsTime().Truncate(time.Microsecond)), nil
	case *firestorepb.Value_ArrayValue:
		vals := make([]*firestorepb.Value, 0, len(t.ArrayValue.GetValues()))

		for _, e := range t.ArrayValue.GetValues() {
			nv, err := normalizeValue(e, depth+1)
			if err != nil {
				return nil, err
			}

			vals = append(vals, nv)
		}

		return arrayVal(vals), nil
	case *firestorepb.Value_MapValue:
		fields := make(map[string]*firestorepb.Value, len(t.MapValue.GetFields()))

		for k, e := range t.MapValue.GetFields() {
			nv, err := normalizeValue(e, depth+1)
			if err != nil {
				return nil, err
			}

			fields[k] = nv
		}

		return mapVal(fields), nil
	case *firestorepb.Value_FieldReferenceValue, *firestorepb.Value_VariableReferenceValue,
		*firestorepb.Value_FunctionValue, *firestorepb.Value_PipelineValue:
		return nil, errInvalid("Pipeline expression values are not supported in documents.")
	}

	c, _ := proto.Clone(v).(*firestorepb.Value)

	return c, nil
}
