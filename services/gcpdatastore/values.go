package gcpdatastore

import (
	"bytes"
	"math"
	"strings"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxIndexedBytes  = 1500
	maxUnindexedSize = 1000000
	maxEntityBytes   = 1048572
	maxEntityDepth   = 20
	meaningReserved  = 18
	keyProperty      = "__key__"
	entriesPerValue  = 2
	nanosPerMicro    = 1000
)

// Value type buckets in index order; values of different buckets never satisfy an inequality together.
const (
	bucketNull = iota
	bucketInt
	bucketBool
	bucketBlob
	bucketString
	bucketDouble
	bucketGeo
	bucketKey
	bucketOther
)

func bucketOf(v *datastorepb.Value) int {
	switch v.GetValueType().(type) {
	case *datastorepb.Value_NullValue:
		return bucketNull
	case *datastorepb.Value_IntegerValue, *datastorepb.Value_TimestampValue:
		return bucketInt
	case *datastorepb.Value_BooleanValue:
		return bucketBool
	case *datastorepb.Value_BlobValue:
		return bucketBlob
	case *datastorepb.Value_StringValue:
		return bucketString
	case *datastorepb.Value_DoubleValue:
		return bucketDouble
	case *datastorepb.Value_GeoPointValue:
		return bucketGeo
	case *datastorepb.Value_KeyValue:
		return bucketKey
	default:
		return bucketOther
	}
}

func intOf(v *datastorepb.Value) int64 {
	if t, ok := v.GetValueType().(*datastorepb.Value_TimestampValue); ok {
		return t.TimestampValue.AsTime().UnixMicro()
	}

	return v.GetIntegerValue()
}

func cmpOrdered[T int64 | float64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpDouble(a, b float64) int {
	an, bn := math.IsNaN(a), math.IsNaN(b)

	switch {
	case an && bn:
		return 0
	case an:
		return 1
	case bn:
		return -1
	default:
		return cmpOrdered(a, b)
	}
}

func cmpBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	default:
		return 1
	}
}

// compareValues totals the index order: by type bucket, then within the type.
func compareValues(a, b *datastorepb.Value) int {
	ba, bb := bucketOf(a), bucketOf(b)
	if ba != bb {
		return cmpOrdered(int64(ba), int64(bb))
	}

	switch ba {
	case bucketInt:
		return cmpOrdered(intOf(a), intOf(b))
	case bucketBool:
		return cmpBool(a.GetBooleanValue(), b.GetBooleanValue())
	case bucketBlob:
		return bytes.Compare(a.GetBlobValue(), b.GetBlobValue())
	case bucketString:
		return strings.Compare(a.GetStringValue(), b.GetStringValue())
	case bucketDouble:
		return cmpDouble(a.GetDoubleValue(), b.GetDoubleValue())
	case bucketGeo:
		if c := cmpOrdered(a.GetGeoPointValue().GetLatitude(), b.GetGeoPointValue().GetLatitude()); c != 0 {
			return c
		}

		return cmpOrdered(a.GetGeoPointValue().GetLongitude(), b.GetGeoPointValue().GetLongitude())
	case bucketKey:
		return compareKeys(a.GetKeyValue(), b.GetKeyValue())
	default:
		return 0
	}
}

func keyValue(k *datastorepb.Key) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_KeyValue{KeyValue: k}}
}

func intValue(n int64) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_IntegerValue{IntegerValue: n}}
}

func doubleValue(f float64) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_DoubleValue{DoubleValue: f}}
}

func nullValue() *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_NullValue{}}
}

func timestampValue(t *timestamppb.Timestamp) *datastorepb.Value {
	return &datastorepb.Value{ValueType: &datastorepb.Value_TimestampValue{TimestampValue: t}}
}

func arrayElems(v *datastorepb.Value) []*datastorepb.Value {
	if a, ok := v.GetValueType().(*datastorepb.Value_ArrayValue); ok {
		return a.ArrayValue.GetValues()
	}

	return nil
}

func isArray(v *datastorepb.Value) bool {
	_, ok := v.GetValueType().(*datastorepb.Value_ArrayValue)

	return ok
}

// indexedLeaves flattens v into the values an index holds: array elements individually, unindexed ones dropped.
func indexedLeaves(v *datastorepb.Value) []*datastorepb.Value {
	if isArray(v) {
		out := make([]*datastorepb.Value, 0, len(arrayElems(v)))

		for _, e := range arrayElems(v) {
			out = append(out, indexedLeaves(e)...)
		}

		return out
	}

	if v.GetExcludeFromIndexes() || bucketOf(v) == bucketOther {
		return nil
	}

	return []*datastorepb.Value{v}
}

// propValues resolves a (possibly dotted) property path to its indexed values.
func propValues(e *datastorepb.Entity, path string) []*datastorepb.Value {
	if v, ok := e.GetProperties()[path]; ok {
		return indexedLeaves(v)
	}

	head, rest, dotted := strings.Cut(path, ".")
	if !dotted {
		return nil
	}

	v, ok := e.GetProperties()[head]
	if !ok {
		return nil
	}

	return descend(v, rest)
}

func descend(v *datastorepb.Value, rest string) []*datastorepb.Value {
	if isArray(v) {
		out := make([]*datastorepb.Value, 0, len(arrayElems(v)))

		for _, el := range arrayElems(v) {
			out = append(out, descend(el, rest)...)
		}

		return out
	}

	if v.GetExcludeFromIndexes() {
		return nil
	}

	if ev, ok := v.GetValueType().(*datastorepb.Value_EntityValue); ok {
		return propValues(ev.EntityValue, rest)
	}

	return nil
}

func countIndexed(e *datastorepb.Entity) int {
	if e == nil {
		return 0
	}

	n := 0

	for _, v := range e.GetProperties() {
		n += entriesPerValue * len(indexedLeaves(v))
	}

	return n
}

func checkValue(name string, v *datastorepb.Value, depth int, inArray bool) error {
	if v.GetMeaning() == meaningReserved {
		return errInvalid("The value of property %q must not have meaning %d.", name, meaningReserved)
	}

	switch t := v.GetValueType().(type) {
	case *datastorepb.Value_ArrayValue:
		if inArray {
			return errInvalid("Property %q: an array value cannot contain another array.", name)
		}

		for _, el := range t.ArrayValue.GetValues() {
			if err := checkValue(name, el, depth, true); err != nil {
				return err
			}
		}
	case *datastorepb.Value_StringValue:
		return checkSize(name, len(t.StringValue), v.GetExcludeFromIndexes())
	case *datastorepb.Value_BlobValue:
		return checkSize(name, len(t.BlobValue), v.GetExcludeFromIndexes())
	case *datastorepb.Value_EntityValue:
		if depth+1 > maxEntityDepth {
			return errInvalid("Property %q: entity values are nested deeper than %d levels.", name, maxEntityDepth)
		}

		return checkProps(t.EntityValue, depth+1)
	case *datastorepb.Value_KeyValue:
		if t.KeyValue == nil || len(t.KeyValue.GetPath()) == 0 {
			return errInvalid("Property %q has an empty key value.", name)
		}
	}

	return nil
}

func checkSize(name string, n int, unindexed bool) error {
	if unindexed {
		if n > maxUnindexedSize {
			return errInvalid("The value of property %q is longer than %d bytes.", name, maxUnindexedSize)
		}

		return nil
	}

	if n > maxIndexedBytes {
		return errInvalid("The value of property %q is longer than %d bytes.", name, maxIndexedBytes)
	}

	return nil
}

func checkProps(e *datastorepb.Entity, depth int) error {
	for name, v := range e.GetProperties() {
		if name == "" {
			return errInvalid("Property names must not be empty.")
		}

		if len(name) > maxNameBytes {
			return errInvalid("Property name %q is longer than %d bytes.", name, maxNameBytes)
		}

		if isReserved(name) {
			return errInvalid("Property name %q is reserved/read-only.", name)
		}

		if v == nil || v.GetValueType() == nil {
			return errInvalid("Property %q has no value.", name)
		}

		if err := checkValue(name, v, depth, false); err != nil {
			return err
		}
	}

	return nil
}

// prepareEntity validates e and returns a copy with timestamps truncated to microseconds.
func prepareEntity(e *datastorepb.Entity, req partition) (*datastorepb.Entity, error) {
	if err := checkProps(e, 0); err != nil {
		return nil, err
	}

	out, ok := proto.Clone(e).(*datastorepb.Entity)
	if !ok {
		return nil, errInvalid("invalid entity")
	}

	truncateTimes(out)

	for _, pv := range out.GetProperties() {
		stampValueKeys(pv, func(k *datastorepb.Key) *datastorepb.Key { return stampKey(k, req, false) })
	}

	if proto.Size(out) > maxEntityBytes {
		return nil, errInvalid("The entity is larger than the maximum of %d bytes.", maxEntityBytes)
	}

	return out, nil
}

func truncateTimes(e *datastorepb.Entity) {
	for _, v := range e.GetProperties() {
		truncateValue(v)
	}
}

func truncateValue(v *datastorepb.Value) {
	switch t := v.GetValueType().(type) {
	case *datastorepb.Value_TimestampValue:
		t.TimestampValue.Nanos -= t.TimestampValue.GetNanos() % nanosPerMicro
	case *datastorepb.Value_ArrayValue:
		for _, el := range t.ArrayValue.GetValues() {
			truncateValue(el)
		}
	case *datastorepb.Value_EntityValue:
		truncateTimes(t.EntityValue)
	}
}
