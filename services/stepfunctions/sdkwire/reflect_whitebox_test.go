package sdkwire

import (
	"encoding/base64"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
)

func fixedTime() time.Time {
	return time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
}

const maxDepth = 5

// fill builds a value of type t with deterministic sentinels: every scalar
// set, one element per list or map, unions and documents left nil.
func fill(t reflect.Type, depth int, seen map[reflect.Type]int) reflect.Value {
	switch t.Kind() {
	case reflect.Pointer:
		if depth > maxDepth {
			return reflect.Zero(t)
		}

		v := reflect.New(t.Elem())
		v.Elem().Set(fill(t.Elem(), depth+1, seen))

		return v
	case reflect.Struct:
		return fillStruct(t, depth, seen)
	case reflect.Slice:
		return fillSlice(t, depth, seen)
	case reflect.Map:
		m := reflect.MakeMap(t)
		if t.Elem().Kind() == reflect.Interface {
			return m
		}

		m.SetMapIndex(reflect.ValueOf("k").Convert(t.Key()), fill(t.Elem(), depth+1, seen))

		return m
	case reflect.String:
		return reflect.ValueOf("v").Convert(t)
	case reflect.Bool:
		return reflect.ValueOf(true).Convert(t)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return reflect.ValueOf(7).Convert(t)
	case reflect.Float32, reflect.Float64:
		return reflect.ValueOf(1.5).Convert(t)
	default:
		return reflect.Zero(t)
	}
}

func fillStruct(t reflect.Type, depth int, seen map[reflect.Type]int) reflect.Value {
	v := reflect.New(t).Elem()

	if t == reflect.TypeFor[time.Time]() {
		v.Set(reflect.ValueOf(fixedTime()))

		return v
	}

	if seen[t] >= 2 || depth > maxDepth {
		return v
	}

	seen[t]++
	defer func() { seen[t]-- }()

	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() || f.Name == "ResultMetadata" {
			continue
		}

		v.Field(i).Set(fill(f.Type, depth+1, seen))
	}

	return v
}

func fillSlice(t reflect.Type, depth int, seen map[reflect.Type]int) reflect.Value {
	if t.Elem().Kind() == reflect.Uint8 {
		return reflect.ValueOf([]byte("blob")).Convert(t)
	}

	if t.Elem().Kind() == reflect.Interface {
		return reflect.MakeSlice(t, 0, 0)
	}

	s := reflect.MakeSlice(t, 1, 1)
	s.Index(0).Set(fill(t.Elem(), depth+1, seen))

	return s
}

// toValue mirrors how Step Functions exposes an SDK struct: PascalCase field
// names, nil members omitted, timestamps as ISO strings, blobs as base64.
func toValue(v reflect.Value) (any, bool) {
	if !v.IsValid() {
		return nil, false
	}

	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return nil, false
		}

		return toValue(v.Elem())
	case reflect.Interface:
		return toInterface(v)
	case reflect.Struct:
		return toStruct(v), true
	case reflect.Slice:
		return toSlice(v)
	case reflect.Map:
		return toMap(v)
	case reflect.String:
		return v.String(), true
	case reflect.Bool:
		return v.Bool(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint()), true
	case reflect.Float32, reflect.Float64:
		return v.Float(), true
	default:
		return nil, false
	}
}

func toInterface(v reflect.Value) (any, bool) {
	if v.IsNil() {
		return nil, false
	}

	concrete := v.Elem()

	if r, ok := reflect.TypeAssert[io.Reader](concrete); ok {
		b, _ := io.ReadAll(r)

		return string(b), true
	}

	if ct := concrete.Type(); ct.Kind() == reflect.Pointer {
		if _, member, ok := strings.Cut(ct.Elem().Name(), "Member"); ok {
			inner, present := toValue(concrete.Elem().FieldByName("Value"))
			if !present {
				return nil, false
			}

			return map[string]any{member: inner}, true
		}
	}

	return toValue(concrete)
}

func toStruct(v reflect.Value) any {
	if t, ok := reflect.TypeAssert[time.Time](v); ok {
		return t.UTC().Format(TimeLayout)
	}

	out := map[string]any{}

	for i := range v.NumField() {
		f := v.Type().Field(i)
		if !f.IsExported() || f.Name == "ResultMetadata" {
			continue
		}

		if isZeroScalar(v.Field(i)) {
			continue
		}

		if val, ok := toValue(v.Field(i)); ok {
			out[strings.TrimSuffix(f.Name, "_")] = val
		}
	}

	return out
}

func isZeroScalar(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return v.IsZero()
	default:
		return false
	}
}

func toSlice(v reflect.Value) (any, bool) {
	if v.IsNil() {
		return nil, false
	}

	if v.Type().Elem().Kind() == reflect.Uint8 {
		return base64.StdEncoding.EncodeToString(v.Bytes()), true
	}

	out := make([]any, 0, v.Len())

	for i := range v.Len() {
		if val, ok := toValue(v.Index(i)); ok {
			out = append(out, val)
		}
	}

	return out, true
}

func toMap(v reflect.Value) (any, bool) {
	if v.IsNil() {
		return nil, false
	}

	out := make(map[string]any, v.Len())

	for it := v.MapRange(); it.Next(); {
		if val, ok := toValue(it.Value()); ok {
			out[fmt.Sprint(it.Key().Interface())] = val
		}
	}

	return out, true
}
