// Package testsnap asserts that a value handed out by a backend is not
// mutated by later backend transitions (async completion, janitor sweeps).
package testsnap

import (
	"reflect"

	"github.com/stretchr/testify/assert"
)

// TB is the subset of testing.TB that Unchanged needs.
type TB interface {
	assert.TestingT
	Helper()
}

// Unchanged deep-copies *held, runs transition, then asserts *held still
// equals the copy. Unexported fields are shared with the copy, so they are
// not checked.
func Unchanged[T any](t TB, held *T, transition func()) {
	t.Helper()

	snap := deepCopy(reflect.ValueOf(held)).Interface().(*T)

	transition()

	assert.Equal(t, snap, held, "handed-out value was mutated by a backend transition")
}

func deepCopy(v reflect.Value) reflect.Value {
	out := reflect.New(v.Type()).Elem()
	copyInto(out, v)

	return out
}

func copyInto(dst, src reflect.Value) {
	switch src.Kind() {
	case reflect.Pointer:
		if src.IsNil() {
			return
		}

		dst.Set(reflect.New(src.Type().Elem()))
		copyInto(dst.Elem(), src.Elem())
	case reflect.Slice:
		if src.IsNil() {
			return
		}

		dst.Set(reflect.MakeSlice(src.Type(), src.Len(), src.Len()))

		for i := range src.Len() {
			copyInto(dst.Index(i), src.Index(i))
		}
	case reflect.Map:
		if src.IsNil() {
			return
		}

		dst.Set(reflect.MakeMapWithSize(src.Type(), src.Len()))

		for it := src.MapRange(); it.Next(); {
			val := reflect.New(src.Type().Elem()).Elem()
			copyInto(val, it.Value())
			dst.SetMapIndex(it.Key(), val)
		}
	case reflect.Struct:
		dst.Set(src)

		for i := range src.NumField() {
			if src.Type().Field(i).IsExported() {
				copyInto(dst.Field(i), src.Field(i))
			}
		}
	case reflect.Interface:
		if src.IsNil() {
			return
		}

		inner := reflect.New(src.Elem().Type()).Elem()
		copyInto(inner, src.Elem())
		dst.Set(inner)
	default:
		dst.Set(src)
	}
}
