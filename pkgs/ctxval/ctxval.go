// Package ctxval is a type-safe wrapper over context.Context values using
// generics. A Key[T] is a strongly-typed, package-private context key bound
// to a value type T; callers get compile-time guarantees that they cannot
// stash an int under a "*Metadata" key or vice versa, and they cannot collide
// with another package's keys because each Key is a distinct address.
//
// Typical use:
//
//	var metaKey = ctxval.NewKey[*Metadata]("awsmeta")
//
//	func Set(ctx context.Context, m *Metadata) context.Context {
//	    return metaKey.Set(ctx, m)
//	}
//
//	func Get(ctx context.Context) (*Metadata, bool) {
//	    return metaKey.Get(ctx)
//	}
package ctxval

import "context"

// Key is a strongly-typed context key bound to value type T. Two Key instances
// are distinct iff they were created by separate calls to NewKey — the name
// is for diagnostics and does not affect identity.
type Key[T any] struct {
	name string
}

// NewKey returns a fresh Key[T]. Each call produces a unique key by address;
// the name is used only for the String method and error messages.
func NewKey[T any](name string) *Key[T] {
	return &Key[T]{name: name}
}

// Name reports the diagnostic name passed to NewKey.
func (k *Key[T]) Name() string {
	if k == nil {
		return ""
	}

	return k.name
}

// String implements fmt.Stringer for friendlier debug output.
func (k *Key[T]) String() string {
	return "ctxval.Key(" + k.Name() + ")"
}

// Set returns a child context that carries v under k. Storing the zero value
// of T is legal; callers that want "absent" semantics should use a pointer T
// and store nil to mean "explicitly cleared".
func (k *Key[T]) Set(ctx context.Context, v T) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	return context.WithValue(ctx, k, v)
}

// Get returns the value carried under k and true, or the zero value of T and
// false when no value is present. Callers that prefer a single-value API can
// wrap this with a fallback helper (see GetOr).
func (k *Key[T]) Get(ctx context.Context) (T, bool) {
	var zero T

	if ctx == nil || k == nil {
		return zero, false
	}

	v, ok := ctx.Value(k).(T)
	if !ok {
		return zero, false
	}

	return v, true
}

// GetOr returns the value carried under k, or fallback if none is present.
// Useful when callers want a single-value API.
func (k *Key[T]) GetOr(ctx context.Context, fallback T) T {
	if v, ok := k.Get(ctx); ok {
		return v
	}

	return fallback
}

// MustGet returns the value carried under k or panics with a descriptive
// message when none is present. Use only in code paths where the caller has
// guaranteed (via middleware or a test setup) that the value will be set.
func (k *Key[T]) MustGet(ctx context.Context) T {
	v, ok := k.Get(ctx)
	if !ok {
		panic("ctxval: missing value for key " + k.String())
	}

	return v
}
