package ctxval_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ctxval"
)

type sample struct {
	N int
	S string
}

func TestKey_NameAndString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		keyName   string
		wantName  string
		wantStr   string
		nilKey    bool
		wantEmpty bool
	}{
		{name: "non-empty", keyName: "foo", wantName: "foo", wantStr: "ctxval.Key(foo)"},
		{name: "empty", keyName: "", wantName: "", wantStr: "ctxval.Key()"},
		{name: "nil-key", nilKey: true, wantEmpty: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.nilKey {
				var k *ctxval.Key[int]
				assert.Equal(t, "", k.Name())

				return
			}

			k := ctxval.NewKey[int](tc.keyName)
			assert.Equal(t, tc.wantName, k.Name())
			assert.Equal(t, tc.wantStr, k.String())
		})
	}
}

func TestSetGet_PrimitiveTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func() (context.Context, func(context.Context) (any, bool))
		name     string
		wantVal  any
		wantOk   bool
		setNoVal bool
	}{
		{
			name: "int-roundtrip",
			setup: func() (context.Context, func(context.Context) (any, bool)) {
				k := ctxval.NewKey[int]("n")
				ctx := k.Set(context.Background(), 42)

				return ctx, func(c context.Context) (any, bool) { return k.Get(c) }
			},
			wantVal: 42, wantOk: true,
		},
		{
			name: "string-roundtrip",
			setup: func() (context.Context, func(context.Context) (any, bool)) {
				k := ctxval.NewKey[string]("s")
				ctx := k.Set(context.Background(), "hello")

				return ctx, func(c context.Context) (any, bool) { return k.Get(c) }
			},
			wantVal: "hello", wantOk: true,
		},
		{
			name: "absent-returns-zero",
			setup: func() (context.Context, func(context.Context) (any, bool)) {
				k := ctxval.NewKey[int]("n")

				return context.Background(), func(c context.Context) (any, bool) { return k.Get(c) }
			},
			wantVal: 0, wantOk: false,
		},
		{
			name: "nil-context-returns-zero",
			setup: func() (context.Context, func(context.Context) (any, bool)) {
				k := ctxval.NewKey[int]("n")

				return nil, func(c context.Context) (any, bool) { return k.Get(c) }
			},
			wantVal: 0, wantOk: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, getter := tc.setup()
			v, ok := getter(ctx)
			assert.Equal(t, tc.wantOk, ok)
			assert.Equal(t, tc.wantVal, v)
		})
	}
}

func TestSetGet_StructAndPointer(t *testing.T) {
	t.Parallel()

	k := ctxval.NewKey[*sample]("sample")
	in := &sample{N: 7, S: "x"}
	ctx := k.Set(context.Background(), in)

	got, ok := k.Get(ctx)
	require.True(t, ok)
	assert.Same(t, in, got)
	assert.Equal(t, 7, got.N)
	assert.Equal(t, "x", got.S)
}

func TestGetOr_AndMustGet(t *testing.T) {
	t.Parallel()

	k := ctxval.NewKey[int]("n")

	// GetOr: absent → fallback.
	assert.Equal(t, 99, k.GetOr(context.Background(), 99))

	// GetOr: present → stored value.
	ctx := k.Set(context.Background(), 5)
	assert.Equal(t, 5, k.GetOr(ctx, 99))

	// MustGet: present → value.
	assert.Equal(t, 5, k.MustGet(ctx))

	// MustGet: absent → panic.
	assert.Panics(t, func() {
		k.MustGet(context.Background())
	})
}

func TestKey_Isolation(t *testing.T) {
	t.Parallel()

	// Two keys for the same type must not collide.
	k1 := ctxval.NewKey[int]("a")
	k2 := ctxval.NewKey[int]("b")
	ctx := k1.Set(context.Background(), 1)
	ctx = k2.Set(ctx, 2)

	v1, ok1 := k1.Get(ctx)
	v2, ok2 := k2.Get(ctx)

	assert.True(t, ok1)
	assert.Equal(t, 1, v1)
	assert.True(t, ok2)
	assert.Equal(t, 2, v2)
}

func TestSet_NilContextIsCoercedToBackground(t *testing.T) {
	t.Parallel()

	k := ctxval.NewKey[string]("s")
	ctx := k.Set(nil, "x") //nolint:staticcheck // testing nil-tolerance

	v, ok := k.Get(ctx)
	require.True(t, ok)
	assert.Equal(t, "x", v)
}
