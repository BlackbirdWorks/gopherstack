package ptrconv_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func TestPtrconvBasics(t *testing.T) {
	t.Parallel()

	t.Run("Int64", func(t *testing.T) {
		t.Parallel()
		v42 := int64(42)
		tests := []struct {
			in   *int64
			name string
			want int64
		}{
			{in: nil, name: "nil", want: 0},
			{in: &v42, name: "value", want: 42},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.want, ptrconv.Int64(tt.in))
			})
		}
	})

	t.Run("Bool", func(t *testing.T) {
		t.Parallel()
		vTrue := true
		vFalse := false
		tests := []struct {
			in   *bool
			name string
			want bool
		}{
			{in: nil, name: "nil", want: false},
			{in: &vTrue, name: "true", want: true},
			{in: &vFalse, name: "false", want: false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.want, ptrconv.Bool(tt.in))
			})
		}
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()
		vOK := "ok"
		vEmpty := ""
		tests := []struct {
			in   *string
			name string
			want string
		}{
			{in: nil, name: "nil", want: ""},
			{in: &vOK, name: "ok", want: "ok"},
			{in: &vEmpty, name: "empty", want: ""},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.Equal(t, tt.want, ptrconv.String(tt.in))
			})
		}
	})

	t.Run("Float64", func(t *testing.T) {
		t.Parallel()
		v125 := 1.25
		tests := []struct {
			in   *float64
			name string
			want float64
		}{
			{in: nil, name: "nil", want: 0},
			{in: &v125, name: "value", want: 1.25},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				assert.InDelta(t, tt.want, ptrconv.Float64(tt.in), 1e-9)
			})
		}
	})
}

func TestInt64FromAny(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   any
		want *int64
		name string
	}{
		{in: float64(3.2), want: int64Ptr(3), name: "float64"},
		{in: int(7), want: int64Ptr(7), name: "int"},
		{in: "nope", want: nil, name: "string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ptrconv.Int64FromAny(tt.in)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("Int64FromAny() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNilIfEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want *string
		name string
		in   string
	}{
		{want: nil, name: "empty", in: ""},
		{want: stringPtr("x"), name: "populated", in: "x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ptrconv.NilIfEmpty(tt.in)
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Errorf("NilIfEmpty() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func int64Ptr(v int64) *int64 {
	p := new(int64)
	*p = v

	return p
}
func stringPtr(v string) *string {
	p := new(string)
	*p = v

	return p
}
