package ptrconv_test

import (
	"testing"

	"Gopherstack/pkgs/ptrconv"
)

func TestPtrconvBasics(t *testing.T) {
	t.Parallel()

	t.Run("Int64", func(t *testing.T) {
		t.Parallel()
		if got := ptrconv.Int64(nil); got != 0 {
			t.Fatalf("expected 0, got %d", got)
		}
		v := int64(42)
		if got := ptrconv.Int64(&v); got != 42 {
			t.Fatalf("expected 42, got %d", got)
		}
	})

	t.Run("Bool", func(t *testing.T) {
		t.Parallel()
		if got := ptrconv.Bool(nil); got {
			t.Fatalf("expected false, got true")
		}
		v := true
		if got := ptrconv.Bool(&v); !got {
			t.Fatalf("expected true, got false")
		}
	})

	t.Run("String", func(t *testing.T) {
		t.Parallel()
		if got := ptrconv.String(nil); got != "" {
			t.Fatalf("expected empty string, got %q", got)
		}
		v := "ok"
		if got := ptrconv.String(&v); got != "ok" {
			t.Fatalf("expected ok, got %q", got)
		}
	})

	t.Run("Float64", func(t *testing.T) {
		t.Parallel()
		if got := ptrconv.Float64(nil); got != 0 {
			t.Fatalf("expected 0, got %v", got)
		}
		v := 1.25
		if got := ptrconv.Float64(&v); got != 1.25 {
			t.Fatalf("expected 1.25, got %v", got)
		}
	})
}

func TestInt64FromAny(t *testing.T) {
	t.Parallel()

	if got := ptrconv.Int64FromAny(float64(3.2)); got == nil || *got != 3 {
		t.Fatalf("expected 3, got %v", got)
	}

	if got := ptrconv.Int64FromAny(int(7)); got == nil || *got != 7 {
		t.Fatalf("expected 7, got %v", got)
	}

	if got := ptrconv.Int64FromAny("nope"); got != nil {
		t.Fatalf("expected nil, got %v", *got)
	}
}

func TestNilIfEmpty(t *testing.T) {
	t.Parallel()

	if got := ptrconv.NilIfEmpty(""); got != nil {
		t.Fatalf("expected nil, got %v", *got)
	}

	if got := ptrconv.NilIfEmpty("x"); got == nil || *got != "x" {
		t.Fatalf("expected x, got %v", got)
	}
}
