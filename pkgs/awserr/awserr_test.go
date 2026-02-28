package awserr_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func TestSentinelsAreDistinct(t *testing.T) {
	t.Parallel()

	sentinels := []struct {
		name string
		err  error
	}{
		{"ErrNotFound", awserr.ErrNotFound},
		{"ErrAlreadyExists", awserr.ErrAlreadyExists},
		{"ErrInvalidParameter", awserr.ErrInvalidParameter},
		{"ErrConflict", awserr.ErrConflict},
	}

	for i, a := range sentinels {
		for j, b := range sentinels {
			if i == j {
				continue
			}
			if errors.Is(a.err, b.err) {
				t.Errorf("%s should not match %s", a.name, b.name)
			}
		}
	}
}

func TestWrappedErrorsIs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		sentinel error
		want     bool
	}{
		{
			name:     "wrapped ErrNotFound matches ErrNotFound",
			err:      awserr.New("ResourceNotFoundException", awserr.ErrNotFound),
			sentinel: awserr.ErrNotFound,
			want:     true,
		},
		{
			name:     "wrapped ErrAlreadyExists matches ErrAlreadyExists",
			err:      awserr.New("ResourceInUseException", awserr.ErrAlreadyExists),
			sentinel: awserr.ErrAlreadyExists,
			want:     true,
		},
		{
			name:     "wrapped ErrNotFound does not match ErrAlreadyExists",
			err:      awserr.New("ResourceNotFoundException", awserr.ErrNotFound),
			sentinel: awserr.ErrAlreadyExists,
			want:     false,
		},
		{
			name:     "wrapped ErrAlreadyExists does not match ErrNotFound",
			err:      awserr.New("ResourceInUseException", awserr.ErrAlreadyExists),
			sentinel: awserr.ErrNotFound,
			want:     false,
		},
		{
			name:     "fmt.Errorf wrapped ErrNotFound matches via chain",
			err:      fmt.Errorf("outer: %w", awserr.New("ResourceNotFoundException", awserr.ErrNotFound)),
			sentinel: awserr.ErrNotFound,
			want:     true,
		},
		{
			name:     "wrapped error preserves message",
			err:      awserr.New("ResourceNotFoundException", awserr.ErrNotFound),
			sentinel: awserr.ErrNotFound,
			want:     true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := errors.Is(tc.err, tc.sentinel)
			if got != tc.want {
				t.Errorf("errors.Is(%q, sentinel) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestWrappedErrorMessage(t *testing.T) {
	t.Parallel()

	err := awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	if err.Error() != "ResourceNotFoundException" {
		t.Errorf("Error() = %q, want %q", err.Error(), "ResourceNotFoundException")
	}
}
