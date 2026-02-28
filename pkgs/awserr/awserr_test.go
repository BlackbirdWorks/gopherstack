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
		err  error
		name string
	}{
		{awserr.ErrNotFound, "ErrNotFound"},
		{awserr.ErrAlreadyExists, "ErrAlreadyExists"},
		{awserr.ErrInvalidParameter, "ErrInvalidParameter"},
		{awserr.ErrConflict, "ErrConflict"},
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
		err      error
		sentinel error
		name     string
		want     bool
	}{
		{
			err:      awserr.New("ResourceNotFoundException", awserr.ErrNotFound),
			sentinel: awserr.ErrNotFound,
			name:     "wrapped ErrNotFound matches ErrNotFound",
			want:     true,
		},
		{
			err:      awserr.New("ResourceInUseException", awserr.ErrAlreadyExists),
			sentinel: awserr.ErrAlreadyExists,
			name:     "wrapped ErrAlreadyExists matches ErrAlreadyExists",
			want:     true,
		},
		{
			err:      awserr.New("ResourceNotFoundException", awserr.ErrNotFound),
			sentinel: awserr.ErrAlreadyExists,
			name:     "wrapped ErrNotFound does not match ErrAlreadyExists",
			want:     false,
		},
		{
			err:      awserr.New("ResourceInUseException", awserr.ErrAlreadyExists),
			sentinel: awserr.ErrNotFound,
			name:     "wrapped ErrAlreadyExists does not match ErrNotFound",
			want:     false,
		},
		{
			err:      fmt.Errorf("outer: %w", awserr.New("ResourceNotFoundException", awserr.ErrNotFound)),
			sentinel: awserr.ErrNotFound,
			name:     "fmt.Errorf wrapped ErrNotFound matches via chain",
			want:     true,
		},
		{
			err:      awserr.New("ResourceNotFoundException", awserr.ErrNotFound),
			sentinel: awserr.ErrNotFound,
			name:     "wrapped error preserves message",
			want:     true,
		},
	}

	for _, tc := range tests {
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
