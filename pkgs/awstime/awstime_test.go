package awstime_test

import (
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

func TestEpoch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   time.Time
		name string
		want float64
	}{
		{
			name: "zero time returns zero",
			in:   time.Time{},
			want: 0,
		},
		{
			name: "whole seconds",
			in:   time.Unix(1_700_000_000, 0).UTC(),
			want: 1_700_000_000,
		},
		{
			name: "sub-second precision preserved",
			in:   time.Unix(1_700_000_000, 500_000_000).UTC(),
			want: 1_700_000_000.5,
		},
		{
			name: "epoch start",
			in:   time.Unix(0, 0).UTC(),
			// Unix(0,0) is not the zero Time, so it serializes as 0 seconds.
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := awstime.Epoch(tt.in)
			if got != tt.want {
				t.Errorf("Epoch(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
