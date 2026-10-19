package awstime_test

import (
	"encoding/json"
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

func TestEpochTimeJSON(t *testing.T) {
	t.Parallel()

	type doc struct {
		At awstime.EpochTime  `json:"at"`
		Op *awstime.EpochTime `json:"op,omitempty"`
	}

	in := doc{At: awstime.NewEpochTime(time.Unix(1_700_000_000, 500_000_000).UTC())}

	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}

	if string(raw) != `{"at":1700000000.5}` {
		t.Fatalf("unexpected wire form %s", raw)
	}

	var out doc
	if err = json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}

	if !out.At.Equal(in.At.Time) || out.Op != nil {
		t.Fatalf("round trip lost data: %+v", out)
	}

	if err = json.Unmarshal([]byte(`{"at":"2026-01-01"}`), &out); err == nil {
		t.Fatal("string timestamp must be rejected")
	}
}
