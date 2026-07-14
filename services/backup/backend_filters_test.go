package backup_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/backup"
)

func TestParseTimeFilter(t *testing.T) {
	t.Parallel()
	cases := []struct {
		input   string
		wantNil bool
	}{
		{input: "", wantNil: true},
		{input: "not-a-date", wantNil: true},
		{input: "2024-01-15T12:00:00Z", wantNil: false},
		{input: "2024-01-15T12:00:00+05:30", wantNil: false},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			got := backup.ParseTimeFilter(tc.input)
			if tc.wantNil && got != nil {
				t.Errorf("expected nil, got %v", got)
			}
			if !tc.wantNil && got == nil {
				t.Error("expected non-nil time")
			}
		})
	}
}
