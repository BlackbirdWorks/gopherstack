package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCheckParseWarnings proves gopherstack-7o96's second fix: an
// unparseable ops:/families: entry must fail the gendocs run rather than
// silently undercounting it -- the same principle as cmd/opcensus' ERROR-row
// change (gopherstack-c7s3).
func TestCheckParseWarnings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		docs    []*ParityDoc
		wantErr bool
	}{
		{
			name: "no warnings across any doc",
			docs: []*ParityDoc{
				{Warnings: nil},
				{Warnings: []string{}},
			},
			wantErr: false,
		},
		{
			name: "one warning on one doc fails the run",
			docs: []*ParityDoc{
				{Warnings: nil},
				{Warnings: []string{"services/example/PARITY.md:5: entry-like line did not parse as an entry"}},
			},
			wantErr: true,
		},
		{
			name: "warnings on multiple docs still fail the run",
			docs: []*ParityDoc{
				{Warnings: []string{"a"}},
				{Warnings: []string{"b", "c"}},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := checkParseWarnings(tc.docs)

			if tc.wantErr {
				require.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
