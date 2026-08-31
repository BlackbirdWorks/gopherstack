package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidate(t *testing.T) {
	t.Parallel()

	knownServices := map[string]bool{"opensearch": true, "medialive": true, "personalize": true}

	tests := []struct {
		name    string
		rows    []Row
		wantErr []string
	}{
		{
			name: "clean ledger",
			rows: []Row{
				{
					Service: "opensearch",
					Class:   "wrong_wire_key",
					Verdict: "fixed",
					Date:    "2026-08-29",
					Commit:  "a576f56ca",
				},
				{
					Service: "medialive",
					Class:   "filter_default_semantics",
					Verdict: "clean",
					Date:    "2026-08-30",
					Commit:  "ac5c674d2",
				},
			},
			wantErr: nil,
		},
		{
			name: "unknown class name",
			rows: []Row{
				{
					Service: "opensearch",
					Class:   "wrong_shoe_size",
					Verdict: "fixed",
					Date:    "2026-08-29",
					Commit:  "a576f56ca",
				},
			},
			wantErr: []string{
				`row 0 (service=opensearch): class "wrong_shoe_size" is not one of the known classes`,
			},
		},
		{
			name: "service not present under services",
			rows: []Row{
				{
					Service: "notaservice",
					Class:   "wrong_wire_key",
					Verdict: "fixed",
					Date:    "2026-08-29",
					Commit:  "a576f56ca",
				},
			},
			wantErr: []string{
				`row 0: service "notaservice" has no directory under services/`,
			},
		},
		{
			name: "duplicate row for the same service and class",
			rows: []Row{
				{
					Service: "opensearch",
					Class:   "wrong_wire_key",
					Verdict: "fixed",
					Date:    "2026-08-29",
					Commit:  "a576f56ca",
				},
				{
					Service: "opensearch",
					Class:   "wrong_wire_key",
					Verdict: "clean",
					Date:    "2026-08-30",
					Commit:  "dd3cbde76",
				},
			},
			wantErr: []string{
				"row 1: duplicate row for (service=opensearch, class=wrong_wire_key) -- also at commit a576f56ca " +
					"(2026-08-29), this one at commit dd3cbde76 (2026-08-30)",
			},
		},
		{
			name: "unknown verdict",
			rows: []Row{
				{
					Service: "opensearch",
					Class:   "wrong_wire_key",
					Verdict: "probably_fine",
					Date:    "2026-08-29",
					Commit:  "a576f56ca",
				},
			},
			wantErr: []string{
				`row 0 (service=opensearch): verdict "probably_fine" is not fixed, clean, or inapplicable`,
			},
		},
		{
			name: "missing commit",
			rows: []Row{
				{Service: "opensearch", Class: "wrong_wire_key", Verdict: "fixed", Date: "2026-08-29", Commit: ""},
			},
			wantErr: []string{
				"row 0 (service=opensearch): no commit recorded as evidence",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := Validate(tt.rows, knownServices)

			if tt.wantErr == nil {
				assert.Empty(t, got)

				return
			}

			require.Len(t, got, len(tt.wantErr))
			assert.Equal(t, tt.wantErr, got)
		})
	}
}
