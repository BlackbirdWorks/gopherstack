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
		{
			name: "inapplicable verdict with reasoning is valid",
			rows: []Row{
				{
					Service: "personalize", Class: "filter_default_semantics", Verdict: "inapplicable",
					Date: "2026-08-30", Commit: "ac5c674d2", Source: "parity",
					Reasoning: "recipeProvider has exactly one legal value, so no legal value could change the result",
				},
			},
			wantErr: nil,
		},
		{
			name: "inapplicable verdict with no reasoning fails loudly",
			rows: []Row{
				{
					Service: "personalize", Class: "filter_default_semantics", Verdict: "inapplicable",
					Date: "2026-08-30", Commit: "ac5c674d2",
				},
			},
			wantErr: []string{
				"row 0 (service=personalize, class=filter_default_semantics): inapplicable verdict has no reasoning recorded",
			},
		},
		{
			name: "multi-source row is valid",
			rows: []Row{
				{
					Service: "opensearch", Class: "filter_default_semantics", Verdict: "fixed",
					Date: "2026-08-30", Commit: "c75ee725b", Source: "commit+parity",
				},
			},
			wantErr: nil,
		},
		{
			name: "unknown source tag fails loudly",
			rows: []Row{
				{
					Service: "opensearch", Class: "wrong_wire_key", Verdict: "fixed",
					Date: "2026-08-29", Commit: "a576f56ca", Source: "hunch",
				},
			},
			wantErr: []string{
				`row 0 (service=opensearch): source "hunch" is not empty or a '+'-joined list of ` +
					`commit/parity/bd_comment with no duplicates`,
			},
		},
		{
			name: "duplicate source tag fails loudly",
			rows: []Row{
				{
					Service: "opensearch", Class: "wrong_wire_key", Verdict: "fixed",
					Date: "2026-08-29", Commit: "a576f56ca", Source: "parity+parity",
				},
			},
			wantErr: []string{
				`row 0 (service=opensearch): source "parity+parity" is not empty or a '+'-joined list of ` +
					`commit/parity/bd_comment with no duplicates`,
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

func TestValidateConflicts(t *testing.T) {
	t.Parallel()

	knownServices := map[string]bool{"opensearch": true, "medialive": true, "personalize": true}

	tests := []struct {
		name      string
		conflicts []Conflict
		rows      []Row
		wantErr   []string
	}{
		{
			name: "a well-formed conflict with no matching row is valid",
			conflicts: []Conflict{
				{
					Service: "medialive",
					Class:   "filter_default_semantics",
					Note:    "PARITY.md records this clean; a bd comment records a bug fixed here in the same class",
				},
			},
			wantErr: nil,
		},
		{
			name: "unknown service fails loudly",
			conflicts: []Conflict{
				{Service: "notaservice", Class: "wrong_wire_key", Note: "two sources disagree"},
			},
			wantErr: []string{
				`conflict 0: service "notaservice" has no directory under services/`,
			},
		},
		{
			name: "unknown class fails loudly",
			conflicts: []Conflict{
				{Service: "medialive", Class: "wrong_shoe_size", Note: "two sources disagree"},
			},
			wantErr: []string{
				`conflict 0 (service=medialive): class "wrong_shoe_size" is not one of the known classes`,
			},
		},
		{
			name: "empty note fails loudly",
			conflicts: []Conflict{
				{Service: "medialive", Class: "wrong_wire_key", Note: ""},
			},
			wantErr: []string{
				"conflict 0 (service=medialive, class=wrong_wire_key): no note recording what the sources disagree about",
			},
		},
		{
			name: "duplicate conflict entries fail loudly",
			conflicts: []Conflict{
				{Service: "medialive", Class: "wrong_wire_key", Note: "PARITY says clean, commit says fixed"},
				{Service: "medialive", Class: "wrong_wire_key", Note: "same pair, recorded twice"},
			},
			wantErr: []string{
				"conflict 1: duplicate conflict entry for (service=medialive, class=wrong_wire_key)",
			},
		},
		{
			name: "a conflict colliding with a resolved row fails loudly",
			conflicts: []Conflict{
				{
					Service: "opensearch",
					Class:   "wrong_wire_key",
					Note:    "two sources disagree, but this pair already has a row",
				},
			},
			rows: []Row{
				{
					Service: "opensearch",
					Class:   "wrong_wire_key",
					Verdict: "fixed",
					Date:    "2026-08-29",
					Commit:  "a576f56ca",
				},
			},
			wantErr: []string{
				"conflict 0: (service=opensearch, class=wrong_wire_key) has both a resolved row and an open conflict -- " +
					"resolve the conflict or remove the row",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ValidateConflicts(tt.conflicts, tt.rows, knownServices)

			if tt.wantErr == nil {
				assert.Empty(t, got)

				return
			}

			require.Len(t, got, len(tt.wantErr))
			assert.Equal(t, tt.wantErr, got)
		})
	}
}
