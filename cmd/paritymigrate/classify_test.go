package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		content     string
		wantVerdict verdict
	}{
		{
			name: "still open: no fix mentioned anywhere else",
			content: `service: example
gaps:
  - "SomeOperation ignores the FilterCriteria field entirely; not implemented."
---
`,
			wantVerdict: verdictStillOpen,
		},
		{
			name: "resolved elsewhere: fixed with a date in the ops: notes",
			content: `service: example
ops:
  SomeOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-01: now honors FilterCriteria."}
gaps:
  - "SomeOperation ignores the FilterCriteria field entirely; not implemented."
---
`,
			wantVerdict: verdictResolvedElsewhere,
		},
		{
			name: "self-resolved: positive plus dated marker, no open language at all",
			content: `service: example
gaps:
  - "FIXED 2026-08-01 (gopherstack-xxxx): SomeOperation's FilterCriteria gap is closed."
---
`,
			wantVerdict: verdictSelfResolved,
		},
		{
			name: "no markers: plain disclosure outside this detector's vocabulary",
			content: `service: example
gaps:
  - "SomeOperation's FilterCriteria is server-computed and has no client-settable equivalent."
---
`,
			wantVerdict: verdictNoMarkers,
		},
		{
			name: "ambiguous partial: two named things, only one resolved",
			content: `service: example
ops:
  SomeOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-01: now honors FilterCriteria."}
gaps:
  - "SomeOperation ignores FilterCriteria (not implemented); OtherOperation also ignores SortOrder, still open."
---
`,
			wantVerdict: verdictAmbiguousPartial,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := parseManifest("example", "services/example/PARITY.md", tc.content)
			require.Len(t, m.sources, 1)
			require.Len(t, m.sources[0].items, 1)

			got := classify(m, m.sources[0].items[0])
			assert.Equal(t, tc.wantVerdict, got.verdict)
		})
	}
}
