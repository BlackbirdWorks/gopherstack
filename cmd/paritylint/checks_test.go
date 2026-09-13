package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckMissingItemsStillOpen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		want    int
	}{
		{
			name: "missing entirely",
			content: `service: example
gaps: []
---
`,
			want: 1,
		},
		{
			name: "present but empty",
			content: `service: example
gaps: []
items_still_open: []
---
`,
			want: 0,
		},
		{
			name: "present with items",
			content: `service: example
items_still_open:
  - "some open item"
---
`,
			want: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			m := parseManifest("example", "services/example/PARITY.md", tc.content)
			got := checkMissingItemsStillOpen(m)
			assert.Len(t, got, tc.want)
		})
	}
}

func TestCheckResolvedElsewhere(t *testing.T) {
	t.Parallel()

	content := `service: example
ops:
  SomeOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-01: now honors FilterCriteria."}
items_still_open:
  - "SomeOperation ignores the FilterCriteria field entirely; not implemented."
  - "OtherOperation ignores SortOrder entirely; still open."
---
`
	m := parseManifest("example", "services/example/PARITY.md", content)

	got := checkResolvedElsewhere(m)
	assert.Len(t, got, 1)
	assert.Equal(t, ruleResolvedElsewhere, got[0].Rule)
}

func TestGatingCount_OnlyMissingItemsStillOpenGates(t *testing.T) {
	t.Parallel()

	findings := []finding{
		{Rule: ruleResolvedElsewhere},
		{Rule: ruleUndisclosedOpenItem},
		{Rule: ruleUndisclosedOpenItem},
		{Rule: ruleMissingItemsStillOpen},
	}
	assert.Equal(t, 0, gatingCount(findings[:3]), "advisory-only rules must never gate the build")
	assert.Equal(t, 1, gatingCount(findings))
}
