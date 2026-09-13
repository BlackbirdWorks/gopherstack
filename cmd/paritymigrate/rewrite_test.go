package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const rewriteFixtureContent = `service: example
ops:
  SomeOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-01: now honors FilterCriteria."}
gaps:
  - "SomeOperation ignores the FilterCriteria field entirely; not implemented."
  - "OtherOperation ignores SortOrder entirely; still open."
---
`

func TestRewrite_RelocatesWithoutDeletingByDefault(t *testing.T) {
	t.Parallel()

	m := parseManifest("example", "services/example/PARITY.md", rewriteFixtureContent)
	res := planMigration(m)
	require.Len(t, res.blocks, 1)
	require.Len(t, res.blocks[0].dropped(), 1, "the SomeOperation item must classify as resolved elsewhere")

	out := rewrite(m, res, false)

	assert.Contains(t, out, "gaps: []", "the drained gaps: block must become an empty list")
	assert.Contains(t, out, "items_still_open:")
	assert.Contains(t, out, "SomeOperation ignores the FilterCriteria field",
		"relocation-only mode must never delete a resolved-elsewhere candidate")
	assert.Contains(t, out, "OtherOperation ignores SortOrder")
}

func TestRewrite_DropResolvedElsewhereDeletesOnlyThatBucket(t *testing.T) {
	t.Parallel()

	m := parseManifest("example", "services/example/PARITY.md", rewriteFixtureContent)
	res := planMigration(m)

	out := rewrite(m, res, true)

	assert.NotContains(t, out, "SomeOperation ignores the FilterCriteria field",
		"-drop-resolved-elsewhere must delete a resolved-elsewhere candidate")
	assert.Contains(t, out, "OtherOperation ignores SortOrder",
		"a still-open item must always be relocated, never deleted")
}

func TestRewrite_NeverTouchesBodyOrComments(t *testing.T) {
	t.Parallel()

	content := `service: example
gaps:
  # a standalone historical comment
  - "SomeOperation ignores FilterCriteria; not implemented."
---

## Notes
- some body prose that must survive untouched
`
	m := parseManifest("example", "services/example/PARITY.md", content)
	res := planMigration(m)

	out := rewrite(m, res, false)

	assert.Contains(t, out, "# a standalone historical comment")
	assert.Contains(t, out, "some body prose that must survive untouched")
	assert.Equal(t, 1, strings.Count(out, "items_still_open:"))
}
