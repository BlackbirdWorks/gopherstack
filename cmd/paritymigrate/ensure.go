package main

import "strings"

// hasUnparsedInlineContent reports whether b's own key line carries real
// content this tool's block/item parser can't see -- confirmed live,
// services/codeconnections/PARITY.md's "gaps: [\"...\", \"...\"]" JSON-flow-
// style list (two real, undisclosed-to-this-tool items), the one occurrence
// of this shape found in the whole corpus. ensurePresent must never paper
// over a block like this with a false-empty items_still_open: [] -- it
// leaves the service out and reports it as a residual instead.
func hasUnparsedInlineContent(m manifest, b block) bool {
	if b.isInline {
		return false
	}

	rest := m.lines[b.keyLine]
	if idx := strings.Index(rest, ":"); idx >= 0 {
		rest = rest[idx+1:]
	}

	return cleanScalar(rest) != "" && len(b.items) == 0
}

// ensurePresent adds "items_still_open: []" right after the last source
// block (or at the top of frontmatter if none) for every manifest whose
// source blocks are genuinely empty and which has no items_still_open: of
// its own yet -- satisfying the parity-lint rule that the field must always
// be present (gopherstack-anjf option 3(c)). Returns the services touched
// and the services skipped because a source block hides unparsed content
// (see hasUnparsedInlineContent).
func ensurePresent(m manifest) (string, bool, string) {
	if m.dest != nil {
		return "", false, ""
	}

	for _, b := range m.sources {
		if hasUnparsedInlineContent(m, b) {
			return "", false, "unparsed inline content in " + b.field + ": (needs manual migration)"
		}

		if len(b.items) > 0 {
			return "", false, "" // handled by the main migration, not here
		}
	}

	insertAt := m.frontStart
	if len(m.sources) > 0 {
		insertAt = m.sources[len(m.sources)-1].end
	}

	out := make([]string, 0, len(m.lines)+1)
	out = append(out, m.lines[:insertAt]...)
	out = append(out, destField+": []")
	out = append(out, m.lines[insertAt:]...)

	return strings.Join(out, "\n"), true, ""
}
