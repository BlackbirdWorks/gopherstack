package main

import "strings"

// normalizeWireName collapses an SDK Go field name (PascalCase) and an
// emulator wire/query-param name (usually camelCase, sometimes snake_case)
// to the same key when they name the same thing: lowercase, letters and
// digits only. This is deliberately loose -- it cannot tell "Arn" from a
// sibling field whose wire name happens to also normalize to "arn" if the
// emulator invented an unrelated field with a colliding name, and it cannot
// match a field whose wire spelling diverges from a simple case-fold of the
// SDK name (a semantic rename, an abbreviation expanded or contracted).
// Both are disclosed scope limits in the package doc; nothing in this
// tool's own ground-truth validation needed anything sharper.
func normalizeWireName(s string) string {
	var b strings.Builder

	b.Grow(len(s))

	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r - 'A' + 'a')
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		}
	}

	return b.String()
}

// missingField is one SDK Input field this scan found no matching declared
// emulator field for, on one operation.
type missingField struct {
	Op      string
	Field   sdkField
	Signals []string
	Tier    int
}

// findMissing compares op's SDK-declared fields against the emulator's
// resolved declared field set and returns the ones with no match by
// normalized wire name.
func findMissing(op sdkOp, res opResolution) []missingField {
	var out []missingField

	for _, f := range op.Fields {
		if _, ok := res.Fields[normalizeWireName(f.Name)]; ok {
			continue
		}

		out = append(out, missingField{Op: op.Name, Field: f})
	}

	return out
}
