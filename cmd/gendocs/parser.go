// Package main implements cmd/gendocs, a documentation generator that reads
// each service's services/<svc>/PARITY.md audit manifest and emits a friendly
// services/<svc>/README.md plus a category-grouped service table injected
// into the root README.md.
//
// The frontmatter in PARITY.md is YAML-shaped but not valid YAML: note:
// fields are unquoted free text containing commas, colons, and braces inside
// {...} flow maps, which a real YAML parser chokes on or mis-parses. This
// file is therefore a deliberately tolerant, line-based parser rather than a
// yaml.Unmarshal call — see the package doc above and parser_test.go for the
// specific shapes it was built against.
package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

// topLevelKeyRe matches a frontmatter key at column 0, e.g. "overall: A".
// Anchored with no leading \s*, so indented lines never match it.
var topLevelKeyRe = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*):(.*)$`)

// entryLineRe matches the start of an ops:/families: block entry, e.g.
// "  CreateAgent: {wire: ok, ...". Indent is tolerated at any depth because
// a handful of PARITY.md files have a mis-indented (0-space) final entry in
// their families: block (e.g. services/mwaa, services/rekognition).
//
// The key class is wider than a Go identifier: real PARITY.md family keys
// name multiple operations or add a parenthetical, e.g.
// "AddPermission/RemovePermission", "Database/TableMetadata (Get/List)",
// "Create/UpdateConfigurationTemplate response shape" (services/athena,
// services/sqs, services/elasticbeanstalk). '/', '()', '-' and space are
// therefore accepted in the key; anything else (',', '*', quotes, ...) is
// deliberately excluded to keep this from matching wrapped note prose that
// coincidentally contains ": {" (gopherstack-udc7).
var entryLineRe = regexp.MustCompile(`^\s*([A-Za-z0-9_][A-Za-z0-9_/() -]*):\s*\{(.*)$`)

// possibleEntryRe is a deliberately looser superset of entryLineRe, used
// only to detect a line that looks like it was meant to be a block entry
// but didn't match entryLineRe -- so the skip can be reported instead of
// silently dropped (gopherstack-udc7). It additionally tolerates ',', '*'
// and '>' (seen in real keys like "BatchDetect* (... 6 families, NOT
// PiiEntities)" and "ec2-provisioning (ASG->EC2 ...)" in services/comprehend
// and services/autoscaling) but still requires an identifier-ish start, so
// it does not fire on quoted/backtick-code note prose or "- " list items.
var possibleEntryRe = regexp.MustCompile(`^\s*[A-Za-z0-9_][A-Za-z0-9_/(),*>\- ]*:\s*\{`)

// listItemRe matches a gaps:/deferred: list item, e.g. "  - some text".
var listItemRe = regexp.MustCompile(`^\s*-\s+(.*)$`)

// reservedTopLevelKeys are the only keys the schema defines at column 0.
// Anything else found at column 0 inside an ops:/families: block is treated
// as a (mis-indented) block entry rather than a new section.
func isReservedKey(key string) bool {
	switch key {
	case "service", "sdk_module", "last_audit_commit", "last_audit_date",
		"overall", "protocol", "ops", "families", "gaps", "structural_gaps", labelDeferred, "leaks":
		return true
	default:
		return false
	}
}

// ParseParityFile reads and tolerantly parses a services/<svc>/PARITY.md
// file. It never returns an error for malformed/odd frontmatter content —
// only for I/O failures — so callers can degrade gracefully on a partially
// understood file. Content it could not confidently parse is instead
// reported in the returned doc's Warnings, so callers can surface it without
// failing the build (gopherstack-udc7).
func ParseParityFile(path string) (*ParityDoc, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	lines := strings.Split(string(data), "\n")
	frontmatter, offset := extractFrontmatter(lines)
	doc := &ParityDoc{}
	parseFrontmatter(frontmatter, doc, path, offset)

	return doc, nil
}

// extractFrontmatter returns the frontmatter lines of a PARITY.md file,
// along with the 0-indexed line number (in the original file) of the first
// returned line, so callers can translate a frontmatter-relative line index
// back into a real file:line for diagnostics.
// The schema template opens with "---" and closes with a second "---", but a
// number of real services/*/PARITY.md files in the corpus drop the opening
// "---" (and sometimes the closing one too), starting directly with
// "service: <name>". To stay tolerant of that drift, this scans from just
// after an opening "---" if present (line 0 otherwise) up to whichever comes
// first: a "---" line, a "## " Markdown heading (the body's own section,
// e.g. "## Notes", for files that dropped the closing "---" but still have a
// body), or end of file (for files that are frontmatter top to bottom).
func extractFrontmatter(lines []string) ([]string, int) {
	if len(lines) == 0 {
		return nil, 0
	}

	start := 0
	if strings.TrimSpace(lines[0]) == "---" {
		start = 1
	}

	for i := start; i < len(lines); i++ {
		t := strings.TrimSpace(lines[i])
		if t == "---" || strings.HasPrefix(t, "## ") {
			return lines[start:i], start
		}
	}

	return lines[start:], start
}

// parseFrontmatter walks the frontmatter lines as a small state machine:
// scalar keys consume one line, ops:/families:/gaps:/structural_gaps:/deferred:
// consume a block of subsequent lines, and anything unrecognized is skipped
// until the next reserved key resumes normal parsing. path and offset are
// used only to attribute Warnings to a real file:line.
func parseFrontmatter(lines []string, doc *ParityDoc, path string, offset int) {
	i := 0
	for i < len(lines) {
		m := topLevelKeyRe.FindStringSubmatch(lines[i])
		if m == nil {
			i++

			continue
		}

		key, rest := m[1], m[2]
		if parseScalarField(doc, key, rest) {
			i++

			continue
		}

		switch key {
		case "ops":
			doc.Ops, i = parseOpsBlock(lines, i+1, doc, path, offset)
		case "families":
			doc.Families, i = parseFamiliesBlock(lines, i+1, doc, path, offset)
		case "gaps":
			doc.Gaps, i = parseListBlock(lines, i, rest)
		case "structural_gaps":
			doc.StructuralGaps, i = parseListBlock(lines, i, rest)
		case "deferred":
			doc.Deferred, i = parseListBlock(lines, i, rest)
		default:
			i = skipUnknownBlock(lines, i+1)
		}
	}
}

// parseScalarField handles the single-line scalar keys (everything except
// ops:/families:/gaps:/structural_gaps:/deferred:, which consume a block of
// following lines). Returns false for any other key, leaving it to the
// caller's block-consuming switch.
func parseScalarField(doc *ParityDoc, key, rest string) bool {
	switch key {
	case "service":
		doc.Service = cleanScalar(rest)
	case "sdk_module":
		doc.SDKModule = cleanScalar(rest)
	case "last_audit_commit":
		doc.LastAuditCommit = cleanScalar(rest)
	case "last_audit_date":
		doc.LastAuditDate = cleanScalar(rest)
	case "overall":
		doc.Overall = cleanScalar(rest)
	case "protocol":
		doc.Protocol = cleanScalar(rest)
	case "leaks":
		doc.LeaksStatus = extractLeaksStatus(rest)
	default:
		return false
	}

	return true
}

// cleanScalar trims a single-line frontmatter scalar value: strips a
// trailing " #..." comment, then surrounding quotes.
func cleanScalar(raw string) string {
	v := strings.TrimSpace(raw)
	if idx := strings.Index(v, " #"); idx >= 0 {
		v = strings.TrimSpace(v[:idx])
	}

	return strings.Trim(v, `"'`)
}

// fieldValue extracts the value of "<key>: <value>" from head, stopping at
// the next ',' or '}' (whichever comes first). Returns "" if key isn't
// present. head must already have any trailing note: text truncated off, so
// this never matches a key name that only appears inside free-text prose.
func fieldValue(head, key string) string {
	_, rest, found := strings.Cut(head, key+":")
	if !found {
		return ""
	}

	end := len(rest)
	if i := strings.IndexAny(rest, ",}"); i >= 0 {
		end = i
	}

	return strings.TrimSpace(rest[:end])
}

// beforeNote truncates content at its first "note:" marker, if any. Status
// fields (wire/errors/state/persist/status) always precede note: in the
// schema, so this keeps fieldValue from matching a coincidental "key:" that
// shows up inside the note's free-text prose (observed in the wild, e.g.
// services/codeartifact/PARITY.md's "state: does not apply..." note text).
func beforeNote(content string) string {
	head, _, found := strings.Cut(content, "note:")
	if found {
		return head
	}

	return content
}

// extractLeaksStatus pulls the status token out of a single-line
// `leaks: {status: clean, note: ...}` frontmatter value.
func extractLeaksStatus(rest string) string {
	content := rest
	if idx := strings.Index(content, "{"); idx >= 0 {
		content = content[idx+1:]
	}

	return fieldValue(beforeNote(content), "status")
}

// matchEntry reports whether line starts an ops:/families: block entry,
// returning its key and the content following the opening brace. Lines
// naming a reserved top-level key are never treated as entries even if they
// happen to match the brace syntax (defensive; shouldn't occur in practice).
func matchEntry(line string) (string, string, bool) {
	m := entryLineRe.FindStringSubmatch(line)
	if m == nil {
		return "", "", false
	}

	key := strings.TrimSpace(m[1])
	if isReservedKey(key) {
		return "", "", false
	}

	return key, m[2], true
}

// isBlockTerminator reports whether line opens a new reserved top-level
// section, i.e. whether the current ops:/families:/gaps:/deferred: block
// should stop consuming lines here.
func isBlockTerminator(line string) bool {
	m := topLevelKeyRe.FindStringSubmatch(line)

	return m != nil && isReservedKey(m[1])
}

// parseOpsBlock consumes entries of the form
// "  OpName: {wire: ok, errors: ok, state: ok, persist: ok, note: ...}"
// starting at lines[start], including any wrapped continuation lines that
// belong to a long note. Returns the parsed ops and the index of the first
// line after the block. A line that looks like it was meant to be an entry
// but didn't match entryLineRe is recorded on doc.Warnings rather than
// silently folded into the previous note (gopherstack-udc7).
func parseOpsBlock(lines []string, start int, doc *ParityDoc, path string, offset int) ([]OpStatus, int) {
	var ops []OpStatus

	i := start
	for i < len(lines) {
		key, content, ok := matchEntry(lines[i])
		if ok {
			head := beforeNote(content)
			ops = append(ops, OpStatus{
				Name:    key,
				Wire:    fieldValue(head, "wire"),
				Errors:  fieldValue(head, "errors"),
				State:   fieldValue(head, "state"),
				Persist: fieldValue(head, "persist"),
			})
			i++

			continue
		}

		if isBlockTerminator(lines[i]) {
			break
		}

		warnUnparsedEntry(doc, path, offset, i, lines[i])
		i++ // continuation line (wrapped note text) — skip.
	}

	return ops, i
}

// parseFamiliesBlock consumes entries of the form
// "  family_name: {status: ok, note: ...}", mirroring parseOpsBlock.
func parseFamiliesBlock(lines []string, start int, doc *ParityDoc, path string, offset int) ([]FamilyStatus, int) {
	var families []FamilyStatus

	i := start
	for i < len(lines) {
		key, content, ok := matchEntry(lines[i])
		if ok {
			head := beforeNote(content)
			families = append(families, FamilyStatus{
				Name:   key,
				Status: fieldValue(head, "status"),
			})
			i++

			continue
		}

		if isBlockTerminator(lines[i]) {
			break
		}

		warnUnparsedEntry(doc, path, offset, i, lines[i])
		i++
	}

	return families, i
}

// warnUnparsedEntry appends a Warnings entry when line looks like it was
// meant to open an ops:/families: block entry (per possibleEntryRe) but
// entryLineRe rejected it -- otherwise the line is ordinary wrapped note
// prose and stays silent.
func warnUnparsedEntry(doc *ParityDoc, path string, offset, lineIdx int, line string) {
	if !possibleEntryRe.MatchString(line) {
		return
	}

	doc.Warnings = append(doc.Warnings, fmt.Sprintf(
		"%s:%d: entry-like line did not parse as a block entry: %q",
		path, offset+lineIdx+1, strings.TrimSpace(line),
	))
}

// parseListBlock consumes a gaps:/deferred: block: either an inline "[]" on
// the key line, or a following run of "  - item" lines. A wrapped
// continuation line (indented, not itself a new list item or a top-level
// key) is folded onto the previous item.
func parseListBlock(lines []string, keyLineIdx int, keyLineRest string) ([]string, int) {
	if cleanScalar(keyLineRest) == "[]" {
		return nil, keyLineIdx + 1
	}

	var items []string

	i := keyLineIdx + 1
	for i < len(lines) {
		line := lines[i]

		if m := listItemRe.FindStringSubmatch(line); m != nil {
			items = append(items, cleanListItem(m[1]))
			i++

			continue
		}

		if isBlockTerminator(line) {
			break
		}

		if strings.TrimSpace(line) != "" && len(items) > 0 {
			items[len(items)-1] += " " + strings.TrimSpace(line)
		}
		i++
	}

	return items, i
}

// cleanListItem strips the surrounding quotes some gaps:/deferred: items use
// (needed because their free text often contains commas/colons that would
// otherwise look like YAML structure), unescaping any \" the source used to
// embed a literal quote inside that wrapper.
func cleanListItem(raw string) string {
	v := strings.TrimSpace(raw)
	if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
		v = strings.ReplaceAll(v[1:len(v)-1], `\"`, `"`)
	}

	return v
}

// skipUnknownBlock advances past a top-level key this parser doesn't know
// about, up to (not including) the next reserved key. The known schema
// covers every key seen in services/*/PARITY.md, so this is purely a
// forward-compatibility guard against panicking on a future field.
func skipUnknownBlock(lines []string, start int) int {
	i := start
	for i < len(lines) && !isBlockTerminator(lines[i]) {
		i++
	}

	return i
}
