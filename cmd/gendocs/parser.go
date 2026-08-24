// Package main implements cmd/gendocs, a documentation generator that reads
// each service's services/<svc>/PARITY.md audit manifest and emits a friendly
// services/<svc>/README.md plus a category-grouped service table injected
// into the root README.md.
//
// The frontmatter in PARITY.md is YAML-shaped but not valid YAML: note:
// fields are unquoted free text containing commas, colons, and braces inside
// {...} flow maps, which a real YAML parser chokes on or mis-parses. This
// file is therefore a deliberately tolerant, line-based parser rather than a
// yaml.Unmarshal call — see parser_test.go for the specific shapes it was
// built against. ops:/families: entries are accepted in either YAML form —
// inline flow maps ("OpName: {wire: ok, ...}") or block mappings ("OpName:"
// followed by indented "wire:"/... field lines) — since both are ordinary,
// valid YAML and rejecting one silently undercounts a manifest that is
// otherwise materially correct (gopherstack-7o96).
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
// their families: block (e.g. services/mwaa, services/rekognition). A key
// that collides with a reserved word (e.g. rds's "leaks" family) is still
// tolerated as an entry as long as it's indented -- only a column-0
// occurrence is read as the reserved key's own section header; see
// matchEntry/isBlockTerminator.
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

// blockEntryKeyRe matches the opening line of a YAML block-style ops:/
// families: entry, e.g. "  GetExecutionHistory:" with nothing else on the
// line -- entryLineRe's block-mapping counterpart. Block style is ordinary,
// valid YAML and is what an editor reaches for once a note grows long
// (gopherstack-7o96); same character class as entryLineRe so it doesn't fire
// on wrapped note prose that happens to end a line with a bare colon.
var blockEntryKeyRe = regexp.MustCompile(`^\s*([A-Za-z0-9_][A-Za-z0-9_/() -]*):\s*$`)

// possibleBlockEntryRe is blockEntryKeyRe's deliberately looser superset,
// mirroring possibleEntryRe: it additionally tolerates ',', '*' and '>' so a
// mistyped block-style key is reported instead of silently skipped, while
// still requiring an identifier-ish start so it doesn't fire on ordinary
// wrapped note prose (verified against every services/*/PARITY.md: the only
// real hit is a still-open flow-map's folded note, handled separately by
// isNoteFoldStart/isNoteFoldEnd in parseOpsBlock/parseFamiliesBlock).
var possibleBlockEntryRe = regexp.MustCompile(`^\s*[A-Za-z0-9_][A-Za-z0-9_/(),*>\- ]*:\s*$`)

// blockFieldRe matches one field line inside a block-style entry, e.g.
// "    wire: fixed" or "    note: >". Only names isBlockFieldName recognizes
// are ever extracted; anything else (note and its folded continuation
// lines) is skipped as part of the entry without being parsed.
var blockFieldRe = regexp.MustCompile(`^(\s+)([A-Za-z_][A-Za-z0-9_]*):\s*(.*)$`)

// isBlockFieldName reports whether name is one of the field lines
// consumeBlockEntry extracts from a block-style entry -- the ops:
// vocabulary (wire/errors/state/persist) plus families:' single status.
// note is deliberately excluded: it is free text, read by
// fieldValue/beforeNote for inline entries but never needed by callers for
// block entries.
func isBlockFieldName(name string) bool {
	switch name {
	case "wire", "errors", "state", "persist", "status":
		return true
	default:
		return false
	}
}

// lastAuditCommitRe matches a bare git commit sha, short or full. Empty is
// allowed (schema's "absent field" class -- gopherstack-33in's own fix for
// an unrecoverable stamp); anything else non-empty must be sha-shaped.
var lastAuditCommitRe = regexp.MustCompile(`^[0-9a-f]{7,40}$`)

// checkLastAuditCommitToken appends a Warnings entry when value is non-empty
// and not sha-shaped. gopherstack-33in found 20 manifests carrying prose
// ("pending", "HEAD", "PENDING_COMMIT", "UNKNOWN_SEE_GIT_LOG", ...) in a
// field the re-audit protocol (git diff <last_audit_commit>..HEAD) treats as
// a commit-ish; this is the same hard-failure treatment checkStatusToken
// gives an unrecognized status token, so the defect can't recur silently.
func checkLastAuditCommitToken(doc *ParityDoc, path string, offset, lineIdx int, value string) {
	if value == "" || lastAuditCommitRe.MatchString(value) {
		return
	}

	doc.Warnings = append(doc.Warnings, fmt.Sprintf(
		"%s:%d: last_audit_commit: value %q is not a commit sha",
		path, offset+lineIdx+1, value,
	))
}

// checkStatusToken appends a Warnings entry when value is a non-empty status
// token classifyToken cannot place in its closed vocabulary -- the same
// hard-failure treatment gopherstack-7o96 gave an entry gendocs can't parse
// at all (gopherstack-cr41).
func checkStatusToken(doc *ParityDoc, path string, offset, lineIdx int, entryName, field, value string) {
	if classifyToken(value) != bucketOther {
		return
	}

	doc.Warnings = append(doc.Warnings, fmt.Sprintf(
		"%s:%d: %s: field %q has unrecognized status value %q",
		path, offset+lineIdx+1, entryName, field, value,
	))
}

// checkDuplicateKey appends a Warnings entry the second time a reserved
// top-level key is seen at column 0. A union merge of two branches' PARITY.md
// frontmatter reproduces this shape even when both copies hold individually
// valid values (e.g. two real shas), which checkLastAuditCommitToken/
// checkStatusToken never catch since neither value is malformed on its own
// -- only the second, silently-winning occurrence is the defect
// (gopherstack-z31a). seen is mutated in place, keyed by the field name and
// valued by the 1-based line of its first occurrence.
func checkDuplicateKey(doc *ParityDoc, path string, offset, lineIdx int, key string, seen map[string]int) {
	line := offset + lineIdx + 1

	first, ok := seen[key]
	if !ok {
		seen[key] = line

		return
	}

	doc.Warnings = append(doc.Warnings, fmt.Sprintf(
		"%s:%d: duplicate top-level key %q (first seen at line %d)",
		path, line, key, first,
	))
}

// checkDuplicateEntryKey appends a Warnings entry the second time a key is
// seen inside an ops:/families: block -- checkDuplicateKey's counterpart for
// entries nested one level down, since that guard only ever watched column-0
// keys and so passed silently over two ops: entries for the same operation
// (e.g. a union merge across branches re-describing the same fix under both
// dates; gopherstack-fg0u). seen is mutated in place, keyed by entry name and
// valued by the 1-based line of its first occurrence; callers pass a fresh
// map per block so an ops: entry can share a name with a families: entry
// without tripping this.
func checkDuplicateEntryKey(
	doc *ParityDoc, path string, offset, lineIdx int, blockKey, key string, seen map[string]int,
) {
	line := offset + lineIdx + 1

	first, ok := seen[key]
	if !ok {
		seen[key] = line

		return
	}

	doc.Warnings = append(doc.Warnings, fmt.Sprintf(
		"%s:%d: duplicate %s key %q (first seen at line %d)",
		path, line, blockKey, key, first,
	))
}

// checkOpStatusTokens runs checkStatusToken over every status field of op.
func checkOpStatusTokens(doc *ParityDoc, path string, offset, lineIdx int, op OpStatus) {
	checkStatusToken(doc, path, offset, lineIdx, op.Name, "wire", op.Wire)
	checkStatusToken(doc, path, offset, lineIdx, op.Name, "errors", op.Errors)
	checkStatusToken(doc, path, offset, lineIdx, op.Name, "state", op.State)
	checkStatusToken(doc, path, offset, lineIdx, op.Name, "persist", op.Persist)
}

// checkFamilyStatusToken runs checkStatusToken over f's single status field.
func checkFamilyStatusToken(doc *ParityDoc, path string, offset, lineIdx int, f FamilyStatus) {
	checkStatusToken(doc, path, offset, lineIdx, f.Name, "status", f.Status)
}

// listItemRe matches a gaps:/deferred: list item, e.g. "  - some text".
var listItemRe = regexp.MustCompile(`^\s*-\s+(.*)$`)

// keyLeaks is the reserved top-level "leaks:" key. Named separately (rather
// than inlined in isReservedKey) since it also collides with a real family
// name in services/rds/PARITY.md — see matchEntry (gopherstack-jw5s).
const keyLeaks = "leaks"

// keyLastAuditCommit is the reserved top-level "last_audit_commit:" key,
// named separately so checkLastAuditCommitToken's validation site doesn't
// duplicate the literal (goconst).
const keyLastAuditCommit = "last_audit_commit"

// reservedTopLevelKeys are the only keys the schema defines at column 0.
// Anything else found at column 0 inside an ops:/families: block is treated
// as a (mis-indented) block entry rather than a new section.
func isReservedKey(key string) bool {
	switch key {
	case "service", "sdk_module", keyLastAuditCommit, "last_audit_date",
		"overall", "protocol", "ops", "families", "gaps", "structural_gaps", labelDeferred, keyLeaks:
		return true
	default:
		return false
	}
}

// ParseParityFile reads and tolerantly parses a services/<svc>/PARITY.md
// file. It never returns an error for malformed/odd frontmatter content —
// only for I/O failures — so callers can degrade gracefully on a partially
// understood file. Content it could not confidently parse -- or an entry
// whose status token isn't in classifyToken's closed vocabulary -- is instead
// reported in the returned doc's Warnings, so callers can surface it without
// failing the build here (gopherstack-udc7, gopherstack-cr41).
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
	seenKeyLine := map[string]int{}
	for i < len(lines) {
		m := topLevelKeyRe.FindStringSubmatch(lines[i])
		if m == nil {
			i++

			continue
		}

		key, rest := m[1], m[2]
		if isReservedKey(key) {
			checkDuplicateKey(doc, path, offset, i, key, seenKeyLine)
		}

		if parseScalarField(doc, key, rest) {
			if key == keyLastAuditCommit {
				checkLastAuditCommitToken(doc, path, offset, i, doc.LastAuditCommit)
			}
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
	case keyLastAuditCommit:
		doc.LastAuditCommit = cleanScalar(rest)
	case "last_audit_date":
		doc.LastAuditDate = cleanScalar(rest)
	case "overall":
		doc.Overall = cleanScalar(rest)
	case "protocol":
		doc.Protocol = cleanScalar(rest)
	case keyLeaks:
		doc.LeaksStatus = extractLeaksStatus(rest)
	default:
		return false
	}

	return true
}

// cleanScalar trims a single-line frontmatter scalar value: strips a
// trailing " #..." comment, then surrounding quotes. A value that is only
// whitespace before the comment (e.g. "key:   # note") trims to an empty
// string rather than the comment text -- gopherstack-33in's fix left
// several last_audit_commit fields exactly this shape on purpose.
func cleanScalar(raw string) string {
	v := strings.TrimSpace(raw)
	if strings.HasPrefix(v, "#") {
		return ""
	}
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
// returning its key and the content following the opening brace. A line is
// rejected as an entry only when isBlockTerminator would also claim it (a
// reserved key at column 0) -- e.g. services/rds's "leaks" family entry is
// indented like every other entry and must parse, while a genuine top-level
// "leaks: {status: ..., note: ...}" header always sits at column 0. This
// keeps matchEntry and isBlockTerminator in agreement: no line can fall
// through both as neither an entry nor a terminator (gopherstack-jw5s).
func matchEntry(line string) (string, string, bool) {
	m := entryLineRe.FindStringSubmatch(line)
	if m == nil {
		return "", "", false
	}

	if isBlockTerminator(line) {
		return "", "", false
	}

	return strings.TrimSpace(m[1]), m[2], true
}

// isBlockTerminator reports whether line opens a new reserved top-level
// section, i.e. whether the current ops:/families:/gaps:/deferred: block
// should stop consuming lines here.
func isBlockTerminator(line string) bool {
	m := topLevelKeyRe.FindStringSubmatch(line)

	return m != nil && isReservedKey(m[1])
}

// nextNonBlankLine returns the first non-blank line after lines[i], or
// ok=false if only blank lines remain.
func nextNonBlankLine(lines []string, i int) (string, bool) {
	for j := i + 1; j < len(lines); j++ {
		if strings.TrimSpace(lines[j]) != "" {
			return lines[j], true
		}
	}

	return "", false
}

// matchBlockEntryKey reports whether lines[i] opens a YAML block-style ops:/
// families: entry -- entryLineRe/matchEntry's counterpart for the form
// "  OpName:" followed by indented "wire:"/"errors:"/... field lines instead
// of an inline "{...}". It requires the first non-blank line that follows to
// actually be a recognized field line at a deeper indent, so an unrelated
// bare-colon key sharing the same section -- e.g. iam's/shield's informal
// "invented_ops_removed:"/"deleted_invented_ops:" changelog lists, each
// followed by "- ..." bullets, not field lines -- is never mistaken for an
// entry (gopherstack-7o96). Callers must check isBlockTerminator first,
// exactly as matchEntry does for the inline form, so a reserved key at
// column 0 still reads as a section header rather than an entry.
func matchBlockEntryKey(lines []string, i int) (string, int, bool) {
	m := blockEntryKeyRe.FindStringSubmatch(lines[i])
	if m == nil {
		return "", 0, false
	}

	keyIndent := len(lines[i]) - len(strings.TrimLeft(lines[i], " "))

	next, found := nextNonBlankLine(lines, i)
	if !found {
		return "", 0, false
	}

	fm := blockFieldRe.FindStringSubmatch(next)
	if fm == nil || !isBlockFieldName(fm[2]) || len(fm[1]) <= keyIndent {
		return "", 0, false
	}

	return strings.TrimSpace(m[1]), keyIndent, true
}

// consumeBlockEntry reads the field lines of a block-style entry that opens
// at lines[keyLineIdx] (already confirmed by matchBlockEntryKey, whose indent
// it takes as keyIndent), returning the recognized field values keyed by
// name and the index of the first line after the entry. A field is only
// recognized at the indent of the first non-blank line following the key --
// e.g. "wire:"/"errors:"/... at 4 spaces when the key sits at 2 -- so a
// deeper-indented "note: >" folded continuation is skipped as part of the
// entry rather than misread as more fields. A line at or above the key's own
// indent ends the entry (the next key, or a terminator).
func consumeBlockEntry(lines []string, keyLineIdx, keyIndent int) (map[string]string, int) {
	fields := map[string]string{}
	fieldIndent := -1

	i := keyLineIdx + 1
	for i < len(lines) {
		line := lines[i]
		if strings.TrimSpace(line) == "" {
			i++

			continue
		}

		indent := len(line) - len(strings.TrimLeft(line, " "))
		if indent <= keyIndent {
			break
		}

		if fieldIndent == -1 {
			fieldIndent = indent
		}

		if indent == fieldIndent {
			if fm := blockFieldRe.FindStringSubmatch(line); fm != nil && isBlockFieldName(fm[2]) {
				fields[fm[2]] = cleanScalar(fm[3])
			}
		}
		i++
	}

	return fields, i
}

// isNoteFoldStart reports whether content (an inline entry's text after its
// opening "{") ends with a YAML folded-block-scalar indicator on "note:"
// (e.g. "status: ok, note: >"), signaling that the entry deliberately
// continues onto following physical lines rather than closing on this one.
// Every services/*/PARITY.md use of this shape (services/rdsdata,
// services/redshiftdata) ends the fold with exactly "note: >" -- no "|" or
// "+"/"-" chomping-indicator variant appears in the corpus.
func isNoteFoldStart(content string) bool {
	return strings.HasSuffix(strings.TrimRight(content, " \t"), ">")
}

// isNoteFoldEnd reports whether line closes an isNoteFoldStart entry: its
// last non-whitespace character is the flow map's "}". Checking suffix
// rather than brace balance is deliberate -- a genuinely malformed entry
// missing its closing "}" entirely (e.g. services/sagemaker's
// model_endpoint_config_crud) must not make brace-counting swallow every
// following line to end of file; this entry's own fields were already
// captured from its opening line regardless; only the fold's own closing
// line matters here (gopherstack-7o96).
func isNoteFoldEnd(line string) bool {
	return strings.HasSuffix(strings.TrimRight(line, " \t"), "}")
}

// parseOpsBlock consumes entries of either the inline form
// "  OpName: {wire: ok, errors: ok, state: ok, persist: ok, note: ...}" or
// the equivalent YAML block form ("  OpName:" followed by indented
// "wire:"/"errors:"/... field lines), starting at lines[start]. Returns the
// parsed ops and the index of the first line after the block. pendingFold
// tracks an inline entry's still-open "note: >" fold across wrapped
// physical lines (e.g. services/rdsdata, services/redshiftdata) so those
// continuation lines are never misread as a new block-style entry. A line
// that looks like it was meant to be an entry but matched neither form is
// recorded on doc.Warnings rather than silently folded into the previous
// note (gopherstack-udc7, gopherstack-7o96); so is a parsed entry carrying a
// status token classifyToken doesn't recognize (gopherstack-cr41).
func parseOpsBlock(lines []string, start int, doc *ParityDoc, path string, offset int) ([]OpStatus, int) {
	var ops []OpStatus

	seenKeyLine := map[string]int{}
	pendingFold := false
	i := start
	for i < len(lines) {
		if pendingFold {
			pendingFold = !isNoteFoldEnd(lines[i])
			i++

			continue
		}

		if key, content, ok := matchEntry(lines[i]); ok {
			checkDuplicateEntryKey(doc, path, offset, i, "ops", key, seenKeyLine)
			head := beforeNote(content)
			op := OpStatus{
				Name:    key,
				Wire:    fieldValue(head, "wire"),
				Errors:  fieldValue(head, "errors"),
				State:   fieldValue(head, "state"),
				Persist: fieldValue(head, "persist"),
			}
			checkOpStatusTokens(doc, path, offset, i, op)
			ops = append(ops, op)
			pendingFold = isNoteFoldStart(content)
			i++

			continue
		}

		if isBlockTerminator(lines[i]) {
			break
		}

		if key, keyIndent, ok := matchBlockEntryKey(lines, i); ok {
			var fields map[string]string
			entryLine := i
			checkDuplicateEntryKey(doc, path, offset, entryLine, "ops", key, seenKeyLine)
			fields, i = consumeBlockEntry(lines, i, keyIndent)
			op := OpStatus{
				Name:    key,
				Wire:    fields["wire"],
				Errors:  fields["errors"],
				State:   fields["state"],
				Persist: fields["persist"],
			}
			checkOpStatusTokens(doc, path, offset, entryLine, op)
			ops = append(ops, op)

			continue
		}

		warnUnparsedEntry(doc, path, offset, lines, i)
		i++ // continuation line (wrapped note text) — skip.
	}

	return ops, i
}

// parseFamiliesBlock consumes entries of either the inline form
// "  family_name: {status: ok, note: ...}" or the equivalent block form,
// mirroring parseOpsBlock.
func parseFamiliesBlock(lines []string, start int, doc *ParityDoc, path string, offset int) ([]FamilyStatus, int) {
	var families []FamilyStatus

	seenKeyLine := map[string]int{}
	pendingFold := false
	i := start
	for i < len(lines) {
		if pendingFold {
			pendingFold = !isNoteFoldEnd(lines[i])
			i++

			continue
		}

		if key, content, ok := matchEntry(lines[i]); ok {
			checkDuplicateEntryKey(doc, path, offset, i, "families", key, seenKeyLine)
			head := beforeNote(content)
			f := FamilyStatus{
				Name:   key,
				Status: fieldValue(head, "status"),
			}
			checkFamilyStatusToken(doc, path, offset, i, f)
			families = append(families, f)
			pendingFold = isNoteFoldStart(content)
			i++

			continue
		}

		if isBlockTerminator(lines[i]) {
			break
		}

		if key, keyIndent, ok := matchBlockEntryKey(lines, i); ok {
			var fields map[string]string
			entryLine := i
			checkDuplicateEntryKey(doc, path, offset, entryLine, "families", key, seenKeyLine)
			fields, i = consumeBlockEntry(lines, i, keyIndent)
			f := FamilyStatus{
				Name:   key,
				Status: fields["status"],
			}
			checkFamilyStatusToken(doc, path, offset, entryLine, f)
			families = append(families, f)

			continue
		}

		warnUnparsedEntry(doc, path, offset, lines, i)
		i++
	}

	return families, i
}

// warnUnparsedEntry appends a Warnings entry when lines[i] looks like it was
// meant to open an ops:/families: entry (per possibleEntryRe's inline form or
// possibleBlockEntryRe's block form) but matched neither entryLineRe nor
// blockEntryKeyRe. A bare-colon candidate immediately followed by a "- ..."
// list item is left silent instead: that's an ad-hoc list subsection nested
// in the block (e.g. iam's/shield's "invented_ops_removed:"/
// "deleted_invented_ops:" changelogs), the same recognized shape gaps:/
// deferred: use at the top level, not a malformed entry.
func warnUnparsedEntry(doc *ParityDoc, path string, offset int, lines []string, i int) {
	line := lines[i]
	if !possibleEntryRe.MatchString(line) {
		if !possibleBlockEntryRe.MatchString(line) {
			return
		}

		if next, found := nextNonBlankLine(lines, i); found && listItemRe.MatchString(next) {
			return
		}
	}

	doc.Warnings = append(doc.Warnings, fmt.Sprintf(
		"%s:%d: entry-like line did not parse as an entry: %q",
		path, offset+i+1, strings.TrimSpace(line),
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
