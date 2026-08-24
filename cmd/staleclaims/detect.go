package main

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// This file implements gopherstack-anjf's detector: a services/<svc>/PARITY.md
// open-list entry (gaps:/items_still_open:/residual_gaps:) or a "not fixed"
// prose paragraph in ## Notes whose named op/field a LATER part of the same
// file records as fixed -- the exact shape that burned codebuild, ecr and
// pipes (see bd show gopherstack-anjf).
//
// Two checks share the same token/marker vocabulary but differ in what
// "later" means, because the two claim shapes order differently:
//
//   - checkStructured: a gaps:/items_still_open:/residual_gaps: entry never
//     carries its own date, so "later" means "the fix reference carries ANY
//     date or round/sweep/pass marker" -- an undated front-matter claim is
//     presumptively older than any dated mention of the same token elsewhere
//     in the file.
//   - checkProse: a ## Notes paragraph is part of an (imperfectly) append-
//     ordered log, so "later" means "a paragraph that appears strictly after
//     this one in the file" -- no date is required, because this is exactly
//     the codebuild shape: two sections dated the SAME calendar day, where
//     only file position tells them apart.
//
// Both checks skip a claim that already resolves itself in place (the same
// text also carries an unnegated fixed-marker for the same token) -- that is
// the "amended with citation" state this detector's own corrections aim for,
// not a defect to keep re-flagging.
//
// PRECISION OVER RECALL, deliberately: a token must be Capitalized (or a
// backtick-quoted identifier), length >= 5, and pass a stoplist of ordinary
// English words -- see candidateStopWords. This misses genuinely-stale
// lowerCamelCase field claims, but a looser filter measured near-100% noise
// in testing against this corpus's prose-heavy manifests.

const (
	// windowRadius is deliberately tight -- roughly one clause/sentence, not
	// a whole paragraph. A wide window measured near-100% false positives in
	// this corpus's prose-dense manifests: two unrelated topics sharing one
	// long paragraph (or two full copies of the same static exception-name
	// enumeration) will always have SOME positive-marker word somewhere in a
	// 500-rune radius, whether or not it has anything to do with the token.
	windowRadius   = 120
	minTokenLength = 6

	// commonTokenMax: a token mentioned more than this many times in the
	// WHOLE manifest is treated as too generic to trust, a backstop for
	// candidateStopWords' generic-field-noun list below (Create/Update/
	// Status/NextToken run into the hundreds on sagemaker's 2600+-line
	// PARITY.md -- no curated stoplist catches every such word, but nothing
	// this corpus names as an actual gap/fix subject comes close to this
	// count even across several audit passes; ecr's heavily-revisited
	// DescribeImages tops out at 18).
	commonTokenMax = 40
)

var (
	backtickTokenRe = regexp.MustCompile("`([A-Za-z][A-Za-z0-9]{3,})`")
	bareTokenRe     = regexp.MustCompile(`\b[A-Z][a-zA-Z0-9]{4,}\b`)

	// positiveMarkerRe: language declaring a claim resolved. Matches are
	// discarded by negatedBeforeRe when immediately preceded by a negation
	// ("not fixed" must never count as a positive marker).
	positiveMarkerRe = regexp.MustCompile(
		`(?i)\b(fixed|implemented|resolved|closed \d{4}|no longer|now correct|now honors?|now honored|` +
			`now modeled|now real|closes? the gap|now returns?|now populated|now wired|now works?)\b`,
	)
	negatedBeforeRe = regexp.MustCompile(
		`(?i)\b(not|never|cannot|can't|isn't|wasn't|won't|doesn't|didn't|hasn't|haven't|without)\s*$`,
	)

	// openClaimMarkerRe: language declaring a SPECIFIC named thing still
	// open/unfixed -- both checks require this within windowRadius of the
	// actual token occurrence (not just "present somewhere in this
	// paragraph/block"), because a long claim entry routinely names several
	// things, only one of which is the thing actually being left open (e.g.
	// an already-fixed op cited in passing as a comparison for a genuinely
	// open one two sentences later -- confirmed live, sagemaker's gaps:
	// entry citing the already-fixed ClusterOrchestrator as an analogy while
	// describing RestrictedInstanceGroups as the real open item).
	openClaimMarkerRe = regexp.MustCompile(
		`(?i)\b(not fixed|not yet fixed|never fixed|remains? unfixed|remains? unmodeled|` +
			`still unfixed|still unmodeled|still open|disclosed,?\s*not fixed|not implemented|` +
			`not modeled|always returns? empty|always return empty|deliberately not fixed|` +
			`accept(ed|s)?[\s-]and[\s-]drop(ped|s)?|left (entirely )?untouched|out of scope|` +
			`silently dropped|unmodeled|unimplemented|not honored|not accepted|not populated|` +
			`not supported|no way to|left as an? (disclosed )?gap|genuinely open|no effect)\b`,
	)

	dateOrRoundRe = regexp.MustCompile(`\d{4}-\d{2}-\d{2}|(?i)\b(round|sweep|pass)\s*#?\d+\b`)

	selfRefRe = regexp.MustCompile(
		`(?i)\b(function|helper|handler|stub|dead code|no longer exists?|was removed|` +
			`was renamed|leftover|vestigial|deleted)\b`,
	)
)

// isStopWord reports whether tok is an ordinary capitalized English word (or
// a generic AWS wire-shape/Go field noun, or a recurring boilerplate/
// product-name mention) common in this corpus's prose that would otherwise
// pass isCandidateToken's shape test. Not exhaustive by design -- see the
// package doc comment's precision note. A switch, not a map, so this list
// isn't a package-level global (gochecknoglobals) -- same shape as
// cmd/gendocs/model.go's classifyToken.
func isStopWord(tok string) bool {
	switch tok {
	case "About", "Above", "Across", "Actually", "Additionally",
		"Against", "Already", "Also", "Always", "Among",
		"Basically", "Because", "Before", "Between", "Cannot",
		"Clearly", "Completely", "Confirmed", "Consistent", "Currently",
		"Deferred", "Deliberately", "Directly", "Disclosed", "Documented",
		"Doesn", "During", "Effectively", "Either", "Entirely",
		"Essentially", "Eventually", "Every", "Existing", "Finally",
		"Firstly", "Fixed", "Found", "Furthermore", "Generally",
		"Genuinely", "Given", "Haven", "However", "Immediately",
		"Implemented", "Indirectly", "Instead", "Isn", "Largely",
		"Literally", "Mainly", "Meanwhile", "Moreover", "Mostly",
		"Naturally", "Necessarily", "Never", "Notes", "Obviously",
		"Originally", "Otherwise", "Overall", "Partially", "Particularly",
		"Practically", "Previously", "Rather", "Reasonably", "Regardless",
		"Should", "Simply", "Since", "Specifically", "Structural",
		"Subsequently", "Technically", "Therefore", "There", "These",
		"Those", "Through", "Toward", "Towards", "Typically",
		"Ultimately", "Unless", "Until", "Usually", "Verified",
		"Wasn", "Where", "Which", "While", "Within", "Without",
		"Would", "Sweep", "Round",
		// Generic AWS wire-shape/Go field & verb nouns: real, but shared
		// across so many unrelated ops in a large manifest that "this token
		// co-occurs with a fixed-marker+date somewhere else in the file"
		// carries almost no information. commonTokenMax is a numeric
		// backstop for the same problem; this list catches words that don't
		// always reach that count but are still too generic to trust
		// (measured live on sagemaker/ec2/ssm).
		"Description", "Descriptions", "Parameter", "Parameters",
		"Property", "Properties", "StartTime", "EndTime",
		"CreationTime", "ModifiedTime", "UpdatedTime", "Timestamp",
		"Timestamps", "Duration", "Priority", "Category",
		"Version", "Versions", "Format", "Encoding",
		"Response", "Responses", "Request", "Requests",
		"Resource", "Resources", "Metadata", "Attribute",
		"Attributes", "Reference", "References", "Identifier",
		"Identifiers", "Location", "Content", "Contents",
		"Detail", "Details", "Summary", "Summaries",
		"Result", "Results", "Output", "Outputs",
		"Input", "Inputs", "Value", "Values",
		"Name", "Names", "Type", "Types",
		"Source", "Sources", "Target", "Targets",
		"Status", "Statuses", "Configuration", "Config",
		"State", "States", "Message", "Messages",
		"Filter", "Filters", "NextToken", "MaxResults",
		// Recurring section-label/boilerplate words and bare product names:
		// used identically as a heading or aside across many DIFFERENT,
		// unrelated paragraphs in a manifest, so "this word co-occurs with a
		// fixed-marker elsewhere" carries no information about any one named
		// op. Measured live: "Modelling"/"Proven" as recurring iot/ecs
		// section labels, "Cognito"/"Gateway" as bare product-family
		// mentions unrelated to any specific op.
		"Modelling", "Modeling", "Proven", "Cognito", "Gateway":
		return true
	default:
		return false
	}
}

// finding is one candidate this detector reports -- a candidate, not a
// verdict; see the package doc comment and cmd's usage banner.
type finding struct {
	Service   string `json:"service"`
	Path      string `json:"path"`
	Check     string `json:"check"` // "structured", "prose", "symbol"
	Field     string `json:"field"`
	Token     string `json:"token"`
	ClaimText string `json:"claimText"`
	FixText   string `json:"fixText,omitempty"`
	Note      string `json:"note,omitempty"`
	ClaimLine int    `json:"claimLine"`
	FixLine   int    `json:"fixLine,omitempty"`
}

// detect runs checkStructured and checkProse always; checkSymbolExistence
// only when includeSymbols is set -- see its own doc comment and main.go's
// -symbols flag for why it's opt-in: measured against this corpus, it is
// dominated by markdown/doc-only backtick tokens (PARITY.md field names like
// `leaks`/`deferred`, linter names like `funlen`) that were never Go
// identifiers to begin with, near-0% true positive rate in manual triage.
func detect(m manifest, svcSrc func(service string) map[string]bool, includeSymbols bool) []finding {
	var out []finding
	out = append(out, checkStructured(m)...)
	out = append(out, checkProse(m)...)

	if includeSymbols {
		out = append(out, checkSymbolExistence(m, svcSrc(m.service))...)
	}

	return out
}

// checkStructured is the gaps:/items_still_open:/residual_gaps: half of the
// detector -- see the package doc comment.
func checkStructured(m manifest) []finding {
	var out []finding

	fullText := m.fullText()

	for _, c := range m.claims {
		if m.blockEmpty(c) {
			continue
		}

		claimText := strings.Join(m.lines[c.start:c.end], "\n")
		for _, tok := range extractTokens(claimText) {
			if isTooCommon(fullText, tok) {
				continue
			}

			openIdx, isOpen := firstOpenOccurrence(claimText, tok)
			if !isOpen {
				continue
			}

			f, ok := findElsewhereFix(m, tok)
			if !ok {
				continue
			}

			f.Service, f.Path, f.Check, f.Field, f.Token = m.service, m.path, "structured", c.field, tok
			f.ClaimLine = c.start + 1 + strings.Count(claimText[:openIdx], "\n")
			f.ClaimText = excerpt(claimText, openIdx, len(tok))
			out = append(out, f)
		}
	}

	return out
}

// findElsewhereFix searches every non-claim region of the file (the rest of
// frontmatter, then the body) for tok co-occurring with an unnegated
// positive marker AND a date/round/sweep/pass marker in the same window.
func findElsewhereFix(m manifest, tok string) (finding, bool) {
	for _, seg := range m.fmOther {
		segText := strings.Join(m.linesIn(seg), "\n")
		if f, ok := scanForPositiveWithDate(segText, tok, seg.start); ok {
			return f, true
		}
	}

	bodyText := strings.Join(m.linesIn(m.body), "\n")

	return scanForPositiveWithDate(bodyText, tok, m.body.start)
}

func scanForPositiveWithDate(text, tok string, lineOffset int) (finding, bool) {
	for _, idx := range allIndexes(text, tok) {
		if hasUnnegatedPositive(text, idx, len(tok)) && hasDateNear(text, idx, len(tok)) {
			line := lineOffset + strings.Count(text[:idx], "\n") + 1

			return finding{FixLine: line, FixText: excerpt(text, idx, len(tok))}, true
		}
	}

	return finding{}, false
}

// checkProse is the "not fixed" paragraph half of the detector -- see the
// package doc comment.
func checkProse(m manifest) []finding {
	paras := splitParagraphs(m.linesIn(m.body), m.body.start)
	hasSections := lastSection(paras) > 0
	fullText := m.fullText()

	var out []finding

	for i, p := range paras {
		if !openClaimMarkerRe.MatchString(p.text) {
			continue
		}

		for _, tok := range extractTokens(p.text) {
			if isTooCommon(fullText, tok) {
				continue
			}

			openIdx, isOpen := firstOpenOccurrence(p.text, tok)
			if !isOpen {
				continue
			}

			f, ok := findLaterProseFix(paras, i, tok, hasSections)
			if !ok {
				continue
			}

			f.Service, f.Path, f.Check, f.Field, f.Token = m.service, m.path, "prose", "notes", tok
			f.ClaimLine = p.startLine
			f.ClaimText = excerpt(p.text, openIdx, len(tok))
			out = append(out, f)
		}
	}

	return out
}

// findLaterProseFix looks for tok, with an unnegated positive marker nearby,
// in a paragraph strictly after claimIdx. When the body has real heading
// sections, the fix paragraph must sit under a DIFFERENT section than the
// claim -- "found this bug ... fixed by doing X" narrated across consecutive
// paragraphs of the SAME dated pass is this corpus's normal problem-then-fix
// shape, not gopherstack-anjf's cross-section staleness (confirmed live,
// workspaces' 2026-08-23 section). When the body has no heading structure at
// all, that guard would suppress every hit, so this falls back to requiring
// the fix be at least 2 paragraphs later -- weaker, but still excludes the
// single riskiest case (the very next paragraph).
func findLaterProseFix(paras []paragraph, claimIdx int, tok string, hasSections bool) (finding, bool) {
	claimSection := paras[claimIdx].section

	for j := claimIdx + 1; j < len(paras); j++ {
		if hasSections {
			if paras[j].section == claimSection {
				continue
			}
		} else if j < claimIdx+2 {
			continue
		}

		p := paras[j]
		for _, idx := range allIndexes(p.text, tok) {
			if hasUnnegatedPositive(p.text, idx, len(tok)) {
				return finding{FixLine: p.startLine, FixText: excerpt(p.text, idx, len(tok))}, true
			}
		}
	}

	return finding{}, false
}

func lastSection(paras []paragraph) int {
	if len(paras) == 0 {
		return 0
	}

	return paras[len(paras)-1].section
}

// checkSymbolExistence is the narrow, disclosed-low-confidence third check:
// a backtick-quoted identifier mentioned near self-referential language
// ("the old X function", "X was removed") that no longer appears ANYWHERE in
// services/<svc>'s own source. Gated tightly on selfRefRe because an
// unimplemented AWS field/op is, definitionally, also absent from the
// source -- without that gate this check would fire on nearly every gap.
func checkSymbolExistence(m manifest, svcSrc map[string]bool) []finding {
	var out []finding

	seen := map[string]bool{}

	scan := func(text string, lineOffset int, field string) {
		for _, idxs := range backtickTokenRe.FindAllStringSubmatchIndex(text, -1) {
			tok := text[idxs[2]:idxs[3]]
			if len(tok) < 4 || seen[tok] {
				continue
			}

			if !selfRefNear(text, idxs[0], idxs[1]) {
				continue
			}

			if svcSrc[tok] {
				continue
			}

			seen[tok] = true
			line := lineOffset + strings.Count(text[:idxs[0]], "\n") + 1
			out = append(out, finding{
				Service: m.service, Path: m.path, Check: "symbol", Field: field, Token: tok,
				ClaimLine: line, ClaimText: excerpt(text, idxs[0], len(tok)),
				Note: "no occurrence of `" + tok + "` found anywhere in services/" + m.service,
			})
		}
	}

	for _, c := range m.claims {
		scan(strings.Join(m.lines[c.start:c.end], "\n"), c.start, c.field)
	}
	scan(strings.Join(m.linesIn(m.body), "\n"), m.body.start, "notes")

	return out
}

// selfRefWindow is wider than windowRadius: a symbol-existence claim's
// self-referential language ("the old X function") is often a clause or two
// away from the backtick-quoted name itself, unlike the tight fixed-marker
// proximity checkStructured/checkProse require.
const selfRefWindow = 200

func selfRefNear(text string, start, end int) bool {
	lo := clampRuneStart(text, start-selfRefWindow)
	hi := clampRuneStart(text, end+selfRefWindow)

	return selfRefRe.MatchString(text[lo:hi])
}

// firstOpenOccurrence returns the first occurrence of tok in text that is
// genuinely being claimed open: an open-claim marker (openClaimMarkerRe)
// nearby, and no unnegated positive marker also nearby (which would mean the
// claim already resolves itself in place -- already amended or pointing
// forward, not the bug shape this detector targets). A token mentioned only
// in passing (no open-claim marker near THIS occurrence) never qualifies,
// even if the same token happens to look "open" elsewhere in the same text.
func firstOpenOccurrence(text, tok string) (int, bool) {
	for _, idx := range allIndexes(text, tok) {
		w := window(text, idx, len(tok))
		if openClaimMarkerRe.MatchString(w) && !hasUnnegatedPositive(text, idx, len(tok)) {
			return idx, true
		}
	}

	return 0, false
}

func hasUnnegatedPositive(text string, aroundIdx, matchLen int) bool {
	w := window(text, aroundIdx, matchLen)
	for _, loc := range positiveMarkerRe.FindAllStringIndex(w, -1) {
		if !negatedBeforeRe.MatchString(w[:loc[0]]) {
			return true
		}
	}

	return false
}

func isTooCommon(fullText, tok string) bool {
	return strings.Count(fullText, tok) > commonTokenMax
}

func hasDateNear(text string, aroundIdx, matchLen int) bool {
	return dateOrRoundRe.MatchString(window(text, aroundIdx, matchLen))
}

func window(text string, aroundIdx, matchLen int) string {
	lo := clampRuneStart(text, aroundIdx-windowRadius)
	hi := clampRuneStart(text, aroundIdx+matchLen+windowRadius)

	return text[lo:hi]
}

// clampRuneStart clamps i into [0, len(s)] and walks back to the nearest
// UTF-8 rune boundary, so a window never slices mid-character.
func clampRuneStart(s string, i int) int {
	if i <= 0 {
		return 0
	}
	if i >= len(s) {
		return len(s)
	}
	for i > 0 && !utf8.RuneStart(s[i]) {
		i--
	}

	return i
}

// allIndexes returns every WHOLE-WORD occurrence of substr in text -- plain
// strings.Index would also match substr as a run inside a longer identifier
// (e.g. "CreateAccessPoint" inside "CreateAccessPointForObjectLambdaResult",
// confirmed live: s3control's PutObjectRetention... no, CreateAccessPoint
// AlreadyExists claim false-matched a completely different op's fix purely
// because one name is a prefix of the other), which is never the same named
// thing this detector is trying to track.
func allIndexes(text, substr string) []int {
	var out []int

	start := 0
	for {
		i := strings.Index(text[start:], substr)
		if i < 0 {
			break
		}

		abs := start + i
		if isWordBoundary(text, abs) && isWordBoundary(text, abs+len(substr)) {
			out = append(out, abs)
		}

		start = abs + len(substr)
	}

	return out
}

// isWordBoundary reports whether byte position i in text sits outside a run
// of identifier characters (letters/digits/underscore) -- i.e. text[i-1] and
// text[i] are not both identifier characters. i may equal 0 or len(text).
func isWordBoundary(text string, i int) bool {
	before := i == 0 || !isIdentByte(text[i-1])
	after := i == len(text) || !isIdentByte(text[i])

	return before || after
}

func isIdentByte(b byte) bool {
	return b == '_' || (b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

// excerpt renders a short, single-line, human-scannable snippet of text
// around idx for a report row.
func excerpt(text string, idx, matchLen int) string {
	const radius = 90

	lo := clampRuneStart(text, idx-radius)
	hi := clampRuneStart(text, idx+matchLen+radius)

	snippet := strings.Join(strings.Fields(text[lo:hi]), " ")
	if lo > 0 {
		snippet = "..." + snippet
	}
	if hi < len(text) {
		snippet += "..."
	}

	return snippet
}

type paragraph struct {
	text      string
	startLine int // 1-based
	section   int // index of the ##/### heading this paragraph falls under
}

// headingRe matches a Markdown heading line, which this corpus uses to mark
// one dated audit pass/section ("### 2026-07-25 pass: Fleet field-diff").
var headingRe = regexp.MustCompile(`^#{2,6}\s`)

// splitParagraphs groups lines (offset by lineOffset in the real file) into
// blank-line-separated paragraphs, each tagged with the index of the most
// recent ##/### heading -- checkProse requires a fix paragraph to sit under
// a DIFFERENT section than its claim, not just later in the file. Within one
// section, "found this bug, fixed by doing X" is the corpus's completely
// normal problem-then-fix narrative shape, not the cross-section staleness
// gopherstack-anjf describes (confirmed live: workspaces' 2026-08-23 section
// narrates a bug then its fix in the same paragraph run -- a real detector
// hit before this guard, and not the bug shape being hunted for).
func splitParagraphs(lines []string, lineOffset int) []paragraph {
	var (
		paras    []paragraph
		cur      []string
		curStart = -1
		section  = 0
	)

	flush := func() {
		if len(cur) == 0 {
			return
		}

		paras = append(paras, paragraph{
			text:      strings.Join(cur, "\n"),
			startLine: lineOffset + curStart + 1,
			section:   section,
		})
		cur = nil
		curStart = -1
	}

	for i, line := range lines {
		if headingRe.MatchString(line) {
			flush()
			section++

			continue
		}

		if strings.TrimSpace(line) == "" {
			flush()

			continue
		}

		if curStart == -1 {
			curStart = i
		}

		cur = append(cur, line)
	}
	flush()

	return paras
}

func extractTokens(text string) []string {
	seen := map[string]bool{}

	var out []string

	add := func(tok string) {
		if !isCandidateToken(tok) || seen[tok] {
			return
		}

		seen[tok] = true
		out = append(out, tok)
	}

	for _, m := range backtickTokenRe.FindAllStringSubmatch(text, -1) {
		add(m[1])
	}
	for _, tok := range bareTokenRe.FindAllString(text, -1) {
		add(tok)
	}

	return out
}

func isCandidateToken(tok string) bool {
	if len(tok) < minTokenLength {
		return false
	}
	// Case-insensitive: a backtick-quoted token can be lowerCamelCase
	// ("output", "note") where the stoplist's Capitalized form otherwise
	// wouldn't match.
	if isStopWord(strings.ToUpper(tok[:1]) + tok[1:]) {
		return false
	}
	// tokenSuffixStopList excludes AWS SDK error-type names (ExceptionType
	// catalogs) and generic Go noun suffixes: these recur verbatim as static
	// enumerations across unrelated paragraphs (an error-mapping table
	// quoted once to declare it, again to justify a fallback path) far more
	// often than they name a single op/field whose fix status changed.
	tokenSuffixStopList := [...]string{"Exception", "Error", "Errors"}
	for _, suf := range tokenSuffixStopList {
		if strings.HasSuffix(tok, suf) {
			return false
		}
	}

	return hasLower(tok)
}

func hasLower(s string) bool {
	for _, r := range s {
		if unicode.IsLower(r) {
			return true
		}
	}

	return false
}
