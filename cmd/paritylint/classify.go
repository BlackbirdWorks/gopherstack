package main

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

// This file's token/marker vocabulary is a deliberate copy of
// cmd/staleclaims/detect.go's -- same corpus, same "is this token currently
// claimed open, and is it marked fixed elsewhere" question, kept as an
// independent copy for the reason cmd/parityfmtcheck's package doc gives: a
// shared reserved-vocabulary list would only drift between the two tools'
// slightly different jobs (staleclaims reports candidates; this tool decides
// what to relocate). See cmd/staleclaims/detect.go for the tuning rationale
// behind windowRadius/commonTokenMax/the stopword list.

const (
	windowRadius   = 120
	minTokenLength = 6
	commonTokenMax = 40
)

var (
	backtickTokenRe = regexp.MustCompile("`([A-Za-z][A-Za-z0-9]{3,})`")
	bareTokenRe     = regexp.MustCompile(`\b[A-Z][a-zA-Z0-9]{4,}\b`)

	positiveMarkerRe = regexp.MustCompile(
		`(?i)\b(fixed|implemented|resolved|closed \d{4}|no longer|now correct|now honors?|now honored|` +
			`now modeled|now real|closes? the gap|now returns?|now populated|now wired|now works?)\b`,
	)
	negatedBeforeRe = regexp.MustCompile(
		`(?i)\b(not|never|cannot|can't|isn't|wasn't|won't|doesn't|didn't|hasn't|haven't|without)\s*$`,
	)

	openClaimMarkerRe = regexp.MustCompile(
		`(?i)\b(not fixed|not yet fixed|never fixed|remains? unfixed|remains? unmodeled|` +
			`still unfixed|still unmodeled|still open|disclosed,?\s*not fixed|not implemented|` +
			`not modeled|always returns? empty|always return empty|deliberately not fixed|` +
			`accept(ed|s)?[\s-]and[\s-]drop(ped|s)?|left (entirely )?untouched|out of scope|` +
			`silently dropped|unmodeled|unimplemented|not honored|not accepted|not populated|` +
			`not supported|no way to|left as an? (disclosed )?gap|genuinely open|no effect)\b`,
	)

	dateOrRoundRe = regexp.MustCompile(`\d{4}-\d{2}-\d{2}|(?i)\b(round|sweep|pass)\s*#?\d+\b`)
)

// isStopWord mirrors cmd/staleclaims/detect.go's isStopWord -- a switch, not
// a map, so this list isn't a package-level global (gochecknoglobals).
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
		"Modelling", "Modeling", "Proven", "Cognito", "Gateway":
		return true
	default:
		return false
	}
}

// verdict is this tool's decision for one item.
type verdict int

const (
	verdictStillOpen verdict = iota
	verdictResolvedElsewhere
	verdictSelfResolved
	verdictAmbiguousPartial
	verdictNoMarkers
)

func (v verdict) String() string {
	switch v {
	case verdictStillOpen:
		return "still-open"
	case verdictResolvedElsewhere:
		return "dropped: resolved elsewhere"
	case verdictSelfResolved:
		return "dropped: self-resolved (positive+dated marker, no open language)"
	case verdictAmbiguousPartial:
		return "ambiguous: some named things resolved, others not -- kept whole"
	case verdictNoMarkers:
		return "ambiguous: no recognized open/fixed phrasing -- kept, hand-check"
	default:
		return "unknown"
	}
}

// classification is one item's verdict plus the evidence for it.
type classification struct {
	fixedText string
	resolved  []tokenFix
	openLeft  []string
	verdict   verdict
}

type tokenFix struct {
	token   string
	excerpt string
	line    int
}

// classify decides it's fate against the rest of m (everything except it's
// own lines).
func classify(m manifest, it item) classification {
	fullText := m.fullText()
	otherText := m.otherText(it.start, it.end)

	tokens := extractTokens(it.text)

	var openTokens []string
	for _, tok := range tokens {
		if isTooCommon(fullText, tok) {
			continue
		}
		if _, isOpen := firstOpenOccurrence(it.text, tok); isOpen {
			openTokens = append(openTokens, tok)
		}
	}

	if len(openTokens) == 0 {
		return classifySelfOnly(it)
	}

	var resolved []tokenFix

	var stillOpen []string

	for _, tok := range openTokens {
		if f, ok := scanForPositiveWithDate(otherText, tok); ok {
			resolved = append(resolved, tokenFix{token: tok, line: f.line, excerpt: f.excerpt})
		} else {
			stillOpen = append(stillOpen, tok)
		}
	}

	switch {
	case len(stillOpen) == 0:
		return classification{verdict: verdictResolvedElsewhere, resolved: resolved}
	case len(resolved) == 0:
		return classification{verdict: verdictStillOpen}
	default:
		return classification{verdict: verdictAmbiguousPartial, resolved: resolved, openLeft: stillOpen}
	}
}

// classifySelfOnly handles an item with no token recognized as "open" by
// this detector's vocabulary: either the item is itself already a
// self-resolved historical note (a positive+dated marker with no open
// language at all -- confirmed live, services/codebuild/PARITY.md's "FIXED
// 2026-09-04 (see ListBuildsForProject above): ... gap is closed." bullet),
// or it's a genuine disclosure phrased outside this detector's vocabulary
// and must be kept for a human to read.
func classifySelfOnly(it item) classification {
	if positiveMarkerRe.MatchString(it.text) && dateOrRoundRe.MatchString(it.text) &&
		!openClaimMarkerRe.MatchString(it.text) {
		return classification{verdict: verdictSelfResolved, fixedText: excerpt(it.text, 0, 0)}
	}

	return classification{verdict: verdictNoMarkers}
}

func scanForPositiveWithDate(text, tok string) (tokenFix, bool) {
	for _, idx := range allIndexes(text, tok) {
		if hasUnnegatedPositive(text, idx, len(tok)) && hasDateNear(text, idx, len(tok)) {
			line := strings.Count(text[:idx], "\n") + 1

			return tokenFix{token: tok, line: line, excerpt: excerpt(text, idx, len(tok))}, true
		}
	}

	return tokenFix{}, false
}

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

func isWordBoundary(text string, i int) bool {
	before := i == 0 || !isIdentByte(text[i-1])
	after := i == len(text) || !isIdentByte(text[i])

	return before || after
}

func isIdentByte(b byte) bool {
	return b == '_' || (b >= '0' && b <= '9') || (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

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
	if isStopWord(strings.ToUpper(tok[:1]) + tok[1:]) {
		return false
	}

	tokenSuffixStopList := [...]string{"Exception", "Error", "Errors"}
	for _, suf := range tokenSuffixStopList {
		if strings.HasSuffix(tok, suf) {
			return false
		}
	}

	return hasLower(tok)
}

// paragraph, headingRe and splitParagraphs mirror cmd/staleclaims/detect.go's
// -- gopherstack-anjf option 3(b) needs the same "body paragraph" unit
// staleclaims already uses for its own prose check.
type paragraph struct {
	text      string
	startLine int // 1-based
	section   int
}

var headingRe = regexp.MustCompile(`^#{2,6}\s`)

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

func hasLower(s string) bool {
	for _, r := range s {
		if unicode.IsLower(r) {
			return true
		}
	}

	return false
}
