package cloudwatchlogs

import (
	"regexp"
	"strconv"
	"strings"
)

// getCompiledPattern returns a cached compiled filter pattern, compiling and caching it on first use.
func (b *InMemoryBackend) getCompiledPattern(pattern string) *compiledFilterPattern {
	b.compiledPatternsMu.RLock()
	if c, ok := b.compiledPatterns[pattern]; ok {
		b.compiledPatternsMu.RUnlock()

		return c
	}
	b.compiledPatternsMu.RUnlock()

	c := compileFilterPattern(pattern)

	b.compiledPatternsMu.Lock()
	if len(b.compiledPatterns) < maxCompiledPatternCache {
		b.compiledPatterns[pattern] = c
	}
	b.compiledPatternsMu.Unlock()

	return c
}

// filterMatchesCompiled returns true when the compiled filter pattern matches at least one event.
// A nil compiled pattern matches all events.
func filterMatchesCompiled(compiled *compiledFilterPattern, events []InputLogEvent) bool {
	if compiled == nil {
		return len(events) > 0
	}

	for _, ev := range events {
		if compiled.matches(ev.Message) {
			return true
		}
	}

	return false
}

// compiledFilterPattern holds a parsed and pre-compiled filter pattern for efficient
// repeated matching across many log events (used by FilterLogEvents, subscription
// filters and metric filters).
//
// AWS unstructured (plain-text) filter-pattern semantics (see
// https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/FilterAndPatternSyntax.html):
//
//   - Plain / quoted terms ("required") are AND-ed: every one must be present.
//   - "-term" (exclude) terms must NOT be present.
//   - "?term" (optional) terms are OR-ed: a message matches if it contains ANY of
//     them. AWS documents that when "?" terms are combined with required or exclude
//     terms, the "?" terms are ignored entirely; we honour that rule, so optional
//     terms only take effect when there are no required and no exclude terms.
type compiledFilterPattern struct {
	// custom, when non-nil, fully determines matching (used for JSON "{...}"
	// selector patterns and space-delimited "[...]" metric-filter patterns);
	// the term slices below are then unused.
	custom func(message string) bool
	// extract, when non-nil (JSON and space-delimited patterns only), resolves a
	// named field reference ("size" / ".bytes", i.e. a MetricValue with its
	// leading "$" already stripped) against one message. Plain-text patterns
	// have no addressable fields, so extract stays nil for them.
	extract  func(message, fieldRef string) (string, bool)
	required []compiledTerm // AND: all must match
	optional []compiledTerm // OR: any matches (only used when required+exclude empty)
	exclude  []compiledTerm // NONE may match
}

// extractString resolves a "$"-prefixed MetricValue-style field reference (e.g. "$size",
// "$.bytes") against message, using this pattern's named-field structure. It reports false
// when the pattern has no addressable fields (plain-text patterns), fieldRef isn't
// "$"-prefixed, or the field is absent from this particular message.
func (p *compiledFilterPattern) extractString(message, fieldRef string) (string, bool) {
	if p == nil || p.extract == nil {
		return "", false
	}

	rest, ok := strings.CutPrefix(fieldRef, "$")
	if !ok {
		return "", false
	}

	return p.extract(message, rest)
}

// extractValue is extractString plus a numeric parse, for MetricTransformation.MetricValue
// resolution: CloudWatch metric values must be numeric, so a present-but-non-numeric field
// (or an absent one) both report false, meaning "emit no data point for this event".
func (p *compiledFilterPattern) extractValue(message, fieldRef string) (float64, bool) {
	raw, ok := p.extractString(message, fieldRef)
	if !ok {
		return 0, false
	}

	f, err := strconv.ParseFloat(raw, 64)

	return f, err == nil
}

// compiledTerm holds a single pre-compiled term from a filter pattern.
type compiledTerm struct {
	// exact is the literal substring for quoted/plain terms (non-wildcard).
	// re is used for wildcard terms.
	re      *regexp.Regexp
	exact   string
	isExact bool // true => use exact (strings.Contains); false => use re
}

// match reports whether the message satisfies this single term.
func (ct compiledTerm) match(message string) bool {
	if ct.isExact {
		return strings.Contains(message, ct.exact)
	}

	return ct.re.MatchString(message)
}

// compileTerm compiles a single (prefix-stripped) raw term into a compiledTerm.
// Quoted terms become exact substrings, terms containing "*" become wildcard
// regexes, and everything else is a plain substring.
func compileTerm(t string) compiledTerm {
	var ct compiledTerm

	switch {
	case len(t) >= 2 && t[0] == '"' && t[len(t)-1] == '"':
		ct.isExact = true
		ct.exact = t[1 : len(t)-1]
	case strings.ContainsRune(t, '*'):
		parts := strings.Split(t, "*")
		escaped := make([]string, len(parts))
		for i, p := range parts {
			escaped[i] = regexp.QuoteMeta(p)
		}
		re, err := regexp.Compile(strings.Join(escaped, ".*"))
		if err != nil {
			// The wildcard expansion produced an invalid regex (this should not
			// happen in practice because QuoteMeta escapes all special chars).
			// Fall back to treating the raw term as a plain substring so the
			// caller still receives a deterministic (if approximate) result.
			ct.isExact = true
			ct.exact = t
		} else {
			ct.re = re
		}
	default:
		ct.isExact = true
		ct.exact = t
	}

	return ct
}

// compileFilterPattern parses pattern into a compiledFilterPattern for efficient reuse.
// An empty pattern always matches all messages.
func compileFilterPattern(pattern string) *compiledFilterPattern {
	if trimmed := strings.TrimSpace(pattern); trimmed != "" {
		switch trimmed[0] {
		case '{':
			return &compiledFilterPattern{
				custom:  compileJSONFilterPattern(trimmed),
				extract: compileJSONFilterPatternExtract(trimmed),
			}
		case '[':
			return &compiledFilterPattern{
				custom:  compileSpaceFilterPattern(trimmed),
				extract: compileSpaceFilterPatternExtract(trimmed),
			}
		}
	}

	rawTerms := parseFilterPatternTerms(pattern)
	cp := &compiledFilterPattern{}

	for _, raw := range rawTerms {
		switch {
		case strings.HasPrefix(raw, "?") && len(raw) > 1:
			cp.optional = append(cp.optional, compileTerm(raw[1:]))
		case strings.HasPrefix(raw, "-") && len(raw) > 1:
			cp.exclude = append(cp.exclude, compileTerm(raw[1:]))
		default:
			cp.required = append(cp.required, compileTerm(raw))
		}
	}

	return cp
}

// patternFieldRefs returns every "$"-prefixed field reference addressable in pattern
// (e.g. ["$.eventType"] for a JSON selector pattern, ["$ip", "$size"] for a
// space-delimited pattern with named fields). Plain-text patterns have no addressable
// fields and return nil. Used by TestMetricFilter to populate ExtractedValues.
func patternFieldRefs(pattern string) []string {
	trimmed := strings.TrimSpace(pattern)
	if trimmed == "" {
		return nil
	}

	switch trimmed[0] {
	case '{':
		return jsonPatternSelectors(trimmed)
	case '[':
		return spacePatternFieldNames(trimmed)
	default:
		return nil
	}
}

// matches reports whether the message satisfies the pattern, following AWS
// unstructured filter-pattern semantics.
func (p *compiledFilterPattern) matches(message string) bool {
	if p.custom != nil {
		return p.custom(message)
	}

	// Exclude terms: the message must not contain any of them.
	for _, ct := range p.exclude {
		if ct.match(message) {
			return false
		}
	}

	// Required terms: all must be present (AND).
	for _, ct := range p.required {
		if !ct.match(message) {
			return false
		}
	}

	// Optional ("?") terms only take effect when there are no required and no
	// exclude terms; AWS ignores "?" terms when combined with other terms.
	if len(p.optional) > 0 && len(p.required) == 0 && len(p.exclude) == 0 {
		for _, ct := range p.optional {
			if ct.match(message) {
				return true
			}
		}

		return false
	}

	return true
}

// filterPatternMatches returns true when the CloudWatch Logs filter pattern matches the message.
//
// Pattern syntax (AWS unstructured / plain-text):
//   - Empty pattern matches all messages.
//   - Space-separated plain or quoted terms are AND-ed: all must be present.
//   - A term prefixed with "?" is optional (OR): the message matches if it
//     contains any "?" term. "?" terms are ignored when combined with plain or
//     "-" terms (matching AWS behaviour).
//   - A term prefixed with "-" must NOT appear in the message.
//   - Quoted terms ("...") require an exact substring match.
//   - "*" inside a term is a wildcard.
func filterPatternMatches(pattern, message string) bool {
	return compileFilterPattern(pattern).matches(message)
}

// parseFilterPatternTerms splits a filter pattern into individual terms,
// respecting double-quoted phrases.
func parseFilterPatternTerms(pattern string) []string {
	var terms []string
	var cur strings.Builder
	inQuote := false

	for i := range len(pattern) {
		ch := pattern[i]

		switch {
		case ch == '"':
			inQuote = !inQuote
			cur.WriteByte(ch)
		case ch == ' ' && !inQuote:
			if cur.Len() > 0 {
				terms = append(terms, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteByte(ch)
		}
	}

	if cur.Len() > 0 {
		terms = append(terms, cur.String())
	}

	return terms
}
