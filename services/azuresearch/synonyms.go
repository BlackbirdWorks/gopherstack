package azuresearch

import (
	"strings"
)

type synRules struct {
	alts map[string][][]string
}

// parseSolr parses a Solr-format synonym map, tokenising entries with the standard analyzer.
func parseSolr(text string) *synRules {
	r := &synRules{alts: map[string][][]string{}}
	an, _ := builtinAnalyzer(analyzerStandard)

	for line := range strings.SplitSeq(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if left, right, ok := strings.Cut(line, "=>"); ok {
			targets := synEntries(right, an)
			for _, l := range synEntries(left, an) {
				r.alts[strings.Join(l, " ")] = targets
			}

			continue
		}

		entries := synEntries(line, an)
		for _, e := range entries {
			r.alts[strings.Join(e, " ")] = entries
		}
	}

	return r
}

func synEntries(s string, an Analyzer) [][]string {
	var out [][]string

	for _, part := range splitUnescaped(s, ',') {
		toks := an(strings.TrimSpace(part))
		if len(toks) == 0 {
			continue
		}

		terms := make([]string, len(toks))
		for i, t := range toks {
			terms[i] = t.Term
		}

		out = append(out, terms)
	}

	return out
}

func splitUnescaped(s string, sep rune) []string {
	var (
		out []string
		sb  strings.Builder
	)

	esc := false

	for _, r := range s {
		switch {
		case esc:
			sb.WriteRune(r)

			esc = false
		case r == '\\':
			esc = true
		case r == sep:
			out = append(out, sb.String())
			sb.Reset()
		default:
			sb.WriteRune(r)
		}
	}

	return append(out, sb.String())
}

// expand returns the alternatives to search for terms; the input itself when no rule applies.
func (r *synRules) expand(terms []string) [][]string {
	if r == nil {
		return [][]string{terms}
	}

	if alts, ok := r.alts[strings.Join(terms, " ")]; ok {
		return alts
	}

	return [][]string{terms}
}
