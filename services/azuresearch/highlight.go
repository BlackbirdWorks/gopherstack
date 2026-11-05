package azuresearch

import (
	"strings"
)

const (
	fragmentSize = 128
	maxFragments = 5
)

// matcher returns a predicate telling whether a document term of field fd matches any positive query clause.
func (pl *searchPlan) matcher() func(fd *fieldDef, term string) bool {
	var leaves []qNode

	collectPositive(pl.query, &leaves)

	return func(fd *fieldDef, term string) bool {
		for _, n := range leaves {
			if leafMatches(pl, n, fd, term) {
				return true
			}
		}

		return false
	}
}

func collectPositive(n qNode, out *[]qNode) {
	b, ok := n.(*qBool)
	if !ok {
		*out = append(*out, n)

		return
	}

	for _, c := range b.clauses {
		if c.occ != occMustNot {
			collectPositive(c.node, out)
		}
	}
}

func leafMatches(pl *searchPlan, n qNode, fd *fieldDef, term string) bool {
	switch t := n.(type) {
	case *qRegex:
		return (t.field == "" || t.field == fd.Path) && t.re.MatchString(term)
	case *qTerm:
		if t.field != "" && t.field != fd.Path {
			return false
		}

		switch {
		case hasWildcard(t.text):
			return wildcardRE(t.text).MatchString(term)
		case t.fuzzy >= 0:
			return editDistance(term, strings.ToLower(t.text), t.fuzzy) <= t.fuzzy
		}

		return pl.termIn(fd, t.text, term)
	case *qPhrase:
		return (t.field == "" || t.field == fd.Path) && pl.termIn(fd, t.text, term)
	}

	return false
}

func (pl *searchPlan) termIn(fd *fieldDef, text, term string) bool {
	for _, alt := range pl.ctx.synonymAlts(fd, pl.ctx.searchTerms(fd, text)) {
		for _, a := range alt {
			if a == term {
				return true
			}
		}
	}

	return false
}

// highlights returns the highlighted fragments of field fd in doc.
func (pl *searchPlan) highlights(d *Doc, fd *fieldDef, pre, post string, match func(*fieldDef, string) bool) []string {
	an := pl.ix.comp.indexAnalyzerFor(fd)

	var out []string

	for _, v := range valuesAt(d.Fields, strings.Split(fd.Path, "/")) {
		s, ok := v.(string)
		if !ok {
			continue
		}

		var spans [][2]int

		for _, t := range an(s) {
			if match(fd, t.Term) {
				spans = append(spans, [2]int{t.Start, t.End})
			}
		}

		out = append(out, fragments(s, spans, pre, post)...)
		if len(out) >= maxFragments {
			return out[:maxFragments]
		}
	}

	return out
}

func fragments(s string, spans [][2]int, pre, post string) []string {
	var out []string

	for i := 0; i < len(spans); {
		start := max(0, spans[i][0]-fragmentSize/4)
		end := min(len(s), start+fragmentSize)
		j := i

		for j < len(spans) && spans[j][1] <= end {
			j++
		}

		if j == i {
			j = i + 1
			end = min(len(s), spans[i][1])
		}

		out = append(out, wrap(s[start:end], spans[i:j], start, pre, post))
		i = j
	}

	return out
}

func wrap(frag string, spans [][2]int, base int, pre, post string) string {
	var sb strings.Builder

	last := 0

	for _, sp := range spans {
		a, b := sp[0]-base, sp[1]-base
		if a < last || b > len(frag) {
			continue
		}

		sb.WriteString(frag[last:a])
		sb.WriteString(pre)
		sb.WriteString(frag[a:b])
		sb.WriteString(post)

		last = b
	}

	sb.WriteString(frag[last:])

	return sb.String()
}
