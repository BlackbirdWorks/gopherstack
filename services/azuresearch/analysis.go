package azuresearch

import (
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Token is one analysed term with its position and byte offsets in the source text.
type Token struct {
	Term  string
	Pos   int
	Start int
	End   int
}

// Analyzer turns text into tokens.
type Analyzer func(text string) []Token

const (
	analyzerStandard = "standard.lucene"
	analyzerKeyword  = "keyword"
	analyzerSimple   = "simple"
	analyzerStop     = "stop"
	analyzerPattern  = "pattern"
	analyzerWhite    = "whitespace"
)

var langAnalyzerRE = regexp.MustCompile( //nolint:gochecknoglobals // compiled once
	`^[a-z]{2,3}(-[a-z]{2,4})?\.(lucene|microsoft)$`)

var stopWords = map[string]map[string]bool{ //nolint:gochecknoglobals // read-only language data
	"en": wordSet("a an and are as at be but by for if in into is it no not of on or such that the their then " +
		"there these they this to was will with"),
	"fr": wordSet("au aux avec ce ces dans de des du elle en et eux il je la le les leur lui ma mais me même " +
		"mes moi mon ne nos notre nous on ou par pas pour qu que qui sa se ses son sur ta te tes toi ton tu un " +
		"une vos votre vous"),
	"de": wordSet("aber alle als also am an auch auf aus bei bin bis da das dass dem den der des die doch du " +
		"ein eine einem einen einer eines er es für hat ich ihr im in ist ja mit nicht noch nur oder sich sie " +
		"sind so und von war was wir zu zum zur"),
	"es": wordSet("a al algo con como cual de del el ella en entre era es esa ese esta este fue ha la las le " +
		"les lo los me mi muy no nos o para pero por que se si sin su sus te un una uno y yo"),
}

func wordSet(s string) map[string]bool {
	out := map[string]bool{}
	for _, w := range strings.Fields(s) {
		out[w] = true
	}

	return out
}

func isWordRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' }

func isLetterRune(r rune) bool { return unicode.IsLetter(r) }

// scan returns the spans of text accepted by keep, joining runs across a joiner rune when both
// neighbours satisfy joinNeighbour.
func scan(text string, keep func(rune) bool, joins func(prev, mid, next rune) bool) []Token {
	var out []Token

	runes := []rune(text)
	offs := make([]int, len(runes)+1)

	off := 0
	for i, r := range runes {
		offs[i] = off
		off += utf8.RuneLen(r)
	}

	offs[len(runes)] = off

	for i := 0; i < len(runes); {
		if !keep(runes[i]) {
			i++

			continue
		}

		j := i + 1
		for j < len(runes) {
			if keep(runes[j]) {
				j++

				continue
			}

			if joins != nil && j+1 < len(runes) && keep(runes[j+1]) && joins(runes[j-1], runes[j], runes[j+1]) {
				j += 2

				continue
			}

			break
		}

		out = append(out, Token{Term: string(runes[i:j]), Start: offs[i], End: offs[j]})
		i = j
	}

	return out
}

func standardJoin(prev, mid, next rune) bool {
	switch mid {
	case '\'', '’', ':':
		return isLetterRune(prev) && isLetterRune(next)
	case '.':
		return (isLetterRune(prev) && isLetterRune(next)) || (unicode.IsDigit(prev) && unicode.IsDigit(next))
	case ',':
		return unicode.IsDigit(prev) && unicode.IsDigit(next)
	}

	return false
}

func renumber(toks []Token) []Token {
	for i := range toks {
		toks[i].Pos = i
	}

	return toks
}

func mapTerms(toks []Token, fn func(string) string) []Token {
	for i := range toks {
		toks[i].Term = fn(toks[i].Term)
	}

	return toks
}

func dropStop(toks []Token, stops map[string]bool) []Token {
	out := toks[:0:0]

	for _, t := range toks {
		if !stops[t.Term] {
			out = append(out, t)
		}
	}

	return out
}

// positions keeps the original positions so phrase queries see gaps left by removed stop words.
func positions(toks []Token) []Token {
	for i := range toks {
		toks[i].Pos = i
	}

	return toks
}

func foldASCII(s string) string {
	d := norm.NFD.String(s)

	var sb strings.Builder

	for _, r := range d {
		if !unicode.Is(unicode.Mn, r) {
			sb.WriteRune(r)
		}
	}

	return sb.String()
}

func standardAnalyzer(stops map[string]bool, fold bool, stem func(string) string) Analyzer {
	return func(text string) []Token {
		toks := positions(mapTerms(scan(text, isWordRune, standardJoin), func(s string) string {
			s = strings.ToLower(s)
			if fold {
				s = foldASCII(s)
			}

			return s
		}))

		toks = dropStop(toks, stops)
		if stem != nil {
			mapTerms(toks, stem)
		}

		return toks
	}
}

func whitespaceAnalyzer(text string) []Token {
	return renumber(scan(text, func(r rune) bool { return !unicode.IsSpace(r) }, nil))
}

func keywordAnalyzer(text string) []Token {
	if text == "" {
		return nil
	}

	return []Token{{Term: text, Start: 0, End: len(text)}}
}

func simpleAnalyzer(stops map[string]bool) Analyzer {
	return func(text string) []Token {
		toks := positions(mapTerms(scan(text, isLetterRune, nil), strings.ToLower))

		return dropStop(toks, stops)
	}
}

func patternAnalyzer(text string) []Token {
	toks := scan(text, isWordRune, nil)

	return renumber(mapTerms(toks, strings.ToLower))
}

// builtinAnalyzer resolves a built-in analyzer name; the second result is false when unknown.
func builtinAnalyzer(name string) (Analyzer, bool) {
	switch strings.ToLower(name) {
	case analyzerStandard, "standardasciifolding.lucene":
		return standardAnalyzer(stopWords["en"], strings.EqualFold(name, "standardasciifolding.lucene"), nil), true
	case analyzerKeyword:
		return keywordAnalyzer, true
	case analyzerSimple:
		return simpleAnalyzer(nil), true
	case analyzerStop:
		return simpleAnalyzer(stopWords["en"]), true
	case analyzerPattern:
		return patternAnalyzer, true
	case analyzerWhite:
		return whitespaceAnalyzer, true
	}

	lower := strings.ToLower(name)
	if !langAnalyzerRE.MatchString(lower) {
		return nil, false
	}

	lang, _, _ := strings.Cut(lower, ".")
	lang, _, _ = strings.Cut(lang, "-")
	stops := stopWords[lang]

	if lang == "en" {
		return standardAnalyzer(stops, false, stemEnglish), true
	}

	return standardAnalyzer(stops, lang == "fr" || lang == "es" || lang == "de", nil), true
}

// stemEnglish is a light English stemmer (plural, -ed, -ing and possessive endings).
func stemEnglish(w string) string {
	w = strings.TrimSuffix(strings.TrimSuffix(w, "'s"), "’s")

	switch {
	case strings.HasSuffix(w, "sses"):
		return strings.TrimSuffix(w, "es")
	case strings.HasSuffix(w, "ies") && len(w) > 4:
		return strings.TrimSuffix(w, "ies") + "y"
	case strings.HasSuffix(w, "ss"), strings.HasSuffix(w, "us"):
		return w
	case strings.HasSuffix(w, "s") && len(w) > 3:
		return strings.TrimSuffix(w, "s")
	}

	for _, suf := range []string{"ing", "ed"} {
		if base, ok := strings.CutSuffix(w, suf); ok && len(base) > 2 && strings.ContainsAny(base, "aeiouy") {
			return undouble(base)
		}
	}

	return w
}

func undouble(w string) string {
	n := len(w)
	if n > 2 && w[n-1] == w[n-2] && !strings.ContainsRune("lsz", rune(w[n-1])) {
		return w[:n-1]
	}

	return w
}
