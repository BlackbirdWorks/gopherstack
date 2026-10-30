package cosmosdb

import (
	"slices"
	"strings"
	"unicode"

	"github.com/blevesearch/snowballstem"
	"github.com/blevesearch/snowballstem/english"
	"github.com/blevesearch/snowballstem/french"
	"github.com/blevesearch/snowballstem/german"
	"github.com/blevesearch/snowballstem/italian"
	"github.com/blevesearch/snowballstem/portuguese"
	"github.com/blevesearch/snowballstem/spanish"
)

// Full-text analysis (Learn, "Use full-text search"): text is tokenized, English stop words are removed and words
// are stemmed. Multi-language support (preview) stems de-DE, es-ES, fr-FR, it-IT and pt-PT/pt-BR with their Snowball
// stemmers; stop-word removal is English only, as that page says. The stop list is the common English list the
// pages do not publish. A language outside that set is tokenized and lower-cased only.
const defaultFullTextLanguage = "en-US"

func stemmerFor(lang string) func(*snowballstem.Env) bool {
	switch strings.ToLower(lang) {
	case "en-us", "en-gb", "en":
		return english.Stem
	case "de-de":
		return german.Stem
	case "es-es":
		return spanish.Stem
	case "fr-fr":
		return french.Stem
	case "it-it":
		return italian.Stem
	case "pt-pt", "pt-br":
		return portuguese.Stem
	}

	return nil
}

func isEnglishStopWord(w string) bool {
	return slices.Contains([]string{
		"a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not",
		"of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", "this", "to", "was",
		"will", "with",
	}, w)
}

// ftAnalyze splits s into lower-cased words, drops English stop words and stems what is left.
func ftAnalyze(lang, s string) []string {
	words := strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})

	stem := stemmerFor(lang)
	english := strings.HasPrefix(strings.ToLower(lang), "en")
	out := make([]string, 0, len(words))

	for _, w := range words {
		if english && isEnglishStopWord(w) {
			continue
		}

		if stem != nil {
			env := snowballstem.NewEnv(w)
			stem(env)
			w = env.Current()
		}

		out = append(out, w)
	}

	return out
}

// editDistance is the Levenshtein distance between a and b.
func editDistance(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)

	for j := range prev {
		prev[j] = j
	}

	for i := 1; i <= len(ra); i++ {
		cur := make([]int, len(rb)+1)
		cur[0] = i

		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}

			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}

		prev = cur
	}

	return prev[len(rb)]
}
