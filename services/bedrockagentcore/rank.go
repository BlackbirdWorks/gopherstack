package bedrockagentcore

import (
	"math"
	"strings"
	"unicode"
)

const (
	bm25K1  = 1.2
	bm25B   = 0.75
	idfHalf = 0.5
)

func tokenize(s string) []string {
	return strings.FieldsFunc(strings.ToLower(s), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
}

// bm25 scores every document against query with Okapi BM25 over the given collection. The ranking is purely
// lexical: documents sharing no term with the query score 0. There are no embeddings.
func bm25(query string, docs []string) []float64 {
	terms := tokenize(query)
	scores := make([]float64, len(docs))

	if len(terms) == 0 || len(docs) == 0 {
		return scores
	}

	tokens := make([][]string, len(docs))
	total := 0

	for i, d := range docs {
		tokens[i] = tokenize(d)
		total += len(tokens[i])
	}

	avg := float64(total) / float64(len(docs))
	if avg == 0 {
		return scores
	}

	freq := make([]map[string]int, len(docs))
	df := map[string]int{}

	for i, toks := range tokens {
		freq[i] = map[string]int{}

		for _, t := range toks {
			if freq[i][t] == 0 {
				df[t]++
			}

			freq[i][t]++
		}
	}

	n := float64(len(docs))

	for i := range docs {
		for _, t := range terms {
			tf := float64(freq[i][t])
			if tf == 0 {
				continue
			}

			idf := math.Log(1 + (n-float64(df[t])+idfHalf)/(float64(df[t])+idfHalf))
			norm := tf + bm25K1*(1-bm25B+bm25B*float64(len(tokens[i]))/avg)
			scores[i] += idf * tf * (bm25K1 + 1) / norm
		}
	}

	return scores
}
