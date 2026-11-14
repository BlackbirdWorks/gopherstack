package athena

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

const maxFilterSets = 32

const kwInterval = "interval"

func isConstSafeWord(w string) bool {
	switch w {
	case "current_date", "current_timestamp", "localtimestamp", "now", kwInterval, typeDate, typeTimestamp, "as",
		"varchar", "bigint", "integer", typeInt, "double", "true", "false", kwNull:
		return true
	default:
		return false
	}
}

func isConstUnitWord(w string) bool {
	switch w {
	case "year", "month", "day", "hour", "minute", "second", "to":
		return true
	default:
		return false
	}
}

// constEval folds column-free expressions on the right of a partition predicate.
type constEval struct {
	now time.Time
}

// value evaluates toks when they reference no column and returns the result as text.
func (e *constEval) value(toks []token) (string, bool) {
	if e == nil {
		return "", false
	}

	sig := significant(toks)
	if len(sig) == 0 || !e.constant(sig) {
		return "", false
	}

	out := make([]token, 0, len(sig))

	for _, t := range sig {
		switch {
		case t.isWord("current_date"):
			out = append(out, token{kind: tkWord, text: "DATE '" + e.now.Format("2006-01-02") + "'"})
		case t.isWord("current_timestamp"), t.isWord("localtimestamp"):
			out = append(out, token{kind: tkWord, text: "TIMESTAMP '" + e.now.Format("2006-01-02 15:04:05.000") + "'"})
		default:
			out = append(out, t)
		}
	}

	parts := make([]string, len(out))
	for i, t := range out {
		parts[i] = t.text
	}

	return evalConstSQL("SELECT " + strings.Join(parts, " "))
}

func (e *constEval) constant(sig []token) bool {
	hasInterval := false

	for i, t := range sig {
		switch t.kind {
		case tkQuoted:
			return false
		case tkWord:
			w := strings.ToLower(t.text)
			isCall := i+1 < len(sig) && sig[i+1].isPunct("(")

			switch {
			case w == kwSelect || w == "from":
				return false
			case isCall, isConstSafeWord(w):
				hasInterval = hasInterval || w == kwInterval
			case isConstUnitWord(w) && hasInterval:
			default:
				return false
			}
		case tkSpace, tkString, tkNumber, tkPunct:
		}
	}

	return true
}

func evalConstSQL(query string) (string, bool) {
	registerEngineFuncs()

	text, _, err := translateSQL(query, func([]string) resolvedRef { return resolvedRef{} })
	if err != nil {
		return "", false
	}

	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return "", false
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var v sql.NullString
	if err = db.QueryRowContext(ctx, text).Scan(&v); err != nil || !v.Valid {
		return "", false
	}

	return v.String, true
}

func splitDisjuncts(toks []token) [][]token {
	var (
		parts [][]token
		depth int
	)

	cur := make([]token, 0, len(toks))

	for _, t := range toks {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case depth == 0 && t.isWord("or"):
			parts = append(parts, cur)
			cur = nil

			continue
		}

		cur = append(cur, t)
	}

	return append(parts, cur)
}

func (w whereFilters) merge(o whereFilters) whereFilters {
	out := whereFilters{}

	for _, src := range []whereFilters{w, o} {
		for col, f := range src {
			for q := range f.quals {
				for _, p := range f.preds {
					out.add(q, col, p)
				}
			}
		}
	}

	return out
}

// extractWhereSets returns the query's WHERE clauses as alternatives: each element is a conjunction,
// and a partition is needed when any alternative needs it.
func extractWhereSets(query string, ev *constEval) []whereFilters {
	sets := []whereFilters{{}}

	toks, err := tokenize(query)
	if err != nil {
		return sets
	}

	for i, t := range toks {
		if !t.isWord("where") {
			continue
		}

		clause := whereAlternatives(toks[i+1:orderByLikeEnd(toks, i+1)], ev)
		next := make([]whereFilters, 0, len(sets)*len(clause))

		for _, a := range sets {
			for _, c := range clause {
				next = append(next, a.merge(c))
			}
		}

		if sets = next; len(sets) > maxFilterSets {
			return []whereFilters{{}}
		}
	}

	return sets
}

func whereAlternatives(toks []token, ev *constEval) []whereFilters {
	djs := splitDisjuncts(toks)
	out := make([]whereFilters, 0, len(djs))

	for _, dj := range djs {
		f := whereFilters{}

		conj, _ := splitConjuncts(dj)
		for _, c := range conj {
			f.conjunct(c, ev)
		}

		out = append(out, f)
	}

	return out
}
