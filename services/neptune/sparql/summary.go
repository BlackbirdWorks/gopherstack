package sparql

import (
	"sort"
	"strings"
)

// SubjectStructure is a distinct predicate set and how many subjects have it.
type SubjectStructure struct {
	Predicates []string `json:"predicates"`
	Count      int      `json:"count"`
}

// Summary is the Neptune graph summary payload for an RDF graph.
type Summary struct {
	Classes             []string           `json:"classes"`
	Predicates          []map[string]int   `json:"predicates"`
	SubjectStructures   []SubjectStructure `json:"subjectStructures"`
	NumDistinctSubjects int                `json:"numDistinctSubjects"`
	NumDistinctPreds    int                `json:"numDistinctPredicates"`
	NumQuads            int                `json:"numQuads"`
	NumClasses          int                `json:"numClasses"`
}

// Summary lists the store's classes, predicate counts and subject structures for the graph summary endpoint.
func (s *Store) Summary() Summary {
	s.mu.RLock("Summary")
	defer s.mu.RUnlock()

	classes := map[string]struct{}{}
	preds := map[string]int{}
	subjects := map[Term]map[string]struct{}{}

	for q := range s.quads {
		preds[q.P.Value]++

		if subjects[q.S] == nil {
			subjects[q.S] = map[string]struct{}{}
		}

		subjects[q.S][q.P.Value] = struct{}{}

		if q.P.Value == RDFType && q.O.Kind == KindIRI {
			classes[q.O.Value] = struct{}{}
		}
	}

	structs := map[string]*SubjectStructure{}

	for _, ps := range subjects {
		keys := make([]string, 0, len(ps))
		for p := range ps {
			keys = append(keys, p)
		}

		sort.Strings(keys)

		sig := strings.Join(keys, "\x00")
		if structs[sig] == nil {
			structs[sig] = &SubjectStructure{Predicates: keys}
		}

		structs[sig].Count++
	}

	out := Summary{
		NumDistinctSubjects: len(subjects), NumDistinctPreds: len(preds), NumQuads: len(s.quads),
		Classes: make([]string, 0, len(classes)), Predicates: predList(preds),
		SubjectStructures: make([]SubjectStructure, 0, len(structs)),
	}

	for c := range classes {
		out.Classes = append(out.Classes, c)
	}

	sort.Strings(out.Classes)
	out.NumClasses = len(out.Classes)

	for _, v := range structs {
		out.SubjectStructures = append(out.SubjectStructures, *v)
	}

	sort.Slice(out.SubjectStructures, func(i, j int) bool {
		a, b := out.SubjectStructures[i], out.SubjectStructures[j]
		if a.Count != b.Count {
			return a.Count > b.Count
		}

		return strings.Join(a.Predicates, ",") < strings.Join(b.Predicates, ",")
	})

	return out
}

func predList(m map[string]int) []map[string]int {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		if m[keys[i]] != m[keys[j]] {
			return m[keys[i]] > m[keys[j]]
		}

		return keys[i] < keys[j]
	})

	out := make([]map[string]int, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]int{k: m[k]})
	}

	return out
}
