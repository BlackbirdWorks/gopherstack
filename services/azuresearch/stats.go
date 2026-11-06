package azuresearch

import "strings"

const positionGap = 100

type termStats struct {
	df     map[string]map[string]int
	lenSum map[string]int
	docs   map[string]int
	n      int
}

func newTermStats() *termStats {
	return &termStats{df: map[string]map[string]int{}, lenSum: map[string]int{}, docs: map[string]int{}}
}

// valuesAt flattens the values found at a slash-separated path, descending into collections.
func valuesAt(v any, segs []string) []any {
	if len(segs) == 0 {
		if arr, ok := v.([]any); ok {
			return arr
		}

		if v == nil {
			return nil
		}

		return []any{v}
	}

	switch t := v.(type) {
	case map[string]any:
		return valuesAt(t[segs[0]], segs[1:])
	case []any:
		var out []any
		for _, el := range t {
			out = append(out, valuesAt(el, segs)...)
		}

		return out
	}

	return nil
}

func (c *compiled) indexAnalyzerFor(fd *fieldDef) Analyzer {
	for _, n := range []string{fd.IndexAnalyzer, fd.Analyzer} {
		if n != "" {
			if fn, err := (&compiler{comp: c}).analyzer(n); err == nil {
				return fn
			}
		}
	}

	fn, _ := builtinAnalyzer(analyzerStandard)

	return fn
}

func (c *compiled) searchAnalyzerFor(fd *fieldDef) Analyzer {
	for _, n := range []string{fd.SearchAnalyzer, fd.Analyzer} {
		if n != "" {
			if fn, err := (&compiler{comp: c}).analyzer(n); err == nil {
				return fn
			}
		}
	}

	fn, _ := builtinAnalyzer(analyzerStandard)

	return fn
}

func (c *compiled) analyzeDoc(doc *Doc) {
	doc.tok = make(map[string]*fieldTokens, len(c.searchable))

	for _, fd := range c.searchable {
		vals := valuesAt(doc.Fields, strings.Split(fd.Path, "/"))
		if len(vals) == 0 {
			continue
		}

		an := c.indexAnalyzerFor(fd)
		ft := &fieldTokens{terms: map[string][]int{}}
		base := 0

		for _, v := range vals {
			s, ok := v.(string)
			if !ok {
				continue
			}

			toks := an(s)
			for _, t := range toks {
				t.Pos += base
				ft.toks = append(ft.toks, t)
				ft.terms[t.Term] = append(ft.terms[t.Term], t.Pos)
			}

			ft.length += len(toks)
			base += len(toks) + positionGap
		}

		if ft.length > 0 {
			doc.tok[fd.Path] = ft
		}
	}
}

func (st *termStats) add(doc *Doc, sign int) {
	st.n += sign

	for path, ft := range doc.tok {
		st.lenSum[path] += sign * ft.length
		st.docs[path] += sign

		m := st.df[path]
		if m == nil {
			m = map[string]int{}
			st.df[path] = m
		}

		for term := range ft.terms {
			m[term] += sign
			if m[term] <= 0 {
				delete(m, term)
			}
		}
	}
}

// rebuild recompiles the definition and re-analyses every document.
func (ix *Index) rebuild() error {
	comp, err := compileIndex(ix.Def)
	if err != nil {
		return err
	}

	comp.searchable = collectSearchable(comp.fields)
	ix.comp = comp
	ix.stats = newTermStats()

	for _, d := range ix.Docs {
		comp.analyzeDoc(d)
		ix.stats.add(d, 1)
	}

	return nil
}

func collectSearchable(fields []*fieldDef) []*fieldDef {
	var out []*fieldDef

	for _, f := range fields {
		switch {
		case f.Base == typeComplex:
			out = append(out, collectSearchable(f.Children)...)
		case f.Searchable && f.isText():
			out = append(out, f)
		}
	}

	return out
}

func (st *termStats) avgLen(path string) float64 {
	if st.docs[path] == 0 {
		return 1
	}

	return float64(st.lenSum[path]) / float64(st.docs[path])
}
