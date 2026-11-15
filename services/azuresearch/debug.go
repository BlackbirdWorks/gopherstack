package azuresearch

type vecScore struct {
	Search     float64
	Similarity float64
}

// debugInfo is the per-document breakdown returned for debug=vector.
type debugInfo struct {
	Text    *float64
	Vectors []map[string]vecScore
	Boost   float64
}

// wire renders @search.documentDebugInfo (searchindex.json DocumentDebugInfo / QueryResultDocumentSubscores).
func (d *debugInfo) wire() map[string]any {
	sub := map[string]any{"documentBoost": d.Boost}

	if d.Text != nil {
		sub["text"] = map[string]any{"searchScore": *d.Text}
	}

	vecs := make([]any, 0, len(d.Vectors))

	for _, m := range d.Vectors {
		row := map[string]any{}
		for f, v := range m {
			row[f] = map[string]any{"searchScore": v.Search, "vectorSimilarity": v.Similarity}
		}

		vecs = append(vecs, row)
	}

	sub["vectors"] = vecs

	return map[string]any{"vectors": map[string]any{"subscores": sub}}
}

func (pl *searchPlan) attachDebug(hits []hit, lists [][]hit) {
	if pl.req.Debug != debugVector {
		return
	}

	text := map[string]float64{}
	vecs := make([]map[string]map[string]vecScore, len(pl.req.Vectors))
	offset := 0

	if pl.text {
		offset = 1

		for _, h := range lists[0] {
			text[h.key] = h.score
		}
	}

	for i := range pl.req.Vectors {
		vecs[i] = map[string]map[string]vecScore{}
		for _, h := range lists[i+offset] {
			vecs[i][h.key] = h.vec
		}
	}

	for i := range hits {
		h := &hits[i]
		d := &debugInfo{Boost: 1, Vectors: make([]map[string]vecScore, len(vecs))}

		if s, ok := text[h.key]; ok {
			d.Text = &s
		}

		for j := range vecs {
			d.Vectors[j] = vecs[j][h.key]
		}

		if pl.prof != nil {
			d.Boost = pl.prof.functionBoost(h.doc, pl.params, pl.now)
		}

		h.dbg = d
	}
}
