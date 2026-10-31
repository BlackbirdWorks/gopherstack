package cosmosdb

import (
	"fmt"
	"slices"
)

// queryColl is the container context a query's search functions need: the vector and full-text policies, the
// indexed paths, and the corpus statistics BM25 scoring uses.
type queryColl struct {
	rows          queryRows
	vectors       map[string]vectorEmbedding
	vectorIdx     map[string]string
	ftPaths       map[string]struct{}
	ftLang        map[string]string
	ftIdx         map[string]struct{}
	ftStats       map[string]*bm25Stats
	ftDefaultLang string
	geometry      bool
}

func (b *InMemoryBackend) newQueryColl(c *storedContainer) *queryColl {
	q := &queryColl{
		vectors:   map[string]vectorEmbedding{},
		vectorIdx: map[string]string{},
		ftPaths:   map[string]struct{}{},
		ftLang:    map[string]string{},
		ftIdx:     map[string]struct{}{},
		ftStats:   map[string]*bm25Stats{},
	}

	if vp, err := parseVectorPolicy(c.VectorEmbeddingPolicy); err == nil {
		for _, e := range vp.VectorEmbeddings {
			q.vectors[e.Path] = e
		}
	}

	if fp, err := parseFullTextPolicy(c.FullTextPolicy); err == nil {
		q.ftDefaultLang = fp.DefaultLanguage

		for _, p := range fp.FullTextPaths {
			q.ftPaths[p.Path] = struct{}{}
			q.ftLang[p.Path] = p.Language
		}
	}

	q.geometry = geospatialType(c.GeospatialConfig) == geoTypeGeometry

	pol := parseIndexingPolicy(c.IndexingPolicy)

	for _, vi := range pol.VectorIndexes {
		q.vectorIdx[vi.Path] = vi.Type
	}

	for _, fi := range pol.FullTextIndexes {
		q.ftIdx[fi.Path] = struct{}{}
	}

	return q
}

func (q *queryColl) embedding(path string) (vectorEmbedding, bool) {
	if q == nil {
		return vectorEmbedding{}, false
	}

	e, ok := q.vectors[path]

	return e, ok
}

// vectorDimsOK reports whether a vector of dims fits the policy dimensions of the call's path, when it has any.
func (q *queryColl) vectorDimsOK(n *exprCall, dims int) bool {
	path, ok := vectorPathOf(n)
	if !ok {
		return true
	}

	e, found := q.embedding(path)

	return !found || e.Dimensions == dims
}

func (q *queryColl) hasVectorIndex(path string) bool {
	if q == nil {
		return false
	}

	_, ok := q.vectorIdx[path]

	return ok
}

func (q *queryColl) hasFullTextIndex(path string) bool {
	if q == nil {
		return false
	}

	_, ok := q.ftIdx[path]

	return ok
}

// checkStmt rejects ORDER BY VectorDistance without a vector index (unless bruteForce) and ORDER BY RANK
// FullTextScore without a full-text index.
func (q *queryColl) checkStmt(ec *evalCtx, stmt *sqlSelectStmt) error {
	for _, comp := range rankComponents(stmt) {
		call, ok := comp.(*exprCall)
		if !ok {
			continue
		}

		if err := q.checkOrderCall(ec, call); err != nil {
			return err
		}
	}

	return nil
}

func (q *queryColl) checkOrderCall(ec *evalCtx, call *exprCall) error {
	switch call.name {
	case biVectorDistance:
		path, ok := vectorPathOf(call)
		if ok && !ec.bruteForceSet(call) && !q.hasVectorIndex(path) {
			return &queryIndexError{msg: fmt.Sprintf(
				"ORDER BY VectorDistance on path %s requires a vector index in the indexing policy, or bruteForce set to true.",
				path,
			)}
		}
	case biFullTextScore:
		if path, ok := rootPath(call.args[0]); ok && path != "" && !q.hasFullTextIndex(path) {
			return &queryIndexError{msg: fmt.Sprintf(
				"ORDER BY RANK FullTextScore on path %s requires a full text index in the indexing policy.", path)}
		}
	}

	return nil
}

// rankComponents are the expressions ordered on: the RRF operands, the single RANK function, or the plain ORDER BY
// keys that are search functions.
func rankComponents(stmt *sqlSelectStmt) []sqlExpr {
	if len(stmt.orderBy) == 0 {
		return nil
	}

	if stmt.rank {
		return rrfOperands(stmt.orderBy[0].expr)
	}

	out := make([]sqlExpr, 0, len(stmt.orderBy))

	for _, k := range stmt.orderBy {
		if call, ok := k.expr.(*exprCall); ok && call.name == biVectorDistance {
			out = append(out, call)
		}
	}

	return out
}

// rrfOperands unwraps RRF(a, b, ..., [weights]) to its operands; any other expression is its own single operand.
func rrfOperands(x sqlExpr) []sqlExpr {
	call, ok := x.(*exprCall)
	if !ok || call.name != biRRF {
		return []sqlExpr{x}
	}

	ops := call.args
	if _, weighted := ops[len(ops)-1].(*exprArrayLit); weighted {
		ops = ops[:len(ops)-1]
	}

	return ops
}

// rrfWeights returns the optional trailing weights array of an RRF call, defaulting to 1 each.
func (ec *evalCtx) rrfWeights(x sqlExpr, n int) []float64 {
	w := make([]float64, n)
	for i := range w {
		w[i] = 1
	}

	call, ok := x.(*exprCall)
	if !ok || call.name != biRRF {
		return w
	}

	arr, ok := call.args[len(call.args)-1].(*exprArrayLit)
	if !ok {
		return w
	}

	for i, item := range arr.items {
		if i >= n {
			break
		}

		if num, isNum := toNumber(ec.staticValue(item)); isNum {
			w[i] = numFloat(num)
		}
	}

	return w
}

// validateRankExpr is the parse-time check that ORDER BY RANK names FullTextScore or RRF over scorable operands.
func validateRankExpr(x sqlExpr) error {
	call, ok := x.(*exprCall)
	if !ok || (call.name != biFullTextScore && call.name != biRRF) {
		return fmt.Errorf("%w: ORDER BY RANK requires FullTextScore or RRF", ErrQueryParse)
	}

	if call.name == biFullTextScore {
		return nil
	}

	for _, op := range rrfOperands(call) {
		c, isCall := op.(*exprCall)
		if !isCall || (c.name != biFullTextScore && c.name != biVectorDistance) {
			return fmt.Errorf("%w: RRF operands must be FullTextScore or VectorDistance", ErrQueryParse)
		}
	}

	if weights, isArr := call.args[len(call.args)-1].(*exprArrayLit); isArr &&
		len(weights.items) != len(rrfOperands(call)) {
		return fmt.Errorf("%w: RRF weights must match the number of operands", ErrQueryParse)
	}

	return nil
}

// higherIsBetter reports whether a larger operand score ranks first: similarity and BM25 do, euclidean distance
// does not.
func (ec *evalCtx) higherIsBetter(x sqlExpr) bool {
	call, ok := x.(*exprCall)

	return !ok || call.name != biVectorDistance || ec.vectorMetric(call) != vectorFnEuclidean
}

// effectiveOrder gives ORDER BY VectorDistance(...) its best-first direction whatever ASC/DESC was written.
func (ec *evalCtx) effectiveOrder(keys []sqlOrderKey) []sqlOrderKey {
	out := make([]sqlOrderKey, len(keys))
	copy(out, keys)

	for i, k := range out {
		if call, ok := k.expr.(*exprCall); ok && call.name == biVectorDistance {
			out[i].desc = ec.higherIsBetter(call)
		}
	}

	return out
}

// rankOutputs orders rows by reciprocal rank fusion over the operand scores: each operand ranks the rows
// best-first and a row scores sum(weight / (60 + rank)). Rows an operand cannot score get no share from it.
func (ec *evalCtx) rankOutputs(outs []outRow, stmt *sqlSelectStmt) {
	comps := rankComponents(stmt)
	weights := ec.rrfWeights(stmt.orderBy[0].expr, len(comps))
	fused := make([]float64, len(outs))

	for j, comp := range comps {
		idx := make([]int, 0, len(outs))

		for i := range outs {
			if _, ok := toNumber(outs[i].keys[j]); ok {
				idx = append(idx, i)
			}
		}

		higher := ec.higherIsBetter(comp)

		slices.SortStableFunc(idx, func(a, b int) int {
			c := compareTotal(outs[a].keys[j], outs[b].keys[j])
			if higher {
				return -c
			}

			return c
		})

		for rank, i := range idx {
			fused[i] += weights[j] / float64(rrfK+rank+1)
		}
	}

	order := make([]int, len(outs))
	for i := range order {
		order[i] = i
	}

	slices.SortStableFunc(order, func(a, b int) int {
		switch {
		case fused[a] > fused[b]:
			return -1
		case fused[a] < fused[b]:
			return 1
		default:
			return 0
		}
	})

	sorted := make([]outRow, len(outs))
	for i, o := range order {
		sorted[i] = outs[o]
	}

	copy(outs, sorted)
}
