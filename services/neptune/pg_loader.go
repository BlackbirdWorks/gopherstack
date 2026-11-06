package neptune

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

const (
	errCodeParsing    = "PARSING_ERROR"
	errCodeMissingEnd = "FROM_OR_TO_VERTEX_ARE_MISSING"
	errCodeMultiEdge  = "ID_ASSIGNED_TO_MULTIPLE_EDGES"
	errCodeSingleCard = "SINGLE_CARDINALITY_VIOLATION"
	errCodeInternal   = "INTERNAL_ERROR"
	defaultVertexLbl  = "vertex"
	defaultEdgeLbl    = "edge"
	pgCSV             = "csv"
	pgOpenCypher      = "opencypher"
	firstDataRecord   = 2
)

var errNoGraph = errors.New("the property graph is not available for this cluster")

type pgErrKind int

const (
	pgErrParse pgErrKind = iota
	pgErrDatatype
	pgErrInsert
)

// pgLoader applies one property-graph CSV feed inside a graph update.
type pgLoader struct {
	st     *opencypher.Store
	job    *loadJob
	res    *feedResult
	hdr    *pgHeader
	uri    string
	cypher bool
	stop   bool
}

func (l *pgLoader) fail(kind pgErrKind, code, msg string, rec int) {
	switch kind {
	case pgErrParse:
		l.res.parsing++
	case pgErrDatatype:
		l.res.datatype++
	case pgErrInsert:
		l.res.insert++
	}

	l.res.errs = append(l.res.errs, loadError{Code: code, Message: msg, File: l.uri, Record: rec})

	if l.job.failOnErr {
		l.stop = true
	}
}

// pgFeedIsEdge reports whether a CSV feed holds edges (header names from/to columns); ok is false when the
// header cannot be read.
func pgFeedIsEdge(data []byte, cypher bool) (bool, bool) {
	recs, _ := parseCSV(data)
	if len(recs) == 0 {
		return false, false
	}

	for _, f := range recs[0] {
		switch strings.TrimSpace(f.val) {
		case "~from", "~to":
			return !cypher, true
		}

		if cypher {
			if i := lastUnescapedColon(f.val); i >= 0 {
				if r := f.val[i+1:]; strings.HasPrefix(r, "START_ID") || strings.HasPrefix(r, "END_ID") {
					return true, true
				}
			}
		}
	}

	return false, true
}

// pgOrder prefetches every feed and orders vertex feeds before edge feeds unless edgeOnlyLoad is set.
func (rs *rdfService) pgOrder(ctx context.Context, job *loadJob) ([]int, map[string][]byte) {
	order := make([]int, len(job.keys))
	for i := range order {
		order[i] = i
	}

	cache := make(map[string][]byte, len(job.keys))

	var edge []int

	nodes := order[:0:0]

	for _, i := range order {
		data, err := rs.readObject(ctx, job.bucket, job.keys[i])
		if err != nil {
			nodes = append(nodes, i)

			continue
		}

		cache[job.keys[i]] = data

		if isEdge, _ := pgFeedIsEdge(data, job.pg == pgOpenCypher); isEdge && !job.edgeOnly {
			edge = append(edge, i)

			continue
		}

		nodes = append(nodes, i)
	}

	return slices.Concat(nodes, edge), cache
}

func (rs *rdfService) readPGFeed(
	ctx context.Context, job *loadJob, cache map[string][]byte, uri, key string,
) feedResult {
	if rs.graph == nil {
		return feedResult{
			status: loadFailed, errs: []loadError{{Code: errCodeInternal, Message: errNoGraph.Error(), File: uri}},
			insert: 1,
		}
	}

	data, ok := cache[key]
	if !ok {
		var err error

		if data, err = rs.readObject(ctx, job.bucket, key); err != nil {
			return feedResult{
				status: loadS3ReadError,
				errs:   []loadError{{Code: "S3_ACCESS_DENIED_ERROR", Message: err.Error(), File: uri}},
			}
		}
	}

	res := feedResult{status: loadCompleted}
	cypher := job.pg == pgOpenCypher

	_ = rs.graph.Update(ctx, func(st *opencypher.Store) error {
		l := &pgLoader{st: st, job: job, res: &res, uri: uri, cypher: cypher}
		l.run(data)

		return nil
	})

	if res.parsing+res.datatype+res.insert > 0 && job.failOnErr {
		res.status = loadFailed
	}

	return res
}

func (l *pgLoader) run(data []byte) {
	recs, perr := parseCSV(data)

	for len(recs) > 0 && len(recs[0]) == 1 && recs[0][0].val == "" && !recs[0][0].quoted {
		recs = recs[1:]
	}

	if len(recs) == 0 {
		if perr != nil {
			l.res.status = loadFailed
			l.fail(pgErrParse, errCodeParsing, perr.Error(), 0)
		}

		return
	}

	hdr, err := parsePGHeader(recs[0], l.cypher, l.job.userEdgeIDs)
	if err != nil {
		l.res.status = loadFailed
		l.fail(pgErrParse, errCodeParsing, err.Error(), 1)

		return
	}

	l.hdr = hdr

	for i, rec := range recs[1:] {
		if l.stop {
			return
		}

		if len(rec) == 1 && rec[0].val == "" && !rec[0].quoted {
			continue
		}

		l.row(rec, i+firstDataRecord)
	}

	if perr != nil {
		l.res.status = loadFailed
		l.fail(pgErrParse, errCodeParsing, perr.Error(), len(recs)+1)
	}
}

type pgProp struct {
	key  string
	card string
	vals []any
}

type pgRow struct {
	id     string
	from   string
	to     string
	typ    string
	labels []string
	props  []pgProp
}

// convertRow turns a record into typed values; any failure skips the whole record.
func (l *pgLoader) convertRow(rec []csvField, recNum int) (*pgRow, bool) {
	if len(rec) != len(l.hdr.cols) {
		l.fail(pgErrParse, errCodeParsing,
			fmt.Sprintf("Expected %d columns, found %d", len(l.hdr.cols), len(rec)), recNum)

		return nil, false
	}

	row := &pgRow{}

	for i, c := range l.hdr.cols {
		f := rec[i]
		if f.val == "" && !l.keepEmpty(c, f) {
			if c.kind != colProp && c.kind != colLabel && c.kind != colType {
				l.fail(pgErrParse, errCodeParsing, "Blank value in required column", recNum)

				return nil, false
			}

			continue
		}

		if !l.setColumn(row, c, f, recNum) {
			return nil, false
		}
	}

	return row, true
}

func (l *pgLoader) keepEmpty(c pgColumn, f csvField) bool {
	return f.quoted && c.kind == colProp && c.typ == typString && l.job.allowEmpty
}

func (l *pgLoader) setColumn(row *pgRow, c pgColumn, f csvField, recNum int) bool {
	switch c.kind {
	case colID:
		row.id = f.val
		if c.idProp {
			row.props = append(row.props, pgProp{key: c.name, vals: []any{f.val}, card: cardSingle})
		}
	case colFrom:
		row.from = f.val
	case colTo:
		row.to = f.val
	case colType:
		row.typ = f.val
	case colLabel:
		if l.hdr.isEdge {
			row.typ = f.val
		} else {
			row.labels = splitLabels(f.val)
		}
	case colProp:
		return l.propColumn(row, c, f, recNum)
	}

	return true
}

func splitLabels(s string) []string {
	var out []string

	for _, lbl := range splitUnescaped(s, ';') {
		if lbl = strings.TrimSpace(lbl); lbl != "" {
			out = append(out, lbl)
		}
	}

	return out
}

func (l *pgLoader) propColumn(row *pgRow, c pgColumn, f csvField, recNum int) bool {
	raw := []string{f.val}
	if c.array {
		raw = splitUnescaped(f.val, ';')
	}

	p := pgProp{key: c.name, card: c.card}
	if p.card == "" {
		p.card = cardSet
		if l.cypher || l.hdr.isEdge {
			p.card = cardSingle
		}
	}

	for _, r := range raw {
		if c.array {
			r = strings.TrimSpace(r)
		}

		v, err := convertPGValue(c.typ, r, l.cypher)
		if err != nil {
			l.fail(pgErrDatatype, errCodeParsing, fmt.Sprintf("Column %q: %s", c.name, err.Error()), recNum)

			return false
		}

		p.vals = append(p.vals, v)
	}

	row.props = append(row.props, p)

	return true
}

func (l *pgLoader) row(rec []csvField, recNum int) {
	row, ok := l.convertRow(rec, recNum)
	if !ok {
		return
	}

	if l.hdr.isEdge {
		l.edgeRow(row, recNum)
	} else {
		l.nodeRow(row, recNum)
	}
}

func (l *pgLoader) nodeRow(row *pgRow, recNum int) {
	n := l.st.Node(row.id)
	labels := row.labels

	if n == nil {
		if len(labels) == 0 && !l.cypher {
			labels = []string{defaultVertexLbl}
		}

		var err error
		if n, err = l.st.AddNode(row.id, labels, nil); err != nil {
			l.fail(pgErrInsert, errCodeInternal, err.Error(), recNum)

			return
		}

		l.res.records += max(len(labels), 1)
	} else {
		l.res.records += max(len(labels), 1)
		l.mergeLabels(n, labels)
	}

	l.applyProps(n.Props, row.props, recNum)
}

func (l *pgLoader) mergeLabels(n *opencypher.Node, labels []string) {
	if len(labels) == 0 {
		l.res.dups++
	}

	for _, lbl := range labels {
		if slices.Contains(n.Labels, lbl) {
			l.res.dups++

			continue
		}

		n.Labels = append(n.Labels, lbl)
	}
}

func (l *pgLoader) edgeRow(row *pgRow, recNum int) {
	from, to := l.st.Node(row.from), l.st.Node(row.to)
	if from == nil || to == nil {
		l.res.records++
		l.fail(pgErrInsert, errCodeMissingEnd,
			fmt.Sprintf("Either from vertex, '%s', or to vertex, '%s', is not present.", row.from, row.to), recNum)

		return
	}

	typ := row.typ
	if typ == "" {
		typ = defaultEdgeLbl
	}

	r := l.st.Rel(row.id)
	if r != nil {
		l.res.records++

		if r.Start != from.ID || r.End != to.ID || r.Type != typ {
			l.fail(pgErrInsert, errCodeMultiEdge,
				fmt.Sprintf("Edge id '%s' is assigned to multiple edges.", row.id), recNum)

			return
		}

		l.res.dups++
		l.applyProps(r.Props, row.props, recNum)

		return
	}

	r, err := l.st.AddRel(row.id, typ, from, to, nil)
	if err != nil {
		l.fail(pgErrInsert, errCodeInternal, err.Error(), recNum)

		return
	}

	l.res.records++
	l.applyProps(r.Props, row.props, recNum)
}

func (l *pgLoader) applyProps(props map[string]any, ps []pgProp, recNum int) {
	for _, p := range ps {
		if l.stop {
			return
		}

		if p.card == cardSingle && len(p.vals) > 1 {
			l.res.records += len(p.vals)
			l.fail(pgErrInsert, errCodeSingleCard,
				fmt.Sprintf("Multiple values for single-cardinality property '%s'.", p.key), recNum)

			continue
		}

		for _, v := range p.vals {
			l.res.records++
			l.applyValue(props, p, v, recNum)
		}
	}
}

func (l *pgLoader) applyValue(props map[string]any, p pgProp, v any, recNum int) {
	cur, exists := props[p.key]

	switch {
	case !exists:
		props[p.key] = v
	case p.card == cardSingle:
		switch {
		case cur == v:
			l.res.dups++
		case l.job.updateSingle:
			props[p.key] = v
		case l.cypher:
			l.res.dups++
		default:
			l.fail(pgErrInsert, errCodeSingleCard,
				fmt.Sprintf("Property '%s' already has a value and has single cardinality.", p.key), recNum)
		}
	default:
		if list, isList := cur.([]any); isList {
			if slices.Contains(list, v) {
				l.res.dups++

				return
			}

			props[p.key] = append(list, v)

			return
		}

		if cur == v {
			l.res.dups++

			return
		}

		props[p.key] = []any{cur, v}
	}
}
