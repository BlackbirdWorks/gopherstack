package gcpdatastore

import (
	"bytes"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	batchSize     = 300
	cursorMagic   = "\x01DSC"
	cursorKeyFmt  = 6
	originCursor  = cursorMagic
	maxQueryScans = 10_000_000
)

type cand struct {
	key *datastorepb.Key
	ent *datastorepb.Entity
	ver *entVer
}

// row is one query result with its sort position.
type row struct {
	key  *datastorepb.Key
	ent  *datastorepb.Entity
	ver  *entVer
	vals []*datastorepb.Value
}

type queryResult struct {
	readTime   *timestamppb.Timestamp
	rows       []*row
	skipCursor []byte
	endCursor  []byte
	skipped    int
	more       datastorepb.QueryResultBatch_MoreResultsType
	resultType datastorepb.EntityResult_ResultType
}

func (b *InMemoryBackend) candidates(p *queryPlan, rv *readView) ([]*cand, error) {
	switch p.kind {
	case kindProperty:
		return b.kindCandidates(p, rv), nil
	case nsProperty:
		return b.namespaceCandidates(p, rv), nil
	case propProperty:
		return nil, errUnimplemented("__property__ metadata queries are not supported.")
	}

	var out []*cand

	for _, rec := range b.ents {
		if keyPartition(rec.key) != p.part {
			continue
		}

		if p.kind != "" && keyKind(rec.key) != p.kind {
			continue
		}

		if p.ancestor != nil && !isDescendantOrSelf(rec.key, p.ancestor) {
			continue
		}

		ver := rec.at(rv.at)
		if ver == nil {
			continue
		}

		out = append(out, &cand{key: rec.key, ent: ver.ent, ver: ver})
	}

	return out, nil
}

func (b *InMemoryBackend) metaCandidate(part partition, id *datastorepb.Key_PathElement) *cand {
	key := &datastorepb.Key{PartitionId: part.stored(), Path: []*datastorepb.Key_PathElement{id}}

	return &cand{key: key, ent: &datastorepb.Entity{Key: key}, ver: &entVer{}}
}

func (b *InMemoryBackend) kindCandidates(p *queryPlan, rv *readView) []*cand {
	kinds := map[string]struct{}{}

	for _, rec := range b.ents {
		if keyPartition(rec.key) == p.part && rec.at(rv.at) != nil && !isReserved(keyKind(rec.key)) {
			kinds[keyKind(rec.key)] = struct{}{}
		}
	}

	out := make([]*cand, 0, len(kinds))

	for k := range kinds {
		out = append(out, b.metaCandidate(p.part, &datastorepb.Key_PathElement{
			Kind: kindProperty, IdType: &datastorepb.Key_PathElement_Name{Name: k},
		}))
	}

	return out
}

func (b *InMemoryBackend) namespaceCandidates(p *queryPlan, rv *readView) []*cand {
	spaces := map[string]struct{}{}

	for _, rec := range b.ents {
		kp := keyPartition(rec.key)
		if kp.project == p.part.project && kp.database == p.part.database && rec.at(rv.at) != nil {
			spaces[kp.namespace] = struct{}{}
		}
	}

	root := partition{project: p.part.project, database: p.part.database}
	out := make([]*cand, 0, len(spaces))

	for ns := range spaces {
		el := &datastorepb.Key_PathElement{Kind: nsProperty, IdType: &datastorepb.Key_PathElement_Id{Id: 1}}
		if ns != "" {
			el.IdType = &datastorepb.Key_PathElement_Name{Name: ns}
		}

		out = append(out, b.metaCandidate(root, el))
	}

	return out
}

func matchFilter(f *datastorepb.Filter, c *cand) bool {
	if f == nil || f.GetFilterType() == nil {
		return true
	}

	switch t := f.GetFilterType().(type) {
	case *datastorepb.Filter_PropertyFilter:
		return matchProp(t.PropertyFilter, c)
	case *datastorepb.Filter_CompositeFilter:
		cf := t.CompositeFilter
		if cf.GetOp() == datastorepb.CompositeFilter_OR {
			return slices.ContainsFunc(cf.GetFilters(), func(ch *datastorepb.Filter) bool { return matchFilter(ch, c) })
		}

		return !slices.ContainsFunc(cf.GetFilters(), func(ch *datastorepb.Filter) bool { return !matchFilter(ch, c) })
	}

	return true
}

func filterValues(pf *datastorepb.PropertyFilter, c *cand) []*datastorepb.Value {
	if pf.GetProperty().GetName() == keyProperty {
		return []*datastorepb.Value{keyValue(c.key)}
	}

	return propValues(c.ent, pf.GetProperty().GetName())
}

func matchProp(pf *datastorepb.PropertyFilter, c *cand) bool {
	vals := filterValues(pf, c)
	fv := pf.GetValue()

	switch pf.GetOp() {
	case datastorepb.PropertyFilter_EQUAL:
		return slices.ContainsFunc(vals, func(v *datastorepb.Value) bool { return compareValues(v, fv) == 0 })
	case datastorepb.PropertyFilter_IN:
		return slices.ContainsFunc(vals, func(v *datastorepb.Value) bool { return inList(v, arrayElems(fv)) })
	case datastorepb.PropertyFilter_NOT_EQUAL:
		return slices.ContainsFunc(vals, func(v *datastorepb.Value) bool { return compareValues(v, fv) != 0 })
	case datastorepb.PropertyFilter_NOT_IN:
		for _, el := range arrayElems(fv) {
			if !slices.ContainsFunc(vals, func(v *datastorepb.Value) bool { return compareValues(v, el) != 0 }) {
				return false
			}
		}

		return len(vals) > 0
	default:
		return slices.ContainsFunc(vals, func(v *datastorepb.Value) bool { return rangeMatch(pf.GetOp(), v, fv) })
	}
}

func inList(v *datastorepb.Value, list []*datastorepb.Value) bool {
	return slices.ContainsFunc(list, func(e *datastorepb.Value) bool { return compareValues(v, e) == 0 })
}

func rangeMatch(op datastorepb.PropertyFilter_Operator, v, fv *datastorepb.Value) bool {
	if bucketOf(v) != bucketOf(fv) {
		return false
	}

	c := compareValues(v, fv)

	switch op {
	case datastorepb.PropertyFilter_LESS_THAN:
		return c < 0
	case datastorepb.PropertyFilter_LESS_THAN_OR_EQUAL:
		return c <= 0
	case datastorepb.PropertyFilter_GREATER_THAN:
		return c > 0
	case datastorepb.PropertyFilter_GREATER_THAN_OR_EQUAL:
		return c >= 0
	default:
		return false
	}
}

// sortValues picks the value a row sorts by for each effective order, or false when a property is missing.
func (p *queryPlan) sortValues(c *cand, projVals map[string]*datastorepb.Value) ([]*datastorepb.Value, bool) {
	out := make([]*datastorepb.Value, 0, len(p.orders))

	for _, o := range p.orders {
		if o.name == keyProperty {
			out = append(out, keyValue(c.key))

			continue
		}

		if v, ok := projVals[o.name]; ok {
			out = append(out, v)

			continue
		}

		vals := propValues(c.ent, o.name)
		if len(vals) == 0 {
			return nil, false
		}

		out = append(out, extreme(vals, o.desc))
	}

	return out, true
}

func extreme(vals []*datastorepb.Value, wantMax bool) *datastorepb.Value {
	best := vals[0]

	for _, v := range vals[1:] {
		if c := compareValues(v, best); (wantMax && c > 0) || (!wantMax && c < 0) {
			best = v
		}
	}

	return best
}

// projectionRows expands a candidate into one row per combination of projected array elements.
func (p *queryPlan) projectionRows(c *cand) []map[string]*datastorepb.Value {
	combos := []map[string]*datastorepb.Value{{}}

	for _, name := range p.proj {
		if name == keyProperty {
			continue
		}

		vals := propValues(c.ent, name)
		if len(vals) == 0 {
			return nil
		}

		next := make([]map[string]*datastorepb.Value, 0, len(combos)*len(vals))

		for _, combo := range combos {
			for _, v := range uniqueValues(vals) {
				m := make(map[string]*datastorepb.Value, len(combo)+1)
				maps.Copy(m, combo)

				m[name] = v
				next = append(next, m)
			}
		}

		combos = next
	}

	return combos
}

func uniqueValues(vals []*datastorepb.Value) []*datastorepb.Value {
	out := make([]*datastorepb.Value, 0, len(vals))

	for _, v := range vals {
		if !inList(v, out) {
			out = append(out, v)
		}
	}

	return out
}

func (p *queryPlan) buildRows(cands []*cand) []*row {
	var rows []*row

	hasProj := len(p.proj) > 0 && !p.keysOnly

	for _, c := range cands {
		if !matchFilter(p.filter, c) {
			continue
		}

		combos := []map[string]*datastorepb.Value{nil}
		if hasProj {
			combos = p.projectionRows(c)
		}

		for _, combo := range combos {
			if r := p.rowFor(c, combo, hasProj); r != nil {
				rows = append(rows, r)
			}
		}
	}

	return rows
}

func (p *queryPlan) rowFor(c *cand, combo map[string]*datastorepb.Value, hasProj bool) *row {
	vals, ok := p.sortValues(c, combo)
	if !ok {
		return nil
	}

	r := &row{key: c.key, ver: c.ver, vals: vals}

	switch {
	case p.keysOnly:
		r.ent = &datastorepb.Entity{Key: c.key}
	case hasProj:
		r.ent = projected(c, p.proj, combo)

		for _, name := range p.proj {
			if name != keyProperty {
				r.vals = append(r.vals, combo[name])
			}
		}
	default:
		r.ent = c.ent
	}

	return r
}

func projected(c *cand, proj []string, combo map[string]*datastorepb.Value) *datastorepb.Entity {
	e := &datastorepb.Entity{Key: c.key, Properties: map[string]*datastorepb.Value{}}

	for _, name := range proj {
		if name != keyProperty {
			e.Properties[name] = combo[name]
		}
	}

	return e
}

func (p *queryPlan) comparePos(a, b []*datastorepb.Value) int {
	for i := range a {
		c := compareValues(a[i], b[i])
		if i < len(p.orders) && p.orders[i].desc {
			c = -c
		}

		if c != 0 {
			return c
		}
	}

	return 0
}

func valueSig(v *datastorepb.Value) string {
	var sb strings.Builder

	sb.WriteString(strconv.Itoa(bucketOf(v)))
	sb.WriteByte(':')

	switch bucketOf(v) {
	case bucketInt:
		sb.WriteString(strconv.FormatInt(intOf(v), 10))
	case bucketBool:
		sb.WriteString(strconv.FormatBool(v.GetBooleanValue()))
	case bucketBlob:
		sb.Write(v.GetBlobValue())
	case bucketString:
		sb.WriteString(v.GetStringValue())
	case bucketDouble:
		sb.WriteString(strconv.FormatFloat(v.GetDoubleValue(), 'g', -1, 64))
	case bucketGeo:
		sb.WriteString(strconv.FormatFloat(v.GetGeoPointValue().GetLatitude(), 'g', -1, 64))
		sb.WriteByte(',')
		sb.WriteString(strconv.FormatFloat(v.GetGeoPointValue().GetLongitude(), 'g', -1, 64))
	case bucketKey:
		sb.WriteString(keyString(v.GetKeyValue()))
	}

	return sb.String()
}

func (p *queryPlan) applyDistinct(rows []*row, cands map[*datastorepb.Key]*cand) []*row {
	if len(p.distinct) == 0 {
		return rows
	}

	seen := map[string]struct{}{}
	out := rows[:0]

	for _, r := range rows {
		var sb strings.Builder

		complete := true

		for _, name := range p.distinct {
			v, ok := p.distinctValue(r, name, cands[r.key])
			if !ok {
				complete = false

				break
			}

			sb.WriteString(valueSig(v))
			sb.WriteByte(0)
		}

		if !complete {
			continue
		}

		if _, dup := seen[sb.String()]; dup {
			continue
		}

		seen[sb.String()] = struct{}{}

		out = append(out, r)
	}

	return out
}

func (p *queryPlan) distinctValue(r *row, name string, c *cand) (*datastorepb.Value, bool) {
	if name == keyProperty {
		return keyValue(r.key), true
	}

	if v, ok := r.ent.GetProperties()[name]; ok && len(p.proj) > 0 {
		return v, true
	}

	vals := propValues(c.ent, name)
	if len(vals) == 0 {
		return nil, false
	}

	return extreme(vals, false), true
}

func encodeCursor(vals []*datastorepb.Value) []byte {
	e := &datastorepb.Entity{Properties: make(map[string]*datastorepb.Value, len(vals))}

	for i, v := range vals {
		e.Properties[cursorProp(i)] = v
	}

	raw, _ := proto.MarshalOptions{Deterministic: true}.Marshal(e)

	return append([]byte(cursorMagic), raw...)
}

func cursorProp(i int) string {
	s := strconv.Itoa(i)

	return strings.Repeat("0", cursorKeyFmt-len(s)) + s
}

func decodeCursor(raw []byte, want int) ([]*datastorepb.Value, error) {
	if !bytes.HasPrefix(raw, []byte(cursorMagic)) {
		return nil, errInvalid("Invalid cursor.")
	}

	e := &datastorepb.Entity{}
	if err := proto.Unmarshal(raw[len(cursorMagic):], e); err != nil {
		return nil, errInvalid("Invalid cursor.")
	}

	if len(e.GetProperties()) == 0 {
		return nil, nil
	}

	if len(e.GetProperties()) != want {
		return nil, errInvalid("Invalid cursor: it does not belong to this query.")
	}

	out := make([]*datastorepb.Value, want)

	for i := range want {
		v, ok := e.GetProperties()[cursorProp(i)]
		if !ok {
			return nil, errInvalid("Invalid cursor.")
		}

		out[i] = v
	}

	return out, nil
}

// execQuery runs a validated plan against rv and slices the rows into one result batch.
func (b *InMemoryBackend) execQuery(p *queryPlan, rv *readView) (*queryResult, error) {
	if rv.txn != nil && !rv.txn.readOnly {
		rv.txn.queries = append(rv.txn.queries, queryScope{ancestor: p.ancestor, part: p.part, kind: p.kind})
	}

	if err := b.checkIndex(p); err != nil {
		return nil, err
	}

	cands, err := b.candidates(p, rv)
	if err != nil {
		return nil, err
	}

	byKey := make(map[*datastorepb.Key]*cand, len(cands))
	for _, c := range cands {
		byKey[c.key] = c
	}

	rows := p.buildRows(cands)

	sort.SliceStable(rows, func(i, j int) bool { return p.comparePos(rows[i].vals, rows[j].vals) < 0 })

	rows = p.applyDistinct(rows, byKey)

	return p.slice(rows, rv)
}

func (p *queryPlan) posWidth() int {
	width := len(p.orders)
	if len(p.proj) > 0 && !p.keysOnly {
		width += len(p.proj)
		if slices.Contains(p.proj, keyProperty) {
			width--
		}
	}

	return width
}

func (p *queryPlan) resultType() datastorepb.EntityResult_ResultType {
	switch {
	case p.keysOnly:
		return datastorepb.EntityResult_KEY_ONLY
	case len(p.proj) > 0:
		return datastorepb.EntityResult_PROJECTION
	default:
		return datastorepb.EntityResult_FULL
	}
}

// window applies the start and end cursors and reports whether rows remain beyond the end cursor.
func (p *queryPlan) window(rows []*row) ([]*row, bool, error) {
	startPos, err := cursorPos(p.q.GetStartCursor(), p.posWidth())
	if err != nil {
		return nil, false, err
	}

	endPos, err := cursorPos(p.q.GetEndCursor(), p.posWidth())
	if err != nil {
		return nil, false, err
	}

	if startPos != nil {
		i := sort.Search(len(rows), func(i int) bool { return p.comparePos(rows[i].vals, startPos) > 0 })
		rows = rows[i:]
	}

	beyondEnd := false

	if endPos != nil {
		i := sort.Search(len(rows), func(i int) bool { return p.comparePos(rows[i].vals, endPos) > 0 })
		beyondEnd = i < len(rows)
		rows = rows[:i]
	}

	return rows, beyondEnd, nil
}

func (p *queryPlan) slice(rows []*row, rv *readView) (*queryResult, error) {
	res := &queryResult{readTime: timestamppb.New(rv.at), resultType: p.resultType()}

	rows, beyondEnd, err := p.window(rows)
	if err != nil {
		return nil, err
	}

	skip := min(p.offset, len(rows))
	if skip > 0 {
		res.skipCursor = encodeCursor(rows[skip-1].vals)
	}

	res.skipped, rows = skip, rows[skip:]

	take := len(rows)
	if p.hasLimit {
		take = min(take, p.limit)
	}

	capped := take
	if !p.unbounded {
		capped = min(take, batchSize)
	}

	res.rows = rows[:capped]
	res.more = moreResults(p, len(rows), capped, take, beyondEnd)

	switch {
	case capped > 0:
		res.endCursor = encodeCursor(res.rows[capped-1].vals)
	case res.skipCursor != nil:
		res.endCursor = res.skipCursor
	case p.q.GetStartCursor() != nil:
		res.endCursor = p.q.GetStartCursor()
	default:
		res.endCursor = []byte(originCursor)
	}

	return res, nil
}

func moreResults(
	p *queryPlan, remaining, returned, take int, beyondEnd bool,
) datastorepb.QueryResultBatch_MoreResultsType {
	switch {
	case returned < take:
		return datastorepb.QueryResultBatch_NOT_FINISHED
	case p.hasLimit && returned == p.limit && remaining > returned:
		return datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_LIMIT
	case beyondEnd:
		return datastorepb.QueryResultBatch_MORE_RESULTS_AFTER_CURSOR
	default:
		return datastorepb.QueryResultBatch_NO_MORE_RESULTS
	}
}

func cursorPos(raw []byte, width int) ([]*datastorepb.Value, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	return decodeCursor(raw, width)
}

func (r *queryResult) batch(mask *datastorepb.PropertyMask) *datastorepb.QueryResultBatch {
	out := &datastorepb.QueryResultBatch{
		SkippedResults:   clampInt32(r.skipped),
		SkippedCursor:    r.skipCursor,
		EntityResultType: r.resultType,
		EndCursor:        r.endCursor,
		MoreResults:      r.more,
		ReadTime:         r.readTime,
		SnapshotVersion:  r.readTime.AsTime().UnixMicro(),
	}

	for _, rw := range r.rows {
		er := &datastorepb.EntityResult{Entity: rw.ent, Cursor: encodeCursor(rw.vals)}

		if mask != nil && r.resultType == datastorepb.EntityResult_FULL {
			er.Entity = maskEntity(rw.key, rw.ent, mask)
		}

		if r.resultType != datastorepb.EntityResult_PROJECTION && rw.ver != nil && rw.ver.ent != nil {
			er.Version = rw.ver.version
			er.CreateTime = timestamppb.New(rw.ver.created)
			er.UpdateTime = timestamppb.New(rw.ver.at)
		}

		out.EntityResults = append(out.EntityResults, er)
	}

	return out
}

// RunQuery executes a structured or GQL query.
func (b *InMemoryBackend) RunQuery(in *datastorepb.RunQueryRequest) (*datastorepb.RunQueryResponse, error) {
	req, err := b.resolve(in.GetProjectId(), in.GetDatabaseId())
	if err != nil {
		return nil, err
	}

	if in.GetExplainOptions() != nil {
		return nil, errUnimplemented("Query explain is not supported.")
	}

	part, err := queryPartition(req, in.GetPartitionId())
	if err != nil {
		return nil, err
	}

	q, parsed, err := resolveQuery(in)
	if err != nil {
		return nil, err
	}

	plan, err := buildPlan(part, q)
	if err != nil {
		return nil, err
	}

	resp := &datastorepb.RunQueryResponse{Query: parsed}

	err = b.read(in.GetReadOptions(), req, func(rv *readView) error {
		res, qerr := b.execQuery(plan, rv)
		if qerr != nil {
			return qerr
		}

		resp.Batch, resp.Transaction = res.batch(in.GetPropertyMask()), rv.newTxnID

		return nil
	})
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func resolveQuery(in *datastorepb.RunQueryRequest) (*datastorepb.Query, *datastorepb.Query, error) {
	switch qt := in.GetQueryType().(type) {
	case *datastorepb.RunQueryRequest_Query:
		return qt.Query, nil, nil
	case *datastorepb.RunQueryRequest_GqlQuery:
		g, err := parseGQL(qt.GqlQuery)
		if err != nil {
			return nil, nil, err
		}

		if g.agg != nil {
			return nil, nil, errInvalid("Aggregation GQL must be run with RunAggregationQuery.")
		}

		return g.query, g.query, nil
	default:
		return nil, nil, errInvalid("A query or gql_query is required.")
	}
}
