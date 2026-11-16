package sparql

import (
	"context"
	pathpkg "path"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// Fetcher retrieves the document behind a LOAD source.
type Fetcher func(ctx context.Context, src string) ([]byte, error)

// Document is a fetched LOAD source with the Content-Type the server reported.
type Document struct {
	ContentType string
	Data        []byte
}

// DocFetcher retrieves a LOAD source together with its Content-Type.
type DocFetcher func(ctx context.Context, src string) (Document, error)

// UpdateOptions carries the update's external dependencies; FetchDoc wins over Fetch.
type UpdateOptions struct {
	Fetch    Fetcher
	FetchDoc DocFetcher
	Service  ServiceFunc
}

// ParameterError is an invalid input value, such as a LOAD source that cannot be read or parsed.
type ParameterError struct {
	Err error
	Msg string
}

func (e *ParameterError) Error() string { return e.Msg }

func (e *ParameterError) Unwrap() error { return e.Err }

// UpdateResult counts the triples an update changed.
type UpdateResult struct{ Inserted, Deleted int }

// Access reports whether the request writes and/or deletes data, for IAM action selection.
func (u *Update) Access() (bool, bool) {
	write, del := false, false

	for i := range u.ops {
		op := &u.ops[i]

		switch op.Kind {
		case upInsertData, upLoad, upAdd, upCopy:
			write = true
		case upMove:
			write, del = true, true
		case upDeleteData, upClear, upDrop:
			del = true
		case upModify:
			write = write || len(op.Ins) > 0
			del = del || len(op.Del) > 0
		case upCreate:
		}

		if op.Kind == upCopy {
			del = true
		}
	}

	return write, del
}

type journalEntry struct {
	q     Quad
	added bool
}

type updateTx struct {
	s       *Store
	service ServiceFunc
	pre     *prefetched
	journal []journalEntry
}

func (t *updateTx) add(q Quad) bool {
	q = canon(q)
	if !t.s.add(q) {
		return false
	}

	t.journal = append(t.journal, journalEntry{q: q, added: true})

	return true
}

func (t *updateTx) remove(q Quad) bool {
	q = canon(q)
	if !t.s.remove(q) {
		return false
	}

	t.journal = append(t.journal, journalEntry{q: q})

	return true
}

func (t *updateTx) rollback() {
	for _, e := range slices.Backward(t.journal) {
		if e.added {
			t.s.remove(e.q)
		} else {
			t.s.add(e.q)
		}
	}
}

// Update parses and applies a SPARQL update request atomically.
func (s *Store) Update(ctx context.Context, src string, opts UpdateOptions) (UpdateResult, error) {
	u, err := ParseUpdate(src)
	if err != nil {
		return UpdateResult{}, err
	}

	return s.Apply(ctx, u, opts)
}

// Apply runs a parsed update under the write lock; a failing operation rolls the whole request back.
func (s *Store) Apply(ctx context.Context, u *Update, opts UpdateOptions) (UpdateResult, error) {
	pre := prefetch(ctx, u, opts)

	s.mu.Lock("Update")
	defer s.mu.Unlock()

	tx := &updateTx{s: s, pre: pre}
	if opts.Service != nil {
		tx.service = pre.serviceFunc
	}
	s.begin()

	var res UpdateResult

	for i := range u.ops {
		if err := tx.apply(ctx, &u.ops[i], opts, &res); err != nil {
			tx.rollback()
			s.pending = nil
			s.recording = false

			return UpdateResult{}, err
		}
	}

	s.commit()

	return res, nil
}

func (t *updateTx) apply(ctx context.Context, op *updateOp, opts UpdateOptions, res *UpdateResult) error {
	switch op.Kind {
	case upInsertData:
		res.Inserted += t.insertData(op)
	case upDeleteData:
		res.Deleted += t.deleteData(op)
	case upModify:
		return t.modify(ctx, op, res)
	case upLoad:
		n, err := t.load(ctx, op, opts)
		if err != nil && !op.Silent {
			return err
		}

		res.Inserted += n
	case upClear, upDrop:
		res.Deleted += t.clear(op)
	case upCreate:
	case upAdd, upMove, upCopy:
		t.copyGraph(op, res)
	}

	return nil
}

func graphOf(target graphTarget, g Term) Term {
	if target == tgDefault {
		return IRI(DefaultGraph)
	}

	return g
}

func ground(n node, blanks map[string]Term) Term {
	if !n.isVar() {
		return n.Term
	}

	if strings.HasPrefix(n.Var, "_:") {
		t, ok := blanks[n.Var]
		if !ok {
			t = Blank(strings.ReplaceAll(uuid.NewString(), "-", "")[:16])
			blanks[n.Var] = t
		}

		return t
	}

	return Term{}
}

func (t *updateTx) insertData(op *updateOp) int {
	blanks := map[string]Term{}
	n := 0

	for _, qp := range op.Ins {
		q := Quad{
			S: ground(qp.T.S, blanks), P: ground(qp.T.P, blanks), O: ground(qp.T.O, blanks),
			G: ground(qp.G, blanks),
		}
		if validTriple(q.Triple) && t.add(q) {
			n++
		}
	}

	return n
}

func (t *updateTx) deleteData(op *updateOp) int {
	n := 0

	for _, qp := range op.Del {
		q := Quad{
			S: qp.T.S.Term, P: qp.T.P.Term, O: qp.T.O.Term,
			G: qp.G.Term,
		}
		if !q.G.Bound() {
			q.G = IRI(DefaultGraph)
		}

		if t.remove(q) {
			n++
		}
	}

	return n
}

func (t *updateTx) modify(ctx context.Context, op *updateOp, res *UpdateResult) error {
	ds := &dataset{s: t.s}

	switch {
	case len(op.Using) > 0 || len(op.UsingNamed) > 0:
		ds.def, ds.named, ds.defR, ds.namedR = op.Using, op.UsingNamed, true, true
	case op.With.Bound():
		ds.def, ds.defR = []Term{op.With}, true
	}

	ev := newEvaluator(ctx, ds, DefaultBase)
	ev.svc, ev.prologue = t.service, op.Prologue

	sols, err := ev.evalGroup(op.Where, Term{})
	if err != nil {
		return err
	}

	dflt := IRI(DefaultGraph)
	if op.With.Bound() {
		dflt = op.With
	}

	var dels, ins []Quad

	for _, b := range sols {
		dels = append(dels, instantiate(op.Del, b, dflt, nil)...)
		ins = append(ins, instantiate(op.Ins, b, dflt, map[string]Term{})...)
	}

	for _, q := range dels {
		if t.remove(q) {
			res.Deleted++
		}
	}

	for _, q := range ins {
		if t.add(q) {
			res.Inserted++
		}
	}

	return nil
}

// instantiate grounds templates under b; blanks (nil to forbid) are fresh per solution.
func instantiate(tmpl []quadPat, b Binding, dflt Term, blanks map[string]Term) []Quad {
	var out []Quad

	res := func(n node) Term {
		if n.isVar() && strings.HasPrefix(n.Var, "_:") {
			if blanks == nil {
				return Term{}
			}

			return ground(n, blanks)
		}

		return resolveNode(n, b)
	}

	for _, qp := range tmpl {
		q := Quad{S: res(qp.T.S), P: res(qp.T.P), O: res(qp.T.O), G: dflt}

		if qp.G.isVar() || qp.G.Term.Bound() {
			q.G = resolveNode(qp.G, b)
		}

		if validTriple(q.Triple) && q.G.Kind == KindIRI {
			out = append(out, q)
		}
	}

	return out
}

// FormatForSource guesses an RDF format from a LOAD source name.
func FormatForSource(src string) Format {
	name, _, _ := strings.Cut(src, "?")

	switch strings.ToLower(pathpkg.Ext(name)) {
	case ".nq":
		return FormatNQuads
	case ".ttl", ".n3":
		return FormatTurtle
	case ".trig":
		return FormatTriG
	case ".jsonld", ".json":
		return FormatJSONLD
	case ".rdf", ".xml", ".owl":
		return FormatRDFXML
	default:
		return FormatNTriples
	}
}

// FormatForContentType maps the media types Neptune documents for SPARQL UPDATE LOAD to a format.
func FormatForContentType(ct string) (Format, bool) {
	mt, _, _ := strings.Cut(ct, ";")

	switch strings.ToLower(strings.TrimSpace(mt)) {
	case "text/plain", "application/n-triples":
		return FormatNTriples, true
	case "application/n-quads", "text/x-nquads":
		return FormatNQuads, true
	case "text/turtle", "application/x-turtle":
		return FormatTurtle, true
	case "text/rdf+n3", "text/n3":
		return FormatTurtle, true
	case "application/trig", "application/x-trig":
		return FormatTriG, true
	case "application/rdf+xml":
		return FormatRDFXML, true
	case "application/ld+json":
		return FormatJSONLD, true
	default:
		return 0, false
	}
}

func fetchDocument(ctx context.Context, src string, opts UpdateOptions) (Document, error) {
	switch {
	case opts.FetchDoc != nil:
		return opts.FetchDoc(ctx, src)
	case opts.Fetch != nil:
		data, err := opts.Fetch(ctx, src)

		return Document{Data: data}, err
	default:
		return Document{}, syntaxf("LOAD is not available: no document fetcher configured")
	}
}

func (t *updateTx) load(ctx context.Context, op *updateOp, opts UpdateOptions) (int, error) {
	doc, err := t.pre.document(ctx, op.Src, opts)
	if err != nil {
		return 0, &ParameterError{Msg: "Failed to load " + op.Src + ": " + err.Error(), Err: err}
	}

	format, known := FormatForContentType(doc.ContentType)
	if !known {
		format = FormatForSource(op.Src)
	}

	into := IRI(DefaultGraph)
	if op.Into.Bound() {
		into = op.Into
	}

	qs, err := ParseRDF(format, doc.Data, ParseOptions{Graph: into, Base: op.Src})
	if err != nil {
		return 0, &ParameterError{Msg: "Invalid syntax in the specified file", Err: err}
	}

	n := 0

	for _, q := range qs {
		if t.add(q) {
			n++
		}
	}

	return n, nil
}

func (t *updateTx) clear(op *updateOp) int {
	var graphs []Term

	dflt := IRI(DefaultGraph)

	switch op.Target {
	case tgGraph:
		graphs = []Term{op.Graph}
	case tgDefault:
		graphs = []Term{dflt}
	default:
		for _, g := range t.s.graphsLocked() {
			if op.Target == tgAll || g != dflt {
				graphs = append(graphs, g)
			}
		}
	}

	n := 0

	for _, g := range graphs {
		for _, q := range t.s.sortedQuads(t.s.byG[g]) {
			if t.remove(q) {
				n++
			}
		}
	}

	return n
}

func (t *updateTx) copyGraph(op *updateOp, res *UpdateResult) {
	from, to := graphOf(op.Target, op.Graph), graphOf(op.ToTarget, op.To)
	if from == to {
		return
	}

	src := t.s.sortedQuads(t.s.byG[from])

	if op.Kind != upAdd {
		for _, q := range t.s.sortedQuads(t.s.byG[to]) {
			if t.remove(q) {
				res.Deleted++
			}
		}
	}

	for _, q := range src {
		q.G = to
		if t.add(q) {
			res.Inserted++
		}
	}

	if op.Kind == upMove {
		for _, q := range src {
			q.G = from
			if t.remove(q) {
				res.Deleted++
			}
		}
	}
}
