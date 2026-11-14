package sparql

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

var errServiceUnavailable = errors.New("SERVICE federation is not available")

// ServiceFunc runs a SELECT query against the remote SPARQL endpoint of a SERVICE clause.
type ServiceFunc func(ctx context.Context, endpoint, query string) (*Result, error)

type serviceEl struct {
	Text   string
	Name   node
	Silent bool
}

func (*serviceEl) patternElement() {}

// SetService supplies the federation transport for SERVICE clauses.
func (q *Query) SetService(f ServiceFunc) { q.service = f }

func (p *parser) parseService(g *group) error {
	p.i++

	silent := p.acceptKW("SILENT")

	name, err := p.parseVarOrIRI()
	if err != nil {
		return err
	}

	open := p.peek()
	if !open.is("{") {
		return p.errf("expected '{' after the SERVICE endpoint")
	}

	if _, err = p.parseGroup(); err != nil {
		return err
	}

	closer := p.toks[p.i-1]
	g.Elems = append(g.Elems, &serviceEl{Name: name, Silent: silent, Text: p.src[open.pos : closer.pos+1]})

	return nil
}

func servicePrologue(q *Query) string { return prologueText(q.Base, q.Prefixes) }

func prologueText(base string, prefixes map[string]string) string {
	var sb strings.Builder

	sb.WriteString("BASE <" + base + ">\n")

	names := make([]string, 0, len(prefixes))
	for k := range prefixes {
		names = append(names, k)
	}

	sort.Strings(names)

	for _, k := range names {
		sb.WriteString("PREFIX " + k + ": <" + prefixes[k] + ">\n")
	}

	return sb.String()
}

func (ev *evaluator) evalService(x *serviceEl, sols []Binding, gc Term) ([]Binding, error) {
	if ev.svc == nil {
		return nil, syntaxf("%v", errServiceUnavailable)
	}

	if !x.Name.isVar() {
		return ev.serviceJoin(x, x.Name.Term.Value, sols, gc)
	}

	groups := map[string][]Binding{}

	var order []string

	for _, b := range sols {
		t := b[x.Name.Var]
		if t.Kind != KindIRI {
			continue
		}

		if _, seen := groups[t.Value]; !seen {
			order = append(order, t.Value)
		}

		groups[t.Value] = append(groups[t.Value], b)
	}

	var out []Binding

	for _, ep := range order {
		part, err := ev.serviceJoin(x, ep, groups[ep], gc)
		if err != nil {
			return nil, err
		}

		out = append(out, part...)
	}

	return out, nil
}

func (ev *evaluator) serviceJoin(x *serviceEl, endpoint string, sols []Binding, gc Term) ([]Binding, error) {
	res, err := ev.svc(ev.ctx, endpoint, ev.prologue+"SELECT * WHERE "+x.Text)
	if err != nil {
		if x.Silent && ev.ctx.Err() == nil {
			return sols, nil
		}

		return nil, fmt.Errorf("SERVICE %s: %w", endpoint, err)
	}

	return ev.join(sols, res.Rows, nil, false, gc)
}

type jsonTermWire struct {
	Type     string `json:"type"`
	Value    string `json:"value"`
	Lang     string `json:"xml:lang"`
	Datatype string `json:"datatype"`
}

type jsonResultsWire struct {
	Boolean *bool `json:"boolean"`
	Head    struct {
		Vars []string `json:"vars"`
	} `json:"head"`
	Results struct {
		Bindings []map[string]jsonTermWire `json:"bindings"`
	} `json:"results"`
}

// ParseSelectJSON decodes a SPARQL 1.1 Query Results JSON document (SELECT or ASK).
func ParseSelectJSON(data []byte) (*Result, error) {
	var w jsonResultsWire
	if err := json.Unmarshal(data, &w, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("invalid SPARQL JSON results: %w", err)
	}

	if w.Boolean != nil {
		return &Result{Kind: ResultAsk, Bool: *w.Boolean}, nil
	}

	res := &Result{Kind: ResultSelect, Vars: w.Head.Vars}

	for _, row := range w.Results.Bindings {
		b := Binding{}

		for v, t := range row {
			b[v] = t.term()
		}

		res.Rows = append(res.Rows, b)
	}

	return res, nil
}

func (t jsonTermWire) term() Term {
	switch t.Type {
	case "uri":
		return IRI(t.Value)
	case "bnode":
		return Blank(t.Value)
	}

	switch {
	case t.Lang != "":
		return LangLiteral(t.Value, t.Lang)
	case t.Datatype != "":
		return Typed(t.Value, t.Datatype)
	default:
		return Literal(t.Value)
	}
}
