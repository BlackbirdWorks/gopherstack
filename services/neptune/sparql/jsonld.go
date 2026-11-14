package sparql

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"maps"
	"strconv"
	"strings"
)

var (
	errJSONLD         = errors.New("invalid JSON-LD")
	errJSONLDRemote   = errors.New("remote JSON-LD contexts are not supported")
	errJSONLDTrailing = errors.New("trailing data after the JSON-LD document")
)

type ldNum string

type ldDef struct {
	iri  string
	typ  string
	list bool
}

type ldContext struct {
	terms map[string]ldDef
	vocab string
	base  string
}

func (c ldContext) clone() ldContext {
	n := ldContext{terms: make(map[string]ldDef, len(c.terms)), vocab: c.vocab, base: c.base}
	maps.Copy(n.terms, c.terms)

	return n
}

type ldParser struct {
	doc   string
	out   []Quad
	graph Term
	seq   int
}

func parseJSONLD(data []byte, opts ParseOptions, doc string) ([]Quad, error) {
	v, err := ldDecode(data)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errJSONLD, err.Error())
	}

	p := &ldParser{graph: opts.graph(), doc: doc}
	ctx := ldContext{terms: map[string]ldDef{}, base: opts.base()}

	if err = p.top(v, ctx); err != nil {
		return p.out, err
	}

	return p.out, nil
}

func ldDecode(data []byte) (any, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(data))

	v, err := ldValue(dec)
	if err != nil {
		return nil, err
	}

	if _, err = dec.ReadToken(); !errors.Is(err, io.EOF) {
		return nil, errJSONLDTrailing
	}

	return v, nil
}

func ldValue(dec *jsontext.Decoder) (any, error) {
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}

	switch tok.Kind() {
	case '{':
		m := map[string]any{}

		for dec.PeekKind() != '}' {
			k, kerr := dec.ReadToken()
			if kerr != nil {
				return nil, kerr
			}

			key := k.String()

			val, verr := ldValue(dec)
			if verr != nil {
				return nil, verr
			}

			m[key] = val
		}

		_, err = dec.ReadToken()

		return m, err
	case '[':
		arr := []any{}

		for dec.PeekKind() != ']' {
			val, verr := ldValue(dec)
			if verr != nil {
				return nil, verr
			}

			arr = append(arr, val)
		}

		_, err = dec.ReadToken()

		return arr, err
	case '"':
		return tok.String(), nil
	case '0':
		return ldNum(tok.String()), nil
	case 't', 'f':
		return tok.Bool(), nil
	default:
		return ldNull{}, nil
	}
}

type ldNull struct{}

func (p *ldParser) top(v any, ctx ldContext) error {
	switch t := v.(type) {
	case []any:
		for _, e := range t {
			if err := p.top(e, ctx); err != nil {
				return err
			}
		}

		return nil
	case map[string]any:
		_, err := p.node(t, ctx, p.graph)

		return err
	default:
		return fmt.Errorf("%w: top-level value must be an object or an array", errJSONLD)
	}
}

func (c ldContext) extend(raw any) (ldContext, error) {
	out := c.clone()

	items, ok := raw.([]any)
	if !ok {
		items = []any{raw}
	}

	for _, it := range items {
		switch d := it.(type) {
		case nil:
			out = ldContext{terms: map[string]ldDef{}, base: c.base}
		case string:
			return out, fmt.Errorf("%w: %q", errJSONLDRemote, d)
		case map[string]any:
			out = out.define(d)
		default:
			return out, fmt.Errorf("%w: bad @context", errJSONLD)
		}
	}

	return out, nil
}

func (c ldContext) define(m map[string]any) ldContext {
	if v, ok := m[jsonLDVocabKey].(string); ok {
		c.vocab = v
	}

	if v, ok := m[jsonLDBaseKey].(string); ok {
		c.base = v
	}

	for k, v := range m {
		if strings.HasPrefix(k, "@") {
			continue
		}

		switch d := v.(type) {
		case string:
			c.terms[k] = ldDef{iri: d}
		case map[string]any:
			def := ldDef{}
			def.iri, _ = d[jsonLDID].(string)
			def.typ, _ = d[jsonLDType].(string)
			def.list = d[jsonLDContainerKey] == jsonLDListKeyword

			if def.iri == "" {
				def.iri = k
			}

			c.terms[k] = def
		}
	}

	return c
}

// expandIRI resolves a term, compact IRI or relative IRI; vocab selects vocabulary-relative resolution.
func (c ldContext) expandIRI(s string, vocab bool) string {
	if vocab {
		if d, ok := c.terms[s]; ok {
			return c.expandIRI(d.iri, false)
		}
	}

	if strings.HasPrefix(s, "_:") {
		return s
	}

	if pre, rest, ok := strings.Cut(s, ":"); ok && !strings.HasPrefix(rest, "//") {
		if d, found := c.terms[pre]; found {
			return c.expandIRI(d.iri, false) + rest
		}

		return s
	}

	switch {
	case strings.Contains(s, "://"):
		return s
	case vocab && c.vocab != "":
		return c.vocab + s
	case !vocab:
		return resolveAgainst(c.base, s)
	default:
		return s
	}
}

func resolveAgainst(base, ref string) string {
	if ref == "" {
		return base
	}

	if i := strings.LastIndex(base, "/"); i >= 0 && !strings.Contains(ref, ":") {
		return base[:i+1] + strings.TrimPrefix(ref, "/")
	}

	return ref
}

func (p *ldParser) fresh() Term {
	p.seq++

	return Blank(p.doc + "jl" + strconv.Itoa(p.seq))
}

func (p *ldParser) idTerm(id string) Term {
	if label, ok := strings.CutPrefix(id, "_:"); ok {
		return Blank(p.doc + label)
	}

	return IRI(id)
}

func (p *ldParser) node(m map[string]any, parent ldContext, graph Term) (Term, error) {
	ctx := parent

	if raw, ok := m[jsonLDContextKey]; ok {
		var err error
		if ctx, err = parent.extend(raw); err != nil {
			return Term{}, err
		}
	}

	subj, err := p.subject(m, ctx)
	if err != nil {
		return subj, err
	}

	for k, v := range m {
		switch {
		case k == jsonLDID || k == jsonLDContextKey:
		case k == jsonLDGraph:
			if err = p.graphContent(v, ctx, subj, m); err != nil {
				return subj, err
			}
		case k == jsonLDType:
			for _, t := range ldList(v) {
				s, _ := t.(string)
				p.emit(subj, IRI(RDFType), IRI(ctx.expandIRI(s, true)), graph)
			}
		case strings.HasPrefix(k, "@"):
			return subj, fmt.Errorf("%w: unsupported keyword %s", errJSONLD, k)
		default:
			if err = p.property(subj, k, v, ctx, graph); err != nil {
				return subj, err
			}
		}
	}

	return subj, nil
}

func (p *ldParser) subject(m map[string]any, ctx ldContext) (Term, error) {
	if raw, ok := m[jsonLDID]; ok {
		id, isStr := raw.(string)
		if !isStr {
			return Term{}, fmt.Errorf("%w: @id must be a string", errJSONLD)
		}

		return p.idTerm(ctx.expandIRI(id, false)), nil
	}

	return p.fresh(), nil
}

func (p *ldParser) graphContent(v any, ctx ldContext, name Term, m map[string]any) error {
	g := name
	if _, named := m[jsonLDID]; !named {
		g = p.graph
	}

	for _, e := range ldList(v) {
		obj, ok := e.(map[string]any)
		if !ok {
			return fmt.Errorf("%w: @graph members must be objects", errJSONLD)
		}

		if _, err := p.node(obj, ctx, g); err != nil {
			return err
		}
	}

	return nil
}

func ldFlatten(items []any) []any {
	var out []any

	for _, e := range items {
		if m, ok := e.(map[string]any); ok {
			if set, isSet := m[jsonLDSetKeyword]; isSet {
				out = append(out, ldFlatten(ldList(set))...)

				continue
			}
		}

		out = append(out, e)
	}

	return out
}

func ldList(v any) []any {
	if l, ok := v.([]any); ok {
		return l
	}

	return []any{v}
}

func (p *ldParser) emit(s, pr, o, g Term) {
	p.out = append(p.out, Quad{Triple{S: s, P: pr, O: o}, g})
}

func (p *ldParser) property(subj Term, key string, v any, ctx ldContext, graph Term) error {
	def := ctx.terms[key]
	pred := ctx.expandIRI(key, true)

	if !strings.Contains(pred, ":") {
		return nil
	}

	if strings.HasPrefix(pred, "_:") {
		return nil
	}

	if def.list {
		head, err := p.list(ldList(v), def, ctx, graph)
		if err != nil {
			return err
		}

		p.emit(subj, IRI(pred), head, graph)

		return nil
	}

	for _, e := range ldFlatten(ldList(v)) {
		if e == nil {
			continue
		}

		o, err := p.object(e, def, ctx, graph)
		if err != nil {
			return err
		}

		p.emit(subj, IRI(pred), o, graph)
	}

	return nil
}

func (p *ldParser) list(items []any, def ldDef, ctx ldContext, graph Term) (Term, error) {
	if len(items) == 0 {
		return IRI(RDFNil), nil
	}

	head := p.fresh()
	cur := head

	for i, it := range items {
		o, err := p.object(it, def, ctx, graph)
		if err != nil {
			return head, err
		}

		p.emit(cur, IRI(RDFFirst), o, graph)

		if i == len(items)-1 {
			p.emit(cur, IRI(RDFRest), IRI(RDFNil), graph)

			break
		}

		next := p.fresh()
		p.emit(cur, IRI(RDFRest), next, graph)
		cur = next
	}

	return head, nil
}

func (p *ldParser) object(v any, def ldDef, ctx ldContext, graph Term) (Term, error) {
	switch t := v.(type) {
	case string:
		switch def.typ {
		case jsonLDID:
			return p.idTerm(ctx.expandIRI(t, false)), nil
		case "@vocab":
			return p.idTerm(ctx.expandIRI(t, true)), nil
		case "":
			return Literal(t), nil
		default:
			return Typed(t, ctx.expandIRI(def.typ, true)), nil
		}
	case bool:
		return Typed(strconv.FormatBool(t), XSDBoolean), nil
	case ldNum:
		return ldNumber(t), nil
	case map[string]any:
		return p.objectMap(t, ctx, graph)
	default:
		return Term{}, fmt.Errorf("%w: unsupported value", errJSONLD)
	}
}

func ldNumber(n ldNum) Term {
	s := string(n)
	if !strings.ContainsAny(s, ".eE") {
		return Typed(s, XSDInteger)
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return Typed(s, XSDDouble)
	}

	return Typed(ldCanonicalDouble(f), XSDDouble)
}

// ldCanonicalDouble renders the JSON-LD canonical double lexical form, e.g. 1.5E0.
func ldCanonicalDouble(f float64) string {
	mant, exp, _ := strings.Cut(strconv.FormatFloat(f, 'E', -1, 64), "E")
	if !strings.Contains(mant, ".") {
		mant += ".0"
	}

	e, _ := strconv.Atoi(exp)

	return mant + "E" + strconv.Itoa(e)
}

func (p *ldParser) objectMap(m map[string]any, ctx ldContext, graph Term) (Term, error) {
	if raw, ok := m[jsonLDValue]; ok {
		return ldValueObject(raw, m, ctx)
	}

	if raw, ok := m[jsonLDListKeyword]; ok {
		return p.list(ldList(raw), ldDef{}, ctx, graph)
	}

	return p.node(m, ctx, graph)
}

func ldValueObject(raw any, m map[string]any, ctx ldContext) (Term, error) {
	lang, _ := m[jsonLDLanguage].(string)
	dt, _ := m[jsonLDType].(string)

	var t Term

	switch v := raw.(type) {
	case string:
		t = Literal(v)
	case bool:
		t = Typed(strconv.FormatBool(v), XSDBoolean)
	case ldNum:
		t = ldNumber(v)
	default:
		return Term{}, fmt.Errorf("%w: bad @value", errJSONLD)
	}

	switch {
	case lang != "":
		return LangLiteral(t.Value, lang), nil
	case dt != "":
		return Typed(t.Value, ctx.expandIRI(dt, true)), nil
	default:
		return t, nil
	}
}
