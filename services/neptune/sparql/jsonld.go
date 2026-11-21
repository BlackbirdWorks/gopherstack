package sparql

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"maps"
	"sort"
	"strconv"
	"strings"
)

var (
	errJSONLD         = errors.New("invalid JSON-LD")
	errJSONLDRemote   = errors.New("remote JSON-LD contexts are not supported")
	errJSONLDTrailing = errors.New("trailing data after the JSON-LD document")
)

type ldNum string

type ldContainer struct {
	list, set, language, index, id, typ, graph bool
}

type ldDef struct {
	scoped    any
	iri       string
	typ       string
	lang      string
	cont      ldContainer
	hasLang   bool
	reverse   bool
	protected bool
}

// ldContext is the active JSON-LD context; revert is where nested node objects restart when it does not propagate.
type ldContext struct {
	terms  map[string]ldDef
	revert *ldContext
	vocab  string
	base   string
	lang   string
}

func (c ldContext) clone() ldContext {
	n := ldContext{
		terms: make(map[string]ldDef, len(c.terms)), vocab: c.vocab, base: c.base, lang: c.lang, revert: c.revert,
	}
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
			var err error
			if out, err = out.define(d); err != nil {
				return out, err
			}
		default:
			return out, fmt.Errorf("%w: bad @context", errJSONLD)
		}
	}

	return out, nil
}

func (c ldContext) define(m map[string]any) (ldContext, error) {
	if _, ok := m[ldImport]; ok {
		return c, fmt.Errorf("%w: @import", errJSONLDRemote)
	}

	if v, ok := m[jsonLDVocabKey].(string); ok {
		c.vocab = v
	}

	if v, ok := m[jsonLDBaseKey].(string); ok {
		c.base = v
	}

	if v, ok := m[jsonLDLanguage]; ok {
		c.lang, _ = v.(string)
	}

	all, _ := m[ldProtected].(bool)

	for _, k := range ldSortedKeys(m) {
		if strings.HasPrefix(k, "@") {
			continue
		}

		def := ldParseDef(k, m[k])
		if v, ok := m[k].(map[string]any); !ok || v[ldProtected] == nil {
			def.protected = def.protected || all
		}

		if old, exists := c.terms[k]; exists && old.protected && !ldSameDef(old, def) {
			return c, fmt.Errorf("%w: protected term %q cannot be redefined", errJSONLD, k)
		}

		c.terms[k] = def
	}

	return c, nil
}

const (
	ldProtected = "@protected"
	ldReverse   = "@reverse"
	ldNest      = "@nest"
	ldIndex     = "@index"
	ldNone      = "@none"
	ldImport    = "@import"
	ldPropagate = "@propagate"
	ldIncluded  = "@included"
)

// ldPropagates reports whether a context map keeps applying inside nested node objects (default true).
func ldPropagates(raw any) bool {
	for _, it := range ldList(raw) {
		if m, ok := it.(map[string]any); ok {
			if v, set := m[ldPropagate].(bool); set && !v {
				return false
			}
		}
	}

	return true
}

func ldSortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func ldParseDef(k string, v any) ldDef {
	switch d := v.(type) {
	case string:
		return ldDef{iri: d}
	case map[string]any:
		def := ldDef{}

		if r, ok := d[ldReverse].(string); ok {
			def.iri, def.reverse = r, true
		} else {
			def.iri, _ = d[jsonLDID].(string)
		}

		def.typ, _ = d[jsonLDType].(string)
		def.cont = ldParseContainer(d[jsonLDContainerKey])
		def.scoped = d[jsonLDContextKey]
		def.protected, _ = d[ldProtected].(bool)

		if l, ok := d[jsonLDLanguage]; ok {
			def.hasLang = true
			def.lang, _ = l.(string)
		}

		return def
	}

	return ldDef{iri: k}
}

func ldParseContainer(v any) ldContainer {
	var c ldContainer

	for _, e := range ldList(v) {
		s, _ := e.(string)

		switch s {
		case jsonLDListKeyword:
			c.list = true
		case jsonLDSetKeyword:
			c.set = true
		case jsonLDLanguage:
			c.language = true
		case ldIndex:
			c.index = true
		case jsonLDID:
			c.id = true
		case jsonLDType:
			c.typ = true
		case jsonLDGraph:
			c.graph = true
		}
	}

	return c
}

func ldSameDef(a, b ldDef) bool {
	return a.iri == b.iri && a.typ == b.typ && a.lang == b.lang && a.cont == b.cont &&
		a.reverse == b.reverse && a.hasLang == b.hasLang && fmt.Sprint(a.scoped) == fmt.Sprint(b.scoped)
}

// ldKeyword looks up a keyword member of m, honouring terms aliased to the keyword.
func ldKeyword(m map[string]any, ctx ldContext, kw string) (any, bool) {
	if v, ok := m[kw]; ok {
		return v, true
	}

	for _, k := range ldSortedKeys(m) {
		if ctx.keyword(k) == kw {
			return m[k], true
		}
	}

	return nil, false
}

// keyword returns the JSON-LD keyword a key stands for, resolving terms aliased to a keyword.
func (c ldContext) keyword(k string) string {
	if d, ok := c.terms[k]; ok && strings.HasPrefix(d.iri, "@") {
		return d.iri
	}

	return k
}

// expandIRI resolves a term, compact IRI or relative IRI; vocab selects vocabulary-relative resolution.
func (c ldContext) expandIRI(s string, vocab bool) string {
	if vocab {
		if d, ok := c.terms[s]; ok && d.iri != "" {
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
	if parent.revert != nil {
		parent = *parent.revert
	}

	ctx := parent

	if raw, ok := m[jsonLDContextKey]; ok {
		var err error
		if ctx, err = parent.extend(raw); err != nil {
			return Term{}, err
		}

		if !ldPropagates(raw) {
			ctx.revert = &parent
		}
	}

	ctx, err := ctx.applyTypeScoped(m)
	if err != nil {
		return Term{}, err
	}

	subj, err := p.subject(m, ctx)
	if err != nil {
		return subj, err
	}

	return subj, p.members(subj, m, ctx, graph)
}

// applyTypeScoped applies the scoped contexts of the node's @type terms in lexicographic order; they do not
// propagate into nested node objects unless the scoped context sets @propagate to true.
func (c ldContext) applyTypeScoped(m map[string]any) (ldContext, error) {
	var types []string

	for _, t := range ldList(m[jsonLDType]) {
		if s, ok := t.(string); ok {
			types = append(types, s)
		}
	}

	sort.Strings(types)

	out := c

	for _, t := range types {
		def, ok := c.terms[t]
		if !ok || def.scoped == nil {
			continue
		}

		next, err := out.extend(def.scoped)
		if err != nil {
			return c, err
		}

		if out.revert == nil && !ldScopedPropagates(def.scoped) {
			prev := c
			next.revert = &prev
		}

		out = next
	}

	return out, nil
}

func ldScopedPropagates(raw any) bool {
	for _, it := range ldList(raw) {
		if m, ok := it.(map[string]any); ok {
			if v, set := m[ldPropagate].(bool); set && v {
				return true
			}
		}
	}

	return false
}

func (p *ldParser) members(subj Term, m map[string]any, ctx ldContext, graph Term) error {
	for _, k := range ldSortedKeys(m) {
		v := m[k]

		var err error

		switch kw := ctx.keyword(k); {
		case kw == jsonLDID || kw == jsonLDContextKey || kw == ldIndex:
		case kw == ldIncluded:
			err = p.included(v, ctx, graph)
		case kw == jsonLDGraph:
			err = p.graphContent(v, ctx, subj, m)
		case kw == jsonLDType:
			for _, t := range ldList(v) {
				s, _ := t.(string)
				p.emit(subj, IRI(RDFType), IRI(ctx.expandIRI(s, true)), graph)
			}
		case kw == ldReverse:
			err = p.reverse(subj, v, ctx, graph)
		case kw == ldNest:
			err = p.nest(subj, v, ctx, graph)
		case strings.HasPrefix(kw, "@"):
			err = fmt.Errorf("%w: unsupported keyword %s", errJSONLD, k)
		default:
			err = p.property(subj, k, v, ctx, graph)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (p *ldParser) included(v any, ctx ldContext, graph Term) error {
	for _, e := range ldList(v) {
		em, ok := e.(map[string]any)
		if !ok {
			return fmt.Errorf("%w: @included values must be node objects", errJSONLD)
		}

		if _, isValue := em[jsonLDValue]; isValue {
			return fmt.Errorf("%w: @included values must be node objects", errJSONLD)
		}

		if _, err := p.node(em, ctx, graph); err != nil {
			return err
		}
	}

	return nil
}

func (p *ldParser) nest(subj Term, v any, ctx ldContext, graph Term) error {
	for _, e := range ldList(v) {
		nm, ok := e.(map[string]any)
		if !ok {
			return fmt.Errorf("%w: @nest values must be objects", errJSONLD)
		}

		inner := ctx

		if raw, has := nm[jsonLDContextKey]; has {
			var err error
			if inner, err = ctx.extend(raw); err != nil {
				return err
			}
		}

		if _, hasID := nm[jsonLDID]; hasID {
			return fmt.Errorf("%w: @nest object must not contain @id", errJSONLD)
		}

		if err := p.members(subj, nm, inner, graph); err != nil {
			return err
		}
	}

	return nil
}

func (p *ldParser) reverse(subj Term, v any, ctx ldContext, graph Term) error {
	rm, ok := v.(map[string]any)
	if !ok {
		return fmt.Errorf("%w: @reverse must be an object", errJSONLD)
	}

	for _, k := range ldSortedKeys(rm) {
		def := ctx.terms[k]
		if def.reverse {
			return fmt.Errorf("%w: @reverse member %q is itself a reverse property", errJSONLD, k)
		}

		pred := ctx.expandIRI(k, true)
		if !strings.Contains(pred, ":") || strings.HasPrefix(pred, "_:") {
			continue
		}

		if err := p.emitReverse(subj, IRI(pred), ldFlatten(ldList(rm[k])), def, ctx, graph); err != nil {
			return err
		}
	}

	return nil
}

func (p *ldParser) emitReverse(subj, pred Term, items []any, def ldDef, ctx ldContext, graph Term) error {
	for _, e := range items {
		if e == nil {
			continue
		}

		o, err := p.object(e, def, ctx, graph)
		if err != nil {
			return err
		}

		if o.Kind == KindLiteral {
			return fmt.Errorf("%w: a reverse property value must be a node", errJSONLD)
		}

		p.emit(o, pred, subj, graph)
	}

	return nil
}

func (p *ldParser) subject(m map[string]any, ctx ldContext) (Term, error) {
	if raw, ok := ldKeyword(m, ctx, jsonLDID); ok {
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

func (c ldContext) withScoped(def ldDef) (ldContext, error) {
	if def.scoped == nil {
		return c, nil
	}

	return c.extend(def.scoped)
}

func (p *ldParser) jsonProperty(subj Term, pred string, v any, graph Term) error {
	o, err := ldJSONLiteral(v)
	if err != nil {
		return err
	}

	p.emit(subj, IRI(pred), o, graph)

	return nil
}

func (p *ldParser) property(subj Term, key string, v any, ctx ldContext, graph Term) error {
	def := ctx.terms[key]
	pred := ctx.expandIRI(key, true)

	if !strings.Contains(pred, ":") || strings.HasPrefix(pred, "_:") {
		return nil
	}

	ctx, err := ctx.withScoped(def)
	if err != nil {
		return err
	}

	if def.typ == ldJSONType && !def.reverse && !def.cont.list {
		return p.jsonProperty(subj, pred, v, graph)
	}

	items, err := p.valueItems(v, def, ctx)
	if err != nil {
		return err
	}

	if def.reverse {
		return p.emitReverse(subj, IRI(pred), items, def, ctx, graph)
	}

	if def.cont.list {
		head, lerr := p.list(ldList(v), def, ctx, graph)
		if lerr != nil {
			return lerr
		}

		p.emit(subj, IRI(pred), head, graph)

		return nil
	}

	for _, e := range items {
		if e == nil {
			continue
		}

		o, oerr := p.propertyObject(e, def, ctx, graph)
		if oerr != nil {
			return oerr
		}

		p.emit(subj, IRI(pred), o, graph)
	}

	return nil
}

// valueItems flattens a property value, unpacking language, index, id and type maps into plain items.
func (p *ldParser) valueItems(v any, def ldDef, ctx ldContext) ([]any, error) {
	m, isMap := v.(map[string]any)
	if !isMap {
		return ldFlatten(ldList(v)), nil
	}

	switch {
	case def.cont.language:
		return ldLanguageMap(m)
	case def.cont.index, def.cont.id, def.cont.typ:
		return ldIndexed(m, def, ctx)
	}

	return ldFlatten(ldList(v)), nil
}

func ldLanguageMap(m map[string]any) ([]any, error) {
	var out []any

	for _, k := range ldSortedKeys(m) {
		for _, e := range ldList(m[k]) {
			if e == nil {
				continue
			}

			s, ok := e.(string)
			if !ok {
				return nil, fmt.Errorf("%w: language map values must be strings", errJSONLD)
			}

			if k == ldNone {
				out = append(out, s)
			} else {
				out = append(out, map[string]any{jsonLDValue: s, jsonLDLanguage: k})
			}
		}
	}

	return out, nil
}

func ldIndexed(m map[string]any, def ldDef, ctx ldContext) ([]any, error) {
	var out []any

	for _, k := range ldSortedKeys(m) {
		for _, e := range ldFlatten(ldList(m[k])) {
			item, err := ldIndexedItem(k, e, def, ctx)
			if err != nil {
				return nil, err
			}

			out = append(out, item)
		}
	}

	return out, nil
}

func ldIndexedItem(k string, e any, def ldDef, ctx ldContext) (any, error) {
	if k == ldNone || (!def.cont.id && !def.cont.typ) {
		return e, nil
	}

	if def.cont.id {
		em, ok := e.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: id map values must be objects", errJSONLD)
		}

		return ldWith(em, jsonLDID, k, false), nil
	}

	em, ok := e.(map[string]any)
	if !ok {
		s, isStr := e.(string)
		if !isStr {
			return nil, fmt.Errorf("%w: type map values must be objects or strings", errJSONLD)
		}

		em = map[string]any{jsonLDID: ctx.expandIRI(s, false)}
	}

	return ldWith(em, jsonLDType, k, true), nil
}

// ldWith copies m with key set to val; prepend adds val to an existing list value instead of replacing it.
func ldWith(m map[string]any, key, val string, prepend bool) map[string]any {
	out := make(map[string]any, len(m)+1)
	maps.Copy(out, m)

	if _, has := out[key]; has && !prepend {
		return out
	}

	if prepend {
		out[key] = []any{val}

		if old, has := m[key]; has {
			out[key] = append([]any{val}, ldList(old)...)
		}

		return out
	}

	out[key] = val

	return out
}

// propertyObject builds one property value, wrapping it as a named graph for @graph containers.
func (p *ldParser) propertyObject(e any, def ldDef, ctx ldContext, graph Term) (Term, error) {
	if def.cont.graph {
		if em, ok := e.(map[string]any); !ok || (em[jsonLDGraph] == nil && em[jsonLDValue] == nil) {
			return p.graphValue(map[string]any{jsonLDGraph: e}, ctx)
		}
	}

	return p.object(e, def, ctx, graph)
}

// graphValue stores gm's @graph contents in a named graph (blank when it has no @id) and returns the graph name.
func (p *ldParser) graphValue(gm map[string]any, ctx ldContext) (Term, error) {
	inner := ctx

	if raw, has := gm[jsonLDContextKey]; has {
		var err error
		if inner, err = ctx.extend(raw); err != nil {
			return Term{}, err
		}
	}

	name, err := p.subject(gm, inner)
	if err != nil {
		return name, err
	}

	for _, e := range ldFlatten(ldList(gm[jsonLDGraph])) {
		obj, ok := e.(map[string]any)
		if !ok {
			return name, fmt.Errorf("%w: @graph members must be objects", errJSONLD)
		}

		if _, nerr := p.node(obj, inner, name); nerr != nil {
			return name, nerr
		}
	}

	return name, nil
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
	if def.typ == ldJSONType {
		return ldJSONLiteral(v)
	}

	switch t := v.(type) {
	case string:
		switch def.typ {
		case jsonLDID:
			return p.idTerm(ctx.expandIRI(t, false)), nil
		case "@vocab":
			return p.idTerm(ctx.expandIRI(t, true)), nil
		case "", ldNone:
			if lang := ldLang(def, ctx); lang != "" {
				return LangLiteral(t, lang), nil
			}

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

func ldLang(def ldDef, ctx ldContext) string {
	if def.hasLang {
		return def.lang
	}

	return ctx.lang
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

	if ldIsGraphObject(m) {
		return p.graphValue(m, ctx)
	}

	return p.node(m, ctx, graph)
}

func ldIsGraphObject(m map[string]any) bool {
	if _, ok := m[jsonLDGraph]; !ok {
		return false
	}

	for k := range m {
		switch k {
		case jsonLDGraph, jsonLDID, jsonLDContextKey, ldIndex:
		default:
			return false
		}
	}

	return true
}

func ldValueObject(raw any, m map[string]any, ctx ldContext) (Term, error) {
	if err := ldCheckDirection(m); err != nil {
		return Term{}, err
	}

	lang, _ := m[jsonLDLanguage].(string)
	dt, _ := m[jsonLDType].(string)

	if dt == ldJSONType {
		return ldJSONLiteral(raw)
	}

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
