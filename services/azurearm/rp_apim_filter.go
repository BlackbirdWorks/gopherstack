package azurearm

import (
	"slices"
	"strconv"
	"strings"
)

// apimFilterField describes a $filter field: strOnly fields take the string operators and functions, the others
// (booleans) take eq and ne only (learn.microsoft.com/rest/api/apimanagement/apis/list-by-service, api-operation/
// list-by-api and product-api/list-by-product, $filter tables).
type apimFilterField struct{ boolean, eqOnly, noFunc bool }

//nolint:gochecknoglobals // static read-only list
var apimCompareOps = []string{"eq", "ne", "gt", "ge", "lt", "le"}

const (
	apimFieldDesc      = "description"
	apimFieldDisplay   = "displayname"
	apimFilterMinSeg   = 3
	apimFilterMaxDepth = 32
	apimFilterMaxLen   = 8192
)

//nolint:gochecknoglobals // static read-only tables
var (
	apimStringFields = map[string]apimFilterField{
		fieldName: {}, apimFieldDisplay: {}, apimFieldDesc: {}, "serviceurl": {}, apimCPath: {},
	}
	apimAPIFields = map[string]apimFilterField{
		fieldName:        {},
		apimFieldDisplay: {},
		apimFieldDesc:    {},
		"serviceurl":     {},
		apimCPath:        {},
		"iscurrent":      {boolean: true},
	}
	apimNameFields    = fieldsOf("name", "displayname")
	apimTopLevelLists = map[string]map[string]apimFilterField{
		"products":               fieldsOf("name", "displayname", "description", "terms", "state:eq"),
		"backends":               fieldsOf("name", "title", "url"),
		"namedvalues":            fieldsOf("displayname"),
		"tags":                   apimNameFields,
		"authorizationservers":   apimNameFields,
		"openidconnectproviders": apimNameFields,
		"groups":                 fieldsOf("name", "displayname", "description", "externalid:eq"),
		"users": fieldsOf(
			"name",
			"firstname",
			"lastname",
			"email",
			"state:eq",
			"registrationdate:nofunc",
			"note",
		),
		sbSubscriptionsType: fieldsOf("name", "displayname", "statecomment", "ownerid", "scope", "userid", "productid",
			"state:eq"),
		"loggers":         fieldsOf("name", "description", "loggertype:eq", "resourceid"),
		"policyfragments": fieldsOf("name", "description", "value"),
	}
	apimOperationFields = map[string]apimFilterField{
		fieldName: {}, apimFieldDisplay: {}, apimCMethod: {}, apimFieldDesc: {}, "urltemplate": {},
	}
)

// fieldsOf builds a field table from the SDK $filter tables: name:eq takes eq only, name:nofunc no functions.
func fieldsOf(specs ...string) map[string]apimFilterField {
	out := make(map[string]apimFilterField, len(specs))

	for _, sp := range specs {
		name, mod, _ := strings.Cut(sp, ":")
		out[name] = apimFilterField{eqOnly: mod == "eq", noFunc: mod == "eq" || mod == "nofunc"}
	}

	return out
}

func apimFilterFieldsFor(path string) map[string]apimFilterField {
	segs := strings.Split(strings.Trim(strings.ToLower(path), "/"), "/")
	if len(segs) < apimFilterMinSeg {
		return nil
	}

	last, parent := segs[len(segs)-1], segs[len(segs)-apimFilterMinSeg]

	if len(segs) == apimMinSegs+1 {
		if t, ok := apimTopLevelLists[last]; ok {
			return t
		}
	}

	switch {
	case last == apimCApis && parent == "products":
		return apimStringFields
	case last == apimCApis:
		return apimAPIFields
	case last == "operations" && parent == apimCApis:
		return apimOperationFields
	}

	return nil
}

type apimTok struct {
	text string
	str  bool
}

func apimLex(s string) ([]apimTok, error) {
	var out []apimTok

	for i := 0; i < len(s); {
		switch s[i] {
		case ' ':
			i++
		case '(', ')', ',':
			out = append(out, apimTok{text: string(s[i])})
			i++
		case '\'':
			lit, next, err := apimLexString(s, i+1)
			if err != nil {
				return nil, err
			}

			out = append(out, apimTok{text: lit, str: true})
			i = next
		default:
			j := i
			for j < len(s) && !strings.ContainsRune(" (),'", rune(s[j])) {
				j++
			}

			out = append(out, apimTok{text: s[i:j]})
			i = j
		}
	}

	return out, nil
}

// apimLexString reads a quoted literal whose opening quote precedes i; ” stands for one quote.
func apimLexString(s string, i int) (string, int, error) {
	var b strings.Builder

	for i < len(s) {
		if s[i] != '\'' {
			b.WriteByte(s[i])
			i++

			continue
		}

		if i+1 < len(s) && s[i+1] == '\'' {
			b.WriteByte('\'')

			i += 2

			continue
		}

		return b.String(), i + 1, nil
	}

	return "", 0, apimInvalid("The $filter has an unterminated string literal.")
}

type apimFilter struct {
	fields map[string]apimFilterField
	toks   []apimTok
	pos    int
	depth  int
}

func (f *apimFilter) peek() string {
	if f.pos < len(f.toks) && !f.toks[f.pos].str {
		return strings.ToLower(f.toks[f.pos].text)
	}

	return ""
}

func (f *apimFilter) next() (apimTok, error) {
	if f.pos >= len(f.toks) {
		return apimTok{}, apimInvalid("The $filter expression is incomplete.")
	}

	f.pos++

	return f.toks[f.pos-1], nil
}

func (f *apimFilter) expect(want string) error {
	t, err := f.next()
	if err != nil || t.str || t.text != want {
		return apimInvalid("The $filter expression expected '%s'.", want)
	}

	return nil
}

type apimPred func(item map[string]any) bool

func (f *apimFilter) or() (apimPred, error) {
	l, err := f.and()
	if err != nil {
		return nil, err
	}

	for f.peek() == "or" {
		f.pos++

		r, rerr := f.and()
		if rerr != nil {
			return nil, rerr
		}

		l = orPred(l, r)
	}

	return l, nil
}

func orPred(a, b apimPred) apimPred { return func(m map[string]any) bool { return a(m) || b(m) } }

func andPred(a, b apimPred) apimPred { return func(m map[string]any) bool { return a(m) && b(m) } }

func (f *apimFilter) and() (apimPred, error) {
	l, err := f.not()
	if err != nil {
		return nil, err
	}

	for f.peek() == "and" {
		f.pos++

		r, rerr := f.not()
		if rerr != nil {
			return nil, rerr
		}

		l = andPred(l, r)
	}

	return l, nil
}

func (f *apimFilter) not() (apimPred, error) {
	if f.peek() == "not" {
		f.pos++

		p, err := f.primary()
		if err != nil {
			return nil, err
		}

		return func(m map[string]any) bool { return !p(m) }, nil
	}

	return f.primary()
}

func (f *apimFilter) primary() (apimPred, error) {
	if f.peek() == "(" {
		f.pos++

		if f.depth++; f.depth > apimFilterMaxDepth {
			return nil, apimInvalid("The $filter expression is nested more than %d levels deep.", apimFilterMaxDepth)
		}

		p, err := f.or()
		if err != nil {
			return nil, err
		}

		f.depth--

		return p, f.expect(")")
	}

	switch name := f.peek(); name {
	case "contains", "startswith", "endswith", "substringof":
		if f.pos+1 < len(f.toks) && f.toks[f.pos+1].text == "(" {
			return f.function(name)
		}
	}

	return f.comparison()
}

func (f *apimFilter) fieldOf(tok apimTok) (string, error) {
	key := strings.ToLower(strings.TrimPrefix(strings.ToLower(tok.text), "properties/"))
	if _, ok := f.fields[key]; tok.str || !ok {
		return "", apimInvalid("The $filter field '%s' is not supported for this list.", tok.text)
	}

	return key, nil
}

func (f *apimFilter) function(name string) (apimPred, error) {
	f.pos += 2

	a, err := f.next()
	if err != nil {
		return nil, err
	}

	if err = f.expect(","); err != nil {
		return nil, err
	}

	b, err := f.next()
	if err != nil {
		return nil, err
	}

	if err = f.expect(")"); err != nil {
		return nil, err
	}

	fieldTok, lit := a, b
	if name == "substringof" {
		fieldTok, lit = b, a
	}

	field, err := f.fieldOf(fieldTok)
	if err != nil || f.fields[field].boolean || f.fields[field].noFunc || !lit.str {
		return nil, apimInvalid("The $filter function %s needs a string field and a string literal.", name)
	}

	needle := strings.ToLower(lit.text)

	return func(m map[string]any) bool {
		v := strings.ToLower(apimFieldString(m, field))

		switch name {
		case "startswith":
			return strings.HasPrefix(v, needle)
		case "endswith":
			return strings.HasSuffix(v, needle)
		}

		return strings.Contains(v, needle)
	}, nil
}

func (f *apimFilter) comparison() (apimPred, error) {
	ft, err := f.next()
	if err != nil {
		return nil, err
	}

	field, err := f.fieldOf(ft)
	if err != nil {
		return nil, err
	}

	opTok, err := f.next()
	if err != nil {
		return nil, err
	}

	op := strings.ToLower(opTok.text)
	if opTok.str || !apimOpAllowed(f.fields[field], op) {
		return nil, apimInvalid("The $filter operator '%s' is not supported for field '%s'.", opTok.text, ft.text)
	}

	lit, err := f.next()
	if err != nil {
		return nil, err
	}

	if f.fields[field].boolean {
		want, perr := strconv.ParseBool(lit.text)
		if perr != nil || lit.str {
			return nil, apimInvalid("The $filter field '%s' compares with true or false.", ft.text)
		}

		return func(m map[string]any) bool { return apimFieldBool(m, field) == want == (op == "eq") }, nil
	}

	if !lit.str {
		return nil, apimInvalid("The $filter field '%s' compares with a string literal.", ft.text)
	}

	want := strings.ToLower(lit.text)

	return func(m map[string]any) bool { return compareFold(strings.ToLower(apimFieldString(m, field)), want, op) }, nil
}

func compareFold(have, want, op string) bool {
	c := strings.Compare(have, want)

	switch op {
	case "eq":
		return c == 0
	case "ne":
		return c != 0
	case "gt":
		return c > 0
	case "ge":
		return c >= 0
	case "lt":
		return c < 0
	}

	return c <= 0
}

func apimFieldString(m map[string]any, field string) string {
	if field == fieldName {
		return strOf(m, fieldName)
	}

	for k, v := range mapOf(m[fieldProperties]) {
		if strings.EqualFold(k, field) {
			s, _ := v.(string)

			return s
		}
	}

	return ""
}

func apimFieldBool(m map[string]any, field string) bool {
	for k, v := range mapOf(m[fieldProperties]) {
		if strings.EqualFold(k, field) {
			b, _ := v.(bool)

			return b
		}
	}

	return false
}

// applyAPIMFilter keeps the items of a list that satisfy the $filter expression; an empty expression keeps all.
func applyAPIMFilter(path, expr string, items []any) ([]any, error) {
	if strings.TrimSpace(expr) == "" {
		return items, nil
	}

	fields := apimFilterFieldsFor(path)
	if fields == nil {
		return items, nil
	}

	if len(expr) > apimFilterMaxLen {
		return nil, apimInvalid("The $filter expression is longer than %d characters.", apimFilterMaxLen)
	}

	toks, err := apimLex(expr)
	if err != nil {
		return nil, err
	}

	f := &apimFilter{toks: toks, fields: fields}

	pred, err := f.or()
	if err != nil {
		return nil, err
	}

	if f.pos != len(f.toks) {
		return nil, apimInvalid("The $filter expression has unexpected text after '%s'.", f.toks[f.pos-1].text)
	}

	out := make([]any, 0, len(items))

	for _, it := range items {
		if m, ok := it.(map[string]any); ok && pred(m) {
			out = append(out, it)
		}
	}

	return out, nil
}

func apimOpAllowed(f apimFilterField, op string) bool {
	switch {
	case !slices.Contains(apimCompareOps, op):
		return false
	case f.boolean:
		return op == "eq" || op == "ne"
	case f.eqOnly:
		return op == "eq"
	}

	return true
}
