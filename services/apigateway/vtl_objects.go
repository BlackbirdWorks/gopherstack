package apigateway

import (
	"encoding/base64"
	"math"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// vtlObject is a host object exposed to templates ($input, $util).
type vtlObject interface {
	property(name string) (any, bool)
	call(name string, args []any) (any, bool)
}

func vtlGlobals(ctx VTLContext) map[string]any {
	stage := stringMapToVTL(ctx.StageVariables)
	stage.emptyForMissing = true

	return map[string]any{
		"input":          &vtlInput{ctx: ctx},
		"util":           &vtlUtil{},
		"context":        buildContextMap(ctx),
		"stageVariables": stage,
	}
}

func buildContextMap(ctx VTLContext) *vtlMap {
	identity := newVTLMap()
	identity.put("sourceIp", ctx.SourceIP)
	identity.put("userAgent", ctx.UserAgent)

	authorizer := newVTLMap()
	for _, k := range sortedKeys(ctx.Authorizer) {
		authorizer.put(k, fromGo(ctx.Authorizer[k]))
	}

	m := newVTLMap()
	m.put("requestId", ctx.RequestID)
	m.put("httpMethod", ctx.HTTPMethod)
	m.put("resourcePath", ctx.ResourcePath)
	m.put("path", ctx.Path)
	m.put("stage", ctx.Stage)
	m.put("apiId", ctx.APIID)
	m.put("accountId", ctx.AccountID)
	m.put("protocol", "HTTP/1.1")
	m.put("domainName", ctx.DomainName)
	m.put("identity", identity)
	m.put("authorizer", authorizer)

	return m
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// fromGo converts decoded-Go JSON values into template values.
func fromGo(v any) any {
	switch t := v.(type) {
	case map[string]any:
		m := newVTLMap()
		for _, k := range sortedKeys(t) {
			m.put(k, fromGo(t[k]))
		}

		return m
	case map[string]string:
		return stringMapToVTL(t)
	case []any:
		l := &vtlList{items: make([]any, len(t))}
		for i, e := range t {
			l.items[i] = fromGo(e)
		}

		return l
	case float64:
		if t == float64(int64(t)) {
			return int64(t)
		}

		return t
	case int:
		return int64(t)
	}

	return v
}

type vtlInput struct {
	root   any
	ctx    VTLContext
	parsed bool
}

func (in *vtlInput) property(name string) (any, bool) {
	if name == exportKeyBody {
		return in.ctx.Body, true
	}

	return nil, false
}

func (in *vtlInput) rootValue() any {
	if !in.parsed {
		in.parsed = true

		if v, err := parseOrderedJSON(in.ctx.Body); err == nil {
			in.root = v
		}
	}

	return in.root
}

func (in *vtlInput) call(name string, args []any) (any, bool) {
	switch name {
	case "json":
		return encodeVTLJSON(evalJSONPath(in.rootValue(), argString(args, 0))), true
	case "path":
		return evalJSONPath(in.rootValue(), argString(args, 0)), true
	case "params":
		return in.params(args), true
	case exportKeyBody:
		return in.ctx.Body, true
	}

	return nil, false
}

func (in *vtlInput) params(args []any) any {
	if len(args) == 0 {
		m := newVTLMap()
		m.put("path", stringMapToVTL(in.ctx.PathParams))
		m.put("querystring", stringMapToVTL(in.ctx.QueryParams))
		m.put("header", stringMapToVTL(in.ctx.Headers))

		return m
	}

	key := javaString(args[0])

	if v, ok := in.ctx.PathParams[key]; ok {
		return v
	}

	if v, ok := in.ctx.QueryParams[key]; ok {
		return v
	}

	for k, v := range in.ctx.Headers {
		if strings.EqualFold(k, key) {
			return v
		}
	}

	return ""
}

type vtlUtil struct{}

func (*vtlUtil) property(string) (any, bool) { return nil, false }

func (*vtlUtil) call(name string, args []any) (any, bool) {
	arg := argString(args, 0)

	switch name {
	case "escapeJavaScript":
		return escapeJavaScript(arg), true
	case "urlEncode":
		return url.QueryEscape(arg), true
	case "urlDecode":
		decoded, err := url.QueryUnescape(arg)
		if err != nil {
			return arg, true
		}

		return decoded, true
	case "base64Encode":
		return base64.StdEncoding.EncodeToString([]byte(arg)), true
	case "base64Decode":
		decoded, err := base64.StdEncoding.DecodeString(arg)
		if err != nil {
			return "", true
		}

		return string(decoded), true
	case "parseJson":
		v, err := parseOrderedJSON(arg)
		if err != nil {
			return nil, true
		}

		return v, true
	}

	return nil, false
}

func argString(args []any, i int) string {
	if i >= len(args) || args[i] == nil {
		return ""
	}

	return javaString(args[i])
}

func argInt(args []any, i int) (int, bool) {
	if i >= len(args) {
		return 0, false
	}

	n, ok := toInt64(args[i])

	v, err := strconv.ParseInt(strconv.FormatInt(n, 10), 10, 32)
	if err != nil {
		if n < 0 {
			v = math.MinInt32
		} else {
			v = math.MaxInt32
		}
	}

	return int(v), ok
}

// callVTLMethod dispatches a method call on any template value.
func callVTLMethod(recv any, name string, args []any) (any, bool) {
	if name == "toString" {
		return javaString(recv), true
	}

	if name == "equals" && len(args) == 1 {
		return vtlEquals(recv, args[0]), true
	}

	switch t := recv.(type) {
	case vtlObject:
		return t.call(name, args)
	case string:
		return callStringMethod(t, name, args)
	case *vtlList:
		return callListMethod(t, name, args)
	case *vtlMap:
		return callMapMethod(t, name, args)
	case int64, float64:
		return callNumberMethod(t, name)
	}

	return nil, false
}

func callNumberMethod(n any, name string) (any, bool) {
	f := numberValue(n)

	switch name {
	case "intValue", "longValue":
		return int64(f), true
	case "doubleValue", "floatValue":
		return f, true
	}

	return nil, false
}

type stringMethod func(s string, args []any) any

func lookupStringMethod(name string) (stringMethod, bool) {
	for _, group := range []func(string) stringMethod{stringCaseMethods, stringSearchMethods, stringEditMethods} {
		if fn := group(name); fn != nil {
			return fn, true
		}
	}

	return nil, false
}

func stringCaseMethods(name string) stringMethod {
	switch name {
	case "length":
		return func(s string, _ []any) any { return int64(len(s)) }
	case methodIsEmpty:
		return func(s string, _ []any) any { return s == "" }
	case "toLowerCase":
		return func(s string, _ []any) any { return strings.ToLower(s) }
	case "toUpperCase":
		return func(s string, _ []any) any { return strings.ToUpper(s) }
	case "trim":
		return func(s string, _ []any) any { return strings.TrimSpace(s) }
	case "equalsIgnoreCase":
		return func(s string, a []any) any { return strings.EqualFold(s, argString(a, 0)) }
	case "compareTo":
		return func(s string, a []any) any { return int64(strings.Compare(s, argString(a, 0))) }
	}

	return nil
}

func stringSearchMethods(name string) stringMethod {
	switch name {
	case "contains":
		return func(s string, a []any) any { return strings.Contains(s, argString(a, 0)) }
	case "startsWith":
		return func(s string, a []any) any { return strings.HasPrefix(s, argString(a, 0)) }
	case "endsWith":
		return func(s string, a []any) any { return strings.HasSuffix(s, argString(a, 0)) }
	case "indexOf":
		return func(s string, a []any) any { return int64(strings.Index(s, argString(a, 0))) }
	case "lastIndexOf":
		return func(s string, a []any) any { return int64(strings.LastIndex(s, argString(a, 0))) }
	case "matches":
		return func(s string, a []any) any {
			re, err := regexp.Compile("^(?:" + argString(a, 0) + ")$")

			return err == nil && re.MatchString(s)
		}
	}

	return nil
}

func stringEditMethods(name string) stringMethod {
	switch name {
	case "replace":
		return func(s string, a []any) any { return strings.ReplaceAll(s, argString(a, 0), argString(a, 1)) }
	case "replaceAll":
		return replaceAllString
	case "concat":
		return func(s string, a []any) any { return s + argString(a, 0) }
	case "split":
		return splitString
	case "substring":
		return substringString
	case "charAt":
		return charAtString
	}

	return nil
}

func replaceAllString(s string, a []any) any {
	re, err := regexp.Compile(argString(a, 0))
	if err != nil {
		return s
	}

	return re.ReplaceAllString(s, argString(a, 1))
}

func charAtString(s string, a []any) any {
	if i, ok := argInt(a, 0); ok && i >= 0 && i < len(s) {
		return string(s[i])
	}

	return nil
}

func splitString(s string, a []any) any {
	re, err := regexp.Compile(argString(a, 0))
	if err != nil {
		return &vtlList{items: []any{s}}
	}

	parts := re.Split(s, -1)
	l := &vtlList{items: make([]any, len(parts))}

	for i, p := range parts {
		l.items[i] = p
	}

	return l
}

func substringString(s string, a []any) any {
	from, ok := argInt(a, 0)
	if !ok || from < 0 || from > len(s) {
		return nil
	}

	to := len(s)
	if t, hasTo := argInt(a, 1); hasTo {
		if t < from || t > len(s) {
			return nil
		}

		to = t
	}

	return s[from:to]
}

func callStringMethod(s, name string, args []any) (any, bool) {
	fn, ok := lookupStringMethod(name)
	if !ok {
		return nil, false
	}

	return fn(s, args), true
}

func callListMethod(l *vtlList, name string, args []any) (any, bool) {
	switch name {
	case "size":
		return int64(len(l.items)), true
	case methodIsEmpty:
		return len(l.items) == 0, true
	case "get":
		if i, ok := argInt(args, 0); ok && i >= 0 && i < len(l.items) {
			return l.items[i], true
		}

		return nil, true
	case "add":
		if len(args) > 0 {
			l.items = append(l.items, args[0])
		}

		return true, true
	case "contains":
		for _, e := range l.items {
			if len(args) > 0 && vtlEquals(e, args[0]) {
				return true, true
			}
		}

		return false, true
	}

	return nil, false
}

func callMapMethod(m *vtlMap, name string, args []any) (any, bool) {
	key := argString(args, 0)

	switch name {
	case "size":
		return int64(len(m.keys)), true
	case methodIsEmpty:
		return len(m.keys) == 0, true
	case "get":
		v, _ := m.get(key)

		return v, true
	case "containsKey":
		_, ok := m.get(key)

		return ok, true
	case "put":
		var old any
		if len(args) > 1 {
			old, _ = m.get(key)
			m.put(key, args[1])
		}

		return old, true
	case "remove":
		return m.remove(key), true
	case "keySet":
		l := &vtlList{items: make([]any, len(m.keys))}
		for i, k := range m.keys {
			l.items[i] = k
		}

		return l, true
	case "values":
		return &vtlList{items: iterableItems(m)}, true
	case "entrySet":
		l := &vtlList{}

		for _, k := range m.keys {
			e := newVTLMap()
			e.put("key", k)
			e.put("value", m.items[k])
			l.items = append(l.items, e)
		}

		return l, true
	case "getKey", "getValue":
		v, _ := m.get(strings.ToLower(strings.TrimPrefix(name, "get")))

		return v, true
	}

	return nil, false
}

var jsonPathToken = regexp.MustCompile(`^(\.\.|\.)?(\*|[A-Za-z0-9_$@-]+)|^\[('([^']*)'|"([^"]*)"|-?\d+|\*)\]`)

// evalJSONPath resolves a JSONPath against a decoded document. Definite paths
// return the value (nil when absent); wildcard and recursive paths return a list.
func evalJSONPath(root any, path string) any {
	path = strings.TrimSpace(path)
	if path == "" || path == "$" {
		return root
	}

	if !strings.HasPrefix(path, "$") {
		path = "$." + path
	}

	rest := path[1:]
	current := []any{root}
	multi := false

	for rest != "" {
		loc := jsonPathToken.FindStringSubmatchIndex(rest)
		if loc == nil {
			return nil
		}

		tok := rest[loc[0]:loc[1]]
		rest = rest[loc[1]:]

		var next []any

		next, multi = stepJSONPath(current, tok, multi)
		current = next
	}

	if multi {
		return &vtlList{items: current}
	}

	if len(current) == 0 {
		return nil
	}

	return current[0]
}

func stepJSONPath(current []any, tok string, multi bool) ([]any, bool) {
	var out []any

	switch {
	case strings.HasPrefix(tok, ".."):
		name := strings.TrimPrefix(tok, "..")

		for _, c := range current {
			recursiveCollect(c, name, &out)
		}

		return out, true
	case tok == ".*" || tok == "[*]":
		for _, c := range current {
			out = append(out, iterableItems(c)...)
		}

		return out, true
	case strings.HasPrefix(tok, "["):
		inner := tok[1 : len(tok)-1]
		inner = strings.Trim(inner, `'"`)

		for _, c := range current {
			if v, ok := selectMember(c, inner, tok); ok {
				out = append(out, v)
			}
		}

		return out, multi
	}

	name := strings.TrimPrefix(tok, ".")

	for _, c := range current {
		if v, ok := selectMember(c, name, ""); ok {
			out = append(out, v)
		}
	}

	return out, multi
}

func selectMember(c any, key, bracketTok string) (any, bool) {
	switch t := c.(type) {
	case *vtlMap:
		return t.get(key)
	case *vtlList:
		if bracketTok == "" {
			return nil, false
		}

		i, err := strconv.Atoi(key)
		if err != nil {
			return nil, false
		}

		if i < 0 {
			i += len(t.items)
		}

		if i >= 0 && i < len(t.items) {
			return t.items[i], true
		}
	}

	return nil, false
}

func recursiveCollect(v any, name string, out *[]any) {
	switch t := v.(type) {
	case *vtlMap:
		for _, k := range t.keys {
			if name == "*" || k == name {
				*out = append(*out, t.items[k])
			}

			recursiveCollect(t.items[k], name, out)
		}
	case *vtlList:
		for _, e := range t.items {
			recursiveCollect(e, name, out)
		}
	}
}

const methodIsEmpty = "isEmpty"
