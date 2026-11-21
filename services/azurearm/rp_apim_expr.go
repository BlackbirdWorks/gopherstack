package azurearm

import (
	"cmp"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

const (
	apxString            = "string"
	apxTrueLit           = "true"
	apxLong              = "long"
	apxScheme            = "Scheme"
	apxInt               = "int"
	apxNull              = "null"
	apxQueryString       = "QueryString"
	apxStartsWith        = "StartsWith"
	apxTrim              = "Trim"
	apimExprKeyDefault   = 2
	apimExprGenericClose = 2
	apimExprGenericLen   = 3
	apimExprMaxArgs      = 8
	apimExprMaxLen       = 4096
	apimExprMaxDepth     = 32
	apimExprMaxNodes     = 512
	apimExprMaxString    = 1 << 20
	apimExprTrue         = "True"
	apimExprFalse        = "False"
)

var (
	errAPIMExprSyntax  = errors.New("syntax error")
	errAPIMExprRuntime = errors.New("expression evaluation failed")
)

const (
	apxTypeBool          = "bool"
	apxVariables         = "Variables"
	apxUser              = "User"
	apxURL               = "Url"
	apxPort              = "Port"
	apxURLTemplate       = "UrlTemplate"
	apxOrdinalIC         = "OrdinalIgnoreCase"
	apxEndsWith          = "EndsWith"
	apxToString          = "ToString"
	apxContext           = "context"
	apxID                = "Id"
	apxHost              = "Host"
	apxAPI               = "Api"
	apxLastError         = "LastError"
	apxHeaders           = "Headers"
	apxName              = "Name"
	apxMessage           = "Message"
	apxLength            = "Length"
	apxBody              = "Body"
	apxContains          = "Contains"
	apxParse             = "Parse"
	apxDateTime          = "DateTime"
	apxRequest           = "Request"
	apxResponse          = "Response"
	apxSubscription      = "Subscription"
	apxProduct           = "Product"
	apxOperation         = "Operation"
	apxTimestamp         = "Timestamp"
	apxRequestID         = "RequestId"
	apxStatusCode        = "StatusCode"
	apxStatusReason      = "StatusReason"
	apxPath              = "Path"
	apxMethod            = "Method"
	apxSource            = "Source"
	apxReason            = "Reason"
	apxScope             = "Scope"
	apxSection           = "Section"
	apxPolicyID          = "PolicyId"
	apxIPAddress         = "IpAddress"
	apxMatchedParameters = "MatchedParameters"
	apxOriginalURL       = "OriginalUrl"
	apxQuery             = "Query"
	apxSecond            = "Second"
)

type apimExprNode struct {
	val  any
	name string
	op   string
	typ  string
	args []*apimExprNode
	kind byte
	opt  bool
}

const (
	nLit byte = iota
	nIdent
	nMember
	nCall
	nIndex
	nUnary
	nBinary
	nTernary
	nCast
)

type apimExprTok struct {
	val  any
	text string
	kind byte
}

const (
	tIdent byte = iota
	tInt
	tStr
	tOp
	tEOF
)

func apimExprLex(src string) ([]apimExprTok, error) {
	var toks []apimExprTok

	rs := []rune(src)

	for i := 0; i < len(rs); {
		if unicode.IsSpace(rs[i]) {
			i++

			continue
		}

		tok, next, err := apimExprLexTok(rs, i)
		if err != nil {
			return nil, err
		}

		if toks = append(toks, tok); len(toks) > apimExprMaxNodes*2 {
			return nil, fmt.Errorf("%w: expression is too long", errAPIMExprSyntax)
		}

		i = next
	}

	return append(toks, apimExprTok{kind: tEOF}), nil
}

func apimExprIdentRune(r rune, first bool) bool {
	return r == '_' || unicode.IsLetter(r) || (!first && unicode.IsDigit(r))
}

func apimExprLexTok(rs []rune, i int) (apimExprTok, int, error) {
	c := rs[i]

	switch {
	case apimExprIdentRune(c, true):
		j := i
		for j < len(rs) && apimExprIdentRune(rs[j], false) {
			j++
		}

		return apimExprTok{kind: tIdent, text: string(rs[i:j])}, j, nil
	case c >= '0' && c <= '9':
		j := i
		for j < len(rs) && rs[j] >= '0' && rs[j] <= '9' {
			j++
		}

		n, err := strconv.ParseInt(string(rs[i:j]), 10, 64)
		if err != nil || (j < len(rs) && (rs[j] == '.' || unicode.IsLetter(rs[j]))) {
			return apimExprTok{}, 0, fmt.Errorf("%w: only integer literals are supported", errAPIMExprSyntax)
		}

		return apimExprTok{kind: tInt, val: n, text: string(rs[i:j])}, j, nil
	case c == '"':
		str, next, err := apimExprLexString(rs, i+1)

		return apimExprTok{kind: tStr, val: str}, next, err
	}

	op, ok := apimExprLexOp(rs, i)
	if !ok {
		return apimExprTok{}, 0, fmt.Errorf("%w: unexpected character %q", errAPIMExprSyntax, string(c))
	}

	return apimExprTok{kind: tOp, text: op}, i + len(op), nil
}

func apimExprLexOp(rs []rune, i int) (string, bool) {
	two := ""
	if i+1 < len(rs) {
		two = string(rs[i : i+2])
	}

	switch two {
	case "==", "!=", "&&", "||", "<=", ">=", "??", "?.":
		return two, true
	}

	if strings.ContainsRune("()[].,?:!<>+-*/%", rs[i]) {
		return string(rs[i]), true
	}

	return "", false
}

func apimExprLexString(rs []rune, i int) (string, int, error) {
	var sb strings.Builder

	for ; i < len(rs); i++ {
		switch rs[i] {
		case '"':
			return sb.String(), i + 1, nil
		case '\\':
			i++
			if i >= len(rs) {
				return "", 0, fmt.Errorf("%w: unterminated string", errAPIMExprSyntax)
			}

			switch rs[i] {
			case 'n':
				sb.WriteByte('\n')
			case 't':
				sb.WriteByte('\t')
			case 'r':
				sb.WriteByte('\r')
			case '\\', '"', '\'':
				sb.WriteRune(rs[i])
			default:
				return "", 0, fmt.Errorf("%w: unsupported escape \\%c", errAPIMExprSyntax, rs[i])
			}
		default:
			sb.WriteRune(rs[i])
		}
	}

	return "", 0, fmt.Errorf("%w: unterminated string", errAPIMExprSyntax)
}

type apimExprParser struct {
	toks  []apimExprTok
	pos   int
	depth int
	nodes int
}

func (p *apimExprParser) peek() apimExprTok { return p.toks[p.pos] }
func (p *apimExprParser) at(i int) apimExprTok {
	if p.pos+i < len(p.toks) {
		return p.toks[p.pos+i]
	}

	return apimExprTok{kind: tEOF}
}

func (p *apimExprParser) isOp(s string) bool {
	t := p.peek()

	return t.kind == tOp && t.text == s
}

func (p *apimExprParser) eat(s string) bool {
	if p.isOp(s) {
		p.pos++

		return true
	}

	return false
}

func (p *apimExprParser) expect(s string) error {
	if !p.eat(s) {
		return fmt.Errorf("%w: expected %q", errAPIMExprSyntax, s)
	}

	return nil
}

func (p *apimExprParser) node(n *apimExprNode) (*apimExprNode, error) {
	if p.nodes++; p.nodes > apimExprMaxNodes {
		return nil, fmt.Errorf("%w: expression is too complex", errAPIMExprSyntax)
	}

	return n, nil
}

func (p *apimExprParser) enter() error {
	if p.depth++; p.depth > apimExprMaxDepth {
		return fmt.Errorf("%w: expression is nested too deeply", errAPIMExprSyntax)
	}

	return nil
}

// apimParseExpr parses the single-expression C# subset documented in PARITY.md.
func apimParseExpr(src string) (*apimExprNode, error) {
	if len(src) > apimExprMaxLen {
		return nil, fmt.Errorf("%w: expression is longer than %d characters", errAPIMExprSyntax, apimExprMaxLen)
	}

	toks, err := apimExprLex(src)
	if err != nil {
		return nil, err
	}

	p := &apimExprParser{toks: toks}

	n, err := p.ternary()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tEOF {
		return nil, fmt.Errorf("%w: unexpected token %q", errAPIMExprSyntax, p.peek().text)
	}

	if err = apimExprCheck(n); err != nil {
		return nil, err
	}

	return n, nil
}

func (p *apimExprParser) ternary() (*apimExprNode, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}

	defer func() { p.depth-- }()

	c, err := p.binary(0)
	if err != nil {
		return nil, err
	}

	if !p.eat("?") {
		return c, nil
	}

	a, err := p.ternary()
	if err != nil {
		return nil, err
	}

	if err = p.expect(":"); err != nil {
		return nil, err
	}

	b, err := p.ternary()
	if err != nil {
		return nil, err
	}

	return p.node(&apimExprNode{kind: nTernary, args: []*apimExprNode{c, a, b}})
}

func apimExprPrec() [][]string {
	return [][]string{{"??"}, {"||"}, {"&&"}, {"==", "!="}, {"<", ">", "<=", ">="}, {"+", "-"}, {"*", "/", "%"}}
}

func (p *apimExprParser) binary(level int) (*apimExprNode, error) {
	if level >= len(apimExprPrec()) {
		return p.unary()
	}

	l, err := p.binary(level + 1)
	if err != nil {
		return nil, err
	}

	for {
		var op string

		for _, o := range apimExprPrec()[level] {
			if p.isOp(o) {
				op = o
			}
		}

		if op == "" {
			return l, nil
		}

		p.pos++

		r, rerr := p.binary(level + 1)
		if rerr != nil {
			return nil, rerr
		}

		if l, err = p.node(&apimExprNode{kind: nBinary, op: op, args: []*apimExprNode{l, r}}); err != nil {
			return nil, err
		}
	}
}

func apimExprIsCastType(s string) bool {
	switch s {
	case apxString, apxInt, apxTypeBool, apxLong, "IResponse", "Jwt":
		return true
	}

	return false
}

func (p *apimExprParser) startsOperand(t apimExprTok) bool {
	return t.kind == tIdent || t.kind == tInt || t.kind == tStr || (t.kind == tOp && (t.text == "(" || t.text == "!"))
}

func (p *apimExprParser) unary() (*apimExprNode, error) {
	if err := p.enter(); err != nil {
		return nil, err
	}

	defer func() { p.depth-- }()

	switch {
	case p.isOp("!") || p.isOp("-"):
		op := p.peek().text
		p.pos++

		x, err := p.unary()
		if err != nil {
			return nil, err
		}

		return p.node(&apimExprNode{kind: nUnary, op: op, args: []*apimExprNode{x}})
	case p.isOp("(") && p.at(1).kind == tIdent && apimExprIsCastType(p.at(1).text) &&
		p.at(apimExprGenericClose).kind == tOp && p.at(apimExprGenericClose).text == ")" &&
		p.startsOperand(p.at(apimExprGenericLen)):
		typ := p.at(1).text
		p.pos += apimExprGenericLen

		x, err := p.unary()
		if err != nil {
			return nil, err
		}

		return p.node(&apimExprNode{kind: nCast, typ: typ, args: []*apimExprNode{x}})
	}

	return p.postfix()
}

func (p *apimExprParser) postfix() (*apimExprNode, error) {
	x, err := p.primary()
	if err != nil {
		return nil, err
	}

	for {
		switch {
		case p.isOp(".") || p.isOp("?."):
			x, err = p.dotMember(x)
		case p.isOp("["):
			x, err = p.indexer(x)
		default:
			return x, nil
		}

		if err != nil {
			return nil, err
		}
	}
}

func (p *apimExprParser) dotMember(x *apimExprNode) (*apimExprNode, error) {
	opt := p.peek().text == "?."
	p.pos++

	id := p.peek()
	if id.kind != tIdent {
		return nil, fmt.Errorf("%w: member name expected", errAPIMExprSyntax)
	}

	p.pos++

	typ := ""
	if p.isOp("<") && p.at(1).kind == tIdent && p.at(apimExprGenericClose).kind == tOp &&
		p.at(apimExprGenericClose).text == ">" {
		typ = p.at(1).text
		p.pos += apimExprGenericLen
	}

	return p.member(x, id.text, typ, opt)
}

func (p *apimExprParser) indexer(x *apimExprNode) (*apimExprNode, error) {
	p.pos++

	i, err := p.ternary()
	if err != nil {
		return nil, err
	}

	if err = p.expect("]"); err != nil {
		return nil, err
	}

	return p.node(&apimExprNode{kind: nIndex, args: []*apimExprNode{x, i}})
}

func (p *apimExprParser) member(x *apimExprNode, name, typ string, opt bool) (*apimExprNode, error) {
	if !p.isOp("(") {
		if typ != "" {
			return nil, fmt.Errorf("%w: generic arguments need a call", errAPIMExprSyntax)
		}

		return p.node(&apimExprNode{kind: nMember, name: name, opt: opt, args: []*apimExprNode{x}})
	}

	p.pos++

	args := []*apimExprNode{x}

	for !p.isOp(")") {
		a, err := p.ternary()
		if err != nil {
			return nil, err
		}

		if args = append(args, a); len(args) > apimExprMaxArgs {
			return nil, fmt.Errorf("%w: too many arguments", errAPIMExprSyntax)
		}

		if !p.eat(",") {
			break
		}
	}

	if err := p.expect(")"); err != nil {
		return nil, err
	}

	return p.node(&apimExprNode{kind: nCall, name: name, typ: typ, opt: opt, args: args})
}

func (p *apimExprParser) primary() (*apimExprNode, error) {
	t := p.peek()

	switch {
	case t.kind == tInt || t.kind == tStr:
		p.pos++

		return p.node(&apimExprNode{kind: nLit, val: t.val})
	case t.kind == tIdent:
		p.pos++

		switch t.text {
		case apxTrueLit:
			return p.node(&apimExprNode{kind: nLit, val: true})
		case apimCFalse:
			return p.node(&apimExprNode{kind: nLit, val: false})
		case apxNull:
			return p.node(&apimExprNode{kind: nLit})
		}

		return p.node(&apimExprNode{kind: nIdent, name: t.text})
	case p.eat("("):
		x, err := p.ternary()
		if err != nil {
			return nil, err
		}

		return x, p.expect(")")
	}

	return nil, fmt.Errorf("%w: unexpected token %q", errAPIMExprSyntax, t.text)
}

func apimExprMemberOK(name string) bool {
	return slices.Contains([]string{
		apxRequest, apxResponse, apxVariables, apxSubscription, apxProduct, apxAPI,
		apxOperation, apxUser, apxLastError, apxRequestID, apxTimestamp, apxMethod,
		apxURL, apxOriginalURL, apxHeaders, apxMatchedParameters, apxIPAddress, apxPath,
		apxQuery, apxHost, apxPort, apxScheme, apxQueryString, apxStatusCode,
		apxStatusReason, apxID, apxName, apxURLTemplate, apxSource, apxReason,
		apxMessage, apxScope, apxSection, apxPolicyID, apxLength, "UtcNow",
		"Year", "Month", "Day", "Hour", "Minute", apxSecond,
		apxBody, "Claims", "Issuer", "Subject", "Algorithm", "Ordinal",
		apxOrdinalIC, "Empty",
	}, name)
}

func apimExprMethodOK(name string) bool {
	return slices.Contains([]string{
		apxContains, apxStartsWith, apxEndsWith, "ToLower", "ToUpper", "Equals",
		apxTrim, apxToString, "GetValueOrDefault", "ContainsKey", apxParse, "NewGuid",
		"AddSeconds", "AddMinutes", "AddHours", "AddDays", "ToLowerInvariant", "As",
		"ToUpperInvariant", "IsNullOrEmpty",
	}, name)
}

func apimExprRootOK(name string) bool {
	return slices.Contains([]string{apxContext, apxInt, "Guid", apxDateTime, apxString, "StringComparison"}, name)
}

func apimExprCheck(n *apimExprNode) error {
	switch n.kind {
	case nIdent:
		if !apimExprRootOK(n.name) {
			return fmt.Errorf("%w: the identifier '%s' is not supported", errAPIMExprSyntax, n.name)
		}
	case nMember:
		if !apimExprMemberOK(n.name) {
			return fmt.Errorf("%w: the member '%s' is not supported", errAPIMExprSyntax, n.name)
		}

		if chain, ok := apimExprChain(n); ok {
			if err := apimExprCheckChain(chain); err != nil {
				return err
			}
		}
	case nCall:
		if !apimExprMethodOK(n.name) {
			return fmt.Errorf("%w: the method '%s' is not supported", errAPIMExprSyntax, n.name)
		}

		if n.typ != "" && !apimExprIsCastType(n.typ) {
			return fmt.Errorf("%w: the type '%s' is not supported", errAPIMExprSyntax, n.typ)
		}
	}

	for _, a := range n.args {
		if err := apimExprCheck(a); err != nil {
			return err
		}
	}

	return nil
}

// apimIsExprText reports whether v is a policy expression; multi is true for the unsupported @{...} form.
func apimIsExprText(v string) (string, bool, bool) {
	t := strings.TrimSpace(v)
	if strings.HasPrefix(t, "@{") || (strings.HasPrefix(t, "@(") && !strings.HasSuffix(t, ")")) {
		return "", false, true
	}

	if strings.HasPrefix(t, "@(") && strings.HasSuffix(t, ")") {
		return t[2 : len(t)-1], true, false
	}

	return "", false, false
}

// apimExprEnv is the runtime context an expression sees.
type apimExprEnv interface {
	member(obj any, name string) (any, error)
	now() time.Time
}

type apimExprDict struct {
	get func(string) (any, bool)
}

type apimExprURL struct {
	q      map[string][]string
	path   string
	query  string
	host   string
	scheme string
	port   int64
}

type apimExprRoot struct{ name string }

type apimExprStatic struct{ name string }

type apimExprCounter struct{ n int }

func apimExprFail(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errAPIMExprRuntime, fmt.Sprintf(format, args...))
}

func apimEvalExpr(n *apimExprNode, env apimExprEnv, c *apimExprCounter) (any, error) {
	if c.n++; c.n > apimExprMaxNodes*4 {
		return nil, apimExprFail("evaluation budget exceeded")
	}

	switch n.kind {
	case nLit:
		return n.val, nil
	case nIdent:
		if n.name == apxContext {
			return apimExprRoot{name: apxContext}, nil
		}

		return apimExprStatic{name: n.name}, nil
	case nMember, nTernary, nCast:
		return apimEvalWrapped(n, env, c)
	case nCall:
		return apimEvalCall(n, env, c)
	case nIndex:
		return apimEvalIndex(n, env, c)
	case nUnary:
		return apimEvalUnary(n, env, c)
	case nBinary:
		return apimEvalBinary(n, env, c)
	}

	return nil, apimExprFail("unsupported node")
}

func apimEvalWrapped(n *apimExprNode, env apimExprEnv, c *apimExprCounter) (any, error) {
	x, err := apimEvalExpr(n.args[0], env, c)
	if err != nil {
		return nil, err
	}

	switch n.kind {
	case nMember:
		if x == nil && n.opt {
			return nil, nil //nolint:nilnil // null-conditional access
		}

		return apimExprMemberOf(x, n.name, env)
	case nCast:
		return apimExprCast(n.typ, x)
	}

	b, ok := x.(bool)
	if !ok {
		return nil, apimExprFail("the ternary condition is not a boolean")
	}

	if b {
		return apimEvalExpr(n.args[1], env, c)
	}

	return apimEvalExpr(n.args[2], env, c)
}

func apimExprCast(typ string, x any) (any, error) {
	switch typ {
	case apxString:
		if s, ok := x.(string); ok || x == nil {
			return s, nil
		}
	case apxInt, apxLong:
		if i, ok := x.(int64); ok {
			return i, nil
		}
	case apxTypeBool:
		if b, ok := x.(bool); ok {
			return b, nil
		}
	case "IResponse":
		if r, ok := x.(*apimExprResponse); ok {
			return r, nil
		}
	case "Jwt":
		if j, ok := x.(*apimExprJWT); ok {
			return j, nil
		}
	}

	return nil, apimExprFail("cannot cast the value to %s", typ)
}

func apimExprMemberOf(x any, name string, env apimExprEnv) (any, error) {
	switch v := x.(type) {
	case apimExprStatic:
		return apimExprStaticMember(v.name, name, env)
	case string:
		if name == apxLength {
			return int64(len([]rune(v))), nil
		}
	case []string:
		if name == apxLength {
			return int64(len(v)), nil
		}
	case time.Time:
		return apimExprTimeMember(v, name)
	case *apimExprURL:
		return apimExprURLMember(v, name)
	}

	return env.member(x, name)
}

func apimExprStaticMember(typ, name string, env apimExprEnv) (any, error) {
	switch {
	case typ == apxDateTime && name == "UtcNow":
		return env.now().UTC(), nil
	case typ == "StringComparison" && (name == "Ordinal" || name == apxOrdinalIC):
		return apimExprStatic{name: name}, nil
	case typ == apxString && name == "Empty":
		return "", nil
	}

	return nil, apimExprFail("%s.%s is not supported", typ, name)
}

func apimExprTimeMember(t time.Time, name string) (any, error) {
	switch name {
	case "Year":
		return int64(t.Year()), nil
	case "Month":
		return int64(t.Month()), nil
	case "Day":
		return int64(t.Day()), nil
	case "Hour":
		return int64(t.Hour()), nil
	case "Minute":
		return int64(t.Minute()), nil
	case apxSecond:
		return int64(t.Second()), nil
	}

	return nil, apimExprFail("DateTime.%s is not supported", name)
}

func apimExprURLMember(u *apimExprURL, name string) (any, error) {
	switch name {
	case apxPath:
		return u.path, nil
	case apxHost:
		return u.host, nil
	case apxScheme:
		return u.scheme, nil
	case apxPort:
		return u.port, nil
	case apxQueryString:
		return u.query, nil
	case apxQuery:
		return &apimExprDict{
			get: func(k string) (any, bool) {
				v, ok := u.q[k]

				return v, ok
			},
		}, nil
	}

	return nil, apimExprFail("Url.%s is not supported", name)
}

func apimEvalIndex(n *apimExprNode, env apimExprEnv, c *apimExprCounter) (any, error) {
	x, err := apimEvalExpr(n.args[0], env, c)
	if err != nil {
		return nil, err
	}

	i, err := apimEvalExpr(n.args[1], env, c)
	if err != nil {
		return nil, err
	}

	switch v := x.(type) {
	case *apimExprDict:
		k, ok := i.(string)
		if !ok {
			return nil, apimExprFail("a dictionary key must be a string")
		}

		val, found := v.get(k)
		if !found {
			return nil, apimExprFail("the key '%s' was not found", k)
		}

		return val, nil
	case []string:
		idx, ok := i.(int64)
		if !ok || idx < 0 || int(idx) >= len(v) {
			return nil, apimExprFail("index out of range")
		}

		return v[idx], nil
	}

	return nil, apimExprFail("the value cannot be indexed")
}

func apimEvalUnary(n *apimExprNode, env apimExprEnv, c *apimExprCounter) (any, error) {
	x, err := apimEvalExpr(n.args[0], env, c)
	if err != nil {
		return nil, err
	}

	switch v := x.(type) {
	case bool:
		if n.op == "!" {
			return !v, nil
		}
	case int64:
		if n.op == "-" {
			return -v, nil
		}
	}

	return nil, apimExprFail("operator %s cannot be applied to the operand", n.op)
}

func apimExprString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case int64:
		return strconv.FormatInt(t, 10)
	case bool:
		if t {
			return apimExprTrue
		}

		return apimExprFalse
	case time.Time:
		return t.UTC().Format("01/02/2006 3:04:05 PM")
	case []string:
		return strings.Join(t, ",")
	}

	return fmt.Sprint(v)
}

func apimEvalBinary(n *apimExprNode, env apimExprEnv, c *apimExprCounter) (any, error) {
	l, err := apimEvalExpr(n.args[0], env, c)
	if err != nil {
		return nil, err
	}

	switch n.op {
	case "&&", "||":
		lb, ok := l.(bool)
		if !ok {
			return nil, apimExprFail("operator %s needs boolean operands", n.op)
		}

		if (n.op == "&&" && !lb) || (n.op == "||" && lb) {
			return lb, nil
		}

		r, rerr := apimEvalExpr(n.args[1], env, c)
		if rerr != nil {
			return nil, rerr
		}

		rb, ok := r.(bool)
		if !ok {
			return nil, apimExprFail("operator %s needs boolean operands", n.op)
		}

		return rb, nil
	case "??":
		if l != nil {
			return l, nil
		}

		return apimEvalExpr(n.args[1], env, c)
	}

	r, err := apimEvalExpr(n.args[1], env, c)
	if err != nil {
		return nil, err
	}

	return apimExprBinOp(n.op, l, r)
}

func apimExprBinOp(op string, l, r any) (any, error) {
	switch op {
	case "==":
		return apimExprEqual(l, r)
	case "!=":
		eq, err := apimExprEqual(l, r)
		if err != nil {
			return nil, err
		}

		same, _ := eq.(bool)

		return !same, nil
	case "+":
		if _, ls := l.(string); ls {
			return apimExprConcat(l, r)
		}

		if _, rs := r.(string); rs {
			return apimExprConcat(l, r)
		}
	}

	if res, ok, err := apimExprOrdered(op, l, r); ok {
		return res, err
	}

	li, lok := l.(int64)
	ri, rok := r.(int64)

	if !lok || !rok {
		return nil, apimExprFail("operator %s cannot be applied to the operands", op)
	}

	return apimExprIntOp(op, li, ri)
}

func apimExprConcat(l, r any) (any, error) {
	s := apimExprString(l) + apimExprString(r)
	if len(s) > apimExprMaxString {
		return nil, apimExprFail("string result is too large")
	}

	return s, nil
}

// apimExprOrdered compares two times, or two strings with a relational operator.
func apimExprOrdered(op string, l, r any) (any, bool, error) {
	if lt, ok := l.(time.Time); ok {
		if rt, ok2 := r.(time.Time); ok2 {
			res, err := apimExprCompare(op, lt.Compare(rt))

			return res, true, err
		}
	}

	ls, ok := l.(string)
	rs, ok2 := r.(string)

	if ok && ok2 && (op == "<" || op == ">" || op == "<=" || op == ">=") {
		res, err := apimExprCompare(op, strings.Compare(ls, rs))

		return res, true, err
	}

	return nil, false, nil
}

func apimExprIntOp(op string, li, ri int64) (any, error) {
	switch op {
	case "+":
		return li + ri, nil
	case "-":
		return li - ri, nil
	case "*":
		return li * ri, nil
	case "/", "%":
		if ri == 0 {
			return nil, apimExprFail("division by zero")
		}

		if op == "/" {
			return li / ri, nil
		}

		return li % ri, nil
	}

	return apimExprCompare(op, cmp.Compare(li, ri))
}

func apimExprCompare(op string, cmp int) (any, error) {
	switch op {
	case "<":
		return cmp < 0, nil
	case ">":
		return cmp > 0, nil
	case "<=":
		return cmp <= 0, nil
	case ">=":
		return cmp >= 0, nil
	}

	return nil, apimExprFail("operator %s is not supported", op)
}

func apimExprEqual(l, r any) (any, error) {
	if l == nil || r == nil {
		return l == nil && r == nil, nil
	}

	switch lv := l.(type) {
	case string:
		if rv, ok := r.(string); ok {
			return lv == rv, nil
		}
	case int64:
		if rv, ok := r.(int64); ok {
			return lv == rv, nil
		}
	case bool:
		if rv, ok := r.(bool); ok {
			return lv == rv, nil
		}
	case time.Time:
		if rv, ok := r.(time.Time); ok {
			return lv.Equal(rv), nil
		}
	}

	return nil, apimExprFail("the operands of == have different types")
}

func apimEvalCall(n *apimExprNode, env apimExprEnv, c *apimExprCounter) (any, error) {
	recv, err := apimEvalExpr(n.args[0], env, c)
	if err != nil {
		return nil, err
	}

	if recv == nil && n.opt {
		return nil, nil //nolint:nilnil // null-conditional call
	}

	args := make([]any, 0, len(n.args)-1)

	for _, a := range n.args[1:] {
		v, aerr := apimEvalExpr(a, env, c)
		if aerr != nil {
			return nil, aerr
		}

		args = append(args, v)
	}

	if n.name == apxToString && len(args) == 0 {
		return apimExprString(recv), nil
	}

	return apimExprDispatch(recv, n, args, env)
}

func apimExprDispatch(recv any, n *apimExprNode, args []any, env apimExprEnv) (any, error) {
	switch r := recv.(type) {
	case apimExprStatic:
		return apimExprStaticCall(r.name, n.name, args)
	case string:
		return apimExprStringCall(r, n.name, args)
	case *apimExprDict:
		return apimExprDictCall(r, n, args)
	case time.Time:
		return apimExprTimeCall(r, n.name, args)
	case apimExprBody:
		if n.name == "As" && n.typ == apxString {
			return string(r.b), nil
		}
	case []string:
		if n.name == apxContains && len(args) == 1 {
			s, _ := args[0].(string)

			return slices.Contains(r, s), nil
		}
	}

	_ = env

	return nil, apimExprFail("the method %s is not supported on this value", n.name)
}

func apimExprStaticCall(typ, name string, args []any) (any, error) {
	switch {
	case typ == apxInt && name == apxParse && len(args) == 1:
		return apimExprIntParse(args[0])
	case typ == "Guid" && name == "NewGuid" && len(args) == 0:
		return uuid.NewString(), nil
	case typ == apxString && name == "IsNullOrEmpty" && len(args) == 1:
		s, _ := args[0].(string)

		return s == "", nil
	case typ == apxDateTime && name == apxParse && len(args) == 1:
		s, _ := args[0].(string)

		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return nil, apimExprFail("the string is not a valid DateTime")
		}

		return t.UTC(), nil
	}

	return nil, apimExprFail("%s.%s is not supported", typ, name)
}

func apimExprIntParse(arg any) (any, error) {
	s, ok := arg.(string)
	if !ok {
		return nil, apimExprFail("int.Parse needs a string")
	}

	i, err := strconv.ParseInt(strings.TrimSpace(s), 10, 32)
	if err != nil {
		return nil, apimExprFail("input string '%s' was not in a correct format", s)
	}

	return i, nil
}

func apimExprFold(args []any, i int) (bool, error) {
	if len(args) <= i {
		return false, nil
	}

	st, ok := args[i].(apimExprStatic)
	if !ok {
		return false, apimExprFail("the comparison argument must be a StringComparison value")
	}

	return st.name == apxOrdinalIC, nil
}

func apimExprStringCall(s, name string, args []any) (any, error) {
	arg := ""
	if len(args) > 0 {
		a, ok := args[0].(string)
		if !ok && name != apxToString {
			return nil, apimExprFail("%s needs a string argument", name)
		}

		arg = a
	}

	switch name {
	case "ToLower", "ToLowerInvariant":
		return strings.ToLower(s), nil
	case "ToUpper", "ToUpperInvariant":
		return strings.ToUpper(s), nil
	case apxTrim:
		return strings.TrimSpace(s), nil
	case apxContains, apxStartsWith, apxEndsWith, "Equals":
		fold, err := apimExprFold(args, 1)
		if err != nil {
			return nil, err
		}

		if len(args) == 0 {
			return nil, apimExprFail("%s needs an argument", name)
		}

		if fold {
			s, arg = strings.ToLower(s), strings.ToLower(arg)
		}

		switch name {
		case apxContains:
			return strings.Contains(s, arg), nil
		case apxStartsWith:
			return strings.HasPrefix(s, arg), nil
		case apxEndsWith:
			return strings.HasSuffix(s, arg), nil
		}

		return s == arg, nil
	}

	return nil, apimExprFail("string.%s is not supported", name)
}

func apimExprDictCall(d *apimExprDict, n *apimExprNode, args []any) (any, error) {
	if len(args) == 0 {
		return nil, apimExprFail("%s needs arguments", n.name)
	}

	key, isStr := args[0].(string)
	if !isStr {
		return nil, apimExprFail("a dictionary key must be a string")
	}

	v, found := d.get(key)

	switch n.name {
	case "ContainsKey":
		return found, nil
	case "GetValueOrDefault":
		if len(args) == 1 && n.typ != "" {
			args = append(args, apimExprZero(n.typ))
		}

		if len(args) != apimExprKeyDefault {
			return nil, apimExprFail("GetValueOrDefault needs a key and a default")
		}

		if !found {
			return args[1], nil
		}

		if sl, ok := v.([]string); ok {
			return strings.Join(sl, ","), nil
		}

		return v, nil
	}

	return nil, apimExprFail("%s is not supported on a dictionary", n.name)
}

func apimExprTimeCall(t time.Time, name string, args []any) (any, error) {
	if len(args) == 1 && name == apxToString {
		f, _ := args[0].(string)

		switch f {
		case "o", "O":
			return t.UTC().Format("2006-01-02T15:04:05.0000000Z"), nil
		case "s":
			return t.UTC().Format("2006-01-02T15:04:05"), nil
		case "yyyy-MM-ddTHH:mm:ssZ":
			return t.UTC().Format("2006-01-02T15:04:05Z"), nil
		}

		return nil, apimExprFail("the DateTime format '%s' is not supported", f)
	}

	if len(args) == 1 {
		if i, ok := args[0].(int64); ok {
			switch name {
			case "AddSeconds":
				return t.Add(time.Duration(i) * time.Second), nil
			case "AddMinutes":
				return t.Add(time.Duration(i) * time.Minute), nil
			case "AddHours":
				return t.Add(time.Duration(i) * time.Hour), nil
			case "AddDays":
				return t.AddDate(0, 0, int(i)), nil
			}
		}
	}

	return nil, apimExprFail("DateTime.%s is not supported", name)
}

// apimExprChain returns the member names of a pure context.A.B.C chain.
func apimExprChain(n *apimExprNode) ([]string, bool) {
	var rev []string

	for n.kind == nMember {
		rev = append(rev, n.name)
		n = n.args[0]
	}

	if n.kind != nIdent || n.name != apxContext {
		return nil, false
	}

	slices.Reverse(rev)

	return rev, true
}

func apimExprContext() map[string][]string {
	return map[string][]string{
		apxRequest:      {apxMethod, apxURL, apxOriginalURL, apxHeaders, apxMatchedParameters, apxIPAddress},
		apxResponse:     {apxStatusCode, apxStatusReason, apxHeaders},
		apxVariables:    {},
		apxSubscription: {apxID, apxName},
		apxProduct:      {apxID, apxName},
		apxAPI:          {apxID, apxName, apxPath},
		apxOperation:    {apxID, apxName, apxMethod, apxURLTemplate},
		apxUser:         {apxID},
		apxLastError:    {apxSource, apxReason, apxMessage, apxScope, apxSection, apxPath, apxPolicyID},
		apxRequestID:    {},
		apxTimestamp:    {},
	}
}

func apimExprURLParts() []string {
	return []string{apxPath, apxQuery, apxHost, apxPort, apxScheme, apxQueryString}
}

func apimExprCheckChain(chain []string) error {
	allowed, ok := apimExprContext()[chain[0]]
	if !ok {
		return fmt.Errorf("%w: context.%s is not supported", errAPIMExprSyntax, chain[0])
	}

	if len(chain) == 1 {
		return nil
	}

	if len(chain) == 2 && slices.Contains(allowed, chain[1]) {
		return nil
	}

	if len(chain) == 3 && chain[0] == apxRequest && (chain[1] == apxURL || chain[1] == apxOriginalURL) &&
		slices.Contains(apimExprURLParts(), chain[2]) {
		return nil
	}

	return fmt.Errorf("%w: context.%s is not supported", errAPIMExprSyntax, strings.Join(chain, "."))
}

var apimXMLEntityRE = regexp.MustCompile(`^&(amp|lt|gt|quot|apos|#[0-9]+|#x[0-9a-fA-F]+);`)

// apimEscapeExpressions XML-escapes the body of each @(...) and @{...} expression, as the real gateway accepts
// documented policies that write unescaped quotes, ampersands and angle brackets inside expressions.
func apimEscapeExpressions(doc string) string {
	if !strings.Contains(doc, "@(") && !strings.Contains(doc, "@{") {
		return doc
	}

	var sb strings.Builder

	for i := 0; i < len(doc); {
		if doc[i] != '@' || i+1 >= len(doc) || (doc[i+1] != '(' && doc[i+1] != '{') {
			sb.WriteByte(doc[i])
			i++

			continue
		}

		end := apimExprEnd(doc, i+1)
		if end < 0 {
			sb.WriteString(doc[i:])

			break
		}

		sb.WriteString(doc[i : i+2])
		sb.WriteString(apimEscapeExprBody(doc[i+2 : end]))
		sb.WriteByte(doc[end])

		i = end + 1
	}

	return sb.String()
}

// apimExprEnd returns the index of the bracket closing the one at open, skipping C# string literals.
func apimExprEnd(doc string, open int) int {
	depth, inStr := 0, false

	for i := open; i < len(doc); i++ {
		c := doc[i]

		switch {
		case inStr && c == '\\':
			i++
		case c == '"' || (inStr && strings.HasPrefix(doc[i:], "&quot;")):
			if c != '"' {
				i += len("&quot;") - 1
			}

			inStr = !inStr
		case inStr:
		case c == '(' || c == '{':
			depth++
		case c == ')' || c == '}':
			if depth--; depth == 0 {
				return i
			}
		}
	}

	return -1
}

func apimEscapeExprBody(body string) string {
	var sb strings.Builder

	for i := range len(body) {
		switch body[i] {
		case '&':
			if apimXMLEntityRE.MatchString(body[i:]) {
				sb.WriteByte('&')
			} else {
				sb.WriteString("&amp;")
			}
		case '<':
			sb.WriteString("&lt;")
		case '"':
			sb.WriteString("&quot;")
		default:
			sb.WriteByte(body[i])
		}
	}

	return sb.String()
}

func apimExprZero(typ string) any {
	switch typ {
	case apxTypeBool:
		return false
	case apxInt, apxLong:
		return int64(0)
	}

	return nil
}
