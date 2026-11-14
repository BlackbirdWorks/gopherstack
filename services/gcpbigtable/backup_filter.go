package gcpbigtable

import (
	"cmp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	fieldName        = "name"
	fieldSourceTable = "source_table"
	fieldState       = "state"
	fieldStart       = "start_time"
	fieldEnd         = "end_time"
	fieldExpire      = "expire_time"
	fieldSize        = "size_bytes"
)

type bkPred func(*adminpb.Backup) bool

type bkTok struct {
	text   string
	quoted bool
}

func readBackupQuoted(rs []rune, i int) (string, int, error) {
	var sb strings.Builder

	for i++; i < len(rs) && rs[i] != '"'; i++ {
		if rs[i] == '\\' && i+1 < len(rs) {
			i++
		}

		sb.WriteRune(rs[i])
	}

	if i >= len(rs) {
		return "", 0, errInvalid("Invalid filter: unterminated quoted string.")
	}

	return sb.String(), i + 1, nil
}

func readBackupOp(rs []rune, i int) int {
	j := i + 1
	if j < len(rs) && rs[j] == '=' && rs[i] != ':' && rs[i] != '=' {
		j++
	}

	return j
}

func readBackupWord(rs []rune, i int) int {
	j := i
	for j < len(rs) && !unicode.IsSpace(rs[j]) && !strings.ContainsRune("()\"=!<>:", rs[j]) {
		j++
	}

	return j
}

func tokenizeBackupFilter(s string) ([]bkTok, error) {
	var toks []bkTok

	rs := []rune(s)

	for i := 0; i < len(rs); {
		r := rs[i]

		switch {
		case unicode.IsSpace(r):
			i++
		case r == '(' || r == ')' || (r == '-' && i+1 < len(rs) && !unicode.IsSpace(rs[i+1])):
			toks = append(toks, bkTok{text: string(r)})
			i++
		case r == '"':
			text, next, err := readBackupQuoted(rs, i)
			if err != nil {
				return nil, err
			}

			toks = append(toks, bkTok{text: text, quoted: true})
			i = next
		case strings.ContainsRune("=!<>:", r):
			j := readBackupOp(rs, i)
			toks = append(toks, bkTok{text: string(rs[i:j])})
			i = j
		default:
			j := readBackupWord(rs, i)
			toks = append(toks, bkTok{text: string(rs[i:j])})
			i = j
		}
	}

	return toks, nil
}

type bkParser struct {
	toks []bkTok
	pos  int
}

func (p *bkParser) peek() (bkTok, bool) {
	if p.pos >= len(p.toks) {
		return bkTok{}, false
	}

	return p.toks[p.pos], true
}

func (p *bkParser) keyword(kw string) bool {
	t, ok := p.peek()
	if ok && !t.quoted && t.text == kw {
		p.pos++

		return true
	}

	return false
}

// parseBackupFilter follows AIP-160: OR binds tighter than (implicit) AND.
func parseBackupFilter(s string) (bkPred, error) {
	toks, err := tokenizeBackupFilter(s)
	if err != nil {
		return nil, err
	}

	p := &bkParser{toks: toks}

	pred, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	if _, more := p.peek(); more {
		return nil, errInvalid("Invalid filter: unexpected %q.", p.toks[p.pos].text)
	}

	return pred, nil
}

func (p *bkParser) parseAnd() (bkPred, error) {
	first, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	preds := []bkPred{first}

	for {
		t, ok := p.peek()
		if !ok || (!t.quoted && t.text == ")") {
			break
		}

		p.keyword("AND")

		next, nerr := p.parseOr()
		if nerr != nil {
			return nil, nerr
		}

		preds = append(preds, next)
	}

	return func(b *adminpb.Backup) bool {
		return !slices.ContainsFunc(preds, func(f bkPred) bool { return !f(b) })
	}, nil
}

func (p *bkParser) parseOr() (bkPred, error) {
	first, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	preds := []bkPred{first}

	for p.keyword("OR") {
		next, nerr := p.parseTerm()
		if nerr != nil {
			return nil, nerr
		}

		preds = append(preds, next)
	}

	return func(b *adminpb.Backup) bool {
		return slices.ContainsFunc(preds, func(f bkPred) bool { return f(b) })
	}, nil
}

func (p *bkParser) parseTerm() (bkPred, error) {
	if p.keyword("NOT") {
		inner, err := p.parseTerm()
		if err != nil {
			return nil, err
		}

		return func(b *adminpb.Backup) bool { return !inner(b) }, nil
	}

	t, ok := p.peek()
	if !ok {
		return nil, errInvalid("Invalid filter: unexpected end of expression.")
	}

	if !t.quoted && t.text == "-" {
		p.pos++

		inner, err := p.parseTerm()
		if err != nil {
			return nil, err
		}

		return func(b *adminpb.Backup) bool { return !inner(b) }, nil
	}

	if !t.quoted && t.text == "(" {
		p.pos++

		inner, err := p.parseAnd()
		if err != nil {
			return nil, err
		}

		if c, more := p.peek(); !more || c.text != ")" || c.quoted {
			return nil, errInvalid("Invalid filter: missing closing parenthesis.")
		}

		p.pos++

		return inner, nil
	}

	return p.parseComparison()
}

func (p *bkParser) parseComparison() (bkPred, error) {
	if p.pos+3 > len(p.toks) {
		return nil, errInvalid("Invalid filter: expected field, operator and value.")
	}

	field, op, val := p.toks[p.pos], p.toks[p.pos+1], p.toks[p.pos+2]
	p.pos += 3

	if field.quoted || op.quoted {
		return nil, errInvalid("Invalid filter: expected field, operator and value.")
	}

	switch op.text {
	case "=", "!=", "<", ">", "<=", ">=", ":":
	default:
		return nil, errInvalid("Invalid filter: unknown operator %q.", op.text)
	}

	return buildBackupComparison(field.text, op.text, val.text)
}

func buildBackupComparison(field, op, val string) (bkPred, error) {
	switch field {
	case fieldName, fieldSourceTable:
		return stringComparison(field, op, val)
	case fieldState:
		return enumComparison(op, val)
	case fieldStart, fieldEnd, fieldExpire:
		return timeComparison(field, op, val)
	case fieldSize:
		return sizeComparison(op, val)
	default:
		return nil, errInvalid("Invalid filter: unsupported field %q.", field)
	}
}

func stringComparison(field, op, val string) (bkPred, error) {
	get := func(b *adminpb.Backup) string {
		if field == fieldName {
			return b.GetName()
		}

		return b.GetSourceTable()
	}

	switch op {
	case ":":
		return func(b *adminpb.Backup) bool { return strings.Contains(get(b), val) }, nil
	case "=":
		return func(b *adminpb.Backup) bool { return get(b) == val }, nil
	case "!=":
		return func(b *adminpb.Backup) bool { return get(b) != val }, nil
	default:
		return nil, errInvalid("Invalid filter: operator %q is not valid for %s.", op, field)
	}
}

func enumComparison(op, val string) (bkPred, error) {
	if _, ok := adminpb.Backup_State_value[val]; !ok {
		return nil, errInvalid("Invalid filter: unknown state %q.", val)
	}

	switch op {
	case ":", "=":
		return func(b *adminpb.Backup) bool { return b.GetState().String() == val }, nil
	case "!=":
		return func(b *adminpb.Backup) bool { return b.GetState().String() != val }, nil
	default:
		return nil, errInvalid("Invalid filter: operator %q is not valid for state.", op)
	}
}

func cmpPred(op string, c func(*adminpb.Backup) int) (bkPred, error) {
	var test func(int) bool

	switch op {
	case "=", ":":
		test = func(n int) bool { return n == 0 }
	case "!=":
		test = func(n int) bool { return n != 0 }
	case "<":
		test = func(n int) bool { return n < 0 }
	case "<=":
		test = func(n int) bool { return n <= 0 }
	case ">":
		test = func(n int) bool { return n > 0 }
	case ">=":
		test = func(n int) bool { return n >= 0 }
	default:
		return nil, errInvalid("Invalid filter: unknown operator %q.", op)
	}

	return func(b *adminpb.Backup) bool { return test(c(b)) }, nil
}

func backupTime(b *adminpb.Backup, field string) *timestamppb.Timestamp {
	switch field {
	case fieldStart:
		return b.GetStartTime()
	case fieldEnd:
		return b.GetEndTime()
	default:
		return b.GetExpireTime()
	}
}

func timeComparison(field, op, val string) (bkPred, error) {
	want, err := time.Parse(time.RFC3339, val)
	if err != nil {
		return nil, errInvalid("Invalid filter: %q is not an RFC 3339 timestamp.", val)
	}

	return cmpPred(op, func(b *adminpb.Backup) int {
		return backupTime(b, field).AsTime().Compare(want)
	})
}

func sizeComparison(op, val string) (bkPred, error) {
	want, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return nil, errInvalid("Invalid filter: %q is not an integer.", val)
	}

	return cmpPred(op, func(b *adminpb.Backup) int { return cmp.Compare(b.GetSizeBytes(), want) })
}

type bkOrder struct {
	field string
	desc  bool
}

func validBackupOrderField(f string) bool {
	switch f {
	case fieldName, fieldSourceTable, fieldExpire, fieldStart, fieldEnd, fieldSize, fieldState:
		return true
	default:
		return false
	}
}

func parseBackupOrder(s string) ([]bkOrder, error) {
	if strings.TrimSpace(s) == "" {
		return []bkOrder{{field: fieldStart, desc: true}}, nil
	}

	var out []bkOrder

	for part := range strings.SplitSeq(s, ",") {
		f := strings.Fields(part)
		if len(f) == 0 || len(f) > 2 {
			return nil, errInvalid("Invalid order_by %q.", s)
		}

		o := bkOrder{field: f[0]}
		if !validBackupOrderField(o.field) {
			return nil, errInvalid("Invalid order_by: unsupported field %q.", o.field)
		}

		if len(f) > 1 {
			switch strings.ToLower(f[1]) {
			case "desc":
				o.desc = true
			case "asc":
			default:
				return nil, errInvalid("Invalid order_by: unknown direction %q.", f[1])
			}
		}

		out = append(out, o)
	}

	return out, nil
}

func compareBackups(a, b *adminpb.Backup, field string) int {
	switch field {
	case fieldSourceTable:
		return strings.Compare(a.GetSourceTable(), b.GetSourceTable())
	case fieldExpire, fieldStart, fieldEnd:
		return backupTime(a, field).AsTime().Compare(backupTime(b, field).AsTime())
	case fieldSize:
		return cmp.Compare(a.GetSizeBytes(), b.GetSizeBytes())
	case fieldState:
		return cmp.Compare(a.GetState(), b.GetState())
	default:
		return strings.Compare(a.GetName(), b.GetName())
	}
}

func sortBackups(all []*adminpb.Backup, order []bkOrder) {
	slices.SortStableFunc(all, func(a, b *adminpb.Backup) int {
		for _, o := range order {
			c := compareBackups(a, b, o.field)
			if o.desc {
				c = -c
			}

			if c != 0 {
				return c
			}
		}

		return strings.Compare(a.GetName(), b.GetName())
	})
}
